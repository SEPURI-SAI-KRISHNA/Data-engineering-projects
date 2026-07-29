import json
import re
import os
import logging
from typing import Dict, List, Any, Optional
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("es_assessor")


class QueryAssessor:
    """Static analysis for Elasticsearch queries before they hit a cluster.

    Checks cover heap cost of aggregations (cardinality-aware bucket
    estimation, global ordinals), shard fan-out, wide _source fetches,
    script/regex hazards, rescore and highlighting overhead, and common
    anti-patterns like match_all with track_total_hits. Optionally asks an
    LLM for a rewrite suggestion when an API key is provided.
    """

    # --- CONSTANTS & THRESHOLDS ---
    AVG_BUCKET_COST_BYTES = 250
    SAFE_HEAP_THRESHOLD = 0.40
    MAX_BOOL_DEPTH = 5
    MAX_SHARD_FANOUT = 50
    LARGE_INDEX_DOCS = 1_000_000  # Threshold to consider an index "Large"

    # Weighted Scoring System
    WEIGHTS = {
        "CRITICAL": 100,  # Immediate Block
        "HIGH": 30,  # Performance degradation likely
        "MEDIUM": 15,  # Optimization recommended
        "LOW": 5  # Best practice warning
    }

    def __init__(self, mapping: Dict, cluster_config: Dict, index_stats: Dict, openai_api_key: Optional[str] = None):
        self.mapping = mapping or {}
        self.config = cluster_config
        self.stats = index_stats
        self.issues = []

        # Parse Configs
        self.heap_limit_bytes = self._parse_bytes(self.config.get('heap_size', '1GB'))
        self.max_buckets_limit = self.config.get('search.max_buckets', 65536)
        self.max_result_window = self.config.get('index.max_result_window', 10000)
        self.doc_count = self.stats.get('docs', {}).get('count', 0)

        # LLM Setup
        self.llm_client = None
        if openai_api_key:
            self.llm_client = OpenAI(api_key=openai_api_key)

    def assess(self, query_body: Dict, params: Dict = None) -> Dict:
        """Main execution pipeline."""
        self.issues = []
        params = params or {}

        # --- PHASE 1: STABILITY & CIRCUIT BREAKERS ---
        self._check_script_compilation(query_body)
        self._check_nested_complexity(query_body)
        self._check_regex_complexity(query_body)

        # --- PHASE 2: CPU & RELEVANCE GREED ---
        self._check_track_total_hits(query_body)
        self._check_expensive_scoring(query_body)  # min_score, rescore
        self._check_wildcards_recursive(query_body)
        self._check_boolean_depth(query_body)
        self._check_joins(query_body)  # has_child/parent

        # --- PHASE 3: DATA HEAVYWEIGHTS (IO) ---
        self._check_highlighting_performance(query_body)
        self._check_script_fields(query_body)
        self._check_docvalue_fields(query_body)

        # --- PHASE 4: NETWORK & INFRASTRUCTURE ---
        self._check_search_type(params)
        self._check_source_filtering(query_body)
        self._check_shard_fanout()
        self._check_pagination(query_body)

        # --- PHASE 5: LOGIC & ANTI-PATTERNS ---
        self._check_routing(params)
        self._check_scroll_abuse(params)
        self._check_blind_match_all(query_body)
        self._check_post_filter(query_body)
        self._check_unbounded_ranges_recursive(query_body)

        # --- PHASE 6: MEMORY (The Smart Engine) ---
        aggs = query_body.get('aggs', query_body.get('aggregations', {}))
        self._check_global_ordinals_risk(aggs)
        self._check_aggregations_memory(aggs)

        # --- PHASE 7: REPORT & OPTIMIZE ---
        score_card = self._generate_score_card()

        llm_advice = None
        # Trigger LLM if score > 20 OR complex features found
        if self.llm_client and (score_card['risk_score'] > 20 or self._has_complex_features(query_body)):
            llm_advice = self._consult_llm(query_body, score_card['details'])

        return {
            **score_card,
            "llm_analysis": llm_advice
        }

    # =========================================================
    # 1. RELEVANCE GREED (CPU)
    # =========================================================

    def _check_track_total_hits(self, query):
        """Checks if user is forcing exact hit counts on large datasets."""
        track = query.get('track_total_hits')
        if track is True and self.doc_count > self.LARGE_INDEX_DOCS:
            self.issues.append({
                "severity": "MEDIUM", "category": "Performance",
                "message": "track_total_hits:true disables Block-Max WAND optimization. Set to 10000 or false."
            })

    def _check_expensive_scoring(self, query):
        """Checks for min_score and heavy rescore windows."""
        if 'min_score' in query:
            self.issues.append({
                "severity": "LOW", "category": "Performance",
                "message": "min_score prevents efficient skipping of non-matching documents."
            })

        if 'rescore' in query:
            # Handle list of rescores or single dict
            rescores = query['rescore'] if isinstance(query['rescore'], list) else [query['rescore']]
            for r in rescores:
                window = r.get('window_size', 10)
                if window > 500:
                    self.issues.append({
                        "severity": "HIGH", "category": "CPU",
                        "message": f"Rescore window {window} is too high (>500). Heavy CPU cost."
                    })

    # =========================================================
    # 2. DATA HEAVYWEIGHTS (IO/Network)
    # =========================================================

    def _check_highlighting_performance(self, query):
        """Checks if highlighting is requested on fields without term_vectors."""
        hl = query.get('highlight', {})
        if not hl: return

        # fields can be a dict or list in some clients, usually dict
        fields = hl.get('fields', {})
        for field_name in fields:
            # Mapping Lookup
            mapping = self._find_field_mapping(field_name)
            if mapping:
                t_vec = mapping.get('term_vector', 'no')
                if t_vec == 'no' and self.doc_count > 10000:
                    self.issues.append({
                        "severity": "HIGH", "category": "IO/CPU",
                        "message": f"Highlighting '{field_name}' is slow. Enable 'term_vector': 'with_positions_offsets' in mapping."
                    })

    def _check_script_fields(self, query):
        """Detects usage of script_fields (generates data on fly)."""
        if 'script_fields' in query:
            self.issues.append({
                "severity": "MEDIUM", "category": "CPU",
                "message": "script_fields detected. Disables _source fetching and requires heap for DocValues."
            })

    def _check_docvalue_fields(self, query):
        """Checks for docvalue fetching on potentially large text fields."""
        dv_fields = query.get('docvalue_fields', [])
        for f in dv_fields:
            fname = f.get('field', f) if isinstance(f, dict) else f
            # Heuristic: if field is 'text' type in mapping, this is bad
            mapping = self._find_field_mapping(fname)
            if mapping and mapping.get('type') == 'text':
                self.issues.append({
                    "severity": "HIGH", "category": "IO",
                    "message": f"Fetching docvalues on text field '{fname}' causes massive Disk I/O."
                })

    # =========================================================
    # 3. LOGIC & ANTI-PATTERNS
    # =========================================================

    def _check_routing(self, params):
        """Checks if routing is missing on an index that requires it."""
        # Check mapping for _routing required
        mappings = self.mapping.get('mappings', {})
        routing_conf = mappings.get('_routing', {})

        if routing_conf.get('required') is True:
            if not params.get('routing'):
                self.issues.append({
                    "severity": "CRITICAL", "category": "Logic",
                    "message": "Index requires custom routing, but none provided. Query will fail."
                })
        elif self.stats.get('shards', {}).get('total', 1) > 20 and not params.get('routing'):
            self.issues.append({
                "severity": "LOW", "category": "Optimization",
                "message": "High shard count. Providing 'routing' param would significantly reduce fan-out."
            })

    def _check_scroll_abuse(self, params):
        """Scroll should not be used for user-facing search."""
        if params.get('scroll'):
            self.issues.append({
                "severity": "MEDIUM", "category": "Stability",
                "message": "Scroll API detected. Do not use for real-time user search; use 'search_after'."
            })

    def _check_blind_match_all(self, query):
        """Detects match_all on large indices."""
        if 'match_all' in query.get('query', {}) and self.doc_count > self.LARGE_INDEX_DOCS:
            self.issues.append({
                "severity": "LOW", "category": "Cache Pollution",
                "message": "Unfiltered match_all on large index pollutes cache with random data."
            })

    def _check_joins(self, query):
        """Detects expensive parent-child joins."""
        q_str = json.dumps(query)
        if 'has_child' in q_str or 'has_parent' in q_str:
            self.issues.append({
                "severity": "HIGH", "category": "Memory",
                "message": "Parent-Child Join detected. Slower than Nested. Requires loading Global Ordinals."
            })

    # =========================================================
    # 4. CORE STABILITY & SECURITY
    # =========================================================

    def _check_nested_complexity(self, query):
        if "nested" in json.dumps(query):
            self.issues.append(
                {"severity": "MEDIUM", "category": "CPU", "message": "Nested Query detected. High join cost."})

    def _check_script_compilation(self, query):
        scripts = self._find_all_values_by_key(query, 'script')
        for script in scripts:
            if isinstance(script, dict):
                src = script.get('source', '')
                params = script.get('params', {})
                if re.search(r"['\"]?\b\d+\b['\"]?", src) and not params:
                    self.issues.append({
                        "severity": "CRITICAL", "category": "Stability",
                        "message": "Hardcoded values in script. Use 'params' to prevent Circuit Breaker trips."
                    })

    def _check_regex_complexity(self, query):
        regexes = self._find_all_values_by_key(query, 'regexp')
        for reg in regexes:
            for f, val in reg.items():
                pat = val.get('value', val) if isinstance(val, dict) else val
                if isinstance(pat, str) and (pat.startswith('.*') or pat.startswith('.+')):
                    self.issues.append({"severity": "CRITICAL", "category": "CPU", "message": f"Evil Regex on {f}"})

    # =========================================================
    # 5. MEMORY & AGGS (Smart Engine)
    # =========================================================

    def _check_global_ordinals_risk(self, aggs):
        """Checks for high cardinality terms aggs."""
        if not aggs: return

        def recurse(node):
            for k, body in node.items():
                if 'terms' in body:
                    f = body['terms'].get('field')
                    card = self._get_field_cardinality(f)
                    if card and card > 1_000_000:
                        self.issues.append({
                            "severity": "HIGH", "category": "Latency",
                            "message": f"Agg on '{f}' (Card: {card:,}) triggers expensive Global Ordinals build."
                        })
                if 'aggs' in body: recurse(body['aggs'])
                if 'aggregations' in body: recurse(body['aggregations'])

        recurse(aggs)

    def _check_aggregations_memory(self, aggs_node):
        if not aggs_node: return
        total = self._calculate_smart_buckets(aggs_node)

        if total > self.max_buckets_limit:
            self.issues.append({
                "severity": "CRITICAL", "category": "OOM",
                "message": f"Est. Buckets {total} > Limit {self.max_buckets_limit}."
            })

        est_ram = total * self.AVG_BUCKET_COST_BYTES
        ram_pct = est_ram / self.heap_limit_bytes
        if ram_pct > self.SAFE_HEAP_THRESHOLD:
            self.issues.append({
                "severity": "CRITICAL", "category": "Heap",
                "message": f"Est. RAM {est_ram / 1024 / 1024:.1f}MB is {ram_pct:.1%} of Heap."
            })

    def _calculate_smart_buckets(self, aggs):
        total = 0
        for _, body in aggs.items():
            current_size = 1
            if 'terms' in body:
                req = body['terms'].get('size', 10)
                card = self._get_field_cardinality(body['terms'].get('field'))
                current_size = min(req, card) if card else req
            elif 'date_histogram' in body:
                current_size = 50

            sub = body.get('aggs', body.get('aggregations'))
            total += (current_size * self._calculate_smart_buckets(sub)) if sub else current_size
        return total

    # =========================================================
    # 6. HELPERS & LLM
    # =========================================================

    def _find_field_mapping(self, field_path):
        """Recursive lookup in mapping dict."""
        try:
            props = self.mapping.get('mappings', {}).get('properties', {})
            parts = field_path.split('.')
            curr = props
            for part in parts:
                if part in curr:
                    if 'properties' in curr[part]:
                        curr = curr[part]['properties']
                    else:
                        return curr[part]  # Found leaf
            return None
        except:
            return None

    def _consult_llm(self, query, issues):
        system_prompt = """
        You are an elite Elasticsearch Engineer.
        Review the Query and Static Analysis Findings.
        Output valid JSON with keys: "analysis" (text), "optimized_query" (json), "suggestions" (list).
        Focus on fixing the specific detected issues.
        """
        user_msg = f"STATS: {json.dumps(self.stats)}\nISSUES: {json.dumps(issues)}\nQUERY: {json.dumps(query)}"
        try:
            resp = self.llm_client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_msg}],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            return json.loads(resp.choices[0].message.content)
        except Exception:
            return None

    # ... (Include Standard recursive checks: wildcards, bool depth, unbounded ranges, etc. from previous versions) ...
    def _check_wildcards_recursive(self, node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == 'wildcard':
                    for f, val in v.items():
                        pat = val.get('value', val) if isinstance(val, dict) else val
                        if str(pat).startswith('*'): self.issues.append(
                            {"severity": "HIGH", "category": "CPU", "message": f"Leading Wildcard on {f}"})
                self._check_wildcards_recursive(v)
        elif isinstance(node, list):
            for i in node: self._check_wildcards_recursive(i)

    def _check_boolean_depth(self, node, depth=0):
        if depth > self.MAX_BOOL_DEPTH:
            self.issues.append(
                {"severity": "MEDIUM", "category": "Complexity", "message": "Deeply nested Boolean query."})
            return
        if isinstance(node, dict):
            for k, v in node.items():
                if k == 'bool':
                    self._check_boolean_depth(v, depth + 1)
                elif isinstance(v, (dict, list)):
                    self._check_boolean_depth(v, depth)
        elif isinstance(node, list):
            for i in node: self._check_boolean_depth(i, depth)

    def _check_unbounded_ranges_recursive(self, node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == 'range':
                    for f, b in v.items():
                        if not (any(x in b for x in ['gt', 'gte', 'from']) and any(
                                x in b for x in ['lt', 'lte', 'to'])):
                            self.issues.append(
                                {"severity": "LOW", "category": "Performance", "message": f"Unbounded range on {f}"})
                else:
                    self._check_unbounded_ranges_recursive(v)
        elif isinstance(node, list):
            for i in node: self._check_unbounded_ranges_recursive(i)

    def _check_post_filter(self, query):
        if 'post_filter' in query: self.issues.append(
            {"severity": "LOW", "category": "Logic", "message": "post_filter used."})

    def _check_search_type(self, params):
        if params.get('search_type') == 'dfs_query_then_fetch':
            self.issues.append(
                {"severity": "MEDIUM", "category": "Latency", "message": "DFS Query Then Fetch enabled."})

    def _check_source_filtering(self, query):
        if query.get('size', 10) > 50 and query.get('_source', True) is True:
            self.issues.append(
                {"severity": "LOW", "category": "Network", "message": "Fetching full _source for >50 docs."})

    def _check_shard_fanout(self):
        shards = self.stats.get('shards', {}).get('total', 1)
        if shards > self.MAX_SHARD_FANOUT:
            self.issues.append({"severity": "MEDIUM", "category": "Network", "message": f"Fan-out: {shards} shards."})

    def _check_pagination(self, query):
        total = query.get('from', 0) + query.get('size', 10)
        if total > self.max_result_window:
            self.issues.append({"severity": "CRITICAL", "category": "Limits", "message": "Result Window Exceeded"})
        elif total > 5000:
            self.issues.append({"severity": "HIGH", "category": "IO", "message": "Deep Pagination."})

    def _find_all_values_by_key(self, node, key):
        found = []
        if isinstance(node, dict):
            for k, v in node.items():
                if k == key: found.append(v)
                found.extend(self._find_all_values_by_key(v, key))
        elif isinstance(node, list):
            for i in node: found.extend(self._find_all_values_by_key(i, key))
        return found

    def _get_field_cardinality(self, field):
        return self.stats.get('field_stats', {}).get(field, {}).get('cardinality')

    def _has_complex_features(self, query):
        return any(x in json.dumps(query) for x in ['script', 'wildcard', 'regexp', 'nested', 'post_filter', 'rescore'])

    def _generate_score_card(self):
        score = 0
        breakdown = {}
        for i in self.issues:
            weight = self.WEIGHTS.get(i['severity'], 5)
            score += weight
            breakdown[i['severity']] = breakdown.get(i['severity'], 0) + 1

        risk = min(100, score)
        if risk >= 80:
            decision = "BLOCKED"
        elif risk >= 40:
            decision = "REVIEW_REQUIRED"
        else:
            decision = "APPROVED"

        return {
            "decision": decision,
            "risk_score": risk,
            "summary": f"Risk Score: {risk}/100. Issues: {len(self.issues)}",
            "details": self.issues
        }

    def _parse_bytes(self, s):
        units = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}
        if not isinstance(s, str): return 1024 ** 3
        m = re.match(r'^(\d+)([A-Z]+)$', s.upper().strip())
        return int(m.group(1)) * units.get(m.group(2), 1) if m else 1024 ** 3


# =========================================================
# RUNTIME EXAMPLE
# =========================================================
if __name__ == "__main__":
    mapping = {
        "mappings": {
            "properties": {
                "bio": {"type": "text", "term_vector": "no"},  # Bad for highlighting
                "status": {"type": "keyword"}
            },
            "_routing": {"required": True}  # Requires routing
        }
    }

    stats = {
        "docs": {"count": 5_000_000},
        "shards": {"total": 100},
        "field_stats": {"trace_id": {"cardinality": 2_000_000}}
    }

    # "All-in-One" Bad Query
    query = {
        "track_total_hits": True,  # Relevance Greed
        "highlight": {"fields": {"bio": {}}},  # Heavyweight (No term vector)
        "rescore": {"window_size": 1000, "query": {}},  # CPU Killer
        "query": {"match_all": {}},  # Anti-Pattern
        "aggs": {"x": {"terms": {"field": "trace_id"}}}  # Global Ordinals Risk
    }

    assessor = QueryAssessor(mapping, {"heap_size": "2GB"}, stats, os.environ.get("OPENAI_API_KEY"))
    report = assessor.assess(query, params={})  # routing param deliberately missing

    print(json.dumps(report, indent=2))