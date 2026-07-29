import json
import logging
import time
import re
from typing import Dict, Any, Tuple, Optional, List, Union

# Try importing elasticsearch for live features (optional)
try:
    from elasticsearch import Elasticsearch, NotFoundError

    HAS_ES_CLIENT = True
except ImportError:
    HAS_ES_CLIENT = False

import urllib.request
import urllib.error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ESQueryOptimizer")


class ElasticsearchOptimizerAgent:
    """LLM-backed helpers for working with Elasticsearch queries: SQL-to-DSL
    transpiling, syntax validation, filter-context and aggregation rewrites,
    caching/fetch advice, deprecation checks, cost estimation, and unit test
    generation.
    """

    def __init__(self, openai_api_key: str, model: str = "gpt-4o"):
        self.api_key = openai_api_key
        self.model = model
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        self.api_url = "https://api.openai.com/v1/chat/completions"

    # --- HELPER: LLM CALLER ---
    def _call_llm(self, system_prompt: str, user_content: str, json_mode: bool = True) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.1,  # Low temp for code determinism
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}

        retries = 3
        for attempt in range(retries):
            try:
                req = urllib.request.Request(
                    self.api_url,
                    data=json.dumps(payload).encode('utf-8'),
                    headers=self.headers,
                    method="POST"
                )
                with urllib.request.urlopen(req) as response:
                    response_data = json.loads(response.read().decode('utf-8'))
                    return response_data['choices'][0]['message']['content']
            except urllib.error.HTTPError as e:
                logger.error(f"API Request failed (Attempt {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise
                time.sleep(2)
        raise Exception("Failed to contact LLM API.")

    # --- FEATURE: SQL TO DSL TRANSPILER ---
    def transpile_sql_if_needed(self, query_input: Union[str, Dict]) -> Dict[str, Any]:
        """
        Checks if input is a SQL string. If so, converts to DSL.
        """
        if isinstance(query_input, dict):
            return query_input

        # Simple heuristic to detect SQL
        cleaned = query_input.strip().upper()
        if not cleaned.startswith("SELECT"):
            # Assume it's a JSON string and try to parse it
            try:
                return json.loads(query_input)
            except json.JSONDecodeError:
                raise ValueError("Input is neither a valid dictionary, JSON string, nor a SQL SELECT statement.")

        logger.info("SQL input detected. Transpiling to Elasticsearch DSL...")
        system_prompt = """
        You are an expert SQL to Elasticsearch DSL Transpiler.
        Convert the given SQL query into a valid Elasticsearch JSON Query DSL.
        Output JSON: {"dsl": object, "conversion_notes": str}
        """

        try:
            response = self._call_llm(system_prompt, query_input)
            result = json.loads(response)
            logger.info("Transpilation successful.")
            return result.get("dsl", {})
        except Exception as e:
            logger.error(f"Transpilation failed: {e}")
            raise ValueError("Failed to convert SQL to DSL.")

    # --- FEATURE: COST & CAPACITY ESTIMATOR ---
    def estimate_cost(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates a complexity score based on query structure.
        """
        complexity_score = 0
        factors = []

        def walk(node):
            nonlocal complexity_score
            if isinstance(node, dict):
                # Penalty: Scripts (CPU intensive)
                if "script" in node:
                    complexity_score += 20
                    factors.append("Script usage (+20)")

                # Penalty: Wildcards (Scan intensive)
                if "wildcard" in node or "regexp" in node:
                    complexity_score += 10
                    factors.append("Wildcard/Regexp (+10)")

                # Penalty: Aggregations (Memory intensive)
                if "aggs" in node or "aggregations" in node:
                    complexity_score += 5
                    factors.append("Aggregations (+5)")

                # Penalty: Deep Pagination
                if "from" in node and isinstance(node["from"], int) and node["from"] > 1000:
                    complexity_score += 5
                    factors.append("Deep Pagination (+5)")

                # Penalty: Sorting on text (Memory intensive/Fielddata)
                # Note: This is a rough check; requires mapping awareness for accuracy
                if "sort" in node:
                    complexity_score += 2

                for k, v in node.items():
                    walk(v)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(query)

        capacity_label = "Low"
        if complexity_score > 10: capacity_label = "Medium"
        if complexity_score > 30: capacity_label = "High"

        return {
            "score": complexity_score,
            "label": capacity_label,
            "cost_factors": factors
        }

    # --- FEATURE: DEPRECATION CHECKER ---
    def check_deprecations(self, query: Dict[str, Any]) -> List[str]:
        """
        Checks for legacy features (targeting ES 7/8 compatibility).
        """
        warnings = []
        query_str = json.dumps(query)

        checks = {
            r'"filtered"\s*:': "The 'filtered' query is deprecated. Use 'bool' query with 'filter' clause.",
            r'"missing"\s*:': "The 'missing' query is deprecated. Use 'must_not': {'exists': ...}.",
            r'"_type"\s*:': "Mapping types are removed in ES 8. Avoid referring to '_type'.",
            r'"common"\s*:': "The 'common' terms query is deprecated. Use 'match' with 'cutoff_frequency'.",
        }

        for pattern, msg in checks.items():
            if re.search(pattern, query_str):
                warnings.append(msg)

        return warnings

    # --- FEATURE: FETCH & CACHING ADVISOR ---
    def analyze_fetch_and_cache(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes _source filtering and caching potential.
        """
        advice = []

        # 1. Fetch Optimization
        if "_source" not in query:
            advice.append(
                "FETCH: Query fetches full source. Use '_source': [...] to retrieve only necessary fields and save network bandwidth.")
        elif query["_source"] is False:
            advice.append("FETCH: Good job disabling source fetching if not needed.")

        if "docvalue_fields" not in query and "stored_fields" not in query and "_source" not in query:
            advice.append("FETCH: Consider using 'docvalue_fields' for fast retrieval of specific atomic fields.")

        # 2. Caching Advisor
        # Check for time ranges that break caching (e.g., raw "now")
        query_str = json.dumps(query)
        if '"now"' in query_str and '"now/d"' not in query_str and '"now/h"' not in query_str:
            advice.append(
                "CACHE: Usage of raw 'now' prevents caching. Round dates (e.g., 'now/h') to enable Request Caching.")

        if "size" in query and query["size"] == 0 and "aggs" in query:
            advice.append(
                "CACHE: This is an aggregation-only query. Ensure 'size: 0' is intentional to maximize caching.")

        return {"advice": advice}

    # --- FEATURE: VALIDATION (LLM) ---
    def validate_syntax(self, query: Dict[str, Any], mapping: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str]:
        logger.info("Validating syntax...")
        system_prompt = """
        You are an Elasticsearch Validator. 
        Output JSON: {"is_valid": bool, "fixed_query": object, "errors": [str], "explanation": str}
        """
        user_content = json.dumps({"query": query, "mapping": mapping}, indent=2)
        try:
            res = json.loads(self._call_llm(system_prompt, user_content))
            return res.get("is_valid", False), res.get("fixed_query", query), res.get("explanation", "")
        except:
            return False, query, "Validation error."

    # --- FEATURE: OPTIMIZER (AGGREGATION + QUERY) ---
    def optimize_query(self, query: Dict[str, Any], mapping: Dict[str, Any], doc_count: int) -> Dict[str, Any]:
        logger.info("Optimizing query & aggregations...")

        system_prompt = """
        You are a Senior Elasticsearch Engineer. Optimize the Query and Aggregations.

        Tasks:
        1. QUERY: Move 'must' to 'filter' context where possible. Use 'keyword' for terms.
        2. AGGREGATIONS: 
           - Suggest 'sampler' or 'diversified_sampler' for high-cardinality terms.
           - Check for deep nested aggregations.
           - Ensure 'execution_hint' is used if beneficial (e.g. 'map' for sparse data).
        3. REDUNDANCY: Remove duplicate boolean clauses.

        Output JSON: 
        {
            "optimized_query": object, 
            "changes_made": [str], 
            "reasoning": str,
            "aggregation_improvements": [str]
        }
        """
        user_content = json.dumps({"query": query, "mapping": mapping, "doc_count": doc_count}, indent=2)
        try:
            return json.loads(self._call_llm(system_prompt, user_content))
        except:
            return {"optimized_query": query, "changes_made": [], "reasoning": "Failed."}

    # --- FEATURE: UNIT TEST GEN ---
    def generate_unit_test(self, query: Dict[str, Any]) -> str:
        logger.info("Generating tests...")
        system_prompt = "Generate a Python `pytest` script that mocks the ES client and asserts this query structure. Output raw code."
        return self._call_llm(system_prompt, json.dumps(query), json_mode=False)

    # --- MAIN PROCESS ---
    def process(self,
                query_input: Union[str, Dict[str, Any]],
                mapping: Dict[str, Any],
                doc_count: int) -> Dict[str, Any]:

        start_time = time.time()
        report = {}

        # 1. SQL Transpiler (Conditional)
        try:
            dsl_query = self.transpile_sql_if_needed(query_input)
            if isinstance(query_input, str) and query_input.strip().upper().startswith("SELECT"):
                report["sql_original"] = query_input
                report["transpiled_dsl"] = dsl_query
        except ValueError as e:
            return {"error": str(e)}

        # 2. Heuristic Analysis (Local Python Logic)
        report["cost_estimate"] = self.estimate_cost(dsl_query)
        report["deprecation_warnings"] = self.check_deprecations(dsl_query)
        report["fetch_cache_advice"] = self.analyze_fetch_and_cache(dsl_query)

        # 3. Validation (LLM)
        is_valid, valid_query, val_msg = self.validate_syntax(dsl_query, mapping)
        report["is_valid"] = is_valid
        report["validation_msg"] = val_msg

        # 4. Optimization (LLM - Query + Aggs)
        query_to_opt = valid_query if not is_valid else dsl_query
        opt_res = self.optimize_query(query_to_opt, mapping, doc_count)

        report["optimized_query"] = opt_res.get("optimized_query")
        report["optimizations"] = opt_res.get("changes_made")
        report["agg_optimizations"] = opt_res.get("aggregation_improvements")

        # 5. Unit Test
        report["unit_test_code"] = self.generate_unit_test(report["optimized_query"])

        report["total_time"] = round(time.time() - start_time, 2)
        return report


# --- USAGE EXAMPLE ---
if __name__ == "__main__":
    API_KEY = "sk-..."  # REPLACE WITH YOUR KEY

    # Mock Mapping
    mapping = {
        "properties": {
            "status": {"type": "integer"},
            "category": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "created_at": {"type": "date"}
        }
    }

    # Example 1: SQL Input (will trigger Transpiler)
    sql_input = "SELECT * FROM orders WHERE status = 200 AND category = 'electronics'"

    # Example 2: Complex DSL (will trigger Cache/Cost/Deprecation checks)
    complex_dsl = {
        "query": {
            "filtered": {  # Deprecated
                "filter": {
                    "range": {"created_at": {"gte": "now"}}  # Bad for caching
                },
                "query": {"match": {"category": "electronics"}}
            }
        },
        "script_fields": {  # Expensive
            "calc_price": {"script": "doc['price'].value * 1.2"}
        }
    }

    print("\n--- Running Agent (SQL Mode) ---")
    if API_KEY == "sk-...":
        print("Please set your API Key to run.")
    else:
        agent = ElasticsearchOptimizerAgent(openai_api_key=API_KEY)

        # Test SQL Input
        result = agent.process(sql_input, mapping, 100000)

        print(json.dumps(result, indent=2))