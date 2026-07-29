import os
import json
import logging
from typing import Dict, Any, Optional, List
from openai import OpenAI
from elasticsearch import Elasticsearch, exceptions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ElasticsearchAgent:
    def __init__(self, openai_api_key: str, es_client: Optional[Elasticsearch] = None, model: str = "gpt-4o"):
        """
        Initialize the agent.

        Args:
            openai_api_key: Your OpenAI API key.
            es_client: An initialized Elasticsearch client. If None, validation is skipped (dry-run mode).
            model: The LLM model to use (default: gpt-4o).
        """
        self.client = OpenAI(api_key=openai_api_key)
        self.es = es_client
        self.model = model

        # System prompt designed for an Expert Elasticsearch Engineer persona
        self.system_prompt = """
You are an elite Elasticsearch Expert and Data Engineer. 
Your goal is to translate natural language questions into highly optimized, syntactically correct Elasticsearch DSL queries.

Rules & Best Practices:
1. **Filter Context**: ALWAYS use `bool` -> `filter` for exact matches, numbers, dates, and boolean logic. Only use `must` or `should` for full-text search (relevance scoring).
2. **Keyword vs Text**: 
   - Use `.keyword` sub-fields for aggregations, sorting, and exact term matching on text fields.
   - Use standard fields for `match` queries.
3. **Optimized Aggregations**: Use `date_histogram` for time, `terms` for categories. prevent returning too many buckets (use `size`).
4. **Dates**: Use standard ISO 8601 strings or math (e.g., "now-1d/d").
5. **Efficiency**: Do not retrieve large fields (`_source` filtering) if the user only asks for specific counts or aggregations.
6. **Nested Fields**: Detect `type: nested` in the mapping and use `nested` queries/aggregations accordingly.

Output Format:
You must return a JSON object with strictly two keys:
1. "reasoning": A brief explanation of your logic (e.g., "Using filter for status because exact match is needed...").
2. "query": The valid Elasticsearch DSL dictionary (starting with "query", "aggs", "size", etc.).
"""

    def _optimize_mapping(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """
        Strips unnecessary metadata from the mapping to save tokens,
        keeping only field names and types.
        """
        if not mapping:
            return {}

        # Handle cases where user passes full index state or just properties
        properties = mapping
        if "mappings" in mapping:
            properties = mapping["mappings"].get("properties", {})
        elif "properties" in mapping:
            properties = mapping["properties"]
        # If it's the root index response {index_name: {mappings: ...}}
        elif isinstance(mapping, dict) and len(mapping) == 1:
            first_key = next(iter(mapping))
            if "mappings" in mapping[first_key]:
                properties = mapping[first_key]["mappings"].get("properties", {})

        def clean_props(props):
            clean = {}
            for field, details in props.items():
                if "type" in details:
                    clean[field] = {"type": details["type"]}
                    # Keep keyword subfields as they are critical for queries
                    if "fields" in details and "keyword" in details["fields"]:
                        clean[field]["fields"] = {"keyword": {"type": "keyword"}}
                elif "properties" in details:  # Nested object
                    clean[field] = {"properties": clean_props(details["properties"])}
            return clean

        return clean_props(properties)

    def validate_query(self, index_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates the generated query against the real Elasticsearch instance using the _validate API.
        Returns {"valid": True} or {"valid": False, "error": "..."}
        """
        if not self.es:
            logger.warning("No Elasticsearch client provided. Skipping validation.")
            return {"valid": True}

        try:
            # We use explain=True to get detailed error reasons
            response = self.es.indices.validate_query(index=index_name, body=query, explain=True)

            if response.get("valid"):
                return {"valid": True}
            else:
                # Extract explanations
                explanations = response.get("explanations", [])
                error_msg = "; ".join([exp.get("error", "Unknown error") for exp in explanations])
                return {"valid": False, "error": error_msg}

        except exceptions.NotFoundError:
            return {"valid": False, "error": f"Index '{index_name}' not found."}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    def generate(self, user_question: str, index_name: str, mapping: Dict[str, Any], max_retries: int = 3) -> Dict[
        str, Any]:
        """
        Generates an Elasticsearch query from natural language.
        Includes a self-correction loop if the query fails validation.
        """
        optimized_mapping = self._optimize_mapping(mapping)

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user",
             "content": f"Index Name: {index_name}\n\nMapping Schema:\n{json.dumps(optimized_mapping, indent=2)}\n\nUser Question: {user_question}"}
        ]

        attempt = 0
        while attempt < max_retries:
            logger.info(f"Generating query (Attempt {attempt + 1}/{max_retries})...")

            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    response_format={"type": "json_object"},
                    temperature=0.2  # Low temperature for deterministic code
                )

                content = response.choices[0].message.content
                result = json.loads(content)

                reasoning = result.get("reasoning", "No reasoning provided")
                es_query = result.get("query")

                logger.info(f"Agent Reasoning: {reasoning}")

                # VALIDATION STEP
                validation = self.validate_query(index_name, es_query)

                if validation["valid"]:
                    logger.info("Query validated successfully.")
                    return es_query
                else:
                    error_msg = validation["error"]
                    logger.warning(f"Validation failed: {error_msg}")

                    # Feed the error back to the LLM for correction
                    messages.append({"role": "assistant", "content": content})
                    messages.append({
                        "role": "user",
                        "content": f"The query you generated resulted in an Elasticsearch error: '{error_msg}'. \n\nPlease fix the query based on this error and the mapping provided. Ensure correct types (e.g. don't sort by text fields, use .keyword)."
                    })
                    attempt += 1

            except json.JSONDecodeError:
                logger.error("LLM failed to produce valid JSON.")
                attempt += 1
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break

        raise Exception("Failed to generate a valid Elasticsearch query after multiple attempts.")


# ==========================================
# Example Usage
# ==========================================
if __name__ == "__main__":
    # 1. Setup (Replace with your actual keys/endpoints)
    # NOTE: Set 'OPENAI_API_KEY' in your environment variables

    API_KEY = os.getenv("OPENAI_API_KEY", "")


    # Mock Elasticsearch Client for demonstration (Since we don't have a live cluster here)
    # In production, use: es = Elasticsearch("http://localhost:9200", basic_auth=("user", "pass"))
    class MockElasticsearch:
        class indices:
            @staticmethod
            def validate_query(index, body, explain):
                # Simulate a validation error for demonstration purposes if query uses 'description' for sorting
                sort_keys = body.get("sort", [])
                if sort_keys and isinstance(sort_keys, list):
                    for s in sort_keys:
                        if "description" in s:
                            return {
                                "valid": False,
                                "explanations": [{
                                                     "error": "Fielddata is disabled on text fields in [description]. Use [description.keyword] instead."}]
                            }
                return {"valid": True}


    # 2. Define a Sample Mapping (e.g., an E-commerce Product Index)
    sample_mapping = {
        "properties": {
            "product_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "description": {"type": "text"},
            "price": {"type": "float"},
            "category": {"type": "keyword"},
            "created_at": {"type": "date"},
            "stock_count": {"type": "integer"},
            "tags": {"type": "keyword"}
        }
    }

    # 3. Initialize Agent
    agent = ElasticsearchAgent(openai_api_key=API_KEY, es_client=MockElasticsearch())

    # 4. Run a Query
    # This specific query is tricky: it asks for full text search AND sorting by a text field (which usually fails without .keyword)
    user_q = "Find all cheap electronics under $50, sort them by product description, and group them by tag."

    print(f"User Question: {user_q}\n")

    try:
        query = agent.generate(
            user_question=user_q,
            index_name="products",
            mapping=sample_mapping
        )
        print("Final Generated Query:")
        print(json.dumps(query, indent=2))

    except Exception as e:
        print(f"Error: {e}")