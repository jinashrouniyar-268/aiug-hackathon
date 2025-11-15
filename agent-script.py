"""
Script to generate responses from an ACL agent for evaluation purposes.
Takes a dataset of queries and returns the agent's responses.
"""

import os
import json
import requests
from typing import List, Dict, Optional
from pathlib import Path


def query_agent_simple(
    api_key: str,
    agent_id: str,
    query: str,
    conversation_id: Optional[str] = None,
    base_url: str = "https://api.contextual.ai/v1"
) -> Dict:
    """
    Query an ACL agent and return the final response (non-streaming).
    
    Args:
        api_key: Your Contextual AI API key
        agent_id: ID of the agent to query
        query: The question or query to ask the agent
        conversation_id: Optional conversation ID to maintain context
        base_url: API base URL
    
    Returns:
        Dictionary with response, conversation_id, and metadata
    """
    payload = {
        "messages": [{"role": "user", "content": query}],
        "stream": False  # Non-streaming for evaluation
    }
    if conversation_id:
        payload["conversation_id"] = conversation_id

    response = requests.post(
        f"{base_url}/agents/{agent_id}/query/acl",
        json=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        timeout=300,  # 5 minute timeout for long queries
    )
    response.raise_for_status()
    data = response.json()
    
    return {
        "query": query,
        "response": data.get("outputs", {}).get("response", ""),
        "conversation_id": data.get("conversation_id"),
        "message_id": data.get("message_id"),
        "workflow_trace": data.get("workflow_trace", []),
        "dynamic_agent_trace": data.get("dynamic_agent_trace", {}),
        "all_outputs": data.get("outputs", {}),
    }


def generate_responses(
    api_key: str,
    agent_id: str,
    queries: List[str],
    conversation_id: Optional[str] = None,
    output_file: Optional[str] = None,
    verbose: bool = True
) -> List[Dict]:
    """
    Generate responses for a list of queries.
    
    Args:
        api_key: Your Contextual AI API key
        agent_id: ID of the agent to query
        queries: List of query strings
        conversation_id: Optional conversation ID to maintain context across queries
        output_file: Optional path to save results as JSON
        verbose: Whether to print progress
    
    Returns:
        List of dictionaries, each containing query, response, and metadata
    """
    results = []
    current_conversation_id = conversation_id
    
    for i, query in enumerate(queries, 1):
        if verbose:
            print(f"[{i}/{len(queries)}] Processing: {query[:80]}...")
        
        try:
            result = query_agent_simple(
                api_key=api_key,
                agent_id=agent_id,
                query=query,
                conversation_id=current_conversation_id
            )
            results.append(result)
            current_conversation_id = result.get("conversation_id")
            
            if verbose:
                response_preview = result["response"][:100] + "..." if len(result["response"]) > 100 else result["response"]
                print(f"  ✓ Response: {response_preview}\n")
        
        except Exception as e:
            error_result = {
                "query": query,
                "response": None,
                "error": str(e),
                "conversation_id": current_conversation_id,
            }
            results.append(error_result)
            if verbose:
                print(f"  ✗ Error: {str(e)}\n")
    
    # Save to file if specified
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        if verbose:
            print(f"\n✓ Results saved to: {output_file}")
    
    return results


def load_queries_from_file(file_path: str) -> List[str]:
    """
    Load queries from a JSON file or text file.
    
    Supports:
    - JSON file with list of strings: ["query1", "query2", ...]
    - JSON file with list of dicts: [{"query": "..."}, ...]
    - Text file with one query per line
    
    Args:
        file_path: Path to the queries file
    
    Returns:
        List of query strings
    """
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Queries file not found: {file_path}")
    
    if path.suffix == '.json':
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle list of strings
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], str):
                return data
            # Handle list of dicts with "query" key
            elif isinstance(data[0], dict):
                return [item.get("query", "") for item in data if item.get("query")]
        
        raise ValueError(f"Unsupported JSON format in {file_path}")
    
    else:
        # Assume text file with one query per line
        with open(path, 'r', encoding='utf-8') as f:
            queries = [line.strip() for line in f if line.strip()]
        return queries


if __name__ == "__main__":
    # Configuration
    API_KEY = os.getenv("CONTEXTUALAI_API_KEY")
    if not API_KEY:
        raise ValueError("CONTEXTUALAI_API_KEY environment variable is not set")
    
    AGENT_ID = "dfd614e9-acd3-4f11-934f-cbd6101934a5"
    
    # Example: Define queries directly or load from file
    queries = [
        "What is Tesla's revenue?",
        "Compare Tesla's revenue across different sections",
        "Section 3.2 revenue figures",
    ]
    
    # Alternative: Load queries from file
    # queries = load_queries_from_file("queries.json")
    # or
    # queries = load_queries_from_file("queries.txt")
    
    print("=" * 70)
    print("GENERATING RESPONSES FOR EVALUATION")
    print("=" * 70)
    print(f"Agent ID: {AGENT_ID}")
    print(f"Number of queries: {len(queries)}\n")
    
    # Generate responses
    results = generate_responses(
        api_key=API_KEY,
        agent_id=AGENT_ID,
        queries=queries,
        output_file="agent_responses.json",  # Save results to file
        verbose=True
    )
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    successful = sum(1 for r in results if r.get("response") is not None)
    failed = len(results) - successful
    print(f"Total queries: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    # Print results in a simple format for evaluation
    print("\n" + "=" * 70)
    print("RESULTS (for evaluation)")
    print("=" * 70)
    for i, result in enumerate(results, 1):
        print(f"\n[{i}] Query: {result['query']}")
        if result.get("error"):
            print(f"    Error: {result['error']}")
        else:
            print(f"    Response: {result['response'][:200]}..." if len(result.get('response', '')) > 200 else f"    Response: {result.get('response', '')}")

