#!/usr/bin/env python3
"""
Script to query ACL agent for specific questions and update answers.csv.
Takes question numbers as input (e.g., 1,2,3 or 1 or 1-10) and processes them.
"""

import os
import json
import csv
import re
import sys
import requests
from typing import List, Dict, Optional, Set
from pathlib import Path


def parse_question_numbers(input_str: str) -> Set[int]:
    """
    Parse question number input in formats: 1,2,3 or 1 or 1-10
    
    Args:
        input_str: User input string
        
    Returns:
        Set of question numbers (1-indexed)
    """
    numbers = set()
    parts = [p.strip() for p in input_str.split(',')]
    
    for part in parts:
        if '-' in part:
            # Range format: 1-10
            try:
                start, end = map(int, part.split('-'))
                numbers.update(range(start, end + 1))
            except ValueError:
                print(f"Warning: Invalid range format '{part}', skipping")
        else:
            # Single number
            try:
                numbers.add(int(part))
            except ValueError:
                print(f"Warning: Invalid number '{part}', skipping")
    
    return numbers


def load_questions(questions_file: str) -> List[Dict]:
    """
    Load questions from a JSON file with question number, question text, and expected schema.
    
    Args:
        questions_file: Path to questions.json file
        
    Returns:
        List of question dictionaries, each with:
        - question_number: int
        - question: str
        - expected_schema: dict
    """
    path = Path(questions_file)
    
    if not path.exists():
        raise FileNotFoundError(f"Questions file not found: {questions_file}")
    
    with open(path, 'r', encoding='utf-8') as f:
        questions_data = json.load(f)
    
    # Validate format
    if not isinstance(questions_data, list):
        raise ValueError(f"Expected list of questions in {questions_file}")
    
    # Sort by question_number to ensure correct order
    questions_data.sort(key=lambda x: x.get('question_number', 0))
    
    return questions_data


def query_agent_streaming(
    api_key: str,
    agent_id: str,
    query: str,
    conversation_id: Optional[str] = None,
    base_url: str = "https://api.contextual.ai/v1"
) -> Dict:
    """
    Query an ACL agent with streaming and return the final response.
    Based on acl-agent.py query_agent function.
    
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
        "stream": True
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
        timeout=600,  # 10 minute timeout
        stream=True,
    )
    response.raise_for_status()
    
    full_content = ""
    conversation_id = None
    message_id = None
    
    try:
        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue
            
            # Skip ping and comment lines
            if line.startswith(": ping") or line.startswith(":"):
                continue
            
            # Process SSE data lines
            if line.startswith("data: "):
                data_payload = line[6:]
                
                try:
                    evt = json.loads(data_payload)
                except json.JSONDecodeError:
                    continue
                
                # Handle nested event structure
                if "version" in evt and "event" in evt:
                    inner_event = evt.get("event", {})
                    event_type = inner_event.get("type", "")
                    
                    if event_type == "dynamic_response_start":
                        print("\n📝 Generating response...", flush=True)
                
                # Handle top-level event structure
                event_type = evt.get("event")
                event_data_content = evt.get("data", {})
                
                if event_type == "metadata":
                    if event_data_content.get("conversation_id"):
                        conversation_id = event_data_content["conversation_id"]
                    if event_data_content.get("message_id"):
                        message_id = event_data_content["message_id"]
                
                elif event_type == "message_delta":
                    delta = event_data_content.get("delta", "")
                    if delta:
                        try:
                            delta = json.loads(f'"{delta}"')  # Unescape JSON strings
                        except:
                            pass
                        print(delta, end="", flush=True)
                        full_content += delta
                
                elif event_type == "message_complete":
                    final_message = event_data_content.get("final_message", "")
                    if final_message:
                        remaining = final_message[len(full_content):]
                        if remaining:
                            print(remaining, end="", flush=True)
                            full_content = final_message
                
                elif event_type == "outputs":
                    if event_data_content.get("response"):
                        response_output = event_data_content["response"]
                        if response_output != full_content:
                            remaining = response_output[len(full_content):]
                            if remaining:
                                print(remaining, end="", flush=True)
                                full_content = response_output
                
                elif event_type == "end":
                    break
                
                elif event_type == "error":
                    error_msg = event_data_content.get("message", "Unknown error")
                    print(f"\n\n⚠️  Error: {error_msg}", file=sys.stderr)
                    break
        
        print("\n")  # New line after streaming
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Stream interrupted by user")
    except Exception as e:
        print(f"\n[ERROR] Stream error: {e}", file=sys.stderr)
    
    return {
        "response": full_content,
        "agent_id": agent_id,
        "query": query,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "streamed": True,
    }


def extract_json_from_response(response_text: str) -> Optional[Dict]:
    """
    Extract JSON object from agent response text.
    Looks for JSON blocks in the response.
    
    Args:
        response_text: The agent's response text
        
    Returns:
        Extracted JSON dictionary or None
    """
    # Try to find JSON blocks (```json ... ``` or ``` ... ```)
    json_patterns = [
        r'```json\s*(\{.*?\})\s*```',  # ```json { ... } ```
        r'```\s*(\{.*?\})\s*```',      # ``` { ... } ```
        r'(\{.*\})',                    # Any JSON object
    ]
    
    for pattern in json_patterns:
        matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
        for match in matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
    
    # If no JSON block found, try to parse the entire response as JSON
    try:
        return json.loads(response_text.strip())
    except json.JSONDecodeError:
        pass
    
    return None


def extract_answers_from_json(json_data: Dict, expected_schema: Dict[str, str] = None, max_cols: int = 5) -> List[str]:
    """
    Extract answer values from JSON data, ordered by expected_schema if provided.
    Returns list of up to max_cols values.
    
    Args:
        json_data: JSON dictionary with answer fields
        expected_schema: Dictionary mapping field names to types (for ordering)
        max_cols: Maximum number of columns (default: 5)
        
    Returns:
        List of answer values (strings), padded to max_cols
    """
    answers = []
    
    # Skip metadata keys
    skip_keys = {'question', 'difficulty', 'id', 'question_number'}
    
    # If expected_schema is provided, use its order
    if expected_schema:
        # Use the order from expected_schema
        for key in expected_schema.keys():
            if len(answers) >= max_cols:
                break
            
            value = json_data.get(key)
            
            # Convert to string, handling special cases
            if value is None:
                answer_str = ""
            elif isinstance(value, (list, dict)):
                answer_str = json.dumps(value)
            else:
                answer_str = str(value)
            
            answers.append(answer_str)
    else:
        # Fallback: sort keys for consistent ordering
        sorted_keys = sorted(json_data.keys())
        
        for key in sorted_keys:
            if key in skip_keys:
                continue
            
            if len(answers) >= max_cols:
                break
            
            value = json_data[key]
            
            # Convert to string, handling special cases
            if value is None:
                answer_str = ""
            elif isinstance(value, (list, dict)):
                answer_str = json.dumps(value)
            else:
                answer_str = str(value)
            
            answers.append(answer_str)
    
    # Pad to max_cols
    while len(answers) < max_cols:
        answers.append("")
    
    return answers[:max_cols]


def load_answers_csv(csv_file: str) -> List[List[str]]:
    """
    Load existing answers.csv file.
    
    Args:
        csv_file: Path to answers.csv
        
    Returns:
        List of rows (each row is a list of strings)
    """
    path = Path(csv_file)
    
    if not path.exists():
        # Create new file with headers
        rows = [["row_index", "col_1", "col_2", "col_3", "col_4", "col_5"]]
        # Add empty rows for questions (assuming 101 questions based on answers.csv)
        for i in range(1, 102):
            rows.append([str(i)] + [""] * 5)
        return rows
    
    rows = []
    with open(path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)
    
    return rows


def save_answers_csv(csv_file: str, rows: List[List[str]]):
    """
    Save answers to CSV file.
    
    Args:
        csv_file: Path to answers.csv
        rows: List of rows (each row is a list of strings)
    """
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def update_answer_row(rows: List[List[str]], question_num: int, answers: List[str]):
    """
    Update a specific row in the answers CSV.
    Question 1 goes to row index 1 (row 2 in file, after header).
    
    Args:
        rows: List of CSV rows
        question_num: Question number (1-indexed)
        answers: List of answer values (up to 5)
    """
    # Question 1 -> row index 1 (after header at index 0)
    row_idx = question_num
    
    # Ensure we have enough rows
    while len(rows) <= row_idx:
        # Add empty rows
        new_row_num = len(rows)
        rows.append([str(new_row_num)] + [""] * 5)
    
    # Update the row: [row_index, col_1, col_2, col_3, col_4, col_5]
    rows[row_idx] = [str(question_num)] + answers


def main():
    """Main execution function."""
    # Configuration
    API_KEY = os.getenv("CONTEXTUALAI_API_KEY")
    if not API_KEY:
        raise ValueError("CONTEXTUALAI_API_KEY environment variable is not set")
    
    AGENT_ID = "3e65834f-d11b-4f21-9e5b-ac620209f647"
    QUESTIONS_FILE = "questions.json"
    ANSWERS_CSV = "answers1.csv"
    
    # Get question numbers from user
    print("=" * 70)
    print("ACL AGENT QUESTION PROCESSOR")
    print("=" * 70)
    print(f"Agent ID: {AGENT_ID}")
    print(f"Questions file: {QUESTIONS_FILE} (JSON format)")
    print(f"Answers CSV: {ANSWERS_CSV}")
    print()
    
    user_input = input("Enter question number(s) (e.g., 1,2,3 or 1 or 1-10): ").strip()
    
    if not user_input:
        print("No input provided. Exiting.")
        return
    
    # Parse question numbers
    question_numbers = parse_question_numbers(user_input)
    
    if not question_numbers:
        print("No valid question numbers found. Exiting.")
        return
    
    print(f"\nProcessing questions: {sorted(question_numbers)}")
    print()
    
    # Load questions
    try:
        questions_data = load_questions(QUESTIONS_FILE)
        print(f"Loaded {len(questions_data)} questions from {QUESTIONS_FILE}")
    except FileNotFoundError:
        print(f"Error: {QUESTIONS_FILE} not found.")
        print("Please create a questions.json file with question data.")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {QUESTIONS_FILE}: {e}")
        return
    
    # Load existing answers CSV
    rows = load_answers_csv(ANSWERS_CSV)
    print(f"Loaded {len(rows)} rows from {ANSWERS_CSV}")
    print()
    
    # Process each question
    conversation_id = None
    successful = 0
    failed = 0
    
    # Create a mapping from question_number to question data
    questions_map = {q['question_number']: q for q in questions_data}
    max_question_num = max(questions_map.keys()) if questions_map else 0
    
    for q_num in sorted(question_numbers):
        if q_num < 1 or q_num > max_question_num:
            print(f"⚠️  Question {q_num} is out of range (1-{max_question_num}). Skipping.")
            continue
        
        if q_num not in questions_map:
            print(f"⚠️  Question {q_num} not found in questions file. Skipping.")
            continue
        
        question_data = questions_map[q_num]
        question_text = question_data['question']
        expected_schema = question_data.get('expected_schema', {})
        
        # Format the query with question and expected schema
        formatted_query = f"question: {question_text}\n\nexpected output format\n\n{json.dumps(expected_schema, indent=2)}"
        
        print("=" * 70)
        print(f"Question {q_num}")
        print("=" * 70)
        print(f"Question: {question_text[:100]}..." if len(question_text) > 100 else f"Question: {question_text}")
        if expected_schema:
            print(f"Expected schema: {expected_schema}")
        print()
        
        try:
            # Query agent with formatted query including schema
            result = query_agent_streaming(
                api_key=API_KEY,
                agent_id=AGENT_ID,
                query=formatted_query,
                conversation_id=conversation_id
            )
            
            conversation_id = result.get("conversation_id")
            response_text = result.get("response", "")
            
            print()
            print("-" * 70)
            print("Extracting answers from response...")
            
            # Extract JSON from response
            json_data = extract_json_from_response(response_text)
            
            if json_data:
                print(f"✓ Extracted JSON: {json.dumps(json_data, indent=2)}")
                
                # Extract answers using expected_schema for ordering
                answers = extract_answers_from_json(json_data, expected_schema=expected_schema)
                
                # Update CSV row
                update_answer_row(rows, q_num, answers)
                
                print(f"✓ Updated row {q_num} with answers: {answers}")
                successful += 1
            else:
                print("⚠️  Could not extract JSON from response.")
                print(f"Response preview: {response_text[:200]}...")
                failed += 1
            
            print()
        
        except Exception as e:
            print(f"✗ Error processing question {q_num}: {str(e)}")
            failed += 1
            print()
    
    # Save updated CSV
    save_answers_csv(ANSWERS_CSV, rows)
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total questions processed: {len(question_numbers)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"✓ Answers saved to {ANSWERS_CSV}")
    print("=" * 70)


if __name__ == "__main__":
    main()

