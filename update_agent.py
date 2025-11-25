"""
Update existing agent with new ACL YAML configuration
"""
import os
import requests
import yaml

API_KEY = os.getenv("CONTEXTUALAI_API_KEY")
AGENT_ID = "dfd614e9-acd3-4f11-934f-cbd6101934a5"
YAML_FILE = "powerful_agent.yaml"

print("=" * 70)
print("UPDATING AGENT WITH NEW ACL YAML")
print("=" * 70)
print(f"Agent ID: {AGENT_ID}")
print(f"YAML File: {YAML_FILE}\n")

# Load YAML
with open(YAML_FILE, 'r') as f:
    acl_config = yaml.safe_load(f)

acl_yaml = yaml.dump(acl_config, default_flow_style=False, sort_keys=False, allow_unicode=True)

# Update agent
payload = {
    "agent_configs": {
        "acl_config": {
            "acl_active": True,
            "acl_yaml": acl_yaml
        }
    }
}

try:
    response = requests.patch(
        f"https://api.contextual.ai/v1/agents/{AGENT_ID}",
        json=payload,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
        timeout=60
    )
    
    if response.ok:
        print("✓ Agent updated successfully!")
        data = response.json()
        print(f"  Agent Name: {data.get('name')}")
        print(f"  Description: {data.get('description')}")
        print(f"  ACL Active: {data.get('agent_configs', {}).get('acl_config', {}).get('acl_active')}")
    else:
        print(f"✗ Error: {response.status_code}")
        print(f"Error details: {response.text}")
        
except Exception as e:
    print(f"✗ Exception: {e}")
    import traceback
    traceback.print_exc()

