import json
import requests
from typing import Dict, Any
from pathlib import Path

from config.config import(
    BASE_URL,
    ENDPOINTS,
    RAW_DATA_PATH,
    API_TIMEOUT
)

def fetch_api(endpoint:str)-> Any:
    #Fetch a single endpoint from pogoapi

    url = f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    response = requests.get(url, timeout=API_TIMEOUT)
    response.raise_for_status()
    return response.json()

def save_raw_json(name:str, data:any)-> None:
    #Save raw JSON response to disk

    file_path = RAW_DATA_PATH/f"{name}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def extract_all() -> Dict[str, Any]:
    #Extracts all endpoints
    #Saves raw json and returns in memory dict

    raw_data = {}

    for name, endpoint in ENDPOINTS.items():
        try:
            data = fetch_api(endpoint)
            save_raw_json(name,data)
            raw_data[name] = data

            record_count = (
                len(data)
                if isinstance(data, (list,dict))
                else 1
            )
            print(f"[RAW] Loaded {name} ({record_count} records)")
        except Exception as e:
            print(f"[RAW] Failed {name}: {e}")
    
    return raw_data

