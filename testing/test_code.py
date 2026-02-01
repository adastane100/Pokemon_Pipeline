import requests
import json
import pandas as pd
from flatten_json import flatten
import hashlib
from datetime import datetime
from sqlalchemy import create_engine, text
import os
from urllib.parse import quote_plus
from psycopg2.extras import execute_values
from dotenv import load_dotenv

BASE_URL = "https://pogoapi.net"
endpoint_1 = "api/v1/pvp_fast_moves.json"
endpoint_2 = "api/v1/pvp_charged_moves.json"
endpoint_3 = "api/v1/type_effectiveness.json"
url_1 = f"{BASE_URL.rstrip('/')}/{endpoint_1.lstrip('/')}"
url_2 = f"{BASE_URL.rstrip('/')}/{endpoint_2.lstrip('/')}"
url_3 = f"{BASE_URL.rstrip('/')}/{endpoint_3.lstrip('/')}"
response_1 = requests.get(url_1, timeout=10)
response_2 = requests.get(url_2, timeout=10)
response_3 = requests.get(url_3, timeout=10)
pvp_fast_moves = pd.DataFrame(response_1.json())
pvp_charged_moves = pd.json_normalize(response_2.json())
type_effectiveness = pd.DataFrame(response_3.json())
print(type_effectiveness)
#print(pokemon_stats[pokemon_stats["pokemon_id"] == 386])
