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
endpoint_1 = "api/v1/pokemon_stats.json"
endpoint_2 = "api/v1/pokemon_max_cp.json"
url_1 = f"{BASE_URL.rstrip('/')}/{endpoint_1.lstrip('/')}"
url_2 = f"{BASE_URL.rstrip('/')}/{endpoint_2.lstrip('/')}"
response_1 = requests.get(url_1, timeout=10)
response_2 = requests.get(url_2, timeout=10)
pokemon_stats = pd.DataFrame(response_1.json())
pokemon_max_cp = pd.DataFrame(response_2.json())
final_df = pokemon_stats.merge(
    pokemon_max_cp[['pokemon_id','max_cp','form']],
    how="left",
    on=["pokemon_id","form"]
)
print(final_df)
#print(pokemon_stats[pokemon_stats["pokemon_id"] == 386])
