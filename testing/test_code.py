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
#type_effectiveness = type_effectiveness.reset_index()

#print(type_effectiveness)
dark_df = type_effectiveness.iloc[1].reset_index().rename({"index":"type",1:"effectiveness"},axis=1)
dark_df["base_type"] = "Dark"
#print(type_effectiveness)
#print(pokemon_stats[pokemon_stats["pokemon_id"] == 386])
#print(dark_df)

damage_map = {
    1.6: "super effective",
    1.0: "neutral",
    0.625: "not very effective",
    0.39: "resistant"
}

type_effectiveness = type_effectiveness.reset_index().melt(
    id_vars="index",
    var_name="defending_type",
    value_name="effectiveness"
).rename(columns={"index": "attacking_type"})
type_effectiveness["damage_effectiveness"] = type_effectiveness["effectiveness"].map(damage_map)

print(type_effectiveness)
'''
for i in range(0, len(type_effectiveness)):
    df = type_effectiveness.iloc[i].reset_index().rename({"index":"attacking_type",i:"effectiveness"},axis=1)
    base_df_type = df.iloc[0]["effectiveness"]
    df = df[1:]
    df["defending_type"] = base_df_type
    df["damage_effectiveness"] = df["effectiveness"].map(damage_map)
    #print(base_df_type)
    print(df)'''