#Import libraries
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

#Configurations
BASE_URL = "https://pogoapi.net"
RAW_PATH = "./raw_data"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT= os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

ENDPOINTS = {
    "apis": "api/v1/api_hashes.json",
    "pokemons": "api/v1/released_pokemon.json",
    "alolan_pokemons": "api/v1/alolan_pokemon.json",
    "pokemon_stats": "api/v1/pokemon_stats.json",
    "pokemon_powerup_requirements": "api/v1/pokemon_powerup_requirements.json",
    "nesting_pokemons": "api/v1/nesting_pokemon.json",
    "shiny_pokemons": "api/v1/shiny_pokemon.json",
    "fast_moves": "api/v1/fast_moves.json",
    "charged_moves": "api/v1/charged_moves.json",
    "pokemon_max_cp": "api/v1/pokemon_max_cp.json",
    "pokemon_type": "api/v1/pokemon_types.json",
    "weather_boost": "api/v1/weather_boosts.json",
    "type_effectiveness": "api/v1/type_effectiveness.json",
    "pvp_exclusive_pokemon": "api/v1/pvp_exclusive_pokemon.json",
    "galarian_pokemon": "api/v1/galarian_pokemon.json",
    "cp_multiplier": "api/v1/cp_multiplier.json",
    "research_task_exclusive_pokemon": "api/v1/research_task_exclusive_pokemon.json",
    "baby_pokemons": "api/v1/baby_pokemon.json",
    "pvp_fast_moves": "api/v1/pvp_fast_moves.json",
    "pvp_charged_moves": "api/v1/pvp_charged_moves.json",
    # Endpoints for separate handling
    "buddy_distances": "api/v1/pokemon_buddy_distances.json",
    "candies_to_evolve": "api/v1/pokemon_candy_to_evolve.json",
    "pokemon_rarity": "api/v1/pokemon_rarity.json",
    "pokemon_generations": "api/v1/pokemon_generations.json",
    "pokemon_forms": "api/v1/pokemon_forms.json",
    "pokemon_evolutions": "api/v1/pokemon_evolutions.json",
    "mega_pokemon": "api/v1/mega_pokemon.json",
}

os.makedirs(RAW_PATH, exist_ok=True)

#Utility functions

def fetch_api(endpoint: str) -> dict:
    url = f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def save_raw_json(name: str, data: dict):
    filename = os.path.join(RAW_PATH, f"{name}.json")
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

def md5_hash(*args)-> str:
    return hashlib.md5("|".join(map(str, args)).encode()).hexdigest()

def flatten_single_list_dict(data: dict, prefix_map=None) -> dict:
    row = {}
    prefix_map = prefix_map or {}
    for key, value in data.items():
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
            prefix = prefix_map.get(key, f"{key}_")
            for k, v in value[0].items():
                row[f"{prefix}{k}"] = v
        else:
            row[key] = value
    return row

def flag_column(df, col_name, value="Yes"):
    df[col_name] = value
    return df

# Raw layer - Fetch and save

raw_data = {}
for name, endpoint in ENDPOINTS.items():
    try:
        data = fetch_api(endpoint)
        save_raw_json(name, data)
        raw_data[name]=data
        print(f"[RAW] Loaded {name} ({len(data) if isinstance(data,list) else 1} records)")
    except Exception as e:
            print(f"[RAW] Failed {name}: {e}")

# Core layer - Transformations

#API Info
#APIs
apis_df = (
    pd.DataFrame.from_dict(raw_data['apis'],orient="index")
    #.rename(columns={"id":"pokemon_id"})
    .reset_index(drop=True)
    )

#Pokemon Attributes APIs

#Pokemons
pokemon_df = (
    pd.DataFrame.from_dict(raw_data['pokemons'],orient="index")
    .rename(columns={"id":"pokemon_id"})
    .reset_index(drop=True)
    )

#Alolan Pokemons
alolan_df = (
        pd.DataFrame.from_dict(raw_data['alolan_pokemons'], orient="index")
        .rename(columns={"id":"pokemon_id"})
        .reset_index(drop=True)
        )
alolan_df["alolan_form"] = "Yes"

#Pokemon Stats
pokemon_stats_df = pd.DataFrame(raw_data["pokemon_stats"])
pokemon_stats_core = (
    pokemon_stats_df
    .drop(columns=["form"], errors="ignore")
    .drop_duplicates(subset=["pokemon_id","base_attack","base_defense","base_stamina"])
    .reset_index(drop=True)
)
pokemon_stats_core["pokemon_stat_profile_id"] = pokemon_stats_core.apply(
    lambda x : md5_hash(x["pokemon_id"], x["base_attack"], x["base_defense"], x["base_stamina"]), axis=1
)

#Pokemon Max CP
pokemon_max_cp = pd.DataFrame(raw_data["pokemon_max_cp"])
pokemon_max_cp_core = (
    pokemon_max_cp.drop(columns=["form"], errors="ignore")
    .drop_duplicates(subset=["pokemon_id","max_cp"])
    .reset_index(drop=True)
)
pokemon_max_cp_core["pokemon_max_cp_profile_id"] = pokemon_max_cp_core.apply(
    lambda x : md5_hash(x["pokemon_id"], x["max_cp"]),axis=1
)

#Pokemon types
pokemon_types_df = pd.DataFrame(raw_data["pokemon_type"])
pokemon_type_core = (
    pokemon_types_df.explode("type")
    .drop(columns=["form"], errors="ignore")
    .drop_duplicates(subset=["pokemon_id","type"])
    .groupby(["pokemon_id","pokemon_name"])["type"].agg(list)
    .reset_index() 
)
pokemon_type_core["type_1"] = pokemon_type_core["type"].str[0]
pokemon_type_core["type_2"] = pokemon_type_core["type"].str[1]
pokemon_type_core = pokemon_type_core.drop(columns=["type"])

#Pokemon Evolutions
pokemon_evolutions_core = pd.DataFrame([
    flatten_single_list_dict(p, prefix_map={"evolutions": "evo_"})
    for p in raw_data.get("pokemon_evolutions", [])
])

EXPECTED_COLS = [
    "pokemon_id",
    "pokemon_name",
    "form",
    "evo_pokemon_id",
    "evo_pokemon_name",
    "evo_form",
    "evo_candy_required",
    "evo_item_required",
    "evo_lure_required",
    "evo_gender_required",
    "evo_only_evolves_in_daytime",
    "evo_only_evolves_in_nighttime",
    "evo_buddy_distance_required",
    "evo_no_candy_cost_if_traded",
    "evo_upside_down"
]
pokemon_evolutions_core = pokemon_evolutions_core.reindex(columns=EXPECTED_COLS)
pokemon_evolutions_core = pokemon_evolutions_core.where(
    pd.notnull(pokemon_evolutions_core), None
)
def safe_str(x):
    return "" if x is None else str(x).lower().strip()

pokemon_evolutions_core["evolution_id"] = pokemon_evolutions_core.apply(
    lambda x: md5_hash(
        safe_str(x["pokemon_id"]),
        safe_str(x["evo_pokemon_name"]),
        safe_str(x["evo_form"]),
        safe_str(x["evo_item_required"]),
        safe_str(x["evo_lure_required"]),
        safe_str(x["evo_only_evolves_in_daytime"]),
        safe_str(x["evo_only_evolves_in_nighttime"])
    ),
    axis=1
)
pokemon_evolutions_core = pokemon_evolutions_core.drop_duplicates(
    subset=["evolution_id"]
)

#Buddy Distances
buddy_df = []
for dist, entries in raw_data["buddy_distances"].items():
    df = pd.DataFrame(entries)
    df["buddy_distance"] = dist
    buddy_df.append(df)
buddy_core = pd.concat(buddy_df, ignore_index=True).groupby(["pokemon_id","pokemon_name","buddy_distance"])['form'].agg(list).reset_index()

#Candies to Evolve
candies_df = []
for candy, entries in raw_data["candies_to_evolve"].items():
    df = pd.DataFrame(entries)
    df["candy_required"] = int(candy)
    candies_df.append(df)
candies_core = pd.concat(candies_df, ignore_index=True).drop_duplicates(subset=["pokemon_id","candy_required"])
candies_core["candy_form_id"] = candies_core.apply(lambda x: md5_hash(x["pokemon_id"], x["form"], x["candy_required"]), axis=1)

#Rarity
rarity_df = []
for rarity_type, entries in raw_data["pokemon_rarity"].items():
    df = pd.DataFrame(entries)
    df["rarity_category"] = rarity_type
    rarity_df.append(df)
rarity_core = pd.concat(rarity_df, ignore_index=True).drop_duplicates(subset=["pokemon_id","rarity_category"])

#Generations
gen_df = []
for gen_name, entries in raw_data["pokemon_generations"].items():
    df = pd.DataFrame(entries)
    df["generation"] = gen_name
    gen_df.append(df)
generations_core = pd.concat(gen_df, ignore_index=True).rename(columns={"id":"pokemon_id","name":"pokemon_name"})

#Mega Pokemon
mega_df = [flatten(p) for p in raw_data.get("mega_pokemon",[])]
mega_core = pd.DataFrame(mega_df)
mega_core["mega_form"] = "Yes"
mega_core["mega_form_id"] = mega_core.apply(
    lambda x: md5_hash(x["pokemon_id"], x["form"],x["stats_base_attack"],x["stats_base_defense"],x["stats_base_stamina"]),
    axis=1
)

#Shiny Pokemon
shiny_core = (
    pd.DataFrame.from_dict(raw_data["shiny_pokemons"],orient="index")
    .rename(columns={"id":"pokemon_id"})
    .pipe(flag_column, "shiny_pokemon")
    .reset_index(drop=True)
    )

#Baby Pokemon
baby_core = flag_column(
    pd.DataFrame(raw_data.get("baby_pokemons",[]))
    .rename(columns={"id":"pokemon_id"}),
    "baby_pokemon"
    )

#Nesting pokemons
nest_core = (
    pd.DataFrame.from_dict(raw_data["nesting_pokemons"],orient="index")
    .rename(columns={"id":"pokemon_id"})
    .pipe(flag_column,"nest_status")
    .reset_index(drop=True)
    )

#Galarian Pokemons
galarian_core = (
    pd.DataFrame.from_dict(raw_data["galarian_pokemon"],orient="index")
    .rename(columns={"id":"pokemon_id"})
    .pipe(flag_column,"galarian_form")
    .reset_index(drop=True)
    )

#Research Task
research_task_core = flag_column(
    pd.DataFrame(
        raw_data.get("research_task_exclusive_pokemon",[])
        )
        .rename(columns={"id":"pokemon_id"}),
        "research_task_exclusive_pokemon"
    )

#Pvp exclusive pokemons
pvp_exclusive_core = flag_column(
    pd.DataFrame(raw_data.get("pvp_exclusive_pokemon",[]))
    .rename(columns={"id":"pokemon_id"}),
    "pvp_exclusive_status"
    )

#Pokemon Power Up Requirements
pokemon_power_up_requirements_df = (
    pd.DataFrame.from_dict(raw_data["pokemon_powerup_requirements"],orient="index")
    #.rename(columns={"id":"pokemon_id"})
    .reset_index(drop=True)
)
pokemon_power_up_requirements_df["pokemon_powerup_requirements_id"] = pokemon_power_up_requirements_df.apply(
    lambda x : md5_hash(x["candy_to_upgrade"], x["current_level"]),axis=1
)

#Pokemon Forms
pokemon_forms_df = (
    pd.DataFrame(
        {"form_name":raw_data.get("pokemon_forms")},
        index=pd.Index(range(len(raw_data.get("pokemon_forms"))), name="form_id")
        )
)

#Battle Attributes APIs
#Fast Moves
fast_moves_df = (
    pd.DataFrame(raw_data['fast_moves'])
    .reset_index(drop=True)
)

#Charged Moves
charged_moves_df = (
    pd.DataFrame(raw_data['charged_moves'])
    .reset_index(drop=True)
)

#Weather Boost
weather_boost_df = pd.DataFrame.from_dict(raw_data['weather_boost'],orient="index").T

#Type Effectiveness
type_effectiveness_df = (
    pd.DataFrame.from_dict(raw_data['type_effectiveness'],orient="index")
    .reset_index()
    .rename(columns={"index":"type"})
)

#PVP Fast Moves
pvp_fast_moves_df = (
    pd.DataFrame(raw_data['pvp_fast_moves'])
    .reset_index(drop=True)
)

#PVP Charged Moves
pvp_charged_moves_df = pd.json_normalize(raw_data['pvp_charged_moves'])

#Cp Multiplier
cp_multiplier_df = pd.DataFrame(raw_data['cp_multiplier'])


#Analytics Layer


#Load to database

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False
)

with engine.connect() as conn:
    result = conn.execute(text("Select 1"))
    print("[DB] Connection successful:", result.fetchone())

def load_core_table(df: pd.DataFrame, table_name: str):
    df["ingested_at"] = pd.Timestamp.utcnow()
    df.to_sql(
        name=table_name,
        con=engine,
        schema="core",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )
    
    print(f"[CORE] Loaded {table_name} ({len(df)} rows)")


# ------------------------
# Load Core Tables
# ------------------------


core_tables = {
    "apis": apis_df,
    "pokemons": pokemon_df,
    "alolan_pokemons": alolan_df,
    "pokemon_stats": pokemon_stats_core,
    "pokemon_max_cp": pokemon_max_cp_core,
    "pokemon_types": pokemon_type_core,
    "pokemon_evolutions": pokemon_evolutions_core,
    "pokemon_buddy_distances": buddy_core,
    "pokemon_candies_to_evolve": candies_core,
    "pokemon_rarity": rarity_core,
    "pokemon_generations": generations_core,
    "mega_pokemons": mega_core,
    "shiny_pokemons": shiny_core,
    "baby_pokemons": baby_core,
    "nesting_pokemons": nest_core,
    "galarian_pokemons": galarian_core,
    "research_task_exclusive_pokemons": research_task_core,
    "pvp_exclusive_pokemons": pvp_exclusive_core,
    "pokemon_powerup_requirements":pokemon_power_up_requirements_df,
    "pokemon_forms":pokemon_forms_df,
    "fast_moves":fast_moves_df,
    "charged_moves":charged_moves_df,
    "weather_boost":weather_boost_df,
    "type_effectiveness":type_effectiveness_df,
    "pvp_fast_moves":pvp_fast_moves_df,
    "pvp_charged_moves":pvp_charged_moves_df,
    "cp_multiplier":cp_multiplier_df
}

for table_name, df in core_tables.items():
    load_core_table(df, table_name)

