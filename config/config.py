#config/config.py

from pathlib import Path
import os
from dotenv import load_dotenv

#Load .env file
load_dotenv()

#Project Paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT/"raw_data"
RAW_DATA_PATH.mkdir(exist_ok=True)

#API Configuration
BASE_URL = "https://pogoapi.net"

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

API_TIMEOUT = 10

#Database Configuration
DB_CONFIG = {
    "user":os.getenv("DB_USER"),
    "password":os.getenv("DB_PASSWORD"),
    "host":os.getenv("DB_HOST"),
    "port":os.getenv("DB_PORT"),
    "database":os.getenv("DB_NAME")
}

DB_SCHEMA = "core"
DB_CHUNK_SIZE = 1000