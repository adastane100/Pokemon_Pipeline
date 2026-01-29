# transform/core_tables.py

from typing import Dict, Any

# Core transforms
from transform.pokemon_core import (
    transform_apis,
    transform_pokemons,
    transform_alolan,
    transform_shiny,
    transform_baby,
    transform_nesting,
    transform_galarian,
    transform_research_task,
    transform_pvp_exclusive,
    transform_generations,
    transform_forms
)

# Stats transforms
from transform.pokemon_stats import (
    transform_pokemon_stats,
    transform_pokemon_max_cp,
    transform_pokemon_types
)

# Evolution transforms
from transform.pokemon_evolutions import (
    transform_pokemon_evolutions,
    transform_buddy_distances,
    transform_candies_to_evolve,
    transform_rarity,
    transform_powerup_requirements,
    transform_mega_pokemon
)

# Battle transforms
from transform.battle_attributes import (
    transform_fast_moves,
    transform_charged_moves,
    transform_weather_boost,
    transform_type_effectiveness,
    transform_pvp_fast_moves,
    transform_pvp_charged_moves,
    transform_cp_multiplier
)


def build_core_tables(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Orchestrate all transforms and return a dictionary of DataFrames
    keyed by table name.
    """
    core_tables = {
        # Core
        "apis": transform_apis(raw_data),
        "pokemons": transform_pokemons(raw_data),
        "alolan_pokemons": transform_alolan(raw_data),
        "shiny_pokemons": transform_shiny(raw_data),
        "baby_pokemons": transform_baby(raw_data),
        "nesting_pokemons": transform_nesting(raw_data),
        "galarian_pokemons": transform_galarian(raw_data),
        "research_task_exclusive_pokemons": transform_research_task(raw_data),
        "pvp_exclusive_pokemons": transform_pvp_exclusive(raw_data),
        "pokemon_generations": transform_generations(raw_data),
        "pokemon_forms": transform_forms(raw_data),

        # Stats
        "pokemon_stats": transform_pokemon_stats(raw_data),
        "pokemon_max_cp": transform_pokemon_max_cp(raw_data),
        "pokemon_types": transform_pokemon_types(raw_data),

        # Evolutions / Progression
        "pokemon_evolutions": transform_pokemon_evolutions(raw_data),
        "pokemon_buddy_distances": transform_buddy_distances(raw_data),
        "pokemon_candies_to_evolve": transform_candies_to_evolve(raw_data),
        "pokemon_rarity": transform_rarity(raw_data),
        "pokemon_powerup_requirements": transform_powerup_requirements(raw_data),
        "mega_pokemons": transform_mega_pokemon(raw_data),

        # Battle / PvP
        "fast_moves": transform_fast_moves(raw_data),
        "charged_moves": transform_charged_moves(raw_data),
        "weather_boost": transform_weather_boost(raw_data),
        "type_effectiveness": transform_type_effectiveness(raw_data),
        "pvp_fast_moves": transform_pvp_fast_moves(raw_data),
        "pvp_charged_moves": transform_pvp_charged_moves(raw_data),
        "cp_multiplier": transform_cp_multiplier(raw_data),
    }

    return core_tables
