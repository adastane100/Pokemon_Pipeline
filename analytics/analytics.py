import pandas as pd
from typing import List, Dict, Any
#from utils.common import merge_on_key, merge_on_keys


def analytics(core_tables:Dict[str,pd.DataFrame])-> Dict[str,pd.DataFrame]:

    dim_pokemon = (
        core_tables['pokemon_generations']
        .merge
        (
        core_tables['alolan_pokemons'][["pokemon_id","alolan_form"]],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['shiny_pokemons'][['found_egg', 'found_evolution', 
                            'found_photobomb', 'found_raid','found_research', 'found_wild', 
                            'pokemon_id', 'alolan_shiny']],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['baby_pokemons'][["pokemon_id","baby_pokemon"]],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['nesting_pokemons'][["pokemon_id","nest_status"]],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['galarian_pokemons'][["pokemon_id","galarian_form"]],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['research_task_exclusive_pokemons'][["pokemon_id","research_task_exclusive_pokemon"]],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['pvp_exclusive_pokemons'][["pokemon_id","pvp_exclusive_status"]],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['pokemon_types'][["pokemon_id",'type_1', 'type_2']],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['pokemon_buddy_distances'][["pokemon_id","buddy_distance"]],
        on="pokemon_id",
        how="left"
        )
        .merge
        (
        core_tables['pokemon_rarity'][["pokemon_id","rarity_category"]],
        on="pokemon_id",
        how="left"
        )
    )
    
    dim_pokemon_form_stats = (
        core_tables['pokemon_stats']
        .merge
        (
        core_tables['pokemon_max_cp'][["pokemon_id","form","max_cp"]],
        on=["pokemon_id","form"],
        how="left"
        )
        .merge
        (
        core_tables['pokemon_evolutions'][["pokemon_id","form",'evo_pokemon_id', 'evo_pokemon_name', 'evo_form', 
                                           'evo_candy_required', 'evo_item_required', 'evo_lure_required', 
                                           'evo_gender_required', 'evo_only_evolves_in_daytime', 'evo_only_evolves_in_nighttime', 
                                           'evo_buddy_distance_required', 'evo_no_candy_cost_if_traded', 
                                           'evo_upside_down', 'evolution_id']],
        on=["pokemon_id","form"],
        how="left"
        )
        .merge
        (
        core_tables['pokemon_forms'][["form_id","form_name"]],
        left_on="form", 
        right_on="form_name", 
        how="left"
        )
        .merge
        (
        core_tables['pokemon_candies_to_evolve'][["pokemon_id","form","candy_required"]],
        on=["pokemon_id","form"],
        how="left"
        )
        .merge
        (
        core_tables['mega_pokemons'][["pokemon_id","form", 'first_time_mega_energy_required', 
                                      'mega_energy_required', 'mega_name', 
                                      'stats_base_attack','stats_base_defense', 
                                      'stats_base_stamina', 'type_0', 'type_1','mega_form']],
        on=["pokemon_id","form"],
        how="left"
        )
        )
    dim_fast_moves = (
        core_tables["pvp_fast_moves"]
    )

    dim_charged_moves = (
        core_tables["pvp_charged_moves"]
    )

    dim_type_effectiveness = (
        core_tables['type_effectiveness']
    )
    
    return { 
        "dim_pokemon":dim_pokemon,
        "dim_pokemon_form_stats":dim_pokemon_form_stats,
        "dim_fast_moves":dim_fast_moves,
        "dim_charged_moves":dim_charged_moves,
        "dim_type_effectiveness":dim_type_effectiveness

    }


