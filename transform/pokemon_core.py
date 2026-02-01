# transform/pokemon_core.py

import pandas as pd
from typing import Dict, Any

from utils.common import flag_column


def transform_apis(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return (
        pd.DataFrame.from_dict(raw_data["apis"], orient="index")
        .reset_index(drop=True)
    )


def transform_pokemons(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return (
        pd.DataFrame.from_dict(raw_data["pokemons"], orient="index")
        .rename(columns={"id": "pokemon_id"})
        .reset_index(drop=True)
    )


def transform_alolan(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return (
        pd.DataFrame.from_dict(raw_data["alolan_pokemons"], orient="index")
        .rename(columns={"id": "pokemon_id"})
        .pipe(flag_column, "alolan_form")
        .reset_index(drop=True)
    )


def transform_shiny(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return (
        pd.DataFrame.from_dict(raw_data["shiny_pokemons"], orient="index")
        .rename(columns={"id": "pokemon_id"})
        #.pipe(flag_column, "shiny_pokemon")
        .reset_index(drop=True)
    )


def transform_baby(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return flag_column(
        pd.DataFrame(raw_data.get("baby_pokemons", []))
        .rename(columns={"id": "pokemon_id"}),
        "baby_pokemon"
    )


def transform_nesting(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return (
        pd.DataFrame.from_dict(raw_data["nesting_pokemons"], orient="index")
        .rename(columns={"id": "pokemon_id"})
        .pipe(flag_column, "nest_status")
        .reset_index(drop=True)
    )


def transform_galarian(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return (
        pd.DataFrame.from_dict(raw_data["galarian_pokemon"], orient="index")
        .rename(columns={"id": "pokemon_id"})
        .pipe(flag_column, "galarian_form")
        .reset_index(drop=True)
    )


def transform_research_task(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return flag_column(
        pd.DataFrame(raw_data.get("research_task_exclusive_pokemon", []))
        .rename(columns={"id": "pokemon_id"}),
        "research_task_exclusive_pokemon"
    )


def transform_pvp_exclusive(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return flag_column(
        pd.DataFrame(raw_data.get("pvp_exclusive_pokemon", []))
        .rename(columns={"id": "pokemon_id"}),
        "pvp_exclusive_status"
    )


def transform_generations(raw_data: Dict[str, Any]) -> pd.DataFrame:
    gen_df = []

    for gen_name, entries in raw_data["pokemon_generations"].items():
        df = pd.DataFrame(entries)
        df["generation"] = gen_name
        gen_df.append(df)

    return (
        pd.concat(gen_df, ignore_index=True)
        .rename(columns={"id": "pokemon_id", "name": "pokemon_name"})
    )


def transform_forms(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(
        {"form_name": raw_data.get("pokemon_forms")},
        index=pd.Index(
            range(len(raw_data.get("pokemon_forms"))),
            name="form_id"
        )
    ).reset_index()
