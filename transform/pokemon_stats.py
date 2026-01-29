# transform/pokemon_stats.py

import pandas as pd
from typing import Dict, Any
from utils.common import md5_hash


def transform_pokemon_stats(raw_data: Dict[str, Any]) -> pd.DataFrame:
    df = pd.DataFrame(raw_data["pokemon_stats"])

    core = (
        df.drop(columns=["form"], errors="ignore")
        .drop_duplicates(
            subset=["pokemon_id", "base_attack", "base_defense", "base_stamina"]
        )
        .reset_index(drop=True)
    )

    core["pokemon_stat_profile_id"] = core.apply(
        lambda x: md5_hash(
            x["pokemon_id"],
            x["base_attack"],
            x["base_defense"],
            x["base_stamina"],
        ),
        axis=1,
    )

    return core


def transform_pokemon_max_cp(raw_data: Dict[str, Any]) -> pd.DataFrame:
    df = pd.DataFrame(raw_data["pokemon_max_cp"])

    core = (
        df.drop(columns=["form"], errors="ignore")
        .drop_duplicates(subset=["pokemon_id", "max_cp"])
        .reset_index(drop=True)
    )

    core["pokemon_max_cp_profile_id"] = core.apply(
        lambda x: md5_hash(x["pokemon_id"], x["max_cp"]),
        axis=1,
    )

    return core


def transform_pokemon_types(raw_data: Dict[str, Any]) -> pd.DataFrame:
    df = pd.DataFrame(raw_data["pokemon_type"])

    core = (
        df.explode("type")
        .drop(columns=["form"], errors="ignore")
        .drop_duplicates(subset=["pokemon_id", "type"])
        .groupby(["pokemon_id", "pokemon_name"])["type"]
        .agg(list)
        .reset_index()
    )

    core["type_1"] = core["type"].str[0]
    core["type_2"] = core["type"].str[1]

    return core.drop(columns=["type"])
