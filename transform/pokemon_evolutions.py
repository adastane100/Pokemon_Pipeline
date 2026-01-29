# transform/pokemon_evolution.py

import pandas as pd
from typing import Dict, Any, List

from utils.common import md5_hash, flatten_single_list_dict, safe_str


def transform_pokemon_evolutions(raw_data: Dict[str, Any]) -> pd.DataFrame:
    rows = [
        flatten_single_list_dict(p, prefix_map={"evolutions": "evo_"})
        for p in raw_data.get("pokemon_evolutions", [])
    ]

    df = pd.DataFrame(rows)

    expected_cols = [
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
        "evo_upside_down",
    ]

    df = df.reindex(columns=expected_cols)
    df = df.where(pd.notnull(df), None)

    df["evolution_id"] = df.apply(
        lambda x: md5_hash(
            safe_str(x["pokemon_id"]),
            safe_str(x["evo_pokemon_name"]),
            safe_str(x["evo_form"]),
            safe_str(x["evo_item_required"]),
            safe_str(x["evo_lure_required"]),
            safe_str(x["evo_only_evolves_in_daytime"]),
            safe_str(x["evo_only_evolves_in_nighttime"]),
        ),
        axis=1,
    )

    return df.drop_duplicates(subset=["evolution_id"])


def transform_buddy_distances(raw_data: Dict[str, Any]) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []

    for dist, entries in raw_data["buddy_distances"].items():
        df = pd.DataFrame(entries)
        df["buddy_distance"] = dist
        dfs.append(df)

    return (
        pd.concat(dfs, ignore_index=True)
        .groupby(["pokemon_id", "pokemon_name", "buddy_distance"])["form"]
        .agg(list)
        .reset_index()
    )


def transform_candies_to_evolve(raw_data: Dict[str, Any]) -> pd.DataFrame:
    dfs = []

    for candy, entries in raw_data["candies_to_evolve"].items():
        df = pd.DataFrame(entries)
        df["candy_required"] = int(candy)
        dfs.append(df)

    core = (
        pd.concat(dfs, ignore_index=True)
        .drop_duplicates(subset=["pokemon_id", "candy_required"])
    )

    core["candy_form_id"] = core.apply(
        lambda x: md5_hash(x["pokemon_id"], x["form"], x["candy_required"]),
        axis=1,
    )

    return core


def transform_rarity(raw_data: Dict[str, Any]) -> pd.DataFrame:
    dfs = []

    for rarity, entries in raw_data["pokemon_rarity"].items():
        df = pd.DataFrame(entries)
        df["rarity_category"] = rarity
        dfs.append(df)

    return (
        pd.concat(dfs, ignore_index=True)
        .drop_duplicates(subset=["pokemon_id", "rarity_category"])
    )


def transform_powerup_requirements(raw_data: Dict[str, Any]) -> pd.DataFrame:
    df = (
        pd.DataFrame.from_dict(
            raw_data["pokemon_powerup_requirements"], orient="index"
        )
        .reset_index(drop=True)
    )

    df["pokemon_powerup_requirements_id"] = df.apply(
        lambda x: md5_hash(x["candy_to_upgrade"], x["current_level"]),
        axis=1,
    )

    return df


def transform_mega_pokemon(raw_data: Dict[str, Any]) -> pd.DataFrame:
    from flatten_json import flatten

    df = pd.DataFrame([flatten(p) for p in raw_data.get("mega_pokemon", [])])

    df["mega_form"] = "Yes"
    df["mega_form_id"] = df.apply(
        lambda x: md5_hash(
            x["pokemon_id"],
            x["form"],
            x["stats_base_attack"],
            x["stats_base_defense"],
            x["stats_base_stamina"],
        ),
        axis=1,
    )

    return df
