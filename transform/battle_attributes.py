# transform/battle_attributes.py

import pandas as pd
from typing import Dict, Any


def transform_fast_moves(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(raw_data["fast_moves"]).reset_index(drop=True)


def transform_charged_moves(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(raw_data["charged_moves"]).reset_index(drop=True)


def transform_weather_boost(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame.from_dict(raw_data["weather_boost"], orient="index").T


def transform_type_effectiveness(raw_data: Dict[str, Any]) -> pd.DataFrame:
    damage_map = {
    1.6: "super effective",
    1.0: "neutral",
    0.625: "not very effective",
    0.39: "resistant"
    }
    return (
        pd.DataFrame.from_dict(raw_data["type_effectiveness"])
        .reset_index()
        .melt(
            id_vars="index",
            var_name="defending_type",
            value_name="effectiveness"
        )
        .rename(columns={"index": "attacking_type"})
        .assign(damage_effectiveness=lambda df: df["effectiveness"].map(damage_map))
    )


def transform_pvp_fast_moves(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(raw_data["pvp_fast_moves"]).reset_index(drop=True)


def transform_pvp_charged_moves(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.json_normalize(raw_data["pvp_charged_moves"])


def transform_cp_multiplier(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(raw_data["cp_multiplier"])
