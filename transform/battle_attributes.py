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
    return (
        pd.DataFrame.from_dict(raw_data["type_effectiveness"], orient="index")
        .reset_index()
        .rename(columns={"index": "type"})
    )


def transform_pvp_fast_moves(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(raw_data["pvp_fast_moves"]).reset_index(drop=True)


def transform_pvp_charged_moves(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.json_normalize(raw_data["pvp_charged_moves"])


def transform_cp_multiplier(raw_data: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(raw_data["cp_multiplier"])
