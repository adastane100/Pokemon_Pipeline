import hashlib
import pandas as pd
from typing import Dict, Any, List
from pathlib import Path
from sqlalchemy import text
from config.config import DB_SCHEMA, DB_CHUNK_SIZE

def md5_hash(*args)->str:
    #Generate MD5 hash from multiple values

    return hashlib.md5(
        "|".join(map(str, args)).encode()
    ).hexdigest()

def flatten_single_list_dict(
    data: Dict[str, Any],
    prefix_map: Dict[str, str] | None = None
) -> Dict[str, Any]:
    
    #Flatten dict values that are lists containing a single dict
    
    row = {}
    prefix_map = prefix_map or {}

    for key, value in data.items():
        if (
            isinstance(value, list)
            and len(value) == 1
            and isinstance(value[0], dict)
        ):
            prefix = prefix_map.get(key, f"{key}_")
            for k, v in value[0].items():
                row[f"{prefix}{k}"] = v
        else:
            row[key] = value

    return row

def flag_column(df, col_name:str, value:str="Yes"):
    #Add a flag column with default value

    df[col_name] = value
    return df

def safe_str(value:Any)->str:
    #Normalize values for hashing/comparison

    if value is None:
        return ""
    return str(value).lower().strip()

def merge_on_key(base_df: pd.DataFrame, tables: List[pd.DataFrame], key:str = "pokemon_id" ) -> pd.DataFrame:
    
    result = base_df.copy()
    for table in tables:
        # Only bring columns that do NOT already exist
        new_cols = [c for c in table.columns if c not in result.columns]
        if key not in new_cols:
            new_cols = [key] + new_cols

        result = result.merge(
            table[new_cols],
            on=key,
            how="left")
    return result

def merge_on_keys(base_df:pd.DataFrame, tables: List[tuple]) -> pd.DataFrame:

    result=base_df.copy()
    for table, keys in tables:
        if isinstance(keys, dict):
            result = result.merge(
                table,
                left_on=list(keys.keys()),
                right_on=list(keys.values()),
                how="left"
            )
        else:
            result = result.merge(table, on=keys, how="left")
    return result

def save_cache(df, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def load_cache(path: str):
    return pd.read_parquet(path) if Path(path).exists() else None
