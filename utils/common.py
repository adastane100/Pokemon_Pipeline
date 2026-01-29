import hashlib
from typing import Dict, Any

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
