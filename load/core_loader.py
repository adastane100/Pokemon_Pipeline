# load/core_loader.py

import pandas as pd
from sqlalchemy.engine import Engine

from config.config import DB_SCHEMA, DB_CHUNK_SIZE


def load_core_table(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    if_exists: str = "replace",
) -> None:
    """
    Load dataframe into Postgres core schema
    """
    df = df.copy()
    df["ingested_at"] = pd.Timestamp.now()

    df.to_sql(
        name=table_name,
        con=engine,
        schema=DB_SCHEMA,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=DB_CHUNK_SIZE,
    )

    print(f"[CORE] Loaded {table_name} ({len(df)} rows)")
