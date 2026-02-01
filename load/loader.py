# load/core_loader.py

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from config.config import DB_SCHEMA, DB_CHUNK_SIZE

def load_df_to_postgres(df:pd.DataFrame, table_name:str, engine, schema:str = 'public', if_exists: str="replace"):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
    
    df = df.copy()
    df["ingested_at"] = pd.Timestamp.now()

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=DB_CHUNK_SIZE
    )

    print(f"Table '{schema}.{table_name}' loaded successfully ({len(df)} records)")

def load_core_table(df, table_name, engine, schema="core", if_exists="replace"):
    load_df_to_postgres(df, table_name, engine, schema=schema, if_exists=if_exists)

def load_analytics_table(df, table_name, engine, schema="analytics", if_exists="replace"):
    load_df_to_postgres(df, table_name, engine, schema=schema, if_exists=if_exists)    
