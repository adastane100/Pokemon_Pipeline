# pokemon_data_de.py

from extract.pogo_api import extract_all
from transform.core_tables import build_core_tables
from analytics.analytics import analytics
from load.db import get_engine, test_connection
from load.loader import load_core_table, load_analytics_table


def run_pipeline() -> None:
    print("🚀 Starting Pokémon Data Pipeline")

    # --------------------------------------------------
    # EXTRACT
    # --------------------------------------------------
    print("\n📥 Extracting data from Pokémon API...")
    raw_data = extract_all()

    # --------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------
    print("\n🔧 Transforming raw data into core tables...")
    core_tables = build_core_tables(raw_data)

    print("\n📊 Building analytics tables...")
    dim_pokemon, dim_pokemon_form_stats = analytics(core_tables)

    # --------------------------------------------------
    # LOAD
    # --------------------------------------------------
    print("\n📤 Loading data into Postgres...")
    engine = get_engine()
    test_connection(engine)

    print("\n📈 Loading core tables...")
    for table_name, df in core_tables.items():
        load_core_table(
            df=df,
            table_name=table_name,
            engine=engine,
            if_exists="replace"  # can later be "append"/"upsert"
        )
    
        # ---- Load analytics tables ----
    print("\n📈 Loading analytics tables...")
    load_analytics_table(
        df=dim_pokemon,
        table_name="dim_pokemon",
        engine=engine,
        if_exists="replace"
    )

    load_analytics_table(
        df=dim_pokemon_form_stats,
        table_name="dim_pokemon_form_stats",
        engine=engine,
        if_exists="replace"
    )

    print("\n✅ Pokémon Data Pipeline completed successfully!")



if __name__ == "__main__":
    run_pipeline()
