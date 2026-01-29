# pokemon_data_de.py

from extract.pogo_api import extract_all
from transform.core_tables import build_core_tables
from load.db import get_engine, test_connection
from load.core_loader import load_core_table


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

    # --------------------------------------------------
    # LOAD
    # --------------------------------------------------
    print("\n📤 Loading data into Postgres...")
    engine = get_engine()
    test_connection(engine)

    for table_name, df in core_tables.items():
        load_core_table(
            df=df,
            table_name=table_name,
            engine=engine,
            if_exists="replace"  # can later be "append"/"upsert"
        )

    print("\n✅ Pokémon Data Pipeline completed successfully!")


if __name__ == "__main__":
    run_pipeline()
