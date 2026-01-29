# load/db.py

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from config.config import DB_CONFIG


def get_engine(echo: bool = False):
    """
    Create and return SQLAlchemy engine
    """
    user = DB_CONFIG["user"]
    password = quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    database = DB_CONFIG["database"]

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        echo=echo,
    )

    return engine


def test_connection(engine) -> None:
    """
    Validate database connectivity
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("[DB] Connection successful:", result.fetchone())
