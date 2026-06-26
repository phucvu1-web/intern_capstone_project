import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
import sys
from pathlib import Path
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent

def get_engine():
    conn_str = f"postgresql+psycopg2://postgres_warehouse:postgres_warehouse@postgres_warehouse:5432/postgres_warehousedb"
    return create_engine(conn_str)

CSV_PATH = {
    "raw_pets" : ROOT / "project" / "seeds" / "pets.csv",
    "raw_owners" : ROOT / "project" / "seeds" / "owners.csv",
    "raw_visits" : ROOT / "project" / "seeds" / "visits.csv",
}

def ingest_csv(engine, table_name, csv_path):
    logger.info(f"loading {csv_path} -> {table_name}")
    csv_file = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    csv_file = csv_file.replace("", None)

    row_cnt = len(csv_file)

    logger.info(f" read {row_cnt} rows")

    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
    logger.info(f"DONE")

    csv_file.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info(f"Inserted {row_cnt} rows into {table_name}")

def ingest_all():
    engine = get_engine()
    logger.info("starting...")
    for table_name, csvpath in CSV_PATH.items():
        ingest_csv(engine, table_name,csvpath)
    logger.info("DONE")
if __name__ == '__main__':
    ingest_all()
