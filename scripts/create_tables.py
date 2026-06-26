import psycopg2
import os 
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger(__name__)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PSPASSWORD"),
        dbname=os.getenv("PGDATABASE"),
    )
CREATE_TABLE_SQL = {
    "raw_pets": """
        DROP TABLE IF EXISTS raw_pets CASCADE; 
        CREATE TABLE IF NOT EXISTS raw_pets(
            pet_id TEXT,
            pet_name TEXT,
            species TEXT,
            breed TEXT,
            age TEXT,
            vaccinated TEXT,
            owner_id TEXT
            )
""",
    "raw_owners": """
        DROP TABLE IF EXISTS raw_owners CASCADE ; 
        CREATE TABLE  IF NOT EXISTs raw_owners
            (owner_id  TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            city TEXT)""",
    "raw_visits": """
        DROP TABLE IF EXISTS  raw_visits CASCADE ;
        CREATE TABLE  IF NOT EXISTS  raw_visits
            (visit_id  TEXT,
            pet_id TEXT,
            visit_date TEXT,
            reason TEXT,
            cost TEXT,
            vet_name TEXT,
            notes TEXT)"""
}

def create_tables():
    logger.info("Connecting to PostgreSQL..")
    conn = get_connection()
    cur = conn.cursor()
    
    for table_name, sql in CREATE_TABLE_SQL.items():
        logger.info(f"Creating table: {table_name}")
        cur.execute(sql)
        logger.info(f"Table {table_name} ready")

    conn.commit()
    cur.close
    conn.close()
    logger.info("all tables created!")

if __name__ == '__main__':
    create_tables()



