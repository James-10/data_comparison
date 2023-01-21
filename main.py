import pandas as pd
from sqlalchemy import create_engine
from utils.logging import get_logger

LOGGER = get_logger()

user = "postgres"
password = ""
server = "localhost"
dbname = "cars"

PG_URI = f"postgresql://{user}:{password}@{server}:5432/{dbname}"

def _connect_db():
    try:
        engine = create_engine(PG_URI)
        conn = engine.connect()
        return conn
    except Exception as err:
        LOGGER.error(err)

     

if __name__ == "__main__":
    conn = _connect_db()