import pandas as pd
from sqlalchemy import create_engine
from utils.logging import get_logger
from utils.get_faker_data import generate_fake_client_data

LOGGER = get_logger()

user = "postgres"
password = "KxZkfQFIjt"
server = "localhost"
dbname = "cars"

PG_URI = f"postgresql://{user}:{password}@{server}:5432/{dbname}"

def _connect_db(uri:str):
    """Connect to a database"""

    try:
        engine = create_engine(uri)
        # conn = engine.connect().execution_options(stream_results=True)
        return engine
    except Exception as err:
        LOGGER.error(err)

def _df_to_sql(uri: str):
    """Write df with client data to SQL"""

    conn = _connect_db(uri)

    df = generate_fake_client_data()
    df.to_sql('clients', conn, if_exists='append')
    conn.dispose()

    LOGGER.info("Successfully wrote fake data to database")

def _read_table_to_df(uri:str):
    """Create datafrom from SQL query"""

    schema = 'public'
    table = 'clients'

    query = f'SELECT * FROM "{schema}"."{table}";'

    conn = _connect_db(uri)
    conn.execution_options(stream_results=True)

    df = pd.read_sql_query(query, conn)
    chunk_df = pd.DataFrame()
    
    conn.dispose()

    LOGGER.info(f"Successfully read {table} from database with no chunking")
    # memory usage: 9.7 MB
    return df

def _read_table_to_df_chunking(uri:str):
    """Create dataframe from from SQL query"""

    schema = 'public'
    table = 'clients'

    query = f'SELECT * FROM "{schema}"."{table}";'

    conn = _connect_db(uri)
    # conn.execution_options(stream_results=True)

    chunk_df = pd.DataFrame()
    for df in pd.read_sql_query(query, conn, chunksize=1000):
        chunk_df = pd.concat([chunk_df, df])

    conn.dispose()

    LOGGER.info(f"Successfully read {table} from database using chunking")
    
    return chunk_df

     

if __name__ == "__main__":
    # _df_to_sql(PG_URI)
    df = _read_table_to_df_chunking(PG_URI)
    # df2 = _read_table_to_df(PG_URI)


#  df.info(memory_usage='deep')
#  df2.info(memory_usage='deep')