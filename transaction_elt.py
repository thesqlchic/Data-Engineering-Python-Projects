import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()
import pyodbc as py
import warnings
warnings.filterwarnings('ignore')
import logging
from contextlib import contextmanager
import kagglehub

#logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    filename="transaction_etl_log.txt",
    filemode="a"
)

# Download latest version of the transaction record from kaggle
path = kagglehub.dataset_download("vipin20/transaction-data")

#creating a connection to the staging database
def staging_schema_conn():
    try:
        db_server = os.getenv('db_servername')
        staging_db = os.getenv('staging_db')


        staging_conn = (
            f"mssql+pyodbc://{db_server}/{staging_db}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&trusted_connection=yes"
        )

        staging_engine = create_engine(
            staging_conn,
            pool_pre_ping=True,
            pool_recycle=3600
        )

        return staging_engine
    except Exception as e:
        logging.error(f"error connecting to staging database = {e}", exc_info=True)
        raise

scheme_db_engine = staging_schema_conn()

@contextmanager
def get_db_connection(engine):
    connection=None
    try:
        connection=engine.connect()
        yield connection
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection:
            connection.close()


#putting the csv file in a data frame
def transaction_extract():
    try:
        csv_df = pd.read_csv(r'C:\Users\DELL\.cache\kagglehub\datasets\vipin20\transaction-data\versions\1\transaction_data.csv')

        return csv_df
    except Exception as e:
        logging.error(f"error reading transaction data file:{e}", exc_info=True)
        raise


#loading the records into the staging schema
def loading_transaction():
    try:
        trans_extract = transaction_extract()

        with get_db_connection(scheme_db_engine) as conn:
            with conn.begin() as transaction:
                transaction_load = trans_extract.to_sql(
                    name = 'transaction_table',
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=50,
                    schema=os.getenv('staging_schema')
                )
        
        print('successfully loaded records to the staging schema')
        return transaction_load
    except Exception as e:
        logging.error(f'error loading records to staging schema: {e}',exc_info=True)
        raise

if __name__ == "__main__":  
    loading_transaction()    