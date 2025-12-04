import kaggle
import pandas as pd
import glob
import warnings
import numpy as np
import logging
import os
from dotenv import load_dotenv
load_dotenv()
import pyodbc as py
from contextlib import contextmanager
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')

#implementing logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    filename="transaction_etl_log.txt",
    filemode="a"
)

#creating a function that downloads the transaction data from Kaggle if it does not exist
def kaggle_extract(dataset='vipin20/transaction-data', pattern= 'transaction_*.csv'):
    try:
        kaggle.api.authenticate()

        #using the glob library to check if the transaction dataset exist. if it does not exist then it should be downloaded
        if not glob.glob(pattern):
            kaggle.api.dataset_download_files('vipin20/transaction-data',path='.',unzip=True)

            print('successfully downloaded records from kaggle')
        else:
            print('file already exist')

        transaction_files = glob.glob(pattern)
        #rechecking if the file exist. if it doesn't an error will be shown
        if not transaction_files:
            raise FileNotFoundError(f'error locating transaction_data, {pattern}')

        #putting the file into a csv dataframe
        tran_df = pd.read_csv(transaction_files[0])

        return tran_df
    except Exception as e:
        logging.error(f'error extracting records {e}', exc_info=True)
        raise

def tran_data_transformation():
    try:
        tran_extract = kaggle_extract()

        #copying the data before transformation and cleaning
        tran_copy = tran_extract.copy()

        #removing duplicates from the data
        tran_copy.drop_duplicates(inplace=True)

        #extracting date part from the date column
        tran_copy['TransactionDate'] = tran_copy['TransactionTime'].str[4:10] +" " + tran_copy['TransactionTime'].str[24:]

        #extracting the time value from the transactiontime column
        tran_copy['TimeofTransaction'] = tran_copy['TransactionTime'].str[11:23]

        #creating a new column called transactionDOW which will have date of week extracted from transactiontime column
        tran_copy['TransactionDOW'] = tran_copy['TransactionTime'].str[0:4]

        #TransactionTime column has been cleaned and can now be removed
        tran_copy.drop('TransactionTime',axis=1, inplace=True)

        #removing negative signs from values in NumberOfItemsPurchased
        tran_copy['NumberOfItemsPurchased']=tran_copy['NumberOfItemsPurchased'].abs()

        #changing the data type of transaction date column to date datetype of year-month-day
        tran_copy['TransactionDate']=pd.to_datetime(tran_copy['TransactionDate'],format= '%b %d %Y')

        #cleaning records with characters like ? in the ItemDescription column
        tran_copy['ItemDescription'] = tran_copy['ItemDescription'].replace(r'^\?+$',np.nan, regex=True)

        #handling null values in the ItemDescription column
        #grouping item description values by itemcode and assigning most common value in each group to null values in item description
        tran_copy['ItemDescription'] = tran_copy.groupby('ItemCode')['ItemDescription'].transform(lambda x: x.fillna(x.mode().iloc[0]) if not x.mode().empty else x )
        print('transformation completed')

        return tran_copy
    except Exception as e:
        logging.error(f'error transdorming transaction data: {e}', exc_info=True)
        raise

#creating connection to the database to store the clean record
def db_connection():
    try:
        db_server = os.getenv('db_servername')
        db_loading = os.getenv('db_name')

        db_conn = (
            f"mssql+pyodbc://{db_server}/{db_loading}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&trusted_connection=yes"
        )

        db_engine = create_engine(
            db_conn,
            pool_pre_ping =True,
            pool_recycle= 3600
        )

        return db_engine
    except Exception as e:
        logging.error(f"error connecting to database {e}", exc_info=True)
        raise

def loading_tran_data():
    try:
        db_conn_engine = db_connection()

        tran_extract = tran_data_transformation()

        print('loading data has started')
        transaction_load = tran_extract.to_sql(
            name = 'transaction_data',
            con=db_conn_engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=50
        )

        print('loading completed')
        return transaction_load
    except Exception as e:
        logging.error(f"error loading records to database:{e}", exc_info=True)
        raise

if __name__ == "__main__":
    loading_tran_data()




#saving the clean records to a new csv file
# tran_unique.to_csv('transaction_data_clean.csv', index=False)