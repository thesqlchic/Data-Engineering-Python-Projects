import kagglehub
import pandas as pd
import os
import logging

#creating a log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    filename="cafe_sales_etl_log.txt",
    filemode="a"
)

#creating a function to download the records from Kaggle and also extract it from the file path downloaded into a csv dataframe
def cafe_sales_extraction():
    try:
        # Get the dataset from kaggle
        download_path = kagglehub.dataset_download(
            "ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training"
        )
        
        # print the file path where the dataset was downloaded
        print(f"Dataset downloaded to: {download_path}")

        # dynamically storing the dataset in a csv dataframe and reading the data from the csv
        csv_files = [f for f in os.listdir(download_path) if f.endswith('.csv')]
        csv_path = os.path.join(download_path,csv_files[0])
        csv_df = pd.read_csv(csv_path)
        
        print(f"cafe sales data loaded. Shape: {csv_df.shape}")
   
        return csv_df
        
    except Exception as e:
        logging.error(f'Error extracting records: {e}', exc_info=True)
        raise

def cafe_sales_transformation():
    try:
        print('cleaning and transforming begins')

        #storing the extracted cafe sales data into a variable called cafe sales data
        cafe_sales_org = cafe_sales_extraction()

        #copying the extracted data before transfomation to avoid data loss
        cafe_sales_data = cafe_sales_org.copy()

        #changing the column headers from proper case to lower case
        #also putting an underscore in column headers with space
        cafe_sales_data.columns = cafe_sales_data.columns.str.lower().str.replace(' ', '_')


        #replacing values like null, unknown and error in text data type columns item, payment_method and location to Not Specified
        cafe_sales_data[['item', 'payment_method', 'location']] = cafe_sales_data[['item', 'payment_method', 'location']].fillna('Not Specified').replace(['ERROR', 'UNKNOWN'], 'Not Specified')

        #replacing values like null, unknown and error in numeric data types columns like quantity, price_per_unit and total_spend to 0
        cafe_sales_data[['quantity', 'price_per_unit', 'total_spent']] = cafe_sales_data[['quantity', 'price_per_unit', 'total_spent']].fillna(0).replace(['ERROR', 'UNKNOWN'], 0) 

        #replacing values like null, unknown and error in transacation data with 1999-01-01'
        cafe_sales_data[['transaction_date']] = cafe_sales_data[['transaction_date']].fillna('1999-01-01').replace(['ERROR', 'UNKNOWN'], '1999-01-01')

        #changing the data type of quantity, price_per_unit and total_spent to numeric from object
        cafe_sales_data[['quantity', 'price_per_unit', 'total_spent']] = cafe_sales_data[['quantity', 'price_per_unit', 'total_spent']].apply(pd.to_numeric)

        #changing the data type of transaction date from object to date in the format year-month-day
        cafe_sales_data['transaction_date'] = pd.to_datetime(cafe_sales_data['transaction_date'],format='%Y-%m-%d')

        transformed_cafe_sales_data = cafe_sales_data

        return transformed_cafe_sales_data
    except Exception as e:
        logging.error(f'Error transforming records: {e}', exc_info=True)
        raise

def loading_cafe_sales():
    try:
        cafe_sales_trans = cafe_sales_transformation

        #writing the transformed data to csv using pandas
        loading_df = pd.DataFrame(cafe_sales_trans)

        load_data = loading_df.to_csv('cafe_sales.csv', index= False)

        return load_data
    except Exception as e:
        logging.error(f'Error loading records: {e}', exc_info=True)
        raise

if __name__ == '__main__':
    loading_cafe_sales()