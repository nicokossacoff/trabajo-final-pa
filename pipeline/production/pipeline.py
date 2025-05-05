import datetime
import pandas as pd
import os
import psycopg2
from google.cloud import storage
from google.oauth2 import service_account
import io
from sqlalchemy import create_engine
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import numpy as np
from airflow.operators.bash import BashOperator


BUCKET_NAME = 'tp-buckets-prog-avanzada'
CURRENT_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
PREVIOUS_DATE = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

def get_authenticated_client(scopes: list = None) -> storage.Client:
    """
    Creates a Google Cloud Storage client with the given service account credentials.

    Args:
        scopes (list): List of scopes for the credentials. If None, defaults to cloud-platform scope.
    Returns:
        storage.Client: Authenticated Google Cloud Storage client.
    """
    # Check if the service account path is provided. If not provided, defaults to cloud-platform scope
    if scopes is None:
        # Gets the credentials from the service account file with the cloud-platform scope
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        # Creates a client with the credentials
        client = storage.Client(credentials=credentials)
        return client
    else:
        # Get credentials from the service account file with the specified scopes
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_PATH,
            scopes=scopes
        )
        # Creates a client with the credentials
        client = storage.Client(credentials=credentials)
        return client

def filter_data() -> None:
    """
    Filters files in the raw data to only include active advertisers.

    Returns:
        None
    """
    try:
        # Create a Bucket Client
        #client = get_authenticated_client()
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)

        # Creates a reference (a point) to the file in GCS that we want to load
        blob = bucket.blob(f'raw_data/advertiser_ids.parquet')
        # Actually downloads the file in memory as bytes
        content = blob.download_as_bytes()

        # Extract active advertisers
        # The io.BytesIO() function wraps the bytes in a buffer that Pandas can read
        advertisers_df = pd.read_parquet(io.BytesIO(content))
        active_advertisers = advertisers_df['advertiser_id'].tolist()

        # Gets the current date and the previous date
        # current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        # previous_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        # Returns all the files in the bucket. This is an iterator
        blobs = client.list_blobs(BUCKET_NAME, prefix='raw_data/')

        # Iterate through the blobs
        for blob in blobs:
            # The attribute .name contains the path to the file in GCS. We only want the last part
            file_name = blob.name.split('/')[-1]
            if file_name.endswith('views.parquet'):
                # Downloads the file in memory as bytes and uses io.BytesIO to read it
                content = blob.download_as_bytes()
                temp_df = pd.read_parquet(io.BytesIO(content))

                # Filters DataFrame
                temp_df['date'] = temp_df['date'].astype('datetime64[ns]')
                filtered_df = temp_df.loc[(temp_df['advertiser_id'].isin(active_advertisers)) & (temp_df['date'] == PREVIOUS_DATE), :]
                print(f'Filtered {file_name[:-8]} with data from {PREVIOUS_DATE}')

                # Creates a reference to the object that we are going to write in GCS. It doesn't exist yet
                output_blob = bucket.blob(f'temp/{file_name[:-8]}_{CURRENT_DATE}_filtered.parquet')

                # Creates a buffer in the memory. It's like a file, but it's in memory and not on disk
                buffer = io.BytesIO()

                # Writes the DataFrame to the buffer in parquet format
                filtered_df.to_parquet(buffer, index=False)
                # To upload
                buffer.seek(0)

                # Uploads the buffer to GCS
                output_blob.upload_from_file(buffer)
    except Exception as error:
        print(f'An error occurred: {error}')

def top_products(n: int = 20) -> None:
    """
    Generates a list of the top 20 products for each advertiser based on the number of views.

    Args:
        n (int): Number of top products to select for each advertiser.
    Returns:
        None
    """
    try:
        # Creates a Bucker client
        #client = get_authenticated_client()
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)

        # Gets the current date and the previous date
        # current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        # previous_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        # Creates a reference (a point) to the file in GCS that we want to load
        blob = bucket.blob(f'temp/product_views_{CURRENT_DATE}_filtered.parquet')
        # Actually downloads the file in memory as bytes
        content = blob.download_as_bytes()
        # Extracts the file
        df = pd.read_parquet(io.BytesIO(content))

        # Groups by (advertiser_id, product_id)
        df_grouped = df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
        df_grouped = df_grouped.sort_values(by='views', ascending=False)

        # Creates a new DataFrame with only the top n products for each advertiser
        final_df = pd.DataFrame(columns=['advertiser_id', 'product_id', 'date'])
        for (advertiser, temp_df) in df_grouped.groupby('advertiser_id'):
            temp_df = temp_df.sort_values(by='views', ascending=False).head(n)
            temp_df['date'] = CURRENT_DATE
            temp_df['ranking'] = np.arange(1, 21)
            temp_df = temp_df.loc[:, ['advertiser_id', 'product_id', 'ranking', 'date']]
            print(f'Added {len(temp_df)} products for {advertiser}')
            final_df = pd.concat([final_df, temp_df], ignore_index=True)

        # Creates a reference to the object that we are going to write in GCS. It doesn't exist yet
        output_blob = bucket.blob(f'temp/top_products_{CURRENT_DATE}.parquet')
        # Creates a buffer in the memory. It's like a file, but it's in memory and not on disk
        buffer = io.BytesIO()
        # Writes the DataFrame to the buffer in parquet format
        final_df.to_parquet(buffer, index=False)
        # To upload, we need to make sure that the buffer is at the beginning.
        buffer.seek(0)

        # Uploads the buffer to GCS
        output_blob.upload_from_file(buffer)
    except Exception as error:
        print(f'An error occurred: {error}')

def top_ctr_products(n: int = 20) -> None:
    """
    Generates a list of the top 20 products for each advertiser based on the CTR.

    Args:
        n (int): Number of top products to select for each advertiser.
    Returns:
        None
    """
    try:
        # Creates an authenticated client to Cloud Storage
        #client = get_authenticated_client()
        client = storage.Client()
        # Creates a bucker instance
        bucket = client.bucket(BUCKET_NAME)

        # Creates a blob instance to access the file in GCS
        blob = bucket.blob(f'temp/ads_views_{CURRENT_DATE}_filtered.parquet')
        # Downloads the file in memory as bytes
        content = blob.download_as_bytes()

        # Loads DataFrame
        df = pd.read_parquet(io.BytesIO(content))

        # Creates new columns
        df['is_impression'] = (df['type'] == 'impression').astype(int)
        df['is_click'] = (df['type'] == 'click').astype(int)

        # Groups by advertiser_id and product_id
        df_grouped = df.groupby(['advertiser_id', 'product_id']).agg({'is_impression': 'sum', 'is_click': 'sum'}).reset_index()
        df_grouped.rename(columns={'is_impression': 'impressions', 'is_click': 'clicks'}, inplace=True)

        # Calculates CTR
        df_grouped['ctr'] = df_grouped['clicks'] / df_grouped['impressions']

        final_df = pd.DataFrame(columns=['advertiser_id', 'product_id', 'date'])
        for (name, temp_df) in df_grouped.groupby('advertiser_id'):
            # df_temp = df_grouped.loc[df_grouped['advertiser_id'] == advertiser, :]
            df_temp = df_temp.sort_values('ctr', ascending=False).head(n)
            df_temp['date'] = CURRENT_DATE
            temp_df['ranking'] = np.arange(1, 21)
            df_temp = df_temp.loc[:, ['advertiser_id', 'product_id', 'ranking', 'date']]
            print(f'Added {len(df_temp)} products for {name}')
            final_df = pd.concat([final_df, df_temp], ignore_index=True)

        # Creates a blob file to upload the DataFrame to GCS
        output_blob = bucket.blob(f'temp/top_ctr_products_{CURRENT_DATE}.parquet')

        # Creates a buffer in the memory. It's like a file, but it's in memory and not on disk
        buffer = io.BytesIO()

        # Writes the DataFrame to the buffer in parquet format
        final_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Uploads the buffer to GCS
        output_blob.upload_from_file(buffer)
    except Exception as error:
        print(f'An error occurred: {error}')

def upload_to_sql(db_params: dict) -> None:
    """
    Uploads DataFrames to the SQLite database.

    Args:
        db_params (dict): Parameters needed to connect to the Database.
    Returns:
        None
    """
    try:
        # Creates an authenticated client to Cloud Storage
        #client = get_authenticated_client()
        client = storage.Client()
        # Creates a bucket instance
        bucket = client.bucket(BUCKET_NAME)

        #path = f'postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}'

        # Create a connection with the database
        conn = psycopg2.connect(
            dbname=db_params['database'],
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
            port=db_params['port']
        )
        cursor = conn.cursor()

        # Uploads DataFrames to SQL
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # Loads the TopProduct DataFrame from GCS
        blob_top_products = bucket.blob(f'temp/top_products_{current_date}.parquet')
        content_top_products = blob_top_products.download_as_bytes()
        df_top_products = pd.read_parquet(io.BytesIO(content_top_products))
        #df_top_products.to_sql('top_products', conn, if_exists='append', index=False)

        # Loads the TopCTRProduct DataFrame from GCS
        blob_ctr_products = bucket.blob(f'temp/top_ctr_products_{current_date}.parquet')
        content_ctr_products = blob_ctr_products.download_as_bytes()
        df_top_ctr_products = pd.read_parquet(io.BytesIO(content_ctr_products))
        #df_top_ctr_products.to_sql('top_ctr_products', conn, if_exists='append', index=False)
        for _, row in df_top_products.iterrows():
            cursor.execute("""
                INSERT INTO top_products (advertiser_id, product_id, ranking, date)
                VALUES (%s, %s, %s, %s)
            """, (row['advertiser_id'], row['product_id'], row['ranking'],row['date']))

        for _, row in df_top_ctr_products.iterrows():
            cursor.execute("""
                INSERT INTO top_ctr_products (advertiser_id, product_id, ranking, date)
                VALUES (%s, %s, %s, %s)
            """, (row['advertiser_id'], row['product_id'], row['ranking'],row['date']))
        
        
        conn.commit()

        print('SQL uploaded successfully')
    except Exception as error:
        print(f'An error occurred while uploading to SQL: {error}')

with DAG(
    dag_id='recommendation-pipeline',
    description='This pipeline is used to generate recommendations for users.',
    start_date=datetime.datetime(2025, 5, 2),
) as dag:
    data_filter = PythonOperator(
        task_id='FilterData',
        python_callable=filter_data
    )

    top_prod = PythonOperator(
        task_id='TopProducts',
        python_callable=top_products,
        op_kwargs={'n': 20}
    )

    top_ctr_prod = PythonOperator(
        task_id='TopCTR',
        python_callable=top_ctr_products,
        op_kwargs={'n': 20}
    )

    db_writing = PythonOperator(
        task_id='DBWriting',
        python_callable=upload_to_sql,
        op_kwargs={
            'db_params': {
                'database': 'postgres',
                'user': 'postgres',
                'password': 'postgres',
                'host': '34.173.90.191',
                'port': '5432'
            }
        }
    )

    data_filter.set_downstream([top_prod, top_ctr_prod])
    db_writing.set_upstream([top_prod, top_ctr_prod])
