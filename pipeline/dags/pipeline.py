import datetime
import pandas as pd
import os
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def filter_data(path: str) -> None:
    """
    Filters files in the raw data to only include active advertisers.

    Args:
        path (str): Path to the directory containing the raw data files.
    Returns:
        None
    """
    try:
        # Extract active advertisers
        advertisers_df = pd.read_parquet(f'{path}/raw_data/advertiser_ids.parquet')
        active_advertisers = advertisers_df['advertiser_id'].tolist()

        # Gets the current date and the previous date
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        previous_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        # Filter files
        for file in os.listdir(f'{path}/raw_data/'):
            if file.endswith('views.parquet'):
                temp_df = pd.read_parquet(f'{path}/raw_data/{file}')
                temp_df['date'] = temp_df['date'].astype('datetime64[ns]')
                filtered_df = temp_df.loc[(temp_df['advertiser_id'].isin(active_advertisers)) & (temp_df['date'] == previous_date), :]
                filtered_df.to_parquet(f'{path}/temp/{file[:-8]}_{current_date}_filtered.parquet', index=False)
    except Exception as error:
        print(f'An error occurred: {error}')

def top_products(path: str, n: int = 20) -> None:
    """
    Generates a list of the top 20 products for each advertiser based on the number of views.

    Args:
        path (str): Path to the directory containing the filtered data files.
        n (int): Number of top products to select for each advertiser.
    Returns:
        None
    """
    try:
        # Gets the current date and the previous date
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        df = pd.read_parquet(f'{path}/temp/product_views_{current_date}_filtered.parquet')
        df_grouped = df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
        df_grouped = df_grouped.sort_values(by='views', ascending=False)

        final_df = pd.DataFrame(columns=['advertiser_id', 'product_id'])
        for (advertiser, temp_df) in df_grouped.groupby('advertiser_id'):
            temp_df = temp_df.sort_values(by='views', ascending=False).head(n)
            final_df = pd.concat([final_df, temp_df], ignore_index=True)

        final_df.to_parquet(f'{path}/temp/top_products_{current_date}.parquet', index=False)
    except Exception as error:
        print(f'An error occurred: {error}')

def top_ctr_products(path: str, n: int = 20) -> None:
    """
    Generates a list of the top 20 products for each advertiser based on the CTR.

    Args:
        path (str): Path to the directory containing the filtered data files.
        n (int): Number of top products to select for each advertiser.
    Returns:
        None
    """
    try:
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # Loads DataFrame
        df = pd.read_parquet(f'{path}/temp/ads_views_{current_date}_filtered.parquet')

        # Creates new columns
        df['is_impression'] = (df['type'] == 'impression').astype(int)
        df['is_click'] = (df['type'] == 'click').astype(int)

        # Groups by advertiser_id and product_id
        df_grouped = df.groupby(['advertiser_id', 'product_id']).agg({'is_impression': 'sum', 'is_click': 'sum'}).reset_index()
        df_grouped.rename(columns={'is_impression': 'impressions', 'is_click': 'clicks'}, inplace=True)

        # Calculates CTR
        df_grouped['ctr'] = df_grouped['clicks'] / df_grouped['impressions']

        final_df = pd.DataFrame(columns=['advertiser_id', 'product_id'])
        for advertiser in df_grouped['advertiser_id'].unique():
            df_temp = df_grouped.loc[df_grouped['advertiser_id'] == advertiser, :]
            df_temp = df_temp.sort_values('ctr', ascending=False).head(n)
            final_df = pd.concat([final_df, df_temp], ignore_index=True)

        final_df.to_parquet(f'{path}/temp/top_ctr_products_{current_date}.parquet', index=False)
    except Exception as error:
        print(f'An error occurred: {error}')

with DAG(
    dag_id='recommendation-pipeline',
    description='This pipeline is used to generate recommendations for users.',
    start_date=datetime.datetime(2025, 5, 2),
) as dag:
    data_filter = PythonOperator(
        task_id='FilterData',
        python_callable=filter_data,
        op_kwargs={'path': '/Users/nicolaskossacoff/Documents/Projects/trabajo-final-pa/data'},
    )

    top_prod = PythonOperator(
        task_id='TopProducts',
        python_callable=top_products,
        op_kwargs={'path': '/Users/nicolaskossacoff/Documents/Projects/trabajo-final-pa/data'},
    )

    top_ctr_prod = PythonOperator(
        task_id='TopCTR',
        python_callable=top_ctr_products,
        op_kwargs={'path': '/Users/nicolaskossacoff/Documents/Projects/trabajo-final-pa/data'},
    )

    data_filter.set_downstream([top_prod, top_ctr_prod])