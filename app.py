from fastapi import FastAPI
import sqlite3 
from datetime import datetime
app= FastAPI()

def sql_query(query):

    conn = sqlite3.connect('/home/javo/Documents/Maestria/MCD06/tp_airflow/airflow.db')
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    return rows

@app.get ("/")
def root():
    return {"message": "Hello World"}

@app.get ("/recommendations/{ADV}/{model}")
def get_recommendations(ADV: str, model: str):
        
    if model == 'topctr' :
        table= 'top_ctr_products'
    elif model == 'topproduct' :
        table= 'top_products'
    else:
        return {"error": "Invalid model specified. Use 'topctr' or 'topproduct'."}
     
    date= '2025-05-03' #date.today().strftime('%Y-%m-%d') 

    query= f"""
    SELECT advertiser_id, product_id FROM {table} WHERE advertiser_id = '{ADV}' AND date = '{date}'
    """
    rows=sql_query(query)

    recommendations_data = []
    for index, row in rows.iterrows():
        recommendation = {
            "advertiser_id": row['advertiser_id'],
            "product_id": row['product_id']
        }
        recommendations_data.append(recommendation)
   
    return {"recommendations": recommendations_data}

@app.get("/history/{ADV}")
#get history o f the las 7 days for adv
def get_history(ADV: str):
    query= f"""
    SELECT advertiser_id, product_id, date FROM top_products WHERE advertiser_id = '{ADV}' AND date >= date('now', '-7 days')
    """
    rows=sql_query(query)
    history_data = []
    for index, row in rows.iterrows():
        history = {
            "advertiser_id": row['advertiser_id'],
            "product_id": row['product_id'],
            "date": row['date']
        }
        history_data.append(history)
    return {"history": history_data}


