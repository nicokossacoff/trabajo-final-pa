from fastapi import FastAPI
import uvicorn
import os

from datetime import datetime
app= FastAPI()
import psycopg2

def sql_query(query):

    engine = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="postgres",
    host="34.173.90.191",
    port='5432'
    )
    cursor = engine.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    engine.close()
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
     
    date= datetime.now().strftime("%Y-%m-%d") 

    query= f"""
    SELECT advertiser_id, product_id FROM {table} WHERE advertiser_id = '{ADV}' AND date = '{date}' and ranking <=20
    """
    rows=sql_query(query)

    recommendations_data = []
    for row in rows:
        recommendation = {
            "advertiser_id": row[0],
            "product_id": row[1]
        }
        recommendations_data.append(recommendation)
   
    return {"recommendations": recommendations_data}

@app.get("/history/{ADV}")
#get history of the las 7 days for adv
def get_history(ADV: str):
    query= f"""
    SELECT advertiser_id, product_id, date FROM top_products WHERE advertiser_id = '{ADV}' AND date >= CURRENT_DATE - INTERVAL '7 days'
    """
    print(query)
    rows=sql_query(query)
    history_data = []
    for row in rows:
        history = {
            "advertiser_id": row[0],
            "product_id": row[1],
            "date": row[2]
        }
        history_data.append(history)
    return {"history": history_data}

@app.get("/test")
def test():
    query=""" select * from top_products limit 10"""
    rows=sql_query(query)
    test_data = []
    for row in rows:
        test = {
            "advertiser_id": row[0],
            "product_id": row[1],
            "date": row[2]
        }
        test_data.append(test)
    return {"test": test_data}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")