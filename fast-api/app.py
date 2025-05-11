from fastapi import FastAPI
import uvicorn
import os

from datetime import datetime
app= FastAPI()
import psycopg2
from credentials import Credentials
credentials = Credentials()

def sql_query(query):

    engine = psycopg2.connect(
        database=credentials.DATABASE,
        user=credentials.USER,
        password=credentials.PASSWORD,
        host=credentials.HOST,
        port=credentials.PORT
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

@app.get("/stats")
def get_recommendation_stats():
    """
    Devuelve estadísticas sobre las recomendaciones, incluyendo:
    - Cantidad de advertisers
    - Advertisers que más varían sus recomendaciones por día
    - Otros insights relevantes
    """
    stats = {}
    
    # 1. Cantidad total de advertisers
    query_total_advertisers = """
    SELECT COUNT(DISTINCT advertiser_id) FROM top_products
    """
    rows = sql_query(query_total_advertisers)
    stats["total_advertisers"] = rows[0][0]
    
    # 2. Top 10 advertisers con más productos recomendados
    query_most_products = """
    SELECT advertiser_id, COUNT(DISTINCT product_id) as product_count
    FROM top_products
    GROUP BY advertiser_id
    ORDER BY product_count DESC
    LIMIT 10
    """
    rows = sql_query(query_most_products)
    top_advertisers_by_products = []
    for row in rows:
        top_advertisers_by_products.append({
            "advertiser_id": row[0],
            "total_products": row[1]
        })
    stats["top_advertisers_by_products"] = top_advertisers_by_products
    
    # 3. Advertisers que más varían sus recomendaciones por día (versión simplificada)
    # Calculamos cuántos productos diferentes recomienda cada advertiser en los últimos 7 días
    query_variation = """
    SELECT 
        advertiser_id,
        COUNT(DISTINCT product_id) as total_unique_products,
        COUNT(DISTINCT date) as days_with_recommendations,
        COUNT(DISTINCT product_id) / COUNT(DISTINCT date) as product_variation_ratio
    FROM top_products
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY advertiser_id
    HAVING COUNT(DISTINCT date) > 0
    ORDER BY product_variation_ratio DESC
    LIMIT 10
    """
    
    rows = sql_query(query_variation)
    top_variation_advertisers = []
    for row in rows:
        top_variation_advertisers.append({
            "advertiser_id": row[0],
            "total_unique_products": row[1],
            "days_with_recommendations": row[2],
            "product_variation_ratio": round(float(row[3]), 2)
        })
    stats["top_variation_advertisers"] = top_variation_advertisers
    
    # 4. Estadísticas de los últimos 7 días
    query_last_week = """
    SELECT 
        date,
        COUNT(DISTINCT advertiser_id) as advertisers_count,
        COUNT(DISTINCT product_id) as products_count,
        COUNT(*) as total_recommendations
    FROM top_products
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY date
    ORDER BY date
    """
    rows = sql_query(query_last_week)
    daily_stats = []
    for row in rows:
        daily_stats.append({
            "date": row[0].strftime("%Y-%m-%d"),
            "advertisers_count": row[1],
            "products_count": row[2],
            "total_recommendations": row[3]
        })
    stats["daily_stats"] = daily_stats
    
    # 5. Comparativa entre modelos top_ctr_products y top_products
    query_model_comparison = """
    WITH top_products_stats AS (
        SELECT 
            'top_products' as model,
            COUNT(DISTINCT advertiser_id) as advertisers_count,
            COUNT(DISTINCT product_id) as products_count,
            COUNT(*) as total_recommendations
        FROM top_products
        WHERE date = CURRENT_DATE
    ),
    top_ctr_stats AS (
        SELECT 
            'top_ctr_products' as model,
            COUNT(DISTINCT advertiser_id) as advertisers_count,
            COUNT(DISTINCT product_id) as products_count,
            COUNT(*) as total_recommendations
        FROM top_ctr_products
        WHERE date = CURRENT_DATE
    )
    SELECT * FROM top_products_stats
    UNION ALL
    SELECT * FROM top_ctr_stats
    """
    rows = sql_query(query_model_comparison)
    model_comparison = []
    for row in rows:
        model_comparison.append({
            "model": row[0],
            "advertisers_count": row[1],
            "products_count": row[2],
            "total_recommendations": row[3]
        })
    stats["model_comparison"] = model_comparison
    
    return stats

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")