import pandas as pd
import numpy as np
import joblib
import json
import s3fs
from sqlalchemy import create_engine
from pathlib import Path

# --- 1. CONFIGURACOES E AMBIENTE ---
STORAGE_OPTIONS = {
    "key": "admin",
    "secret": "admin12345",
    "client_kwargs": {"endpoint_url": "http://minio:9000"},
    "use_listings_cache": False
}

DB_STR = "postgresql+psycopg2://postgres:root@postgres_dw:5432/dw"
MODEL_DIR = Path("/opt/airflow/jobs/ml/models/")

def haversine_distance(lat1, lon1, lat2, lon2):
    r = 6371 
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * r * np.arctan2(np.sqrt(a), np.sqrt(1-a))

def run_inference():
    print("Iniciando Inferencia de Machine Learning")
    
    try:
        model = joblib.load(MODEL_DIR / "logistic_model_v1.pkl")
        with open(MODEL_DIR / "route_risk.json", "r") as f: route_risk = json.load(f)
        with open(MODEL_DIR / "category_risk.json", "r") as f: category_risk = json.load(f)
        with open(MODEL_DIR / "model_config.json", "r") as f: config = json.load(f)
        
        features = config['features']
        threshold = config['best_threshold']
    except Exception as e:
        print(f"Erro ao carregar artefatos: {e}")
        return

    # Carga de dados
    df_orders = pd.read_parquet("s3://silver/olist_orders/olist_orders.parquet", storage_options=STORAGE_OPTIONS)
    df_ongoing = df_orders[df_orders['order_status'].isin(['shipped', 'processing', 'invoiced'])].copy()
    
    if df_ongoing.empty:
        print("Nenhum pedido em andamento.")
        return

    df_items = pd.read_parquet("s3://silver/olist_order_items/olist_order_items.parquet", storage_options=STORAGE_OPTIONS)
    df_prod = pd.read_parquet("s3://silver/olist_products/olist_products.parquet", storage_options=STORAGE_OPTIONS)
    df_cust = pd.read_parquet("s3://silver/olist_customers/olist_customers.parquet", storage_options=STORAGE_OPTIONS)
    df_sell = pd.read_parquet("s3://silver/olist_sellers/olist_sellers.parquet", storage_options=STORAGE_OPTIONS)

    # Merge inicial 
    df = df_ongoing.merge(df_items, on='order_id') \
                   .merge(df_prod, on='product_id') \
                   .merge(df_cust[['customer_id', 'geolocation_lat', 'geolocation_lng', 'customer_state']], on='customer_id') \
                   .merge(df_sell[['seller_id', 'geolocation_lat', 'geolocation_lng', 'seller_state']], 
                          on='seller_id', suffixes=('_cust', '_sell'))

    # Feature Engineering
    df['distancia_km'] = haversine_distance(df['geolocation_lat_cust'], df['geolocation_lng_cust'],
                                            df['geolocation_lat_sell'], df['geolocation_lng_sell'])
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'])
    
    agora = df_orders['order_purchase_timestamp'].max() + pd.Timedelta(days=1)
    df['handling_time_h'] = (df['order_delivered_carrier_date'] - df['order_approved_at']).dt.total_seconds() / 3600
    mask_pending = df['order_delivered_carrier_date'].isna()
    df.loc[mask_pending, 'handling_time_h'] = (agora - df['order_approved_at']).dt.total_seconds() / 3600
    df['handling_time_h'] = df['handling_time_h'].apply(lambda x: x if x >= 0 else 24).fillna(24)

    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['dia_semana_compra'] = df['order_purchase_timestamp'].dt.dayofweek
    df['densidade_prod'] = df['product_weight_g'] / (df['volume_cm3'] + 1)
    df['route_key'] = df['seller_state'] + "_" + df['customer_state']
    
    df['risk_route'] = df['route_key'].map(route_risk).fillna(0.07)
    df['risk_category'] = df['product_category_name'].map(category_risk).fillna(0.07)

    # Predição
    X = df[features].fillna(0)
    df['probabilidade_atraso'] = model.predict_proba(X)[:, 1]
    
    # --- DEDUPLICACAO  ---
    # Se um pedido tem 2 itens, pega a maior probabilidade de atraso 
    df_results = df.groupby('order_id').agg({
        'probabilidade_atraso': 'max'
    }).reset_index()
    
    df_results['alerta_atraso'] = (df_results['probabilidade_atraso'] >= threshold).astype(int)

    # Exportação
    engine = create_engine(DB_STR)
    try:
        df_results.to_sql('fato_previsoes_logistica', engine, if_exists='replace', index=False)
        print(f"Sucesso: {len(df_results)} pedidos unicos previstos.")
    except Exception as e:
        print(f"Erro ao salvar: {e}")

if __name__ == "__main__":
    run_inference()
