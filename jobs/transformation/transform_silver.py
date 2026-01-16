import pandas as pd
import numpy as np
import unicodedata
import logging
import boto3
import re

# Configurações de Ambiente
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

storage_options = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

# Funções Auxiliares

def normalize_text(text):
    """Padroniza texto: minúsculo, sem acentos, remove caracteres especiais e siglas."""
    if text is None or pd.isna(text): return None
    text = str(text).lower()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    text = re.sub(r'[/,-]', ' ', text)
    text = re.sub(r'(\s+[a-z]{2})+$', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def save_to_minio(df, table_name):
    output_path = f"s3://{SILVER_BUCKET}/{table_name}/{table_name}.parquet"
    print(f"Salvando {table_name} na Silver...")
    try:
        df.to_parquet(output_path, index=False, storage_options=storage_options)
    except Exception as e:
        print(f"Erro ao salvar {table_name}: {e}")

# 1. Consolidação da Geolocalização (Referência Técnica)

def create_geo_reference():
    """Cria referência com 1 linha por CEP para saneamento de cidades."""
    print("Processando Referencia de Geolocalizacao (CEP unico)")
    path = f"s3://{BRONZE_BUCKET}/olist_geolocation/olist_geolocation.parquet"
    df = pd.read_parquet(path, storage_options=storage_options)
    
    # Garantia de tipo para o join
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    
    # Agregação por CEP: Média para coordenadas e Moda para nomes
    agg_logic = {
        'geolocation_city': lambda x: x.value_counts().index[0] if not x.empty else None,
        'geolocation_state': lambda x: x.value_counts().index[0] if not x.empty else None,
        'geolocation_lat': 'mean',
        'geolocation_lng': 'mean'
    }
    
    df_geo = df.groupby('geolocation_zip_code_prefix').agg(agg_logic).reset_index()
    df_geo['geolocation_city_normalized'] = df_geo['geolocation_city'].apply(normalize_text)
    
    save_to_minio(df_geo, "olist_geolocation_ref")
    return df_geo

# 2. Processamento por Tabela

def process_orders():
    print("Processando Orders (SLA e Atraso)")
    df = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_orders/olist_orders.parquet", storage_options=storage_options)
    
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
                 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df['delivery_days'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
    df['delay_diff_days'] = (df['order_delivered_customer_date'] - df['order_estimated_delivery_date']).dt.days
    df['is_delayed'] = np.where(df['delay_diff_days'] > 0, 1, 0)

    mask_not_delivered = df['order_delivered_customer_date'].isna()
    df.loc[mask_not_delivered, ['delivery_days', 'delay_diff_days']] = None

    save_to_minio(df, 'olist_orders')

def process_products():
    print("Processando Products (Mediana e Volume)")
    df = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_products/olist_products.parquet", storage_options=storage_options)
    
    df['product_category_name'] = df['product_category_name'].fillna('outros').apply(normalize_text)

    cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in cols_dims:
        median_val = df[df[col] > 0][col].median()
        df[col] = df[col].fillna(median_val).replace(0, median_val)

    df['volume_cm3'] = df['product_length_cm'] * df['product_height_cm'] * df['product_width_cm']
    save_to_minio(df, "olist_products")

def process_customers_sellers(df_geo_ref):
    """Enriquece entidades usando a referencia oficial de CEPs."""
    print("Processando Customers e Sellers (Enriquecimento Geografico)")

    for entity in ['customers', 'sellers']:
        prefix = 'customer' if entity == 'customers' else 'seller'
        df = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_{entity}/olist_{entity}.parquet", storage_options=storage_options)
        
        # Padronização da chave para o merge
        df[f'{prefix}_zip_code_prefix'] = df[f'{prefix}_zip_code_prefix'].astype(str).str.zfill(5)
        
        df = pd.merge(
            df,
            df_geo_ref,
            left_on=f'{prefix}_zip_code_prefix',
            right_on='geolocation_zip_code_prefix',
            how='left'
        )
        
        # Priorização da cidade saneada via Geolocation
        df['city_final'] = np.where(df['geolocation_city_normalized'].notna(),
                                     df['geolocation_city_normalized'],
                                     df[f'{prefix}_city'].apply(normalize_text))
        
        df['location_full'] = df['city_final'].str.title() + ", " + df[f'{prefix}_state'].str.upper() + ", Brazil"
        
        save_to_minio(df, f"olist_{entity}")

def process_items_payments():
    print("Processando Items e Payments")
    # Items
    df_items = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_order_items/olist_order_items.parquet", storage_options=storage_options)
    df_items['total_value'] = df_items['price'] + df_items['freight_value']
    save_to_minio(df_items, "olist_order_items")

    # Payments
    df_pay = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_order_payments/olist_order_payments.parquet", storage_options=storage_options)
    df_pay = df_pay[df_pay['payment_value'] > 0]
    save_to_minio(df_pay, "olist_order_payments")

def process_reviews():
    print("Processando Reviews")
    df_rev = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_order_reviews/olist_order_reviews.parquet", storage_options=storage_options)
    
    if 'review_comment_message' in df_rev.columns:
        df_rev['review_comment_message'] = df_rev['review_comment_message'].astype(str)\
            .str.replace(r'\n', ' ', regex=True).str.replace(r'\r', ' ', regex=True)\
            .replace(['nan', 'None'], None)
    
    df_rev['has_comment'] = np.where(df_rev['review_comment_message'].notnull(), 1, 0)
    save_to_minio(df_rev, 'olist_order_reviews')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Iniciando Transformacao Silver...")

    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
    try:
        s3.head_bucket(Bucket=SILVER_BUCKET)
    except:
        s3.create_bucket(Bucket=SILVER_BUCKET)

    geo_ref = create_geo_reference()

    process_orders()
    process_products()
    process_customers_sellers(geo_ref)
    process_items_payments()
    process_reviews()

    print("Processamento Silver Concluido.")
