import pandas as pd 
import numpy as np
import unicodedata
import logging
import boto3
import re

# --- Configurações de Ambiente --- 
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY = "admin12345"
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

# --- Configurações s3 para Pandas (s3fs) ---

storage_options = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

# --- Funções Auxiliares ---

def normalize_text (text):
    """
    Padroniza texto: minúsculo, sem acentos, remove caracteres especiais (/ - ,)
    e elimina siglas de estado residuais no final da string.
    """
    if text is None or pd.isna(text):
        return None
    
    # 1. Lowercase inicial
    text = str(text).lower()
    
    # 2. Remover acentos (Normalização Unicode)
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    
    # 3. LIMPEZA DE SEPARADORES: Substitui /, - ou , por espaço
    text = re.sub(r'[/,-]', ' ', text)
    
    # 4. REMOVER SIGLAS: remove um ou mais blocos de 'espaço+2letras' no fim
    text = re.sub(r'(\s+[a-z]{2})+$', '', text)
    
    # 5. REMOVER ESPAÇOS EXTRAS: Transforma múltiplos espaços em um só e faz o trim
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def save_to_minio(df, table_name):
    # Salva o dataframe processado na camada silver no formato parquet  
    output_path= f"s3://{SILVER_BUCKET}/{table_name}/{table_name}.parquet"
    print(f"Salvando {table_name} em: {output_path}")
    try:
        df.to_parquet(output_path, index=False, storage_options=storage_options)
        print("Sucesso!!!")

    except Exception as e:
        print(f" XXX Erro XXX: {e}")

# --- Consolidaçao da Geolocalização ---
def create_geo_reference():
    # Cria uma tabela com 1 linha por cep para limpeza de nomes de cidades
    print("\n Criando Referência de Geolocalização")
    path = f"s3://{BRONZE_BUCKET}/olist_geolocation/olist_geolocation.parquet"
    df  = pd.read_parquet(path, storage_options=storage_options)
    # Média para coordenadas e Moda para nomes (cidades e estados)
    agg_logic = {
        'geolocation_city': lambda x: x.value_counts().index[0] if not\
        x.empty else None,
        'geolocation_state' : lambda x: x.value_counts().index[0] if not\
        x.empty else None,
        'geolocation_lat':'mean',
        'geolocation_lng': 'mean'
    }
    df_geo = df.groupby('geolocation_zip_code_prefix').agg(agg_logic).reset_index()
    df_geo['geolocation_city_normalized'] = df_geo['geolocation_city'].apply(normalize_text)

    save_to_minio(df_geo, "olist_geolocation_ref")
    return df_geo

# --- Processamento por Tabela ---
def process_orders():
    # --- Orders ---
    print("\n --- Processando a Tabela Orders (Foco: ML Atraso) ---") 
    try:
       df= pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_orders/olist_orders.parquet",
                           storage_options=storage_options
                           )
       
       # Conversão de Datas
       date_cols=[
        'order_purchase_timestamp', 
        'order_approved_at', 
        'order_delivered_carrier_date', 
        'order_delivered_customer_date', 
        'order_estimated_delivery_date'
        ]
       for col in date_cols:
        df[col]=pd.to_datetime(df[col], errors="coerce")

        # Feature Engineering
        # Dias reais de entrega
        df['delivery_days'] = (df['order_delivered_customer_date'] 
                               - df['order_purchase_timestamp']).dt.days
        
        # Diferença da estimativa (Positivo = Atraso)
        df['delay_diff_days'] = (df['order_delivered_customer_date'] - 
                                 df['order_estimated_delivery_date']).dt.days

        # Flag de Atraso 
        df['is_delayed'] = np.where(df['delay_diff_days'] > 0, 1, 0) ### np.where(condição, valor_true, valor_false)

        # Ajuste: Se não foi entregue métricas de tempo ficam Nulas
        mask_not_delivered = df['order_delivered_customer_date'].isna()
        df.loc[mask_not_delivered, ['delivery_days', 'delay_diff_days']] = None

        save_to_minio(df, 'olist_orders')
    except Exception as e:
        print(f"Erro crítico em Orders: {e}")

def process_products():
    print("\n--- Processando a Tabela Products (foco: ML Frete e Catálogo) ---")
    try:
        df = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_products/olist_products.parquet", 
                             storage_options=storage_options
                             )
        # --- Categorias Nulas ---
        df['product_category_name'] = df['product_category_name'].fillna('outros').apply(normalize_text)

        # --- Aplicação de Mediana (Pesos/Dimensões Zerados ou Nulos) ---
        cols_dims = ['product_weight_g', 
                    'product_length_cm',
                    'product_height_cm',
                    'product_width_cm' 
                     ]
        for col in cols_dims:
            # Calcula mediana apenas dos valores válidos (>0)
            median_val = df[df[col] > 0 ][col].median() ### df[ condição ][ coluna ]
            # Preenche Nulos 
            df[col] = df[col].fillna(median_val)
            # Substitui Zeros
            df[col] = df[col].replace(0, median_val)

        # --- Volume cúbico (Feature ML) ---
        df['volume_cm3'] = df['product_length_cm'] *\
        df['product_height_cm'] * df['product_width_cm']

        save_to_minio(df, "olist_products")
    except Exception as e:
        print(f"Erro crítico em Products: {e}")

def process_customers_sellers(df_geo_ref):
    print("\n --- Processando Customers e Sellers (foco: Normalização Geo) ---")
    try:
        for entity in ['customers', 'sellers']:
            prefix = 'customer' if entity == 'customers' else 'seller'
            df = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_{entity}/olist_{entity}.parquet",
                                 storage_options=storage_options
                                 )
            # Join 
            df = pd.merge(
                df, 
                df_geo_ref,
                left_on=f'{prefix}_zip_code_prefix',
                right_on='geolocation_zip_code_prefix',
                how='left'
            )
            # Prioriza nome da cidade vindo de Geolocalization
            df['city_final'] = np.where(df['geolocation_city_normalized'].notna(),
                                        df['geolocation_city_normalized'],
                                        df[f'{prefix}_city'].apply(normalize_text)
                                        )
            # Campo composto para BI
            df['location_full'] = (
                df['city_final'].str.title() + ', ' +
            df[f'{prefix}_state'].str.upper() + ', Brazil'
            )
            save_to_minio(df, f'olist_{entity}')

    except Exception as e:
        print(f"Erro crítico em Customers & Sellers: {e}")

def process_items_payments():
    print("\n--- Processando Items e Payments (Foco: Finanças) ---")

    # --- Items ---
    try:
        df_items = pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_order_items/olist_order_items.parquet",
                                   storage_options=storage_options
                                   )
        df_items['price'] = pd.to_numeric(df_items['price'], errors='coerce')
        df_items['freight_value'] = pd.to_numeric(df_items['freight_value'], errors='coerce')
        # Valor Total do Item (BI: Receita Bruta)
        df_items['total_value'] = df_items['price'] + df_items['freight_value']
        save_to_minio(df_items, "olist_order_items")
    except Exception as e:
        print(f"Erro crítico em Items: {e}")

    # --- Payments --- 
    try:
        df_pay=pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_order_payments/olist_order_payments.parquet",
                               storage_options=storage_options
                               )
        # Remove qualquer pagamentos <= 0
        df_pay = df_pay[df_pay['payment_value'] > 0]

        save_to_minio(df_pay, "olist_order_payments")
    except Exception as e:
        print(f"Erro crítico em Payments: {e}")

def process_reviews():
    print("\n--- Processando a Tabela Reviews (Foco: NLP) ---")
    try:
        df_rev=pd.read_parquet(f"s3://{BRONZE_BUCKET}/olist_order_reviews/olist_order_reviews.parquet", 
                                storage_options=storage_options
                                )
        # --- Reviews ---
        # Limpeza de texto (remove quebras de linhas)
        if 'review_comment_message' in df_rev.columns: # exceção de segurança porque manipulação de texto livre é onde ocorrem mais erros inesperados
            df_rev['review_comment_message'] = df_rev['review_comment_message'].astype(str)\
            .str.replace(r'\n', ' ', regex=True).str.replace(r'\r', ' ', regex=True)
            # Voltar 'nan' para None 
            df_rev['review_comment_message'] = df_rev['review_comment_message'].replace(['nan', 'None'], None)
          
        # Feature: Tem comentário escrito ?
        df_rev['has_comment'] = np.where(df_rev['review_comment_message'].notnull(), 1, 0)
        save_to_minio(df_rev, 'olist_order_reviews')
    except Exception as e:
        print(f"Erro crítico em Reviews (talvez não ingerida ou com erro): {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("\n--- Iniciando Transformação Silver ---")

    # Cria Bucket Silver se não existir 
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY
                      )
    try:
        s3.head_bucket(Bucket=SILVER_BUCKET)
        print(f"Bucket {SILVER_BUCKET} econtrado.")
    except:
        print(f"Criando Bucket {SILVER_BUCKET}...")
        s3.create_bucket(Bucket=SILVER_BUCKET)
    
    geo_ref = create_geo_reference()

    # Executar pipeline completo
    process_orders()
    process_products()
    process_customers_sellers(geo_ref)
    process_items_payments()
    process_reviews()

    print("\n Processamento Silver Concluído!")
