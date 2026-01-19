
-- SCRIPT DE ESTRUTURA DO BANCO DE ORIGEM (MySQL) - source_db


CREATE DATABASE IF NOT EXISTS source_db;
USE source_db;


-- Clientes
CREATE TABLE IF NOT EXISTS olist_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2),
    INDEX idx_cust_zip (customer_zip_code_prefix)
);

-- Produtos
CREATE TABLE IF NOT EXISTS olist_products (
    product_id VARCHAR(100) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- Vendedores
CREATE TABLE IF NOT EXISTS olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5),
    INDEX idx_sell_zip (seller_zip_code_prefix)
);

-- Geolocalização
CREATE TABLE IF NOT EXISTS olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat DOUBLE,
    geolocation_lng DOUBLE,
    geolocation_city VARCHAR(255),
    geolocation_state CHAR(2),
    INDEX idx_geo_zip (geolocation_zip_code_prefix)
);


-- Pedidos (Orders)
CREATE TABLE IF NOT EXISTS olist_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES olist_customers(customer_id)
);

-- Itens do Pedido
CREATE TABLE IF NOT EXISTS olist_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    PRIMARY KEY (order_id, order_item_id),
    INDEX idx_items_product (product_id),
    INDEX idx_items_seller (seller_id),
    CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES olist_orders(order_id),
    CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES olist_products(product_id),
    CONSTRAINT fk_items_seller FOREIGN KEY (seller_id) REFERENCES olist_sellers(seller_id)
);

-- Pagamentos
CREATE TABLE IF NOT EXISTS olist_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    PRIMARY KEY (order_id, payment_sequential),
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES olist_orders(order_id)
);

-- Avaliações (Reviews)
CREATE TABLE IF NOT EXISTS olist_order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    PRIMARY KEY (review_id, order_id),
    INDEX idx_reviews_order (order_id),
    CONSTRAINT fk_reviews_order FOREIGN KEY (order_id) REFERENCES olist_orders(order_id)
);
