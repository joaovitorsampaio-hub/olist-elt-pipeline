# Olist ELT Pipeline: Data Engineering and Logistics Prediction

🇺🇸 This README is written in English.  
🇧🇷 Leia a versão em português: [README.md](README.md)

This project presents a **complete Data Engineering solution** using the public **Olist** dataset.  
The pipeline implements an **ELT (Extract, Load, Transform)** process following the **Medallion Architecture**, culminating in a **Machine Learning model** capable of predicting order delays in near real time.

---

## Architecture and Data Flow

The project is orchestrated with **Apache Airflow** and structured into the following layers:

### 🟤 Bronze (Raw)
- Ingestion of raw data from **MySQL**
- Persistence in **MinIO** using **Parquet** format
- Inclusion of ingestion metadata (`ingestion_date`)

---

### ⚪ Silver (Cleaned)
- **Geographic Cleansing**
  - Consolidation of the geolocation table
  - One record per ZIP code
  - Mean aggregation for coordinates (lat/lng) and mode for city names
- **Normalization**
  - String cleaning (removal of accents and special characters)
  - Handling of missing values (median imputation for product dimensions)
- **Feature Engineering**
  - SLA calculations:
    - delivery days
    - delay days
    - delay flag
  - Product volume calculation (`cm³`)

---

### 🟡 Gold (Curated)
- **Star Schema** modeling in **PostgreSQL (Data Warehouse)**
- Dimensions:
  - Calendar
  - Customers
  - Products
  - Sellers
- Fact Tables:
  - Sales
  - Payments
  - Reviews
  - Logistics Predictions
- Layer ready for **BI, Analytics, and Machine Learning**

<img width="632" height="805" alt="Screenshot 2026-01-19 170244" src="https://github.com/user-attachments/assets/e0bcbda8-1279-4830-807d-5fce96ad4118" />

---

## Technology Stack

- **Language:** Python  
  - Pandas  
  - SQLAlchemy  
  - Boto3  
  - Scikit-Learn  
- **Orchestration:** Apache Airflow  
- **Object Storage:** MinIO (S3 compatible)  
- **Data Warehouse:** PostgreSQL  
- **Source Database:** MySQL  
- **Infrastructure:** Docker and Docker Compose  

---

## Machine Learning: Logistics Intelligence

The pipeline integrates an **inference script** that consumes data from the **Silver** layer to predict the **risk of delay** for orders still in transit (`shipped`, `processing`, `invoiced`). The selected model is a **Random Forest**, trained with automatic class weighting to handle target imbalance. After training with a realistic handling time logic and decision threshold optimization, an optimal threshold of **0.5163** was defined, balancing precision and recall. The model achieved an overall accuracy of **0.89** and, for the critical class (delay), a recall of **0.39**, precision of **0.27**, and F1-score of **0.32**, demonstrating consistent performance as an early warning system despite strong class imbalance.

### Model Feature Engineering

- **Haversine Distance**
  - Mathematical calculation of the real distance between seller and buyer (lat/lng)
- **Handling Time**
  - Time between order approval and handoff to the carrier
- **Calculated Risks**
  - Historical risk by **route** (origin × destination)
  - Historical risk by **product category**
- **Logistics Density**
  - Weight-to-volume ratio to identify critical logistics profiles

### Model Outputs
- `delay_probability`
- `delay_alert` (based on optimized threshold)

Results are persisted directly into the **Gold layer**, ready for consumption by **BI** tools or operational applications.

---

## Data Visualization (Power BI)

The dashboard consumes tables from **PostgreSQL** and is organized into **four analytical pillars**:

### 1. Executive Performance
- GMV by year and month
- Average ticket
- Delay rate
- Total number of orders

<img width="1535" height="854" alt="Screenshot 2026-01-19 151302" src="https://github.com/user-attachments/assets/e8e2fac7-497f-4f98-82d8-f5862bf03cfa" />

### 2. Logistics Efficiency
- SLA monitoring by region
- Delay rate analysis over time (year and month)
- Identification of regions with higher delay incidence
- Evaluation of average freight cost by state
- Analysis of average delivery time by city
- Identification of orders at critical operational risk

<img width="1532" height="853" alt="Screenshot 2026-01-19 151027" src="https://github.com/user-attachments/assets/6e2d8685-ee91-43e5-a6c4-c4e4fb0316ee" />

### 3. Revenue Drivers
- Pareto analysis (cumulative %) to identify top-revenue categories
- Identification of strategic categories for growth and commercial optimization
- Customer Lifetime Value (LTV) calculation and ranking

<img width="1540" height="850" alt="Screenshot 2026-01-19 151041" src="https://github.com/user-attachments/assets/51c4898e-0a40-432f-9d98-44a4235a5c9e" />

### 4. Customer Satisfaction (Reviews)
- Overall average customer rating
- Distribution of review counts by score (1 to 5)
- Analysis of the relationship between delivery delays and customer ratings
- Correlation between average delay days and average rating
- Identification of product categories with poorer customer experience
- Qualitative exploration of customer comments to identify satisfaction and dissatisfaction patterns

<img width="1527" height="849" alt="Screenshot 2026-01-19 151059" src="https://github.com/user-attachments/assets/496b5dee-d0b9-40ab-b4e5-ec231486d4c6" />

---

## Project Objective

To demonstrate a **modern data architecture**, integrating:
- Data Engineering
- Analytical Modeling
- Machine Learning
- Business Intelligence

<img width="1873" height="339" alt="Screenshot 2026-01-19 153952" src="https://github.com/user-attachments/assets/19329209-71e9-43aa-9677-d9d7b231530f" />
