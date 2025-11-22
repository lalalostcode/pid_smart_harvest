# 🌾 Smart Harvest Prediction System
## Complete End-to-End Data Engineering & Machine Learning Pipeline

> **SDG 2: Zero Hunger** - Agricultural Yield Prediction using Data Warehouse, Machine Learning & Real-time Streaming

[![MySQL](https://img.shields.io/badge/MySQL-8.0-blue.svg)](https://www.mysql.com/)
[![Python](https://img.shields.io/badge/Python-3.10-green.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-black.svg)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.1-orange.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [System Architecture](#-system-architecture)
3. [Data Warehouse Design](#-data-warehouse-design)
4. [Technology Stack](#-technology-stack)
5. [Installation & Setup](#-installation--setup)
6. [Complete Pipeline Guide](#-complete-pipeline-guide)
7. [Real-time Streaming](#-real-time-streaming-with-kafka)
8. [Web Interfaces](#-web-interfaces)
9. [Project Structure](#-project-structure)
10. [Troubleshooting](#-troubleshooting)

---

## 🎯 Project Overview

### What is Smart Harvest?

Smart Harvest adalah sistem prediksi hasil panen berbasis **Data Engineering** dan **Machine Learning** yang mengintegrasikan:

- **Data Warehouse** dengan Star Schema design
- **ETL Pipeline** untuk data preparation
- **Machine Learning** untuk prediksi produksi
- **Real-time Streaming** dengan Apache Kafka
- **Workflow Orchestration** dengan Apache Airflow
- **Data Visualization** dengan Grafana

### Key Features

✅ **Star Schema Data Warehouse** - Proper dimensional modeling  
✅ **Automated ETL Pipeline** - From raw CSV to clean data  
✅ **ML-based Prediction** - 7 commodity models (padi, jagung, dll)  
✅ **Real-time Streaming** - Kafka producer & consumer  
✅ **Workflow Orchestration** - Airflow DAG automation  
✅ **Interactive Dashboards** - phpMyAdmin & Grafana  

### Business Problem

**Challenge:** Petani kesulitan memprediksi hasil panen karena ketidakpastian cuaca.

**Solution:** Sistem prediksi berbasis ML yang menganalisis:
- Data cuaca historis (suhu, curah hujan, kelembaban)
- Data produksi historis (2010-2018)
- Pattern seasonal & regional

**Impact:** Membantu petani & pemerintah dalam:
- Perencanaan tanam
- Estimasi hasil panen
- Manajemen stok pangan
- Ketahanan pangan nasional

---

## 🏗️ System Architecture

### Complete Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         RAW DATA SOURCES                                │
├─────────────────────────────────────────────────────────────────────────┤
│  📁 Climate Data (2010-2020)     📁 Harvest Data (2010-2015)           │
│     - 35MB CSV                      - Yearly production data            │
│     - Daily weather records         - 7 commodities                     │
│     - 38 provinces                  - Province-level aggregation        │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                    PHASE 1: DATA PREPARATION (ETL)                      │
├─────────────────────────────────────────────────────────────────────────┤
│  🔧 clean_weather.py                                                    │
│     → Merge weather + station mapping                                   │
│     → Clean missing values                                              │
│     → Output: weather_cleaned.csv (51MB)                                │
│                                                                          │
│  🔧 clean_harvest.py                                                    │
│     → Combine yearly files (2010-2015)                                  │
│     → Standardize province names                                        │
│     → Output: harvest_cleaned.csv                                       │
│                                                                          │
│  🔧 generate_dummy_harvest.py                                           │
│     → Extrapolate 2016-2022 data                                        │
│     → Output: harvest_extrapolated.csv                                  │
│                                                                          │
│  🔧 disaggregate_harvest.py                                             │
│     → Convert yearly → monthly data                                     │
│     → Output: harvest_monthly_final.csv (5,928 rows)                    │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│              PHASE 2: DATA WAREHOUSE POPULATION                         │
├─────────────────────────────────────────────────────────────────────────┤
│  🗄️ MySQL Data Warehouse (harvest_dw)                                  │
│                                                                          │
│  DIMENSION TABLES:                                                      │
│  ├─ dim_province (38 rows)      - Master provinsi                      │
│  ├─ dim_time (192 rows)         - Time dimension 2010-2025             │
│  └─ dim_commodity (7 rows)      - Komoditas pangan                     │
│                                                                          │
│  FACT TABLES:                                                           │
│  ├─ fact_production_monthly (41,496 rows)  - Historical production     │
│  ├─ fact_weather_monthly (4,305 rows)      - Weather aggregated        │
│  └─ fact_crop_prediction (30,135+ rows)    - ML predictions            │
│                                                                          │
│  📊 populate_data_warehouse.py                                          │
│     → Create Star Schema                                                │
│     → Load dimensions                                                   │
│     → Load facts                                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                PHASE 3: MACHINE LEARNING (BATCH)                        │
├─────────────────────────────────────────────────────────────────────────┤
│  🤖 ml_train_predict_simple.py                                          │
│                                                                          │
│  TRAINING DATA: 2010-2018 (9 years, 3,513 rows/commodity)              │
│  TESTING DATA:  2019-2020 (2 years, 792 rows/commodity)                │
│                                                                          │
│  MODELS TRAINED: 7 commodities                                          │
│  ├─ padi          (R² = 0.1048)                                        │
│  ├─ jagung        (R² = 0.1279)                                        │
│  ├─ kedelai       (R² = 0.1137)                                        │
│  ├─ kacang_tanah  (R² = 0.1140)                                        │
│  ├─ kacang_hijau  (R² = 0.1060)                                        │
│  ├─ ubi_kayu      (R² = 0.0320)                                        │
│  └─ ubi_jalar     (R² = 0.3275) ⭐ Best!                               │
│                                                                          │
│  OUTPUT:                                                                │
│  ├─ 7 model files (.pkl)                                               │
│  └─ 30,135 predictions → MySQL                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│           PHASE 4: REAL-TIME STREAMING (OPTIONAL)                       │
├─────────────────────────────────────────────────────────────────────────┤
│  📡 Apache Kafka Streaming Pipeline                                     │
│                                                                          │
│  kafka_producer_stream.py                                               │
│     → Load test data (2019-2020)                                        │
│     → Stream to Kafka (100 records/sec)                                 │
│     → Topic: "harvest-weather-stream"                                   │
│                    ↓                                                    │
│  kafka_consumer_predict.py                                              │
│     → Consume from Kafka                                                │
│     → Load ML models                                                    │
│     → Real-time prediction                                              │
│     → Save to MySQL (batch 100)                                         │
│                                                                          │
│  RESULT: 5,544 streaming predictions                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                  PHASE 5: VISUALIZATION & ANALYTICS                     │
├─────────────────────────────────────────────────────────────────────────┤
│  🌐 Web Interfaces:                                                     │
│  ├─ phpMyAdmin (localhost:9090)    - Database management               │
│  ├─ Grafana (localhost:3000)       - Real-time dashboards              │
│  └─ Airflow (localhost:8085)       - Workflow monitoring               │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Diagram

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│   CSV    │────▶│   ETL    │────▶│   Data   │────▶│    ML    │
│  Files   │     │ Scripts  │     │Warehouse │     │ Training │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
                                                          │
                                                          ▼
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Grafana  │◀────│  MySQL   │◀────│  Kafka   │◀────│  Models  │
│Dashboard │     │  (DW)    │     │ Consumer │     │  (.pkl)  │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
                                        ▲
                                        │
                                  ┌──────────┐
                                  │  Kafka   │
                                  │ Producer │
                                  └──────────┘
```

---

## 🗄️ Data Warehouse Design

### Star Schema Architecture

```
                    ┌─────────────────┐
                    │  dim_province   │
                    ├─────────────────┤
                    │ province_id PK  │
                    │ province_name   │
                    └────────┬────────┘
                             │
                             │ 1:N
                             ▼
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   dim_time      │    │ fact_production_     │    │ dim_commodity   │
├─────────────────┤    │     monthly          │    ├─────────────────┤
│ time_id PK      │◀───┤──────────────────────┤───▶│ commodity_id PK │
│ year            │    │ production_id PK     │    │ commodity_name  │
│ month           │    │ time_id FK           │    │ commodity_type  │
│ month_name      │    │ province_id FK       │    └─────────────────┘
│ quarter         │    │ commodity_id FK      │
│ season          │    │ production_ton       │
└─────────────────┘    └──────────────────────┘
                             │
                             │ 1:1
                             ▼
                    ┌──────────────────────┐
                    │ fact_weather_        │
                    │     monthly          │
                    ├──────────────────────┤
                    │ weather_id PK        │
                    │ time_id FK           │
                    │ province_id FK       │
                    │ total_rainfall_mm    │
                    │ avg_temperature_c    │
                    │ avg_humidity_pct     │
                    └──────────────────────┘
                             │
                             │ 1:1
                             ▼
                    ┌──────────────────────┐
                    │ fact_crop_           │
                    │   prediction         │
                    ├──────────────────────┤
                    │ prediction_id PK     │
                    │ time_id FK           │
                    │ province_id FK       │
                    │ commodity_id FK      │
                    │ predicted_ton        │
                    │ model_name           │
                    │ confidence_score     │
                    │ created_at           │
                    └──────────────────────┘
```

### Database Schema Details

#### **Dimension Tables**

**1. dim_province** (38 rows)
```sql
province_id    INT PRIMARY KEY AUTO_INCREMENT
province_name  VARCHAR(100) UNIQUE
created_at     TIMESTAMP
```
*Example:* ACEH, BALI, DKI JAKARTA, JAWA BARAT, ...

**2. dim_time** (192 rows: 2010-2025, 12 months each)
```sql
time_id        INT PRIMARY KEY  -- Format: YYYYMM (e.g., 201501)
year           INT
month          INT
month_name     VARCHAR(20)      -- Januari, Februari, ...
quarter        INT              -- 1, 2, 3, 4
season         VARCHAR(20)      -- Kemarau (Apr-Sep) / Hujan (Oct-Mar)
created_at     TIMESTAMP
```

**3. dim_commodity** (7 rows)
```sql
commodity_id   INT PRIMARY KEY AUTO_INCREMENT
commodity_name VARCHAR(50) UNIQUE
commodity_type VARCHAR(50)
created_at     TIMESTAMP
```
*Commodities:*
- padi (Pangan Utama)
- jagung (Pangan Utama)
- kedelai (Palawija)
- kacang_tanah (Palawija)
- kacang_hijau (Palawija)
- ubi_kayu (Umbi-umbian)
- ubi_jalar (Umbi-umbian)

#### **Fact Tables**

**1. fact_production_monthly** (41,496 rows)
```sql
production_id  INT PRIMARY KEY AUTO_INCREMENT
time_id        INT FK → dim_time
province_id    INT FK → dim_province
commodity_id   INT FK → dim_commodity
production_ton DECIMAL(12,2)
created_at     TIMESTAMP

UNIQUE (time_id, province_id, commodity_id)
```
*Calculation:* 38 provinces × 13 years × 12 months × 7 commodities = 41,496 rows

**2. fact_weather_monthly** (4,305 rows)
```sql
weather_id           INT PRIMARY KEY AUTO_INCREMENT
time_id              INT FK → dim_time
province_id          INT FK → dim_province
total_rainfall_mm    DECIMAL(10,2)
avg_temperature_c    DECIMAL(5,2)
avg_humidity_pct     DECIMAL(5,2)
created_at           TIMESTAMP

UNIQUE (time_id, province_id)
```
*Note:* Aggregated from daily weather data

**3. fact_crop_prediction** (30,135+ rows)
```sql
prediction_id    INT PRIMARY KEY AUTO_INCREMENT
time_id          INT FK → dim_time
province_id      INT FK → dim_province
commodity_id     INT FK → dim_commodity
predicted_ton    DECIMAL(12,2)
model_name       VARCHAR(100)  -- e.g., "LinearRegression_padi_v1"
confidence_score DECIMAL(5,4)  -- R² score
created_at       TIMESTAMP
```

#### **Analytical Views**

**vw_production_detail**
```sql
SELECT 
    p.production_id,
    t.year, t.month, t.month_name, t.season,
    prov.province_name,
    c.commodity_name, c.commodity_type,
    p.production_ton,
    p.created_at
FROM fact_production_monthly p
JOIN dim_time t ON p.time_id = t.time_id
JOIN dim_province prov ON p.province_id = prov.province_id
JOIN dim_commodity c ON p.commodity_id = c.commodity_id
```

**vw_prediction_detail**
```sql
SELECT 
    pred.prediction_id,
    t.year, t.month, t.month_name, t.season,
    prov.province_name,
    c.commodity_name,
    pred.predicted_ton,
    pred.model_name,
    pred.confidence_score,
    pred.created_at
FROM fact_crop_prediction pred
JOIN dim_time t ON pred.time_id = t.time_id
JOIN dim_province prov ON pred.province_id = prov.province_id
JOIN dim_commodity c ON pred.commodity_id = c.commodity_id
```

---

## 🛠️ Technology Stack

### Infrastructure Layer
- **Docker Compose** - Container orchestration
- **MySQL 8.0** - Relational database (Data Warehouse)
- **Apache Kafka 7.4.0** - Message broker for streaming
- **Zookeeper** - Kafka coordination service
- **Apache Airflow 2.7.1** - Workflow orchestration

### Data Processing Layer
- **Python 3.10** - Primary programming language
- **Pandas 2.0** - Data manipulation & analysis
- **NumPy 1.24** - Numerical computing

### Machine Learning Layer
- **scikit-learn 1.3** - ML algorithms
  - Linear Regression (current)
  - Extensible to Random Forest, XGBoost, etc.

### Streaming Layer
- **kafka-python 2.0** - Python Kafka client
- **Producer-Consumer** pattern

### Visualization Layer
- **phpMyAdmin** - Database management UI
- **Grafana** - Real-time dashboards & analytics

### Orchestration Layer
- **Apache Airflow** - DAG-based workflow automation

---

## 🚀 Installation & Setup

### Prerequisites

**System Requirements:**
- OS: Linux / macOS / Windows (WSL2)
- RAM: 8GB minimum (16GB recommended)
- Disk: 20GB free space
- Docker: 20.10+
- Docker Compose: 2.0+
- Python: 3.10+

**Check Prerequisites:**
```bash
docker --version
docker compose version
python3 --version
```

### Step 1: Clone & Navigate

```bash
cd /home/lalalostnux/PemrosesaInfrastrukturData/DE_SmartHarvest
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- pandas
- numpy
- scikit-learn
- mysql-connector-python
- kafka-python

### Step 3: Start Infrastructure

```bash
# Start all services
docker compose up -d

# Verify services are running
docker compose ps
```

**Expected Services:**
```
NAME                    STATUS          PORTS
mysql_warehouse         Up (healthy)    3306
phpmyadmin              Up              9090
kafka                   Up              9092
zookeeper               Up              2181
airflow-webserver       Up (healthy)    8085
airflow-scheduler       Up
grafana                 Up              3000
postgres_airflow        Up (healthy)    5432
```

### Step 4: Wait for Services

```bash
# Wait ~2 minutes for all services to be healthy
sleep 120

# Check Airflow is ready
curl http://localhost:8085/health
```

---

## 📊 Complete Pipeline Guide

### PHASE 1: Data Preparation (ETL)

#### Step 1.1: Clean Weather Data

```bash
python3 scripts/clean_weather.py
```

**What it does:**
- Loads `climate_data.csv` (35MB, 589K rows)
- Merges with station → province mapping
- Cleans missing values
- Converts date formats
- **Output:** `dataset/processed/weather_cleaned.csv` (51MB)

**Duration:** ~30-60 seconds

#### Step 1.2: Clean Harvest Data

```bash
python3 scripts/clean_harvest.py
```

**What it does:**
- Combines yearly files (2010-2015)
- Standardizes province names (UPPERCASE)
- Handles missing values
- **Output:** `dataset/processed/harvest_cleaned.csv`

**Duration:** ~5 seconds

#### Step 1.3: Generate Dummy Data

```bash
python3 scripts/generate_dummy_harvest.py
```

**What it does:**
- Extrapolates 2016-2022 data from 2010-2015 trends
- Uses linear interpolation
- **Output:** `dataset/processed/harvest_extrapolated.csv`

**Duration:** ~10 seconds

#### Step 1.4: Disaggregate to Monthly

```bash
python3 scripts/disaggregate_harvest.py
```

**What it does:**
- Converts yearly → monthly data
- Distributes production across 12 months
- **Output:** `dataset/processed/harvest_monthly_final.csv` (5,928 rows)

**Duration:** ~5 seconds

**Verify Processed Data:**
```bash
ls -lh dataset/processed/
# Should see:
# - weather_cleaned.csv (51MB)
# - harvest_monthly_final.csv (297KB)
```

---

### PHASE 2: Data Warehouse Population

#### Step 2.1: Populate Data Warehouse

```bash
python3 scripts/populate_data_warehouse.py
```

**What it does:**

**[1/6] Create Database & Schema**
- Creates database `harvest_dw`
- Creates 3 dimension tables
- Creates 3 fact tables
- Creates 3 analytical views

**[2/6] Populate dim_time**
- Generates 192 time records (2010-2025, monthly)
- Calculates season (Kemarau/Hujan)
- Calculates quarter

**[3/6] Populate dim_province**
- Extracts unique provinces from harvest data
- Inserts 38 provinces

**[4/6] Populate dim_commodity**
- Pre-populated with 7 commodities

**[5/6] Populate fact_production_monthly**
- Loads from `harvest_monthly_final.csv`
- Unpivots commodities (wide → long format)
- Inserts 41,496 rows

**[6/6] Populate fact_weather_monthly**
- Loads from `weather_cleaned.csv`
- Aggregates daily → monthly
- Inserts 4,305 rows

**Duration:** ~2-3 minutes

**Output:**
```
============================================================
DATA WAREHOUSE POPULATION COMPLETE!
============================================================

📊 DIMENSION TABLES:
   - dim_province: 38 rows
   - dim_time: 192 rows
   - dim_commodity: 7 rows

📈 FACT TABLES:
   - fact_production_monthly: 41496 rows
   - fact_weather_monthly: 4305 rows
   - fact_crop_prediction: 0 rows (will be populated by ML)

🌐 ACCESS DATA WAREHOUSE:
   phpMyAdmin: http://localhost:9090
   Database: harvest_dw
   Login: root / root

✅ Ready for ML training and predictions!
============================================================
```

#### Step 2.2: Verify Data Warehouse

```bash
# Check tables
docker exec mysql_warehouse mysql -uroot -proot -e "
USE harvest_dw;
SHOW TABLES;
"

# Check row counts
docker exec mysql_warehouse mysql -uroot -proot -e "
USE harvest_dw;
SELECT 
    'dim_province' as table_name, COUNT(*) as rows FROM dim_province
UNION ALL SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL SELECT 'dim_commodity', COUNT(*) FROM dim_commodity
UNION ALL SELECT 'fact_production_monthly', COUNT(*) FROM fact_production_monthly
UNION ALL SELECT 'fact_weather_monthly', COUNT(*) FROM fact_weather_monthly;
"
```

---

### PHASE 3: Machine Learning Training

#### Step 3.1: Train Models & Generate Predictions

```bash
python3 src/ml_train_predict_simple.py
```

**What it does:**

**[1/5] Load Data from MySQL**
- Queries all 7 commodities
- Joins production + weather data
- Loads 4,305 records per commodity

**[2/5] Prepare Data (Time-based Split)**
- **Training:** 2010-2018 (9 years, 3,513 rows)
- **Testing:** 2019-2020 (2 years, 792 rows)
- Features: temperature, rainfall, humidity
- Target: production_ton

**[3/5] Train Models**
- Algorithm: Linear Regression
- Trains 7 separate models (one per commodity)
- Fits on training data

**[4/5] Evaluate Models**
- Predicts on test data
- Calculates RMSE & R²
- Displays performance metrics

**[5/5] Generate & Save Predictions**
- Predicts for ALL data (2010-2020)
- Saves to `fact_crop_prediction`
- Saves models to `models/*.pkl`

**Duration:** ~30-60 seconds

**Output:**
```
============================================================
ML TRAINING & PREDICTION COMPLETE!
============================================================

📊 MODELS TRAINED: 7 commodities

------------------------------------------------------------
Commodity            Records    RMSE            R²        
------------------------------------------------------------
padi                 4305         325,189.67   0.1048
jagung               4305         114,867.22   0.1279
kedelai              4305           6,360.25   0.1137
kacang_tanah         4305           3,817.57   0.1140
kacang_hijau         4305           2,042.49   0.1060
ubi_kayu             4305         161,735.77   0.0320
ubi_jalar            4305          10,098.19   0.3275
------------------------------------------------------------

💾 PREDICTIONS SAVED:
   - Database: harvest_dw
   - Table: fact_crop_prediction
   - Total predictions: 30,135
   - Commodities: 7

📁 MODELS SAVED:
   - models/linear_regression_padi_v1.pkl
   - models/linear_regression_jagung_v1.pkl
   - models/linear_regression_kedelai_v1.pkl
   - models/linear_regression_kacang_tanah_v1.pkl
   - models/linear_regression_kacang_hijau_v1.pkl
   - models/linear_regression_ubi_kayu_v1.pkl
   - models/linear_regression_ubi_jalar_v1.pkl

🌐 VIEW RESULTS:
   phpMyAdmin: http://localhost:9090
   Database: harvest_dw
============================================================
```

#### Step 3.2: Verify Predictions

```bash
# Check prediction count
docker exec mysql_warehouse mysql -uroot -proot -e "
USE harvest_dw;
SELECT COUNT(*) as total_predictions FROM fact_crop_prediction;
"

# View sample predictions
docker exec mysql_warehouse mysql -uroot -proot -e "
USE harvest_dw;
SELECT * FROM vw_prediction_detail LIMIT 10;
"
```

---

### PHASE 4: Real-time Streaming (Optional)

#### Step 4.1: Start Kafka Consumer (Terminal 1)

```bash
python3 src/kafka_consumer_predict.py
```

**What it does:**
- Loads all 7 ML models
- Connects to Kafka topic `harvest-weather-stream`
- Waits for messages
- On message received:
  - Extracts features
  - Loads appropriate model
  - Makes prediction
  - Saves to MySQL (batch 100)

**Output:**
```
============================================================
KAFKA CONSUMER - REAL-TIME PREDICTION
============================================================

[1/4] Loading ML models...
   ✅ Loaded model for padi
   ✅ Loaded model for jagung
   ✅ Loaded model for kedelai
   ✅ Loaded model for kacang_tanah
   ✅ Loaded model for kacang_hijau
   ✅ Loaded model for ubi_kayu
   ✅ Loaded model for ubi_jalar

   Total models loaded: 7

[2/4] Connecting to MySQL...
   ✅ Connected to MySQL
   ✅ Cleared old streaming predictions

[3/4] Connecting to Kafka consumer...
   ✅ Subscribed to topic: harvest-weather-stream

[4/4] Consuming messages and making predictions...
   Press Ctrl+C to stop

   (waiting for messages...)
```

#### Step 4.2: Start Kafka Producer (Terminal 2)

```bash
python3 src/kafka_producer_stream.py
```

**What it does:**
- Loads test data (2019-2020) from MySQL
- Streams 5,544 records to Kafka
- Rate: 100 records/second (10ms delay)
- Topic: `harvest-weather-stream`

**Output:**
```
============================================================
KAFKA PRODUCER - STREAMING TEST DATA
============================================================

[1/3] Connecting to MySQL...
   Loading test data (2019-2020)...
   ✅ Loaded 5544 records to stream
   Year range: 2019 - 2020

[2/3] Connecting to Kafka...
   ✅ Connected to Kafka broker: localhost:9092

[3/3] Streaming 5544 records to topic 'harvest-weather-stream'...
   Press Ctrl+C to stop

   Streamed 100/5544 records... (1.8%)
   Streamed 200/5544 records... (3.6%)
   Streamed 300/5544 records... (5.4%)
   ...
```

#### Step 4.3: Watch Real-time Predictions (Terminal 1)

Consumer will show:
```
   ✅ Processed 100 predictions | Latest: 2019-01 | ACEH | padi
   ✅ Processed 200 predictions | Latest: 2019-01 | BALI | jagung
   ✅ Processed 300 predictions | Latest: 2019-02 | DKI JAKARTA | kedelai
   ✅ Processed 400 predictions | Latest: 2019-02 | JAWA BARAT | padi
   ...
```

#### Step 4.4: Verify Streaming Results

```bash
# Check streaming predictions
docker exec mysql_warehouse mysql -uroot -proot -e "
USE harvest_dw;
SELECT 
    model_name,
    COUNT(*) as predictions,
    MIN(created_at) as first_prediction,
    MAX(created_at) as last_prediction
FROM fact_crop_prediction
WHERE model_name LIKE '%_streaming'
GROUP BY model_name;
"
```

---

### PHASE 5: Workflow Automation (Airflow)

#### Step 5.1: Access Airflow UI

Open browser: **http://localhost:8085**

**Login:**
- Username: `airflow`
- Password: `airflow`

#### Step 5.2: Enable & Trigger DAG

1. Find DAG: `smart_harvest_complete_pipeline`
2. Toggle switch to **ON**
3. Click **Trigger DAG** (play button ▶️)

**DAG Tasks:**
```
1. clean_weather_data
2. clean_harvest_data
3. generate_dummy_harvest
4. disaggregate_to_monthly
5. populate_data_warehouse
6. ml_train_and_predict
```

#### Step 5.3: Monitor Execution

- Click on DAG name
- View **Graph** or **Grid** view
- Check task status:
  - 🟢 Green = Success
  - 🔴 Red = Failed
  - 🟡 Yellow = Running
  - ⚪ White = Queued

---

## 🌐 Web Interfaces

### 1. phpMyAdmin - Database Management

**URL:** http://localhost:9090  
**Login:** root / root

**Features:**
- Browse all tables
- Run SQL queries
- Export data
- View table structures

**Sample Queries:**

```sql
-- Top 10 provinces by rice production (2020)
SELECT 
    province_name,
    SUM(production_ton) as total_production
FROM vw_production_detail
WHERE commodity_name = 'padi' AND year = 2020
GROUP BY province_name
ORDER BY total_production DESC
LIMIT 10;

-- Compare actual vs predicted (2019-2020)
SELECT 
    p.province_name,
    p.year,
    p.commodity_name,
    AVG(prod.production_ton) as actual_avg,
    AVG(p.predicted_ton) as predicted_avg,
    AVG(ABS(prod.production_ton - p.predicted_ton)) as avg_error
FROM vw_prediction_detail p
JOIN fact_production_monthly prod 
    ON p.time_id = prod.time_id 
    AND p.province_id = prod.province_id
    AND p.commodity_id = prod.commodity_id
WHERE p.year >= 2019
GROUP BY p.province_name, p.year, p.commodity_name
ORDER BY avg_error DESC
LIMIT 20;

-- Weather patterns by season
SELECT 
    season,
    ROUND(AVG(total_rainfall_mm), 2) as avg_rainfall,
    ROUND(AVG(avg_temperature_c), 2) as avg_temp,
    ROUND(AVG(avg_humidity_pct), 2) as avg_humidity
FROM vw_weather_detail
GROUP BY season;

-- Model performance summary
SELECT 
    commodity_name,
    model_name,
    AVG(confidence_score) as avg_r2,
    COUNT(*) as total_predictions
FROM vw_prediction_detail
GROUP BY commodity_name, model_name
ORDER BY avg_r2 DESC;
```

### 2. Grafana - Data Visualization

**URL:** http://localhost:3000  
**Login:** admin / admin

**Setup MySQL Datasource:**

1. Go to **Configuration** → **Data Sources**
2. Click **Add data source**
3. Select **MySQL**
4. Configure:
   - Host: `mysql_warehouse:3306`
   - Database: `harvest_dw`
   - User: `root`
   - Password: `root`
5. Click **Save & Test**

**Sample Dashboard Panels:**

**Panel 1: Production Trend by Commodity**
```sql
SELECT 
    CONCAT(year, '-', LPAD(month, 2, '0'), '-01') as time,
    commodity_name as metric,
    SUM(production_ton) as value
FROM vw_production_detail
WHERE $__timeFilter(CONCAT(year, '-', LPAD(month, 2, '0'), '-01'))
GROUP BY time, metric
ORDER BY time
```

**Panel 2: Actual vs Predicted (Time Series)**
```sql
SELECT 
    CONCAT(year, '-', LPAD(month, 2, '0'), '-01') as time,
    'Actual' as metric,
    SUM(production_ton) as value
FROM vw_production_detail
WHERE commodity_name = 'padi'
GROUP BY time

UNION ALL

SELECT 
    CONCAT(year, '-', LPAD(month, 2, '0'), '-01') as time,
    'Predicted' as metric,
    SUM(predicted_ton) as value
FROM vw_prediction_detail
WHERE commodity_name = 'padi'
GROUP BY time
ORDER BY time
```

**Panel 3: Real-time Streaming Predictions**
```sql
SELECT 
    created_at as time,
    predicted_ton as value,
    CONCAT(province_name, ' - ', commodity_name) as metric
FROM vw_prediction_detail
WHERE model_name LIKE '%_streaming'
ORDER BY created_at DESC
LIMIT 1000
```

### 3. Airflow - Workflow Orchestration

**URL:** http://localhost:8085  
**Login:** airflow / airflow

**Features:**
- View all DAGs
- Trigger manual runs
- Monitor task execution
- View logs
- Set schedules

**DAG Schedule:**
- Current: `@weekly` (runs every Sunday)
- Can change to: `@daily`, `@monthly`, or custom cron

---

## 📁 Project Structure

```
DE_SmartHarvest/
│
├── README.md                         # 📖 This complete documentation
├── STREAMING_GUIDE.md                # 📡 Kafka streaming guide
├── docker-compose.yml                # 🐳 Infrastructure definition
├── requirements.txt                  # 📦 Python dependencies
│
├── dataset/                          # 📁 Data files
│   ├── Cuaca Indonesia by Provinsi 2010-2020/
│   │   └── climate_data.csv         # 35MB, 589K rows
│   ├── Data Produksi Pangan by Provinsi 2010-2015/
│   │   └── Produksi *.csv           # Yearly harvest data
│   └── processed/
│       ├── weather_cleaned.csv      # 51MB (ETL output)
│       └── harvest_monthly_final.csv # 297KB (ETL output)
│
├── scripts/                          # 🔧 ETL Scripts
│   ├── clean_weather.py             # Clean & merge weather data
│   ├── clean_harvest.py             # Clean harvest data
│   ├── generate_dummy_harvest.py    # Extrapolate 2016-2022
│   ├── disaggregate_harvest.py      # Yearly → Monthly
│   ├── init_data_warehouse.sql      # Star Schema DDL
│   └── populate_data_warehouse.py   # Load data to DW
│
├── src/                              # 🤖 ML & Streaming Scripts
│   ├── ml_train_predict_simple.py   # ML training & batch prediction
│   ├── kafka_producer_stream.py     # Stream test data to Kafka
│   └── kafka_consumer_predict.py    # Real-time prediction consumer
│
├── dags/                             # 🔄 Airflow DAGs
│   └── smart_harvest_pipeline.py    # Complete pipeline DAG
│
├── models/                           # 💾 Saved ML Models
│   ├── linear_regression_padi_v1.pkl
│   ├── linear_regression_jagung_v1.pkl
│   ├── linear_regression_kedelai_v1.pkl
│   ├── linear_regression_kacang_tanah_v1.pkl
│   ├── linear_regression_kacang_hijau_v1.pkl
│   ├── linear_regression_ubi_kayu_v1.pkl
│   └── linear_regression_ubi_jalar_v1.pkl
│
├── logs/                             # 📝 Airflow logs (auto-generated)
├── plugins/                          # 🔌 Airflow plugins (empty)
└── grafana/                          # 📊 Grafana config
    └── provisioning/
```

---

## 🔧 Troubleshooting

### Issue 1: Docker Services Not Starting

**Symptoms:**
```bash
docker compose ps
# Shows services as "Exited" or "Unhealthy"
```

**Solutions:**

```bash
# Check logs
docker compose logs [service_name]

# Restart specific service
docker compose restart [service_name]

# Restart all services
docker compose down
docker compose up -d

# Check disk space
df -h

# Check memory
free -h
```

### Issue 2: MySQL Connection Refused

**Symptoms:**
```
ERROR 2002 (HY000): Can't connect to MySQL server
```

**Solutions:**

```bash
# Check MySQL is running
docker compose ps mysql_warehouse

# Check MySQL logs
docker compose logs mysql_warehouse

# Restart MySQL
docker compose restart mysql_warehouse

# Wait for healthy status
docker compose ps | grep mysql_warehouse
# Should show "(healthy)"

# Test connection
docker exec mysql_warehouse mysql -uroot -proot -e "SELECT 1;"
```

### Issue 3: Kafka Not Receiving Messages

**Symptoms:**
- Producer runs but consumer doesn't receive messages

**Solutions:**

```bash
# Check Kafka is running
docker compose ps kafka zookeeper

# List Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check consumer groups
docker exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092

# Delete topic (reset)
docker exec kafka kafka-topics --delete --topic harvest-weather-stream --bootstrap-server localhost:9092

# Restart Kafka
docker compose restart kafka zookeeper
```

### Issue 4: Airflow DAG Not Showing

**Symptoms:**
- DAG doesn't appear in Airflow UI

**Solutions:**

```bash
# Check DAG file exists
ls -la dags/smart_harvest_pipeline.py

# Check Airflow scheduler logs
docker logs de_smartharvest-airflow-scheduler-1

# Restart Airflow
docker compose restart airflow-webserver airflow-scheduler

# Wait 30 seconds and refresh browser
```

### Issue 5: ML Training Fails

**Symptoms:**
```
KeyError: 'year'
ValueError: No data found
```

**Solutions:**

```bash
# Verify data warehouse is populated
docker exec mysql_warehouse mysql -uroot -proot -e "
USE harvest_dw;
SELECT COUNT(*) FROM fact_production_monthly;
SELECT COUNT(*) FROM fact_weather_monthly;
"

# Re-run data warehouse population
python3 scripts/populate_data_warehouse.py

# Check Python dependencies
pip install -r requirements.txt

# Re-run ML training
python3 src/ml_train_predict_simple.py
```

### Issue 6: Port Already in Use

**Symptoms:**
```
Error: bind: address already in use
```

**Solutions:**

```bash
# Check what's using the port
sudo lsof -i :9090  # phpMyAdmin
sudo lsof -i :3000  # Grafana
sudo lsof -i :8085  # Airflow

# Kill the process
sudo kill -9 [PID]

# Or change port in docker-compose.yml
# Example: "9091:80" instead of "9090:80"
```

### Issue 7: Out of Memory

**Symptoms:**
- Services crash randomly
- Docker becomes unresponsive

**Solutions:**

```bash
# Check memory usage
docker stats

# Stop unused services
docker compose stop spark-master spark-worker

# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → 8GB

# Clean up Docker
docker system prune -a
```

---

## 🎓 Learning Outcomes

This project demonstrates:

✅ **Data Engineering**
- ETL pipeline design & implementation
- Data cleaning & transformation
- Data quality management

✅ **Data Warehousing**
- Star Schema modeling
- Dimensional design
- Fact & dimension tables
- Analytical views

✅ **Data Processing**
- Pandas data manipulation
- Aggregation & joins
- Time-series handling

✅ **Machine Learning**
- Regression modeling
- Train/test splitting
- Model evaluation (RMSE, R²)
- Model persistence

✅ **Real-time Streaming**
- Kafka producer/consumer
- Message serialization
- Stream processing

✅ **Workflow Orchestration**
- Airflow DAG design
- Task dependencies
- Scheduling & monitoring

✅ **Containerization**
- Docker Compose
- Multi-service architecture
- Service dependencies

✅ **Database Design**
- Normalization
- Indexing
- Query optimization

✅ **Data Visualization**
- Dashboard creation
- SQL queries for analytics
- Real-time monitoring

---

## 📈 Model Performance & Insights

### Current Performance

| Commodity | RMSE (tons) | R² Score | Interpretation |
|-----------|-------------|----------|----------------|
| padi | 325,190 | 0.1048 | Weather explains 10.48% of variance |
| jagung | 114,867 | 0.1279 | Weather explains 12.79% of variance |
| kedelai | 6,360 | 0.1137 | Weather explains 11.37% of variance |
| kacang_tanah | 3,818 | 0.1140 | Weather explains 11.40% of variance |
| kacang_hijau | 2,042 | 0.1060 | Weather explains 10.60% of variance |
| ubi_kayu | 161,736 | 0.0320 | Weather explains 3.20% of variance |
| **ubi_jalar** | **10,098** | **0.3275** | **Weather explains 32.75% of variance** ⭐ |

### Key Insights

**1. Ubi Jalar (Sweet Potato) - Best Performance**
- R² = 0.3275 (32.75%)
- Weather has strongest correlation
- More predictable based on climate

**2. Ubi Kayu (Cassava) - Poorest Performance**
- R² = 0.0320 (3.20%)
- Weather has minimal impact
- Likely influenced by other factors (soil, fertilizer, etc.)

**3. Overall Low R² Scores**
- Average R² ≈ 12%
- **Conclusion:** Weather alone is not sufficient
- **Need additional features:**
  - Land area (luas lahan)
  - Soil type (jenis tanah)
  - Fertilizer usage (pupuk)
  - Irrigation (irigasi)
  - Pest control (pengendalian hama)

### Improvement Opportunities

**1. Feature Engineering**
- Add lag features (previous month's production)
- Seasonal patterns (moving averages)
- Interaction terms (temp × rainfall)

**2. More Features**
- Land area per province
- Soil quality index
- Fertilizer application rates
- Irrigation coverage

**3. Advanced Models**
- Random Forest (ensemble)
- XGBoost (gradient boosting)
- Neural Networks (deep learning)

**4. Province-specific Models**
- Train separate models per province
- Capture regional patterns

**5. Time-series Models**
- ARIMA / SARIMA
- Prophet (Facebook)
- LSTM (Recurrent Neural Networks)

---

## 🚀 Next Steps & Extensions

### Short-term (1-2 weeks)

1. **Improve Model Performance**
   - Add more features
   - Try Random Forest
   - Hyperparameter tuning

2. **Create Grafana Dashboards**
   - Production trends
   - Actual vs Predicted
   - Real-time streaming monitor

3. **Add Data Validation**
   - Great Expectations
   - Data quality checks
   - Anomaly detection

### Medium-term (1-2 months)

1. **API Development**
   - FastAPI REST endpoints
   - Prediction API
   - Data query API

2. **Web Application**
   - User-friendly UI
   - Interactive charts
   - Prediction requests

3. **Automated Retraining**
   - Monthly model updates
   - A/B testing
   - Model versioning

### Long-term (3-6 months)

1. **Production Deployment**
   - Kubernetes orchestration
   - CI/CD pipeline
   - Monitoring & alerting

2. **Advanced Analytics**
   - Causal inference
   - What-if analysis
   - Recommendation system

3. **Integration**
   - Government data sources
   - Real-time weather APIs
   - Farmer mobile app

---

## 📄 License & Credits

**Project:** Smart Harvest Prediction System  
**Purpose:** Educational (Data Engineering Infrastructure Course)  
**Institution:** [Your University]  
**Course:** Pemrosesan Infrastruktur Data  

**Data Sources:**
- Climate Data: BPS (Badan Pusat Statistik)
- Harvest Data: Kementerian Pertanian

**Technologies:**
- MySQL, Apache Kafka, Apache Airflow
- Python, Pandas, scikit-learn
- Docker, Grafana, phpMyAdmin

---

## 🤝 Contributing

This is an educational project. Suggestions for improvement:

1. Fork the repository
2. Create feature branch
3. Make improvements
4. Submit pull request

**Areas for contribution:**
- Model improvements
- Additional features
- Better visualizations
- Documentation enhancements
- Bug fixes

---

## 📞 Support

**Issues?** Check:
1. [Troubleshooting](#-troubleshooting) section
2. Docker logs: `docker compose logs [service]`
3. Airflow logs: Check UI
4. MySQL logs: `docker logs mysql_warehouse`

**Questions?**
- Review this documentation
- Check `STREAMING_GUIDE.md` for Kafka details
- Inspect code comments

---

**Made with ❤️ for SDG 2: Zero Hunger**

*Last Updated: November 2025*
