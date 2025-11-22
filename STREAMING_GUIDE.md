# Real-Time Prediction with Kafka Streaming

## Overview

This demonstrates **real-time prediction** by streaming test data (2019-2020) through Kafka and making predictions on-the-fly.

## Architecture

```
Test Data (2019-2020)
    ↓
Kafka Producer (stream row-by-row)
    ↓
Kafka Topic: "harvest-weather-stream"
    ↓
Kafka Consumer (load model → predict)
    ↓
MySQL (save predictions)
    ↓
MySQL (save predictions)
    ↓
Streamlit Dashboard (real-time visualization)
```

## Prerequisites

1. **Kafka must be running:**
   ```bash
   docker compose ps | grep kafka
   ```

2. **ML models must be trained:**
   ```bash
   python3 src/ml_train_predict_simple.py
   ```

3. **Install dependencies:**
   ```bash
   pip install kafka-python
   ```

## How to Run

### Step 1: Start Kafka Consumer (Terminal 1)

```bash
cd /home/lalalostnux/PemrosesaInfrastrukturData/DE_SmartHarvest
python3 src/kafka_consumer_predict.py
```

**Output:**
```
============================================================
KAFKA CONSUMER - REAL-TIME PREDICTION
============================================================

[1/4] Loading ML models...
   ✅ Loaded model for padi
   ✅ Loaded model for jagung
   ...
   
[2/4] Connecting to MySQL...
   ✅ Connected to MySQL
   
[3/4] Connecting to Kafka consumer...
   ✅ Subscribed to topic: harvest-weather-stream
   
[4/4] Consuming messages and making predictions...
   Press Ctrl+C to stop

   (waiting for messages...)
```

### Step 2: Start Kafka Producer (Terminal 2)

```bash
cd /home/lalalostnux/PemrosesaInfrastrukturData/DE_SmartHarvest
python3 src/kafka_producer_stream.py
```

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
   ...
```

### Step 3: Watch Real-time Predictions (Terminal 1)

Consumer will show:
```
   ✅ Processed 100 predictions | Latest: 2019-01 | ACEH | padi
   ✅ Processed 200 predictions | Latest: 2019-01 | BALI | jagung
   ✅ Processed 300 predictions | Latest: 2019-02 | DKI JAKARTA | kedelai
   ...
```

### Step 4: View Results in MySQL

```bash
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

## Streaming Speed

Default: **10ms per record** (100 records/second)

To adjust speed, edit `src/kafka_producer_stream.py`:
```python
time.sleep(0.01)  # Change this value
# 0.001 = 1ms (faster)
# 0.1 = 100ms (slower, more realistic)
```

## Stop Streaming

Press **Ctrl+C** in both terminals:
- Producer will flush remaining messages
- Consumer will save remaining predictions

## Verify Results

### Check prediction count:
```bash
docker exec mysql_warehouse mysql -uroot -proot -e "
USE harvest_dw;
SELECT COUNT(*) as streaming_predictions 
FROM fact_crop_prediction 
WHERE model_name LIKE '%_streaming';
"
```

### Compare batch vs streaming predictions:
```sql
SELECT 
    CASE 
        WHEN model_name LIKE '%_streaming' THEN 'Streaming'
        ELSE 'Batch'
    END as prediction_type,
    COUNT(*) as total_predictions,
    AVG(predicted_ton) as avg_prediction
FROM fact_crop_prediction
GROUP BY prediction_type;
```

## Streamlit Dashboard

Visualize **real-time predictions** using the interactive dashboard:

1. Run the dashboard:
   ```bash
   python3 -m streamlit run dashboard.py
   ```
2. Open http://localhost:8501
3. Select **"All Commodities"** and **"All Provinces"**
4. Watch the **"Total Production"** and charts update as new streaming data flows into MySQL!

**Note:** The dashboard reads directly from the `harvest_dw` database, so as soon as the Kafka Consumer saves predictions, they will appear in the dashboard (refresh may be required depending on cache settings).

## Troubleshooting

### Kafka not running:
```bash
docker compose up -d zookeeper kafka
```

### Consumer not receiving messages:
```bash
# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check consumer groups
docker exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
```

### Clear Kafka topic:
```bash
docker exec kafka kafka-topics --delete --topic harvest-weather-stream --bootstrap-server localhost:9092
```

## Performance

- **Streaming rate**: ~100 records/second
- **Prediction latency**: <10ms per record
- **Batch insert**: Every 100 predictions
- **Total time for 5544 records**: ~55 seconds

## Next Steps

1. **Add more features** to improve predictions
2. **Use Streamlit dashboard** for real-time monitoring
3. **Add alerting** for anomaly detection
4. **Scale consumers** for higher throughput
