"""
Kafka Producer - Stream Test Data (2019-2020)
Simulates real-time weather + production data streaming
"""

import json
import time
import mysql.connector
from kafka import KafkaProducer
from datetime import datetime

print("=" * 60)
print("KAFKA PRODUCER - STREAMING TEST DATA")
print("=" * 60)

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'harvest-weather-stream'

# Connect to MySQL to get test data
print("\n[1/3] Connecting to MySQL...")
conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root",
    database="harvest_dw"
)

# Get test data (2019-2020) for all commodities
query = """
SELECT 
    p.time_id,
    p.province_id,
    p.commodity_id,
    p.production_ton as actual_production,
    w.total_rainfall_mm,
    w.avg_temperature_c,
    w.avg_humidity_pct,
    t.year,
    t.month,
    prov.province_name,
    c.commodity_name
FROM fact_production_monthly p
JOIN fact_weather_monthly w 
    ON p.time_id = w.time_id AND p.province_id = w.province_id
JOIN dim_time t ON p.time_id = t.time_id
JOIN dim_province prov ON p.province_id = prov.province_id
JOIN dim_commodity c ON p.commodity_id = c.commodity_id
WHERE t.year >= 2019
ORDER BY t.year, t.month, prov.province_name, c.commodity_name
"""

print("   Loading test data (2019-2020)...")
import pandas as pd
df = pd.read_sql(query, conn)
conn.close()

print(f"   ✅ Loaded {len(df)} records to stream")
print(f"   Year range: {df['year'].min()} - {df['year'].max()}")

# Initialize Kafka Producer
print("\n[2/3] Connecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    print(f"   ✅ Connected to Kafka broker: {KAFKA_BROKER}")
except Exception as e:
    print(f"   ❌ Failed to connect to Kafka: {e}")
    print("   Make sure Kafka is running: docker compose ps | grep kafka")
    exit(1)

# Stream data row by row
print(f"\n[3/3] Streaming {len(df)} records to topic '{TOPIC_NAME}'...")
print("   Press Ctrl+C to stop\n")

try:
    count = 0
    for idx, row in df.iterrows():
        # Prepare message
        message = {
            'time_id': int(row['time_id']),
            'province_id': int(row['province_id']),
            'commodity_id': int(row['commodity_id']),
            'province_name': row['province_name'],
            'commodity_name': row['commodity_name'],
            'year': int(row['year']),
            'month': int(row['month']),
            'avg_temperature_c': float(row['avg_temperature_c']),
            'total_rainfall_mm': float(row['total_rainfall_mm']),
            'avg_humidity_pct': float(row['avg_humidity_pct']),
            'actual_production': float(row['actual_production']),
            'timestamp': datetime.now().isoformat()
        }
        
        # Send to Kafka
        producer.send(TOPIC_NAME, value=message)
        count += 1
        
        # Progress update every 100 records
        if count % 100 == 0:
            print(f"   Streamed {count}/{len(df)} records... ({count/len(df)*100:.1f}%)")
        
        # Simulate real-time delay (10ms per record)
        # Adjust this to control streaming speed
        time.sleep(0.01)
    
    # Flush remaining messages
    producer.flush()
    
    print(f"\n✅ Streaming complete!")
    print(f"   Total records sent: {count}")
    print(f"   Topic: {TOPIC_NAME}")
    
except KeyboardInterrupt:
    print(f"\n\n⚠️  Streaming stopped by user")
    print(f"   Records sent: {count}/{len(df)}")
    producer.flush()
    
except Exception as e:
    print(f"\n❌ Error during streaming: {e}")
    
finally:
    producer.close()
    print("\n" + "=" * 60)
