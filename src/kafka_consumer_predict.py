"""
Kafka Consumer - Real-time Prediction
Consumes streaming data, makes predictions, saves to MySQL
"""

import json
import pickle
import mysql.connector
from kafka import KafkaConsumer
import numpy as np
from datetime import datetime

print("=" * 60)
print("KAFKA CONSUMER - REAL-TIME PREDICTION")
print("=" * 60)

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'harvest-weather-stream'
GROUP_ID = 'harvest-prediction-consumer'

# Load ML models
print("\n[1/4] Loading ML models...")
models = {}
commodities = ['padi', 'jagung', 'kedelai', 'kacang_tanah', 'kacang_hijau', 'ubi_kayu', 'ubi_jalar']

for commodity in commodities:
    try:
        with open(f'models/linear_regression_{commodity}_v1.pkl', 'rb') as f:
            models[commodity] = pickle.load(f)
        print(f"   ✅ Loaded model for {commodity}")
    except Exception as e:
        print(f"   ⚠️  Failed to load model for {commodity}: {e}")

print(f"\n   Total models loaded: {len(models)}")

# Connect to MySQL
print("\n[2/4] Connecting to MySQL...")
conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root",
    database="harvest_dw"
)
cursor = conn.cursor()
print("   ✅ Connected to MySQL")

# Clear old streaming predictions
cursor.execute("DELETE FROM fact_crop_prediction WHERE model_name LIKE '%_streaming'")
conn.commit()
print("   ✅ Cleared old streaming predictions")

# Initialize Kafka Consumer
print(f"\n[3/4] Connecting to Kafka consumer...")
try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    print(f"   ✅ Subscribed to topic: {TOPIC_NAME}")
except Exception as e:
    print(f"   ❌ Failed to connect to Kafka: {e}")
    exit(1)

# Consume and predict
print(f"\n[4/4] Consuming messages and making predictions...")
print("   Press Ctrl+C to stop\n")

count = 0
batch_data = []
BATCH_SIZE = 100

try:
    for message in consumer:
        data = message.value
        
        # Extract features
        commodity_name = data['commodity_name']
        features = np.array([[
            data['avg_temperature_c'],
            data['total_rainfall_mm'],
            data['avg_humidity_pct']
        ]])
        
        # Make prediction
        if commodity_name in models:
            model = models[commodity_name]
            predicted_ton = model.predict(features)[0]
            
            # Get model R² score from filename (stored during training)
            # For now, use a placeholder
            confidence_score = 0.15  # Average R² from training
            
            # Prepare for batch insert
            batch_data.append((
                int(data['time_id']),
                int(data['province_id']),
                int(data['commodity_id']),
                float(predicted_ton),
                f"LinearRegression_{commodity_name}_streaming",
                float(confidence_score),
                data['timestamp']
            ))
            
            count += 1
            
            # Batch insert every BATCH_SIZE records
            if len(batch_data) >= BATCH_SIZE:
                cursor.executemany("""
                    INSERT INTO fact_crop_prediction 
                    (time_id, province_id, commodity_id, predicted_ton, model_name, confidence_score, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        predicted_ton = VALUES(predicted_ton),
                        model_name = VALUES(model_name),
                        confidence_score = VALUES(confidence_score),
                        created_at = VALUES(created_at)
                """, batch_data)
                conn.commit()
                
                print(f"   ✅ Processed {count} predictions | Latest: {data['year']}-{data['month']:02d} | {data['province_name']} | {commodity_name}")
                batch_data = []
        
        else:
            print(f"   ⚠️  No model for {commodity_name}, skipping...")
    
except KeyboardInterrupt:
    print(f"\n\n⚠️  Consumer stopped by user")
    
except Exception as e:
    print(f"\n❌ Error during consumption: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    # Insert remaining batch
    if batch_data:
        cursor.executemany("""
            INSERT INTO fact_crop_prediction 
            (time_id, province_id, commodity_id, predicted_ton, model_name, confidence_score, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                predicted_ton = VALUES(predicted_ton),
                model_name = VALUES(model_name),
                confidence_score = VALUES(confidence_score),
                created_at = VALUES(created_at)
        """, batch_data)
        conn.commit()
    
    consumer.close()
    conn.close()
    
    print(f"\n" + "=" * 60)
    print(f"SUMMARY")
    print("=" * 60)
    print(f"   Total predictions made: {count}")
    print(f"   Saved to: harvest_dw.fact_crop_prediction")
    print(f"   Model type: *_streaming")
    print("=" * 60)
