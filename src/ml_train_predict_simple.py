"""
Smart Harvest - ML Training & Prediction (Simplified)
Using Data Warehouse with scikit-learn
"""

import mysql.connector
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from datetime import datetime
import pickle

print("=" * 60)
print("SMART HARVEST - ML TRAINING & PREDICTION")
print("Using Data Warehouse (harvest_dw)")
print("=" * 60)

# ==========================================
# 1. LOAD DATA FROM MYSQL
# ==========================================
print("\n[1/5] Loading data from MySQL...")

conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root",
    database="harvest_dw"
)

# Get all commodities
cursor = conn.cursor()
cursor.execute("SELECT commodity_id, commodity_name FROM dim_commodity ORDER BY commodity_id")
commodities = cursor.fetchall()

print(f"   Found {len(commodities)} commodities to train")
print(f"   Commodities: {', '.join([c[1] for c in commodities])}")

# Store results for all commodities
all_results = []

# ==========================================
# TRAIN MODEL FOR EACH COMMODITY
# ==========================================
for commodity_id, commodity_name in commodities:
    print("\n" + "=" * 60)
    print(f"TRAINING MODEL FOR: {commodity_name.upper()}")
    print("=" * 60)
    
    # Load production + weather data for this commodity
    query = f"""
    SELECT 
        p.time_id,
        p.province_id,
        p.commodity_id,
        p.production_ton,
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
    WHERE c.commodity_name = '{commodity_name}'
    """
    
    df = pd.read_sql(query, conn)
    
    if len(df) == 0:
        print(f"   ⚠️  No data found for {commodity_name}, skipping...")
        continue
    
    print(f"   Loaded {len(df)} records")
    print(f"   DEBUG - Columns: {df.columns.tolist()}")
    print(f"   DEBUG - Year range: {df['year'].min()} - {df['year'].max()}")
    
    # ==========================================
    # PREPARE DATA (TIME-BASED SPLIT)
    # ==========================================
    # Training: 2010-2018 (9 years)
    # Testing: 2019-2020 (2 years)
    
    train_df = df[df['year'] <= 2018].copy()
    test_df = df[df['year'] >= 2019].copy()
    
    print(f"   DEBUG - Train rows: {len(train_df)}, Test rows: {len(test_df)}")
    
    if len(train_df) == 0 or len(test_df) == 0:
        print(f"   ⚠️  Insufficient data for time-based split, skipping...")
        continue
    
    X_train = train_df[['avg_temperature_c', 'total_rainfall_mm', 'avg_humidity_pct']]
    y_train = train_df['production_ton']
    
    X_test = test_df[['avg_temperature_c', 'total_rainfall_mm', 'avg_humidity_pct']]
    y_test = test_df['production_ton']
    
    print(f"   Train set: {len(X_train)} rows (2010-2018)")
    print(f"   Test set: {len(X_test)} rows (2019-2020)")

    
    # ==========================================
    # TRAIN MODEL
    # ==========================================
    print(f"\n   Training model for {commodity_name}...")
    
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # ==========================================
    # EVALUATE MODEL
    # ==========================================
    y_pred_test = model.predict(X_test)
    
    rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
    r2 = r2_score(y_test, y_pred_test)
    
    print(f"   RMSE: {rmse:,.2f} tons")
    print(f"   R²: {r2:.4f}")
    
    # ==========================================
    # GENERATE PREDICTIONS (for ALL data)
    # ==========================================
    X_all = df[['avg_temperature_c', 'total_rainfall_mm', 'avg_humidity_pct']]
    df['predicted_ton'] = model.predict(X_all)
    
    # ==========================================
    # SAVE TO DATABASE
    # ==========================================
    print(f"   Saving {len(df)} predictions to database...")
    
    batch_data = []
    for _, row in df.iterrows():
        batch_data.append((
            int(row['time_id']),
            int(row['province_id']),
            int(row['commodity_id']),
            float(row['predicted_ton']),
            f'LinearRegression_{commodity_name}_v1',
            float(r2)
        ))
        
        # Batch insert every 1000 rows
        if len(batch_data) >= 1000:
            cursor.executemany("""
                INSERT INTO fact_crop_prediction 
                (time_id, province_id, commodity_id, predicted_ton, model_name, confidence_score)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    predicted_ton = VALUES(predicted_ton),
                    model_name = VALUES(model_name),
                    confidence_score = VALUES(confidence_score)
            """, batch_data)
            conn.commit()
            batch_data = []
    
    # Insert remaining
    if batch_data:
        cursor.executemany("""
            INSERT INTO fact_crop_prediction 
            (time_id, province_id, commodity_id, predicted_ton, model_name, confidence_score)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                predicted_ton = VALUES(predicted_ton),
                model_name = VALUES(model_name),
                confidence_score = VALUES(confidence_score)
        """, batch_data)
        conn.commit()
    
    # Save model to file
    model_filename = f'models/linear_regression_{commodity_name}_v1.pkl'
    with open(model_filename, 'wb') as f:
        pickle.dump(model, f)
    
    # Store results
    all_results.append({
        'commodity': commodity_name,
        'records': len(df),
        'train_size': len(X_train),
        'test_size': len(X_test),
        'rmse': rmse,
        'r2': r2,
        'model_file': model_filename
    })
    
    print(f"   ✅ {commodity_name.upper()} model complete!")

# ==========================================
# SUMMARY FOR ALL COMMODITIES
# ==========================================
conn.close()

print("\n" + "=" * 60)
print("ML TRAINING & PREDICTION COMPLETE!")
print("=" * 60)

print(f"\n📊 MODELS TRAINED: {len(all_results)} commodities")
print("\n" + "-" * 60)
print(f"{'Commodity':<20} {'Records':<10} {'RMSE':<15} {'R²':<10}")
print("-" * 60)

for result in all_results:
    print(f"{result['commodity']:<20} {result['records']:<10} {result['rmse']:>12,.2f}   {result['r2']:>6.4f}")

print("-" * 60)

# Get total predictions
conn_temp = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root",
    database="harvest_dw"
)
cursor_temp = conn_temp.cursor()
cursor_temp.execute("SELECT COUNT(*) FROM fact_crop_prediction")
total_predictions = cursor_temp.fetchone()[0]

cursor_temp.execute("SELECT COUNT(DISTINCT commodity_id) FROM fact_crop_prediction")
total_commodities = cursor_temp.fetchone()[0]

conn_temp.close()

print(f"\n💾 PREDICTIONS SAVED:")
print(f"   - Database: harvest_dw")
print(f"   - Table: fact_crop_prediction")
print(f"   - Total predictions: {total_predictions:,}")
print(f"   - Commodities: {total_commodities}")

print(f"\n📁 MODELS SAVED:")
for result in all_results:
    print(f"   - {result['model_file']}")

print(f"\n🌐 VIEW RESULTS:")
print(f"   phpMyAdmin: http://localhost:9090")
print(f"   Database: harvest_dw")
print(f"\n   Sample queries:")
print(f"   - SELECT * FROM vw_prediction_detail LIMIT 10;")
print(f"   - SELECT commodity_name, AVG(predicted_ton) as avg_prediction")
print(f"     FROM vw_prediction_detail GROUP BY commodity_name;")

print("\n" + "=" * 60)
