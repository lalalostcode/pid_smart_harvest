"""
Smart Harvest - Populate Data Warehouse
Migrate data from CSV to Star Schema
"""

import mysql.connector
import pandas as pd
from datetime import datetime

print("=" * 60)
print("SMART HARVEST - POPULATE DATA WAREHOUSE")
print("=" * 60)

# ==========================================
# 1. CONNECT TO MYSQL
# ==========================================
print("\n[1/6] Connecting to MySQL...")
conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root"
)
cursor = conn.cursor()
print("✅ Connected!")

# ==========================================
# 2. CREATE DATABASE & TABLES
# ==========================================
print("\n[2/6] Creating Data Warehouse schema...")
with open('scripts/init_data_warehouse.sql', 'r') as f:
    sql_script = f.read()
    
# Execute each statement
for statement in sql_script.split(';'):
    if statement.strip():
        try:
            cursor.execute(statement)
        except Exception as e:
            if "already exists" not in str(e).lower():
                print(f"Warning: {e}")

conn.commit()
print("✅ Schema created!")

# Switch to DW database
cursor.execute("USE harvest_dw")

# ==========================================
# 3. POPULATE DIM_TIME (2010-2025)
# ==========================================
print("\n[3/6] Populating dim_time...")

def get_season(month):
    """Kemarau: Apr-Sep (4-9), Hujan: Oct-Mar (10-3)"""
    return "Kemarau" if 4 <= month <= 9 else "Hujan"

def get_month_name(month):
    months = ['', 'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
              'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
    return months[month]

def get_quarter(month):
    return (month - 1) // 3 + 1

time_data = []
for year in range(2010, 2026):  # 2010-2025
    for month in range(1, 13):
        time_id = year * 100 + month  # e.g., 201501
        time_data.append((
            time_id,
            year,
            month,
            get_month_name(month),
            get_quarter(month),
            get_season(month)
        ))

cursor.executemany("""
    INSERT IGNORE INTO dim_time (time_id, year, month, month_name, quarter, season)
    VALUES (%s, %s, %s, %s, %s, %s)
""", time_data)
conn.commit()
print(f"✅ Inserted {len(time_data)} time records (2010-2025)")

# ==========================================
# 4. POPULATE DIM_PROVINCE
# ==========================================
print("\n[4/6] Populating dim_province...")

# Load from harvest data
harvest_df = pd.read_csv('dataset/processed/harvest_monthly_final.csv')
provinces = harvest_df['province_name'].unique()

province_data = [(prov,) for prov in sorted(provinces)]
cursor.executemany("""
    INSERT IGNORE INTO dim_province (province_name)
    VALUES (%s)
""", province_data)
conn.commit()
print(f"✅ Inserted {len(province_data)} provinces")

# ==========================================
# 5. POPULATE FACT_PRODUCTION_MONTHLY
# ==========================================
print("\n[5/6] Populating fact_production_monthly...")

# Get province_id mapping
cursor.execute("SELECT province_id, province_name FROM dim_province")
province_map = {name: pid for pid, name in cursor.fetchall()}

# Get commodity_id mapping
cursor.execute("SELECT commodity_id, commodity_name FROM dim_commodity")
commodity_map = {name: cid for cid, name in cursor.fetchall()}

# Prepare production data
production_data = []
commodities = ['padi', 'jagung', 'kedelai', 'kacang_tanah', 'kacang_hijau', 'ubi_kayu', 'ubi_jalar']

count = 0
for _, row in harvest_df.iterrows():
    time_id = int(row['year']) * 100 + int(row['month'])
    province_id = province_map.get(row['province_name'])
    
    if not province_id:
        continue
    
    # Insert each commodity as separate row
    for commodity in commodities:
        if commodity in row and pd.notna(row[commodity]):
            commodity_id = commodity_map.get(commodity)
            if commodity_id:
                production_data.append((
                    time_id,
                    province_id,
                    commodity_id,
                    float(row[commodity])
                ))
                count += 1
                
                # Batch insert every 1000 rows
                if len(production_data) >= 1000:
                    cursor.executemany("""
                        INSERT IGNORE INTO fact_production_monthly 
                        (time_id, province_id, commodity_id, production_ton)
                        VALUES (%s, %s, %s, %s)
                    """, production_data)
                    conn.commit()
                    print(f"   Inserted {count} production records...")
                    production_data = []

# Insert remaining
if production_data:
    cursor.executemany("""
        INSERT IGNORE INTO fact_production_monthly 
        (time_id, province_id, commodity_id, production_ton)
        VALUES (%s, %s, %s, %s)
    """, production_data)
    conn.commit()

print(f"✅ Total production records inserted: {count}")

# ==========================================
# 6. POPULATE FACT_WEATHER_MONTHLY
# ==========================================
print("\n[6/6] Populating fact_weather_monthly...")

# Load weather data
weather_df = pd.read_csv('dataset/processed/weather_cleaned.csv')

# Convert date to datetime (auto-detect format)
weather_df['date'] = pd.to_datetime(weather_df['date'])
weather_df['year'] = weather_df['date'].dt.year
weather_df['month'] = weather_df['date'].dt.month

# Aggregate to monthly
weather_monthly = weather_df.groupby(['province_name', 'year', 'month']).agg({
    'RR': 'sum',        # Total rainfall
    'Tavg': 'mean',     # Average temperature
    'RH_avg': 'mean'    # Average humidity
}).reset_index()

weather_data = []
count = 0

for _, row in weather_monthly.iterrows():
    time_id = int(row['year']) * 100 + int(row['month'])
    province_id = province_map.get(row['province_name'].upper())
    
    if not province_id:
        continue
    
    weather_data.append((
        time_id,
        province_id,
        float(row['RR']) if pd.notna(row['RR']) else 0,
        float(row['Tavg']) if pd.notna(row['Tavg']) else 0,
        float(row['RH_avg']) if pd.notna(row['RH_avg']) else 0
    ))
    count += 1
    
    # Batch insert every 1000 rows
    if len(weather_data) >= 1000:
        cursor.executemany("""
            INSERT IGNORE INTO fact_weather_monthly 
            (time_id, province_id, total_rainfall_mm, avg_temperature_c, avg_humidity_pct)
            VALUES (%s, %s, %s, %s, %s)
        """, weather_data)
        conn.commit()
        print(f"   Inserted {count} weather records...")
        weather_data = []

# Insert remaining
if weather_data:
    cursor.executemany("""
        INSERT IGNORE INTO fact_weather_monthly 
        (time_id, province_id, total_rainfall_mm, avg_temperature_c, avg_humidity_pct)
        VALUES (%s, %s, %s, %s, %s)
    """, weather_data)
    conn.commit()

print(f"✅ Total weather records inserted: {count}")

# ==========================================
# 7. SUMMARY
# ==========================================
print("\n" + "=" * 60)
print("DATA WAREHOUSE POPULATION COMPLETE!")
print("=" * 60)

# Get counts
cursor.execute("SELECT COUNT(*) FROM dim_province")
prov_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM dim_time")
time_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM dim_commodity")
comm_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM fact_production_monthly")
prod_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM fact_weather_monthly")
weather_count = cursor.fetchone()[0]

print(f"\n📊 DIMENSION TABLES:")
print(f"   - dim_province: {prov_count} rows")
print(f"   - dim_time: {time_count} rows")
print(f"   - dim_commodity: {comm_count} rows")

print(f"\n📈 FACT TABLES:")
print(f"   - fact_production_monthly: {prod_count} rows")
print(f"   - fact_weather_monthly: {weather_count} rows")
print(f"   - fact_crop_prediction: 0 rows (will be populated by ML)")

print(f"\n🌐 ACCESS DATA WAREHOUSE:")
print(f"   phpMyAdmin: http://localhost:9090")
print(f"   Database: harvest_dw")
print(f"   Login: root / root")

print("\n✅ Ready for ML training and predictions!")
print("=" * 60)

conn.close()
