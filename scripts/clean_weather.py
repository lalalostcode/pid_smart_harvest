import pandas as pd
import os

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATASET_DIR = "/home/lalalostnux/PemrosesaInfrastrukturData/DE_SmartHarvest/dataset/Cuaca Indonesia by Provinsi 2010-2020"
OUTPUT_DIR = os.path.join(BASE_DIR, "dataset", "processed")

# Create output directory if not exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_weather_data():
    print("Loading weather data...")
    climate_df = pd.read_csv(os.path.join(DATASET_DIR, "climate_data.csv"))
    station_df = pd.read_csv(os.path.join(DATASET_DIR, "station_detail.csv"))
    province_df = pd.read_csv(os.path.join(DATASET_DIR, "province_detail.csv"))

    print("Cleaning weather data...")
    # Convert date column
    climate_df['date'] = pd.to_datetime(climate_df['date'], format='%d-%m-%Y', errors='coerce')
    
    # Drop rows with invalid dates
    climate_df = climate_df.dropna(subset=['date'])

    # Select relevant columns
    # Tn: Minimum Temperature
    # Tx: Maximum Temperature
    # Tavg: Average Temperature
    # RH_avg: Average Humidity
    # RR: Rainfall
    cols_to_keep = ['date', 'station_id', 'Tn', 'Tx', 'Tavg', 'RH_avg', 'RR']
    climate_df = climate_df[cols_to_keep]

    # Fill missing values with 0 for Rainfall, and forward fill for others (simple strategy)
    climate_df['RR'] = climate_df['RR'].fillna(0)
    climate_df = climate_df.fillna(method='ffill').fillna(method='bfill')

    print("Joining with station and province info...")
    # Join with station details
    merged_df = pd.merge(climate_df, station_df[['station_id', 'province_id', 'station_name']], on='station_id', how='left')
    
    # Join with province details
    final_df = pd.merge(merged_df, province_df[['province_id', 'province_name']], on='province_id', how='left')

    # Drop rows where province is missing
    final_df = final_df.dropna(subset=['province_name'])

    # Save to CSV
    output_path = os.path.join(OUTPUT_DIR, "weather_cleaned.csv")
    final_df.to_csv(output_path, index=False)
    print(f"Cleaned weather data saved to {output_path}")
    print(final_df.head())

if __name__ == "__main__":
    clean_weather_data()
