import pandas as pd
import os
import glob

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATASET_DIR = "/home/lalalostnux/PemrosesaInfrastrukturData/DE_SmartHarvest/dataset/Data Produksi Pangan by Provinsi 2010-2015"
OUTPUT_DIR = os.path.join(BASE_DIR, "dataset", "processed")

# Create output directory if not exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_harvest_data():
    print("Loading harvest data...")
    all_files = glob.glob(os.path.join(DATASET_DIR, "*.csv"))
    
    combined_data = []

    for file in all_files:
        # Extract year from filename (e.g., "Produksi Tanaman Pangan, 2015.csv")
        filename = os.path.basename(file)
        try:
            year = int(filename.split(",")[1].strip().replace(".csv", ""))
        except:
            print(f"Skipping file {filename}, cannot parse year.")
            continue
            
        print(f"Processing year {year}...")
        
        # Read CSV, skipping first 4 rows of header junk
        # Columns: Province, Padi, Jagung, Kedelai, Kacang tanah, Kacang Hijau, Ubi Kayu, Ubi Jalar
        df = pd.read_csv(file, skiprows=4, header=None)
        
        # Rename columns
        df.columns = ['province_name', 'padi', 'jagung', 'kedelai', 'kacang_tanah', 'kacang_hijau', 'ubi_kayu', 'ubi_jalar']
        
        # Add year column
        df['year'] = year
        
        # Clean province name (remove leading/trailing spaces)
        df['province_name'] = df['province_name'].astype(str).str.strip()
        
        # Drop rows where province is NaN or "Indonesia" (total)
        df = df.dropna(subset=['province_name'])
        df = df[df['province_name'] != 'INDONESIA']
        
        combined_data.append(df)

    if not combined_data:
        print("No data found!")
        return

    final_df = pd.concat(combined_data, ignore_index=True)

    # Standardize Province Names to match Weather Data (Simple Mapping)
    # Weather Data uses "Nanggroe Aceh Darussalam", Harvest uses "ACEH"
    # We will convert everything to UPPERCASE for easier matching later
    final_df['province_name'] = final_df['province_name'].str.upper()
    
    # Save to CSV
    output_path = os.path.join(OUTPUT_DIR, "harvest_cleaned.csv")
    final_df.to_csv(output_path, index=False)
    print(f"Cleaned harvest data saved to {output_path}")
    print(final_df.head())

if __name__ == "__main__":
    clean_harvest_data()
