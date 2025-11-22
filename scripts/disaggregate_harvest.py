import pandas as pd
import os

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "dataset", "processed", "harvest_extrapolated.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "dataset", "processed", "harvest_monthly_final.csv")

# Seasonal Weights (Approximation of Indonesian Rice Harvest)
# Peak 1: March-April
# Peak 2: August-September
SEASONAL_WEIGHTS = {
    1: 0.05,  # Jan
    2: 0.05,  # Feb
    3: 0.15,  # Mar (Peak 1)
    4: 0.15,  # Apr (Peak 1)
    5: 0.10,  # May
    6: 0.05,  # Jun
    7: 0.05,  # Jul
    8: 0.10,  # Aug (Peak 2)
    9: 0.10,  # Sep (Peak 2)
    10: 0.05, # Oct
    11: 0.05, # Nov
    12: 0.10  # Dec (End of year push)
}

def disaggregate_data():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found. Run generate_dummy_harvest.py first.")
        return

    print("Loading extrapolated harvest data...")
    df = pd.read_csv(INPUT_FILE)
    
    monthly_rows = []
    
    print("Disaggregating Yearly -> Monthly...")
    for _, row in df.iterrows():
        year = int(row['year'])
        province = row['province_name']
        
        for month, weight in SEASONAL_WEIGHTS.items():
            new_row = row.copy()
            new_row['month'] = month
            
            # Distribute yearly total to months
            for col in ['padi', 'jagung', 'kedelai', 'kacang_tanah', 'kacang_hijau', 'ubi_kayu', 'ubi_jalar']:
                new_row[col] = int(row[col] * weight)
                
            monthly_rows.append(new_row)
            
    monthly_df = pd.DataFrame(monthly_rows)
    
    # Reorder columns
    cols = ['province_name', 'year', 'month'] + [c for c in monthly_df.columns if c not in ['province_name', 'year', 'month']]
    monthly_df = monthly_df[cols]
    
    # Save
    monthly_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Monthly harvest data saved to {OUTPUT_FILE}")
    print(monthly_df.head())

if __name__ == "__main__":
    disaggregate_data()
