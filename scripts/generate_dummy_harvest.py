import pandas as pd
import numpy as np
import os

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "dataset", "processed", "harvest_cleaned.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "dataset", "processed", "harvest_extrapolated.csv")

def generate_dummy_data():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found. Run clean_harvest.py first.")
        return

    print("Loading cleaned harvest data...")
    df = pd.read_csv(INPUT_FILE)
    
    # Get list of provinces
    provinces = df['province_name'].unique()
    
    # Ensure numeric columns are actually numeric
    numeric_cols = ['padi', 'jagung', 'kedelai', 'kacang_tanah', 'kacang_hijau', 'ubi_kayu', 'ubi_jalar']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Years to generate
    target_years = range(2016, 2023) # 2016 to 2022
    
    new_rows = []
    
    print("Generating dummy data for 2016-2022...")
    for province in provinces:
        prov_data = df[df['province_name'] == province].sort_values('year')
        
        if len(prov_data) < 2:
            continue
            
        # Calculate simple average growth rate for 'padi'
        # (We will apply same rate to other crops for simplicity)
        initial_val = prov_data.iloc[0]['padi']
        final_val = prov_data.iloc[-1]['padi']
        years_diff = prov_data.iloc[-1]['year'] - prov_data.iloc[0]['year']
        
        if initial_val == 0 or years_diff == 0:
            growth_rate = 0.02 # Default 2%
        else:
            growth_rate = (final_val / initial_val) ** (1/years_diff) - 1
            
        # Cap growth rate to avoid explosion (max 5%, min -5%)
        growth_rate = max(min(growth_rate, 0.05), -0.05)
        
        last_row = prov_data.iloc[-1]
        current_values = {col: last_row[col] for col in ['padi', 'jagung', 'kedelai', 'kacang_tanah', 'kacang_hijau', 'ubi_kayu', 'ubi_jalar']}
        
        for year in target_years:
            # Add random noise (-2% to +2%)
            noise = np.random.uniform(-0.02, 0.02)
            effective_rate = growth_rate + noise
            
            row = {'province_name': province, 'year': year}
            
            for col in current_values:
                # Update value
                new_val = current_values[col] * (1 + effective_rate)
                current_values[col] = max(0, int(new_val)) # Ensure no negative
                row[col] = current_values[col]
            
            new_rows.append(row)
            
    dummy_df = pd.DataFrame(new_rows)
    
    # Combine original and dummy
    final_df = pd.concat([df, dummy_df], ignore_index=True).sort_values(['province_name', 'year'])
    
    # Save
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Extrapolated harvest data saved to {OUTPUT_FILE}")
    print(final_df.tail())

if __name__ == "__main__":
    generate_dummy_data()
