"""
Smart Harvest - Complete ETL & ML Pipeline DAG
Orchestrates: Data Prep → Data Warehouse → ML Training → Predictions
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_script(script_path):
    """Run Python script and return output"""
    result = subprocess.run(
        [sys.executable, script_path],
        cwd='/opt/airflow',
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"Script failed with return code {result.returncode}")
    
    return result.stdout

def task_clean_weather():
    """Clean weather data"""
    print("=" * 60)
    print("TASK 1: Cleaning Weather Data")
    print("=" * 60)
    return run_script('scripts/clean_weather.py')

def task_clean_harvest():
    """Clean harvest data"""
    print("=" * 60)
    print("TASK 2: Cleaning Harvest Data")
    print("=" * 60)
    return run_script('scripts/clean_harvest.py')

def task_generate_dummy():
    """Generate dummy harvest data"""
    print("=" * 60)
    print("TASK 3: Generating Dummy Harvest Data")
    print("=" * 60)
    return run_script('scripts/generate_dummy_harvest.py')

def task_disaggregate():
    """Disaggregate harvest data to monthly"""
    print("=" * 60)
    print("TASK 4: Disaggregating to Monthly Data")
    print("=" * 60)
    return run_script('scripts/disaggregate_harvest.py')

def task_populate_dw():
    """Populate Data Warehouse"""
    print("=" * 60)
    print("TASK 5: Populating Data Warehouse")
    print("=" * 60)
    return run_script('scripts/populate_data_warehouse.py')

def task_ml_train_predict():
    """Train ML model and generate predictions"""
    print("=" * 60)
    print("TASK 6: ML Training & Prediction")
    print("=" * 60)
    return run_script('src/ml_train_predict_simple.py')

# Define DAG
with DAG(
    'smart_harvest_complete_pipeline',
    default_args=default_args,
    description='Complete ETL & ML Pipeline for Smart Harvest System',
    schedule_interval='@weekly',  # Run weekly, or '@daily' for daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['smart-harvest', 'etl', 'ml', 'data-warehouse'],
) as dag:

    # ===== PHASE 1: DATA PREPARATION =====
    clean_weather = PythonOperator(
        task_id='clean_weather_data',
        python_callable=task_clean_weather,
    )

    clean_harvest = PythonOperator(
        task_id='clean_harvest_data',
        python_callable=task_clean_harvest,
    )

    generate_dummy = PythonOperator(
        task_id='generate_dummy_harvest',
        python_callable=task_generate_dummy,
    )

    disaggregate = PythonOperator(
        task_id='disaggregate_to_monthly',
        python_callable=task_disaggregate,
    )

    # ===== PHASE 2: DATA WAREHOUSE =====
    populate_dw = PythonOperator(
        task_id='populate_data_warehouse',
        python_callable=task_populate_dw,
    )

    # ===== PHASE 3: MACHINE LEARNING =====
    ml_train = PythonOperator(
        task_id='ml_train_and_predict',
        python_callable=task_ml_train_predict,
    )

    # ===== PIPELINE FLOW =====
    # Phase 1: Data Prep (parallel where possible)
    [clean_weather, clean_harvest] >> generate_dummy >> disaggregate
    
    # Phase 2: Load to DW (after all data prep done)
    [clean_weather, disaggregate] >> populate_dw
    
    # Phase 3: ML (after DW populated)
    populate_dw >> ml_train
