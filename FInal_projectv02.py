from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import zipfile
import sqlite3
import subprocess
import os

# Defining Dynamic Paths 
USER_HOME = os.path.expanduser("~")
BASE_DIR = os.path.join(USER_HOME, "airflow", "datasets", "FinalProject")

# File Paths
RAW_ZIP_PATH = os.path.join(BASE_DIR, "weather-dataset.zip")
RAW_CSV_PATH = os.path.join(BASE_DIR, "weatherHistory.csv")
CLEANED_CSV_PATH = os.path.join(BASE_DIR, "cleaned_weather.csv")
DAILY_CSV_PATH = os.path.join(BASE_DIR, "daily_weather.csv")
MONTHLY_CSV_PATH = os.path.join(BASE_DIR, "monthly_weather.csv")
DB_PATH = os.path.join(BASE_DIR, "weather.db")


default_args = {
    "owner": "Group_03",
    "depends_on_past": False,
    "start_date": days_ago(1), 
    "email_on_failure": False,
    "retries": 0,  
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "final_projectv2_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL Pipeline for Historical Weather Data (Kaggle)",
    tags=['weather', 'etl', 'final_project']
)
#-----------------------------(Member A)------------------------------------------------------------------
# Task 1: Download
def download_kaggle_dataset(**kwargs):
    """Downloads dataset using Kaggle API."""
    if not os.path.exists(BASE_DIR):
        print(f"Creating directory: {BASE_DIR}")
        os.makedirs(BASE_DIR)
        
    cmd = [
        "kaggle", "datasets", "download",
        "-d", "muthuj7/weather-dataset",
        "-p", BASE_DIR,
        "--force"
    ]
    subprocess.run(cmd, check=True)
    print(f"Dataset downloaded to {BASE_DIR}")

download_task = PythonOperator(
    task_id="download_dataset",
    python_callable=download_kaggle_dataset,
    dag=dag,
)

# Task 2: Extract
def extract_weather_data(**kwargs):
    """Unzips the downloaded file."""
    if os.path.exists(RAW_ZIP_PATH):
        with zipfile.ZipFile(RAW_ZIP_PATH, "r") as zip_ref:
            zip_ref.extractall(BASE_DIR)
        print(f"Extracted to {RAW_CSV_PATH}")
    else:
        raise FileNotFoundError(f"Zip file not found at {RAW_ZIP_PATH}")
    
    # Push raw file path to XCom for next task
    kwargs["ti"].xcom_push(key="raw_file_path", value=RAW_CSV_PATH)

extract_task = PythonOperator(
    task_id="extract_zip",
    python_callable=extract_weather_data,
    dag=dag,
)

#-----------------------------(Member B)------------------------------------------------------------------
# Task 3: Transform (Clean & Feature Engineering)
def clean_transform_data(**kwargs):
    """Cleans data, fills nulls, and adds Wind Strength category."""
    raw_path = kwargs["ti"].xcom_pull(key="raw_file_path", task_ids="extract_zip")
    
    print(f"Reading raw data from {raw_path}")
    df = pd.read_csv(raw_path)

    # Standardize Column Names
    df = df.rename(columns={
        "Formatted Date": "formatted_date",
        "Precip Type": "precip_type",
        "Temperature (C)": "temperature_c",
        "Apparent Temperature (C)": "apparent_temperature_c",
        "Humidity": "humidity",
        "Wind Speed (km/h)": "wind_speed_kmh",
        "Visibility (km)": "visibility_km",
        "Pressure (millibars)": "pressure_millibars"
    })

    # Convert Date
    df["formatted_date"] = pd.to_datetime(df["formatted_date"], errors="coerce", utc=True)
    df["formatted_date"] = df["formatted_date"].dt.tz_convert(None)
        
    # Handle Missing Values (Numeric: Median)
    num_cols = [
        "temperature_c", "apparent_temperature_c", "humidity",
        "wind_speed_kmh", "visibility_km", "pressure_millibars"
    ]
    df[num_cols] = df[num_cols].fillna(df[num_cols].median())

    # Handle Missing Values (Categorical: Mode)
    df["precip_type"] = df["precip_type"].replace("null", pd.NA)
    df["precip_type"] = df["precip_type"].fillna(df["precip_type"].mode()[0])

    # Feature Engineering: Wind Strength
    def categorize_wind(speed):
        if speed <= 5.4: return "Calm"
        elif speed <= 11.9: return "Light Air"
        elif speed <= 19.4: return "Light Breeze"
        elif speed <= 28.4: return "Gentle Breeze"
        elif speed <= 38.5: return "Moderate Breeze"
        elif speed <= 49.7: return "Fresh Breeze"
        elif speed <= 61.6: return "Strong Breeze"
        elif speed <= 74.5: return "Near Gale"
        elif speed <= 87.8: return "Gale"
        elif speed <= 102.2: return "Strong Gale"
        elif speed <= 117.4: return "Storm"
        else: return "Violent Storm"

    df["wind_strength"] = df["wind_speed_kmh"].apply(categorize_wind)
    
    df.to_csv(CLEANED_CSV_PATH, index=False)
    kwargs["ti"].xcom_push(key="cleaned_file", value=CLEANED_CSV_PATH)

clean_task = PythonOperator(
    task_id="clean_transform",
    python_callable=clean_transform_data,
    dag=dag,
)

# Task 4: Transform (Daily Aggregates)
def create_daily_aggregates(**kwargs):
    """Calculates daily averages."""
    cleaned_file = kwargs["ti"].xcom_pull(key="cleaned_file", task_ids="clean_transform")
    df = pd.read_csv(cleaned_file)

    df["formatted_date"] = pd.to_datetime(df["formatted_date"])
    df = df.set_index("formatted_date")

    daily_avg = df.resample("D").agg({
        "temperature_c": "mean",
        "humidity": "mean",
        "wind_speed_kmh": "mean"
    }).reset_index()

    daily_avg = daily_avg.rename(columns={
        "temperature_c": "avg_temperature_c",
        "humidity": "avg_humidity",
        "wind_speed_kmh": "avg_wind_speed_kmh"
    })

    daily_raw = df.sort_index().groupby(df.index.normalize()).first().reset_index()
    daily_raw = daily_raw.rename(columns={daily_raw.columns[0]: "formatted_date"})

    daily_df = daily_raw.merge(daily_avg, on="formatted_date", how="left")
    
    # Final Selection
    cols = [
        "formatted_date", "precip_type", "temperature_c", "apparent_temperature_c",
        "humidity", "wind_speed_kmh", "visibility_km", "pressure_millibars",
        "wind_strength", "avg_temperature_c", "avg_humidity", "avg_wind_speed_kmh"
    ]
    daily_df = daily_df[cols]
    
    daily_df.to_csv(DAILY_CSV_PATH, index=False)
    kwargs["ti"].xcom_push("daily_file", DAILY_CSV_PATH)

daily_task = PythonOperator(
    task_id="daily_aggregates",
    python_callable=create_daily_aggregates,
    dag=dag,
)

# Task 5: Transform (Monthly Aggregates)
def create_monthly_aggregates(**kwargs):
    """Calculates monthly statistics."""
    cleaned_file = kwargs["ti"].xcom_pull(key="cleaned_file", task_ids="clean_transform")
    df = pd.read_csv(cleaned_file)

    df["formatted_date"] = pd.to_datetime(df["formatted_date"])
    df["year"] = df["formatted_date"].dt.year
    df["month"] = df["formatted_date"].dt.month

    monthly = df.groupby(["year", "month"]).agg({
        "temperature_c": "mean",
        "apparent_temperature_c": "mean",
        "humidity": "mean",
        "visibility_km": "mean",
        "pressure_millibars": "mean",
    }).reset_index()

    modes = (
        df.groupby(["year","month"])["precip_type"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        .reset_index()
        .rename(columns={"precip_type": "mode_precip_type"})
    )

    monthly = monthly.merge(modes, on=["year", "month"], how="left")
    
    monthly = monthly.rename(columns={
        "temperature_c": "avg_temperature_c",
        "apparent_temperature_c": "avg_apparent_temperature_c",
        "humidity": "avg_humidity",
        "visibility_km": "avg_visibility_km",
        "pressure_millibars": "avg_pressure_millibars"
    })

    # Create 'month' string column 
    monthly['month_str'] = monthly['year'].astype(str) + '-' + monthly['month'].astype(str).str.zfill(2)
    
    cols = [
        "month_str", "avg_temperature_c", "avg_apparent_temperature_c",
        "avg_humidity", "avg_visibility_km", "avg_pressure_millibars", "mode_precip_type"
    ]
    monthly_final = monthly[cols].rename(columns={"month_str": "month"})
    
    monthly_final.to_csv(MONTHLY_CSV_PATH, index=False)
    kwargs["ti"].xcom_push("monthly_file", MONTHLY_CSV_PATH)

monthly_task = PythonOperator(
    task_id="monthly_aggregates",
    python_callable=create_monthly_aggregates,
    dag=dag,
)

#-----------------------------(Member C)------------------------------------------------------------------
# Task 6: Validation
def validate_data(**kwargs):
    """ Check data quality. Raises ValueError (stops pipeline) if constraints are violated."""
    
    daily_file = kwargs["ti"].xcom_pull(key="daily_file", task_ids="daily_aggregates")
    print(f"Validating file: {daily_file}")
    
    daily_df = pd.read_csv(daily_file)

    # Check for Missing Values
    if daily_df.isnull().any().any():
        raise ValueError("Validation Failed: Dataset contains missing values.")

    # Check Temperature Range (-80 to +80)
    if (daily_df["temperature_c"] < -80).any() or (daily_df["temperature_c"] > 80).any():
        raise ValueError("Validation Failed: Temperature out of range (-80 to +80).")
    
    # Check Humidity Range (0 to 1)
    if (daily_df["humidity"] < 0).any() or (daily_df["humidity"] > 1).any():
        raise ValueError("Validation Failed: Humidity must be between 0 and 1.")

    print("Validation Passed: Data is clean.")

validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag,
)

# Task 7: Load to DB (Member C)
def load_data_to_sqlite(**kwargs):
    """Load validated data to SQLite. Only runs if Validation succeeds."""
    
    daily_file = kwargs["ti"].xcom_pull(key="daily_file", task_ids="daily_aggregates")
    monthly_file = kwargs["ti"].xcom_pull(key="monthly_file", task_ids="monthly_aggregates")

    daily_df = pd.read_csv(daily_file)
    monthly_df = pd.read_csv(monthly_file)

    print(f"Connecting to SQLite DB at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    
    # Create Tables (DDL)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formatted_date TEXT,
            precip_type TEXT,
            temperature_c REAL,
            apparent_temperature_c REAL,
            humidity REAL,
            wind_speed_kmh REAL,
            visibility_km REAL,
            pressure_millibars REAL,
            wind_strength TEXT,
            avg_temperature_c REAL,
            avg_humidity REAL,
            avg_wind_speed_kmh REAL
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT,
            avg_temperature_c REAL,
            avg_apparent_temperature_c REAL,
            avg_humidity REAL,
            avg_visibility_km REAL,
            avg_pressure_millibars REAL,
            mode_precip_type TEXT
        )
    """)
    
    # Load Data
    daily_df.to_sql("daily_weather", conn, if_exists="append", index=False)
    monthly_df.to_sql("monthly_weather", conn, if_exists="append", index=False)
    
    conn.close()
    print("Successfully loaded data into weather.db")

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data_to_sqlite,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

# PIPELINE DEPENDENCIES

download_task >> extract_task >> clean_task >> [daily_task, monthly_task] >> validate_task >> load_task
