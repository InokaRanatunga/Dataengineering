from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import zipfile
import sqlite3
import subprocess
import os

data_dir = "/home/chamith/airflow/datasets/FinalProject"

raw_zip_path = f"{data_dir}/weather-dataset.zip"
raw_csv_path = f"{data_dir}/weatherHistory.csv"

cleaned_out = f"{data_dir}/cleaned_weather.csv"
daily_out = f"{data_dir}/daily_weather.csv"
monthly_out = f"{data_dir}/monthly_weather.csv"

db_path = f"{data_dir}/weather.db"

default_args = {
    "owner": "teamrocks",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "final_project_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="etl workflow for Final Project",
)

# Task 1 – download dataset from kaggle
def download_kaggle_dataset(**kwargs):
    cmd = [
        "kaggle", "datasets", "download",
        "-d", "muthuj7/weather-dataset",
        "-p", data_dir,
        "--force"
    ]
    subprocess.run(cmd, check=True)

download_task = PythonOperator(
    task_id="download_task",
    python_callable=download_kaggle_dataset,
    dag=dag,
)

# Task 2 – extract zip
def extract_weather_data(**kwargs):
    if os.path.exists(raw_zip_path):
        with zipfile.ZipFile(raw_zip_path, "r") as zip_ref:
            zip_ref.extractall(data_dir)
    kwargs["ti"].xcom_push(key="raw_file_path", value=raw_csv_path)

extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract_weather_data,
    dag=dag,
)

# Task 3 – clean and engineer features
def clean_transform(**kwargs):
    raw_path = kwargs["ti"].xcom_pull(key="raw_file_path", task_ids="extract_task")
    df = pd.read_csv(raw_path)

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

    df["formatted_date"] = pd.to_datetime(df["formatted_date"], errors="coerce", utc=True)
    df["formatted_date"] = df["formatted_date"].dt.tz_convert(None)
        
    #Filling missing value with median for the numeric coloumns
    num_cols = [
        "temperature_c",
        "apparent_temperature_c",
        "humidity",
        "wind_speed_kmh",
        "visibility_km",
        "pressure_millibars"
    ]
    df[num_cols] = df[num_cols].fillna(df[num_cols].median())

    # Convert null to NaN
    df["precip_type"] = df["precip_type"].replace("null", pd.NA)

    # Fill missing with mode
    df["precip_type"] = df["precip_type"].fillna(df["precip_type"].mode()[0])

    def categorize(speed):
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

    df["wind_strength"] = df["wind_speed_kmh"].apply(categorize)
    df.to_csv(cleaned_out, index=False)

    kwargs["ti"].xcom_push(key="cleaned_file", value=cleaned_out)

clean_task = PythonOperator(
    task_id="clean_task",
    python_callable=clean_transform,
    dag=dag,
)

# Task 4 – daily aggregates
def daily_agg(**kwargs):
    cleaned_file = kwargs["ti"].xcom_pull(key="cleaned_file", task_ids="clean_task")
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
    daily_df = daily_df[[
        "formatted_date",
        "precip_type",
        "temperature_c",
        "apparent_temperature_c",
        "humidity",
        "wind_speed_kmh",
        "visibility_km",
        "pressure_millibars",
        "wind_strength",
        "avg_temperature_c",
        "avg_humidity",
        "avg_wind_speed_kmh"
    ]]
    daily_df.to_csv(daily_out, index=False)

    kwargs["ti"].xcom_push("daily_file", daily_out)

daily_task = PythonOperator(
    task_id="daily_task",
    python_callable=daily_agg,
    dag=dag,
)

# Task 5 – monthly aggregates
def monthly_agg(**kwargs):
    cleaned_file = kwargs["ti"].xcom_pull(key="cleaned_file", task_ids="clean_task")
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
    monthly.to_csv(monthly_out, index=False)

    kwargs["ti"].xcom_push("monthly_file", monthly_out)

monthly_task = PythonOperator(
    task_id="monthly_task",
    python_callable=monthly_agg,
    dag=dag,
)

# Task 6 – validation
def validate_weather_data(**kwargs):
    daily_file = kwargs["ti"].xcom_pull(key="daily_file", task_ids="daily_task")
    daily_df = pd.read_csv(daily_file)

    if daily_df.isnull().any().any():
        print("validation failed: missing values")
        return "failed"

    if (daily_df["temperature_c"] < -80).any() or (daily_df["temperature_c"] > 80).any():
        print("validation failed: temperature out of range")
        return "failed"

    print("validation passed")
    return "passed"

validate_task = PythonOperator(
    task_id="validate_task",
    python_callable=validate_weather_data,
    dag=dag,
)

# Task 7 – load to sqlite
def load_weather_data(**kwargs):
    status = kwargs["ti"].xcom_pull(task_ids="validate_task")
    if status != "passed":
        print("validation failed – skipping load")
        return

    daily_file = kwargs["ti"].xcom_pull(key="daily_file", task_ids="daily_task")
    monthly_file = kwargs["ti"].xcom_pull(key="monthly_file", task_ids="monthly_task")

    daily_df = pd.read_csv(daily_file)
    monthly_df = pd.read_csv(monthly_file)

    conn = sqlite3.connect(db_path)
    daily_df.to_sql("daily_weather", conn, if_exists="replace", index=False)
    monthly_df.to_sql("monthly_weather", conn, if_exists="replace", index=False)
    conn.close()

    print("loaded data into weather.db")

load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_weather_data,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

# single-line dag flow
download_task >> extract_task >> clean_task >> [daily_task, monthly_task] >> validate_task >> load_task
