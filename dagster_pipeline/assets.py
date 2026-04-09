import subprocess
import sys
from dagster import asset

@asset
def raw_data():
    subprocess.run(
        [sys.executable, "ingestion/ingest_labour.py"],
        check=True
    )

@asset(deps=[raw_data])
def dbt_models():
    subprocess.run(
        ["dbt", "run", "--project-dir", "dbt_labour", "--profiles-dir", "dbt_labour"],
        check=True
    )
    subprocess.run(
        ["dbt", "test", "--project-dir", "dbt_labour", "--profiles-dir", "dbt_labour"],
        check=True
    )

@asset(deps=[dbt_models])
def forecast_data():
    subprocess.run(
        [sys.executable, "models/forecast_employment.py"],
        check=True
    )