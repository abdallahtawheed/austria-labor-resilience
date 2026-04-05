from dagster import asset, AssetExecutionContext
import subprocess
import sys


@asset
def raw_data(context: AssetExecutionContext):
    """
    Extract and load raw CSVs from Statistik Austria into PostgreSQL.
    Produces three raw tables: raw_sector_employment,
    raw_age_demographics, raw_total_employment.
    """
    context.log.info("Running ingestion pipeline...")
    result = subprocess.run(
        [sys.executable, "ingestion/ingest_labour.py"],
        capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed:\n{result.stderr}")
    context.log.info("Raw data loaded successfully.")


@asset(deps=[raw_data])
def transformed_data(context: AssetExecutionContext):
    """
    Clean, reshape and join raw tables into transformed_labour
    and recovery_scores analytical tables.
    Depends on: raw_data
    """
    context.log.info("Running transformation pipeline...")
    result = subprocess.run(
        [sys.executable, "transformation/transform_labour.py"],
        capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Transformation failed:\n{result.stderr}")
    context.log.info("Transformation complete.")


@asset(deps=[transformed_data])
def forecast_data(context: AssetExecutionContext):
    """
    Fit Prophet models on regional employment data and write
    2026-2028 forecasts to model_output table.
    Depends on: transformed_data
    """
    context.log.info("Running forecasting pipeline...")
    result = subprocess.run(
        [sys.executable, "models/forecast_employment.py"],
        capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Forecasting failed:\n{result.stderr}")
    context.log.info("Forecast complete.")