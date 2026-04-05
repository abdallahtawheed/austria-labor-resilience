from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster_pipeline.assets import raw_data, transformed_data, forecast_data

# Define the full pipeline job
labour_pipeline_job = define_asset_job(
    name="labour_pipeline",
    selection=[raw_data, transformed_data, forecast_data]
)

# Schedule: run monthly on the 1st at 6am
# Statistik Austria updates data quarterly so monthly check is sufficient
monthly_schedule = ScheduleDefinition(
    job=labour_pipeline_job,
    cron_schedule="0 6 1 * *",
    name="monthly_labour_pipeline"
)

defs = Definitions(
    assets=[raw_data, transformed_data, forecast_data],
    jobs=[labour_pipeline_job],
    schedules=[monthly_schedule]
)