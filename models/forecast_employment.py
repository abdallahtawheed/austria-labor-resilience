import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://abdallah:password@localhost:5432/labour_db"
engine = create_engine(DATABASE_URL)


def prepare_prophet_input(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    Prophet requires exactly two columns: ds (datestamp) and y (value).
    We use January 1st of each year as the datestamp since our data is annual.
    """
    region_df = df[df['region'] == region][['year', 'total_employed']].copy()
    region_df['ds'] = pd.to_datetime(region_df['year'].astype(str) + '-01-01')
    region_df['y'] = region_df['total_employed']
    return region_df[['ds', 'y']].sort_values('ds')


def forecast_region(df: pd.DataFrame, region: str, periods: int = 3) -> pd.DataFrame:
    prophet_df = prepare_prophet_input(df, region)

    model = Prophet(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        interval_width=0.95
    )

    model.fit(prophet_df)

    # Build future dataframe manually to avoid pandas freq compatibility issues
    last_year = prophet_df['ds'].dt.year.max()
    future_years = pd.date_range(
        start=f'{last_year + 1}-01-01',
        periods=periods,
        freq='YS'  # Year Start — stable across pandas versions
    )
    
    # Combine historical dates with future dates
    all_dates = pd.concat([
        prophet_df[['ds']],
        pd.DataFrame({'ds': future_years})
    ]).reset_index(drop=True)

    forecast = model.predict(all_dates)

    forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    forecast['region'] = region
    forecast['year'] = forecast['ds'].dt.year
    forecast['is_forecast'] = forecast['year'] > df['year'].max()

    return forecast


def build_forecast_table() -> None:
    logger.info("Loading transformed data...")
    df = pd.read_sql("SELECT * FROM transformed_labour", engine)
    df['year'] = df['year'].astype(int)

    regions = df['region'].unique()
    all_forecasts = []

    for region in regions:
        logger.info(f"Forecasting: {region}")
        forecast = forecast_region(df, region, periods=3)
        all_forecasts.append(forecast)

    combined = pd.concat(all_forecasts, ignore_index=True)

    # Round employment figures to whole persons
    combined['yhat'] = combined['yhat'].round(0).astype(int)
    combined['yhat_lower'] = combined['yhat_lower'].round(0).astype(int)
    combined['yhat_upper'] = combined['yhat_upper'].round(0).astype(int)

    combined = combined[[
        'year', 'region', 'yhat', 'yhat_lower', 'yhat_upper', 'is_forecast'
    ]]

    combined.to_sql('model_output', engine, if_exists='replace', index=False)
    logger.info(f"Forecast table written: {combined.shape}")
    logger.info(f"\n{combined[combined['is_forecast']].to_string()}")


if __name__ == "__main__":
    logger.info("Starting forecasting pipeline...")
    build_forecast_table()
    logger.info("Forecasting complete.")