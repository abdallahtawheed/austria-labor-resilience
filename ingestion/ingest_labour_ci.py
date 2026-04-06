import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)

# CI uses sample data — 2 regions, all years
# Full dataset lives in data/raw/ which is gitignored
SECTOR_FILE = "data/samples/sample_sector.csv"
AGE_FILE = "data/samples/sample_age.csv"
EMPLOYMENT_FILE = "data/samples/sample_employment.csv"

VALID_YEARS = [str(y) for y in range(2013, 2026)]


def drop_metadata_rows(df: pd.DataFrame, year_col: str = 'Year') -> pd.DataFrame:
    return df[df[year_col].isin(VALID_YEARS)].copy()


def ingest_ci() -> None:
    logger.info("CI ingestion — loading sample data...")

    sector_df = pd.read_csv(SECTOR_FILE)
    age_df = pd.read_csv(AGE_FILE)
    employment_df = pd.read_csv(EMPLOYMENT_FILE)

    # Sample CSVs come from the database directly so no metadata rows to skip
    # Column names are already clean from the raw postgres tables
    sector_df.to_sql('raw_sector_employment', engine, 
                     if_exists='replace', index=False)
    logger.info(f"Sector sample loaded: {sector_df.shape}")

    age_df.to_sql('raw_age_demographics', engine, 
                  if_exists='replace', index=False)
    logger.info(f"Age sample loaded: {age_df.shape}")

    employment_df.to_sql('raw_total_employment', engine, 
                         if_exists='replace', index=False)
    logger.info(f"Employment sample loaded: {employment_df.shape}")

    logger.info("CI ingestion complete.")


if __name__ == "__main__":
    ingest_ci()