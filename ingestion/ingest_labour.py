import pandas as pd
from sqlalchemy import create_engine, text
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# DB conn
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)


# file paths
SECTOR_FILE = "data/raw/table_sectorbyregionbyyear_2026-04-04_00-41-46.csv"
AGE_FILE = "data/raw/table_totalagebyregionbyyear_2026-04-04_01-29-40.csv"
EMPLOYMENT_FILE = "data/raw/table_totalbyregionbyyear_2026-04-04_01-29-40.csv"

# ============================================================
# PARSING CONSTANTS
# ============================================================
SKIPROWS = 9
ENCODING = 'latin1'

VALID_YEARS = [str(y) for y in range(2013, 2026)]


def load_csv(filepath: str) -> pd.DataFrame:
    """Load a STATcube CSV export into a raw dataframe."""
    df = pd.read_csv(
        filepath,
        encoding=ENCODING,
        sep=',',
        skiprows=SKIPROWS,
        quotechar='"'
    )
    return df


def drop_metadata_rows(df: pd.DataFrame, year_col: str = 'Year') -> pd.DataFrame:
    """
    STATcube appends metadata rows at the bottom of the file.
    We keep only rows where Year is a valid year in our analysis window.
    """
    return df[df[year_col].isin(VALID_YEARS)].copy()


def write_to_postgres(df: pd.DataFrame, table_name: str) -> None:
    """Write dataframe to PostgreSQL, replacing table if it exists.
    Uses CASCADE drop to remove any dependent dbt views before recreating."""
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {table_name} CASCADE'))
        conn.commit()
    df.to_sql(
        table_name,
        engine,
        if_exists='replace',
        index=False
    )
    logger.info(f"Written {len(df)} rows to table '{table_name}'")


def ingest_sector_data() -> None:
    logger.info("Ingesting sector employment data...")
    df = load_csv(SECTOR_FILE)
    df = drop_metadata_rows(df)
    write_to_postgres(df, 'raw_sector_employment')
    logger.info(f"Sector data shape: {df.shape}")


def ingest_age_data() -> None:
    logger.info("Ingesting age demographics data...")
    df = load_csv(AGE_FILE)
    df = drop_metadata_rows(df)
    write_to_postgres(df, 'raw_age_demographics')
    logger.info(f"Age data shape: {df.shape}")


def ingest_employment_data() -> None:
    logger.info("Ingesting total employment data...")
    df = load_csv(EMPLOYMENT_FILE)
    df = drop_metadata_rows(df)
    write_to_postgres(df, 'raw_total_employment')
    logger.info(f"Employment data shape: {df.shape}")


if __name__ == "__main__":
    logger.info("Starting ingestion pipeline...")
    ingest_sector_data()
    ingest_age_data()
    ingest_employment_data()
    logger.info("Ingestion complete.")