import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://abdallah:password@localhost:5432/labour_db"
engine = create_engine(DATABASE_URL)

# ============================================================
# HELPERS
# ============================================================

def clean_region(region: str) -> str:
    """
    Strip NUTS codes from region names.
    'Burgenland <AT11>' -> 'Burgenland'
    """
    return region.split('<')[0].strip()


def clean_sector(sector: str) -> str:
    """
    Shorten sector names to readable labels.
    'SECTION C  MANUFACTURING' -> 'manufacturing'
    'SECTION Q  HUMAN HEALTH AND SOCIAL WORK ACTIVITIES' -> 'healthcare'
    """
    sector = sector.upper()
    if 'MANUFACTURING' in sector:
        return 'manufacturing'
    elif 'HEALTH' in sector:
        return 'healthcare'
    else:
        return 'other'


def clean_age_group(age: str) -> str:
    """
    Map STATcube age labels to our three analytical buckets.
    'Under 15 years'     -> 'under_15'
    '15 to 24 years' etc -> 'working_age'
    '65 years and older' -> 'over_65'
    """
    age = age.lower()
    if 'under 15' in age:
        return 'under_15'
    elif '65' in age:
        return 'over_65'
    else:
        return 'working_age'


# ============================================================
# TRANSFORM EACH TABLE
# ============================================================

def transform_sector_employment() -> pd.DataFrame:
    logger.info("Transforming sector employment data...")

    df = pd.read_sql("SELECT * FROM raw_sector_employment", engine)

    # Drop columns that carry no analytical value
    df = df.drop(columns=['Values', 'Labour Status (ILO) <5>', 'Annotations', 'Unnamed: 7'])

    # Rename for clarity before cleaning
    df = df.rename(columns={
        'Year': 'year',
        'Province (NUTS 2-unit) <9>': 'region',
        'ÖNACE 2008 Wirtschaftsabschnitt (1-Steller)': 'sector',
        'Number': 'employed_thousands'
    })

    # Clean region and sector names
    df['region'] = df['region'].apply(clean_region)
    df['sector'] = df['sector'].apply(clean_sector)

    # Convert year to integer
    df['year'] = df['year'].astype(int)

    # Convert thousands to actual persons and cast to integer
    # Original values are survey projections — decimals are artifacts not precision
    df['employed'] = (df['employed_thousands'] * 1000).astype(int)
    df = df.drop(columns=['employed_thousands'])

    # Pivot so each row is one region-year with separate columns per sector
    df_pivot = df.pivot_table(
        index=['year', 'region'],
        columns='sector',
        values='employed',
        aggfunc='sum'
    ).reset_index()

    # Flatten column names after pivot
    df_pivot.columns.name = None
    df_pivot = df_pivot.rename(columns={
        'manufacturing': 'manufacturing_employed',
        'healthcare': 'healthcare_employed'
    })

    logger.info(f"Sector employment transformed: {df_pivot.shape}")
    return df_pivot


def transform_total_employment() -> pd.DataFrame:
    logger.info("Transforming total employment data...")

    df = pd.read_sql("SELECT * FROM raw_total_employment", engine)

    df = df.drop(columns=['Values', 'Labour Status (ILO) <5>', 'Annotations', 'Unnamed: 6'])

    df = df.rename(columns={
        'Year': 'year',
        'Province (NUTS 2-unit) <9>': 'region',
        'Number': 'total_employed_thousands'
    })

    df['region'] = df['region'].apply(clean_region)
    df['year'] = df['year'].astype(int)

    df['total_employed'] = (df['total_employed_thousands'] * 1000).astype(int)
    df = df.drop(columns=['total_employed_thousands'])

    logger.info(f"Total employment transformed: {df.shape}")
    return df


def transform_age_demographics() -> pd.DataFrame:
    logger.info("Transforming age demographics data...")

    df = pd.read_sql("SELECT * FROM raw_age_demographics", engine)

    df = df.drop(columns=['Values', 'Annotations', 'Unnamed: 6'])

    df = df.rename(columns={
        'Year': 'year',
        'Province (NUTS 2-unit) <9>': 'region',
        'Alter unter/über 15 Jahren': 'age_group',
        'Number': 'population_thousands'
    })

    df['region'] = df['region'].apply(clean_region)
    df['year'] = df['year'].astype(int)
    df['age_bucket'] = df['age_group'].apply(clean_age_group)

    df['population'] = (df['population_thousands'] * 1000).astype(int)
    df = df.drop(columns=['age_group', 'population_thousands'])

    # Aggregate into three buckets per region-year
    df_agg = df.groupby(['year', 'region', 'age_bucket'])['population'].sum().reset_index()

    # Pivot to get one column per age bucket
    df_pivot = df_agg.pivot_table(
        index=['year', 'region'],
        columns='age_bucket',
        values='population',
        aggfunc='sum'
    ).reset_index()

    df_pivot.columns.name = None
    df_pivot = df_pivot.rename(columns={
        'under_15': 'pop_under_15',
        'working_age': 'pop_working_age',
        'over_65': 'pop_over_65'
    })

    # Old age dependency ratio: how many over-65 per 100 working age people
    # Higher ratio = older region = our key independent variable
    df_pivot['old_age_dependency_ratio'] = (
        df_pivot['pop_over_65'] / df_pivot['pop_working_age'] * 100
    ).round(2)

    logger.info(f"Age demographics transformed: {df_pivot.shape}")
    return df_pivot


# ============================================================
# JOIN AND WRITE MASTER TABLE
# ============================================================

def build_master_table() -> None:
    logger.info("Building master analytical table...")

    sector_df = transform_sector_employment()
    total_df = transform_total_employment()
    age_df = transform_age_demographics()

    # Join all three on year + region
    df = total_df.merge(sector_df, on=['year', 'region'], how='inner')
    df = df.merge(age_df, on=['year', 'region'], how='inner')

    # Calculate sector shares as percentage of total employed
    df['manufacturing_share'] = (
        df['manufacturing_employed'] / df['total_employed'] * 100
    ).round(2)

    df['healthcare_share'] = (
        df['healthcare_employed'] / df['total_employed'] * 100
    ).round(2)

    # Final column order for clarity
    df = df[[
        'year',
        'region',
        'total_employed',
        'manufacturing_employed',
        'healthcare_employed',
        'manufacturing_share',
        'healthcare_share',
        'pop_under_15',
        'pop_working_age',
        'pop_over_65',
        'old_age_dependency_ratio'
    ]]

    # Write to postgres
    df.to_sql('transformed_labour', engine, if_exists='replace', index=False)
    logger.info(f"Master table written: {df.shape}")
    logger.info(f"\n{df.head(10).to_string()}")


if __name__ == "__main__":
    logger.info("Starting transformation pipeline...")
    build_master_table()
    logger.info("Transformation complete.")