import pytest
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)

EXPECTED_REGIONS = {
    'Burgenland', 'Carinthia', 'Lower Austria', 'Upper Austria',
    'Salzburg', 'Styria', 'Tyrol', 'Vorarlberg', 'Vienna'
}
EXPECTED_YEARS = set(range(2013, 2026))
EXPECTED_ROWS = 117  # 9 regions × 13 years
CI_ROWS = 26  # 2 regions × 13 years for CI sample



@pytest.fixture(scope='module')
def transformed_df():
    return pd.read_sql("SELECT * FROM transformed_labour", engine)


@pytest.fixture(scope='module')
def model_df():
    return pd.read_sql("SELECT * FROM model_output", engine)


# ============================================================
# TRANSFORMED TABLE CHECKS
# ============================================================

def test_transformed_row_count(transformed_df):
    """Table must have expected row count."""
    assert len(transformed_df) in [CI_ROWS, EXPECTED_ROWS], (
        f"Unexpected row count: {len(transformed_df)}"
    )


def test_transformed_no_nulls(transformed_df):
    """No nulls allowed in any column."""
    null_counts = transformed_df.isnull().sum()
    columns_with_nulls = null_counts[null_counts > 0]
    assert len(columns_with_nulls) == 0, (
        f"Null values found in: {columns_with_nulls.to_dict()}"
    )


def test_transformed_all_regions_present(transformed_df):
    """All regions in the table must be valid Austrian Bundesländer."""
    actual_regions = set(transformed_df['region'].unique())
    assert actual_regions.issubset(EXPECTED_REGIONS), (
        f"Unknown regions found: {actual_regions - EXPECTED_REGIONS}"
    )


def test_transformed_year_range(transformed_df):
    """Data must cover exactly 2013 to 2025."""
    actual_years = set(transformed_df['year'].astype(int).unique())
    assert actual_years == EXPECTED_YEARS, (
        f"Missing years: {EXPECTED_YEARS - actual_years}"
    )


def test_employment_figures_positive(transformed_df):
    """All employment figures must be positive."""
    assert (transformed_df['total_employed'] > 0).all()
    assert (transformed_df['manufacturing_employed'] > 0).all()
    assert (transformed_df['healthcare_employed'] > 0).all()


def test_sector_shares_valid_range(transformed_df):
    """Sector shares must be between 0 and 100."""
    assert transformed_df['manufacturing_share'].between(0, 100).all()
    assert transformed_df['healthcare_share'].between(0, 100).all()


def test_sector_employed_less_than_total(transformed_df):
    """Sector employment must never exceed total employment."""
    assert (transformed_df['manufacturing_employed'] 
            <= transformed_df['total_employed']).all()
    assert (transformed_df['healthcare_employed'] 
            <= transformed_df['total_employed']).all()


def test_dependency_ratio_realistic_range(transformed_df):
    """Old age dependency ratio must be within realistic Austrian range."""
    assert transformed_df['old_age_dependency_ratio'].between(20, 60).all(), (
        f"Dependency ratio out of range: "
        f"{transformed_df['old_age_dependency_ratio'].describe()}"
    )


# ============================================================
# MODEL OUTPUT CHECKS
# ============================================================

def test_model_output_has_forecasts(model_df):
    """Model output must contain forecast rows."""
    forecast_rows = model_df[model_df['is_forecast'] == True]
    assert len(forecast_rows) > 0, "No forecast rows found in model_output"


def test_forecast_years_correct(model_df):
    """Forecast must cover exactly 2026, 2027, 2028."""
    forecast_df = model_df[model_df['is_forecast'] == True]
    actual_years = set(forecast_df['year'].astype(int).unique())
    expected = {2026, 2027, 2028}
    assert actual_years == expected, (
        f"Expected forecast years {expected}, got {actual_years}"
    )


def test_confidence_intervals_valid(model_df):
    """yhat must always fall between yhat_lower and yhat_upper."""
    assert (model_df['yhat'] >= model_df['yhat_lower']).all()
    assert (model_df['yhat'] <= model_df['yhat_upper']).all()


def test_forecast_employment_positive(model_df):
    """Forecast employment values must be positive."""
    assert (model_df['yhat'] > 0).all()
    assert (model_df['yhat_lower'] > 0).all()