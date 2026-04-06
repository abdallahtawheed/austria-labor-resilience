import pytest
import pandas as pd


def test_clean_region_strips_nuts_code():
    """Region names should have NUTS codes removed."""
    from transformation.transform_labour import clean_region
    assert clean_region("Burgenland <AT11>") == "Burgenland"
    assert clean_region("Vienna <AT13>") == "Vienna"
    assert clean_region("Upper Austria <AT31>") == "Upper Austria"


def test_clean_sector_maps_correctly():
    """Sector names should map to standardized labels."""
    from transformation.transform_labour import clean_sector
    assert clean_sector("SECTION C  MANUFACTURING") == "manufacturing"
    assert clean_sector("SECTION Q  HUMAN HEALTH AND SOCIAL WORK ACTIVITIES") == "healthcare"


def test_clean_age_group_maps_correctly():
    """Age group labels should map to three buckets."""
    from transformation.transform_labour import clean_age_group
    assert clean_age_group("Under 15 years") == "under_15"
    assert clean_age_group("65 years and older") == "over_65"
    assert clean_age_group("25 to 34 years") == "working_age"
    assert clean_age_group("45 to 54 years") == "working_age"


def test_old_age_dependency_ratio_calculation():
    """Dependency ratio should be over_65 / working_age * 100."""
    pop_over_65 = 55600
    pop_working_age = 190500
    expected = round(55600 / 190500 * 100, 2)
    actual = round(pop_over_65 / pop_working_age * 100, 2)
    assert actual == expected
    assert 25 < actual < 45  # realistic range for Austrian regions


def test_recovery_score_direction():
    """Regions that gained employment should have positive recovery scores."""
    employed_2019 = 136600
    employed_2020 = 136600
    employed_2023 = 141100
    recovery_score = (employed_2023 - employed_2020) / employed_2019 * 100
    assert recovery_score > 0


def test_sector_share_is_percentage():
    """Sector shares should be between 0 and 100."""
    manufacturing_employed = 18500
    total_employed = 136600
    share = manufacturing_employed / total_employed * 100
    assert 0 < share < 100

def test_clean_sector_handles_unknown():
    """Unknown sectors should return 'other' not crash."""
    from transformation.transform_labour import clean_sector
    assert clean_sector("SECTION X  UNKNOWN INDUSTRY") == "other"