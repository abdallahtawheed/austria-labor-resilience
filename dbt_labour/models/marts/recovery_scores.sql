with base as (
    select * from {{ ref('int_labour_joined') }}
),

pivoted as (
    select
        region,
        max(case when year = 2019 then total_employed end) as employed_2019,
        max(case when year = 2020 then total_employed end) as employed_2020,
        max(case when year = 2023 then total_employed end) as employed_2023,
        avg(old_age_dependency_ratio) as old_age_dependency_ratio,
        avg(manufacturing_share) as manufacturing_share,
        avg(healthcare_share) as healthcare_share
    from base
    group by region
)

select
    region,
    employed_2019,
    employed_2020,
    employed_2023,
    round(old_age_dependency_ratio::numeric, 2) as old_age_dependency_ratio,
    round(manufacturing_share::numeric, 2) as manufacturing_share,
    round(healthcare_share::numeric, 2) as healthcare_share,
    round((employed_2020 - employed_2019)::numeric / nullif(employed_2019, 0) * 100, 2) as shock_magnitude,
    round((employed_2023 - employed_2019)::numeric / nullif(employed_2019, 0) * 100, 2) as recovery_score
from pivoted