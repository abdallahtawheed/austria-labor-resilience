with total as (
    select * from {{ ref('stg_total_employment') }}
),

sectors as (
    select * from {{ ref('stg_sector_employment') }}
),

demographics as (
    select * from {{ ref('stg_age_demographics') }}
)

select
    t.year,
    t.region,
    t.total_employed,
    s.manufacturing_employed,
    s.healthcare_employed,
    round(s.manufacturing_employed::numeric / nullif(t.total_employed, 0) * 100, 2) as manufacturing_share,
    round(s.healthcare_employed::numeric / nullif(t.total_employed, 0) * 100, 2) as healthcare_share,
    d.pop_under_15,
    d.pop_working_age,
    d.pop_over_65,
    round(d.pop_over_65::numeric / nullif(d.pop_working_age, 0) * 100, 2) as old_age_dependency_ratio
from total t
inner join sectors s on t.year = s.year and t.region = s.region
inner join demographics d on t.year = d.year and t.region = d.region