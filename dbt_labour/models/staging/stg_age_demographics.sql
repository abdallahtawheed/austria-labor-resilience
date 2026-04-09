with source as (
    select * from {{ source('labour', 'raw_age_demographics') }}
),

cleaned as (
    select
        "Year"::integer as year,
        trim(split_part("Province (NUTS 2-unit) <9>", '<', 1)) as region,
        case
            when "Alter unter/über 15 Jahren" = 'Under 15 years' then 'under_15'
            when "Alter unter/über 15 Jahren" = '65 years and older' then 'over_65'
            else 'working_age'
        end as age_bucket,
        ("Number" * 1000)::integer as population
    from source
    where "Year"::integer between 2013 and 2025
)

select
    year,
    region,
    sum(case when age_bucket = 'under_15' then population end) as pop_under_15,
    sum(case when age_bucket = 'working_age' then population end) as pop_working_age,
    sum(case when age_bucket = 'over_65' then population end) as pop_over_65
from cleaned
group by year, region