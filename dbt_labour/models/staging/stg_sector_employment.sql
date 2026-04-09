with source as (
    select * from {{ source('labour', 'raw_sector_employment') }}
),

cleaned as (
    select
        "Year"::integer as year,
        trim(split_part("Province (NUTS 2-unit) <9>", '<', 1)) as region,
        case
            when upper("ÖNACE 2008 Wirtschaftsabschnitt (1-Steller)") like '%MANUFACTUR%' then 'manufacturing'
            when upper("ÖNACE 2008 Wirtschaftsabschnitt (1-Steller)") like '%HEALTH%' then 'healthcare'
            else 'other'
        end as sector,
        ("Number" * 1000)::integer as employed
    from source
    where "Year"::integer between 2013 and 2025
)

select
    year,
    region,
    sum(case when sector = 'manufacturing' then employed end) as manufacturing_employed,
    sum(case when sector = 'healthcare' then employed end) as healthcare_employed
from cleaned
group by year, region