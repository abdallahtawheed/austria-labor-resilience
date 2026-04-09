with source as (
    select * from {{ source('labour', 'raw_total_employment') }}
)

select
    "Year"::integer as year,
    trim(split_part("Province (NUTS 2-unit) <9>", '<', 1)) as region,
    ("Number" * 1000)::integer as total_employed
from source
where "Year"::integer between 2013 and 2025