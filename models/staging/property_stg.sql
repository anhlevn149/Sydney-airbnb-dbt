-- Create a new file named "property_stg.sql" in the folder "models/staging":
{{
    config(
        unique_key='property_type'
    )
}}

with

source  as (

    select * from {{ ref('property_snapshot') }}

),

renamed as (
    select
        scraped_date,
        property_type,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::date else dbt_valid_from::date end as dbt_valid_from,
        dbt_valid_to::date
    from source
),

unknown as (
    select
        null as scraped_date,
        'unknown' as property_type,
        '1900-01-01'::date as dbt_valid_from,
        null::date as dbt_valid_to

)
select 
    *
from unknown
union all
select 
    *    
from renamed