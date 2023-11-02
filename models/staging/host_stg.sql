-- Create a new file named "host_stg.sql" in the folder "models/staging":
{{
    config(
        unique_key='host_id'
    )
}}

with

source  as (

    select * from {{ ref('host_snapshot') }}

),

cleaned as (
    select
        host_id,
        case when host_name is not null then host_name else 'unknown' end as host_name,
        case when host_since ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' then to_date(host_since, 'DD/MM/YYYY') else null::date end as host_since,
        case when host_is_superhost is not null then host_is_superhost else 'unknown' end as host_is_superhost,
        upper(case when host_neighbourhood is not null then host_neighbourhood else 'unknown' end) as host_neighbourhood,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::date else dbt_valid_from::date end as dbt_valid_from,
        dbt_valid_to::date
    from source
),

unknown as (
    select
        0 as host_id,
        'unknown' as host_name,
        null::date as host_since,
        'unknown' as host_is_superhost,
        '' as host_neighbourhood,
        '1900-01-01'::date as dbt_valid_from,
        null::date as dbt_valid_to

)
select * from unknown
union all
select * from cleaned