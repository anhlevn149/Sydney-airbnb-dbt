-- Create a new file named "suburb_stg.sql" in the folder "models/staging":

{{
    config(
        unique_key='suburb_name'
    )
}}

with

source  as (

    select * from {{ ref('raw_suburb') }}

),

renamed as (
    select
        upper(lga_name) as lga_name,
        upper(suburb_name) as suburb_name
    from source
),

unknown as (
    select
        'UNKNOWN' as lga_name,
        'UNKNOWN' as suburb_name
)

select * from unknown
union
select * from renamed