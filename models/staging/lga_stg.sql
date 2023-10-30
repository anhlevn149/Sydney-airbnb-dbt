-- Create a new file named "lga_stg.sql" in the folder "models/staging":

{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (

    select * from {{ ref('raw_lga') }}

),

renamed as (
    select
        lga_code,
        upper(lga_name) as lga_name
    from source
),

unknown as (
    select
        0 as lga_code,
        'UNKNOWN' as lga_name
)

select * from unknown
union
select * from renamed