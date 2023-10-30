-- Create a new file named "census2_stg.sql" in the folder "models/staging":

{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (

    select * from {{ ref('raw_census2') }}

),

renamed as (
    select
        substring(lga_code_2016 from 5)::integer as lga_code, --extract the lga_code_2016 strings and remove the prefix "lga_" by taking only the 5th character onwards
        median_age_persons,
        median_mortgage_repay_monthly
    from source
)

select * from renamed