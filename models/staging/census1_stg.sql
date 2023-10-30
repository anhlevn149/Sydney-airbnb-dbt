-- Create a new file named "census1_stg.sql" in the folder "models/staging":

{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (

    select * from {{ ref('raw_census1') }}

),

renamed as (
    select
        substring(lga_code_2016 from 5)::integer as lga_code, --extract the lga_code_2016 strings and remove the prefix "lga_" by taking only the 5th character onwards 
        Tot_P_P, 
        Age_0_4_yr_P, 
        Age_5_14_yr_P,
        Age_15_19_yr_P,
        Age_20_24_yr_P,
        Age_25_34_yr_P,
        Age_35_44_yr_P,
        Age_45_54_yr_P,
        Age_55_64_yr_P,
        Age_65_74_yr_P,
        Age_75_84_yr_P,
        Age_85ov_P
    from source
)

select * from renamed