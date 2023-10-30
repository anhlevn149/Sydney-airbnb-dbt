-- Create a new file named "dim_census2.sql" in "models/warehouse"
{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('census2_stg') }}