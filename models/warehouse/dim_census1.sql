-- Create a new file named "dim_census1.sql" in "models/warehouse"
{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('census1_stg') }}