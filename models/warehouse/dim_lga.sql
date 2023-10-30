-- Create a new file named "dim_lga.sql" in "models/warehouse"
{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('lga_stg') }}