-- Create a new file named "dim_lga.sql" in "models/warehouse"
{{
    config(
        unique_key='suburb_name'
    )
}}

select * from {{ ref('suburb_stg') }}