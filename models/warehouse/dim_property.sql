-- Create a new file named "dim_property.sql" in "models/warehouse"
{{
    config(
        unique_key='surrogate_key'
    )
}}

select * from {{ ref('property_stg') }}