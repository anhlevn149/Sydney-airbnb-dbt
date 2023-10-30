-- Create a new file named "dim_host.sql" in "models/warehouse"
{{
    config(
        unique_key='host_id'
    )
}}

select * from {{ ref('host_stg') }}