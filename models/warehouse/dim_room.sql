-- Create a new file named "dim_room.sql" in "models/warehouse"
{{
    config(
        unique_key='surrogate_key'
    )
}}

select * from {{ ref('room_stg') }}