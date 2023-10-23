-- Create the raw_suburb table by referencing the `suburb` table
with raw_suburb as (
    select
        lga_name,
        suburb_name
    from {{ source('raw','suburb') }}
)

select * from raw_suburb