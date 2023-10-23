-- Create the raw_code table by referencing the `code` table
with raw_code as (
    select
        lga_code,
        lga_name
    from {{ source('raw','code') }}
)

select * from raw_code