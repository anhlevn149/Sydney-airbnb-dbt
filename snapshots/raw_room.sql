-- Create the room_snapshot snapshot model using a timestamp strategy
{% snapshot room_snapshot %}

{{ 
    config(
        target_schema='raw',
        strategy='timestamp',
        unique_key='listing_id',
        updated_at='scraped_date',
    ) 
}}

select 
    listing_id,
    room_type,
    scraped_date
from {{ source('raw', 'listings') }}

{% endsnapshot %}