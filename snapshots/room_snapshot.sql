-- Create the room_snapshot snapshot model using a timestamp strategy
{% snapshot room_snapshot %}

{{ 
    config(
        target_schema='raw',
        strategy='timestamp',
        unique_key='room_type',
        updated_at='scraped_date',
    ) 
}}

select 
    distinct
    room_type,
    scraped_date
from {{ source('raw', 'listings') }}

{% endsnapshot %}