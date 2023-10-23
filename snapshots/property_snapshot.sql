-- Create the raw_property_snapshot snapshot model using a timestamp strategy
{% snapshot property_snapshot %}

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
    listing_neighbourhood,
    property_type,
    has_availability,
    scraped_date
from {{ source('raw', 'listings') }}

{% endsnapshot %}