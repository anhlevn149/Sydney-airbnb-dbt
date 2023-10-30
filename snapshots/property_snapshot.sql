-- Create the raw_property_snapshot snapshot model using a timestamp strategy
{% snapshot property_snapshot %}

{{ 
    config(
        target_schema='raw',
        strategy='timestamp',
        unique_key='property_type',
        updated_at='scraped_date',
    ) 
}}

select 
    distinct property_type,
    scraped_date
from {{ source('raw', 'listings') }}

{% endsnapshot %}