-- -- Create a new file named "facts_listings.sql" in "models/warehouse"
{{
    config(
        unique_key='surrogate_key'
    )
}}

with 

check_dimensions as
(select
    surrogate_key,
    listing_id,
    scraped_date,
    listing_neighbourhood,
    case when host_id in (select distinct host_id from {{ ref('host_stg') }}) then host_id else 0 end as host_id,
    case when property_type in (select distinct property_type from {{ ref('property_stg') }}) then property_type else 'unknown' end as property_type,
    case when room_type in (select distinct room_type from {{ ref('room_stg') }}) then room_type else 'unknown' end as room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
from {{ ref('listings_stg') }}),

join_lga_suburb as
(select
    lga.lga_code,
    lga.lga_name,
    suburb.suburb_name
from {{ ref('lga_stg') }} lga
left join {{ ref('suburb_stg') }} suburb
on lga.lga_name = suburb.lga_name)


select
    a.listing_id,
    a.scraped_date,
    a.listing_neighbourhood,
    b.lga_code as listing_neighbourhood_lga_code,
    a.host_id,
    c.host_name,
    c.host_since,
    c.host_is_superhost,
    c.host_neighbourhood,
    d.lga_code as host_neighbourhood_lga_code,
    d.lga_name as host_neighbourhood_lga_name,
    a.property_type,
    a.room_type,
    a.accommodates,
    a.price,
    a.has_availability,
    a.availability_30,
    a.number_of_reviews,
    a.review_scores_rating,
    a.review_scores_accuracy,
    a.review_scores_cleanliness,
    a.review_scores_checkin,
    a.review_scores_communication,
    a.review_scores_value
 from check_dimensions a
 left join staging.lga_stg b on a.listing_neighbourhood = b.lga_name 
 left join staging.host_stg c on a.host_id = c.host_id and a.scraped_date::date >= c.dbt_valid_from and a.scraped_date::date < coalesce(c.dbt_valid_to, '9999-12-31'::date)
 left join join_lga_suburb d on c.host_neighbourhood = d.suburb_name 

