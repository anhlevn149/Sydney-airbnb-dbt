-- Create a new file named "lga_stg.sql" in the folder "models/staging":
{{
    config(
        unique_key='surrogate_key'
    )
}}

with

source  as (

    select 
    md5(concat(listing_id, scraped_date)) as surrogate_key,
    * from "postgres"."raw"."listings"

),

neighbourhood_rank as (
    select
        listing_id,
        listing_neighbourhood,
        row_number() over (partition by listing_id order by count(*) desc) as neighbourhood_rank --count the occurrences of each listing_neighbourhood for each listing_id and rank them by the count in descending order
    from source
    group by listing_id, listing_neighbourhood
)

select 
    surrogate_key,
    s.listing_id,
    s.scraped_date::date,
    s.host_id,
    upper(case when r.listing_neighbourhood is not null then r.listing_neighbourhood else 'UNKNOWN' end) as listing_neighbourhood, 
    s.property_type,
    s.room_type,
    s.accommodates,
    s.price,
    s.has_availability,
    s.availability_30,
    case when s.number_of_reviews != 'NaN' then s.number_of_reviews else 0 end as number_of_reviews,
    case when s.review_scores_rating != 'NaN'  then s.review_scores_rating else 0 end as review_scores_rating,
    case when s.review_scores_accuracy != 'NaN'  then s.review_scores_accuracy else 0 end as review_scores_accuracy,
    case when s.review_scores_cleanliness != 'NaN'  then s.review_scores_cleanliness else 0 end as review_scores_cleanliness,
    case when s.review_scores_checkin != 'NaN'  then s.review_scores_checkin else 0 end as review_scores_checkin,
    case when s.review_scores_communication != 'NaN'  then s.review_scores_communication else 0 end as review_scores_communication,
    case when s.review_scores_value != 'NaN'  then s.review_scores_value else 0 end as review_scores_value
from source s
left join neighbourhood_rank r
on s.listing_id = r.listing_id
where r.neighbourhood_rank = 1