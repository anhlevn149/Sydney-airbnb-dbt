with 

total as
(select 
	listing_neighbourhood,
    to_date(concat(extract(month from scraped_date)
		   , '-'
		   , extract(year from scraped_date)), 'MM/YYYY') as month_year,
	count(distinct listing_id) as total_listings,
	count(distinct host_id) as total_distinct_hosts
from {{ ref('facts_listings') }}
group by listing_neighbourhood , month_year
),

superhost as
(
select
	listing_neighbourhood,
    to_date(concat(extract(month from scraped_date)
		   , '-'
		   , extract(year from scraped_date)), 'MM/YYYY') as month_year,
	count(distinct host_id) as active_distinct_hosts
from {{ ref('facts_listings') }}
where host_is_superhost = 't'
group by listing_neighbourhood , month_year
),

inactive as
(
select
	listing_neighbourhood,
	month_year,
	lag(count(distinct listing_id)) over (order by month_year) as inactive_listings_original,
	count(distinct listing_id) as inactive_listings_final
from
(select 
	listing_neighbourhood,
    to_date(concat(extract(month from scraped_date)
		   , '-'
		   , extract(year from scraped_date)), 'MM/YYYY') as month_year,
	listing_id
from {{ ref('facts_listings') }}
where has_availability = 'f'
group by listing_neighbourhood , month_year, listing_id
) sub
group by listing_neighbourhood , month_year
),

active as
(
select
	listing_neighbourhood,
	month_year,
	(count(distinct listing_id)) as active_listings,
    min(price)::integer as min_price,
    max(price)::integer as max_price,
    percentile_cont(0.5) within group (order by price)::integer as median_price,
    avg(price)::integer as average_price,
    avg(review_scores_rating)::integer as average_review_scores_rating,
    count(distinct listing_id) as active_listings_final,
	lag(count(distinct listing_id)) over (order by month_year) as active_listings_original,
	avg(price * number_of_stays)::integer as average_revenue_per_listing
from 
(select 
	listing_neighbourhood,
    to_date(concat(extract(month from scraped_date)
		   , '-'
		   , extract(year from scraped_date)), 'MM/YYYY') as month_year,
	listing_id,
	host_id,
	review_scores_rating,
	sum(30 - availability_30) as number_of_stays,
    price
from {{ ref('facts_listings') }} 
where has_availability='t'
group by listing_neighbourhood , month_year, listing_id, host_id, review_scores_rating, price
) sub
group by listing_neighbourhood , month_year
)


select 
	a.listing_neighbourhood,
	a.month_year,
	a.active_listings,
	b.total_listings,
	case 
		when b.total_listings = 0 or b.total_listings is null or a.active_listings = 0 or a.active_listings is null
		then 0
		else (a.active_listings * 100 / b.total_listings)::integer
	end as active_listings_rate,
	a.min_price,
	a.max_price,
	a.median_price,
	a.average_price,
	b.total_distinct_hosts,
	c.active_distinct_hosts,
	case 
		when c.active_distinct_hosts = 0 or c.active_distinct_hosts is null or b.total_distinct_hosts = 0 or b.total_distinct_hosts is null
		then 0
		else (c.active_distinct_hosts * 100 / b.total_distinct_hosts )::integer
	end as superhost_rate,
	a.average_review_scores_rating,
	case 
		when a.active_listings_original = 0 or a.active_listings_original is null or a.active_listings_final = 0 or a.active_listings_final is null
		then 0 
		else ((a.active_listings_final - a.active_listings_original) * 100 / a.active_listings_original)::integer
	end as active_listings_percent_change,
	case
		when d.inactive_listings_original = 0 or d.inactive_listings_original is null or d.inactive_listings_final = 0 or d.inactive_listings_final is null
		then 0
		else ((d.inactive_listings_final - d.inactive_listings_original) * 100 / d.inactive_listings_original)::integer 
	end as inactive_listings_percent_change,
	a.average_revenue_per_listing
from active a
left join total b on a.listing_neighbourhood = b.listing_neighbourhood and a.month_year = b.month_year
left join superhost c on a.listing_neighbourhood = c.listing_neighbourhood and a.month_year = c.month_year
left join inactive d  on a.listing_neighbourhood = d.listing_neighbourhood and a.month_year = d.month_year
order by listing_neighbourhood , month_year
