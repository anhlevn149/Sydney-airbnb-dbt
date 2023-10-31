select 
	*,
	(total_revenue / distinct_hosts) as revenue_per_host
from 
(select 
	host_neighbourhood_lga_name,
	to_date(concat(extract(month from scraped_date)
		   , '-'
		   , extract(year from scraped_date)), 'MM/YYYY') as month_year,
	count(distinct host_id) as distinct_hosts,
	sum(price * (30 - case when has_availability='t' then availability_30 end)) as total_revenue
from {{ ref('facts_listings') }}
group by host_neighbourhood_lga_name, month_year
order by host_neighbourhood_lga_name, month_year
) foo


