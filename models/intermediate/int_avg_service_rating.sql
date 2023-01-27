with rating_metrics as (
	select
		location_id,
		location_name,
		sum(rating) as rating_sum,
		count(id) as rating_count
	from {{ ref('stg_service_ratings') }} 
	group by 1,2
)
select
	location_id,
	location_name,
	rating_sum/rating_count as avg_rating
from rating_metrics