with rating_metrics as (
	select
		location_geopoint as location_id,
		location_name,
		sum(rating::float) as rating_sum,
		count(id) as rating_count
	from {{ ref('stg_service_ratings') }} 
	group by 1,2
)
select
	location_id,
	location_name,
	rating_sum/rating_count::integer as avg_rating
from rating_metrics