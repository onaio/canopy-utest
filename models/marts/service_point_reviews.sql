select
    rtg.id::int,
    location_geopoint::int as location_id,
    location_name,
    unnest(string_to_array(act.location_activity_types, ', ')) as location_activity_types,
    rating,
    gender,
    age,
    feedback,
    submitted_at as submission_date
from {{ ref('stg_service_ratings') }} rtg
left join {{ ref('int_activities_rated') }} act on rtg.id = act.id