select
    rtg.id::int,
    location_geopoint::int as location_id,
    location_name,
    act.location_activity_types,
    act.health_offered,
    act.orientation_offered,
    act.education_offered,
    act.protection_offered,
    act.justice_offered,
    rating,
    gender,
    feedback,
    submitted_at as submission_date
from {{ ref('stg_service_ratings') }} rtg 
left join {{ ref('int_activities_rated') }} act on rtg.id = act.id