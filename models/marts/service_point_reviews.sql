select
    rtg.id,
    location_id,
    rtg.location_name,
    svc.location_activity_types,
    rating,
    gender,
    feedback1,
    feedback2,
    feedback3,
    submission_date
from {{ ref('stg_service_ratings') }} rtg 
left join {{ ref('service_points_unnested') }} svc on rtg.location_id = svc.id
    