select
    id,
    location_geopoint::int as location_id,
    location_name,
    location_activity_types as activity_types,
    health_offered,
    orientation_offered,
    education_offered,
    protection_offered,
    justice_offered,
    rating::integer,
    gender,
    feedback1,
    feedback2,
    feedback3,
    submitted_at as submission_date
from {{ source('ona_data', 'service_rating') }}