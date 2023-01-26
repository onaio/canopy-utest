select
    id,
    location_geopoint as location_id,
    location_name,
    location_activity_types as activity_types,
    rating::integer,
    gender,
    feedback1,
    feedback2,
    feedback3,
    submitted_at as submission_date
from {{ source('ona_data', 'service_rating') }}