select
    rvw.id,
    rvw.location_id,
    rvw.location_name,
    rvw.location_activity_types,
    avg_rating
from {{ ref('service_point_reviews') }} rvw
left join {{ ref('int_avg_service_rating') }} avg on rvw.location_id = avg.location_id::int