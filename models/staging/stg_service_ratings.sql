select
    *
from {{ source('ona_data', 'service_rating') }}