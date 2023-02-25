select
    *
from {{ source('ona_data', 'hotspot_utest') }}