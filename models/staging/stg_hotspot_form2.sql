select
    *
from {{ source('ona_data', 'hotspot_utest_form2') }}