select
 *
from {{ source('csv', 'hotspot_labels') }}