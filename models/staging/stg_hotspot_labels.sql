select
 list_name,
 name,
 label
from {{ source('csv', 'hotspot_labels') }}