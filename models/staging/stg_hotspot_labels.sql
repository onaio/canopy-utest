select
 id,
 list_name,
 name,
 label
from {{ source('csv', 'hotspot_labels') }}
order by id asc