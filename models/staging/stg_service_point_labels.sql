SELECT 
    list_name,
    name,
    label
FROM {{source('csv', 'service_point_labels')}}