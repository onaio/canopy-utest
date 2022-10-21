with activity_types as (
select
    id,
    jsonb_array_elements(location_activity_types) as eng
    label.label as location_activity_types
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.location_activity_types)
), health as (
select
    id,
    jsonb_array_elements(health_offered) as eng
    label.label as health_offered
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.health_offered)
), orientation as (
select
    id,
    jsonb_array_elements(orientation_offered) as eng
    label.label as orientation_offered
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.orientation_offered)
), education as (
select
    id,
    jsonb_array_elements(education_offered) as eng
    label.label as education_offered
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.education_offered)
), protection as (
select
    id,
    jsonb_array_elements(protection_offered) as eng
    label.label as protection_offered
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.protection_offered)
), justice as (
select
    id,
    jsonb_array_elements(justice_offered) as eng
    label.label as justice_offered
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.justice_offered)
)


select
    main.id,
    l.location_activity_types as location_activity_types,
    h.health_offered as health_offered,
    orientation_offered as orientation_offered,
    education_offered as education_offered,
    protection_offered as protection_offered,
    justice_offered as justice_offered
from {{ref('updated_registration_details')}} main
left join activity_types l on l.id = main.id
left join health h on h.id = main.id
left join orientation o on o.id = main.id
left join education e on e.id = main.id
left join protection p on p.id = main.id
left join justice j on j.id = main.id
group by 1