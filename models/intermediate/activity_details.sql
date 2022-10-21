with activity_types as (
select
    id,
    eng,
    label.label as location_activity_types
from (
select
    id,
    replace(jsonb_array_elements(location_activity_types)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label."name" = main.eng
), health as (
select
    id,
    eng,
    label.label as health_offered
from (
select
    id,
    replace(jsonb_array_elements(health_offered)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label."name" = main.eng
), orientation as (
select
    id,
    eng,
    label.label as orientation_offered
from (
select
    id,
    replace(jsonb_array_elements(orientation_offered)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label."name" = main.eng
), education as (
select
    id,
    eng,
    label.label as education_offered
from (
select
    id,
    replace(jsonb_array_elements(education_offered)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label."name" = main.eng
), protection as (
select
    id,
    eng,
    label.label as protection_offered
from (
select
    id,
    replace(jsonb_array_elements(protection_offered)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label."name" = main.eng
), justice as (
select
    id,
    eng,
    label.label as justice_offered
from (
select
    id,
    replace(jsonb_array_elements(justice_offered)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label."name" = main.eng
)


select
    main.id,
    string_agg(distinct l.location_activity_types, ', ') as location_activity_types,
    case when string_agg(distinct h.health_offered, ', ') is null then '-' else string_agg(distinct h.health_offered, ', ') end as health_offered,
    case when string_agg(distinct o.orientation_offered, ', ') is null then '-' else string_agg(distinct o.orientation_offered, ', ') end as orientation_offered,
    case when string_agg(distinct e.education_offered, ', ') is null then '-' else string_agg(distinct e.education_offered, ', ') end as education_offered,
    case when string_agg(distinct p.protection_offered, ', ') is null then '-' else string_agg(distinct p.protection_offered, ', ') end as protection_offered,
    case when string_agg(distinct j.justice_offered, ', ') is null then '-' else string_agg(distinct j.justice_offered, ', ') end as justice_offered
from {{ref('updated_registration_details')}} main
left join activity_types l on l.id = main.id
left join health h on h.id = main.id
left join orientation o on o.id = main.id
left join education e on e.id = main.id
left join protection p on p.id = main.id
left join justice j on j.id = main.id
group by 1