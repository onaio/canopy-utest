with monday as (
select
    id,
    eng,
    label.label as open_monday
from (
select
    id,
    replace(jsonb_array_elements(open_monday)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label."name" = main.eng
), tuesday as (
select
    id,
    eng,
    label.label as open_tuesday
from (
select
    id,
    replace(jsonb_array_elements(open_tuesday)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label.name = main.eng
), wednesday as (
select
    id,
    eng,
    label.label as open_wednesday
from (
select
    id,
    replace(jsonb_array_elements(open_wednesday)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label.name = main.eng
), thursday as (
select
    id,
    eng,
    label.label as open_thursday
from (
select
    id,
    replace(jsonb_array_elements(open_thursday)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label.name = main.eng
), friday as (
select
    id,
    eng,
    label.label as open_friday
from (
select
    id,
    replace(jsonb_array_elements(open_friday)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label.name = main.eng
), saturday as (
select
    id,
    eng,
    label.label as open_saturday
from (
select
    id,
    replace(jsonb_array_elements(open_saturday)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label.name = main.eng
), sunday as (
select
    id,
    eng,
    label.label as open_sunday
from (
select
    id,
    replace(jsonb_array_elements(open_sunday)::varchar, '"', '') as eng
from {{ref('updated_registration_details')}}) main
left join {{ref('stg_service_point_labels')}} label on label.name = main.eng
)

select
    main.id,
    case when open_24_7 = 'yes' then '-'
        when string_agg(distinct m.open_monday, ', ') is null then 'Pas ouverte' else string_agg(distinct m.open_monday, ', ') end as open_monday,
    case when open_24_7 = 'yes' then '-'
        when string_agg(distinct t.open_tuesday, ', ') is null then 'Pas ouverte' else string_agg(distinct t.open_tuesday, ', ') end as open_tuesday,
    case when open_24_7 = 'yes' then '-'
        when string_agg(distinct w.open_wednesday, ', ') is null then 'Pas ouverte' else string_agg(distinct w.open_wednesday, ', ') end as open_wednesday,
    case when open_24_7 = 'yes' then '-'
        when string_agg(distinct th.open_thursday, ', ') is null then 'Pas ouverte' else string_agg(distinct th.open_thursday, ', ') end as open_thursday,
    case when open_24_7 = 'yes' then '-'
        when string_agg(distinct f.open_friday, ', ') is null then 'Pas ouverte' else string_agg(distinct f.open_friday, ', ') end as open_friday,
    case when open_24_7 = 'yes' then '-'
        when string_agg(distinct sa.open_saturday, ', ') is null then 'Pas ouverte' else string_agg(distinct sa.open_saturday, ', ') end as open_saturday,
    case when open_24_7 = 'yes' then '-'
        when string_agg(distinct su.open_sunday, ', ') is null then 'Pas ouverte' else string_agg(distinct su.open_sunday, ', ') end as open_sunday
from {{ref('updated_registration_details')}} main
left join monday m on m.id = main.id
left join tuesday t on t.id = main.id
left join wednesday w on w.id = main.id
left join thursday th on th.id = main.id
left join friday f on f.id = main.id
left join saturday sa on sa.id = main.id
left join sunday su on su.id = main.id
group by main.id, open_24_7
