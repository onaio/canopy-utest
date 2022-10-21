with monday as (
select
    id,
    jsonb_array_elements(open_monday) as eng
    label.label as open_monday
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.open_monday)
), tuesday as (
select
    id,
    jsonb_array_elements(open_tuesday) as eng
    label.label as open_tuesday
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.open_tuesday)
), wednesday as (
select
    id,
    jsonb_array_elements(open_wednesday) as eng
    label.label as open_wednesday
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.open_wednesday)
), thursday as (
select
    id,
    jsonb_array_elements(open_thursday) as eng
    label.label as open_thursday
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.open_thursday)
), friday as (
select
    id,
    jsonb_array_elements(open_friday) as eng
    label.label as open_friday
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.open_friday)
), saturday as (
select
    id,
    jsonb_array_elements(open_saturday) as eng
    label.label as open_saturday
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.open_saturday)
), sunday as (
select
    id,
    jsonb_array_elements(open_sunday) as eng
    label.label as open_sunday
from {{ref('updated_registration_details')}} main
left join {{ref('stg_service_point_labels')}} label on label.name = jsonb_array_elements(main.open_sunday)
)

select
    main.id,
    string_agg(m.open_monday, ',') as open_monday,
    string_agg(t.open_tuesday, ',') as open_tuesday,
    string_agg(w.open_wednesday, ',') as open_wednesday,
    string_agg(th.open_thursday, ',') as open_thursday,
    string_agg(f.open_friday, ',') as open_friday,
    string_agg(sa.open_saturday, ',') as open_saturday,
    string_agg(su.open_sunday, ',') as open_sunday
from {{ref('updated_registration_details')}} main
left join monday m on m.id = main.id
left join tuesday t on t.id = main.id
left join wednesday w on w.id = main.id
left join thursday th on th.id = main.id
left join friday f on f.id = main.id
left join saturday sa on sa.id = main.id
left join sunday su on su.id = main.id
group by 1
