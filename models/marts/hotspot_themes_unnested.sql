with main as (
    select
        id,
        hotspot_id,
        today,
        replace(jsonb_array_elements(theme)::varchar, '"', '') as theme
    from {{ ref('int_hotspot_form2') }}
), theme_label as (
    select
        theme as eng,
        label.label as theme
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.theme
    where label.list_name = 'theme'
    group by 1,2
)

select
    main.id,
    hotspot_id,
    hspt.health_region,
    hspt.health_district,
    hspt.region,
    hspt.district,
    hspt.department,
    hspt.commune_prefecture,
    hspt.population_type,
    main.today,
    theme_l.theme
from main
left join theme_label theme_l on main.theme = theme_l.eng
left join {{ ref('hotspot_population_unnested') }} hspt on main.hotspot_id = hspt.id