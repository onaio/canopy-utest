with main as (
    select
        id,
        hotspot_name,
        health_region,
        health_district,
        region,
        district,
        department,
        commune_prefecture,
        type_of_site,
        replace(jsonb_array_elements(population_type)::varchar, '"', '') as population_type,
        physical_site,
        virtual_site,
        type_activity_hotspot,
        duplicate,
        today
    from {{ ref('hotspot_sites') }}
), population_label as (
    select
        population_type as eng,
        label.label as population_type
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.population_type
    where label.list_name = 'population_type'
    group by 1,2
)

select
    id,
    hotspot_name,
    health_region,
    health_district,
    region,
    district,
    department,
    commune_prefecture,
    type_of_site,
    pop_label.population_type,
    physical_site,
    virtual_site,
    type_activity_hotspot,
    duplicate,
    today
from main
left join population_label pop_label on main.population_type = pop_label.eng
