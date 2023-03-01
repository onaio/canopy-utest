with main as (
    select  
        id,
        hotspot_name,
        latitude,
        longitude,
        health_region,
        health_district,
        region,
        district,
        department,
        commune_prefecture,
        today,
        replace(jsonb_array_elements(type_activity_hotspot)::varchar, '"', '') as type_activity_hotspot
    from {{ ref('hotspot_sites') }}
    where type_of_site = 'Physique'
), activity_label as (
    select
        type_activity_hotspot as eng,
        label.label as type_activity_hotspot
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.type_activity_hotspot
    where label.list_name = 'type_activity_hotspot'
    group by 1,2
)

select
    main.id,
    main.hotspot_name,
    latitude,
    longitude,
    main.health_region,
    main.health_district,
    main.region,
    main.district,
    main.department,
    main.commune_prefecture,
    main.today,
    pop.population_type as population_type,
    string_agg(pop.population_type, ', ') as population_type_list,
    string_agg(act_l.type_activity_hotspot, ', ') as type_activity_hotspot
from main
left join {{ ref('hotspot_population_unnested') }} pop on main.id = pop.id
left join activity_label act_l on main.type_activity_hotspot = act_l.eng
group by 1,2,3,4,5,6,7,8,9,10,11,12