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
        population_type,
        replace(jsonb_array_elements(physical_site)::varchar, '"', '') as physical_site,
        replace(jsonb_array_elements(virtual_site)::varchar, '"', '') as virtual_site,
        type_activity_hotspot,
        duplicate,
        today
    from {{ ref('hotspot_population_type_unnested') }}
), physical_label as (
    select
        physical_site as eng,
        label.label as physical_site
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.physical_site
    group by 1,2
), virtual_label as (
    select
        virtual_site as eng,
        label.label as virtual_site
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.virtual_site
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
    population_type,
    phy_label.physical_site,
    vrt_label.virtual_site,
    type_activity_hotspot,
    duplicate,
    today
from main
left join physical_label phy_label on main.physical_site = phy_label.eng
left join virtual_label vrt_label on main.virtual_site = vrt_label.eng
