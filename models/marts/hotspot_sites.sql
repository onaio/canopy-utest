{%- set list_name = ['health_region', 'health_district', 'region', 'district', 'department', 'commune_prefecture', 'type_of_site'] -%}


with {% for name in  list_name %}
    {{name}}_label as (
    select
        {{name}} as eng,
        label.label as {{name}}
    from {{ ref('stg_hotspot_form1') }} main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.{{name}}
    group by 1,2
    )
    {%- if not loop.last -%}
        ,
    {%- endif -%}       
{% endfor %}


select
    main.id,
    hotspot_name,
    hotspot_gps -> 'coordinates' -> 1 as latitude,
    hotspot_gps -> 'coordinates' -> 0 as longitude,
    health_region_l.health_region,
    health_district_l.health_district,
    region_l.region,
    district_l.district,
    department_l.department,
    commune_prefecture_l.commune_prefecture,
    type_of_site_l.type_of_site,
    physical_site,
    virtual_site,
    type_activity_hotspot,
    population_type,
    duplicate,
    min_site_visits,
    max_site_visits,
    today
from {{ ref('stg_hotspot_form1') }} main
{% for name in  list_name %}
    left join {{name}}_label {{name}}_l on main.{{name}} = {{name}}_l.eng    
{% endfor %}