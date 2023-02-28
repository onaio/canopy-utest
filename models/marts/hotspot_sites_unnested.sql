{%- set site_types = ['physical_site', 'virtual_site'] -%}

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
    from {{ ref('hotspot_population_unnested') }}
),{% for type in  site_types %}
    {{type}}_label as (
    select
        {{type}} as eng,
        label.label as {{type}}
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.{{type}}
    where list_name = '{{type}}'
    group by 1,2
    )
    {%- if not loop.last -%}
        ,
    {%- endif -%}       
{% endfor %}


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
    physical_site_l.physical_site,
    virtual_site_l.virtual_site,
    type_activity_hotspot,
    duplicate,
    today
from main
{% for type in  site_types %}
    left join {{type}}_label {{type}}_l on main.{{type}} = {{type}}_l.eng    
{% endfor %}