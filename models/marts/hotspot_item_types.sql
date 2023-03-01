{%- set item_list = ['self_test', 'male_condoms', 'female_condoms', 'lube_gels'] -%}

with item_distribution as (
    select
        id,
        hotspot_id,
        health_region,
        health_district,
        region,
        district,
        department,
        commune_prefecture,
        population_type,
        today,
        unnest (array{{item_list}}) as item_type,
        unnest(array[{% for item in  item_list %}
            {{item}}
            {%- if not loop.last -%}
                ,
            {%- endif -%}       
        {% endfor %}]) as item_count
    from {{ ref('int_items_distributed') }}
), item_label as (
    select
        item_type as eng,
        label.label as item_type
    from item_distribution
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = item_distribution.item_type
    where label.list_name = 'item_type'
    group by 1,2
)

select
    id,
    hotspot_id,
    health_region,
    health_district,
    region,
    district,
    department,
    commune_prefecture,
    population_type,
    today,
    item_l.item_type,
    item_count
from item_distribution
left join item_label item_l on item_distribution.item_type = item_l.eng
where item_count is not null