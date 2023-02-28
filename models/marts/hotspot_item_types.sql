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
)

select
    *
from item_distribution
where item_count is not null