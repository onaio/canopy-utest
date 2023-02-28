{%- set group_list = ['phv', 'female_partner', 'male_partner', 'female_child', 'male_child'] -%}

with group_distribution as (
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
        unnest (array{{group_list}}) as group_type,
        unnest(array[{% for group in  group_list %}
                {{group}}
                {%- if not loop.last -%}
                    ,
                {%- endif -%}       
            {% endfor %}]) as self_tests
    from {{ ref('int_items_distributed') }}
)

select
    *
from group_distribution
where self_tests is not null