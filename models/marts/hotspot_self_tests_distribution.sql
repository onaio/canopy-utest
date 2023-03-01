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
), group_label as (
    select
        group_type as eng,
        label.label as group_type
    from group_distribution
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = group_distribution.group_type
    where label.list_name = 'group_type'
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
        group_l.group_type,
        self_tests
from group_distribution
left join group_label group_l on group_distribution.group_type = group_l.eng 
where self_tests is not null