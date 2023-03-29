{%- set field_list = ['peak_hour_max_number_people_pc', 'peak_day_max_number_people_pc', 'peak_hour_max_number_people_jav', 'peak_day_max_number_people_jav', 'peak_hour_max_number_people_ud', 'peak_day_max_number_people_ud'] -%}
{%- set day_list = ['peak_day_max_number_people_pc', 'peak_day_max_number_people_jav', 'peak_day_max_number_people_ud'] -%}
{%- set time_list = ['peak_hour_max_number_people_pc', 'peak_hour_max_number_people_jav', 'peak_hour_max_number_people_ud'] -%}

with elements as (
    select
        id,
        hotspot_name,
        type_of_site,
        duplicate,
        today,
        {% for field in  field_list %}
            replace(jsonb_array_elements({{field}})::varchar, '"', '') as {{field}}
            {%- if not loop.last -%}
                ,
            {%- endif -%}       
        {% endfor %}
    from {{ ref('hotspot_sites') }}
), main as (
    select
        id,
        hotspot_name,
        type_of_site,
        duplicate,
        today,
        unnest (array{{day_list}}) as day_group_type,
        unnest(array[{% for day in  day_list %}
                    {{day}}
                    {%- if not loop.last -%}
                        ,
                    {%- endif -%}       
                {% endfor %}]) as peak_day,
        unnest (array{{time_list}}) as time_group_type,
        unnest(array[{% for time in  time_list %}
                    {{time}}
                    {%- if not loop.last -%}
                        ,
                    {%- endif -%}       
                {% endfor %}]) as peak_time
    from elements
), day_label as (
    select
        peak_day as eng,
        label.label as peak_day
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.peak_day
    where label.list_name = 'peak_day'
    group by 1,2
), time_label as (
    select
        peak_time as eng,
        label.label as peak_time
    from main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.peak_time
    where label.list_name = 'peak_time'
    group by 1,2
)

select
    id,
    hotspot_name,
    type_of_site,
    case when (day_group_type = 'peak_day_max_number_people_pc' and time_group_type = 'peak_hour_max_number_people_pc') then 'PC'
         when (day_group_type = 'peak_day_max_number_people_jav' and time_group_type = 'peak_hour_max_number_people_jav') then 'JAV'
         when (day_group_type = 'peak_day_max_number_people_ud' and time_group_type = 'peak_hour_max_number_people_ud') then 'UD' else null end as group_type,
    day_l.peak_day,
    time_l.peak_time,
    duplicate,
    today
from main
left join day_label day_l on main.peak_day = day_l.eng
left join time_label time_l on main.peak_time = time_l.eng
where main.peak_day notnull and main.peak_time notnull
