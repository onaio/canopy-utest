{%- set service_list = ['health_offered', 'orientation_offered', 'education_offered', 'protection_offered', 'justice_offered'] -%}

with unnested_activities as(
    select
        id,
        unnest(string_to_array(location_activity_types, ',')) as location_activity_types
    from {{ ref('int_activities_rated') }}
), unnested_services as (select
    id,
    {% for service in  service_list %}
        unnest(string_to_array({{service}}, ',')) as {{service}}
        {%- if not loop.last -%}
            ,
        {%- endif -%}       
    {% endfor %}
    from {{ ref('int_activities_rated') }}
    ), combined_services as (
        select
            id,
            unnest(array[health_offered, orientation_offered, education_offered, protection_offered, justice_offered]) as services
        from unnested_services
    )


select
    rtg.id::int,
    location_geopoint::int as location_id,
    location_name,
    act.location_activity_types as location_activity_types,
    coalesce(svc.services, '-') as rated_services,
    rating,
    gender,
    feedback,
    submitted_at as submission_date
from {{ ref('stg_service_ratings') }} rtg
left join unnested_activities act on rtg.id = act.id
left join combined_services svc on rtg.id = svc.id
