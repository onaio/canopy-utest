{% set all_columns = adapter.get_columns_in_relation(
    ref('stg_service_registration_form')
) %}
{% set except_col_names=["id", "uuid", "location_geopoint"] %}

select 
    main.id,
    main.uuid,
    main.location_geopoint,
    main.location_geopoint::jsonb -> 'coordinates' -> 1 as latitude,
    main.location_geopoint::jsonb -> 'coordinates' -> 0 as longitude,

    {%- for col in all_columns if col.name not in except_col_names %}

        coalesce(updates."{{ col.name|lower }}", main."{{ col.name|lower }}")  as "{{ col.name|lower }}"

        {%- if not loop.last %}
            ,
        {% endif %}

    {%- endfor %}

from {{ ref('stg_service_registration_form') }} main
left join {{ ref('recent_service_point_updates') }}  updates on updates.id::BIGINT = main.id