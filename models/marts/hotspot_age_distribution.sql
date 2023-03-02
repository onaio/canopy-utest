with sex_label as (
    select
        sex as eng,
        label.label as sex
    from {{ ref('int_hotspot_form2') }} main
    left join {{ ref('stg_hotspot_labels') }} label on label."name" = main.sex
    where label.list_name = 'sex'
    group by 1,2
)



select
    main.id,
    hotspot_id,
    hspt.health_region,
    hspt.health_district,
    hspt.region,
    hspt.district,
    hspt.department,
    hspt.commune_prefecture,
    string_agg(hspt.population_type, ', ') as population_type,
    main.today,
    main.age,
    sex_l.sex
from {{ ref('int_hotspot_form2') }} main
left join {{ ref('hotspot_population_unnested') }} hspt on main.hotspot_id = hspt.id
left join sex_label sex_l on main.sex = sex_l.eng
group by 1,2,3,4,5,6,7,8,10,11,12