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
    self_test_phv as phv,
    self_test_female_partner as female_partner,
    self_test_male_partner as male_partner,
    self_test_female_child as female_child,
    self_test_male_child as male_child,
    coalesce(self_test_phv,0) + coalesce(self_test_female_partner,0) + coalesce(self_test_male_partner,0) + coalesce(self_test_female_child,0) + coalesce(self_test_male_child,0) as self_test,
    male_condoms as male_condoms,
    female_condoms as female_condoms,
    lube_gels as lube_gels
from {{ ref('int_hotspot_form2') }} main
left join {{ ref('hotspot_population_unnested') }} hspt on main.hotspot_id = hspt.id
group by 1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19