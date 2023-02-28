select
    main.id,
    hotspot_id,
    hspt.health_region,
    hspt.health_district,
    hspt.region,
    hspt.district,
    hspt.department,
    hspt.commune_prefecture,
    hspt.population_type,
    main.today,
    self_test_phv as phv,
    self_test_female_partner as female_partner,
    self_test_male_partner as male_partner,
    self_test_female_child as female_child,
    self_test_male_child as male_child,
    self_test_phv + self_test_female_partner + self_test_male_partner + self_test_female_child + self_test_male_child as self_test,
    male_condoms as male_condoms,
    female_condoms as female_condoms,
    lube_gels as lube_gels
from {{ ref('int_hotspot_form2') }} main
left join {{ ref('hotspot_population_unnested') }} hspt on main.hotspot_id = hspt.id
