select
    id,
    hotspot::int as hotspot_id,
    today,
    age,
    theme_ccc_session as theme,
    no_test_distributed_phv as self_test_phv,
    no_test_distributed_female_partner as self_test_female_partner,
    no_test_distributed_male_partner as self_test_male_partner,
    no_test_distributed_female_child as self_test_female_child,
    no_test_distributed_male_child as self_test_male_child,
    no_male_condoms_distributed as male_condoms,
    no_female_condoms_distributed as female_condoms,
    no_lube_gels_distributed as lube_gels
from {{ ref('stg_hotspot_form2') }}
