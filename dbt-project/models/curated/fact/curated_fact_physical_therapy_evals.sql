SELECT 
    NULL AS start_week,
    NULL AS end_week,
    start_abc_score,
    start_tug_score_fastest_attempt,
    start_tandem_balance_total_time,
    start_successful_tandem_balance_positions,
    start_functional_reach,
    end_abc_score,
    end_tug_score_fastest_attempt,
    end_tandem_balance_score_total,
    end_successful_tandem_balance_positions,
    end_functional_reach,
    d.device_id,
    u.user_id,
    fac.facility_id,
    current_timestamp() AS lastupdated
from {{ source('dev_wh', 'physical_therapy_evals') }} pt
left join {{ ref('enriched_device')}} d
    on d.device = pt.device
left join {{ ref('enriched_user')}} u
    on u.user = pt.user
left join {{ ref('enriched_facility')}} fac
    on fac.facility = pt.facility