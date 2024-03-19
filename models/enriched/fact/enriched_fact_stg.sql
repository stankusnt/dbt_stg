
select
    left_misuse_flag,
    right_misuse_flag,
    hip_misuse_flag,
    total_misuse_flag,
    accelerometer_motion_flag AS activity_sec,
    left_lbf,
    right_lbf,
    left_adc,
    right_adc,
    hip_distance,
    left_vibration_trigger,
    right_vibration_trigger,
    hip_vibration_trigger,
    force_threshold,
    hip_distance_threshold,
    baseline_left_adc,
    baseline_right_adc,
    left_adc_change,
    right_adc_change,
    fsr_length_adjustment,
    -- PT metrics
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
    -- Survey metrics
    would_purchase_stg,
    how_much_would_you_pay,
    would_recommend_stg,
    stg_helped_learn_use_walker_better,
    
    u.user_key,
    h.hour_key,
    t.trial_key,
    d.device_key,
    current_timestamp() as lastupdated
from {{ ref('curated_fact_stg')}} e
join {{ ref('enriched_user')}} u
    on u.user = e.user
join {{ ref('enriched_hour')}} h
    on h.week = e.week
    and h.hour = date_trunc('hour', e.user_timestamp)
join {{ ref('enriched_trial')}} t
    on t.trial_type = e.trial_type
join {{ ref('enriched_device')}} d 
    on d.device = e.device
left join {{ ref('curated_fact_physical_therapy_evals')}} pt
    on pt.user_key = e.user_key
    and pt.device_key = e.device_key
left join {{ ref('curated_fact_user_surveys')}} us
    on us.user_key = e.user_key
    and us.device_key = e.device_key