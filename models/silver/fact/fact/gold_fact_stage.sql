
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
    u.user_key,
    w.hour_key,
    t.trial_key,
    f.file_key,
    current_timestamp() as lastupdated
from {{ ref('silver_fact')}} e
join {{ ref('files')}} f
    on f.file_path = e.file_path
join {{ ref('gold_user')}} u
    on u.user = e.user
join {{ ref('gold_hour')}} h
    on h.week = e.week
    and date_trunc('hour', h.user_timestamp) = date_trunc('hour', e.user_timestamp)
join {{ ref('gold_trial')}} t
    on t.trial_type = e.trial_type