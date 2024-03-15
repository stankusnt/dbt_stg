
select
    session_timestamp,
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
    d.user_week_key,
    f.file_key
from {{ ref('silver_eaton')}} e
left join {{ ref('files')}} f
    on f.file_path = e.file_path
left join {{ ref('gold_eaton_user')}} d
    on d.user = e.user
    and d.week = e.week
