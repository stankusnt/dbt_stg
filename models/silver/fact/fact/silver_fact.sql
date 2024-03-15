SELECT 
    -- Timestamp extrapolation
    CAST(from_unixtime( -- Convert from unixtime back to UTC
        (unix_timestamp(timestamp) -- Convert timestamp to sec from ms
        - MIN(unix_timestamp(timestamp)) OVER (PARTITION BY user, week)) -- Subtract by min timestamp over user/week
        + start_time)  -- Add start time for user/week 
    AS timestamp) AS user_timestamp, 
    CAST(unix_timestamp(timestamp) -- Convert timestamp to sec from ms
        - MIN(unix_timestamp(timestamp)) OVER (PARTITION BY user, week) -- Subtract by min timestamp over user/week
    AS timestamp) AS session_timestamp,
    current_timestamp() as lastupdated,
    -- Metrics definition
    CASE 
        WHEN left_vibration_trigger = 3 AND accelerometer_motion_flag = 1 THEN 1
        ELSE 0 
        END AS left_misuse_flag,
    CASE 
        WHEN right_vibration_trigger = 3 AND accelerometer_motion_flag = 1 THEN 1
        ELSE 0 
        END AS right_misuse_flag,
    CASE 
        WHEN hip_vibration_trigger = 3 AND accelerometer_motion_flag = 1 THEN 1
        ELSE 0 
        END AS hip_misuse_flag,
    CASE 
        WHEN (left_vibration_trigger = 3 OR right_vibration_trigger = 3 OR hip_vibration_trigger = 3) AND accelerometer_motion_flag = 1 THEN 1
        ELSE 0 
        END AS total_misuse_flag,
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
    accelerometer_motion_flag,
    file_path,
    device,
    week,
    file,
    user,
    fsr_length_adjustment,
    trial_type
FROM {{ source('dev_wh', 'eaton_stage') }} base