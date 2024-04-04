SELECT 
    -- Timestamp extrapolation
    CAST(from_unixtime( -- Convert from unixtime back to UTC
        (unix_timestamp(device_time) -- Convert timestamp to sec from ms
        - MIN(unix_timestamp(device_time)) OVER (PARTITION BY user, week)) -- Subtract by min timestamp over user/week
        + start_time)  -- Add start time for user/week 
    AS timestamp) AS user_time, 
    CAST(unix_timestamp(device_time) -- Convert timestamp to sec from ms
        - MIN(unix_timestamp(device_time)) OVER (PARTITION BY user, week) -- Subtract by min timestamp over user/week
    AS timestamp) AS session_time,
    -- Metrics definition
    CASE 
        WHEN left_vibration_trigger = 3 AND accelerometer_motion_flag = 1 THEN 1
            WHEN left_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS left_misuse_flag,
    CASE 
        WHEN right_vibration_trigger = 3 AND accelerometer_motion_flag = 1 THEN 1
            WHEN right_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS right_misuse_flag,
    CASE 
        WHEN hip_vibration_trigger = 3 AND accelerometer_motion_flag = 1 THEN 1
            WHEN hip_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS hip_misuse_flag,
    CASE 
        WHEN (left_vibration_trigger = 3 OR right_vibration_trigger = 3 OR hip_vibration_trigger = 3) AND accelerometer_motion_flag = 1 THEN 1
            WHEN COALESCE(left_vibration_trigger, right_vibration_trigger, hip_vibration_trigger) IS NOT NULL THEN 0
            ELSE NULL
    END AS total_misuse_flag,
    activity_flag,
    -- Imposing range constraints on lbf
    CASE 
        WHEN left_lbf < 0 THEN 0
        WHEN left_lbf > 100 THEN 100
        ELSE left_lbf
    END AS left_lbf,
    CASE 
        WHEN right_lbf < 0 THEN 0
        WHEN right_lbf > 100 THEN 100
        ELSE right_lbf
    END AS right_lbf,
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
    trial_type,
    run_id,
    extraction_time as extraction_time
FROM {{ source('dev_wh', 'stg_nodes') }} base