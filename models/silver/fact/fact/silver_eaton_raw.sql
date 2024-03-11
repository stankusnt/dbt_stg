SELECT 
    base.left_lbf,
    base.right_lbf,
    base.left_adc,
    base.right_adc,
    base.hip_distance,
    base.left_vibration_trigger,
    base.right_vibration_trigger,
    base.hip_vibration_trigger,
    base.force_threshold,
    base.hip_distance_threshold,
    base.baseline_left_adc,
    base.baseline_right_adc,
    base.left_adc_change,
    base.right_adc_change,
    base.accelerometer_motion_flag,
        from_unixtime( -- Convert from unixtime back to UTC
        (unix_timestamp(base.timestamp)/1000 -- Convert timestamp to sec from ms
        - MIN(unix_timestamp(base.timestamp)/1000) OVER (PARTITION BY base.user, base.week)) -- Subtract by min timestamp over user/week
        + unix_timestamp(t.start_time_utc))  -- Add start time for user/week 
        AS user_timestamp, 
    unix_timestamp(base.timestamp)/1000 -- Convert timestamp to sec from ms
        - MIN(unix_timestamp(base.timestamp)/1000) OVER (PARTITION BY base.user, base.week) -- Subtract by min timestamp over user/week
        AS session_timestamp,
    base.test_type,
    base.device,
    base.user
FROM {{ source('dev_wh', 'eaton_stage') }} base
JOIN {{ source('dev_wh', 'eaton_start_times') }} t
    ON t.week = base.week AND t.user = base.user