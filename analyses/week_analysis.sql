WITH OBT_week AS (
SELECT  h.*, 
        u.*, 
        t.*,
        f.*
FROM {{ref('gold_fact_stage')}} f
JOIN {{ref('gold_user')}} u
    ON u.user_key = f.user_key
JOIN {{ref('gold_hour')}} h
    ON h.hour_key = f.hour_key
JOIN {{ref('gold_trial')}} t
    ON t.trial_key = f.trial_key
)


select
    count(1) as totalcapturedsec,
    CAST(SUM(activity_sec) AS NUMERIC) AS totalactivitysec,
    TRY_CAST(AVG(left_lbf) AS NUMERIC) AS avgleft,
    MAX(left_lbf) AS maxleft,
    MIN(left_lbf) AS minleft,
    TRY_CAST(AVG(right_lbf) AS NUMERIC) AS avgright,
    MAX(right_lbf) AS maxright,
    MIN(right_lbf) AS minright,
    AVG(force_threshold) AS forcethresh,
    AVG(hip_distance) AS avghipd,
    MAX(hip_distance) AS maxhipd,
    MIN(hip_distance) AS minhipd,
    AVG(hip_distance_threshold) AS hipthresh,
    SUM(left_misuse_flag) AS leftmisuseevent,
    SUM(right_misuse_flag) AS rightmisuseevent,
    SUM(hip_misuse_flag) AS hipmisuseevent,
    SUM(total_misuse_flag) AS totalmisuseevent,                                       
    user,
    week,
    week_num,
    trial_type
FROM OBT_week
WHERE user = 'U1'
GROUP BY week, user, trial_type, week_num
ORDER BY week, user, trial_type, week_num