WITH OBT_week AS (
SELECT  h.*, 
        u.*, 
        t.*,
        f.*
FROM {{ref('enriched_fact_stg')}} f
JOIN {{ref('enriched_user')}} u
    ON u.user_id = f.user_id
JOIN {{ref('enriched_hour')}} h
    ON h.hour_id = f.hour_id
JOIN {{ref('enriched_trial')}} t
    ON t.trial_id = f.trial_id
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
    -- PT metrics
    MAX(start_abc_score) AS start_abc_score,
    MAX(start_tug_score_fastest_attempt) AS start_tug_score_fastest_attempt,
    MAX(start_tandem_balance_total_time) AS start_tandem_balance_total_time,
    MAX(start_successful_tandem_balance_positions) AS start_successful_tandem_balance_positions,
    MAX(start_functional_reach) AS start_functional_reach,
    MAX(end_abc_score) AS end_abc_score,
    MAX(end_tug_score_fastest_attempt) AS end_tug_score_fastest_attempt,
    MAX(end_tandem_balance_score_total) AS end_tandem_balance_score_total,
    MAX(end_successful_tandem_balance_positions) AS end_successful_tandem_balance_positions,
    MAX(end_functional_reach) AS end_functional_reach,
    -- Survey metrics
    MAX(would_purchase_stg) AS would_purchase_stg,
    MAX(how_much_would_you_pay) AS how_much_would_you_pay,
    MAX(would_recommend_stg) AS would_recommend_stg,
    MAX(stg_helped_learn_use_walker_better) AS stg_helped_learn_use_walker_better,                                       
    user,
    week,
    week_num,
    trial_type
FROM OBT_week
WHERE user = 'U1'
GROUP BY week, user, trial_type, week_num
ORDER BY week, user, trial_type, week_num


SELECT 
    ROUND(AVG(force_threshold),0) AS average_force_thresh,
    ROUND(AVG(hip_distance_threshold),0) AS hip_distance_thresh,
    ROUND(AVG((left_lbf + right_lbf)/2),0) AS average_total_force,
    ROUND(AVG(hip_distance),0) AS average_hip_distance,
    week_num
FROM {{ref('enriched_fact_stg')}} s
JOIN {{ref('enriched_hour')}} h
    ON s.hour_id = h.hour_id
WHERE 
    left_lbf > 0
AND right_lbf > 0
GROUP BY week_num
ORDER BY week_num ASC


SELECT user, week, COUNT(*), avg(left_lbf)
FROM {{ref('curated_fact_stg')}}
where activity_flag > 0
group by user,week

    force_threshold,
    hip_distance_threshold,