with
eaton_hourly as (
    select
*,
    date_trunc('hour', user_timestamp)
        as truncatedhour
    from {{ ref('silver_eaton') }}
)

select
    trial_type as trialtype,
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
    week as week,
    TRIM(user) as user,
    truncatedhour
from eaton_hourly
group by truncatedhour, trial_type, user, week
