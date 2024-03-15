with
eaton_hourly as (
    select
*,
    date_trunc('hour', user_timestamp)
        as truncatedhour
    from {{ ref('gold_eaton_stage') }}
)

select
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
    truncatedhour,
    upper(user) as user,
    try_cast(substring(user, len(user)-1, len(user)) as int) as usernum,
    week,
    try_cast(substring(week, len(week)-1, len(week)) as int) as weeknum,
    concat(upper(user), '.', week) as userweek,
    (
        datediff(
            day,
            max(cast(truncatedhour as date)) over (partition by week, user),
            min(cast(truncatedhour as date)) over (partition by week, user)
        )
    ) as daynum,
    truncatedhour,
    file_key
from {{ ref('gold_eaton_stage') }}
group by truncatedhour, trial_type, user, week

