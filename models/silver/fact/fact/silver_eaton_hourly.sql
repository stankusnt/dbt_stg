select
    totalcapturedsec,
    (totalactivitysec*60) as totalactivitymin,
    avgleft,
    maxleft,
    minleft,
    avgright,
    maxright,
    minright,
    forcethresh,
    avghipd,
    maxhipd,
    minhipd,
    hipthresh,
    leftmisuseevent,
    rightmisuseevent,
    hipmisuseevent,
    totalmisuseevent,
    trialtype,
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
    truncatedhour
from {{ ref('silver_eaton_stage') }}
