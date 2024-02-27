select
    totalcapturedsec,
    totalactivitymin,
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
    avgleftlat,
    maxleftlat,
    minleftlat,
    avgrightlat,
    maxrightlat,
    minrightlat,
    avghiplat,
    maxhiplat,
    minhiplat,
    leftmisuseevent,
    rightmisuseevent,
    hipmisuseevent,
    trialtype,
    upper([user]) as [user],
    weeknum,
    concat(upper([user]), '.', weeknum) as userweek,
    (
        datediff(
            day,
            max(cast(truncatedhour as date)) over (partition by weeknum, user),
            min(cast(truncatedhour as date)) over (partition by weeknum, user)
        )
    ) as daynum,
    cast(truncatedhour as datetime2(0)) as truncatedhour
from {{ ref('silver_eaton_stage') }}
