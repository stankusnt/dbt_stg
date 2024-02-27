with
eaton_hourly as (
    select
*,
    cast(dateadd(hour, datediff(hour, 0, [datetime]), 0) as datetime2(1))
        as truncatedhour
    from {{ source("raw_wh", "eatonalaysisWH") }}
)

select
    trial_type as trialtype,
    [totalcapturedsec] = count([Activity]),
    [totalactivitymin] = cast(sum(try_cast([Activity] as decimal(5,2))) as numeric),
    [avgleft] = try_cast(avg(left_lbf) as numeric),
    [maxleft] = max(left_lbf),
    [minleft] = min(left_lbf),
    [avgright] = try_cast(avg(right_lbf) as numeric),
    [maxright] = max(right_lbf),
    [minright] = min(left_lbf),
    [forcethresh] = avg(force_threshold),
    [avghipd] = avg(hip_distance),
    [maxhipd] = max(hip_distance),
    [minhipd] = min(hip_distance),
    [hipthresh] = avg(hip_distance_threshold),
    [avgleftlat] = avg(left_latency_sec),
    [maxleftlat] = max(left_latency_sec),
    [minleftlat] = min(left_latency_sec),
    [avgrightlat] = avg(right_latency_sec),
    [maxrightlat] = max(right_latency_sec),
    [minrightlat] = min(right_latency_sec),
    [avghiplat] = avg(hip_latency_sec),
    [maxhiplat] = max(hip_latency_sec),
    [minhiplat] = min(hip_latency_sec),
    [leftmisuseevent] = sum(left_misuse_flag),
    [rightmisuseevent] = sum(right_misuse_flag),
    [hipmisuseevent] = sum(hip_misuse_flag),
    [week] as weeknum,
    [User] as [user],
    truncatedhour
from eaton_hourly
group by [truncatedhour], trial_type, [User], [week]
