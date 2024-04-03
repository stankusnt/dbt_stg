SELECT 
    week,
    cast(substring(week, len(week)-1, len(week)) AS int) AS week_num,
    date_trunc('day', user_time) AS day,
    date_trunc('hour', user_time) AS hour,
    md5(date_trunc('hour', user_time) || week) AS hour_id,
    current_timestamp() AS lastupdated
FROM {{ ref('curated_fact_stg')}}
GROUP BY 
    week,
    date_trunc('day', user_time),
    date_trunc('hour', user_time),
    md5(date_trunc('hour', user_time) || week)
