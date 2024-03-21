SELECT 
    week,
    cast(substring(week, len(week)-1, len(week)) AS int) AS week_num,
    date_trunc('day', user_timestamp) AS day,
    date_trunc('hour', user_timestamp) AS hour,
    md5(date_trunc('hour', user_timestamp) || week) AS hour_id,
    current_timestamp() AS lastupdated
FROM {{ ref('curated_fact_stg')}}
GROUP BY 
    week,
    date_trunc('day', user_timestamp),
    date_trunc('hour', user_timestamp),
    md5(date_trunc('hour', user_timestamp) || week)
