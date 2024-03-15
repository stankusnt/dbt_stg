SELECT 
    week,
    date_trunc('day', user_timestamp) AS day,
    date_trunc('hour', user_timestamp) AS hour,
    md5(date_trunc('hour', user_timestamp) || week) AS hour_key,
    current_timestamp() AS lastupdated
FROM {{ ref('silver_fact')}}
GROUP BY 
    week,
    date_trunc('day', user_timestamp),
    date_trunc('hour', user_timestamp),
    md5(date_trunc('hour', user_timestamp) || week)