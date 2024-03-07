SELECT a.*
FROM {{ ref('silver_eaton_raw') }} a 
JOIN {{ ref('silver_eaton_hourly') }} b
ON a.week = b.week
WHERE b.week = 'Week 12'