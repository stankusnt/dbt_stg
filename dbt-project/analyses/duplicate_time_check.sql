
    SELECT 
        base.week,
        base.user,
        count(*) AS time_sec


    FROM {{ source('dev_wh', 'stg_nodes') }} base
    --WHERE base.User = 'U1' AND base.week = 'Week 1'
    GROUP BY base.user, base.week, unix_timestamp(base.timestamp)/1000
    HAVING count(*) > 1
