SELECT 
    user,
    week,
    fsr_length_adjustment,
    cast(substring(user, len(user)-1, len(user)) as int) AS user_key,
    cast(substring(weeknum, len(weeknum)-1, len(weeknum)) as int) AS week_key,
    current_timestamp() AS lastupdated
FROM {{ ref('silver_eaton')}}
GROUP BY user