SELECT 
    user,
    cast(substring(user, 2, len(user)) AS int) AS user_key,
    current_timestamp() AS lastupdated
FROM {{ ref('stg_silver_fact')}}
GROUP BY 
    user,
    cast(substring(user, 2, len(user)) AS int)