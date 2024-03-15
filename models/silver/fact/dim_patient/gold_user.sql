SELECT 
    user,
    cast(substring(user, 2, len(user)) AS int) AS user_key,
    current_timestamp() AS lastupdated
FROM {{ ref('silver_fact')}}
GROUP BY 
    user,
    cast(substring(user, 2, len(user)) AS int)