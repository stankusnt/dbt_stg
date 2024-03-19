SELECT 
    trial_type,
    MD5(trial_type) AS trial_key,
    current_timestamp() AS lastupdated
FROM {{ ref('stg_silver_fact')}}
GROUP BY 
    trial_type,
    MD5(trial_type)