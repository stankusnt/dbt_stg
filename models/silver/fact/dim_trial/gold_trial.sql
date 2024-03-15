SELECT 
    trial_type,
    MD5(trial_type) AS trial_key,
    current_timestamp() AS lastupdated
FROM {{ ref('silver_fact')}}
GROUP BY 
    trial_type,
    MD5(trial_type)