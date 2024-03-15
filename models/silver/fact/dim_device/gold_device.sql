SELECT 
    device,
    md5(device) AS device_key,
    current_timestamp() AS lastupdated
FROM {{ ref('silver_fact')}}
GROUP BY 
    device,
    md5(device)