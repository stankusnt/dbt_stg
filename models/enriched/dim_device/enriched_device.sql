SELECT 
    COALESCE(f.device, pt.device, s.device) AS device,
    md5(COALESCE(f.device, pt.device, s.device)) AS device_id,
    current_timestamp() AS lastupdated
FROM {{ ref('curated_fact_stg')}} f
FULL JOIN {{ source('dev_wh', 'physical_therapy_evals')}} pt
    ON pt.device = f.device
FULL JOIN {{ source('dev_wh', 'user_surveys')}} s
    ON s.device = f.device
WHERE COALESCE(f.device, pt.device, s.device) IS NOT NULL
GROUP BY 
    COALESCE(f.device, pt.device, s.device),
    md5(COALESCE(f.device, pt.device, s.device))