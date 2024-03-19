SELECT 
    COALESCE(f.user, pt.user, s.user) AS user,
    md5(COALESCE(f.user, pt.user, s.user)) AS user_key,
    current_timestamp() AS lastupdated
FROM {{ ref('curated_fact_stg')}} f
FULL JOIN {{ source('dev_wh', 'physical_therapy_evals')}} pt
    ON pt.user = f.user
FULL JOIN {{ source('dev_wh', 'user_surveys')}} s
    ON s.user = f.user
WHERE COALESCE(f.user, pt.user, s.user) IS NOT NULL
GROUP BY 
    COALESCE(f.user, pt.user, s.user),
    md5(COALESCE(f.user, pt.user, s.user))