SELECT DISTINCT 
    COALESCE(f.user, pt.user, s.user) AS user,
    s.age,
    s.gender,
    s.height_in,
    s.weight_lbs,
    s.dob,
    s.walker_trainer,
    s.walker_purchased_from,
    s.walker_manufacturer,
    md5(COALESCE(f.user, pt.user, s.user)) AS user_id,
    current_timestamp() AS lastupdated
FROM {{ ref('curated_fact_stg')}} f
LEFT JOIN {{ source('dev_wh', 'physical_therapy_evals')}} pt
    ON pt.user = f.user
LEFT JOIN {{ source('dev_wh', 'user_surveys')}} s
    ON s.user = f.user
WHERE COALESCE(f.user, pt.user, s.user) IS NOT NULL