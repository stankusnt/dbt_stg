SELECT 
    would_purchase_stg,
    how_much_would_you_pay,
    would_recommend_stg,
    stg_helped_learn_use_walker_better,
    d.device_key,
    u.user_key,
    fac.facility_key,
    current_timestamp() AS lastupdated
FROM {{ source('dev_wh', 'user_surveys') }} us
left join {{ ref('enriched_device')}} d
    on d.device = us.device
left join {{ ref('enriched_user')}} u
    on u.user = us.user
left join {{ ref('enriched_facility')}} fac
    on fac.facility = us.facility