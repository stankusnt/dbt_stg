SELECT 
    self_reported_physical_activity_per_week_hrs,
    satisfied_with_activity,
    fear_of_falling,
    fall_history,
    length_of_time_using_walker,
    likes_using_walker,
    received_walker_training,
    description_of_changes_in_DLa,
    would_purchase_stg,
    how_much_would_you_pay,
    would_recommend_stg,
    stg_helped_learn_use_walker_better,
    d.device_id,
    u.user_id,
    fac.facility_id,
    current_timestamp() AS lastupdated
FROM {{ source('dev_wh', 'user_surveys') }} us
left join {{ ref('enriched_device')}} d
    on d.device = us.device
left join {{ ref('enriched_user')}} u
    on u.user = us.user
left join {{ ref('enriched_facility')}} fac
    on fac.facility = us.facility