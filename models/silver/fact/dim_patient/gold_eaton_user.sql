SELECT 
    user,
    device,
    week,
    trial_type,
    fsr_length_adjustment,
    concat(user, concat('.', week)) AS user_week,
    cast(substring(user, 2, len(user)) AS int) AS user_key,
    cast(substring(week, len(week)-1, len(week)) as int) AS week_key,
    concat(cast(substring(user, 2, len(user)) AS int),
        cast(substring(week, len(week)-1, len(week)) as int)) AS user_week_key,
    current_timestamp() AS lastupdated
FROM {{ ref('silver_eaton')}}