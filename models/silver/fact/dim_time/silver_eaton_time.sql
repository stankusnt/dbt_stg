SELECT 
    user_timestamp,
    session_timestamp,
    current_timestamp() AS lastupdated
FROM {{ ref('silver_eaton')}}