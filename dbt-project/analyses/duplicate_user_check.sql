SELECT user_key, user, COUNT(*)
FROM {{ ref('enriched_user') }}
GROUP BY user_key, user