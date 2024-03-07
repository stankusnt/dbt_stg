{{
    config(
        materialized='view'
    )
}}

SELECT 
*
FROM {{ source('dev_wh', 'eaton_analysis') }}
WHERE TRY_CAST(Activity AS DECIMAL(5,2)) > 0  