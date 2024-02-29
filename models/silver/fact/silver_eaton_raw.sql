{{
    config(
        materialized='view'
    )
}}

SELECT 
*
FROM {{ source('raw_wh', 'eatonalaysisWH') }}
WHERE TRY_CAST(Activity AS DECIMAL(5,2)) > 0  