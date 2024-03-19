SELECT 
    facility,
    md5(facility) AS facility_key,
    current_timestamp() AS lastupdated
FROM {{ source('dev_wh', 'physical_therapy_evals')}}
GROUP BY 
    facility,
    md5(facility)