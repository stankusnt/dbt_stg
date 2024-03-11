SELECT *
FROM {{ source('dev_wh', 'eaton_stage_time') }}