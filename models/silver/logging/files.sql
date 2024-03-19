    SELECT 
        file,
        file_path,
        md5(file || file_path) as file_key
    FROM {{ref('stg_silver_fact')}}
    GROUP BY 
        file, 
        file_path, 
        md5(file || file_path)