    SELECT 
        file,
        file_path,
        md5(file_path) as file_key
    FROM {{ref('silver_eaton')}}
    GROUP BY file, file_path, file_key