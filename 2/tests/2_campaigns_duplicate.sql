    SELECT  
        client_id,
        id,
        name,
        COUNT(*) AS row_cnt
    FROM {{ ref("campaigns") }}
    GROUP BY
        client_id,
        id,
        name
    HAVING row_cnt > 1
