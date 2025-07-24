    SELECT
        clicks.id,
        campaign_id,
        COUNT(*) AS row_cnt
    FROM {{ ref("clicks") }}
    GROUP BY
        clicks.id,
        campaign_id
    HAVING row_cnt > 1
