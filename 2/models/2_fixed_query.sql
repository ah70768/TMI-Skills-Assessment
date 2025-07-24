

-- deduplicate constituent tables beforehand in CTEs

WITH campaigns AS 
(
    SELECT DISTINCT 
        client_id,
        id,
        name
    FROM {{ ref("campaigns") }}
),

clicks AS 

(
    SELECT DISTINCT
        clicks.id,
        campaign_id,
    FROM {{ ref("clicks") }}
)


SELECT
  campaigns.client_id,
  campaigns.id,
  campaigns.name,
  COALESCE(COUNT(clicks.id), 0) AS total_clicks
FROM campaigns
LEFT JOIN clicks
  ON campaigns.id = clicks.campaign_id
GROUP BY
  campaigns.client_id,
  campaigns.id,
  campaigns.name