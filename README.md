
# 1. Google, Meta, TikTok Ads Data Ingestion


## Architecture & Reasoning

Hybrid architecture that primarily uses **PyAirbyte**, an open-source Python library, to ingest advertising data from Facebook Ads, Google Ads, and TikTok into `BigQuery`. 

However, depending on the business requirements such as custom transformations or budgeting restrictions, selectively implement **custom ETL pipelines** using Python and Airflow.

- **PyAirbyte:**  
 PyAirbyte provides pre-built source and destionation [connectors ](https://docs.airbyte.com/integrations/ ) specifically in our use case of ingesting data from Facebook Ads, Google Ads, and TikTok to BigQuery. 

- **Custom ETL (where necessary):**  
  If more granular control over extraction and transformation is required, a custom pipeline will be developed.

This hybrid approach strikes a balance by using the PyAirbyte library to handle the bulk of data extraction from necessary sources, while still allowing us to leverage Python when specific business logic requires it.



## Tools / Libraries

- **Open Source ETL:** PyAirbyte
- **Custom ETL Scripting:** Python (`google.cloud`, `requests`, `functions_framework`, `pandas`, etc.)  
- **Workflow Orchestration:** Apache Airflow (Google Cloud Composer)  
- **Landing Data Storage:** Google Cloud Storage  
- **Data Warehouse:** Google BigQuery (Staging/Production Models)
- **Monitoring & Deployment:** Cloud Build  



## Key Challenges

### API Rate Limits

- PyAirbyte handles rate limiting and throttling by default using exponential backoffs and retry mechanisms. **n.b.** PyAirbyte does not support specifying a fixed rate limit for requests.   
- Custom pipelines will implement exponential backoff and retry mechanisms. A fixed rate limit can also be specified as a solution to the aforementioned limitation of Airbyte. 

### Schema Drift

- PyAirbyte automatically detects and manages schema changes using a process called `DiscoverSchema`. This compares the old schema to the new one and propagates modifications to the destination.   
- Custom ETL, we will detect and compare incoming schemas against stored versions, track changes in a schema registry, validate data types before loading, and stage raw data first to ensure safe processing.

### Incremental Loading

- PyAirbyte supports Incremental Loading so as to process only newly added or updated data without replicating the entire dataset. It has two methods, *Incremental Append* and *Incremental Append + Deduped*, the latter involves deduplication based on a primary key.   
- Custom ETL, we can use a timestamp or ID to track new or updated records. We store the last successful sync value in a metadata store, and use it to filter data during the next run. After processing, update the checkpoint in the metadata only if the job succeeds.

## Security, Cost & Maintenance

### Security

- Use IAM service accounts with least privilege principle.  
- Secure secrets/keys using GCP Secret Manager.  

### Cost

- By using PyAirbyte and custom ETL, we avoid licensing fees that usually come with managed ETL platforms.  
- This approach offers full control over data processing and infrastructure and our costs are tied solely to GCP resource usage.  
- BigQuery costs can be further optimised through techniques such as table partitioning, scheduled materialisations and data retention policies.


### Maintenance

- PyAirbyte’s pre-built connectors are a maintained client library by AirByte reducing the maintenance burden on our end.  
- Custom ETL will be modular, version-controlled, and monitored via Airflow and logging.  
- Regular schema validation and cost audits will maintain data integrity and efficiency.


---


# 2. Duplicates in dbt model

## Potential issues for duplication: 

Worked examples included in accompanying excel `2/2_dbt_duplicates.xlsx`

### a) Duplicate rows in `clicks` Table
- Same `clicks.id` appears multiple times as rows are duplicated.
- Each duplicated row in the clicks table will join multiple times to the same `campaigns.id`. 
- Therefore `COUNT(clicks.id)` will be inflated after the group by.

### b) Duplicate Rows in `campaigns` Table
- Same `campaigns.id` appears multiple times as rows are duplicated.
- Each duplicated row in the campaigns table will join multiple times to the same `clicks.campaign_id`. 
- Therefore `COUNT(clicks.id)` will be inflated after the group by.


### c) Non-unique `clicks.id` Values
- `clicks.id` is reused across rows i.e. the row is not completely duplicated, different rows assigned same `clicks.id`. Data quality is bad. 
- As above, each row where `clicks.id` has been re-used will join multiple times to the same `campaign.id`.
- Therefore `COUNT(clicks.id)` will be inflated after the group by.
- This particular problem will need to be fixed upstream, checking source integrity.


## Fixed Query

```sql


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
        campaign_id
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

```


### Changes

- Deduplicated `campaigns` and `clicks` before the join in separate CTEs.
- Used a `LEFT JOIN` to include campaigns with 0 clicks. 
- Final aggregation safe now as removed prior duplicates.

### Limitations
- Possible performance cost depending on data volume.
- Assumption is that columns in `DISTINCT` clauses are a unique representation of that row.

## dbt Testing Strategy

### Generic Tests

The following can be added to the `schema.yml` file for generic out-of-the-box tests which dbt ships with:

```yaml
version: 2

models:
  - name: campaigns
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: client_id
        tests:
          - not_null
      - name: name

  - name: clicks
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: campaign_id
        tests:
          - not_null
          - relationships:
              to: ref('campaigns')
              field: id
      - name: click_time

```

Using single examples for explanation, these data tests translate to:

- **unique:** The `id` column in the `campaigns` model should be unique (no duplicate campaigns).  
  
- **not_null:** The `id` and `campaign_id` columns in the `clicks` model should not contain null values.

- **relationships**:  
  Each `campaign_id` in the `clicks` model must exist as an `id` in the `campaigns` model.  
  (This enforces **referential integrity** between clicks and campaigns.)


### Singular Tests

`.sql` files inside `/tests` directory which return failing records. These are one-off assertions usable for a single purpose.

- `2_campaigns_duplicates.sql` : will fail if there are duplicate rows.
- `2_clicks_duplicates.sql`: will fail if there are duplicate rows.

---

# 3. DAG Code Extract

## Issues in code:
1. The API Key `Bearer 1234567890` is hardcoded into the code. Major security vulnerability. Should abstract this away and alternatively use secrets manager.
1. The `requests.get` call does not include any error handling. It does not check the response status to allow for graceful handling of various errors such as `429 Too Many Requests`. 
1. There is no retry logic. A `429 Too Many Requests` error can most often be handled using a delay and exponential backoff. 
1. `insert_rows_json` does not support schema relaxation and not suitable for larger payloads.
1. No logging or debug info. There is no information on what has been fetched, loaded or even failed.


## Rewritten Functions

Please find the relevant file in `3/ETL.py`

## Unit Tests

Please find the relevant file in `3/unit_tests.py`. This is not an exhaustive list, only a few unit tests have been included for the purposes of demonstration. 


