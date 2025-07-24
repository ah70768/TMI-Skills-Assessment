
import pandas as pd
import logging
import time
import requests
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


logging.basicConfig(level=logging.INFO)

def fetch_data(access_token, start_date, end_date, identifier, max_retries=3, delay=5, request_timeout=30):
    """
    Fetches paginated data from an API.
    """

    url = "https://api.adserver.com/data"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    all_records = []
    has_next_page = True
    cursor = None

    while has_next_page:

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "identifier": identifier
        }

        if cursor:
            params["cursor"] = cursor

        # Retry if status != "SUCCESS"
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=request_timeout)
                response.raise_for_status()
                response_json = response.json()
            except Exception as e:
                wait_time = delay * (2 ** attempt)
                logging.warning(f"[Attempt {attempt + 1}] Request error: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue

            status = response_json.get("status")
            if status == "SUCCESS":
                logging.info("Data ready for retrieval.")
                break
            else:
                wait_time = delay * (2 ** attempt)
                logging.warning(f"[Attempt {attempt + 1}] Data processing. Retrying in {wait_time}s...")
                time.sleep(wait_time)
        else:
            logging.error("Max retries exceeded. Data still processing.")
            return None

        try:
            logging.info(f"Parsing records at cursor point: {cursor}")
            records = parse_records(response_json)
        except Exception as e:
            logging.error(f"Error during parsing records: {e}")
            return None

        all_records.extend(records)

        has_next_page = response_json.get("hasNextPage", False)
        cursor = response_json.get("nextCursor")

        logging.info(f"Fetched {len(records)} records. hasNextPage={has_next_page}, nextCursor={cursor}")

    return all_records


def load_to_bigquery(bq_client, data, dataset_id, table_name, formatted_date, data_format="df"):
    """
    Load data into a BigQuery table from a pandas DataFrame or a JSON list.
    """

    table_id = f"{dataset_id}.{table_name}_{formatted_date}"
    logging.info(f"Loading data into BigQuery table: {table_id}")

    try:
        if data_format == "df":

            schema = [
                bigquery.SchemaField(str(col), "STRING") for col in data.columns
            ]

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]
            )

            load_job = bq_client.load_table_from_dataframe(
                data, table_id, job_config=job_config
            )

        elif data_format == "json":

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]
            )

            load_job = bq_client.load_table_from_json(
                data, table_id, job_config=job_config
            )

        else:
            raise ValueError(f"Unsupported data_format: {data_format}")

        result = load_job.result()
        logging.info(f"BigQuery load job {load_job.job_id} succeeded: {result.output_rows} rows")
        return result

    except Exception as e:
        logging.error(f"BigQuery load job failed for table {table_id}: {e}", exc_info=True)
        raise


def parse_records(response):
    """
    Flattens a nested API response into a list of ad metrics.
    """

    all_records = []

    try:
        campaigns = response.get("data", {}).get("campaigns", [])
    except Exception as e:
        logging.error("Error accessing campaigns from response: %s", e)
        return []

    for campaign in campaigns:
        try:
            campaign_name = campaign.get("name", "")
            ads = campaign.get("ads", [])
        except Exception as e:
            logging.error("Error accessing campaign data: %s", e)
            continue

        for ad in ads:
            try:
                ad_name = ad.get("name", "")
                metrics = ad.get("metrics", [])
            except Exception as e:
                logging.error("Error accessing ad data in campaign '%s': %s", campaign_name, e)
                continue

            for metric in metrics:
                try:
                    record = {
                        "campaign": campaign_name,
                        "ad_name": ad_name,
                        "date": metric.get("date", ""),
                        "cost": metric.get("cost", 0),
                        "impressions": metric.get("impressions", 0),
                        "clicks": metric.get("clicks", 0),
                        "conversions": metric.get("conversions", 0),
                        "revenue": metric.get("revenue", 0)
                    }
                    all_records.append(record)
                except Exception as e:
                    logging.error("Error parsing metric in ad '%s': %s", ad_name, e)
                    continue

    return all_records


