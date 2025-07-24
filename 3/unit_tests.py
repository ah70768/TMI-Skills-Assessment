import pytest
from unittest.mock import patch, MagicMock
import ETL


# Simple parser dummy that just pulls out 'records' from the response JSON
def dummy_parse(response_json):
    return response_json.get("records", [])


# This test will check if fetch_data function is operating correctly given mock data
@patch("ETL.parse_records", side_effect=dummy_parse)
@patch("ETL.requests.get")
def test_fetch_data_success_first_try(requests_mock, parse_mock):
    # Mock a successful API response on first call
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "status": "SUCCESS",
        "records": [{"id": 1}],
        "hasNextPage": False,
        "nextCursor": None
    }
    requests_mock.return_value = mock_response

    result = ETL.fetch_data(
        access_token="dummy-token",
        start_date="2025-06-01",
        end_date="2025-06-30",
        identifier="test"
    )

    # test output from the .json.return_value should be the records field
    assert result == [{"id": 1}]
    
    # test there is no pagination as hasNextPage is false in return value  
    requests_mock.assert_called_once()  


# This test will check if data in json format has successfully been loaded via the load_to_bigquery function
@patch("ETL.bigquery")
def test_load_bigquery_json_success(mock_bq):
    mock_client = MagicMock()
    json_data = [{"col_1": "a"}, {"col_1": "b"}]
    mock_job = MagicMock()
    mock_job.result.return_value.output_rows = 2
    mock_client.load_table_from_json.return_value = mock_job

    result = ETL.load_to_bigquery(
        bq_client=mock_client,
        data=json_data,
        dataset_id="test_dataset",
        table_name="test_table",
        formatted_date="20250701",
        data_format="json"
    )

    # test output from json_data should show two rows loaded.
    assert result.output_rows == 2


# This test will check if an unsupported data format has been passed into load_to_bigquery function
def test_load_bigquery_invalid_format():
    with pytest.raises(ValueError):
        ETL.load_to_bigquery(
            bq_client=MagicMock(),
            data=[],
            dataset_id="test_dataset",
            table_name="test_table",
            formatted_date="20250701",
            data_format="csv"  # This format is not supported in the code
        )