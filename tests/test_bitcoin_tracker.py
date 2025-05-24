import pytest
import requests
from unittest.mock import MagicMock, patch, call # call for checking multiple calls

# Ensure the bitcoin_tracker module can be found.
# This is typically handled by running pytest from the project root directory,
# or by setting PYTHONPATH. For robustness in various execution environments:
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Imports for the functions under test
from bitcoin_tracker import fetch_bitcoin_prices, connect_to_mongodb, insert_bitcoin_data # noqa E402 module level import not at top of file
import pymongo # For pymongo.errors # noqa E402
from pymongo.results import UpdateResult # To mock the result of update_one # noqa E402

# ---------- Tests for fetch_bitcoin_prices ----------
# Test case 1: Successful API response (15m interval)
@patch('bitcoin_tracker.requests.get')
def test_fetch_successful_15m(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'prices': [[1609459200000, 30000], [1609460100000, 30100]]}
    mock_get.return_value = mock_response

    expected_data = [{'timestamp': 1609459200000, 'price': 30000}, {'timestamp': 1609460100000, 'price': 30100}]
    result = fetch_bitcoin_prices()

    assert result == expected_data
    # Check that requests.get was called with '15m' interval
    args, kwargs = mock_get.call_args
    assert 'params' in kwargs
    assert kwargs['params']['interval'] == '15m'
    assert kwargs['params']['days'] == '5' # Ensure other default params are there

# Test case 2: Successful API response (hourly interval after 15m fails)
@patch('bitcoin_tracker.requests.get')
def test_fetch_successful_hourly_after_15m_fails(mock_get):
    # Define the side effect function
    def side_effect_15m_fail_hourly_ok(*args, **kwargs):
        params = kwargs.get('params', {})
        if params.get('interval') == '15m':
            error_response = MagicMock()
            error_response.status_code = 400  # Simulate an API error for 15m
            error_response.json.return_value = {"error": "Interval too granular for this range"}
            error_response.text = '{"error": "Interval too granular for this range"}' # for logging
            return error_response
        elif params.get('interval') == 'hourly':
            success_response = MagicMock()
            success_response.status_code = 200
            success_response.json.return_value = {'prices': [[1609459200000, 29000]]}
            return success_response
        # Fallback, should not be reached in a well-defined test
        fallback_response = MagicMock()
        fallback_response.status_code = 500 
        return fallback_response

    mock_get.side_effect = side_effect_15m_fail_hourly_ok
    
    expected_data = [{'timestamp': 1609459200000, 'price': 29000}]
    result = fetch_bitcoin_prices()

    assert result == expected_data
    assert mock_get.call_count == 2
    
    # Check params for the first call (15m)
    args_15m, kwargs_15m = mock_get.call_args_list[0]
    assert kwargs_15m['params']['interval'] == '15m'
    
    # Check params for the second call (hourly)
    args_hourly, kwargs_hourly = mock_get.call_args_list[1]
    assert kwargs_hourly['params']['interval'] == 'hourly'

# Test case 3: API error (e.g., status code 500)
@patch('bitcoin_tracker.requests.get')
def test_fetch_api_error_500(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error" # for logging
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    # The function should try 15m, fail, then try hourly, fail again
    assert mock_get.call_count == 2 


# Test case 4: Network error (requests.exceptions.RequestException)
@patch('bitcoin_tracker.requests.get')
def test_fetch_network_error(mock_get):
    mock_get.side_effect = requests.exceptions.RequestException("Network error")

    result = fetch_bitcoin_prices()
    assert result == []
    # Network error on first attempt, so only one call
    assert mock_get.call_count == 1

# Test case 5: Malformed JSON response
@patch('bitcoin_tracker.requests.get')
def test_fetch_malformed_json(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    # Simulate malformed JSON by raising ValueError when .json() is called
    mock_response.json.side_effect = ValueError("Malformed JSON") 
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    # Malformed JSON on first attempt, so only one call
    assert mock_get.call_count == 1

# Test case 6: Empty prices list in successful response
@patch('bitcoin_tracker.requests.get')
def test_fetch_successful_empty_prices(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'prices': []} # Empty prices list
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == [] # Expect an empty list if API returns no price points
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert kwargs['params']['interval'] == '15m'

# Test case for when the API returns data but the 'prices' key is missing.
@patch('bitcoin_tracker.requests.get')
def test_fetch_missing_prices_key(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'some_other_data': 'value'} # 'prices' key is missing
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert kwargs['params']['interval'] == '15m'

# Test case for when the API returns non-200 for hourly fallback as well
@patch('bitcoin_tracker.requests.get')
def test_fetch_hourly_fallback_also_fails(mock_get):
    error_response_15m = MagicMock()
    error_response_15m.status_code = 400
    error_response_15m.text = "Error 15m"

    error_response_hourly = MagicMock()
    error_response_hourly.status_code = 403
    error_response_hourly.text = "Error hourly"
    
    mock_get.side_effect = [error_response_15m, error_response_hourly]

    result = fetch_bitcoin_prices()
    assert result == []
    assert mock_get.call_count == 2
    assert mock_get.call_args_list[0][1]['params']['interval'] == '15m'
    assert mock_get.call_args_list[1][1]['params']['interval'] == 'hourly'

# Test case for unexpected exception during requests.get()
@patch('bitcoin_tracker.requests.get')
def test_fetch_unexpected_exception_on_get(mock_get):
    mock_get.side_effect = Exception("Something totally unexpected")

    result = fetch_bitcoin_prices()
    assert result == []
    assert mock_get.call_count == 1 # Fails on the first '15m' attempt
    
# Test case for unexpected exception during response.json()
@patch('bitcoin_tracker.requests.get')
def test_fetch_unexpected_exception_on_json_parse(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = Exception("Unexpected JSON parsing error")
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    assert mock_get.call_count == 1

# Test case for unexpected exception after a successful 15m API call but before formatting
# (e.g., data.get('prices', []) fails if data is not a dict)
@patch('bitcoin_tracker.requests.get')
def test_fetch_unexpected_exception_post_successful_call(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    # Simulate data being something other than a dict, causing data.get() to fail
    mock_response.json.return_value = "this is not a dict" 
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    assert mock_get.call_count == 1

# ---------- Tests for connect_to_mongodb ----------

# Test case 1: Successful MongoDB connection
@patch('bitcoin_tracker.os.getenv')
@patch('bitcoin_tracker.pymongo.MongoClient')
def test_connect_successful(mock_MongoClient, mock_getenv):
    mock_getenv.return_value = "dummy_mongodb_uri"
    mock_mongo_client_instance = MagicMock()
    # Simulate the client.admin.command('ismaster') call
    mock_mongo_client_instance.admin.command.return_value = {"ok": 1}
    mock_MongoClient.return_value = mock_mongo_client_instance
    
    client = connect_to_mongodb()

    assert client is mock_mongo_client_instance
    mock_getenv.assert_called_once_with('MONGODB_URI')
    mock_MongoClient.assert_called_once_with("dummy_mongodb_uri")
    mock_mongo_client_instance.admin.command.assert_called_once_with('ismaster')

# Test case 2: Missing MONGODB_URI environment variable
@patch('bitcoin_tracker.os.getenv')
@patch('bitcoin_tracker.pymongo.MongoClient') # To ensure it's not called
def test_connect_missing_uri(mock_MongoClient, mock_getenv):
    mock_getenv.return_value = None
    
    client = connect_to_mongodb()

    assert client is None
    mock_getenv.assert_called_once_with('MONGODB_URI')
    mock_MongoClient.assert_not_called()

# Test case 3: pymongo.errors.ConnectionFailure on connection
@patch('bitcoin_tracker.os.getenv')
@patch('bitcoin_tracker.pymongo.MongoClient')
def test_connect_connection_failure(mock_MongoClient, mock_getenv):
    mock_getenv.return_value = "dummy_mongodb_uri"
    mock_MongoClient.side_effect = pymongo.errors.ConnectionFailure("Connection failed")
    
    client = connect_to_mongodb()

    assert client is None
    mock_getenv.assert_called_once_with('MONGODB_URI')
    mock_MongoClient.assert_called_once_with("dummy_mongodb_uri")

# Test case 4: Other pymongo.errors.PyMongoError (e.g., ConfigurationError) on connection
@patch('bitcoin_tracker.os.getenv')
@patch('bitcoin_tracker.pymongo.MongoClient')
def test_connect_other_pymongo_error(mock_MongoClient, mock_getenv):
    mock_getenv.return_value = "dummy_mongodb_uri"
    # Using ConfigurationError as an example of another PyMongoError
    mock_MongoClient.side_effect = pymongo.errors.ConfigurationError("Bad configuration")
    
    client = connect_to_mongodb()

    assert client is None
    mock_getenv.assert_called_once_with('MONGODB_URI')
    mock_MongoClient.assert_called_once_with("dummy_mongodb_uri")

# Test case for unexpected general exception during connection attempt
@patch('bitcoin_tracker.os.getenv')
@patch('bitcoin_tracker.pymongo.MongoClient')
def test_connect_unexpected_exception(mock_MongoClient, mock_getenv):
    mock_getenv.return_value = "dummy_mongodb_uri"
    mock_MongoClient.side_effect = Exception("Some unexpected error")

    client = connect_to_mongodb()
    assert client is None
    mock_getenv.assert_called_once_with('MONGODB_URI')
    mock_MongoClient.assert_called_once_with("dummy_mongodb_uri")

# ---------- Tests for insert_bitcoin_data ----------

# Test case 1: Successful insertion of multiple new documents
def test_insert_multiple_new_documents():
    mock_mongo_client = MagicMock()
    mock_collection = MagicMock()
    mock_mongo_client.crypto_prices.bitcoin_usd = mock_collection

    mock_update_result = MagicMock(spec=UpdateResult)
    mock_update_result.upserted_id = "some_object_id" # Indicates a new document was inserted
    mock_update_result.matched_count = 0
    mock_update_result.modified_count = 0
    mock_collection.update_one.return_value = mock_update_result

    sample_price_data = [{'timestamp': 1, 'price': 100}, {'timestamp': 2, 'price': 200}]
    count = insert_bitcoin_data(mock_mongo_client, sample_price_data)

    assert count == 2
    assert mock_collection.update_one.call_count == 2
    expected_calls = [
        call({'timestamp': 1}, {'$set': {'timestamp': 1, 'price': 100}}, upsert=True),
        call({'timestamp': 2}, {'$set': {'timestamp': 2, 'price': 200}}, upsert=True)
    ]
    mock_collection.update_one.assert_has_calls(expected_calls, any_order=False)

# Test case 2: Data already existing (no new documents inserted)
def test_insert_data_already_exists():
    mock_mongo_client = MagicMock()
    mock_collection = MagicMock()
    mock_mongo_client.crypto_prices.bitcoin_usd = mock_collection

    mock_update_result_no_upsert = MagicMock(spec=UpdateResult)
    mock_update_result_no_upsert.upserted_id = None # Indicates no new document was inserted
    mock_update_result_no_upsert.matched_count = 1 # Document was found
    mock_update_result_no_upsert.modified_count = 1 # Document was updated (or 0 if same data)
    mock_collection.update_one.return_value = mock_update_result_no_upsert

    sample_price_data = [{'timestamp': 1, 'price': 100}]
    count = insert_bitcoin_data(mock_mongo_client, sample_price_data)

    assert count == 0
    mock_collection.update_one.assert_called_once_with(
        {'timestamp': 1}, {'$set': {'timestamp': 1, 'price': 100}}, upsert=True
    )

# Test case 3: Empty price data list
def test_insert_empty_price_data_list():
    mock_mongo_client = MagicMock()
    # Accessing mock_mongo_client.crypto_prices.bitcoin_usd creates the mock collection
    # if it doesn't exist, so we can check calls on it.
    
    count = insert_bitcoin_data(mock_mongo_client, [])
    
    assert count == 0
    mock_mongo_client.crypto_prices.bitcoin_usd.update_one.assert_not_called()

# Test case 4: mongo_client is None
def test_insert_mongo_client_none():
    sample_price_data = [{'timestamp': 1, 'price': 100}]
    count = insert_bitcoin_data(None, sample_price_data)
    assert count == 0

# Test case 5: pymongo.errors.PyMongoError during one of several update_one calls
def test_insert_pymongo_error_during_update():
    mock_mongo_client = MagicMock()
    mock_collection = MagicMock()
    mock_mongo_client.crypto_prices.bitcoin_usd = mock_collection

    mock_success_result = MagicMock(spec=UpdateResult)
    mock_success_result.upserted_id = "some_id"
    mock_success_result.matched_count = 0
    mock_success_result.modified_count = 0
    
    # The actual function catches the exception inside the loop and continues.
    # The item causing an error won't be counted as an insert.
    mock_collection.update_one.side_effect = [
        mock_success_result, # First call succeeds
        pymongo.errors.PyMongoError("DB error"), # Second call fails
        mock_success_result  # Third call succeeds
    ]

    sample_price_data = [
        {'timestamp': 1, 'price': 100}, 
        {'timestamp': 2, 'price': 200}, 
        {'timestamp': 3, 'price': 300}
    ]
    count = insert_bitcoin_data(mock_mongo_client, sample_price_data)

    # The function's error handling is per-item.
    # So, if an error occurs, that item is skipped, but others are processed.
    assert count == 2 # First and third succeed
    assert mock_collection.update_one.call_count == 3
    expected_calls = [
        call({'timestamp': 1}, {'$set': {'timestamp': 1, 'price': 100}}, upsert=True),
        call({'timestamp': 2}, {'$set': {'timestamp': 2, 'price': 200}}, upsert=True),
        call({'timestamp': 3}, {'$set': {'timestamp': 3, 'price': 300}}, upsert=True)
    ]
    mock_collection.update_one.assert_has_calls(expected_calls, any_order=False)

# Test case for unexpected general exception during insert_bitcoin_data
# This tests the outer try-except in the insert_bitcoin_data if any part of the
# setup before the loop or something unexpected in the loop itself (not PyMongoError) fails.
def test_insert_unexpected_exception():
    mock_mongo_client = MagicMock()
    mock_collection = MagicMock()
    mock_mongo_client.crypto_prices.bitcoin_usd = mock_collection

    # Simulate an error when update_one is called
    mock_collection.update_one.side_effect = Exception("Highly unexpected error")

    sample_price_data = [{'timestamp': 1, 'price': 100}]
    count = insert_bitcoin_data(mock_mongo_client, sample_price_data)

    # The current implementation catches general Exception and returns current inserted_count
    assert count == 0 
    mock_collection.update_one.assert_called_once()


# Note: The sys.path.append is a common way to handle imports when running tests,
# but a more standard approach is to install the package in editable mode (`pip install -e .`)
# or ensure pytest is run from the project root.
# The `noqa E402` is to suppress flake8 warning about module level import not at top of file
# due to the sys.path manipulation.
