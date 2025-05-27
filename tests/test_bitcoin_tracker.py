import pytest
import requests
from unittest.mock import MagicMock, patch, call # call for checking multiple calls

import sys
import os
from datetime import datetime, timedelta, timezone
import logging # For capturing log messages

# Ensure the bitcoin_tracker module can be found.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Imports for the functions under test
from bitcoin_tracker import ( # noqa E402 module level import not at top of file
    fetch_bitcoin_prices,
    fetch_bitcoin_prices_coinbase, # New function to test
    connect_to_mongodb,
    insert_bitcoin_data,
    ThreadPoolExecutor # For testing concurrent logic
)
import pymongo # For pymongo.errors and pymongo.ASCENDING # noqa E402
from pymongo.results import UpdateResult # To mock the result of update_one # noqa E402

# ---------- Tests for fetch_bitcoin_prices (CoinGecko) ----------
# Test case 1: Successful API response (default interval, typically hourly for 5 days)
@patch('bitcoin_tracker.requests.get')
def test_fetch_successful_default_interval(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'prices': [[1609459200000, 30000], [1609460100000, 30100]]}
    mock_get.return_value = mock_response

    expected_data = [{'timestamp': 1609459200000, 'price': 30000}, {'timestamp': 1609460100000, 'price': 30100}]
    result = fetch_bitcoin_prices()

    assert result == expected_data
    mock_get.assert_called_once() # Ensure only one call is made
    args, kwargs = mock_get.call_args
    assert 'params' in kwargs
    # Ensure 'interval' is NOT in params, and 'days' is '5'
    assert 'interval' not in kwargs['params']
    assert kwargs['params']['days'] == '5'
    assert kwargs['params']['vs_currency'] == 'usd'


# Test case 2: API error (e.g., status code 500)
@patch('bitcoin_tracker.requests.get')
def test_fetch_api_error_500(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error" # for logging
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once() # Only one call attempt


# Test case 3: Network error (requests.exceptions.RequestException)
@patch('bitcoin_tracker.requests.get')
def test_fetch_network_error(mock_get):
    mock_get.side_effect = requests.exceptions.RequestException("Network error")

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once() # Only one call attempt

# Test case 4: Malformed JSON response (ValueError during .json())
@patch('bitcoin_tracker.requests.get')
def test_fetch_malformed_json(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    # Simulate malformed JSON by raising ValueError when .json() is called
    mock_response.json.side_effect = ValueError("Malformed JSON") 
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once()

# Test case 5: Empty prices list in successful response
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
    assert 'interval' not in kwargs['params'] # Check no interval param

# Test case 6: 'prices' key is present but None
@patch('bitcoin_tracker.requests.get')
def test_fetch_prices_key_is_none(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'prices': None} # 'prices' key exists but is None
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert 'interval' not in kwargs['params']

# Test case 7: 'prices' key is missing in successful response
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
    assert 'interval' not in kwargs['params']


# Test case 8: Unexpected exception during requests.get()
@patch('bitcoin_tracker.requests.get')
def test_fetch_unexpected_exception_on_get(mock_get):
    mock_get.side_effect = Exception("Something totally unexpected")

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once()
    
# Test case 9: Unexpected exception during response.json() (other than ValueError)
@patch('bitcoin_tracker.requests.get')
def test_fetch_unexpected_exception_on_json_parse(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = Exception("Unexpected JSON parsing error") # Not ValueError
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once()

# Test case 10: Unexpected exception after a successful API call but during data processing
# (e.g., data.get('prices', []) fails if data is not a dict, or list comprehension fails)
@patch('bitcoin_tracker.requests.get')
def test_fetch_unexpected_exception_post_successful_call(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    # Simulate data being something other than a dict, causing data.get() to fail
    mock_response.json.return_value = "this is not a dict" 
    mock_get.return_value = mock_response

    result = fetch_bitcoin_prices()
    assert result == []
    mock_get.assert_called_once()

# ---------- Tests for connect_to_mongodb ----------

# Test case 1: Successful MongoDB connection
@patch('bitcoin_tracker.os.getenv')
@patch('bitcoin_tracker.pymongo.MongoClient')
def test_connect_successful_and_index_created(mock_MongoClient, mock_getenv, caplog):
    mock_getenv.return_value = "dummy_mongodb_uri"
    mock_mongo_client_instance = MagicMock()
    mock_mongo_client_instance.admin.command.return_value = {"ok": 1} # ismaster check
    
    # Mock the database and collection for index creation
    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_mongo_client_instance.crypto_prices = mock_db
    mock_db.bitcoin_usd = mock_collection
    
    mock_MongoClient.return_value = mock_mongo_client_instance
    
    with caplog.at_level(logging.INFO):
        client = connect_to_mongodb()

    assert client is mock_mongo_client_instance
    mock_getenv.assert_called_once_with('MONGODB_URI')
    mock_MongoClient.assert_called_once_with("dummy_mongodb_uri")
    mock_mongo_client_instance.admin.command.assert_called_once_with('ismaster')
    
    # Assert index creation was called
    mock_collection.create_index.assert_called_once_with(
        [('timestamp', pymongo.ASCENDING)], 
        unique=True,
        name='timestamp_unique_asc_idx'
    )
    assert "MongoDB connection successful" in caplog.text
    assert "Attempting to create/ensure unique ascending index on 'timestamp'" in caplog.text
    assert "Unique ascending index on 'timestamp' for collection 'bitcoin_usd' ensured." in caplog.text


@patch('bitcoin_tracker.os.getenv')
@patch('bitcoin_tracker.pymongo.MongoClient')
def test_connect_successful_index_creation_fails(mock_MongoClient, mock_getenv, caplog):
    mock_getenv.return_value = "dummy_mongodb_uri"
    mock_mongo_client_instance = MagicMock()
    mock_mongo_client_instance.admin.command.return_value = {"ok": 1}

    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_mongo_client_instance.crypto_prices = mock_db
    mock_db.bitcoin_usd = mock_collection
    mock_collection.create_index.side_effect = pymongo.errors.OperationFailure("Failed to create index")
    
    mock_MongoClient.return_value = mock_mongo_client_instance

    with caplog.at_level(logging.INFO): # Capture warnings/errors too
        client = connect_to_mongodb()

    assert client is mock_mongo_client_instance # Client should still be returned
    mock_collection.create_index.assert_called_once()
    assert "MongoDB connection successful" in caplog.text
    assert "OperationFailure occurred while creating unique index" in caplog.text

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

# Test case 1: Successful insertion of multiple new documents and new logging
def test_insert_multiple_new_documents_and_logging(caplog):
    mock_mongo_client = MagicMock()
    mock_collection = MagicMock()
    mock_mongo_client.crypto_prices.bitcoin_usd = mock_collection

    mock_update_result = MagicMock(spec=UpdateResult)
    mock_update_result.upserted_id = "some_object_id" # Indicates a new document was inserted
    mock_update_result.matched_count = 0
    mock_update_result.modified_count = 0
    mock_collection.update_one.return_value = mock_update_result

    sample_price_data = [{'timestamp': 1, 'price': 100}, {'timestamp': 2, 'price': 200}]
    
    with caplog.at_level(logging.INFO):
        count = insert_bitcoin_data(mock_mongo_client, sample_price_data)

    assert count == 2
    assert mock_collection.update_one.call_count == 2
    expected_calls = [
        call({'timestamp': 1}, {'$set': {'timestamp': 1, 'price': 100}}, upsert=True),
        call({'timestamp': 2}, {'$set': {'timestamp': 2, 'price': 200}}, upsert=True)
    ]
    mock_collection.update_one.assert_has_calls(expected_calls, any_order=False)
    
    # Verify new logging message
    expected_log_msg = (
        f"Data insertion/update attempt complete for {len(sample_price_data)} items from combined sources. "
        f"{count} new documents were inserted, duplicates skipped/updated due to unique timestamp."
    )
    assert expected_log_msg in caplog.text

# Test case 2: Data already existing (no new documents inserted)
def test_insert_data_already_exists(caplog):
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


# ---------- Tests for fetch_bitcoin_prices_coinbase ----------

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_successful(mock_get, mocker):
    mock_response = mocker.Mock() # Use mocker fixture for consistency
    mock_response.status_code = 200
    # Coinbase format: [time_epoch_sec, low, high, open, close, volume]
    mock_response.json.return_value = [
        [1678886400, 24000, 24100, 24050, 24080.50, 10], # 2023-03-15T12:00:00Z
        [1678887300, 24080, 24200, 24080, 24150.75, 12]  # 2023-03-15T12:15:00Z
    ]
    mock_get.return_value = mock_response

    # Clear any preset env vars for this test
    mocker.patch.dict(os.environ, clear=True)

    result = fetch_bitcoin_prices_coinbase()

    expected_data = [
        {'timestamp': 1678886400000, 'price': 24080.50},
        {'timestamp': 1678887300000, 'price': 24150.75}
    ]
    assert result == expected_data
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert "https://api.pro.coinbase.com/products/BTC-USD/candles" in args[0]
    assert kwargs['params']['granularity'] == 900 # Default

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_api_error(mock_get, mocker, caplog):
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mock_response.text = "Coinbase Server Error"
    mock_get.return_value = mock_response
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.ERROR):
        result = fetch_bitcoin_prices_coinbase()
    
    assert result == []
    assert "Failed to fetch data for product ID 'BTC-USD' from Coinbase Pro." in caplog.text
    assert "Status code: 500, Response: Coinbase Server Error" in caplog.text

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_request_exception(mock_get, mocker, caplog):
    mock_get.side_effect = requests.exceptions.RequestException("Coinbase network error")
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.ERROR): # logging.exception logs at ERROR level
        result = fetch_bitcoin_prices_coinbase()

    assert result == []
    assert "RequestException occurred during Coinbase API request for product ID 'BTC-USD'" in caplog.text

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_timeout_exception(mock_get, mocker, caplog):
    mock_get.side_effect = requests.exceptions.Timeout("Coinbase timeout")
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.ERROR):
        result = fetch_bitcoin_prices_coinbase()
    
    assert result == []
    assert "Timeout occurred during Coinbase API request for product ID 'BTC-USD'" in caplog.text

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_malformed_json(mock_get, mocker, caplog):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Bad JSON from Coinbase")
    mock_get.return_value = mock_response
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.ERROR):
        result = fetch_bitcoin_prices_coinbase()

    assert result == []
    assert "ValueError (e.g., malformed JSON) occurred during Coinbase API response processing for product ID 'BTC-USD'" in caplog.text

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_empty_data_list(mock_get, mocker, caplog):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [] # Empty list from API
    mock_get.return_value = mock_response
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.INFO):
        result = fetch_bitcoin_prices_coinbase()

    assert result == []
    assert "Coinbase API returned no data for the requested range." in caplog.text


@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_malformed_candle_data(mock_get, mocker, caplog):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    # Valid candle, then malformed (too few elements), then another valid one
    mock_response.json.return_value = [
        [1678886400, 24000, 24100, 24050, 24080.50, 10],
        [1678887300, 24150.75], # Malformed
        [1678888200, 24200, 24300, 24250, 24280.00, 15]
    ]
    mock_get.return_value = mock_response
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.WARNING):
        result = fetch_bitcoin_prices_coinbase()
    
    expected_data = [
        {'timestamp': 1678886400000, 'price': 24080.50},
        {'timestamp': 1678888200000, 'price': 24280.00}
    ]
    assert result == expected_data
    assert "Skipping malformed candle data: [1678887300, 24150.75]" in caplog.text
    assert len(result) == 2


@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_invalid_granularity_env(mock_get, mocker, caplog):
    # Set invalid granularity, mock_get will be used to check params
    mocker.patch.dict(os.environ, {"COINBASE_GRANULARITY_SECONDS": "xyz"})
    
    # Mock a successful response to allow the function to proceed to the requests.get call
    mock_api_response = mocker.Mock()
    mock_api_response.status_code = 200
    mock_api_response.json.return_value = [[1678886400, 1, 1, 1, 1, 1]] # Dummy data
    mock_get.return_value = mock_api_response

    with caplog.at_level(logging.WARNING):
        fetch_bitcoin_prices_coinbase()

    assert "Invalid value 'xyz' for COINBASE_GRANULARITY_SECONDS. Must be an integer. Using default: 900 seconds." in caplog.text
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert kwargs['params']['granularity'] == 900 # Default granularity

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_configurable_product_id_and_granularity(mock_get, mocker):
    mocker.patch.dict(os.environ, {
        "COINBASE_PRODUCT_ID": "ETH-USD",
        "COINBASE_GRANULARITY_SECONDS": "300"
    })

    mock_api_response = mocker.Mock()
    mock_api_response.status_code = 200
    mock_api_response.json.return_value = [[1678886400, 1, 1, 1, 1, 1]] # Dummy data
    mock_get.return_value = mock_api_response
    
    fetch_bitcoin_prices_coinbase()

    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert "https://api.pro.coinbase.com/products/ETH-USD/candles" in args[0]
    assert kwargs['params']['granularity'] == 300

# ---------- Tests for Concurrent Fetching Logic (Simulated __main__) ----------

@patch('bitcoin_tracker.insert_bitcoin_data')
@patch('bitcoin_tracker.connect_to_mongodb')
@patch('bitcoin_tracker.fetch_bitcoin_prices_coinbase')
@patch('bitcoin_tracker.fetch_bitcoin_prices')
def test_concurrent_fetching_and_insertion(
    mock_fetch_coingecko, mock_fetch_coinbase, 
    mock_connect_mongo, mock_insert_data, 
    mocker, caplog
):
    # 1. Setup Mocks for fetching functions
    mock_fetch_coingecko.return_value = [{'timestamp': 1600000000001, 'price': 100.0}]
    mock_fetch_coinbase.return_value = [{'timestamp': 1700000000002, 'price': 200.0}]

    # 2. Setup Mock for MongoDB connection
    mock_mongo_client_instance = MagicMock()
    mock_mongo_client_instance.close = MagicMock() # Mock the close method
    mock_connect_mongo.return_value = mock_mongo_client_instance
    
    # 3. (insert_bitcoin_data is already mocked by @patch)

    # 4. Simulate the core logic of the __main__ block
    # This part is a simplified version of the __main__ block's flow
    caplog.set_level(logging.INFO)
    
    combined_prices = []
    logging.info("Test: Initiating concurrent fetching...")
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_coingecko = executor.submit(mock_fetch_coingecko)
        future_coinbase = executor.submit(mock_fetch_coinbase)

        try:
            coingecko_prices = future_coingecko.result()
            if coingecko_prices:
                combined_prices.extend(coingecko_prices)
        except Exception: # Simplified error handling for test
            logging.error("Test: Error fetching from coingecko mock")


        try:
            coinbase_prices = future_coinbase.result()
            if coinbase_prices:
                combined_prices.extend(coinbase_prices)
        except Exception:
            logging.error("Test: Error fetching from coinbase mock")
            
    logging.info(f"Test: Total combined prices: {len(combined_prices)}")

    mongo_client_main_sim = None
    if combined_prices:
        mongo_client_main_sim = mock_connect_mongo()
        if mongo_client_main_sim:
            mock_insert_data(mongo_client_main_sim, combined_prices)
    
    if mongo_client_main_sim:
        mongo_client_main_sim.close()

    # 5. Assertions
    mock_fetch_coingecko.assert_called_once()
    mock_fetch_coinbase.assert_called_once()
    
    # Ensure connect_to_mongodb was called if there were prices
    if combined_prices:
        mock_connect_mongo.assert_called_once()
        # Assert that insert_bitcoin_data was called with the combined data
        expected_combined_data = [
            {'timestamp': 1600000000001, 'price': 100.0},
            {'timestamp': 1700000000002, 'price': 200.0}
        ]
        # The order might vary slightly if extend was called in a different order or if list was sorted after
        # For this test, we assume the order is predictable based on extend calls.
        # A more robust check might involve sorting both lists or converting to sets of tuples.
        # However, given the direct extend, the order should be as above.
        mock_insert_data.assert_called_once_with(mock_mongo_client_instance, expected_combined_data)
        mock_mongo_client_instance.close.assert_called_once() # Check client was closed
    else: # If, hypothetically, both fetches failed and returned nothing
        mock_connect_mongo.assert_not_called()
        mock_insert_data.assert_not_called()
        
    assert "Test: Initiating concurrent fetching..." in caplog.text
    assert f"Test: Total combined prices: {len(combined_prices)}" in caplog.text

# Note: The sys.path.append is a common way to handle imports when running tests,
# but a more standard approach is to install the package in editable mode (`pip install -e .`)
# or ensure pytest is run from the project root.
# The `noqa E402` is to suppress flake8 warning about module level import not at top of file
# due to the sys.path manipulation.
