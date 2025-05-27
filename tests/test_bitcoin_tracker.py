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

@patch('bitcoin_tracker.datetime')
@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_successful_default_pagination(mock_requests_get, mock_datetime, mocker, caplog):
    # --- Setup for predictable time ---
    # Fixed current time for predictable chunk calculations
    fixed_now_utc = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc) # Example: 12:00 PM UTC
    mock_datetime.now.return_value = fixed_now_utc
    mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) # Allow datetime constructors

    # --- Configuration ---
    mocker.patch.dict(os.environ, clear=True) # Use default granularity (60s)
    granularity = 60
    product_id = "BTC-USD"
    max_candles_per_request = 300
    total_duration_hours = 48
    
    # Total candles: 48 hours * 60 min/hr * 1 candle/min (for 60s granularity) = 2880 candles
    # Number of requests: 2880 / 300 = 9.6 => 10 requests
    expected_num_calls = (total_duration_hours * 3600 // granularity) // max_candles_per_request + \
                         (1 if (total_duration_hours * 3600 // granularity) % max_candles_per_request > 0 else 0)

    # --- Mock API Responses ---
    mock_responses = []
    all_expected_data_points = []
    
    overall_start_time_utc = fixed_now_utc - timedelta(hours=total_duration_hours)

    for i in range(expected_num_calls):
        response = mocker.Mock()
        response.status_code = 200
        
        # Generate data for this chunk
        chunk_data = []
        # Calculate start time for this specific chunk based on overall_start_time_utc and how many candles processed so far
        candles_processed_so_far = i * max_candles_per_request
        chunk_base_time_sec = int((overall_start_time_utc + timedelta(seconds=candles_processed_so_far * granularity)).timestamp())

        num_candles_in_chunk = max_candles_per_request
        if i == expected_num_calls -1: # Last chunk might have fewer candles
            remaining_candles = (total_duration_hours * 3600 // granularity) - candles_processed_so_far
            if remaining_candles > 0:
                 num_candles_in_chunk = remaining_candles
            else: # Should not happen if expected_num_calls is correct
                 mock_responses.append(response) # Add empty if calculation was off
                 continue


        for j in range(num_candles_in_chunk):
            candle_time_sec = chunk_base_time_sec + j * granularity
            price = 20000 + i * 100 + j  # Unique price for each candle
            # Coinbase format: [time_epoch_sec, low, high, open, close, volume]
            candle = [candle_time_sec, price - 10, price + 10, price - 5, price, 1.0]
            chunk_data.append(candle)
            all_expected_data_points.append({'timestamp': candle_time_sec * 1000, 'price': float(price)})
        
        response.json.return_value = chunk_data
        mock_responses.append(response)

    mock_requests_get.side_effect = mock_responses

    # --- Execute ---
    with caplog.at_level(logging.INFO):
        result = fetch_bitcoin_prices_coinbase()

    # --- Assertions ---
    assert len(result) == len(all_expected_data_points)
    assert result == all_expected_data_points # Assumes data is sorted by the function
    
    assert mock_requests_get.call_count == expected_num_calls
    
    # Check params for each call
    current_assert_start_time = overall_start_time_utc
    for i in range(expected_num_calls):
        args, kwargs = mock_requests_get.call_args_list[i]
        assert f"https://api.exchange.coinbase.com/products/{product_id}/candles" in args[0]
        assert kwargs['params']['granularity'] == granularity
        
        expected_chunk_end_time = min(current_assert_start_time + timedelta(seconds=max_candles_per_request * granularity), fixed_now_utc)
        
        assert kwargs['params']['start'] == current_assert_start_time.isoformat().replace(".000000", "")
        assert kwargs['params']['end'] == expected_chunk_end_time.isoformat().replace(".000000", "")
        
        current_assert_start_time = expected_chunk_end_time

    assert f"Attempting to fetch Coinbase Exchange API data for product ID '{product_id}'" in caplog.text
    assert f"Successfully fetched a total of {len(all_expected_data_points)} price points" in caplog.text

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_api_error_first_chunk(mock_get, mocker, caplog):
    # Test error on the very first chunk
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mock_response.text = "Coinbase Server Error on first chunk"
    mock_get.return_value = mock_response # Fails on first call
    mocker.patch.dict(os.environ, clear=True) # Defaults: BTC-USD, 60s

    with caplog.at_level(logging.ERROR):
        result = fetch_bitcoin_prices_coinbase()
    
    assert result == []
    assert "Failed to fetch data for chunk 1 (product ID 'BTC-USD') from Coinbase Exchange API." in caplog.text
    assert "Status code: 500, Response: Coinbase Server Error on first chunk" in caplog.text
    mock_get.assert_called_once() # Should stop after first failed chunk

@patch('bitcoin_tracker.datetime')
@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_error_during_pagination(mock_requests_get, mock_datetime_now, mocker, caplog):
    fixed_now_utc = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime_now.now.return_value = fixed_now_utc
    mock_datetime_now.side_effect = lambda *args, **kw: datetime(*args, **kw)

    mocker.patch.dict(os.environ, {"COINBASE_GRANULARITY_SECONDS": "18000"}) # Approx 1 chunk for 48h if 300 limit
                                                                            # No, 48h = 172800s. 172800/18000 = 9.6 candles. 1 chunk.
                                                                            # Let's use 3600s (1hr) for 48 candles, requiring 1 chunk.
                                                                            # To force 2 chunks: 48h * 3600s/h = 172800s. Granularity X. Candles = 172800/X.
                                                                            # If X=900 (15min), candles = 192. (1 chunk)
                                                                            # If X=60 (1min), candles = 2880. (10 chunks)
                                                                            # Let's use 60s for multiple chunks.
    mocker.patch.dict(os.environ, {"COINBASE_GRANULARITY_SECONDS": "60"})


    # First response successful (e.g. 300 candles)
    successful_response = mocker.Mock()
    successful_response.status_code = 200
    first_chunk_data = [[int((fixed_now_utc - timedelta(hours=48) + timedelta(seconds=i*60)).timestamp()), 20000, 20000, 20000, 20000+i, 1] for i in range(300)]
    successful_response.json.return_value = first_chunk_data
    
    # Second response is an error
    error_response = mocker.Mock()
    error_response.status_code = 500
    error_response.text = "Coinbase Server Error on second chunk"

    mock_requests_get.side_effect = [successful_response, error_response]

    with caplog.at_level(logging.INFO): # Capture INFO for success messages too
        result = fetch_bitcoin_prices_coinbase()

    assert len(result) == 300 # Only data from the first chunk
    assert result[0]['price'] == 20000
    assert result[299]['price'] == 20000 + 299
    
    assert "Successfully fetched 300 price points for chunk 1." in caplog.text
    assert "Failed to fetch data for chunk 2 (product ID 'BTC-USD') from Coinbase Exchange API." in caplog.text
    assert "Status code: 500, Response: Coinbase Server Error on second chunk" in caplog.text
    assert mock_requests_get.call_count == 2


@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_request_exception_first_chunk(mock_get, mocker, caplog):
    mock_get.side_effect = requests.exceptions.RequestException("Coinbase network error on first chunk")
    mocker.patch.dict(os.environ, clear=True) # Defaults: BTC-USD, 60s

    with caplog.at_level(logging.ERROR):
        result = fetch_bitcoin_prices_coinbase()

    assert result == []
    assert "RequestException occurred during Coinbase Exchange API request for chunk 1 (product ID 'BTC-USD')" in caplog.text
    mock_get.assert_called_once()

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_timeout_exception_first_chunk(mock_get, mocker, caplog):
    mock_get.side_effect = requests.exceptions.Timeout("Coinbase timeout on first chunk")
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.ERROR):
        result = fetch_bitcoin_prices_coinbase()
    
    assert result == []
    assert "Timeout occurred during Coinbase Exchange API request for chunk 1 (product ID 'BTC-USD')" in caplog.text
    mock_get.assert_called_once()

@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_malformed_json_first_chunk(mock_get, mocker, caplog):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Bad JSON from Coinbase on first chunk")
    mock_get.return_value = mock_response
    mocker.patch.dict(os.environ, clear=True)

    with caplog.at_level(logging.ERROR):
        result = fetch_bitcoin_prices_coinbase()

    assert result == []
    assert "ValueError (e.g., malformed JSON) for chunk 1 (product ID 'BTC-USD') from Coinbase Exchange API" in caplog.text
    mock_get.assert_called_once()

@patch('bitcoin_tracker.datetime')
@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_empty_data_all_chunks(mock_requests_get, mock_datetime_now, mocker, caplog):
    fixed_now_utc = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime_now.now.return_value = fixed_now_utc
    mock_datetime_now.side_effect = lambda *args, **kw: datetime(*args, **kw)
    
    mocker.patch.dict(os.environ, clear=True) # Defaults BTC-USD, 60s granularity
    granularity = 60
    expected_num_calls = (48 * 3600 // granularity) // 300 + \
                         (1 if (48 * 3600 // granularity) % 300 > 0 else 0)

    mock_empty_responses = []
    for _ in range(expected_num_calls):
        response = mocker.Mock()
        response.status_code = 200
        response.json.return_value = [] # Empty list for each chunk
        mock_empty_responses.append(response)
    
    mock_requests_get.side_effect = mock_empty_responses

    with caplog.at_level(logging.INFO):
        result = fetch_bitcoin_prices_coinbase()

    assert result == []
    assert mock_requests_get.call_count == expected_num_calls
    for i in range(expected_num_calls):
        assert f"Coinbase Exchange API returned no data for chunk {i+1}" in caplog.text
    assert "No data fetched for product ID 'BTC-USD' from Coinbase Exchange API after attempting all chunks." in caplog.text


@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_malformed_candle_data_single_chunk(mock_get, mocker, caplog):
    # This test focuses on malformed data within one chunk, pagination complexity is secondary.
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        [1678886400, 24000, 24100, 24050, 24080.50, 10],
        [1678887300, 24150.75], # Malformed
        [1678888200, 24200, 24300, 24250, 24280.00, 15]
    ]
    # Simulate only one call needed by making granularity large enough for 48h in one chunk
    # 48 hours * 3600s/hr = 172800s. If granularity is 172800, 1 candle.
    # Max 300 candles. So 172800/300 = 576s minimum granularity for 1 chunk.
    mocker.patch.dict(os.environ, {"COINBASE_GRANULARITY_SECONDS": "900"}) # 15 min, 192 candles, 1 chunk
    mock_get.return_value = mock_response


    with caplog.at_level(logging.WARNING):
        result = fetch_bitcoin_prices_coinbase()
    
    expected_data = [
        {'timestamp': 1678886400000, 'price': 24080.50},
        {'timestamp': 1678888200000, 'price': 24280.00}
    ]
    assert result == expected_data
    assert "Skipping malformed candle data in chunk 1: [1678887300, 24150.75]" in caplog.text
    assert len(result) == 2
    mock_get.assert_called_once() # Ensure it was indeed one chunk
    args, kwargs = mock_get.call_args
    assert "https://api.exchange.coinbase.com/products/BTC-USD/candles" in args[0]


@patch('bitcoin_tracker.datetime')
@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_invalid_granularity_env_pagination(mock_requests_get, mock_datetime_now, mocker, caplog):
    fixed_now_utc = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime_now.now.return_value = fixed_now_utc
    mock_datetime_now.side_effect = lambda *args, **kw: datetime(*args, **kw)

    mocker.patch.dict(os.environ, {"COINBASE_GRANULARITY_SECONDS": "xyzINVALID"})
    
    # Expect default granularity of 60s, so multiple calls
    default_granularity = 60
    expected_num_calls = (48 * 3600 // default_granularity) // 300 + \
                         (1 if (48 * 3600 // default_granularity) % 300 > 0 else 0)

    mock_responses = []
    for i in range(expected_num_calls):
        response = mocker.Mock()
        response.status_code = 200
        # Minimal data for each chunk to make it pass through processing
        response.json.return_value = [[int((fixed_now_utc - timedelta(hours=48) + timedelta(seconds=i*300*default_granularity)).timestamp()), 1,1,1,1,1]]
        mock_responses.append(response)
    mock_requests_get.side_effect = mock_responses

    with caplog.at_level(logging.WARNING):
        fetch_bitcoin_prices_coinbase()

    assert "Invalid value 'xyzINVALID' for COINBASE_GRANULARITY_SECONDS. Must be an integer. Using default: 60 seconds." in caplog.text
    assert mock_requests_get.call_count == expected_num_calls
    args, kwargs = mock_requests_get.call_args_list[0] # Check first call
    assert kwargs['params']['granularity'] == default_granularity # New default 60s
    assert "https://api.exchange.coinbase.com/products/BTC-USD/candles" in args[0]


@patch('bitcoin_tracker.datetime')
@patch('bitcoin_tracker.requests.get')
def test_fetch_coinbase_configurable_product_id_and_granularity_pagination(mock_requests_get, mock_datetime_now, mocker, caplog):
    fixed_now_utc = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime_now.now.return_value = fixed_now_utc
    mock_datetime_now.side_effect = lambda *args, **kw: datetime(*args, **kw)

    custom_product_id = "ETH-USD"
    custom_granularity = 900 # 15 minutes
    mocker.patch.dict(os.environ, {
        "COINBASE_PRODUCT_ID": custom_product_id,
        "COINBASE_GRANULARITY_SECONDS": str(custom_granularity)
    })

    # For 48 hours at 900s granularity: 48 * 3600 / 900 = 192 candles. This fits in ONE request.
    expected_num_calls = 1 

    mock_api_response = mocker.Mock()
    mock_api_response.status_code = 200
    # One candle for simplicity, the focus is on params and call count
    mock_api_response.json.return_value = [[int((fixed_now_utc - timedelta(hours=48)).timestamp()), 1500,1500,1500,1500,2]] 
    mock_requests_get.return_value = mock_api_response # Single response, not side_effect list
    
    with caplog.at_level(logging.INFO):
      fetch_bitcoin_prices_coinbase()

    assert mock_requests_get.call_count == expected_num_calls
    args, kwargs = mock_requests_get.call_args_list[0]
    assert f"https://api.exchange.coinbase.com/products/{custom_product_id}/candles" in args[0]
    assert kwargs['params']['granularity'] == custom_granularity
    assert f"product ID '{custom_product_id}'" in caplog.text
    assert f"granularity {custom_granularity} seconds" in caplog.text

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
