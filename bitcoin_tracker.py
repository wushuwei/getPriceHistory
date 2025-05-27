# It is recommended to run this script in a Python virtual environment.
# Create a virtual environment: python -m venv venv
# Activate the virtual environment:
# On Windows: venv\Scripts\activate
# On macOS/Linux: source venv/bin/activate
# Install dependencies: pip install -r requirements.txt

# Import necessary libraries
import requests # (for making HTTP requests)
import pymongo # (for MongoDB interaction)
from dotenv import load_dotenv # (for environment variables)
import os # (for accessing environment variables)
import logging # For logging
from datetime import datetime, timedelta, timezone # For timestamp manipulation
from concurrent.futures import ThreadPoolExecutor # For concurrent fetching

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Load environment variables from .env file
load_dotenv()

def fetch_bitcoin_prices():
    """
    Fetches Bitcoin price data from the CoinGecko API.

    Returns:
        list: A list of dictionaries, where each dictionary has keys 'timestamp' 
              (in milliseconds) and 'price'. Returns an empty list if an error occurs.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    days_value = '5' # Standard value for this script
    params = {
        'vs_currency': 'usd',
        'days': days_value,
        # No 'interval' key, API defaults to hourly for 'days' > 1
    }
    logging.info(f"Attempting to fetch hourly Bitcoin price data for the last {days_value} days (default interval)...")

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            prices = data.get('prices', [])
            if prices is None: # Handle cases where 'prices' key might exist but be None
                logging.warning("API response successful, but 'prices' key is None.")
                return []
            formatted_prices = [{'timestamp': item[0], 'price': item[1]} for item in prices]
            logging.info(f"Successfully fetched {len(formatted_prices)} hourly price points for the last {days_value} days.")
            return formatted_prices
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        logging.exception("RequestException occurred during API request:")
        return []
    except ValueError as e: # Specifically catch JSON parsing errors
        logging.exception("ValueError (e.g., malformed JSON) occurred during API response processing:")
        return []
    except Exception as e: 
        logging.exception("An unexpected error occurred during fetch_bitcoin_prices:")
        return []

def fetch_bitcoin_prices_coinbase():
    """
    Fetches Bitcoin price data from the Coinbase Pro API for the last 48 hours.
    Product ID and granularity are configurable via environment variables.

    Returns:
        list: A list of dictionaries, where each dictionary has keys 'timestamp'
              (epoch milliseconds) and 'price' (closing price).
              Returns an empty list if an error occurs.
    """
    # Configuration for Product ID
    default_product_id = "BTC-USD"
    product_id = os.getenv('COINBASE_PRODUCT_ID', default_product_id)

    # Configuration for Granularity
    default_granularity_seconds = 60  # New default: 1 minute
    granularity_env = os.getenv('COINBASE_GRANULARITY_SECONDS')
    granularity = default_granularity_seconds

    if granularity_env:
        try:
            granularity_int = int(granularity_env)
            # Coinbase Exchange API allowed granularities:
            # UNKNOWN_GRANULARITY, ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, THIRTY_MINUTE, ONE_HOUR, TWO_HOUR, SIX_HOUR, ONE_DAY
            # Corresponding seconds: N/A, 60, 300, 900, 1800, 3600, 7200, 21600, 86400
            # We'll allow any integer for now, but validation against these specific values would be more robust.
            granularity = granularity_int
            logging.info(f"Using COINBASE_GRANULARITY_SECONDS from environment: {granularity}s.")
        except ValueError:
            logging.warning(
                f"Invalid value '{granularity_env}' for COINBASE_GRANULARITY_SECONDS. "
                f"Must be an integer. Using default: {default_granularity_seconds} seconds."
            )
    else:
        logging.info(f"COINBASE_GRANULARITY_SECONDS not set. Using default: {default_granularity_seconds} seconds.")

    # New API endpoint
    base_url = "https://api.exchange.coinbase.com"
    url = f"{base_url}/products/{product_id}/candles"
    
    all_formatted_prices = []
    
    # Pagination logic
    # Total duration to fetch: 48 hours
    total_duration = timedelta(hours=48)
    # Maximum candles per API request (Coinbase typically limits to 300)
    max_candles_per_request = 300 
    
    # Calculate the overall start and end for the 48-hour window
    overall_end_time_utc = datetime.now(timezone.utc)
    overall_start_time_utc = overall_end_time_utc - total_duration

    current_chunk_start_time = overall_start_time_utc
    chunk_number = 0

    logging.info(
        f"Attempting to fetch Coinbase Exchange API data for product ID '{product_id}' "
        f"with granularity {granularity} seconds for the last 48 hours."
    )
    logging.info(f"Total window: {overall_start_time_utc.isoformat()} to {overall_end_time_utc.isoformat()}")

    while current_chunk_start_time < overall_end_time_utc:
        chunk_number += 1
        # Calculate end time for the current chunk
        # Fetch up to max_candles_per_request worth of data
        potential_chunk_end_time = current_chunk_start_time + timedelta(seconds=max_candles_per_request * granularity)
        current_chunk_end_time = min(potential_chunk_end_time, overall_end_time_utc)

        start_iso = current_chunk_start_time.isoformat().replace(".%f", "") # Remove microseconds if any for cleaner ISO
        end_iso = current_chunk_end_time.isoformat().replace(".%f", "")     # Remove microseconds

        params = {
            'granularity': granularity,
            'start': start_iso,
            'end': end_iso
        }

        logging.info(
            f"Fetching chunk {chunk_number}: product ID '{product_id}', granularity {granularity}s, "
            f"start: {start_iso}, end: {end_iso}"
        )

        try:
            response = requests.get(url, params=params, timeout=20) # Increased timeout for potentially larger requests
            if response.status_code == 200:
                data = response.json()
                # Coinbase returns candles as: [time_epoch_sec, low, high, open, close, volume]
                # time is epoch seconds. We need to convert to milliseconds.
                if not data:
                    logging.info(f"Coinbase Exchange API returned no data for chunk {chunk_number} in range {start_iso} to {end_iso}.")
                    # If no data, and this isn't the absolute end, we might want to break or adjust.
                    # For now, we continue to the next chunk.
                    current_chunk_start_time = current_chunk_end_time 
                    if current_chunk_start_time >= overall_end_time_utc and not all_formatted_prices:
                         logging.warning(f"No data received for any chunk for product ID '{product_id}'.") # Warn if all chunks are empty
                    continue


                formatted_prices_chunk = []
                for candle in data:
                    if len(candle) >= 5: # time, low, high, open, close
                        timestamp_ms = int(candle[0]) * 1000
                        close_price = candle[4]
                        formatted_prices_chunk.append({'timestamp': timestamp_ms, 'price': float(close_price)})
                    else:
                        logging.warning(f"Skipping malformed candle data in chunk {chunk_number}: {candle}")
                
                # Data from Coinbase /candles endpoint is typically returned in ascending order of time (oldest first).
                # So, we can directly extend. If it were descending, we'd use `formatted_prices_chunk.reverse()` first.
                all_formatted_prices.extend(formatted_prices_chunk)
                logging.info(f"Successfully fetched {len(formatted_prices_chunk)} price points for chunk {chunk_number}.")

            else:
                logging.error(
                    f"Failed to fetch data for chunk {chunk_number} (product ID '{product_id}') from Coinbase Exchange API. "
                    f"Status code: {response.status_code}, Response: {response.text}"
                )
                # Optionally, break or implement retry logic here for robust fetching
                break # Stop fetching if one chunk fails

        except requests.exceptions.Timeout as e:
            logging.exception(f"Timeout occurred during Coinbase Exchange API request for chunk {chunk_number} (product ID '{product_id}'):")
            break 
        except requests.exceptions.RequestException as e:
            logging.exception(f"RequestException occurred during Coinbase Exchange API request for chunk {chunk_number} (product ID '{product_id}'):")
            break
        except ValueError as e:  # JSON parsing errors
            logging.exception(
                f"ValueError (e.g., malformed JSON) for chunk {chunk_number} (product ID '{product_id}') from Coinbase Exchange API:"
            )
            break
        except Exception as e:
            logging.exception(f"An unexpected error occurred during fetch for chunk {chunk_number} (product ID '{product_id}'):")
            break # Stop on unexpected error

        # Move to the next time window
        current_chunk_start_time = current_chunk_end_time
        
        # Small delay to be polite to the API, especially if many chunks
        # time.sleep(0.2) # Consider importing 'time' if using sleep

    if all_formatted_prices:
        # Sort by timestamp just in case chunks were out of order or API behaves unexpectedly
        all_formatted_prices.sort(key=lambda x: x['timestamp'])
        logging.info(f"Successfully fetched a total of {len(all_formatted_prices)} price points for product ID '{product_id}' from Coinbase Exchange API after processing all chunks.")
    else:
        logging.warning(f"No data fetched for product ID '{product_id}' from Coinbase Exchange API after attempting all chunks.")
        
    return all_formatted_prices

def connect_to_mongodb():
    """
    Connects to MongoDB using the connection string from environment variables.

    Returns:
        pymongo.MongoClient: MongoClient object if connection is successful, None otherwise.
    """
    uri = os.getenv('MONGODB_URI')
    if not uri:
        logging.error("MONGODB_URI not found in environment variables.")
        return None

    try:
        client = pymongo.MongoClient(uri)
        client.admin.command('ismaster')
        logging.info("MongoDB connection successful (checked with ismaster).")

        # Create a unique index on the timestamp field
        try:
            db = client.crypto_prices
            collection = db.bitcoin_usd
            logging.info(f"Attempting to create/ensure unique ascending index on 'timestamp' for collection '{collection.name}' in database '{db.name}'...")
            # Ensure index exists, create if not.
            # PyMongo's create_index is idempotent. If the index already exists with the same specs, no operation is performed.
            collection.create_index([('timestamp', pymongo.ASCENDING)], unique=True, name='timestamp_unique_asc_idx')
            logging.info(f"Unique ascending index on 'timestamp' for collection '{collection.name}' ensured.")
        except pymongo.errors.OperationFailure as e: # Specific PyMongo operational errors
            # This can happen if index exists but with different options, or other operational issues.
            logging.exception(f"OperationFailure occurred while creating unique index for '{collection.name}':")
            # Depending on policy, you might not want to return client if index is critical and fails.
            # For this task, we return client if connection was okay.
        except pymongo.errors.PyMongoError as e: # Broader PyMongo errors
            logging.exception(f"PyMongoError occurred while creating unique index for '{collection.name}':")
        except Exception as e: # Catch any other unexpected errors
            logging.exception(f"An unexpected error occurred while creating unique index for '{collection.name}':")
        
        return client
    except pymongo.errors.ConnectionFailure as e:
        logging.exception("MongoDB ConnectionFailure: Failed to connect to MongoDB:")
        return None
    except pymongo.errors.PyMongoError as e: # This will catch connection errors if MongoClient fails before ismaster
        logging.exception("PyMongoError occurred with MongoDB connection (initial connection phase):")
        return None
    except Exception as e: # Catch any other unexpected errors during connection phase
        logging.exception("An unexpected error occurred during MongoDB connection (initial connection phase):")
        return None

def insert_bitcoin_data(mongo_client, price_data_list):
    """
    Inserts Bitcoin price data into MongoDB.

    Args:
        mongo_client (pymongo.MongoClient): The MongoDB client.
        price_data_list (list): A list of price data dictionaries.

    Returns:
        int: The number of newly inserted documents.
    """
    if not mongo_client:
        logging.error("MongoDB client is None. Cannot insert data.")
        return 0
    if not price_data_list:
        logging.info("No price data provided to insert.")
        return 0

    db = mongo_client.crypto_prices
    collection = db.bitcoin_usd
    inserted_count = 0

    try:
        logging.info(f"Attempting to insert/update {len(price_data_list)} price data items.")
        for item in price_data_list:
            result = collection.update_one(
                {'timestamp': item['timestamp']},
                {'$set': item},
                upsert=True
            )
            if result.upserted_id is not None:
                inserted_count += 1
        logging.info(f"Data insertion/update attempt complete for {len(price_data_list)} items from combined sources. {inserted_count} new documents were inserted, duplicates skipped/updated due to unique timestamp.")
    except pymongo.errors.PyMongoError as e:
        logging.exception("PyMongoError occurred during MongoDB data insertion:")
        return inserted_count 
    except Exception as e:
        logging.exception("An unexpected error occurred during data insertion:")
        return inserted_count

    return inserted_count

if __name__ == "__main__":
    logging.info("Starting Bitcoin price tracking script with concurrent fetching.")

    combined_prices = []
    
    logging.info("Initiating concurrent fetching from CoinGecko and Coinbase Pro...")
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_coingecko = executor.submit(fetch_bitcoin_prices)
        future_coinbase = executor.submit(fetch_bitcoin_prices_coinbase)

        try:
            logging.info("Waiting for CoinGecko data...")
            coingecko_prices = future_coingecko.result() # Add timeout if necessary, e.g., timeout=60
            if coingecko_prices:
                logging.info(f"Fetched {len(coingecko_prices)} price points from CoinGecko.")
                combined_prices.extend(coingecko_prices)
            else:
                # fetch_bitcoin_prices already logs errors/warnings if it returns an empty list
                logging.warning("No price data returned from CoinGecko or an error occurred during its fetching.")
        except Exception as e:
            logging.exception("Exception occurred while fetching data from CoinGecko:")

        try:
            logging.info("Waiting for Coinbase Pro data...")
            coinbase_prices = future_coinbase.result() # Add timeout if necessary
            if coinbase_prices:
                logging.info(f"Fetched {len(coinbase_prices)} price points from Coinbase Pro.")
                combined_prices.extend(coinbase_prices)
            else:
                # fetch_bitcoin_prices_coinbase already logs errors/warnings if it returns an empty list
                logging.warning("No price data returned from Coinbase Pro or an error occurred during its fetching.")
        except Exception as e:
            logging.exception("Exception occurred while fetching data from Coinbase Pro:")

    if not combined_prices:
        logging.error("No price data fetched from any source. Further processing (MongoDB insertion) will be skipped.")
    else:
        logging.info(f"Total of {len(combined_prices)} price points fetched from all sources.")

    # MongoDB operations
    mongo_client = None # Initialize to ensure it's defined for the finally block
    if combined_prices: # Only connect and insert if there's data
        mongo_client = connect_to_mongodb()
        if mongo_client:
            logging.info("Successfully connected to MongoDB.")
            inserted_docs = insert_bitcoin_data(mongo_client, combined_prices)
            logging.info(f"{inserted_docs} new documents inserted into MongoDB from combined sources.")
        else:
            logging.error("Failed to connect to MongoDB. Data insertion skipped for combined data.")
    else:
        logging.info("No combined price data available to insert into MongoDB.")

    # Close MongoDB connection if it was opened
    if mongo_client:
        try:
            mongo_client.close()
            logging.info("MongoDB connection closed.")
        except Exception as e:
            logging.exception("Error occurred while closing MongoDB connection:")
    
    logging.info("Bitcoin price tracking script finished.")
