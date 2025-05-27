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
    default_granularity_seconds = 900  # 15 minutes
    granularity_env = os.getenv('COINBASE_GRANULARITY_SECONDS')
    granularity = default_granularity_seconds

    if granularity_env:
        try:
            granularity_int = int(granularity_env)
            # Coinbase Pro API has specific allowed granularities: 60, 300, 900, 3600, 21600, 86400
            # For this example, we'll allow any integer, but in a real scenario, validation against allowed values would be good.
            granularity = granularity_int
        except ValueError:
            logging.warning(
                f"Invalid value '{granularity_env}' for COINBASE_GRANULARITY_SECONDS. "
                f"Must be an integer. Using default: {default_granularity_seconds} seconds."
            )
    
    url = f"https://api.pro.coinbase.com/products/{product_id}/candles"
    
    # Calculate start and end times for the API request (last 48 hours)
    # Coinbase API expects ISO 8601 format for start and end times.
    # It's crucial these are in UTC.
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=48)

    # Format times for the API
    start_iso = start_time.isoformat().replace('+00:00', 'Z') 
    end_iso = end_time.isoformat().replace('+00:00', 'Z')

    params = {
        'granularity': granularity, # Use the configured or default granularity
        'start': start_iso,
        'end': end_iso
    }

    logging.info(
        f"Attempting to fetch Coinbase Pro data for product ID '{product_id}' "
        f"with granularity {granularity} seconds for the last 48 hours..."
    )
    # The following log is more specific about the exact time window and granularity
    logging.info(f"Requesting data from {start_iso} to {end_iso} with granularity {granularity}s for product '{product_id}'.")

    all_formatted_prices = []

    try:
        response = requests.get(url, params=params, timeout=10) # Added timeout
        if response.status_code == 200:
            data = response.json()
            # Coinbase returns candles as: [time, low, high, open, close, volume]
            # time is epoch seconds. We need to convert to milliseconds.
            if not data:
                logging.info("Coinbase API returned no data for the requested range.")
                return []

            formatted_prices = []
            for candle in data:
                # Ensure candle has enough elements (at least 5 for time and close)
                if len(candle) >= 5:
                    timestamp_ms = int(candle[0]) * 1000  # Convert epoch seconds to milliseconds
                    close_price = candle[4]
                    formatted_prices.append({'timestamp': timestamp_ms, 'price': float(close_price)})
                else:
                    logging.warning(f"Skipping malformed candle data: {candle}")
            
            # Coinbase returns data in ascending order of time by default with start/end parameters.
            # If it were descending, we might need to reverse: formatted_prices.reverse()
            all_formatted_prices.extend(formatted_prices)
            
            logging.info(f"Successfully fetched {len(all_formatted_prices)} price points for product ID '{product_id}' from Coinbase Pro.")
            return all_formatted_prices
        else:
            logging.error(
                f"Failed to fetch data for product ID '{product_id}' from Coinbase Pro. "
                f"Status code: {response.status_code}, Response: {response.text}"
            )
            return []
    except requests.exceptions.Timeout as e:
        logging.exception(f"Timeout occurred during Coinbase API request for product ID '{product_id}':")
        return []
    except requests.exceptions.RequestException as e:
        logging.exception(f"RequestException occurred during Coinbase API request for product ID '{product_id}':")
        return []
    except ValueError as e:  # Specifically catch JSON parsing errors
        logging.exception(
            f"ValueError (e.g., malformed JSON) occurred during Coinbase API response processing for product ID '{product_id}':"
        )
        return []
    except Exception as e:
        logging.exception(f"An unexpected error occurred during fetch_bitcoin_prices_coinbase for product ID '{product_id}':")
        return []

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
