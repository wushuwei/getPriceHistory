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
        return client
    except pymongo.errors.ConnectionFailure as e:
        logging.exception("MongoDB ConnectionFailure: Failed to connect to MongoDB:")
        return None
    except pymongo.errors.PyMongoError as e:
        logging.exception("PyMongoError occurred with MongoDB connection:")
        return None
    except Exception as e:
        logging.exception("An unexpected error occurred during MongoDB connection:")
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
        logging.info(f"Data insertion/update attempt complete. {inserted_count} new documents were inserted.")
    except pymongo.errors.PyMongoError as e:
        logging.exception("PyMongoError occurred during MongoDB data insertion:")
        return inserted_count 
    except Exception as e:
        logging.exception("An unexpected error occurred during data insertion:")
        return inserted_count

    return inserted_count

if __name__ == "__main__":
    logging.info("Starting Bitcoin price tracking script.")
    
    prices = fetch_bitcoin_prices()
    if prices:
        logging.info(f"Fetched {len(prices)} Bitcoin price points.")
    else:
        logging.error("Failed to fetch Bitcoin prices. No data to process.")

    mongo_client = connect_to_mongodb()
    if mongo_client:
        logging.info("Successfully connected to MongoDB.")
        if prices: # Only attempt to insert if prices were fetched
            inserted_docs = insert_bitcoin_data(mongo_client, prices)
            logging.info(f"{inserted_docs} new documents inserted into MongoDB.")
        else:
            logging.info("No price data to insert into MongoDB.")
        
        try:
            mongo_client.close()
            logging.info("MongoDB connection closed.")
        except Exception as e:
            logging.exception("Error occurred while closing MongoDB connection:")
    else:
        logging.error("Failed to connect to MongoDB. Data insertion skipped.")
    
    logging.info("Bitcoin price tracking script finished.")
