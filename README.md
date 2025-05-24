# Bitcoin Price Tracker

## Description
This script fetches Bitcoin (BTC) price data against USD for the last 5 days using the CoinGecko API. It attempts to retrieve data at 15-minute intervals but will fall back to hourly intervals if the API restricts the finer granularity for the requested range. The collected price data is then stored in a MongoDB database.

## Features
*   Fetches Bitcoin (BTC) price data against USD.
*   Retrieves data for the past 5 days.
*   Attempts 15-minute intervals, falls back to hourly if necessary (due to API limitations for extended ranges).
*   Stores data in a MongoDB database:
    *   Database: `crypto_prices`
    *   Collection: `bitcoin_usd`
*   Prevents duplicate entries by using the timestamp as a unique key for an upsert operation.
*   Uses environment variables for secure and flexible MongoDB configuration.
*   Includes comprehensive logging for monitoring script execution and tracking errors.

## Prerequisites
*   Python 3.7+
*   A running MongoDB instance (either local or cloud-based, e.g., MongoDB Atlas).

## Setup & Installation

**1. Clone the repository:**
   ```bash
   git clone <repository_url> # Replace <repository_url> with the actual repository URL
   cd <repository_directory> # Replace <repository_directory> with the name of the cloned folder
   ```

**2. Create and activate a virtual environment:**
   It's highly recommended to use a virtual environment to manage project dependencies.
   ```bash
   python -m venv venv
   ```
   Activate the environment:
   *   On Windows:
       ```bash
       venv\Scripts\activate
       ```
   *   On macOS/Linux:
       ```bash
       source venv/bin/activate
       ```

**3. Install dependencies:**
   Install the required Python libraries using the `requirements.txt` file:
   ```bash
   pip install -r requirements.txt
   ```

**4. Configure environment variables:**
   The script uses a `.env` file to manage the MongoDB connection string.
   *   Copy the example environment file:
       *   On macOS/Linux:
           ```bash
           cp .env.example .env
           ```
       *   On Windows:
           ```bash
           copy .env.example .env
           ```
   *   Edit the newly created `.env` file and set your `MONGODB_URI`. Refer to the comments within the `.env.example` file for guidance on formatting the URI for local MongoDB instances or MongoDB Atlas.
       Example placeholder in `.env`:
       ```
       MONGODB_URI="your_mongodb_connection_string_here"
       ```

## Running the Script
Once the setup is complete and the virtual environment is activated, run the script from the root directory of the project:
```bash
python bitcoin_tracker.py
```
The script will fetch the data and attempt to store it in your configured MongoDB database.

## Logging
*   The script uses Python's built-in `logging` module for output.
*   Informational messages, errors, and warnings will be printed to the console.
*   The default log level is set to `INFO`.
*   Log messages are formatted to include a timestamp, log level, and the message content (e.g., `YYYY-MM-DD HH:MM:SS - INFO - Your message here`).

## API and Data Interval
*   This script relies on the public CoinGecko API (specifically the `/coins/{id}/market_chart` endpoint).
*   It initially requests data at 15-minute intervals for the last 5 days. However, the CoinGecko API may return data at coarser intervals (like hourly) when a longer time frame (such as 5 days) is requested with fine-grained intervals (like '15m'). The script is designed to attempt the '15m' interval first and then automatically retry with an 'hourly' interval if the initial request does not yield a successful response (e.g., due to API limitations on data points for the given range).