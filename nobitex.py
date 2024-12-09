import requests
import psycopg2
from psycopg2 import pool
from irdatetime import get_persian_date

# Database connection details
DB_CONFIG = {
    'dbname': 'arzjoo1',
    'user': 'postgres',
    'password': 'Kertob93',
    'host': 'localhost',
    'port': 5432
}
db_pool = pool.SimpleConnectionPool(
    1, 10,  # Min and max connections in the pool
    **DB_CONFIG
)
conn = db_pool.getconn()

def get_price_data():
    try:
        nobitex = {
            "USDT": "https://api.nobitex.ir/v3/orderbook/USDTIRT",
            "BTCIRT": "https://api.nobitex.ir/v3/orderbook/BTCIRT"}
        all_data = {}
        for name,url in nobitex.items():
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            api_output = response.json()  # Parse JSON response

            # Check if status is "ok"
            if api_output.get("status") == "ok":
                last_trade_price = api_output.get("lastTradePrice")
                all_data[name] = float(last_trade_price) /10
            else:
                print("Status is not 'ok'.")
        print(all_data)
        with conn:
            source_id = 4  # Example source ID for Navasan
            source_time = get_persian_date()  # Current timestamp
            save_prices_batch(all_data, source_id, source_time)
    except requests.exceptions.RequestException as e:
        print(f"Error while calling the API: {e}")

def save_prices_batch(commodity_prices, source_id, source_time):
    """
    Save multiple prices to the database in a single batch.
    """
    try:
        with conn.cursor() as cur:
            # Get all commodity IDs for the given symbols in a single query
            symbols = tuple(commodity_prices.keys())
            cur.execute("""
                SELECT id, symbol FROM commodities WHERE symbol IN %s;
            """, (symbols,))
            commodity_mapping = {row[1]: row[0] for row in cur.fetchall()}

            # Prepare data for bulk insertion
            rows_to_insert = []
            for symbol, price in commodity_prices.items():
                if symbol in commodity_mapping and price is not None:
                    commodity_id = commodity_mapping[symbol]
                    # Ensure price is converted to a proper float
                    try:
                        price = float(price)
                    except ValueError:
                        print(f"Skipping invalid price for {symbol}: {price}")
                        continue
                    rows_to_insert.append((commodity_id, source_id, price, source_time))

            # Debugging: Check the data to be inserted
            print(f"Rows to insert: {rows_to_insert}")

            # Bulk insert into prices table
            if rows_to_insert:
                cur.executemany("""
                    INSERT INTO prices (commodity_id, source_id, price, source_time)
                    VALUES (%s, %s, %s, %s);
                """, rows_to_insert)
                conn.commit()
                print(f"Inserted {len(rows_to_insert)} price records successfully.")
            else:
                print("No valid rows to insert.")

    except Exception as e:
        print(f"Error in batch save: {e}")

get_price_data()