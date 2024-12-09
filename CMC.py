import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from psycopg2 import pool
from irdatetime import get_persian_date

DB_CONFIG = {
    'dbname': 'arzjoo1',
    'user': 'postgres',
    'password': 'Kertob93',
    'host': 'localhost',
    'port': 5432
}
db_pool = pool.SimpleConnectionPool(
    1, 10,
    **DB_CONFIG
)
conn = db_pool.getconn()

apiurl = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
    'start':1,
    'limit':2,
    'convert': 'USD'
}
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': 'a22e5c1a-0a3e-486e-9048-d69a990b7ba8',
}

def get_price_data():
    session = requests.Session()
    session.headers.update(headers)
    try:
        response = session.get(apiurl, params=parameters)
        data = response.json()
        all_data = {}
        commodities = ["BTC", "ETH"]

        for crypto in data['data']:
            symbol = crypto['symbol']
            for name in commodities:
                if symbol == name:
                    price = crypto['quote']['USD']['price']  # Get price in USD
                    all_data[name] = round(price, 2)
        print(all_data)
        with conn:
            source_id = 3
            source_time = get_persian_date()
            save_prices_batch(all_data, source_id, source_time)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"Error occurred: {e}")
        return {}


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
