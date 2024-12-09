import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import psycopg2
from psycopg2 import sql

URL = "https://coinmarketcap.com/api/"
DB_CONFIG = {
    'dbname': 'arzjoo',
    'user': 'postgres',
    'password': 'Kertob93',
    'host': 'localhost',
    'port': 5432
}

def create_tables(conn):
    """Create tables in the database."""
    commands = [
        """
        CREATE TABLE IF NOT EXISTS source (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            url VARCHAR(2083) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS commodities (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            unit VARCHAR(25) NOT NULL,
            name_fa VARCHAR(25) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS prices (
            id SERIAL PRIMARY KEY,
            commodity_id INT NOT NULL,
            source_id INT NOT NULL,
            price NUMERIC(15, 2) NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (commodity_id) REFERENCES commodities (id),
            FOREIGN KEY (source_id) REFERENCES source (id)
        )
        """
    ]
    cursor = conn.cursor()
    for command in commands:
        cursor.execute(command)
    conn.commit()
    cursor.close()


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
        commodities = {
            "Bitcoin": {"symbol": "BTC", "name_fa": "بیت کوین", "unit": "USD"},
            "Ethereum": {"symbol": "ETH", "name_fa": "اتریوم", "unit": "USD"}
        }


        for crypto in data['data']:
            symbol = crypto['symbol']
            for name, details in commodities.items():
                if symbol == details["symbol"]:
                    price = crypto['quote']['USD']['price']  # Get price in USD
                    all_data[name] = {
                        'symbol': details['symbol'],
                        'price': round(price, 2),  # Round to two decimal places for clarity
                        'name_fa': details['name_fa'],
                        'unit': details['unit']
                    }
        print(all_data)
        return all_data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"Error occurred: {e}")
        return {}

def insert_data(conn, source_name, source_url, data):
    """Insert data into the database."""
    cursor = conn.cursor()

    # Insert the source
    cursor.execute(
        sql.SQL("""
            INSERT INTO source (name, url)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING RETURNING id
        """),
        [source_name, source_url]
    )
    source_id = cursor.fetchone()[0] if cursor.rowcount else None
    if not source_id:
        cursor.execute("SELECT id FROM source WHERE name = %s", [source_name])
        source_id = cursor.fetchone()[0]

    # Prepare data for commodities and prices insertion
    commodity_data = []
    price_data = []

    for commodity_name, details in data.items():
        # Prepare commodity data
        commodity_data.append((
            commodity_name,
            details["symbol"],
            details["unit"],
            details["name_fa"]
        ))

        # Get commodity ID
        cursor.execute(
            sql.SQL("SELECT id FROM commodities WHERE name = %s"),
            [commodity_name]
        )
        commodity_id = cursor.fetchone()[0] if cursor.rowcount else None
        if not commodity_id:
            # Insert the commodity if it doesn't exist
            cursor.execute(
                sql.SQL("""
                    INSERT INTO commodities (name, symbol, unit, name_fa)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name) DO NOTHING RETURNING id
                """),
                [commodity_name, details["symbol"], details["unit"], details["name_fa"]]
            )
            commodity_id = cursor.fetchone()[0]

        # Prepare price data
        price_data.append((commodity_id, source_id, details["price"]))

    # Insert commodities in bulk
    cursor.executemany(
        sql.SQL("""
            INSERT INTO commodities (name, symbol, unit, name_fa)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name) DO NOTHING
        """),
        commodity_data
    )

    # Insert prices in bulk
    cursor.executemany(
        sql.SQL("""
            INSERT INTO prices (commodity_id, source_id, price)
            VALUES (%s, %s, %s)
        """),
        price_data
    )

    conn.commit()
    cursor.close()

def main():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        # Create tables if they don't exist
        create_tables(conn)

        # Scrape price data
        price_data = get_price_data()

        # Insert data into the database
        insert_data(conn, "CMC", URL, price_data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()