import requests
import psycopg2
from psycopg2 import sql

# API endpoint
URL = "https://apidocs.nobitex.ir/"  # Replace with your API URL

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


def get_price_data():
    try:
        nobitex = {
            "Tether": {"symbol": "USDT", "url": "https://api.nobitex.ir/v3/orderbook/USDTIRT","name_fa" : "تتر" ,"unit" : "Toman"},
            "BTCToman": {"symbol": "BTCIRT", "url": "https://api.nobitex.ir/v3/orderbook/BTCIRT","name_fa" : "بیت کوین به تومن" ,"unit" : "Toman"}
        }
        all_data = {}
        for name,details in nobitex.items():
            response = requests.get(details["url"])
            response.raise_for_status()  # Raise an exception for HTTP errors
            api_output = response.json()  # Parse JSON response

            # Check if status is "ok"
            if api_output.get("status") == "ok":
                last_trade_price = api_output.get("lastTradePrice")
                all_data[name] = {
                        "symbol": details["symbol"],
                        "price": float(last_trade_price) / 10,
                        "name_fa" : details["name_fa"],
                        "unit": details["unit"]
                    }
                # Print results
            else:
                print("Status is not 'ok'.")
        print(all_data)
        return all_data
    except requests.exceptions.RequestException as e:
        print(f"Error while calling the API: {e}")

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
        insert_data(conn, "Nobitex", URL, price_data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()