import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql

# Database connection details
DB_CONFIG = {
    'dbname': 'arzjoo',
    'user': 'postgres',
    'password': 'Kertob93',
    'host': 'localhost',
    'port': 5432
}

# Webpage to scrape
URL = "https://www.tgju.org/"

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
    """Scrape price data from the webpage and convert to integers."""
    response = requests.get(URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    
    data = {
        "Dollar": {
            "symbol": "USD",
            "price": float(soup.select_one("#l-price_dollar_rl .info-price").text.strip().replace(",", "")) / 10,
            "name_fa" : "دلار",
            "unit": "Toman"
        },
        "Ounce": {
            "symbol": "XAUO",
            "price": float(soup.select_one("#l-ons .info-price").text.strip().replace(",", "")),
            "name_fa" : "انس جهانی",
            "unit": "USD"

        },
        "Mesghal": {
            "symbol": "MSQ",
            "price": float(soup.select_one("#l-mesghal .info-price").text.strip().replace(",", "")) / 10,
            "name_fa" : "مثقال",
            "unit": "Toman"
        },
        "Gold18": {
            "symbol": "XAU18",
            "price": float(soup.select_one("#l-geram18 .info-price").text.strip().replace(",", "")) / 10,
            "name_fa" : "گرم 18 عیار",
            "unit": "Toman"
        },
        "CoinEmam": {
            "symbol": "coine",
            "price": float(soup.select_one("#l-sekee .info-price").text.strip().replace(",", "")) / 10,
            "name_fa" : "سکه امامی",
            "unit": "Toman"
        },
        "Bitcoin": {
            "symbol": "BTC",
            "price": float(soup.select_one("#l-crypto-bitcoin .info-price").text.strip().replace(",", "")),
            "name_fa" : "بیت کوین",
            "unit": "USD"
        },
        "Euro": {
            "symbol": "EUR",
            "price": float(soup.find('tr', {'data-market-row': 'price_eur'}).find('td', class_='market-price').text.strip().replace(",", "")) / 10,
            "name_fa" : "یورو",
            "unit": "Toman"
        },
        "Dirham": {
            "symbol": "AED",
            "price": float(soup.find('tr', {'data-market-row': 'price_aed'}).find('td', class_='market-price').text.strip().replace(",", "")) / 10,
            "name_fa" : "درهم",
            "unit": "Toman"
        },
        "Lira": {
            "symbol": "TRY",
            "price": float(soup.find('tr', {'data-market-row': 'price_try'}).find('td', class_='market-price').text.strip().replace(",", "")) / 10,
            "name_fa" : "لیر",
            "unit": "Toman"
        },
    }
    print(data)
    return data

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
        insert_data(conn, "TGJU", URL, price_data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()