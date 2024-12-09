import psycopg2
from psycopg2 import sql

# Database connection settings
DB_NAME = "arzjoo1"
DB_USER = "postgres"
DB_PASSWORD = "Kertob93"
DB_HOST = "localhost"  # Or your database host

# Data for commodities table
COMMODITIES_DATA = [
    (1, "US Dollar", "USD", "Toman", "دلار آمریکا"),
    (2, "Euro", "EUR", "Toman", "یورو"),
    (3, "Sekkeh Emami", "EMAMI", "Toman", "سکه امامی"),
    (4, "Sekkeh Bahar", "BAHAR", "Toman", "سکه بهار آزادی"),
    (5, "Gold 18 Karat", "GOLD18", "Toman", "طلای 18 عیار"),
    (6, "Mesqal", "MESQAL", "Toman", "مثقال طلای آبشده"),
    (7, "Nim Sekkeh", "NIM_SEKKEH", "Toman", "نیم سکه"),
    (8, "Rob Sekkeh", "ROB_SEKKEH", "Toman", "ربع سکه"),
    (9, "Ounce", "XAUO", "USD", "انس جهانی"),
    (10, "Dirham", "AED", "Toman", "درهم امارات"),
    (11, "Lira", "TRY", "Toman", "لیر ترکیه"),
    (12, "Bitcoin", "BTC", "USD", "بیت کوین"),
    (13, "Ethereum", "ETH", "USD", "اتریوم"),
    (14, "Tether", "USDT", "Toman", "تتر"),
    (15, "Bitcoin/Toman", "BTCIRT", "Toman", "بیت کوین به تومان"),
]

# Data for sources table (example source)
SOURCES_DATA = [
    (1, "Navasan Channel", "https://t.me/navasanchannel"),
    (2, "TGJU", "https://www.tgju.org/"),
    (3, "CoinMarketCap", "https://coinmarketcap.com/api/"),
    (4, "Nobitex", "https://apidocs.nobitex.ir/"),
]

# Function to create tables
def create_tables(conn):
    with conn.cursor() as cur:
        # SQL statements to create tables
        create_commodities_table = """
        CREATE TABLE IF NOT EXISTS commodities (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            unit VARCHAR(20) NOT NULL,
            name_fa VARCHAR(50) NOT NULL
        );
        """
        create_sources_table = """
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            url VARCHAR(200) NOT NULL
        );
        """
        create_prices_table = """
        CREATE TABLE IF NOT EXISTS prices (
            id SERIAL PRIMARY KEY,
            commodity_id INT REFERENCES commodities(id),
            source_id INT REFERENCES sources(id),
            price FLOAT NOT NULL,
            source_time VARCHAR(255) NULL, 
            read_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        # Execute table creation statements
        cur.execute(create_commodities_table)
        cur.execute(create_sources_table)
        cur.execute(create_prices_table)
        conn.commit()
        print("Tables created successfully.")

# Function to insert initial data
def insert_initial_data(conn):
    with conn.cursor() as cur:
        # Insert data into commodities table
        for commodity in COMMODITIES_DATA:
            cur.execute("""
                INSERT INTO commodities (id, name, symbol, unit, name_fa)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, commodity)

        # Insert data into sources table
        for source in SOURCES_DATA:
            cur.execute("""
                INSERT INTO sources (id, name, url)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, source)

        conn.commit()
        print("Initial data inserted successfully.")

# Main function to set up the database
def setup_database():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        print("Connected to the database.")
        
        # Create tables
        create_tables(conn)
        
        # Insert initial data
        insert_initial_data(conn)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    setup_database()
