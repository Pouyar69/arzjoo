import psycopg2
from psycopg2 import sql

# Database connection details
DB_CONFIG = {
    'dbname': 'arzjooN',
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
            name_fa VARCHAR(25) NOT NULL,
            source_name VARCHAR(25) NOT NULL,
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS prices (
            id SERIAL PRIMARY KEY,
            commodity_id INT NOT NULL,
            source_id INT NOT NULL,
            price NUMERIC(15, 2) NOT NULL,
            read_date VARCHAR(100) NOT NULL,
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


commodity_details = [
    (1, "Gold", "XAU", "ounce", "طلا"),
    (2, "Silver", "XAG", "ounce", "نقره"),
    (3, "Oil", "WTI", "barrel", "نفت"),
    # Add more commodities...
]

def preload_commodities_with_ids(conn, commodity_details):
    """
    Preload commodities with fixed IDs.
    """
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """
            INSERT INTO commodities (id, name, symbol, unit, name_fa, source_name)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            commodity_details
        )
        conn.commit()
        print("Commodities preloaded successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error preloading commodities: {e}")
        raise
    finally:
        cursor.close()
