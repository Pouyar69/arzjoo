from telethon import TelegramClient, events
import re
from psycopg2 import pool
from irdatetime import get_persian_date
import subprocess


# Replace these with your own values
API_ID = '16909233'
API_HASH = '2cab0e3986ed4575888fdf0ed4338a7d'
CHANNEL_USERNAME = '@navasanchannel'

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

# Create a Telegram client
client = TelegramClient('session_name', API_ID, API_HASH)

# Function to extract specific commodities
def extract_specific_commodities(message_text):
    try:
        # Patterns for each commodity
        patterns = {
            "USD": r"💵 دلار آمریکا فروش.*?:\s*([\d,]+)",
            "EUR": r"💶 یورو فروش.*?:\s*([\d,]+)",
            "EMAMI": r"سکه امامی ✴️.*?:\s*([\d,]+)",
            "BAHAR": r"سکه بهار آزادی ✴️.*?:\s*([\d,]+)",
            "NIM_SEKKEH": r"نیم سکه✴️.*?:\s*([\d,]+)",
            "ROB_SEKKEH": r"ربع سکه✴️.*?:\s*([\d,]+)",
            "GOLD18": r"طلای 18 عیار هر گرم ✴️.*?:\s*([\d,]+)",
            "MESQAL": r"مثقال طلای آبشده✴️.*?:\s*([\d,]+)"
        }
        
        # Extract prices for each commodity
        commodities = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, message_text)
            if match:
                commodities[key] = match.group(1).replace(",", "")  # Remove commas from the price
            else:
                commodities[key] = None  # Commodity not found in the message
        return commodities
    except Exception as e:
        print(f"Error extracting commodities: {e}")
        return {}

# Event handler for new messages
@client.on(events.NewMessage(chats=CHANNEL_USERNAME))
async def new_message_listener(event):
    message = event.message.text
    if message.startswith("💵 دلار آمریکا فروش"):
        print("Relevant message received!")
        # Extract specific commodities
        commodities = extract_specific_commodities(message)
        print(commodities)
        with conn:
            source_id = 1  # Example source ID for Navasan
            source_time = get_persian_date()  # Current timestamp
            save_prices_batch(commodities, source_id, source_time)

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
    subprocess.run(["python3", "telbotNavasan.py"])


# Main program
if __name__ == "__main__":
    print("Listening for new messages...")
    with client:
        client.run_until_disconnected()