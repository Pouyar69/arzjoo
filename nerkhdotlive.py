import asyncio
import psycopg2
from telegram import Bot
from irdatetime import get_persian_date



# Database connection details
DB_HOST = "localhost"
DB_NAME = "arzjoo1"
DB_USER = "postgres"
DB_PASSWORD = "Kertob93"

# Telegram bot details
TELEGRAM_TOKEN = "7846382168:AAFMImbPZIjvqA6yME_WNm6QoX5d0mcwEcw"  # Replace with your bot's token
TELEGRAM_CHANNEL = "@nerkhdotlive"  # Replace with your channel username

def get_latest_prices():
    """
    Fetch the latest prices from the database.
    """
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Query to get the latest prices
        query = """
        SELECT 
            c.symbol AS commodity_name, 
            p.price, 
            p.read_time
        FROM 
            prices p
        JOIN 
            commodities c ON p.commodity_id = c.id
        ORDER BY 
            p.read_time DESC
        LIMIT 8;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Close the connection
        cursor.close()
        conn.close()

        return rows

    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return []

# Example mapping of commodities to emojis and custom text
commodity_decorations = {
    "USD": {"emoji": "💵", "text": "دلار"},
    "MESQAL": {"emoji": "🧈", "text": "مثقال طلا"},
    "GOLD18": {"emoji": "🌟", "text": "گرم 18 عیار"},
    "EMAMI": {"emoji": "🟡", "text": "سکه امامی"},
    "BAHAR": {"emoji": "🟡", "text": "سکه بهار آزادی"},
    "NIM_SEKKEH": {"emoji": "🟡", "text": "نیم سکه"},
    "ROB_SEKKEH": {"emoji": "🟡", "text": "ربع سکه"},
    "EUR": {"emoji": "💶", "text": "یورو"},
}

commodity_print_order = ["USD", "EUR", "GOLD18", "MESQAL", "EMAMI", "BAHAR", "NIM_SEKKEH", "ROB_SEKKEH"]

def format_price(commodity_name, price):
    return f"{int(price):,}"

# Function to get the sorting key for each row
def get_sort_key(row):
    commodity_name = row[0]
    # Use the index in the custom order list, fallback to a high number for unlisted items
    return commodity_print_order.index(commodity_name) if commodity_name in commodity_print_order else float('inf')

async def post_to_telegram(prices):
    """
    Post the latest prices to the Telegram channel.
    """
    bot = Bot(token=TELEGRAM_TOKEN)

    # Create the message
    message = f"آخرین نرخ ها {get_persian_date()} 📈\n\n"
    for row in prices:
        commodity_name = row[0]
        price = row[1]
        recorded_at = row[2]
        # Get emoji and text for the commodity, or default values if not found
        decoration = commodity_decorations.get(commodity_name, {"emoji": "❓", "text": "No description available"})
        emoji = decoration["emoji"]
        custom_text = decoration["text"]
        formatted_price = format_price(commodity_name, price)
        message += f"\u202B {emoji} {custom_text} : {formatted_price} \n\n" 
    print(message)
    # Send the message to the channel
    await bot.send_message(chat_id=TELEGRAM_CHANNEL, text=message)


sorted_prices = sorted(get_latest_prices(), key=get_sort_key)
#Post to Telegram
if sorted_prices:
    asyncio.run(post_to_telegram(sorted_prices))
else:
    print("No prices to post.")
