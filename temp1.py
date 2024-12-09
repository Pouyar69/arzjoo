from telethon import TelegramClient

# Replace these with your own values
API_ID = '16909233'
API_HASH = '2cab0e3986ed4575888fdf0ed4338a7d'
CHANNEL_USERNAME = '@navasanchannel'

# Create a Telegram client
client = TelegramClient('session_name', API_ID, API_HASH)

async def fetch_messages():
    # Connect to the client
    await client.start()
    print("Client connected!")

    # Get the channel entity
    try:
        channel = await client.get_entity(CHANNEL_USERNAME)
        print(f"Fetched channel: {channel.title}")
    except Exception as e:
        print(f"Failed to fetch channel: {e}")
        return

    # Fetch messages
    try:
        async for message in client.iter_messages(channel, limit=1):  # Adjust limit as needed
            print(f"Message ID: {message.id}, Text: {message.text}")
    except Exception as e:
        print(f"Error fetching messages: {e}")

# Main program
if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(fetch_messages())
