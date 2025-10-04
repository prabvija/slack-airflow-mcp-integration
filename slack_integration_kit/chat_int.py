# app.py
import os
import asyncio
import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from airflow_mcp_client.client import handle_airflow_alert

# Load environment variables first
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize your app with your bot token and signing secret
app = App(token=os.environ["SLACK_BOT_TOKEN"])


# This listener handles all messages in channels the bot is in
@app.message()
def handle_message(message, say):
    """
    Listen for messages and handle Airflow-related queries through PydanticAI MCP
    """
    logging.info(f"Messages Response {message}")
    # text = message.get("text", "").lower()
    thread_ts = message.get("ts")

    response = asyncio.run(handle_airflow_alert(message))

    say(text=response, thread_ts=thread_ts)


# Start your app
if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
