import os

import requests
from dotenv import load_dotenv


load_dotenv()


def send_telegram_message(text: str) -> bool:
    """Send a Telegram Bot API message and return whether it succeeded."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("Telegram send failed: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing.")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    try:
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
            },
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Telegram send failed: {exc}")
        return False

    data = response.json()
    if not data.get("ok"):
        print(f"Telegram send failed: {data}")
        return False

    return True
