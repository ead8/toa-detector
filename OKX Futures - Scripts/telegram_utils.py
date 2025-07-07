import requests
import yaml
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def load_settings():
    try:
        with open("config/settings.yaml", "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
        return None

def send_telegram(message, markdown=False):
    settings = load_settings()
    if not settings:
        logger.error("Telegram settings not loaded")
        return
    
    tg1 = settings["tg"]
    tg2 = settings.get("second_tg", None)
    
    payload = {
        "chat_id": tg1["chat_id"],
        "text": message,
        "parse_mode": "Markdown" if markdown else None,
    }

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{tg1['token']}/sendMessage",
            json={k: v for k, v in payload.items() if v is not None},
            timeout=5,
        )
        logger.info(f"Sent to {tg1['chat_id']} - {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send TG1 message: {e}")

    if tg2:
        payload["chat_id"] = tg2["chat_id"]
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{tg2['token']}/sendMessage",
                json={k: v for k, v in payload.items() if v is not None},
                timeout=5,
            )
            logger.info(f"Sent to {tg2['chat_id']} - {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to send TG2 message: {e}")

def alert_new_listing(symbol, list_time, exchange="OKX", message=""):
    """
    Send formatted Telegram alert with verified genesis candle time
    
    Args:
        symbol: Trading symbol
        list_time: datetime object of listing time
        exchange: Exchange name
        message: Additional message content
    """
    formatted_time = list_time.strftime('%Y-%m-%d %H:%M:%S UTC')
    
    msg = (
        f"🔥 *New Listing Detected* 🔥\n"
        f"• *Exchange*: {exchange.upper()}\n"
        f"• *Symbol*: {symbol}\n"
        f"• *Listing Time*: `{formatted_time}`\n"
        f"• *Verified Genesis Candle*: Yes\n"
        f"\n{message}"
    )
    send_telegram(msg, markdown=True)