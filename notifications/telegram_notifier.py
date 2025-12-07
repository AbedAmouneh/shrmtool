# notifications/telegram_notifier.py

import os
import logging
import requests

logger = logging.getLogger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


def send_telegram_message(text: str) -> None:
    """Send a Telegram message if TELEGRAM_* env vars are configured.

    Env vars:
      TELEGRAM_BOT_TOKEN  - Bot token from BotFather
      TELEGRAM_CHAT_ID    - Your chat ID (int or string)
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logger.info(
            "Telegram notification skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"
        )
        return

    try:
        resp = requests.post(
            TELEGRAM_API_URL.format(token=token),
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(
                "Telegram notification failed: %s %s",
                resp.status_code,
                resp.text[:500],
            )
    except Exception as e:
        logger.error("Error sending Telegram notification: %s", e)


if __name__ == "__main__":
    # quick manual test:
    import sys

    message = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "Test message from SHRM collector bot ✅"
    )
    send_telegram_message(message)

