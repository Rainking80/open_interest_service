import os
import logging
import requests
from datetime import datetime, timezone
from sqlalchemy import select
from database import AsyncSessionLocal, OIFunding

logger = logging.getLogger("healthcheck")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning(f"Telegram credentials not configured. Would have sent: {message}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error(f"Failed to send telegram message. Status: {response.status_code}, Response: {response.text}")
        else:
            logger.info("Successfully sent healthcheck report to Telegram.")
    except Exception as e:
        logger.error(f"Error sending telegram message: {e}")

async def run_healthcheck(candle_time: datetime, expected_pairs: list):
    logger.info(f"Running healthcheck for candle: {candle_time}")
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(OIFunding).where(OIFunding.timestamp == candle_time)
        )
        records = result.scalars().all()

    record_map = {r.pair: r for r in records}
    
    missing_pairs = []
    zero_volumes = []
    zero_oi = []
    
    for pair in expected_pairs:
        if pair not in record_map:
            missing_pairs.append(pair)
            continue
            
        record = record_map[pair]
        
        # Check for zero volumes (possible websocket or cache issue)
        if record.taker_buy_volume == 0.0 and record.taker_sell_volume == 0.0:
            zero_volumes.append(pair)
            
        # Check for zero OI
        if record.open_interest == 0.0:
            zero_oi.append(pair)

    # Determine status
    has_errors = bool(missing_pairs or zero_volumes or zero_oi)
    
    # Construct message
    status_icon = "❌" if has_errors else "✅"
    message = f"<b>Healthcheck Report {status_icon}</b>\n"
    message += f"<b>Time:</b> {candle_time.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
    message += f"<b>Expected Pairs:</b> {len(expected_pairs)}\n"
    message += f"<b>Stored Pairs:</b> {len(records)}\n\n"
    
    if has_errors:
        if missing_pairs:
            # truncate if too long
            missing_str = ", ".join(missing_pairs)
            if len(missing_str) > 100: missing_str = missing_str[:100] + "..."
            message += f"⚠️ <b>Missing Data ({len(missing_pairs)}):</b> {missing_str}\n"
            
        if zero_volumes:
            zero_vol_str = ", ".join(zero_volumes)
            if len(zero_vol_str) > 100: zero_vol_str = zero_vol_str[:100] + "..."
            message += f"⚠️ <b>Zero Taker Volume (buys & sells is 0.0) ({len(zero_volumes)}):</b> {zero_vol_str}\n"
            
        if zero_oi:
            zero_oi_str = ", ".join(zero_oi)
            if len(zero_oi_str) > 100: zero_oi_str = zero_oi_str[:100] + "..."
            message += f"⚠️ <b>Zero Open Interest ({len(zero_oi)}):</b> {zero_oi_str}\n"
            
        message += "\n❌ <i>Errors detected! Please check the service.</i>"
    else:
        message += "✅ <i>All data seems valid. No zero volumes detected.</i>"

    # Send telegram
    send_telegram_message(message)
    
    if has_errors:
        logger.error(f"Healthcheck failed for {candle_time}.")
    else:
        logger.info(f"Healthcheck passed for {candle_time}.")
