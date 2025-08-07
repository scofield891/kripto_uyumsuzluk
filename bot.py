import ccxt
import time
import asyncio
from telegram import Bot
import numpy as np
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime
import pytz  # Saat için timezone kütüphanesi ekledim

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
RSI_LOW = float(os.getenv('RSI_LOW', 40))
RSI_HIGH = float(os.getenv('RSI_HIGH', 60))
TEST_MODE = os.getenv('TEST_MODE', 'False').lower() == 'true'

# Logging setup: Hem dosya hem console
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler('bot.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}, 'verbose': False})

telegram_bot = Bot(token=BOT_TOKEN)

signal_cache = {}

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return np.zeros(len(closes))
    deltas = np.diff(closes)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = np.zeros_like(closes)
    rsi[:period] = 100. - 100. / (1. + rs)
    for i in range(period, len(closes)):
        delta = deltas[i-1]
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)
    return rsi

def calculate_rsi_ema(rsi, ema_length=14):
    ema = np.zeros_like(rsi)
    if len(rsi) < ema_length:
        return ema
    ema[ema_length-1] = np.mean(rsi[:ema_length])
    for i in range(ema_length, len(rsi)):
        ema[i] = (rsi[i] * (2 / (ema_length + 1))) + (ema[i-1] * (1 - (2 / (ema_length + 1))))
    return ema

def find_local_extrema(arr, order=4):
    highs = []
    lows = []
    for i in range(order, len(arr) - order):
        if arr[i] == max(arr[i-order:i+order+1]):
            highs.append(i)
        if arr[i] == min(arr[i-order:i+order+1]):
            lows.append(i)
    return np.array(highs), np.array(lows)

async def check_divergence(symbol, timeframe):
    try:
        if TEST_MODE:
            closes = np.random.rand(100) * 100
            logging.info(f"Test modu: {symbol} {timeframe} için dummy data kullanıldı")
        else:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
            closes = np.array([x[4] for x in ohlcv])

        rsi = calculate_rsi(closes, 14)
        rsi_ema = calculate_rsi_ema(rsi, 14)
        rsi_ema2 = np.roll(rsi_ema, 1)

        ema_color = 'lime' if rsi_ema[-1] > rsi_ema2[-1] else 'red'

        lookback = 50
        if len(closes) < lookback:
            return

        price_slice = closes[-lookback:]
        ema_slice = rsi_ema[-lookback:]

        price_highs, price_lows = find_local_extrema(price_slice)

        bullish = False
        bearish = False

        min_distance = 5
        ema_threshold = 0.5

        if len(price_lows) >= 2:
            last_low = price_lows[-1]
            prev_low = price_lows[-2]
            if (last_low - prev_low) >= min_distance:
                if price_slice[last_low] < price_slice[prev_low] and ema_slice[last_low] > (ema_slice[prev_low] + ema_threshold):
                    bullish = True

        if len(price_highs) >= 2:
            last_high = price_highs[-1]
            prev_high = price_highs[-2]
            if (last_high - prev_high) >= min_distance:
                if price_slice[last_high] > price_slice[prev_high] and ema_slice[last_high] < (ema_slice[prev_high] - ema_threshold):
                    bearish = True

        logging.info(f"{symbol} {timeframe}: Pozitif: {bullish}, Negatif: {bearish}, RSI_EMA: {rsi_ema[-1]:.2f}, Color: {ema_color}")

        key = f"{symbol} {timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (bullish or bearish) and (bullish, bearish) != last_signal:
            if (bullish and rsi_ema[-1] < RSI_LOW and ema_color == 'red') or (bearish and rsi_ema[-1] > RSI_HIGH and ema_color == 'lime'):
                rsi_str = f"{rsi_ema[-1]:.2f}"
                current_price = f"{closes[-1]:.2f}"
                tz = pytz.timezone('Europe/Istanbul')  # Türkiye timezone (UTC+3)
                timestamp = datetime.now(tz).strftime('%H:%M:%S')
                if bullish:
                    message = f"{symbol} {timeframe}\nPozitif Uyumsuzluk: {bullish} 🚀 (Price LL, EMA HL)\nRSI_EMA: {rsi_str} ({ema_color.upper()})\nCurrent Price: {current_price} USDT\nSaat: {timestamp}"
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logging.info(f"Sinyal gönderildi: {message}")
                if bearish:
                    message = f"{symbol} {timeframe}\nNegatif Uyumsuzluk: {bearish} 📉 (Price HH, EMA LH)\nRSI_EMA: {rsi_str} ({ema_color.upper()})\nCurrent Price: {current_price} USDT\nSaat: {timestamp}"
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logging.info(f"Sinyal gönderildi: {message}")
                signal_cache[key] = (bullish, bearish)

    except Exception as e:
        logging.error(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S'))
    timeframes = ['30m', '1h', '2h', '4h']
    symbols = [
        'ETHUSDT.P', 'BTCUSDT.P', 'SOLUSDT.P', 'XRPUSDT.P', 'DOGEUSDT.P', 'FARTCOINUSDT.P', '1000PEPEUSDT.P', 'ADAUSDT.P', 'SUIUSDT.P', 'WIFUSDT.P', 'ENAUSDT.P', 'PENGUUSDT.P', '1000BONKUSDT.P', 'HYPEUSDT.P', 'AVAXUSDT.P', 'MOODENGUSDT.P', 'LINKUSDT.P', 'PUMPFUNUSDT.P', 'LTCUSDT.P', 'TRUMPUSDT.P', 'AAVEUSDT.P', 'ARBUSDT.P', 'NEARUSDT.P', 'ONDOUSDT.P', 'POPCATUSDT.P', 'TONUSDT.P', 'OPUSDT.P', '1000FLOKIUSDT.P', 'SEIUSDT.P', 'HBARUSDT.P', 'WLDUSDT.P', 'BNBUSDT.P', 'UNIUSDT.P', 'XLMUSDT.P', 'CRVUSDT.P', 'VIRTUALUSDT.P', 'AI16ZUSDT.P', 'TIAUSDT.P', 'TAOUSDT.P', 'APTUSDT.P', 'DOTUSDT.P', 'SPXUSDT.P', 'ETCUSDT.P', 'LDOUSDT.P', 'BCHUSDT.P', 'INJUSDT.P', 'KASUSDT.P', 'ALGOUSDT.P', 'TRXUSDT.P', 'IPUSDT.P',
        'FILUSDT.P', 'STXUSDT.P', 'ATOMUSDT.P', 'RUNEUSDT.P', 'THETAUSDT.P', 'FETUSDT.P', 'AXSUSDT.P', 'SANDUSDT.P', 'MANAUSDT.P', 'CHZUSDT.P', 'APEUSDT.P', 'GALAUSDT.P', 'IMXUSDT.P', 'DYDXUSDT.P', 'GMTUSDT.P', 'EGLDUSDT.P', 'ZKUSDT.P', 'NOTUSDT.P',
        'ENSUSDT.P', 'JUPUSDT.P', 'ATHUSDT.P', 'ICPUSDT.P', 'STRKUSDT.P', 'ORDIUSDT.P', 'PENDLEUSDT.P', 'PNUTUSDT.P', 'RENDERUSDT.P', 'OMUSDT.P', 'ZORAUSDT.P', 'SUSDT.P', 'GRASSUSDT.P', 'TRBUSDT.P', 'MOVEUSDT.P', 'XAUTUSDT.P', 'POLUSDT.P', 'CVXUSDT.P', 'BRETTUSDT.P', 'SAROSUSDT.P', 'GOATUSDT.P', 'AEROUSDT.P', 'JTOUSDT.P', 'HYPERUSDT.P', 'ETHFIUSDT.P', 'BERAUSDT.P'
    ]

    while True:
        tasks = []
        for timeframe in timeframes:
            for symbol in symbols:
                tasks.append(check_divergence(symbol, timeframe))
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            await asyncio.sleep(1)
        logging.info("Tüm taramalar tamamlandı, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())