import ccxt
import time
import asyncio
from telegram import Bot
import numpy as np
<<<<<<< HEAD
from scipy.signal import argrelextrema
=======
>>>>>>> d4f55e762b851b30f2c9ad5083d19e910b9184e6
from dotenv import load_dotenv
import os

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}})

telegram_bot = Bot(token=BOT_TOKEN)

signal_cache = {}  # Duplicate önleme

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
<<<<<<< HEAD
        return np.zeros(len(closes))
=======
        return np.zeros(len(closes))  # Yetersiz veri
>>>>>>> d4f55e762b851b30f2c9ad5083d19e910b9184e6
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

<<<<<<< HEAD
def calculate_rsi_ema(rsi, ema_length=14):
    ema = np.zeros_like(rsi)
    if len(rsi) < ema_length:
        return ema
    ema[ema_length-1] = np.mean(rsi[:ema_length])
    for i in range(ema_length, len(rsi)):
        ema[i] = (rsi[i] * (2 / (ema_length + 1))) + (ema[i-1] * (1 - (2 / (ema_length + 1))))
    return ema

async def check_divergence(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=50)  # Son 50 mum (35+ buffer)
        closes = np.array([x[4] for x in ohlcv])
        rsi = calculate_rsi(closes, 14)
        rsi_ema = calculate_rsi_ema(rsi, 14)
        rsi_ema2 = np.roll(rsi_ema, 2)  # EMA[2]

        # EMA crossover and gray zone
        ema_color = 'lime' if rsi_ema[-1] > rsi_ema2[-1] else 'red'
        in_gray_zone = 48 <= rsi_ema[-1] <= 52

        # Divergence for EMA (son 8-35 mum aralığı, argrelextrema ile)
        min_lookback = 8
        max_lookback = 35
        lookback = min(max_lookback, len(closes))
        if lookback < min_lookback:
            return

        price_slice = closes[-lookback:]
        ema_slice = rsi_ema[-lookback:]

        # Local highs/lows (order=2 for smooth detection)
        price_highs = argrelextrema(price_slice, np.greater, order=2)[0]
        ema_highs = argrelextrema(ema_slice, np.greater, order=2)[0]
        price_lows = argrelextrema(price_slice, np.less, order=2)[0]
        ema_lows = argrelextrema(ema_slice, np.less, order=2)[0]

        bullish = False  # Pozitif: Price LL, EMA HL
        bearish = False  # Negatif: Price HH, EMA LH

        if len(price_lows) >= 2 and len(ema_lows) >= 2:
            last_low = price_lows[-1]
            prev_low = price_lows[-2]
            last_ema_low = ema_lows[-1]
            prev_ema_low = ema_lows[-2]
            if price_slice[last_low] < price_slice[prev_low] and ema_slice[last_ema_low] > ema_slice[prev_ema_low]:
                bullish = True

        if len(price_highs) >= 2 and len(ema_highs) >= 2:
            last_high = price_highs[-1]
            prev_high = price_highs[-2]
            last_ema_high = ema_highs[-1]
            prev_ema_high = ema_highs[-2]
            if price_slice[last_high] > price_slice[prev_high] and ema_slice[last_ema_high] < ema_slice[prev_ema_high]:
                bearish = True

        print(f"{symbol} {timeframe}: Pozitif: {bullish}, Negatif: {bearish}, RSI_EMA: {rsi_ema[-1]:.2f}, Color: {ema_color}, Gray Zone: {in_gray_zone}")
=======
async def check_divergence(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=30)  # Son 30 mum
        closes = np.array([x[4] for x in ohlcv])
        rsi = calculate_rsi(closes, 14)

        last_rsi = rsi[-1]
        prev_rsi = rsi[-2]

        bullish = False  # Pozitif uyumsuzluk
        bearish = False  # Negatif uyumsuzluk
        if last_rsi > 70 and prev_rsi < 70:
            bearish = True
        elif last_rsi < 30 and prev_rsi > 30:
            bullish = True

        print(f"{symbol} {timeframe}: Pozitif: {bullish}, Negatif: {bearish}, RSI: {last_rsi:.2f}")
>>>>>>> d4f55e762b851b30f2c9ad5083d19e910b9184e6

        key = f"{symbol}_{timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (bullish, bearish) != last_signal:
<<<<<<< HEAD
            if bullish:
                message = f"*{symbol} {timeframe}*: \nPozitif Uyumsuzluk: {bullish} 🚀 (Price LL, EMA HL)\nRSI_EMA: {rsi_ema[-1]:.2f} ({ema_color.upper()})\nGray Zone: {in_gray_zone}"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
            if bearish:
                message = f"*{symbol} {timeframe}*: \nNegatif Uyumsuzluk: {bearish} 📉 (Price HH, EMA LH)\nRSI_EMA: {rsi_ema[-1]:.2f} ({ema_color.upper()})\nGray Zone: {in_gray_zone}"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
=======
            message = f"*{symbol} {timeframe}*: \nPozitif Uyumsuzluk: {bullish} 🚀\nNegatif Uyumsuzluk: {bearish} 📉\nRSI: {last_rsi:.2f}"
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
>>>>>>> d4f55e762b851b30f2c9ad5083d19e910b9184e6
            signal_cache[key] = (bullish, bearish)

    except Exception as e:
        print(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + time.strftime('%H:%M:%S'))
    timeframes = ['30m', '1h', '2h', '4h']
    symbols = [
        'ETHUSDT', 'BTCUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'FARTCOINUSDT', '1000PEPEUSDT', 'ADAUSDT', 'SUIUSDT', 'WIFUSDT', 'ENAUSDT', 'PENGUUSDT', '1000BONKUSDT', 'HYPEUSDT', 'AVAXUSDT', 'MOODENGUSDT', 'LINKUSDT', 'PUMPFUNUSDT', 'LTCUSDT', 'TRUMPUSDT', 'AAVEUSDT', 'ARBUSDT', 'NEARUSDT', 'ONDOUSDT', 'POPCATUSDT', 'TONUSDT', 'OPUSDT', '1000FLOKIUSDT', 'SEIUSDT', 'HBARUSDT', 'WLDUSDT', 'BNBUSDT', 'UNIUSDT', 'XLMUSDT', 'CRVUSDT', 'VIRTUALUSDT', 'AI16ZUSDT', 'TIAUSDT', 'TAOUSDT', 'APTUSDT', 'DOTUSDT', 'SPXUSDT', 'ETCUSDT', 'LDOUSDT', 'BCHUSDT', 'INJUSDT', 'KASUSDT', 'ALGOUSDT', 'TRXUSDT', 'IPUSDT'
<<<<<<< HEAD
    ]  # Listeni kullandım
=======
    ]
>>>>>>> d4f55e762b851b30f2c9ad5083d19e910b9184e6

    while True:
        for timeframe in timeframes:
            for symbol in symbols:
                await check_divergence(symbol, timeframe)
                await asyncio.sleep(1)  # Rate limit
        print("Tüm taramalar tamamlandı, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
<<<<<<< HEAD
    asyncio.run(main())
=======
    asyncio.run(main())
>>>>>>> d4f55e762b851b30f2c9ad5083d19e910b9184e6
