import ccxt
import numpy as np
import pandas as pd
import telegram
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
import sys
import os

# ================== Sabit Değerler ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "7677279035:AAHMecBYUliT7QlUl9OtB0kgXl8uyyuxbsQ")
CHAT_ID = os.getenv("CHAT_ID", "-1002878297025")
TEST_MODE = False  # Test için True, gerçek veri için False
TRAILING_ACTIVATION = 0.8
TRAILING_DISTANCE_BASE = 1.5
TRAILING_DISTANCE_HIGH_VOL = 2.5
VOLATILITY_THRESHOLD = 0.02
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0
TP_MULTIPLIER2 = 3.5
SL_BUFFER = 0.3
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05
LOOKBACK_CROSSOVER = 10
LOOKBACK_SMI = 20
MACD_MODE = "regime"  # Sabit net tanımlandı

# ================== Logging ==================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = logging.FileHandler('bot.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ================== Borsa & Bot ==================
exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}, 'timeout': 60000})
telegram_bot = telegram.Bot(
    token=BOT_TOKEN,
    request=telegram.request.HTTPXRequest(
        connection_pool_size=20,
        pool_timeout=30.0
    )
)
signal_cache = {}
message_queue = asyncio.Queue()

# ================== Mesaj Gönderici ==================
async def message_sender():
    while True:
        message = await message_queue.get()
        try:
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            await asyncio.sleep(1)
        except (telegram.error.RetryAfter, telegram.error.TimedOut) as e:
            wait_time = getattr(e, 'retry_after', 5) + 2
            logger.warning(f"Error: {type(e).__name__}, {wait_time-2} saniye bekle")
            await asyncio.sleep(wait_time)
            await message_queue.put(message)
        message_queue.task_done()

# ================== İndikatör Fonksiyonları ==================
def calculate_ema(closes, span):
    k = 2 / (span + 1)
    ema = np.zeros(len(closes), dtype=np.float64)
    ema[0] = closes[0] if closes[0] != 0 else np.mean(closes[1:span+1])
    for i in range(1, len(closes)):
        ema[i] = closes[i] * k + ema[i-1] * (1 - k)
    return ema

def calculate_sma(closes, period):
    sma = np.zeros(len(closes), dtype=np.float64)
    for i in range(len(closes)):
        if i < period - 1:
            sma[i] = 0.0
        else:
            sma[i] = np.mean(closes[i-period+1:i+1])
    return sma

def calculate_macd(closes):
    fast, slow, signal = 12, 26, 9
    def ema(x, n):
        k = 2 / (n + 1)
        e = np.zeros(len(x), dtype=np.float64)
        e[0] = x[0] if x[0] != 0 else np.mean(x[1:n+1])
        for i in range(1, len(x)):
            e[i] = x[i] * k + e[i-1] * (1 - k)
        return e
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def ensure_atr(df, period=20):
    if 'atr' not in df.columns:
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=period).mean().fillna(0)
    return df

def calculate_smi(df):
    # Bollinger Bands
    bb_middle = calculate_sma(df['close'].values, 20)
    bb_std = np.std(df['close'].values[-20:], ddof=1)
    bb_upper = bb_middle + 2 * bb_std
    bb_lower = bb_middle - 2 * bb_std

    # Keltner Channels
    df = ensure_atr(df, 20)
    kc_middle = calculate_ema(df['close'].values, 20)
    kc_upper = kc_middle + 1.5 * df['atr'].values
    kc_lower = kc_middle - 1.5 * df['atr'].values

    # Squeeze Durumu (son mum için)
    squeeze_on = (bb_upper[-1] < kc_upper[-1]) and (bb_lower[-1] > kc_lower[-1])
    squeeze_off = not squeeze_on

    # Momentum Histogramı
    hl2 = (df['high'].values + df['low'].values) / 2
    sma_hl2 = calculate_sma(hl2, 20)
    diff = hl2 - sma_hl2
    if len(diff) >= 20:
        linreg = np.polyfit(range(20), diff[-20:], 1)[0]  # Slope
        momentum = linreg * 10  # Ölçeklendirme
    else:
        momentum = 0.0

    # Renk (teyit için)
    color = 'green' if momentum > 0 else 'red' if momentum < 0 else 'gray'

    return squeeze_off, momentum, color

def get_atr_values(df, lookback_atr=18):
    df = ensure_atr(df, period=20)
    if len(df) < lookback_atr + 2:
        return np.nan, np.nan
    atr_value = float(df['atr'].iloc[-2])
    close_last = float(df['close'].iloc[-2])
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1]
    avg_atr_ratio = float(atr_series.mean() / close_last) if len(atr_series) else np.nan
    return atr_value, avg_atr_ratio

def calculate_indicators(df):
    df = df.copy()
    if len(df) < 80:
        logger.warning("DF çok kısa, indikatör hesaplanamadı.")
        return None
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, 13)
    df['sma34'] = calculate_sma(closes, 34)
    df['macd'], df['macd_signal'], df['macd_hist'] = calculate_macd(closes)
    df['volume_sma20'] = df['volume'].rolling(window=20).mean().fillna(0)
    df['atr'] = ensure_atr(df)['atr']
    smi_squeeze_off, smi_histogram, smi_color = calculate_smi(df)
    return df, smi_squeeze_off, smi_histogram, smi_color

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol):
    try:
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(250))) * 0.05 + 0.3
            highs = closes + np.random.rand(250) * 0.02 * closes
            lows = closes - np.random.rand(250) * 0.02 * closes
            volumes = np.random.rand(250) * 10000
            ohlcv = [[i*3600, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(250)]
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).iloc[50:]
            logger.info(f"Test modu: {symbol}")
        else:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    ohlcv = exchange.fetch_ohlcv(symbol, '4h', limit=max(150, LOOKBACK_ATR + 80))
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    break
                except (ccxt.RequestTimeout, ccxt.NetworkError):
                    logger.warning(f"Timeout/Network ({symbol}), retry {attempt+1}/{max_retries}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
                except (ccxt.BadSymbol, ccxt.BadRequest) as e:
                    logger.warning(f"Skip {symbol}: {e.__class__.__name__} - {e}")
                    return
            if df is None or df.empty:
                return
        # İndikatörler
        df, smi_squeeze_off, smi_histogram, smi_color = calculate_indicators(df)
        if df is None:
            return
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol}), skip.")
            return
        closed_candle = df.iloc[-2]
        current_price = float(df['close'].iloc[-1])
        logger.info(f"{symbol} Closed Candle Close: {closed_candle['close']:.4f}, Current Price: {current_price:.4f}")
        key = f"{symbol}_4h"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None,
            'trailing_activated': False, 'avg_atr_ratio': None, 'trailing_distance': None,
            'remaining_ratio': 1.0,
            'last_signal_time': None, 'last_signal_type': None,
            'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False
        })
        # EMA/SMA crossover check
        ema13_slice = df['ema13'].values[-LOOKBACK_CROSSOVER-1:]
        sma34_slice = df['sma34'].values[-LOOKBACK_CROSSOVER-1:]
        price_slice = df['close'].values[-LOOKBACK_CROSSOVER-1:]
        if len(ema13_slice) <= LOOKBACK_CROSSOVER:
            logger.warning(f"Veri yetersiz ({symbol}), skip.")
            return
        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        for i in range(1, LOOKBACK_CROSSOVER + 1):
            if 0 <= i < len(ema13_slice):  # Güvenli indeks kontrolü
                if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and price_slice[-i] > sma34_slice[-i]:
                    ema_sma_crossover_buy = True
                if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and price_slice[-i] < sma34_slice[-i]:
                    ema_sma_crossover_sell = True
        logger.info(f"{symbol} EMA/SMA crossover_buy: {ema_sma_crossover_buy}, crossover_sell: {ema_sma_crossover_sell}")
        # Pullback kontrolü
        recent = df['close'].values[-LOOKBACK_CROSSOVER-1:-1]
        ema13_recent = df['ema13'].values[-LOOKBACK_CROSSOVER-1:-1]
        pullback_long = (recent < ema13_recent).any() and (closed_candle['close'] > closed_candle['ema13']) and (closed_candle['close'] > closed_candle['sma34'])
        pullback_short = (recent > ema13_recent).any() and (closed_candle['close'] < closed_candle['ema13']) and (closed_candle['close'] < closed_candle['sma34'])
        logger.info(f"{symbol} pullback_long: {pullback_long}, pullback_short: {pullback_short}")
        # Volume filtresi
        volume_ok = closed_candle['volume'] > 1.0 * closed_candle['volume_sma20']
        logger.info(f"{symbol} volume_ok: {volume_ok}")
        # MACD filtre
        macd_up = df['macd'].iloc[-2] > df['macd_signal'].iloc[-2]
        macd_down = df['macd'].iloc[-2] < df['macd_signal'].iloc[-2]
        if MACD_MODE == "regime":
            macd_ok_long = macd_up
            macd_ok_short = macd_down
        logger.info(f"{symbol} macd_ok_long: {macd_ok_long}, macd_ok_short: {macd_ok_short}")
        # SMI kontrolü
        smi_condition_long = smi_squeeze_off and (smi_histogram > 0 or smi_color == 'green')
        smi_condition_short = smi_squeeze_off and (smi_histogram < 0 or smi_color == 'red')
        logger.info(f"{symbol} SMI_squeeze_off: {smi_squeeze_off}, SMI_histogram: {smi_histogram}, SMI_color: {smi_color}")
        logger.info(f"{symbol} SMI_long_condition: {smi_condition_long}, SMI_short_condition: {smi_condition_short}")
        # Sinyal şartları
        buy_condition = ema_sma_crossover_buy and pullback_long and volume_ok and macd_ok_long and smi_condition_long
        sell_condition = ema_sma_crossover_sell and pullback_short and volume_ok and macd_ok_short and smi_condition_short
        logger.info(f"{symbol} buy_condition: {buy_condition}, sell_condition: {sell_condition}")
        # Pozisyon yönetimi
        current_pos = signal_cache.get(key, current_pos)
        current_price = float(df['close'].iloc[-1])
        tz = pytz.timezone('Europe/Istanbul')
        now = datetime.now(tz)
        if buy_condition or sell_condition:
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] is not None and current_pos['signal'] != new_signal:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if current_pos['signal'] == 'buy' else ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                message_type = "REVERSAL CLOSE 🚀" if profit_percent > 0 else "REVERSAL STOP 📉"
                profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"
                message = (
                    f"{symbol}: {message_type}\n"
                    f"Price: {current_price:.4f}\n"
                    f"{profit_text}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı (reversal)\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"Reversal Close kuyruğa eklendi: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'trailing_activated': False, 'avg_atr_ratio': None, 'trailing_distance': None,
                    'remaining_ratio': 1.0, 'last_signal_time': None, 'last_signal_type': None,
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                current_pos = signal_cache[key]
            if buy_condition and current_pos['signal'] != 'buy':
                if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'buy' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                    message = f"{symbol}: BUY sinyali atlanıyor (cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                    await message_queue.put(message)
                    logger.info(message)
                else:
                    entry_price = float(closed_candle['close'])
                    sl_price = entry_price - (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
                    if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                        message = f"{symbol}: BUY sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nSL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                        await message_queue.put(message)
                        logger.info(message)
                    else:
                        tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                        tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                        trailing_distance = TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE
                        current_pos = {
                            'signal': 'buy', 'entry_price': entry_price, 'sl_price': sl_price,
                            'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': entry_price,
                            'lowest_price': None, 'trailing_activated': False, 'avg_atr_ratio': avg_atr_ratio,
                            'trailing_distance': trailing_distance, 'remaining_ratio': 1.0,
                            'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': now,
                            'tp1_hit': False, 'tp2_hit': False
                        }
                        signal_cache[key] = current_pos
                        message = (
                            f"{symbol}: BUY (LONG) 🚀\n"
                            f"SMI Confirm: {smi_condition_long}\n"
                            f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                            f"Time: {now.strftime('%H:%M:%S')}"
                        )
                        await message_queue.put(message)
                        logger.info(f"Buy sinyali kuyruğa eklendi: {message}")
            elif sell_condition and current_pos['signal'] != 'sell':
                if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'sell' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                    message = f"{symbol}: SELL sinyali atlanıyor (cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                    await message_queue.put(message)
                    logger.info(message)
                else:
                    entry_price = float(closed_candle['close'])
                    sl_price = entry_price + (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
                    if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                        message = f"{symbol}: SELL sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nSL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                        await message_queue.put(message)
                        logger.info(message)
                    else:
                        tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                        tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                        trailing_distance = TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE
                        current_pos = {
                            'signal': 'sell', 'entry_price': entry_price, 'sl_price': sl_price,
                            'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': None,
                            'lowest_price': entry_price, 'trailing_activated': False, 'avg_atr_ratio': avg_atr_ratio,
                            'trailing_distance': trailing_distance, 'remaining_ratio': 1.0,
                            'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': now,
                            'tp1_hit': False, 'tp2_hit': False
                        }
                        signal_cache[key] = current_pos
                        message = (
                            f"{symbol}: SELL (SHORT) 📉\n"
                            f"SMI Confirm: {smi_condition_short}\n"
                            f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                            f"Time: {now.strftime('%H:%M:%S')}"
                        )
                        await message_queue.put(message)
                        logger.info(f"Sell sinyali kuyruğa eklendi: {message}")
        # Pozisyon yönetimi
        if current_pos['signal'] == 'buy':
            atr_value, _ = get_atr_values(df, LOOKBACK_ATR)
            if not np.isfinite(atr_value):
                return
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            td = current_pos['trailing_distance']
            if current_price >= current_pos['entry_price'] + (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']:
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
                message = (
                    f"{symbol}: TRAILING ACTIVE 🚧\n"
                    f"Current: {current_price:.4f}\nEntry: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"Trailing Activated kuyruğa eklendi: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                message = (
                    f"{symbol}: TP1 Hit 🚀\nCurrent: {current_price:.4f}\nTP1: {current_pos['tp1_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%30 satıldı, SL: {current_pos['sl_price']:.4f}\n"
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP1 Hit kuyruğa eklendi: {message}")
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol}: TP2 Hit 🚀\nCurrent: {current_price:.4f}\nTP2: {current_pos['tp2_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%40 satıldı, kalan %30 trailing\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP2 Hit kuyruğa eklendi: {message}")
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                message = f"{symbol}: {'LONG 🚀' if profit_percent > 0 else 'STOP LONG 📉'}\nPrice: {current_price:.4f}\n{'Profit: ' if profit_percent > 0 else 'Loss: '}{profit_percent:.2f}%\n{'PARAYI VURDUK 🚀' if profit_percent > 0 else 'STOP 😞'}\nKalan %{current_pos['remaining_ratio']*100:.0f} satıldı\nTime: {now.strftime('%H:%M:%S')}"
                await message_queue.put(message)
                logger.info(f"Exit kuyruğa eklendi: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'], 'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                return
            signal_cache[key] = current_pos
        elif current_pos['signal'] == 'sell':
            atr_value, _ = get_atr_values(df, LOOKBACK_ATR)
            if not np.isfinite(atr_value):
                return
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            td = current_pos['trailing_distance']
            if current_price <= current_pos['entry_price'] - (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']:
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                message = (
                    f"{symbol}: TRAILING ACTIVE 🚧\nCurrent: {current_price:.4f}\nEntry: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\nProfit: {profit_percent:.2f}%\nVol Ratio: {current_pos['avg_atr_ratio']:.4f}\n"
                    f"TSL Distance: {td}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"Trailing Activated kuyruğa eklendi: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                message = (
                    f"{symbol}: TP1 Hit 🚀\nCurrent: {current_price:.4f}\nTP1: {current_pos['tp1_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%30 satıldı, SL: {current_pos['sl_price']:.4f}\n"
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP1 Hit kuyruğa eklendi: {message}")
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol}: TP2 Hit 🚀\nCurrent: {current_price:.4f}\nTP2: {current_pos['tp2_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%40 satıldı, kalan %30 trailing\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP2 Hit kuyruğa eklendi: {message}")
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                message = f"{symbol}: {'SHORT 🚀' if profit_percent > 0 else 'STOP SHORT 📉'}\nPrice: {current_price:.4f}\n{'Profit: ' if profit_percent > 0 else 'Loss: '}{profit_percent:.2f}%\n{'PARAYI VURDUK 🚀' if profit_percent > 0 else 'STOP 😞'}\nKalan %{current_pos['remaining_ratio']*100:.0f} satıldı\nTime: {now.strftime('%H:%M:%S')}"
                await message_queue.put(message)
                logger.info(f"Exit kuyruğa eklendi: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'], 'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                return
            signal_cache[key] = current_pos
    except Exception as e:
        logger.exception(f"Hata ({symbol}): {str(e)}")
        return

# ================== Main ==================
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))
    asyncio.create_task(message_sender())
    symbols = [
        'ETHUSDT', 'BTCUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'FARTCOINUSDT', '1000PEPEUSDT', 'ADAUSDT', 'SUIUSDT', 'WIFUSDT',
        'ENAUSDT', 'PENGUUSDT', '1000BONKUSDT', 'HYPEUSDT', 'AVAXUSDT', 'MOODENGUSDT', 'LINKUSDT', 'PUMPFUNUSDT', 'LTCUSDT', 'TRUMPUSDT',
        'AAVEUSDT', 'ARBUSDT', 'NEARUSDT', 'ONDOUSDT', 'POPCATUSDT', 'TONUSDT', 'OPUSDT', '1000FLOKIUSDT', 'SEIUSDT', 'HBARUSDT',
        'WLDUSDT', 'BNBUSDT', 'UNIUSDT', 'XLMUSDT', 'CRVUSDT', 'VIRTUALUSDT', 'AI16ZUSDT', 'TIAUSDT', 'TAOUSDT', 'APTUSDT',
        'DOTUSDT', 'SPXUSDT', 'ETCUSDT', 'LDOUSDT', 'BCHUSDT', 'INJUSDT', 'KASUSDT', 'ALGOUSDT', 'TRXUSDT', 'IPUSDT',
        'FILUSDT', 'STXUSDT', 'ATOMUSDT', 'RUNEUSDT', 'THETAUSDT', 'FETUSDT', 'AXSUSDT', 'SANDUSDT', 'MANAUSDT', 'CHZUSDT',
        'APEUSDT', 'GALAUSDT', 'IMXUSDT', 'DYDXUSDT', 'GMTUSDT', 'EGLDUSDT', 'ZKUSDT', 'NOTUSDT', 'ENSUSDT', 'JUPUSDT',
        'ATHUSDT', 'ICPUSDT', 'STRKUSDT', 'ORDIUSDT', 'PENDLEUSDT', 'PNUTUSDT', 'RENDERUSDT', 'OMUSDT', 'ZORAUSDT', 'SUSDT',
        'GRASSUSDT', 'TRBUSDT', 'MOVEUSDT', 'XAUTUSDT', 'POLUSDT', 'CVXUSDT', 'BRETTUSDT', 'SAROSUSDT', 'GOATUSDT', 'AEROUSDT',
        'JTOUSDT', 'HYPERUSDT', 'ETHFIUSDT', 'BERAUSDT'
    ]
    while True:
        tasks = []
        for symbol in symbols:
            tasks.append(check_signals(symbol))
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            await asyncio.sleep(3)
        logger.info("Taramalar tamam, 5 dk bekle...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())