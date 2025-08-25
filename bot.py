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
TEST_MODE = False  # Gerçek veriyle çalışsın
RSI_LOW = 40
RSI_HIGH = 60
EMA_THRESHOLD = 0.5
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
MACD_MODE = "regime"
LOOKBACK_CROSSOVER = 10
LOOKBACK_STOCH = 10
STOCH_OVERBOUGHT = 70
STOCH_OVERSOLD = 30
USE_STOCH_RSI = True

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
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

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
        except telegram.error.RetryAfter as e:
            logger.warning(f"RetryAfter: {e.retry_after} saniye bekle")
            await asyncio.sleep(e.retry_after + 2)
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
        except telegram.error.TimedOut:
            logger.warning("TimedOut: Connection pool doldu, 5 saniye bekle")
            await asyncio.sleep(5)
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
        message_queue.task_done()

# ================== İndikatör Fonksiyonları ==================
def calculate_ema(closes, span):
    k = 2 / (span + 1)
    ema = np.zeros_like(closes, dtype=np.float64)
    ema[0] = closes[0]
    for i in range(1, len(closes)):
        ema[i] = (closes[i] * k) + (ema[i-1] * (1 - k))
    return ema

def calculate_sma(closes, period):
    sma = np.zeros_like(closes, dtype=np.float64)
    for i in range(len(closes)):
        if i < period - 1:
            sma[i] = 0.0
        else:
            sma[i] = np.mean(closes[i-period+1:i+1])
    return sma

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return np.zeros(len(closes), dtype=np.float64)
    deltas = np.diff(closes)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = (up / down) if down != 0 else (float('inf') if up > 0 else 0)
    rsi = np.zeros_like(closes, dtype=np.float64)
    rsi[:period] = 100. - 100. / (1. + rs) if rs != float('inf') else 100.
    for i in range(period, len(closes)):
        delta = deltas[i-1]
        upval = max(delta, 0.)
        downval = max(-delta, 0.)
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = (up / down) if down != 0 else (float('inf') if up > 0 else 0)
        rsi[i] = 100. - 100. / (1. + rs) if rs != float('inf') else 100.
    return rsi

def calculate_rsi_ema(rsi, ema_length=14):
    ema = np.zeros_like(rsi, dtype=np.float64)
    if len(rsi) < ema_length:
        return ema
    ema[ema_length-1] = np.mean(rsi[:ema_length])
    alpha = 2 / (ema_length + 1)
    for i in range(ema_length, len(rsi)):
        ema[i] = (rsi[i] * alpha) + (ema[i-1] * (1 - alpha))
    return ema

def calculate_macd(closes, timeframe):
    if timeframe == '1h':
        fast, slow, signal = 8, 17, 9
    else:
        fast, slow, signal = 12, 26, 9
    def ema(x, n):
        k = 2 / (n + 1)
        e = np.zeros_like(x, dtype=np.float64)
        e[0] = x[0]
        for i in range(1, len(x)):
            e[i] = x[i] * k + e[i-1] * (1 - k)
        return e
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def calculate_stoch_rsi(df, rsi_period=14, stoch_period=14, d_period=3):
    rsi = calculate_rsi(df['close'].values, period=rsi_period)
    rsi_series = pd.Series(rsi)
    rsi_low = rsi_series.rolling(stoch_period).min()
    rsi_high = rsi_series.rolling(stoch_period).max()
    stoch_rsi = 100 * (rsi_series - rsi_low) / (rsi_high - rsi_low + 1e-9)
    d_line = stoch_rsi.rolling(d_period).mean()
    return stoch_rsi, d_line

def ensure_atr(df, period=14):
    if 'atr' in df.columns:
        return df
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=period).mean()
    return df

def get_atr_values(df, lookback_atr=18):
    df = ensure_atr(df, period=14)
    if len(df) < lookback_atr + 2:
        return np.nan, np.nan
    atr_value = float(df['atr'].iloc[-2])
    close_last = float(df['close'].iloc[-2])
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1]
    avg_atr_ratio = float(atr_series.mean() / close_last) if len(atr_series) else np.nan
    return atr_value, avg_atr_ratio

def calculate_indicators(df, timeframe, symbol):
    df = df.copy()
    if len(df) < 80:
        logger.warning(f"DF çok kısa, indikatör hesaplanamadı. Length: {len(df)}")
        return None
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, span=13)
    df['sma34'] = calculate_sma(closes, period=34)
    df['rsi'] = calculate_rsi(closes)
    df['rsi_ema'] = calculate_rsi_ema(df['rsi'])
    df['macd'], df['macd_signal'], df['macd_hist'] = calculate_macd(closes, timeframe)
    df['stoch_rsi_k'], df['stoch_rsi_d'] = calculate_stoch_rsi(df)
    df['volume_sma20'] = df['volume'].rolling(window=20).mean()
    if np.any(np.isnan(df['ema13'])) or np.any(np.isnan(df['sma34'])) or np.any(np.isnan(df['stoch_rsi_k'])):
        logger.warning(f"NaN values detected in indicators for {symbol} {timeframe}")
    return df

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe, df=None):
    try:
        # Veri
        if df is None:
            if TEST_MODE:
                closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
                highs = closes + np.random.rand(200) * 0.02 * closes
                lows = closes - np.random.rand(200) * 0.02 * closes
                volumes = np.random.rand(200) * 10000
                ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                logger.info(f"Test modu: {symbol} {timeframe}")
            else:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=200)
                        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                        logger.info(f"Data fetched for {symbol} {timeframe}, length: {len(df)}")
                        break
                    except (ccxt.RequestTimeout, ccxt.NetworkError):
                        logger.warning(f"Timeout/Network ({symbol} {timeframe}), retry {attempt+1}/{max_retries}")
                        if attempt == max_retries - 1:
                            raise
                        await asyncio.sleep(5)
                    except (ccxt.BadSymbol, ccxt.BadRequest) as e:
                        logger.warning(f"Skip {symbol} {timeframe}: {e.__class__.__name__} - {e}")
                        return
                if df is None or df.empty:
                    logger.error(f"No data for {symbol} {timeframe}")
                    return
        # İndikatörler
        df = calculate_indicators(df, timeframe, symbol)
        if df is None:
            return
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol} {timeframe}), skip.")
            return
        closed_candle = df.iloc[-2]
        current_price = float(df.iloc[-1]['close'])
        logger.info(f"{symbol} {timeframe} Closed Candle Close: {closed_candle['close']:.4f}, Current Price: {current_price:.4f}")
        key = f"{symbol}_{timeframe}"
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
        ema13_slice = df['ema13'].values[-(LOOKBACK_CROSSOVER+1):]
        sma34_slice = df['sma34'].values[-(LOOKBACK_CROSSOVER+1):]
        price_slice = df['close'].values[-(LOOKBACK_CROSSOVER+1):]
        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        for i in range(1, LOOKBACK_CROSSOVER + 1):
            if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and price_slice[-i] > sma34_slice[-i]:
                ema_sma_crossover_buy = True
            if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and price_slice[-i] < sma34_slice[-i]:
                ema_sma_crossover_sell = True
        logger.info(f"{symbol} {timeframe} EMA/SMA Slice: EMA13={ema13_slice}, SMA34={sma34_slice}, Price={price_slice}")
        logger.info(f"{symbol} {timeframe} EMA/SMA crossover_buy: {ema_sma_crossover_buy}, crossover_sell: {ema_sma_crossover_sell}")
        # Pullback kontrolü
        recent = df.iloc[-(LOOKBACK_CROSSOVER+1):-1]
        pullback_long = (recent['close'] < recent['ema13']).any() and (closed_candle['close'] > closed_candle['ema13']) and (closed_candle['close'] > closed_candle['sma34'])
        pullback_short = (recent['close'] > recent['ema13']).any() and (closed_candle['close'] < closed_candle['ema13']) and (closed_candle['close'] < closed_candle['sma34'])
        logger.info(f"{symbol} {timeframe} pullback_long: {pullback_long}, pullback_short: {pullback_short}")
        # StochRSI teyit
        stoch_rsi_k = df['stoch_rsi_k'].values[-(LOOKBACK_STOCH+1):]
        stoch_rsi_d = df['stoch_rsi_d'].values[-(LOOKBACK_STOCH+1):]
        stoch_rsi_cross_up = False
        stoch_rsi_cross_down = False
        for i in range(1, LOOKBACK_STOCH + 1):
            if stoch_rsi_k[-i-1] <= stoch_rsi_d[-i-1] and stoch_rsi_k[-i] > stoch_rsi_d[-i] and stoch_rsi_k[-i-1] < STOCH_OVERSOLD:
                stoch_rsi_cross_up = True
            if stoch_rsi_k[-i-1] >= stoch_rsi_d[-i-1] and stoch_rsi_k[-i] < stoch_rsi_d[-i] and stoch_rsi_k[-i-1] > STOCH_OVERBOUGHT:
                stoch_rsi_cross_down = True
        logger.info(f"{symbol} {timeframe} StochRSI K: {stoch_rsi_k}, D: {stoch_rsi_d}")
        logger.info(f"{symbol} {timeframe} stoch_rsi_cross_up: {stoch_rsi_cross_up}, stoch_rsi_cross_down: {stoch_rsi_cross_down}")
        # Volume filtresi
        volume_ok = closed_candle['volume'] > 1.2 * closed_candle['volume_sma20']
        logger.info(f"{symbol} {timeframe} volume_ok: {volume_ok}")
        # MACD filtre
        macd_up = df['macd'].iloc[-2] > df['macd_signal'].iloc[-2]
        macd_down = df['macd'].iloc[-2] < df['macd_signal'].iloc[-2]
        if MACD_MODE == "and":
            macd_ok_long = macd_up and df['macd_hist'].iloc[-2] > 0
            macd_ok_short = macd_down and df['macd_hist'].iloc[-2] < 0
        elif MACD_MODE == "regime":
            macd_ok_long = macd_up
            macd_ok_short = macd_down
        else:
            macd_ok_long = True
            macd_ok_short = True
        logger.info(f"{symbol} {timeframe} macd_ok_long: {macd_ok_long}, macd_ok_short: {macd_ok_short}")
        buy_condition = macd_ok_long and pullback_long and volume_ok and ema_sma_crossover_buy and stoch_rsi_cross_up
        sell_condition = macd_ok_short and pullback_short and volume_ok and ema_sma_crossover_sell and stoch_rsi_cross_down
        logger.info(f"{symbol} {timeframe} buy_condition: {buy_condition}, sell_condition: {sell_condition}")
        # Pozisyon yönetimi
        current_pos = signal_cache.get(key, current_pos)
        tz = pytz.timezone('Europe/Istanbul')
        now = datetime.now(tz)
        if buy_condition or sell_condition:
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] is None:
                entry_price = float(closed_candle['close'])
                sl_price = entry_price - (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value) if buy_condition else entry_price + (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
                if (buy_condition and current_price <= sl_price + INSTANT_SL_BUFFER * atr_value) or \
                   (sell_condition and current_price >= sl_price - INSTANT_SL_BUFFER * atr_value):
                    message = f"{symbol} {timeframe}: {new_signal.upper()} sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nPotansiyel SL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                    await message_queue.put(message)
                    logger.info(message)
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value) if buy_condition else entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value) if buy_condition else entry_price - (TP_MULTIPLIER2 * atr_value)
                    trailing_distance = TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE
                    current_pos = {
                        'signal': new_signal,
                        'entry_price': entry_price,
                        'sl_price': sl_price,
                        'tp1_price': tp1_price,
                        'tp2_price': tp2_price,
                        'highest_price': entry_price if buy_condition else None,
                        'lowest_price': None if buy_condition else entry_price,
                        'trailing_activated': False,
                        'avg_atr_ratio': avg_atr_ratio,
                        'trailing_distance': trailing_distance,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now,
                        'last_signal_type': new_signal,
                        'entry_time': now,
                        'tp1_hit': False,
                        'tp2_hit': False
                    }
                    signal_cache[key] = current_pos
                    message = (
                        f"{symbol} {timeframe}: {new_signal.upper()} ({'LONG' if buy_condition else 'SHORT'}) 🚀\n"
                        f"StochRSI Confirm: {stoch_rsi_cross_up if buy_condition else stoch_rsi_cross_down}\n"
                        f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                    await message_queue.put(message)
                    logger.info(f"Sinyal kuyruğa eklendi: {message}")
            elif current_pos['signal'] != new_signal:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if current_pos['signal'] == 'buy' else ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                message_type = "REVERSAL CLOSE 🚀" if profit_percent > 0 else "REVERSAL STOP 📉"
                profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"
                message = (
                    f"{symbol} {timeframe}: {message_type}\n"
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
                    'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None,
                    'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
        # Pozisyon yönetimi
        if current_pos['signal'] == 'buy':
            atr_value, _ = get_atr_values(df, LOOKBACK_ATR)
            if not np.isfinite(atr_value):
                return
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            td = current_pos['trailing_distance']
            if (current_price >= current_pos['entry_price'] + (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']):
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"Entry Price: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
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
                    f"{symbol} {timeframe}: TP1 Hit 🚀\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"TP1: {current_pos['tp1_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%30 satıldı, SL entry'ye çekildi: {current_pos['sl_price']:.4f}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP1 Hit kuyruğa eklendi: {message}")
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP2 Hit 🚀\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"TP2: {current_pos['tp2_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%40 satıldı, kalan %30 trailing\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP2 Hit kuyruğa eklendi: {message}")
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: LONG 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP LONG 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await message_queue.put(message)
                logger.info(f"Exit kuyruğa eklendi: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None,
                    'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
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
            if (current_price <= current_pos['entry_price'] - (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']):
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"Entry Price: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"Vol Ratio: {current_pos['avg_atr_ratio']:.4f}\n"
                    f"TSL Distance: {td}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
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
                    f"{symbol} {timeframe}: TP1 Hit 🚀\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"TP1: {current_pos['tp1_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%30 satıldı, SL entry'ye çekildi: {current_pos['sl_price']:.4f}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP1 Hit kuyruğa eklendi: {message}")
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP2 Hit 🚀\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"TP2: {current_pos['tp2_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%40 satıldı, kalan %30 trailing\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP2 Hit kuyruğa eklendi: {message}")
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: SHORT 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP SHORT 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await message_queue.put(message)
                logger.info(f"Exit kuyruğa eklendi: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None,
                    'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                return
            signal_cache[key] = current_pos
    except telegram.error.RetryAfter as e:
        logger.warning(f"Telegram flood kontrolü, {e.retry_after} saniye bekle: {symbol} {timeframe}")
        await asyncio.sleep(e.retry_after + 2)
        await message_queue.put(message)
        logger.info(f"RetryAfter sonrası kuyruğa eklendi: {message}")
    except telegram.error.TimedOut:
        logger.warning(f"TimedOut: Connection pool doldu, 5 saniye bekle: {symbol} {timeframe}")
        await asyncio.sleep(5)
        await message_queue.put(message)
        logger.info(f"TimedOut sonrası kuyruğa eklendi: {message}")
    except Exception as e:
        logger.exception(f"Hata ({symbol} {timeframe}): {str(e)}")
        return

# ================== Main ==================
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))
    asyncio.create_task(message_sender())
    timeframes = ['4h']
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
        for timeframe in timeframes:
            for symbol in symbols:
                tasks.append(check_signals(symbol, timeframe))
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            await asyncio.sleep(3)
        logger.info("Taramalar tamam, 5 dk bekle...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())