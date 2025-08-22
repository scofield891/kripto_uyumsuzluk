import ccxt
import numpy as np
import pandas as pd
from telegram import Bot
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
import sys
import os

# ================== Sabit Değerler ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "7677279035:AAHMecBYUliT7QlUl9OtB0kgXl8uyyuxbsQ")
CHAT_ID = os.getenv("CHAT_ID", "-1002878297025")
TEST_MODE = False
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
USE_STOCH = True  # Stochastic teyit filtresi aktif (kaliteli sinyaller)

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
telegram_bot = Bot(token=BOT_TOKEN)
signal_cache = {}

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

def calculate_stoch(high, low, close, k=14, d=3):
    ll = pd.Series(low).rolling(k).min()
    hh = pd.Series(high).rolling(k).max()
    k_raw = 100 * (close - ll) / (hh - ll + 1e-9)
    d_line = k_raw.rolling(d).mean()
    return k_raw, d_line

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

def calculate_indicators(df, timeframe):
    if len(df) < 80:
        logger.warning("DF çok kısa, indikatör hesaplanamadı.")
        return None
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, span=13)
    df['sma34'] = calculate_sma(closes, period=34)
    df['rsi'] = calculate_rsi(closes)
    df['rsi_ema'] = calculate_rsi_ema(df['rsi'])
    df['macd'], df['macd_signal'], df['macd_hist'] = calculate_macd(closes, timeframe)
    df['stoch_k'], df['stoch_d'] = calculate_stoch(df['high'], df['low'], df['close'])
    df['volume_sma20'] = df['volume'].rolling(window=20).mean()
    return df

# ================== Backtest Fonksiyonu ==================
async def backtest(symbol, timeframe, limit=2190):  # ~1 yıl 4h veri (365*6)
    wins = 0
    total = 0
    try:
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(limit))) * 0.05 + 0.3
            highs = closes + np.random.rand(limit) * 0.02 * closes
            lows = closes - np.random.rand(limit) * 0.02 * closes
            volumes = np.random.rand(limit) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(limit)]
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
        else:
            since = int((datetime.now() - timedelta(days=365)).timestamp() * 1000)
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
        
        for i in range(80, len(df)):
            temp_df = df.iloc[:i+1]
            key = f"{symbol}_{timeframe}"
            prev_pos = signal_cache.get(key, {
                'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
                'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0,
                'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
                'tp1_hit': False, 'tp2_hit': False
            })
            await check_signals(symbol, timeframe, df=temp_df)
            pos = signal_cache.get(key, prev_pos)
            if pos.get('signal'):
                current_price = float(temp_df.iloc[-1]['close'])
                if pos.get('tp1_hit') or pos.get('tp2_hit'):
                    wins += 1
                    total += 1
                    signal_cache[key] = prev_pos  # Pozisyonu sıfırla
                elif (pos['signal'] == 'buy' and current_price <= pos['sl_price']) or \
                     (pos['signal'] == 'sell' and current_price >= pos['sl_price']):
                    total += 1
                    signal_cache[key] = prev_pos
        winrate = (wins / total * 100) if total > 0 else 0
        logger.info(f"Backtest {symbol} {timeframe}: Winrate {winrate:.2f}%, Total Trades {total}")
        return winrate, total
    except Exception as e:
        logger.exception(f"Backtest Hata ({symbol} {timeframe}): {str(e)}")
        return 0, 0

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
                df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                logger.info(f"Test modu: {symbol} {timeframe}")
            else:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=max(150, LOOKBACK_ATR + 80))
                        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
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
                    return
        # İndikatörler
        df = calculate_indicators(df, timeframe)
        if df is None:
            return
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol} {timeframe}), skip.")
            return
        closed_candle = df.iloc[-2]
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
            if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and \
               price_slice[-i] > sma34_slice[-i]:
                ema_sma_crossover_buy = True
            if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and \
               price_slice[-i] < sma34_slice[-i]:
                ema_sma_crossover_sell = True
        # RSI-EMA Zone Bounce
        zone_cross_up = (df['rsi_ema'].iloc[-2] < 50 <= df['rsi_ema'].iloc[-1]) and (df['rsi_ema'].iloc[-1] > df['rsi_ema'].iloc[-2])
        zone_cross_down = (df['rsi_ema'].iloc[-2] > 50 >= df['rsi_ema'].iloc[-1]) and (df['rsi_ema'].iloc[-1] < df['rsi_ema'].iloc[-2])
        # Pullback kontrolü
        recent = df.iloc[-(LOOKBACK_CROSSOVER+1):-1]
        pullback_long = (recent['close'] < recent['ema13']).any() and (closed_candle['close'] > closed_candle['ema13']) and (closed_candle['close'] > closed_candle['sma34'])
        pullback_short = (recent['close'] > recent['ema13']).any() and (closed_candle['close'] < closed_candle['ema13']) and (closed_candle['close'] < closed_candle['sma34'])
        # Stochastic teyit
        stoch_cross_up = (df['stoch_k'].iloc[-2] < df['stoch_d'].iloc[-2]) and (df['stoch_k'].iloc[-1] > df['stoch_d'].iloc[-1]) and (df['stoch_k'].iloc[-2] < 30)
        stoch_cross_down = (df['stoch_k'].iloc[-2] > df['stoch_d'].iloc[-2]) and (df['stoch_k'].iloc[-1] < df['stoch_d'].iloc[-1]) and (df['stoch_k'].iloc[-2] > 70)
        # Volume filtresi
        volume_ok = closed_candle['volume'] > closed_candle['volume_sma20']
        # MACD filtre
        macd_up = df['macd'].iloc[-2] > df['macd_signal'].iloc[-2]
        macd_down = df['macd'].iloc[-2] < df['macd_signal'].iloc[-2]
        hist_up = df['macd_hist'].iloc[-2] > 0
        hist_down = df['macd_hist'].iloc[-2] < 0
        if MACD_MODE == "and":
            macd_ok_long = macd_up and hist_up
            macd_ok_short = macd_down and hist_down
        elif MACD_MODE == "regime":
            macd_ok_long = macd_up
            macd_ok_short = macd_down
        else:
            macd_ok_long = True
            macd_ok_short = True
        logger.info(
            f"{symbol} {timeframe} | "
            f"ZoneUp={zone_cross_up}, ZoneDown={zone_cross_down} | "
            f"PullLong={pullback_long}, PullShort={pullback_short} | "
            f"CrossBuy={ema_sma_crossover_buy}, CrossSell={ema_sma_crossover_sell} | "
            f"StochUp={stoch_cross_up}, StochDown={stoch_cross_down} | "
            f"VolumeOK={volume_ok} | "
            f"MACD_MODE={MACD_MODE} (up={macd_up}, hist_up={hist_up}) | "
            f"BUY_OK={'YES' if macd_ok_long and pullback_long and volume_ok and (zone_cross_up or ema_sma_crossover_buy or (stoch_cross_up and USE_STOCH)) else 'no'} | "
            f"SELL_OK={'YES' if macd_ok_short and pullback_short and volume_ok and (zone_cross_down or ema_sma_crossover_sell or (stoch_cross_down and USE_STOCH)) else 'no'}"
        )
        buy_condition = macd_ok_long and pullback_long and volume_ok and (zone_cross_up or ema_sma_crossover_buy or (stoch_cross_up and USE_STOCH))
        sell_condition = macd_ok_short and pullback_short and volume_ok and (zone_cross_down or ema_sma_crossover_sell or (stoch_cross_down and USE_STOCH))
        # Pozisyon yönetimi (önce reversal check)
        current_pos = signal_cache.get(key, current_pos)
        current_price = float(df.iloc[-1]['close'])
        tz = pytz.timezone('Europe/Istanbul')
        now = datetime.now(tz)
        if buy_condition or sell_condition:
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] is not None and current_pos['signal'] != new_signal:
                # Reversal close
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                    message_type = "LONG REVERSAL CLOSE 🚀" if profit_percent > 0 else "LONG REVERSAL STOP 📉"
                    profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                    message_type = "SHORT REVERSAL CLOSE 🚀" if profit_percent > 0 else "SHORT REVERSAL STOP 📉"
                    profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"
                message = (
                    f"{symbol} {timeframe}: {message_type}\n"
                    f"Price: {current_price:.4f}\n"
                    f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                    f"{profit_text}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı (reversal)\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Reversal Close: {message}")
                # Reset state
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
                current_pos = signal_cache[key]
        # Sinyal açılışı
        if buy_condition and current_pos['signal'] != 'buy':
            if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'buy' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                message = f"{symbol} {timeframe}: BUY sinyali atlanıyor (duplicate, cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(message)
            else:
                entry_price = float(closed_candle['close'])
                sl_price = entry_price - (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    message = f"{symbol} {timeframe}: BUY sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nPotansiyel SL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                    await asyncio.sleep(1)
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(message)
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                    trailing_distance = (TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE)
                    current_pos = {
                        'signal': 'buy',
                        'entry_price': entry_price,
                        'sl_price': sl_price,
                        'tp1_price': tp1_price,
                        'tp2_price': tp2_price,
                        'highest_price': entry_price,
                        'lowest_price': None,
                        'trailing_activated': False,
                        'avg_atr_ratio': avg_atr_ratio,
                        'trailing_distance': trailing_distance,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now,
                        'last_signal_type': 'buy',
                        'entry_time': now,
                        'tp1_hit': False,
                        'tp2_hit': False
                    }
                    signal_cache[key] = current_pos
                    message = (
                        f"{symbol} {timeframe}: BUY (LONG) 🚀\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Zone Bounce: Up\n"
                        f"Stoch Confirm: {stoch_cross_up}\n"
                        f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                    await asyncio.sleep(1)
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(f"Sinyal: {message}")
        elif sell_condition and current_pos['signal'] != 'sell':
            if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'sell' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                message = f"{symbol} {timeframe}: SELL sinyali atlanıyor (duplicate, cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(message)
            else:
                entry_price = float(closed_candle['close'])
                sl_price = entry_price + (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    message = f"{symbol} {timeframe}: SELL sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nPotansiyel SL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                    await asyncio.sleep(1)
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(message)
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                    trailing_distance = (TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE)
                    current_pos = {
                        'signal': 'sell',
                        'entry_price': entry_price,
                        'sl_price': sl_price,
                        'tp1_price': tp1_price,
                        'tp2_price': tp2_price,
                        'highest_price': None,
                        'lowest_price': entry_price,
                        'trailing_activated': False,
                        'avg_atr_ratio': avg_atr_ratio,
                        'trailing_distance': trailing_distance,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now,
                        'last_signal_type': 'sell',
                        'entry_time': now,
                        'tp1_hit': False,
                        'tp2_hit': False
                    }
                    signal_cache[key] = current_pos
                    message = (
                        f"{symbol} {timeframe}: SELL (SHORT) 📉\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Zone Bounce: Down\n"
                        f"Stoch Confirm: {stoch_cross_down}\n"
                        f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                    await asyncio.sleep(1)
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(f"Sinyal: {message}")
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
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
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
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Trailing Activated: {message}")
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
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP1 Hit: {message}")
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
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP2 Hit: {message}")
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: LONG 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP LONG 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
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
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Trailing Activated: {message}")
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
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP1 Hit: {message}")
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
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP2 Hit: {message}")
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: SHORT 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP SHORT 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await asyncio.sleep(1)
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
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
        await asyncio.sleep(e.retry_after)
        await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        logger.exception(f"Hata ({symbol} {timeframe}): {str(e)}")
        return

# ================== Main ==================
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))
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
    # Backtest için
    for symbol in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:  # Test için 3 symbol
        winrate, total = await backtest(symbol, '4h', limit=2190)
        await telegram_bot.send_message(chat_id=CHAT_ID, text=f"Backtest {symbol} 4h: Winrate {winrate:.2f}%, Total Trades {total}")
    # Canlı mod
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