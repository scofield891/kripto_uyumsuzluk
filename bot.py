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
TEST_MODE = False
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
ADX_THRESHOLD = 25
ADX_PERIOD = 14

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
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'linear'},
    'timeout': 60000
})
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
        except Exception as e:
            logger.error(f"Telegram mesaj hatası: {str(e)}")
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

def calculate_adx(df, symbol, period=ADX_PERIOD):
    df['high_diff'] = df['high'] - df['high'].shift(1)
    df['low_diff'] = df['low'].shift(1) - df['low']
    df['+DM'] = np.where((df['high_diff'] > df['low_diff']) & (df['high_diff'] > 0), df['high_diff'], 0)
    df['-DM'] = np.where((df['low_diff'] > df['high_diff']) & (df['low_diff'] > 0), df['low_diff'], 0)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    df['TR'] = np.maximum(high_low, np.maximum(high_close, low_close))
    # Wilder EMA: alpha = 1/period
    alpha = 1.0 / period
    tr_ema = df['TR'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    df['di_plus'] = 100 * (df['+DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    df['di_minus'] = 100 * (df['-DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    df['DX'] = 100 * np.abs(df['di_plus'] - df['di_minus']) / (df['di_plus'] + df['di_minus']).replace(0, np.nan).fillna(0)
    df['adx'] = df['DX'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    adx_condition = df['adx'].iloc[-2] > ADX_THRESHOLD if pd.notna(df['adx'].iloc[-2]) else False
    di_condition_long = df['di_plus'].iloc[-2] > df['di_minus'].iloc[-2] if pd.notna(df['di_plus'].iloc[-2]) and pd.notna(df['di_minus'].iloc[-2]) else False
    di_condition_short = df['di_plus'].iloc[-2] < df['di_minus'].iloc[-2] if pd.notna(df['di_plus'].iloc[-2]) and pd.notna(df['di_minus'].iloc[-2]) else False
    logger.info(f"ADX calculated: {df['adx'].iloc[-2]:.2f} for {symbol} at {df.index[-2]}")
    return df, adx_condition, di_condition_long, di_condition_short

def calculate_bb(df, period=20, mult=2.0):
    df['bb_mid'] = df['close'].rolling(period).mean()
    df['bb_std'] = df['close'].rolling(period).std()
    df['bb_upper'] = df['bb_mid'] + mult * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - mult * df['bb_std']
    return df

def calculate_kc(df, period=20, atr_period=20, mult=1.5):
    df['kc_mid'] = pd.Series(calculate_ema(df['close'].values, period))
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = np.maximum(high_low, np.maximum(high_close, low_close))
    df['atr_kc'] = tr.rolling(atr_period).mean()
    df['kc_upper'] = df['kc_mid'] + mult * df['atr_kc']
    df['kc_lower'] = df['kc_mid'] - mult * df['atr_kc']
    return df

def calculate_squeeze(df):
    df['squeeze_on'] = (df['bb_lower'] > df['kc_lower']) & (df['bb_upper'] < df['kc_upper'])
    df['squeeze_off'] = (df['bb_lower'] < df['kc_lower']) & (df['bb_upper'] > df['kc_upper'])
    return df

def calculate_smi_momentum(df, length=LOOKBACK_SMI):
    highest = df['high'].rolling(length).max()
    lowest = df['low'].rolling(length).min()
    avg1 = (highest + lowest) / 2
    avg2 = df['close'].rolling(length).mean()
    avg = (avg1 + avg2) / 2
    diff = df['close'] - avg
    smi = pd.Series(np.nan, index=df.index)
    for i in range(length-1, len(df)):
        y = diff.iloc[i-length+1:i+1].values
        x = np.arange(length)
        mask = ~np.isnan(y)
        if mask.sum() < 2:
            smi.iloc[i] = np.nan
            continue
        slope, intercept = np.polyfit(x[mask], y[mask], 1)
        smi.iloc[i] = slope * (length - 1) + intercept
    df['smi'] = smi
    return df

def ensure_atr(df, period=14):
    if 'atr' in df.columns:
        return df
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=period).mean()
    return df

def get_atr_values(df, lookback_atr=LOOKBACK_ATR):
    df = ensure_atr(df, period=14)
    if len(df) < lookback_atr + 2:
        return np.nan, np.nan
    atr_value = float(df['atr'].iloc[-2]) if pd.notna(df['atr'].iloc[-2]) else np.nan
    close_last = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1]
    avg_atr_ratio = float(atr_series.mean() / close_last) if len(atr_series) and pd.notna(close_last) and close_last != 0 else np.nan
    return atr_value, avg_atr_ratio

def calculate_indicators(df, symbol, timeframe):
    if len(df) < 80:
        logger.warning(f"DF çok kısa ({len(df)}), indikatör hesaplanamadı.")
        return None, None, None, None, None, None, None
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, 13)
    df['sma34'] = calculate_sma(closes, 34)
    df['volume_sma20'] = df['volume'].rolling(20).mean().ffill()
    df = calculate_bb(df)
    df = calculate_kc(df)
    df = calculate_squeeze(df)
    df = calculate_smi_momentum(df)
    df, adx_condition, di_condition_long, di_condition_short = calculate_adx(df, symbol)
    return df, df['squeeze_off'].iloc[-2], df['smi'].iloc[-2], 'green' if df['smi'].iloc[-2] > 0 else 'red' if df['smi'].iloc[-2] < 0 else 'gray', adx_condition, di_condition_long, di_condition_short

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe='4h'):
    tz = pytz.timezone('Europe/Istanbul')
    try:
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
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=max(150, LOOKBACK_ATR + 80))
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    break
                except (ccxt.RequestTimeout, ccxt.NetworkError):
                    logger.warning(f"Timeout/Network ({symbol} {timeframe}), retry {attempt+1}/{max_retries}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
                except (ccxt.BadSymbol, ccxt.BadRequest) as e:
                    logger.warning(f"Skip {symbol} {timeframe}: {e.__class__.__name__} - {e}")
                    return
            if df is None or df.empty or len(df) < 80:
                logger.warning(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), skip.")
                return

        df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short = calculate_indicators(df, symbol, timeframe)
        if df is None:
            return

        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol} {timeframe}), skip.")
            return

        closed_candle = df.iloc[-2]
        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
        logger.info(f"{symbol} {timeframe} Closed Candle Close: {closed_candle['close']:.4f}, Current Price: {current_price:.4f}")

        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
            'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0,
            'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False
        })

        ema13_slice = df['ema13'].values[-LOOKBACK_CROSSOVER-1:-1]
        sma34_slice = df['sma34'].values[-LOOKBACK_CROSSOVER-1:-1]
        price_slice = df['close'].values[-LOOKBACK_CROSSOVER-1:-1]
        if len(ema13_slice) < LOOKBACK_CROSSOVER:
            logger.warning(f"Veri yetersiz ({symbol} {timeframe}), skip.")
            return

        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        for i in range(1, LOOKBACK_CROSSOVER + 1):
            if 0 <= i < len(ema13_slice):
                if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and price_slice[-i] > sma34_slice[-i]:
                    ema_sma_crossover_buy = True
                if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and price_slice[-i] < sma34_slice[-i]:
                    ema_sma_crossover_sell = True
        logger.info(f"{symbol} {timeframe} EMA/SMA crossover_buy: {ema_sma_crossover_buy}, crossover_sell: {ema_sma_crossover_sell}")

        recent = df['close'].values[-LOOKBACK_CROSSOVER-1:-1]
        ema13_recent = df['ema13'].values[-LOOKBACK_CROSSOVER-1:-1]
        pullback_long = (recent < ema13_recent).any() and (closed_candle['close'] > closed_candle['ema13']) and (closed_candle['close'] > closed_candle['sma34'])
        pullback_short = (recent > ema13_recent).any() and (closed_candle['close'] < closed_candle['ema13']) and (closed_candle['close'] < closed_candle['sma34'])
        logger.info(f"{symbol} {timeframe} pullback_long: {pullback_long}, pullback_short: {pullback_short}")

        volume_multiplier = 1.0 + min(avg_atr_ratio * 3, 0.2) if np.isfinite(avg_atr_ratio) else 1.0
        volume_ok = closed_candle['volume'] > closed_candle['volume_sma20'] * volume_multiplier if pd.notna(closed_candle['volume']) and pd.notna(closed_candle['volume_sma20']) else False
        logger.info(f"{symbol} {timeframe} volume_ok: {volume_ok}, multiplier: {volume_multiplier:.2f}")

        smi_condition_long = smi_squeeze_off and (smi_histogram > 0 or smi_color == 'green')
        smi_condition_short = smi_squeeze_off and (smi_histogram < 0 or smi_color == 'red')
        logger.info(f"{symbol} {timeframe} SMI_squeeze_off: {smi_squeeze_off}, SMI_histogram: {smi_histogram:.2f}, SMI_color: {smi_color}")
        logger.info(f"{symbol} {timeframe} SMI_long_condition: {smi_condition_long}, SMI_short_condition: {smi_condition_short}")

        adx_value = f"{closed_candle['adx']:.2f}" if pd.notna(closed_candle['adx']) else 'NaN'
        di_plus_value = f"{closed_candle['di_plus']:.2f}" if pd.notna(closed_candle['di_plus']) else 'NaN'
        di_minus_value = f"{closed_candle['di_minus']:.2f}" if pd.notna(closed_candle['di_minus']) else 'NaN'
        logger.info(f"{symbol} {timeframe} ADX: {adx_value}, ADX_condition: {adx_condition}, DI+: {di_plus_value}, DI-: {di_minus_value}, DI_long: {di_condition_long}, DI_short: {di_condition_short}")

        buy_condition = ema_sma_crossover_buy and pullback_long and volume_ok and smi_condition_long and adx_condition and di_condition_long
        sell_condition = ema_sma_crossover_sell and pullback_short and volume_ok and smi_condition_short and adx_condition and di_condition_short
        logger.info(f"{symbol} {timeframe} buy_condition: {buy_condition}, sell_condition: {sell_condition}")

        current_pos = signal_cache.get(key, current_pos)
        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
        now = datetime.now(tz)

        smi_value = f"{closed_candle['smi']:.2f}" if pd.notna(closed_candle['smi']) else 'NaN'

        if buy_condition and sell_condition:
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return

        if buy_condition or sell_condition:
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] is not None and current_pos['signal'] != new_signal:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
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
                    'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0,
                    'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                current_pos = signal_cache[key]

            if buy_condition and current_pos['signal'] != 'buy':
                if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'buy' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                    message = f"{symbol} {timeframe}: BUY sinyali atlanıyor (cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                    await message_queue.put(message)
                    logger.info(message)
                else:
                    entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                    sl_multiplier = SL_MULTIPLIER * (1 + avg_atr_ratio * 0.3) if np.isfinite(avg_atr_ratio) else SL_MULTIPLIER
                    tp1_multiplier = TP_MULTIPLIER1 * (1 + avg_atr_ratio * 0.3) if np.isfinite(avg_atr_ratio) else TP_MULTIPLIER1
                    tp2_multiplier = TP_MULTIPLIER2 * (1 + avg_atr_ratio * 0.3) if np.isfinite(avg_atr_ratio) else TP_MULTIPLIER2
                    sl_price = entry_price - (sl_multiplier * atr_value + SL_BUFFER * atr_value) if np.isfinite(atr_value) else np.nan
                    if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                        logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                        return
                    if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                        message = f"{symbol} {timeframe}: BUY sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nSL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                        await message_queue.put(message)
                        logger.info(message)
                    else:
                        tp1_price = entry_price + (tp1_multiplier * atr_value)
                        tp2_price = entry_price + (tp2_multiplier * atr_value)
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
                            f"{symbol} {timeframe}: BUY (LONG) 🚀\n"
                            f"SMI: {smi_value}\n"
                            f"ADX: {adx_value}, DI+: {di_plus_value}, DI-: {di_minus_value}\n"
                            f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                            f"Time: {now.strftime('%H:%M:%S')}"
                        )
                        await message_queue.put(message)
                        logger.info(f"Buy sinyali kuyruğa eklendi: {message}")
            elif sell_condition and current_pos['signal'] != 'sell':
                if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'sell' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                    message = f"{symbol} {timeframe}: SELL sinyali atlanıyor (cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                    await message_queue.put(message)
                    logger.info(message)
                else:
                    entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                    sl_multiplier = SL_MULTIPLIER * (1 + avg_atr_ratio * 0.3) if np.isfinite(avg_atr_ratio) else SL_MULTIPLIER
                    tp1_multiplier = TP_MULTIPLIER1 * (1 + avg_atr_ratio * 0.3) if np.isfinite(avg_atr_ratio) else TP_MULTIPLIER1
                    tp2_multiplier = TP_MULTIPLIER2 * (1 + avg_atr_ratio * 0.3) if np.isfinite(avg_atr_ratio) else TP_MULTIPLIER2
                    sl_price = entry_price + (sl_multiplier * atr_value + SL_BUFFER * atr_value) if np.isfinite(atr_value) else np.nan
                    if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                        logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                        return
                    if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                        message = f"{symbol} {timeframe}: SELL sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nSL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                        await message_queue.put(message)
                        logger.info(message)
                    else:
                        tp1_price = entry_price - (tp1_multiplier * atr_value)
                        tp2_price = entry_price - (tp2_multiplier * atr_value)
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
                            f"{symbol} {timeframe}: SELL (SHORT) 📉\n"
                            f"SMI: {smi_value}\n"
                            f"ADX: {adx_value}, DI+: {di_plus_value}, DI-: {di_minus_value}\n"
                            f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                            f"Time: {now.strftime('%H:%M:%S')}"
                        )
                        await message_queue.put(message)
                        logger.info(f"Sell sinyali kuyruğa eklendi: {message}")

        if current_pos['signal'] == 'buy':
            atr_value, _ = get_atr_values(df, LOOKBACK_ATR)
            if not np.isfinite(atr_value):
                logger.warning(f"ATR NaN ({symbol} {timeframe}), skip.")
                return
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            td = current_pos['trailing_distance']
            if current_price >= current_pos['entry_price'] + (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']:
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current: {current_price:.4f}\nEntry: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"Trailing Activated kuyruğa eklendi: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP1 Hit 🚀\nCurrent: {current_price:.4f}\nTP1: {current_pos['tp1_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%30 satıldı, SL: {current_pos['sl_price']:.4f}\n"
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP1 Hit kuyruğa eklendi: {message}")
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP2 Hit 🚀\nCurrent: {current_price:.4f}\nTP2: {current_pos['tp2_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%40 satıldı, kalan %30 trailing\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP2 Hit kuyruğa eklendi: {message}")
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                message = (
                    f"{symbol} {timeframe}: {'LONG 🚀' if profit_percent > 0 else 'STOP LONG 📉'}\n"
                    f"Price: {current_price:.4f}\n"
                    f"{'Profit: ' if profit_percent > 0 else 'Loss: '}{profit_percent:.2f}%\n"
                    f"{'PARAYI VURDUK 🚀' if profit_percent > 0 else 'STOP 😞'}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
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
                logger.warning(f"ATR NaN ({symbol} {timeframe}), skip.")
                return
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            td = current_pos['trailing_distance']
            if current_price <= current_pos['entry_price'] - (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']:
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current: {current_price:.4f}\nEntry: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"Trailing Activated kuyruğa eklendi: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP1 Hit 🚀\nCurrent: {current_price:.4f}\nTP1: {current_pos['tp1_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%30 satıldı, SL: {current_pos['sl_price']:.4f}\n"
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP1 Hit kuyruğa eklendi: {message}")
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP2 Hit 🚀\nCurrent: {current_price:.4f}\nTP2: {current_pos['tp2_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n%40 satıldı, kalan %30 trailing\nTime: {now.strftime('%H:%M:%S')}"
                )
                await message_queue.put(message)
                logger.info(f"TP2 Hit kuyruğa eklendi: {message}")
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                message = (
                    f"{symbol} {timeframe}: {'SHORT 🚀' if profit_percent > 0 else 'STOP SHORT 📉'}\n"
                    f"Price: {current_price:.4f}\n"
                    f"{'Profit: ' if profit_percent > 0 else 'Loss: '}{profit_percent:.2f}%\n"
                    f"{'PARAYI VURDUK 🚀' if profit_percent > 0 else 'STOP 😞'}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
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
        logger.exception(f"Hata ({symbol} {timeframe}): {str(e)}")
        await message_queue.put(f"{symbol} {timeframe}: KRİTİK HATA ⚠️\n{str(e)}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}")
        return

# ================== Main ==================
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))
    asyncio.create_task(message_sender())  # Mesaj göndericiyi başlat
    timeframes = ['4h']  # Backtest'e göre uyarlandı, sadece 4h
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