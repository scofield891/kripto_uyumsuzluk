import ccxt
import numpy as np
import pandas as pd
import telegram
import logging
from logging.handlers import RotatingFileHandler  # Doğrudan import
import asyncio
from datetime import datetime, timedelta
import pytz
import sys
import os
import pandas_ta as ta

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
ADX_THRESHOLD = 25
ADX_PERIOD = 14

# ================== Logging ==================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = RotatingFileHandler('bot.log', maxBytes=10*1024*1024, backupCount=5)
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
        except Exception as e:
            logger.error(f"Telegram mesaj hatası: {str(e)}")
        message_queue.task_done()

# ================== İndikatör Fonksiyonları ==================
def calculate_ema(closes, span):
    return pd.Series(closes).ewm(span=span, adjust=True).mean().values

def calculate_sma(closes, period):
    return pd.Series(closes).rolling(window=period).mean().values

def ensure_atr(df, period=20):
    if 'atr' not in df.columns:
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=period).mean().ffill()
    return df

def calculate_smi(df):
    bb_middle = calculate_sma(df['close'].values, 20)
    bb_std = pd.Series(df['close']).rolling(window=20).std(ddof=1).values
    bb_upper = bb_middle + 2 * bb_std
    bb_lower = bb_middle - 2 * bb_std
    df = ensure_atr(df, 20)
    kc_middle = calculate_ema(df['close'].values, 20)
    kc_upper = kc_middle + 1.5 * df['atr'].values
    kc_lower = kc_middle - 1.5 * df['atr'].values
    squeeze_on = (bb_upper[-1] < kc_upper[-1]) and (bb_lower[-1] > kc_lower[-1])
    squeeze_off = not squeeze_on
    hl2 = (df['high'].values + df['low'].values) / 2
    sma_hl2 = calculate_sma(hl2, 20)
    diff = hl2 - sma_hl2
    momentum = 0.0
    if len(diff) >= 20:
        linreg = np.polyfit(range(20), diff[-20:], 1)[0]
        momentum = linreg * 10
    color = 'green' if momentum > 0 else 'red' if momentum < 0 else 'gray'
    return squeeze_off, momentum, color

def calculate_adx(df, period=14, threshold=25):
    adx = ta.ADX(df['high'], df['low'], df['close'], timeperiod=period)
    df['adx'] = adx['ADX_' + str(period)]
    df['di_plus'] = adx['DIp_' + str(period)]
    df['di_minus'] = adx['DIm_' + str(period)]
    adx_condition = df['adx'].iloc[-2] > threshold
    di_condition_long = df['di_plus'].iloc[-2] > df['di_minus'].iloc[-2]
    di_condition_short = df['di_plus'].iloc[-2] < df['di_minus'].iloc[-2]
    return df, adx_condition, di_condition_long, di_condition_short

def get_atr_values(df, lookback_atr=18):
    df = ensure_atr(df, period=20)
    if len(df) < lookback_atr + 2:
        return np.nan, np.nan
    atr_value = float(df['atr'].iloc[-2])
    close_last = float(df['close'].iloc[-2])
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1]
    avg_atr_ratio = float(atr_series.mean() / close_last) if len(atr_series) and close_last != 0 else np.nan
    return atr_value, avg_atr_ratio

def calculate_indicators(df, adx_threshold=25):
    df = df.copy()
    if len(df) < 80:
        logger.warning("DF çok kısa, indikatör hesaplanamadı.")
        return None, None, None, None, None, None, None
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, 13)
    df['sma34'] = calculate_sma(closes, 34)
    df['volume_sma20'] = df['volume'].rolling(window=20).mean().ffill()
    df, adx_condition, di_condition_long, di_condition_short = calculate_adx(df, period=ADX_PERIOD, threshold=adx_threshold)
    smi_squeeze_off, smi_histogram, smi_color = calculate_smi(df)
    return df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short

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
                    markets = exchange.load_markets()
                    if not markets.get(symbol, {}).get('active', False):
                        logger.warning(f"{symbol} aktif değil, skip.")
                        return
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
            if df is None or df.empty or len(df) < 80:
                logger.warning(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), skip.")
                return

        df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short = calculate_indicators(df, adx_threshold=ADX_THRESHOLD)
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
            'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
            'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0,
            'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False
        })

        ema13_slice = df['ema13'].values[-LOOKBACK_CROSSOVER-1:]
        sma34_slice = df['sma34'].values[-LOOKBACK_CROSSOVER-1:]
        price_slice = df['close'].values[-LOOKBACK_CROSSOVER-1:]
        if len(ema13_slice) <= LOOKBACK_CROSSOVER:
            logger.warning(f"Veri yetersiz ({symbol}), skip.")
            return

        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        for i in range(1, LOOKBACK_CROSSOVER + 1):
            if 0 <= i < len(ema13_slice):
                if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and price_slice[-i] > sma34_slice[-i]:
                    ema_sma_crossover_buy = True
                if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and price_slice[-i] < sma34_slice[-i]:
                    ema_sma_crossover_sell = True
        logger.info(f"{symbol} EMA/SMA crossover_buy: {ema_sma_crossover_buy}, crossover_sell: {ema_sma_crossover_sell}")

        recent = df['close'].values[-LOOKBACK_CROSSOVER-1:-1]
        ema13_recent = df['ema13'].values[-LOOKBACK_CROSSOVER-1:-1]
        pullback_long = (recent < ema13_recent).any() and (closed_candle['close'] > closed_candle['ema13']) and (closed_candle['close'] > closed_candle['sma34'])
        pullback_short = (recent > ema13_recent).any() and (closed_candle['close'] < closed_candle['ema13']) and (closed_candle['close'] < closed_candle['sma34'])
        logger.info(f"{symbol} pullback_long: {pullback_long}, pullback_short: {pullback_short}")

        volume_multiplier = 1.0 + min(avg_atr_ratio * 3, 0.2)
        volume_ok = closed_candle['volume'] > closed_candle['volume_sma20'] * volume_multiplier
        logger.info(f"{symbol} volume_ok: {volume_ok}, multiplier: {volume_multiplier:.2f}")

        smi_condition_long = smi_squeeze_off and (smi_histogram > 0 or smi_color == 'green')
        smi_condition_short = smi_squeeze_off and (smi_histogram < 0 or smi_color == 'red')
        logger.info(f"{symbol} SMI_squeeze_off: {smi_squeeze_off}, SMI_histogram: {smi_histogram:.2f}, SMI_color: {smi_color}")
        logger.info(f"{symbol} SMI_long_condition: {smi_condition_long}, SMI_short_condition: {smi_condition_short}")

        logger.info(f"{symbol} ADX: {df['adx'].iloc[-2]:.2f}, ADX_condition: {adx_condition}, DI_long: {di_condition_long}, DI_short: {di_condition_short}")

        buy_condition = ema_sma_crossover_buy and pullback_long and volume_ok and smi_condition_long and adx_condition and di_condition_long
        sell_condition = ema_sma_crossover_sell and pullback_short and volume_ok and smi_condition_short and adx_condition and di_condition_short
        logger.info(f"{symbol} buy_condition: {buy_condition}, sell_condition: {sell_condition}")

        current_pos = signal_cache.get(key, current_pos)
        current_price = float(df['close'].iloc[-1])
        tz = pytz.timezone('Europe/Istanbul')
        now = datetime.now(tz)

        if buy_condition and sell_condition:
            logger.warning(f"{symbol}: Çakışan sinyaller, işlem yapılmadı.")
            return

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
                    'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0,
                    'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                current_pos = signal_cache[key]

            if buy_condition and current_pos['signal'] != 'buy':
                if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'buy' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                    message = f"{symbol}: BUY sinyali atlanıyor (cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                    await message_queue.put(message)
                    logger.info(message)
                else:
                    entry_price = float(closed_candle['close'])
                    sl_multiplier = SL_MULTIPLIER * (1 + avg_atr_ratio * 0.3)
                    tp1_multiplier = TP_MULTIPLIER1 * (1 + avg_atr_ratio * 0.3)
                    tp2_multiplier = TP_MULTIPLIER2 * (1 + avg_atr_ratio * 0.3)
                    sl_price = entry_price - (sl_multiplier * atr_value + SL_BUFFER * atr_value)
                    if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                        message = f"{symbol}: BUY sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nSL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
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
                            f"{symbol}: BUY (LONG) 🚀\n"
                            f"SMI Confirm: {smi_condition_long}\n"
                            f"ADX: {df['adx'].iloc[-2]:.2f}, DI+: {df['di_plus'].iloc[-2]:.2f}, DI-: {df['di_minus'].iloc[-2]:.2f}\n"
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
                    sl_multiplier = SL_MULTIPLIER * (1 + avg_atr_ratio * 0.3)
                    tp1_multiplier = TP_MULTIPLIER1 * (1 + avg_atr_ratio * 0.3)
                    tp2_multiplier = TP_MULTIPLIER2 * (1 + avg_atr_ratio * 0.3)
                    sl_price = entry_price + (sl_multiplier * atr_value + SL_BUFFER * atr_value)
                    if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                        message = f"{symbol}: SELL sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nSL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
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
                            f"{symbol}: SELL (SHORT) 📉\n"
                            f"SMI Confirm: {smi_condition_short}\n"
                            f"ADX: {df['adx'].iloc[-2]:.2f}, DI+: {df['di_plus'].iloc[-2]:.2f}, DI-: {df['di_minus'].iloc[-2]:.2f}\n"
                            f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                            f"Time: {now.strftime('%H:%M:%S')}"
                        )
                        await message_queue.put(message)
                        logger.info(f"Sell sinyali kuyruğa eklendi: {message}")

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
                message = (
                    f"{symbol}: {'LONG 🚀' if profit_percent > 0 else 'STOP LONG 📉'}\n"
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
                return
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            td = current_pos['trailing_distance']
            if current_price <= current_pos['entry_price'] - (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']:
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
                message = (
                    f"{symbol}: TRAILING ACTIVE 🚧\n"
                    f"Current: {current_price:.4f}\nEntry: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\nTime: {now.strftime('%H:%M:%S')}"
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
                message = (
                    f"{symbol}: {'SHORT 🚀' if profit_percent > 0 else 'STOP SHORT 📉'}\n"
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
        logger.exception(f"Hata ({symbol}): {str(e)}")
        await message_queue.put(f"{symbol}: KRİTİK HATA ⚠️\n{str(e)}\nTime: {datetime.now(tz).strftime('%H:%M:%S')}")
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