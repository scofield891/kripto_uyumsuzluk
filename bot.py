import ccxt
import numpy as np
import pandas as pd
from telegram import Bot
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
import sys

# ================== Sabit Değerler ==================
BOT_TOKEN = "7677279035:AAHMecBYUliT7QlUl9OtB0kgXl8uyyuxbsQ"
CHAT_ID = '-1002878297025'
TEST_MODE = False
TRAILING_ACTIVATION = 0.8
TRAILING_DISTANCE_BASE = 1.5
TRAILING_DISTANCE_HIGH_VOL = 2.5
VOLATILITY_THRESHOLD = 0.02
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0  # TP1 for 30% take profit
TP_MULTIPLIER2 = 3.5  # TP2 for 40% take profit
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

def calculate_adx(df, period=ADX_PERIOD):
    df['high_diff'] = df['high'] - df['high'].shift(1)
    df['low_diff'] = df['low'].shift(1) - df['low']
    df['+DM'] = np.where((df['high_diff'] > df['low_diff']) & (df['high_diff'] > 0), df['high_diff'], 0)
    df['-DM'] = np.where((df['low_diff'] > df['high_diff']) & (df['low_diff'] > 0), df['low_diff'], 0)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    df['TR'] = np.maximum(high_low, np.maximum(high_close, low_close))
    df['+DI'] = 100 * (df['+DM'].ewm(span=period, adjust=False).mean() / df['TR'].ewm(span=period, adjust=False).mean())
    df['-DI'] = 100 * (df['-DM'].ewm(span=period, adjust=False).mean() / df['TR'].ewm(span=period, adjust=False).mean())
    df['DX'] = 100 * np.abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI']).replace(0, np.nan)
    df['ADX'] = df['DX'].ewm(span=period, adjust=False).mean()
    return df

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

def calculate_indicators(df, timeframe):
    if len(df) < 80:
        logger.warning(f"DF çok kısa ({len(df)}), indikatör hesaplanamadı.")
        return None
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, span=13)
    df['sma34'] = calculate_sma(closes, period=34)
    df = calculate_bb(df)
    df = calculate_kc(df)
    df = calculate_squeeze(df)
    df = calculate_smi_momentum(df)
    df = calculate_adx(df)
    df['volume_sma20'] = df['volume'].rolling(20).mean()
    return df

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe):
    try:
        # Veri
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
            df = None
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
                logger.warning(f"Veri alınamadı: {symbol} {timeframe}")
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

        # Crossover + pullback + diğer şartlar
        lookback_cross = LOOKBACK_CROSSOVER
        ema13_slice = df['ema13'].values[-lookback_cross-1 : -1]
        sma34_slice = df['sma34'].values[-lookback_cross-1 : -1]
        price_slice = df['close'].values[-lookback_cross-1 : -1]
        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        for i in range(1, min(lookback_cross + 1, len(ema13_slice))):
            if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and \
               price_slice[-i] > sma34_slice[-i]:
                ema_sma_crossover_buy = True
            if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and \
               price_slice[-i] < sma34_slice[-i]:
                ema_sma_crossover_sell = True
        pullback_buy = closed_candle['close'] > closed_candle['ema13'] if pd.notna(closed_candle['close']) and pd.notna(closed_candle['ema13']) else False
        pullback_sell = closed_candle['close'] < closed_candle['ema13'] if pd.notna(closed_candle['close']) and pd.notna(closed_candle['ema13']) else False
        volume_multiplier = 1.0 + min(avg_atr_ratio * 3, 0.2) if np.isfinite(avg_atr_ratio) else 1.0
        volume_condition = closed_candle['volume'] > closed_candle['volume_sma20'] * volume_multiplier if pd.notna(closed_candle['volume']) and pd.notna(closed_candle['volume_sma20']) else False
        adx_ok = closed_candle['ADX'] > ADX_THRESHOLD if pd.notna(closed_candle['ADX']) else False
        di_buy = closed_candle['+DI'] > closed_candle['-DI'] if pd.notna(closed_candle['+DI']) and pd.notna(closed_candle['-DI']) else False
        di_sell = closed_candle['+DI'] < closed_candle['-DI'] if pd.notna(closed_candle['+DI']) and pd.notna(closed_candle['-DI']) else False
        smi_buy = closed_candle['squeeze_off'] and closed_candle['smi'] > 0 if pd.notna(closed_candle['smi']) and pd.notna(closed_candle['squeeze_off']) else False
        smi_sell = closed_candle['squeeze_off'] and closed_candle['smi'] < 0 if pd.notna(closed_candle['smi']) and pd.notna(closed_candle['squeeze_off']) else False

        logger.info(
            f"{symbol} {timeframe} | "
            f"CrossBuy={ema_sma_crossover_buy}, CrossSell={ema_sma_crossover_sell} | "
            f"PullBuy={pullback_buy}, PullSell={pullback_sell} | "
            f"VolCond={volume_condition} (mult={volume_multiplier:.2f}) | "
            f"SMI_Buy={smi_buy} (smi={closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}), SMI_Sell={smi_sell} | "
            f"ADX={closed_candle['ADX']:.2f if pd.notna(closed_candle['ADX']) else 'NaN'} (>25={adx_ok}) | "
            f"DI+={closed_candle['+DI']:.2f if pd.notna(closed_candle['+DI']) else 'NaN'}, DI-={closed_candle['-DI']:.2f if pd.notna(closed_candle['-DI']) else 'NaN'} | "
            f"BUY_OK={'YES' if (ema_sma_crossover_buy and pullback_buy and volume_condition and smi_buy and adx_ok and di_buy) else 'no'} | "
            f"SELL_OK={'YES' if (ema_sma_crossover_sell and pullback_sell and volume_condition and smi_sell and adx_ok and di_sell) else 'no'}"
        )

        buy_condition = ema_sma_crossover_buy and pullback_buy and volume_condition and smi_buy and adx_ok and di_buy
        sell_condition = ema_sma_crossover_sell and pullback_sell and volume_condition and smi_sell and adx_ok and di_sell

        # Pozisyon yönetimi (önce reversal check)
        current_pos = signal_cache.get(key, current_pos)
        current_price = float(df.iloc[-1]['close']) if pd.notna(df.iloc[-1]['close']) else np.nan
        tz = pytz.timezone('Europe/Istanbul')
        now = datetime.now(tz)

        if buy_condition or sell_condition:
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] is not None and current_pos['signal'] != new_signal:
                # Reversal close
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                    message_type = "LONG REVERSAL CLOSE 🚀" if profit_percent > 0 else "LONG REVERSAL STOP 📉"
                    profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                    message_type = "SHORT REVERSAL CLOSE 🚀" if profit_percent > 0 else "SHORT REVERSAL STOP 📉"
                    profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"
                message = (
                    f"{symbol} {timeframe}: {message_type}\n"
                    f"Price: {current_price:.4f}\n"
                    f"SMI: {closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}\n"
                    f"{profit_text}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı (reversal)\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Reversal Close: {message}")
                # Reset state
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'trailing_activated': False, 'avg_atr_ratio': None, 'trailing_distance': None,
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
                logger.info(message)
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                sl_price = entry_price - (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value) if np.isfinite(atr_value) else np.nan
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    message = f"{symbol} {timeframe}: BUY sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nPotansiyel SL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                    logger.info(message)
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                    trailing_distance = (TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE) if np.isfinite(avg_atr_ratio) else TRAILING_DISTANCE_BASE
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
                        f"SMI: {closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}\n"
                        f"SMI: Squeeze Off, Positive\n"
                        f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(f"Sinyal: {message}")
        elif sell_condition and current_pos['signal'] != 'sell':
            if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'sell' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                message = f"{symbol} {timeframe}: SELL sinyali atlanıyor (duplicate, cooldown: {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}"
                logger.info(message)
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                sl_price = entry_price + (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value) if np.isfinite(atr_value) else np.nan
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    message = f"{symbol} {timeframe}: SELL sinyali atlanıyor (anında SL riski) 🚫\nCurrent: {current_price:.4f}\nPotansiyel SL: {sl_price:.4f}\nTime: {now.strftime('%H:%M:%S')}"
                    logger.info(message)
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                    trailing_distance = (TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE) if np.isfinite(avg_atr_ratio) else TRAILING_DISTANCE_BASE
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
                        f"SMI: {closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}\n"
                        f"SMI: Squeeze Off, Negative\n"
                        f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP1: {tp1_price:.4f}\nTP2: {tp2_price:.4f}\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(f"Sinyal: {message}")

        # Pozisyon yönetimi
        if current_pos['signal'] == 'buy':
            atr_value, _ = get_atr_values(df, LOOKBACK_ATR)
            if not np.isfinite(atr_value):
                logger.warning(f"ATR NaN ({symbol} {timeframe}), skip.")
                return
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            td = current_pos['trailing_distance']
            if (current_price >= current_pos['entry_price'] + (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']):
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"Entry Price: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"Vol Ratio: {current_pos['avg_atr_ratio']:.4f if pd.notna(current_pos['avg_atr_ratio']) else 'NaN'}\n"
                    f"TSL Distance: {td}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Trailing Activated: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
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
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP1 Hit: {message}")
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
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
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP2 Hit: {message}")
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: LONG 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"SMI: {closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP LONG 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"SMI: {closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'trailing_activated': False, 'avg_atr_ratio': None, 'trailing_distance': None,
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
                logger.warning(f"ATR NaN ({symbol} {timeframe}), skip.")
                return
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            td = current_pos['trailing_distance']
            if (current_price <= current_pos['entry_price'] - (TRAILING_ACTIVATION * atr_value) and not current_pos['trailing_activated']):
                current_pos['trailing_activated'] = True
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"Entry Price: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"Vol Ratio: {current_pos['avg_atr_ratio']:.4f if pd.notna(current_pos['avg_atr_ratio']) else 'NaN'}\n"
                    f"TSL Distance: {td}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Trailing Activated: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
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
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP1 Hit: {message}")
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
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
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"TP2 Hit: {message}")
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: SHORT 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"SMI: {closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP SHORT 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"SMI: {closed_candle['smi']:.2f if pd.notna(closed_candle['smi']) else 'NaN'}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'trailing_activated': False, 'avg_atr_ratio': None, 'trailing_distance': None,
                    'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                return
            signal_cache[key] = current_pos

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