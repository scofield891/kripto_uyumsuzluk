# bot.py
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
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logging.handlers import RotatingFileHandler

# ================== Sabit Değerler ==================
# Güvenlik: ENV zorunlu
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("BOT_TOKEN ve CHAT_ID ortam değişkenlerini ayarla.")

TEST_MODE = False

# ---- Sinyal / Risk Parametreleri ----
LOOKBACK_ATR = 18

SL_MULTIPLIER = 1.8            # ATR x
TP_MULTIPLIER1 = 2.0           # ATR x  (TP1)
TP_MULTIPLIER2 = 3.5           # ATR x  (TP2)
SL_BUFFER = 0.3                # ATR x (SL'e ilave) -> toplam 2.1 ATR

COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05       # ATR x
LOOKBACK_CROSSOVER = 30
LOOKBACK_SMI = 20

ADX_PERIOD = 14
ADX_THRESHOLD = 18             # >= 18
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
SMI_LIGHT_NORM_MAX = 0.5       # |SMI/ATR| < 0.5 "light"

# === ADX sinyal modu: "2of3" (önerilen) ===
SIGNAL_MODE = "2of3"  # (ADX>=18, ADX rising, DI yönü) en az 2 doğruysa OK

# === Hibrit ATR + yüzde sınırları ===
# SL için clamp
MIN_SL_PCT = 0.006   # %0.6
MAX_SL_PCT = 0.030   # %3.0
# TP için clamp
MIN_TP_PCT = 0.008   # %0.8   (taban)
MAX_TP_PCT = 0.040   # %4.0   (tavan)

# === Crossover exit histerezis ===
# EMA-SMA farkı en az şu kadar ATR olsun; mikro çaprazlarda whipsaw'ı azaltır
EXIT_HYST_ATR = 0.15

# ---- Rate-limit & tarama pacing ----
MAX_CONCURRENT_FETCHES = 4
RATE_LIMIT_MS = 200
N_SHARDS = 5
BATCH_SIZE = 10
INTER_BATCH_SLEEP = 5.0

# ---- Sembol keşif ayarları ----
LINEAR_ONLY = True
QUOTE_WHITELIST = ("USDT",)

# ---- (Opsiyonel) Likidite filtresi ----
USE_LIQ_FILTER = False
LIQ_ROLL_BARS = 60
LIQ_QUANTILE  = 0.70
LIQ_MIN_DVOL_USD = 0

# ---- Spike Bull-Trap filtresi (özellikle LONG'lar) ----
USE_VOL_SPIKE_TRAP = True
VOL_MA_WINDOW = 20            # klasik vol MA
SPIKE_WINDOW = 60             # robust istatistik pencere (Median/MAD)
SPIKE_Z = 3.0                 # robust z-score eşiği
SPIKE_MULTI = 2.0             # alternatif: vol >= 2x vol_ma
PRELOW_BARS = 5               # önceki düşük hacimli sürünme
PRELOW_MULT = 0.9             # avg(prev) <= 0.9*vol_ma
UPPER_WICK_MIN = 0.45         # üst fitil/tüm aralık min
BODY_TO_RANGE_MAX = 0.50      # gövde/tüm aralık max

# ---- Bull-trap yumuşatma (kaçırmamak için) ----
BODY_TO_RANGE_BREAKOUT_MIN = 0.60   # gövde/tüm aralık >= 0.60 ise trap BYPASS
FOLLOW_THROUGH_LOOKAHEAD   = 2      # trap'ten sonra 1-2 bar takip penceresi
FOLLOW_THROUGH_WICK_MAX    = 0.35   # follow-through barında üst fitil tavan
FOLLOW_THROUGH_VOL_Z_MIN   = 1.5    # follow-through için vol z-score alt limiti

# ---- Bull-trap için BB/KC kapısı (opsiyonel) ----
USE_TRAP_BBKC_GATE = True
BB_UPPER_PROX = 0.20          # BB üst banda göre yakınlık eşiği (0→banda yapışık)
KC_REQUIRE_ABOVE = False      # True yaparsan close >= kc_upper şartı eklenir

# ================== Logging ==================
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler('bot.log', maxBytes=5_000_000, backupCount=3)
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

def configure_exchange_session(exchange, pool=50):
    """Bybit HTTP session'ı: bağlantı havuzu + retry."""
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool,
        pool_maxsize=pool,
        max_retries=Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    )
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    exchange.session = s

configure_exchange_session(exchange, pool=50)

telegram_bot = telegram.Bot(
    token=BOT_TOKEN,
    request=telegram.request.HTTPXRequest(connection_pool_size=20, pool_timeout=30.0)
)

# ================== Global State ==================
signal_cache = {}
message_queue = asyncio.Queue(maxsize=1000)
_fetch_sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
_rate_lock = asyncio.Lock()
_last_call_ts = 0.0

# ================== Util ==================
def clamp(x, lo, hi):
    return max(lo, min(hi, x))

# ================== Mesaj Kuyruğu ==================
async def enqueue_message(text: str):
    try:
        message_queue.put_nowait(text)
    except asyncio.QueueFull:
        logger.warning("Mesaj kuyruğu dolu, mesaj düşürüldü.")

async def message_sender():
    while True:
        message = await message_queue.get()
        try:
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            await asyncio.sleep(1)
        except (telegram.error.RetryAfter, telegram.error.TimedOut) as e:
            wait_time = getattr(e, 'retry_after', 5) + 2
            logger.warning(f"Telegram: RetryAfter, {wait_time-2}s bekle")
            await asyncio.sleep(wait_time)
            await enqueue_message(message)
        except Exception as e:
            logger.error(f"Telegram mesaj hatası: {str(e)}")
        message_queue.task_done()

# ================== Rate-limit Dostu Fetch ==================
async def fetch_ohlcv_async(symbol, timeframe, limit):
    """Bybit rate-limit dostu, retry'li OHLCV çekimi."""
    global _last_call_ts
    for attempt in range(4):
        try:
            async with _fetch_sem:
                # GLOBAL throttle
                async with _rate_lock:
                    now = asyncio.get_event_loop().time()
                    wait = max(0.0, (_last_call_ts + RATE_LIMIT_MS/1000.0) - now)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    _last_call_ts = asyncio.get_event_loop().time()
                # gerçek çağrı (thread'e atıyoruz)
                return await asyncio.to_thread(exchange.fetch_ohlcv, symbol, timeframe, None, limit)
        except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
            backoff = (2 ** attempt) * 1.5
            logger.warning(f"Rate limit {symbol} {timeframe}, backoff {backoff:.1f}s ({e.__class__.__name__})")
            await asyncio.sleep(backoff)
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            backoff = 1.0 + attempt
            logger.warning(f"Network/Timeout {symbol} {timeframe}, retry in {backoff:.1f}s ({e.__class__.__name__})")
            await asyncio.sleep(backoff)
    raise ccxt.NetworkError(f"fetch_ohlcv failed after retries: {symbol} {timeframe}")

# ================== Sembol Keşfi (Bybit) ==================
async def discover_bybit_symbols(linear_only=True, quote_whitelist=("USDT",)):
    markets = await asyncio.to_thread(exchange.load_markets)
    syms = []
    for s, m in markets.items():
        if not m.get('active', True):
            continue
        if not m.get('swap', False):
            continue
        if linear_only and not m.get('linear', False):
            continue
        if m.get('quote') not in quote_whitelist:
            continue
        syms.append(s)  # örn: "BTC/USDT:USDT"
    syms = sorted(set(syms))
    logger.info(f"Keşfedilen sembol sayısı: {len(syms)} (linear={linear_only}, quotes={quote_whitelist})")
    return syms

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
    alpha = 1.0 / period  # Wilder
    tr_ema = df['TR'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    df['di_plus'] = 100 * (df['+DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    df['di_minus'] = 100 * (df['-DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    df['DX'] = 100 * np.abs(df['di_plus'] - df['di_minus']) / (df['di_plus'] + df['di_minus']).replace(0, np.nan).fillna(0)
    df['adx'] = df['DX'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    adx_condition = df['adx'].iloc[-2] >= ADX_THRESHOLD if pd.notna(df['adx'].iloc[-2]) else False
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

# === Wick/Gövde ölçümü ===
def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng

# === Robust hacim istatistikleri + vol_ma ===
def calculate_obv_and_volma(df, vol_ma_window=VOL_MA_WINDOW):
    close = df['close'].values
    vol = df['volume'].values
    obv = np.zeros_like(close, dtype=float)
    for i in range(1, len(close)):
        if close[i] > close[i-1]:
            obv[i] = obv[i-1] + vol[i]
        elif close[i] < close[i-1]:
            obv[i] = obv[i-1] - vol[i]
        else:
            obv[i] = obv[i-1]
    df['obv'] = obv
    df['vol_ma'] = pd.Series(vol, index=df.index, dtype="float64").rolling(vol_ma_window).mean()

    vol_s = pd.Series(vol, index=df.index, dtype="float64")
    df['vol_med'] = vol_s.rolling(SPIKE_WINDOW).median()
    df['vol_mad'] = vol_s.rolling(SPIKE_WINDOW).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True
    )
    denom = (1.4826 * df['vol_mad']).replace(0, np.nan)  # MAD -> sigma
    df['vol_z'] = (vol_s - df['vol_med']) / denom
    return df

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
    # Bull-trap filtresi için hacim kolonları
    df = calculate_obv_and_volma(df, vol_ma_window=VOL_MA_WINDOW)
    return (
        df,
        df['squeeze_off'].iloc[-2],
        df['smi'].iloc[-2],
        'green' if df['smi'].iloc[-2] > 0 else 'red' if df['smi'].iloc[-2] < 0 else 'gray',
        adx_condition, di_condition_long, di_condition_short
    )

# ================== Likidite Filtresi ==================
def liquidity_ok(df: pd.DataFrame) -> bool:
    if not USE_LIQ_FILTER:
        return True
    if len(df) < LIQ_ROLL_BARS + 2:
        return False
    dv = (df['close'] * df['volume']).astype(float)  # yaklaşık $ hacim
    roll = dv.rolling(LIQ_ROLL_BARS, min_periods=LIQ_ROLL_BARS)
    q = roll.apply(lambda x: np.nanquantile(x, LIQ_QUANTILE), raw=True)
    ok_q = bool(dv.iloc[-2] >= q.iloc[-2]) if pd.notna(q.iloc[-2]) else False
    ok_min = True if LIQ_MIN_DVOL_USD <= 0 else bool(dv.iloc[-2] >= LIQ_MIN_DVOL_USD)
    return ok_q and ok_min

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe='4h'):
    tz = pytz.timezone('Europe/Istanbul')
    try:
        # --- Veri ---
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.02 * closes
            lows = closes - np.random.rand(200) * 0.02 * closes
            volumes = np.random.rand(200) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            logger.info(f"Test modu: {symbol} {timeframe}")
        else:
            ohlcv = await fetch_ohlcv_async(symbol, timeframe, limit=max(150, LOOKBACK_ATR + 80))
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            if df is None or df.empty or len(df) < 80:
                logger.warning(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), skip.")
                return

        # timestamp -> index
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
            df.set_index('timestamp', inplace=True)

        # --- İndikatörler ---
        df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short = calculate_indicators(df, symbol, timeframe)
        if df is None:
            return

        # --- Likidite ---
        liq_ok = liquidity_ok(df)

        # ATR değerleri (SMI normalize + risk)
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol} {timeframe}), skip.")
            return

        # SMI 'light' koşulu ATR ile normalize
        smi_raw = smi_histogram
        atr_for_norm = max(atr_value, 1e-9)
        smi_norm = (smi_raw / atr_for_norm) if np.isfinite(smi_raw) else np.nan
        smi_condition_long  = smi_squeeze_off and (smi_norm > 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX)
        smi_condition_short = smi_squeeze_off and (smi_norm < 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX)

        closed_candle = df.iloc[-2]
        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
        logger.info(f"{symbol} {timeframe} Closed:{closed_candle['close']:.4f} Cur:{current_price:.4f} | LIQ_OK={liq_ok}")

        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'avg_atr_ratio': None, 'remaining_ratio': 1.0,
            'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False,
            # --- trap state ---
            'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
        })

        # --- EMA13 / SMA34 Kesişimi (giriş için tarama) ---
        ema_arr = df['ema13'].to_numpy(dtype=np.float64)
        sma_arr = df['sma34'].to_numpy(dtype=np.float64)
        price_arr = df['close'].to_numpy(dtype=np.float64)
        window = max(0, min(LOOKBACK_CROSSOVER, len(ema_arr) - 1))
        if window == 0:
            logger.warning(f"Veri yetersiz (crossover) {symbol} {timeframe}, skip.")
            return

        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        for i in range(1, window + 1):
            pre = -i-1
            cur = -i
            if not (np.isfinite(ema_arr[pre]) and np.isfinite(sma_arr[pre]) and np.isfinite(ema_arr[cur]) and np.isfinite(sma_arr[cur]) and np.isfinite(price_arr[cur])):
                continue
            if ema_arr[pre] <= sma_arr[pre] and ema_arr[cur] > sma_arr[cur] and price_arr[cur] > sma_arr[cur]:
                ema_sma_crossover_buy = True
            if ema_arr[pre] >= sma_arr[pre] and ema_arr[cur] < sma_arr[cur] and price_arr[cur] < sma_arr[cur]:
                ema_sma_crossover_sell = True

        logger.info(f"{symbol} {timeframe} EMA/SMA buy:{ema_sma_crossover_buy} sell:{ema_sma_crossover_sell}")

        volume_multiplier = 1.0 + min(avg_atr_ratio * 3, 0.2) if np.isfinite(avg_atr_ratio) else 1.0
        volume_ok = closed_candle['volume'] > closed_candle['volume_sma20'] * volume_multiplier if pd.notna(closed_candle['volume']) and pd.notna(closed_candle['volume_sma20']) else False

        adx_value = f"{closed_candle['adx']:.2f}" if pd.notna(closed_candle['adx']) else 'NaN'
        adx_ok = adx_condition  # (>= 18)
        adx_rising = df['adx'].iloc[-2] > df['adx'].iloc[-3] if pd.notna(df['adx'].iloc[-3]) and pd.notna(df['adx'].iloc[-2]) else False
        di_long = di_condition_long
        di_short = di_condition_short

        # === ADX 2/3 kuralı ===
        if SIGNAL_MODE == "2of3":
            dir_long_ok  = (int(adx_ok) + int(adx_rising) + int(di_long))  >= 2
            dir_short_ok = (int(adx_ok) + int(adx_rising) + int(di_short)) >= 2
            str_ok = True
        else:
            dir_long_ok, dir_short_ok = di_long, di_short
            str_ok = adx_ok

        # === BULL-TRAP FİLTRESİ (sadece LONG) ===
        spike_trap_buy = False
        follow_through_ok = False

        # 1) body/wick oranları
        body_r, upper_wick_r, lower_wick_r = candle_body_wicks(closed_candle)

        # 2) Breakout BYPASS: büyük gövdeyse trap sayma
        breakout_bypass = (body_r >= BODY_TO_RANGE_BREAKOUT_MIN and upper_wick_r <= (1.0 - BODY_TO_RANGE_BREAKOUT_MIN))

        # 2.5) BB/KC GATE (opsiyonel): tuzağı nerede arayalım?
        gate_ok = True
        if USE_TRAP_BBKC_GATE:
            bb_up  = float(closed_candle.get('bb_upper', np.nan))
            bb_mid = float(closed_candle.get('bb_mid',   np.nan))
            kc_up  = float(closed_candle.get('kc_upper', np.nan))
            close_ = float(closed_candle['close'])

            prox_ok = False
            if np.isfinite(bb_up) and np.isfinite(bb_mid) and (bb_up - bb_mid) > 0:
                rel = (bb_up - close_) / (bb_up - bb_mid)  # 0 → banda yapışık
                prox_ok = (rel <= BB_UPPER_PROX)

            kc_ok = True
            if KC_REQUIRE_ABOVE and np.isfinite(kc_up):
                kc_ok = (close_ >= kc_up)

            gate_ok = bool(prox_ok and kc_ok)

        if USE_VOL_SPIKE_TRAP and gate_ok and not breakout_bypass:
            closed = closed_candle

            # Öncesi sürünme: PRELOW_BARS kapalı mumun öncesi
            pre_slice = df['volume'].iloc[-(PRELOW_BARS+2):-2]
            vol_ma = float(closed.get('vol_ma', np.nan)) if pd.notna(closed.get('vol_ma', np.nan)) else np.nan
            pre_ok = False
            if len(pre_slice) >= max(2, PRELOW_BARS - 1) and np.isfinite(vol_ma) and vol_ma > 0:
                pre_ok = (pre_slice.mean() <= PRELOW_MULT * vol_ma)

            # Hacim spike
            vol_now = float(closed['volume'])
            vol_z_val = closed.get('vol_z', np.nan)
            vol_z_ok = (pd.notna(vol_z_val) and np.isfinite(vol_z_val) and float(vol_z_val) >= SPIKE_Z)
            multi_ok = (np.isfinite(vol_ma) and vol_ma > 0 and vol_now >= SPIKE_MULTI * vol_ma)
            spike_ok = bool(vol_z_ok or multi_ok)

            # Mum morfolojisi
            wick_ok = (upper_wick_r >= UPPER_WICK_MIN) and (body_r <= BODY_TO_RANGE_MAX)

            spike_trap_buy = bool(pre_ok and spike_ok and wick_ok)

            # 3) Follow-through OVERRIDE
            pos = signal_cache.get(key, {})
            if pos.get('trap_active', False):
                in_window = True
                if pos.get('trap_expire_idx') is not None:
                    in_window = (df.index[-2] <= pos['trap_expire_idx'])
                ft_vol_z = float(vol_z_val) if (pd.notna(vol_z_val) and np.isfinite(vol_z_val)) else np.nan
                if (in_window and
                    pd.notna(pos.get('trap_high')) and
                    closed['close'] > float(pos['trap_high']) and
                    body_r >= BODY_TO_RANGE_BREAKOUT_MIN and
                    upper_wick_r <= FOLLOW_THROUGH_WICK_MAX and
                    (pd.isna(ft_vol_z) or ft_vol_z >= FOLLOW_THROUGH_VOL_Z_MIN)):
                    follow_through_ok = True
                    spike_trap_buy = False
                    pos['trap_active'] = False
                    pos['trap_high'] = None
                    pos['trap_expire_idx'] = None
                    signal_cache[key] = pos

            vz = float(vol_z_val) if (pd.notna(vol_z_val) and np.isfinite(vol_z_val)) else float('nan')
            logger.info(f"{symbol} {timeframe} | TRAP={spike_trap_buy} BYPASS={breakout_bypass} FTHRU={follow_through_ok} GATE={gate_ok} "
                        f"(pre_ok={pre_ok}, spike_ok={spike_ok}, wick_ok={wick_ok}, vol_z={vz:.2f})")
        else:
            logger.info(f"{symbol} {timeframe} | TRAP SKIP (gate_ok={gate_ok}, bypass={breakout_bypass}) body={body_r:.2f} wickU={upper_wick_r:.2f}")

        # 4) Trap yakalandıysa state'e kaydet (follow-through için)
        if spike_trap_buy:
            bar_delta = (df.index[-2] - df.index[-3]) if len(df.index) >= 3 else pd.Timedelta(0)
            signal_cache[key] = {
                **current_pos,
                'trap_active': True,
                'trap_high': float(closed_candle['high']),
                'trap_expire_idx': df.index[-2] + bar_delta * FOLLOW_THROUGH_LOOKAHEAD
            }
        else:
            signal_cache[key] = {**current_pos, **signal_cache.get(key, {})}

        # === BUY/SELL giriş koşulları ===
        buy_condition = (
            liq_ok and
            ema_sma_crossover_buy and volume_ok and smi_condition_long and
            str_ok and dir_long_ok and
            (closed_candle['close'] > closed_candle['ema13'] and closed_candle['close'] > closed_candle['sma34']) and
            (not spike_trap_buy)
        )
        sell_condition = (
            liq_ok and
            ema_sma_crossover_sell and volume_ok and smi_condition_short and
            str_ok and dir_short_ok and
            (closed_candle['close'] < closed_candle['ema13'] and closed_candle['close'] < closed_candle['sma34'])
        )

        logger.info(f"{symbol} {timeframe} ADX:{adx_value} adx_ok:{adx_ok} rising:{adx_rising} di_long:{di_long} di_short:{di_short}")
        logger.info(f"{symbol} {timeframe} buy:{buy_condition} sell:{sell_condition}")

        current_pos = signal_cache.get(key, current_pos)
        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
        now = datetime.now(tz)
        smi_value = f"{closed_candle['smi']:.2f}" if pd.notna(closed_candle['smi']) else 'NaN'

        if buy_condition and sell_condition:
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return

        # === Reversal kapama (zıt sinyal doğarsa) ===
        if (buy_condition or sell_condition) and (current_pos['signal'] is not None):
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] != new_signal:
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                message_type = "REVERSAL CLOSE 🔁"
                profit_text = f"P/L: {profit_percent:.2f}%"
                await enqueue_message(
                    f"{symbol} {timeframe}: {message_type}\n"
                    f"Price: {current_price:.4f}\n"
                    f"{profit_text}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı (reversal)\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                current_pos = signal_cache[key]

        # === Pozisyon aç — BUY ===
        if buy_condition and current_pos['signal'] != 'buy':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'buy')
            )
            if cooldown_active:
                await enqueue_message(f"{symbol} {timeframe}: BUY atlandı (cooldown {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan

                # ATR'yi yüzdeye normalize et (entry'ye göre)
                atr_ratio = (atr_value / entry_price) if (np.isfinite(atr_value) and np.isfinite(entry_price) and entry_price > 0) else np.nan

                # SL: 2.1*ATR_ratio -> clamp
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER  # 2.1 ATR
                sl_pct = clamp(eff_sl_mult * atr_ratio, MIN_SL_PCT, MAX_SL_PCT)
                sl_price = entry_price * (1 - sl_pct)

                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    await enqueue_message(f"{symbol} {timeframe}: BUY atlandı (anında SL riski) 🚫\nCur:{current_price:.4f}\nSL:{sl_price:.4f}\nTime:{now.strftime('%H:%M:%S')}")
                else:
                    # TP'ler: multiplier * ATR_ratio -> % clamp
                    tp1_pct = clamp(TP_MULTIPLIER1 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp2_pct = clamp(TP_MULTIPLIER2 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp1_price = entry_price * (1 + tp1_pct)
                    tp2_price = entry_price * (1 + tp2_pct)

                    current_pos = {
                        'signal': 'buy', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price,
                        'avg_atr_ratio': avg_atr_ratio, 'remaining_ratio': 1.0,
                        'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False,
                        'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                    }
                    signal_cache[key] = current_pos
                    await enqueue_message(
                        f"{symbol} {timeframe}: BUY (LONG) ✅\nSMI:{smi_value}\nADX:{adx_value}\n"
                        f"Entry:{entry_price:.4f}\nSL:{sl_price:.4f}\nTP1:{tp1_price:.4f}\nTP2:{tp2_price:.4f}\n"
                        f"Time:{now.strftime('%H:%M:%S')}"
                    )

        # === Pozisyon aç — SELL ===
        elif sell_condition and current_pos['signal'] != 'sell':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'sell')
            )
            if cooldown_active:
                await enqueue_message(f"{symbol} {timeframe}: SELL atlandı (cooldown {COOLDOWN_MINUTES} dk) 🚫\nTime: {now.strftime('%H:%M:%S')}")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan

                # ATR'yi yüzdeye normalize et (entry'ye göre)
                atr_ratio = (atr_value / entry_price) if (np.isfinite(atr_value) and np.isfinite(entry_price) and entry_price > 0) else np.nan

                # SL: 2.1*ATR_ratio -> clamp
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_pct = clamp(eff_sl_mult * atr_ratio, MIN_SL_PCT, MAX_SL_PCT)
                sl_price = entry_price * (1 + sl_pct)

                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    await enqueue_message(f"{symbol} {timeframe}: SELL atlandı (anında SL riski) 🚫\nCur:{current_price:.4f}\nSL:{sl_price:.4f}\nTime:{now.strftime('%H:%M:%S')}")
                else:
                    # TP'ler: multiplier * ATR_ratio -> % clamp
                    tp1_pct = clamp(TP_MULTIPLIER1 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp2_pct = clamp(TP_MULTIPLIER2 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp1_price = entry_price * (1 - tp1_pct)
                    tp2_price = entry_price * (1 - tp2_pct)

                    current_pos = {
                        'signal': 'sell', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price,
                        'avg_atr_ratio': avg_atr_ratio, 'remaining_ratio': 1.0,
                        'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False,
                        'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                    }
                    signal_cache[key] = current_pos
                    await enqueue_message(
                        f"{symbol} {timeframe}: SELL (SHORT) ✅\nSMI:{smi_value}\nADX:{adx_value}\n"
                        f"Entry:{entry_price:.4f}\nSL:{sl_price:.4f}\nTP1:{tp1_price:.4f}\nTP2:{tp2_price:.4f}\n"
                        f"Time:{now.strftime('%H:%M:%S')}"
                    )

        # === Pozisyon yönetimi: LONG ===
        if current_pos['signal'] == 'buy':
            # TP1
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']  # break-even
                current_pos['tp1_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\nCur:{current_price:.4f}\nTP1:{current_pos['tp1_price']:.4f}\n"
                    f"P/L:{profit_percent:.2f}%\n%30 kapandı, SL BE'ye alındı\n"
                    f"Kalan:%{current_pos['remaining_ratio']*100:.0f}\nTime:{datetime.now(tz).strftime('%H:%M:%S')}"
                )
            # TP2
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\nCur:{current_price:.4f}\nTP2:{current_pos['tp2_price']:.4f}\n"
                    f"P/L:{profit_percent:.2f}%\n%40 kapandı, kalan %30 açık\nTime:{datetime.now(tz).strftime('%H:%M:%S')}"
                )

            # SL tetik (artık trailing yok, klasik SL/BE)
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP LONG ⛔\n"
                    f"Price:{current_price:.4f}\nP/L: {profit_percent:.2f}%\n"
                    f"Time:{datetime.now(tz).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'], 'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            # === Crossover EXIT (LONG kapama): close < ema < sma + histerezis ===
            ema13 = float(df['ema13'].iloc[-2])
            sma34 = float(df['sma34'].iloc[-2])
            close_ = float(df['close'].iloc[-2])
            ema_sma_gap = abs(ema13 - sma34)
            exit_ok = (close_ < ema13 < sma34) and (ema_sma_gap >= EXIT_HYST_ATR * float(df['atr'].iloc[-2]))
            if exit_ok:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if (np.isfinite(current_price) and current_pos['entry_price']) else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: CROSS EXIT LONG 🔚\n"
                    f"Price:{current_price:.4f}\nP/L:{profit_percent:.2f}%\n"
                    f"EMA13:{ema13:.4f} < SMA34:{sma34:.4f} (gap {ema_sma_gap:.4f})\n"
                    f"Time:{datetime.now(tz).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'], 'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            signal_cache[key] = current_pos

        # === Pozisyon yönetimi: SHORT ===
        elif current_pos['signal'] == 'sell':
            # TP1
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']  # BE
                current_pos['tp1_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\nCur:{current_price:.4f}\nTP1:{current_pos['tp1_price']:.4f}\n"
                    f"P/L:{profit_percent:.2f}%\n%30 kapandı, SL BE'ye alındı\n"
                    f"Kalan:%{current_pos['remaining_ratio']*100:.0f}\nTime:{datetime.now(tz).strftime('%H:%M:%S')}"
                )
            # TP2
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\nCur:{current_price:.4f}\nTP2:{current_pos['tp2_price']:.4f}\n"
                    f"P/L:{profit_percent:.2f}%\n%40 kapandı, kalan %30 açık\nTime:{datetime.now(tz).strftime('%H:%M:%S')}"
                )

            # SL tetik
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP SHORT ⛔\n"
                    f"Price:{current_price:.4f}\nP/L: {profit_percent:.2f}%\n"
                    f"Time:{datetime.now(tz).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'], 'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            # === Crossover EXIT (SHORT kapama): close > ema > sma + histerezis ===
            ema13 = float(df['ema13'].iloc[-2])
            sma34 = float(df['sma34'].iloc[-2])
            close_ = float(df['close'].iloc[-2])
            ema_sma_gap = abs(ema13 - sma34)
            exit_ok = (close_ > ema13 > sma34) and (ema_sma_gap >= EXIT_HYST_ATR * float(df['atr'].iloc[-2]))
            if exit_ok:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if (np.isfinite(current_price) and current_pos['entry_price']) else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: CROSS EXIT SHORT 🔚\n"
                    f"Price:{current_price:.4f}\nP/L:{profit_percent:.2f}%\n"
                    f"EMA13:{ema13:.4f} > SMA34:{sma34:.4f} (gap {ema_sma_gap:.4f})\n"
                    f"Time:{datetime.now(tz).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'], 'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            signal_cache[key] = current_pos

    except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
        logger.warning(f"{symbol} {timeframe}: Rate limit -> skip this round ({e.__class__.__name__})")
        return
    except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
        logger.warning(f"{symbol} {timeframe}: Network/Timeout -> skip this round ({e.__class__.__name__})")
        return
    except Exception as e:
        logger.exception(f"Hata ({symbol} {timeframe}): {str(e)}")
        return

# ================== Main ==================
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    try:
        await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı 🟢 " + datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        logger.warning(f"Açılış mesajı gönderilemedi: {e}")

    asyncio.create_task(message_sender())
    timeframes = ['4h']

    # Bybit USDT linear perp sembollerini otomatik keşfet
    symbols = await discover_bybit_symbols(linear_only=LINEAR_ONLY, quote_whitelist=QUOTE_WHITELIST)
    if not symbols:
        raise RuntimeError("Uygun sembol bulunamadı. Permissions/region?")

    shard_index = 0
    while True:
        shard_symbols = [s for i, s in enumerate(symbols) if (i % N_SHARDS) == shard_index]
        logger.info(f"Shard {shard_index+1}/{N_SHARDS} -> {len(shard_symbols)} sembol taranacak")
        shard_index = (shard_index + 1) % N_SHARDS

        tasks = [check_signals(sym, tf) for tf in timeframes for sym in shard_symbols]
        for i in range(0, len(tasks), BATCH_SIZE):
            await asyncio.gather(*tasks[i:i+BATCH_SIZE])
            await asyncio.sleep(INTER_BATCH_SLEEP + random.random()*0.5)  # jitter

        logger.info("Tur bitti, 5 dk bekle...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
