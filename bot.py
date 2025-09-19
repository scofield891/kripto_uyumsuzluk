# bot.py
import ccxt
import numpy as np
import pandas as pd
import telegram
import logging
import asyncio
from datetime import datetime, timedelta
import time
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
SL_MULTIPLIER = 1.8 # SL = 1.8 x ATR
TP_MULTIPLIER1 = 2.0 # TP1 = 2.0 x ATR (satış %30)
TP_MULTIPLIER2 = 3.5 # TP2 = 3.5 x ATR (satış %40)
SL_BUFFER = 0.3 # ATR x (SL'e ilave buffer)
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05 # ATR x (entry anında SL'e çok yakınsa atla)
LOOKBACK_CROSSOVER = 30
LOOKBACK_SMI = 20
ADX_PERIOD = 14
ADX_THRESHOLD = 18 # >= 18
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
# ==== SMI Light (Adaptif + Slope teyidi + opsiyonel froth guard) ====
SMI_LIGHT_NORM_MAX = 0.75 # statik fallback/başlangıç
SMI_LIGHT_ADAPTIVE = True
SMI_LIGHT_PCTL = 0.65 # 0.60 agresif, 0.70 muhafazakar
SMI_LIGHT_MAX_MIN = 0.60 # adaptif alt sınır
SMI_LIGHT_MAX_MAX = 1.10 # adaptif üst sınır
SMI_LIGHT_REQUIRE_SQUEEZE = False # squeeze_off zorunlu değil
USE_SMI_SLOPE_CONFIRM = True # SMI eğimi yön teyidi
USE_FROTH_GUARD = False # fiyat EMA13'ten aşırı kopmuşsa sinyali pas geç
FROTH_GUARD_K_ATR = 0.9 # |close-ema13| <= K * ATR
# === ADX sinyal modu ===
SIGNAL_MODE = "2of3" # Üçlü: (ADX>=18, ADX rising, DI yönü). En az 2 doğruysa yön teyidi geçer.
# ---- Rate-limit & tarama pacing ----
MAX_CONCURRENT_FETCHES = 4
RATE_LIMIT_MS = 200
N_SHARDS = 5
BATCH_SIZE = 10
INTER_BATCH_SLEEP = 5.0
# ---- Sembol keşif ----
LINEAR_ONLY = True
QUOTE_WHITELIST = ("USDT",)
# ================== TRAP SKORLAMA ==================
USE_TRAP_SCORING = True # sadece puanlama; filtre YOK (kapı aşağıda)
SCORING_CTX_BARS = 3 # son 3 bar bağlam (wick/vol/RSI medyanı)
SCORING_WIN = 120 # persentil/z-score penceresi (bar)
# Ağırlıklar (toplam ~100)
W_WICK = 20.0 # wick/boy oranı
W_VOL = 20.0 # hacim z / vol_ma oranı
W_BBPROX = 15.0 # BB üst/alt banda yakınlık
W_ATRZ = 15.0 # ATR z-score
W_RSI = 15.0 # RSI aşırılık
W_MISC = 5.0 # ufak bağlam (ADX zayıf / squeeze vb.)
W_FB = 10.0 # Yeni: Fitil-Bias (yön aleyhine fitil => risk ↑, lehine fitil => risk ↓)
# RSI aşırılık seviyeleri
RSI_LONG_EXCESS = 70.0   # Klasik overbought
RSI_SHORT_EXCESS = 30.0  # Klasik oversold
# === Trap risk sinyal kapısı + çıktı formatı ===
TRAP_ONLY_LOW = True # True: sadece "Çok düşük / Düşük" risk sinyali gönder
TRAP_MAX_SCORE = 40.0 # 0-39 izinli (40 ve üstü = Orta/Yüksek/Aşırı -> blok)
# ==== Dinamik trap eşiği (ADX'e göre) ====
TRAP_DYN_USE = True
TRAP_BASE_MAX = 40.0 # TRAP_DYN_USE=False iken kullanılan sabit eşiğin karşılığı
TRAP_DYN_MIN = 35.0 # ADX çok zayıfken izin verilen max skor
TRAP_DYN_MAX = 48.0 # ADX güçlü iken izin verilen max skor
TRAP_ADX_LO = 14.0 # alt referans ADX
TRAP_ADX_HI = 30.0 # üst referans ADX
PRICE_DECIMALS = 5 # mesajlarda ondalık hane
def fmt(x, d=PRICE_DECIMALS):
    try:
        return f"{float(x):.{d}f}"
    except Exception:
        return str(x)
# TT mesaj etiketleri
def _risk_label(score: float) -> str:
    if score < 20: return "Çok düşük risk 🟢"
    if score < 40: return "Düşük risk 🟢"
    if score < 60: return "Orta risk ⚠️"
    if score < 80: return "Yüksek risk 🟠"
    return "Aşırı risk 🔴"
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
def pct_rank(series: pd.Series, value: float) -> float:
    s = series.dropna()
    if not np.isfinite(value) or s.empty:
        return 0.0
    return float((s < value).mean())
def rolling_z(series: pd.Series, win: int) -> float:
    s = series.tail(win).astype(float)
    if s.size < 5 or s.std(ddof=0) == 0 or not np.isfinite(s.iloc[-1]):
        return 0.0
    return float((s.iloc[-1] - s.mean()) / (s.std(ddof=0) + 1e-12))
# ---- Dinamik trap eşiği yardımcı fonksiyonu ----
def trap_threshold_by_adx(adx_value: float) -> float:
    """ADX'e göre low-risk barajını [TRAP_DYN_MIN .. TRAP_DYN_MAX] arasında ölçekle."""
    if not TRAP_DYN_USE or not np.isfinite(adx_value):
        return TRAP_BASE_MAX if TRAP_BASE_MAX is not None else TRAP_MAX_SCORE
    a = clamp((adx_value - TRAP_ADX_LO) / max(TRAP_ADX_HI - TRAP_ADX_LO, 1e-9), 0.0, 1.0)
    return TRAP_DYN_MIN + a * (TRAP_DYN_MAX - TRAP_DYN_MIN)
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
    global _last_call_ts
    for attempt in range(4):
        try:
            async with _fetch_sem:
                async with _rate_lock:
                    now = asyncio.get_event_loop().time()
                    wait = max(0.0, (_last_call_ts + RATE_LIMIT_MS/1000.0) - now)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    _last_call_ts = asyncio.get_event_loop().time()
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
        if not m.get('active', True): continue
        if not m.get('swap', False): continue
        if linear_only and not m.get('linear', False): continue
        if m.get('quote') not in quote_whitelist: continue
        syms.append(s) # "BTC/USDT:USDT"
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
    arr = np.full(len(closes), np.nan, dtype=np.float64)
    for i in range(period-1, len(closes)):
        arr[i] = np.mean(closes[i-period+1:i+1])
    return arr
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
def calculate_adx(df, symbol, period=ADX_PERIOD):
    df['high_diff'] = df['high'] - df['high'].shift(1)
    df['low_diff'] = df['low'].shift(1) - df['low']
    df['+DM'] = np.where((df['high_diff'] > df['low_diff']) & (df['high_diff'] > 0), df['high_diff'], 0)
    df['-DM'] = np.where((df['low_diff'] > df['high_diff']) & (df['low_diff'] > 0), df['low_diff'], 0)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    df['TR'] = np.maximum(high_low, np.maximum(high_close, low_close))
    alpha = 1.0 / period # Wilder smoothing
    tr_ema = df['TR'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    df['di_plus'] = 100 * (df['+DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    df['di_minus'] = 100 * (df['-DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    denom = (df['di_plus'] + df['di_minus']).clip(lower=1e-9)
    df['DX'] = (100 * (df['di_plus'] - df['di_minus']).abs() / denom).fillna(0)
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
# --- VOL & OBV + robust z (vol_z) ---
def calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60):
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
    df['vol_med'] = vol_s.rolling(spike_window).median()
    df['vol_mad'] = vol_s.rolling(spike_window).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True
    )
    denom = (1.4826 * df['vol_mad']).replace(0, np.nan) # MAD -> sigma
    df['vol_z'] = (vol_s - df['vol_med']) / denom
    return df
def get_atr_values(df, lookback_atr=LOOKBACK_ATR):
    df = ensure_atr(df, period=14)
    if len(df) < lookback_atr + 2:
        return np.nan, np.nan
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1].dropna()
    close_last = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
    if atr_series.empty or not np.isfinite(close_last) or close_last == 0:
        return np.nan, np.nan
    atr_value = float(df['atr'].iloc[-2]) if pd.notna(df['atr'].iloc[-2]) else np.nan
    avg_atr_ratio = float(atr_series.mean() / close_last)
    return atr_value, avg_atr_ratio
def calculate_indicators(df, symbol, timeframe):
    if len(df) < 80:
        logger.warning(f"DF çok kısa ({len(df)}), indikatör hesaplanamadı.")
        return None, None, None, None, None, None, None
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
        df.set_index('timestamp', inplace=True)
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, 13)
    df['sma34'] = calculate_sma(closes, 34)
    df['rsi'] = calculate_rsi(closes, 14)
    df = calculate_bb(df)
    df = calculate_kc(df)
    df = calculate_squeeze(df)
    df = calculate_smi_momentum(df)
    df, adx_condition, di_condition_long, di_condition_short = calculate_adx(df, symbol)
    df = ensure_atr(df, period=14)
    df = calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60)
    return df, df['squeeze_off'].iloc[-2], df['smi'].iloc[-2], 'green' if df['smi'].iloc[-2] > 0 else 'red' if df['smi'].iloc[-2] < 0 else 'gray', adx_condition, di_condition_long, di_condition_short
# ========= ADX Rising (k=5 ana test + k=3 hibrit hızlı test) =========
ADX_RISE_K = 5
ADX_RISE_MIN_NET = 1.0
ADX_RISE_POS_RATIO = 0.6
ADX_RISE_EPS = 0.0
ADX_RISE_USE_HYBRID = True # k=3 hızlı test açık
def adx_rising_strict(df_adx: pd.Series) -> bool:
    if df_adx is None or len(df_adx) < ADX_RISE_K + 1:
        return False
    window = df_adx.iloc[-(ADX_RISE_K+1):-1].astype(float)
    if window.isna().any():
        return False
    x = np.arange(len(window))
    slope, _ = np.polyfit(x, window.values, 1)
    diffs = np.diff(window.values)
    pos_ratio = (diffs > ADX_RISE_EPS).mean() if diffs.size > 0 else 0.0
    net = window.iloc[-1] - window.iloc[0]
    return (slope > 0) and (net >= ADX_RISE_MIN_NET) and (pos_ratio >= ADX_RISE_POS_RATIO)
def adx_rising_hybrid(df_adx: pd.Series) -> bool:
    if not ADX_RISE_USE_HYBRID:
        return False
    if df_adx is None or len(df_adx) < 4:
        return False
    window = df_adx.iloc[-4:-1].astype(float)
    if window.isna().any():
        return False
    x = np.arange(len(window))
    slope, _ = np.polyfit(x, window.values, 1)
    last_diff = window.values[-1] - window.values[-2]
    return (slope > 0) and (last_diff > ADX_RISE_EPS)
def adx_rising(df: pd.DataFrame) -> bool:
    if 'adx' not in df.columns:
        return False
    return adx_rising_strict(df['adx']) or adx_rising_hybrid(df['adx'])
# ================== TRAP SKORLAMA HESABI ==================
def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng
def _fitil_bias_sig(last_row, ctx_u_median, ctx_l_median, side="long", deadband=0.05, scale=0.50):
    body, u, l = candle_body_wicks(last_row)  # oranlar 0..1
    u_ref = max(ctx_u_median, 1e-6)
    l_ref = max(ctx_l_median, 1e-6)
    u_adj = u / u_ref
    l_adj = l / l_ref
    if side == "long":
        diff = (u_adj - l_adj)
    else:
        diff = (l_adj - u_adj)
    if abs(diff) <= deadband:
        return 0.0
    if diff < 0:
        return 0.0
    return clamp(diff / scale, 0.0, 1.0)
def compute_trap_scores(df: pd.DataFrame, side: str = "long") -> dict:
    try:
        ctx = df.iloc[-(SCORING_CTX_BARS+1):-1]
        last = df.iloc[-2]
        body_u_l = ctx.apply(candle_body_wicks, axis=1, result_type='expand')
        body_ctx = float(body_u_l[0].median()) if not body_u_l.empty else 0.0
        upper_ctx = float(body_u_l[1].median()) if not body_u_l.empty else 0.0
        lower_ctx = float(body_u_l[2].median()) if not body_u_l.empty else 0.0
        wick_ctx = upper_ctx if side == "long" else lower_ctx
        vol_z_ctx = float(ctx['vol_z'].median()) if 'vol_z' in ctx else 0.0
        vol_ma = float(last.get('vol_ma', np.nan))
        vol_now = float(last['volume'])
        vol_ratio = (vol_now / vol_ma) if (np.isfinite(vol_ma) and vol_ma > 0) else 1.0
        vol_sig = np.tanh(max(0.0, vol_z_ctx) / 3.0)
        vol_sig = max(vol_sig, np.tanh(max(0.0, vol_ratio - 1.0)))
        if side == "long":
            num = float(last['close'] - last['bb_mid'])
            den = float(last['bb_upper'] - last['bb_mid'])
        else:
            num = float(last['bb_mid'] - last['close'])
            den = float(last['bb_mid'] - last['bb_lower'])
        bb_prox = clamp(num / (den + 1e-12), 0.0, 1.0) if np.isfinite(num) and np.isfinite(den) else 0.0
        atr_z = rolling_z(df['atr'], SCORING_WIN) if 'atr' in df else 0.0
        atr_sig = clamp((atr_z + 1.0) / 3.0, 0.0, 1.0)
        rsi_ctx = float(ctx['rsi'].median()) if 'rsi' in ctx else 50.0
        if side == "long":
            rsi_sig = clamp((rsi_ctx - RSI_LONG_EXCESS) / 20.0, 0.0, 1.0)
        else:
            rsi_sig = clamp((RSI_SHORT_EXCESS - rsi_ctx) / 20.0, 0.0, 1.0)
        misc = 0.0
        if 'adx' in last and np.isfinite(last['adx']) and last['adx'] < ADX_THRESHOLD:
            misc += 0.5
        if 'squeeze_on' in last and bool(last['squeeze_on']):
            misc += 0.5
        misc_sig = clamp(misc / 1.0, 0.0, 1.0)
        wick_sig = clamp((wick_ctx - 0.25) / 0.5, 0.0, 1.0)
        if len(df) >= SCORING_WIN:
            wick_ref = df.iloc[-(SCORING_WIN+1):-1].apply(candle_body_wicks, axis=1, result_type='expand')
            if not wick_ref.empty:
                uref = float(wick_ref[1].median()) if side == "long" else float(wick_ref[2].median())
                if np.isfinite(uref) and uref > 0:
                    wick_sig *= clamp(0.8 + 0.4 * (0.5 / (uref + 1e-9)), 0.5, 1.2)
        fb_sig = _fitil_bias_sig(last, upper_ctx, lower_ctx, side=side, deadband=0.05, scale=0.50)
        score = (
            W_WICK * wick_sig +
            W_VOL * vol_sig +
            W_BBPROX * bb_prox +
            W_ATRZ * atr_sig +
            W_RSI * rsi_sig +
            W_MISC * misc_sig +
            W_FB * fb_sig
        )
        score = float(clamp(score, 0.0, 100.0))
        return {"score": score, "label": _risk_label(score)}
    except Exception as e:
        logger.warning(f"compute_trap_scores hata: {e}")
        return {"score": 0.0, "label": _risk_label(0.0)}
# === Hacim Filtresi ===
VOLUME_GATE_MODE = "lite"  # "lite" (önerilen) | "full"
# Ortak parametreler
VOL_REF_WIN = 20
VOL_ATR_K = 3.5
VOL_ATR_CAP = 0.25
VOL_MIN_BASE = 1.05
# Likidite (yumuşatılmış/adaptif)
VOL_LIQ_USE = True
VOL_LIQ_ROLL = 60
VOL_LIQ_QUANTILE = 0.70
VOL_LIQ_MIN_DVOL_USD = 50_000
VOL_LIQ_MIN_DVOL_LO = 10_000
VOL_LIQ_MIN_DVOL_HI = 150_000
VOL_LIQ_MED_FACTOR = 0.40
# “İyi spike” liq baypas koşulu
LIQ_BYPASS_GOOD_SPIKE = True
GOOD_SPIKE_Z = 2.5
GOOD_BODY_MIN = 0.60
GOOD_UPWICK_MAX = 0.20
GOOD_DNWICK_MAX = 0.20
# FULL mod ek parametreler
VOL_Z_GOOD = 2.0
TRAP_WICK_MIN = 0.45
TRAP_BB_NEAR = 0.80
VOL_RELAX = 1.00
VOL_TIGHT = 1.10
OBV_SLOPE_WIN = 5
VOL_OBV_TIGHT = 1.00  # FULL’de istersen 1.08 yaparsın; şimdilik nötr
# Yardımcılar
def _bb_prox(last, side="long"):
    if side == "long":
        num = float(last['close'] - last['bb_mid']); den = float(last['bb_upper'] - last['bb_mid'])
    else:
        num = float(last['bb_mid'] - last['close']); den = float(last['bb_mid'] - last['bb_lower'])
    if not (np.isfinite(num) and np.isfinite(den)) or den <= 0: return 0.0
    return clamp(num/den, 0.0, 1.0)
def _obv_slope_recent(df: pd.DataFrame, win=OBV_SLOPE_WIN) -> float:
    s = df['obv'].iloc[-(win+1):-1].astype(float)
    if s.size < 3 or s.isna().any(): return 0.0
    x = np.arange(len(s)); m,_ = np.polyfit(x, s.values, 1)
    return float(m)
def _dynamic_liq_floor(dv_series: pd.Series) -> float:
    s = dv_series.astype(float).dropna()
    if s.size < VOL_LIQ_ROLL:
        return VOL_LIQ_MIN_DVOL_USD
    med = float(s.tail(VOL_LIQ_ROLL).median())
    dyn = med * VOL_LIQ_MED_FACTOR
    return clamp(dyn, VOL_LIQ_MIN_DVOL_LO, VOL_LIQ_MIN_DVOL_HI)
MAJOR_SYMBOLS = {"BTC/USDT:USDT", "ETH/USDT:USDT"}
MAJOR_LIQ_MIN = 100_000
def volume_gate(df: pd.DataFrame, side: str, atr_ratio: float, symbol: str = "") -> (bool, str):
    if len(df) < max(VOL_LIQ_ROLL+2, VOL_REF_WIN+2):
        return False, "data_short"
    last = df.iloc[-2]
    vol = float(last['volume']); close = float(last['close'])
    dvol_usd = vol * close
    # 0) Likidite (yumuşak + adaptif)
    if VOL_LIQ_USE and not TEST_MODE:
        dv = (df['close'] * df['volume']).astype(float)
        roll = dv.rolling(VOL_LIQ_ROLL, min_periods=VOL_LIQ_ROLL)
        q = roll.apply(lambda x: np.nanquantile(x, VOL_LIQ_QUANTILE), raw=True)
        qv = float(q.iloc[-2]) if pd.notna(q.iloc[-2]) else 0.0
        dyn_min = _dynamic_liq_floor(dv)
        base = symbol.split('/')[0]
        is_major = base in {"BTC", "ETH"}
        if is_major:
            dyn_min = max(dyn_min, MAJOR_LIQ_MIN)
        min_required = max(qv, dyn_min)
        liq_bypass = False
        if LIQ_BYPASS_GOOD_SPIKE:
            vol_z = float(last.get('vol_z', np.nan))
            body, up, dn = candle_body_wicks(last)
            if np.isfinite(vol_z) and vol_z >= GOOD_SPIKE_Z:
                if (side == "long"  and body >= GOOD_BODY_MIN and up <= GOOD_UPWICK_MAX) or \
                   (side == "short" and body >= GOOD_BODY_MIN and dn <= GOOD_DNWICK_MAX):
                    liq_bypass = True
        if (dvol_usd < min_required) and not liq_bypass:
            return False, f"liq_gate dvol={dvol_usd:.0f} < min={min_required:.0f} (q{int(VOL_LIQ_QUANTILE*100)}={qv:.0f}, dyn={dyn_min:.0f})"
    # 1) Referans hacim (dolar bazlı, sade)
    dvol_ref = float((df['close']*df['volume']).rolling(VOL_REF_WIN).mean().iloc[-2])
    # 2) ATR-adaptif multiplier (taban +%5)
    mult = 1.0 + clamp(atr_ratio * VOL_ATR_K, 0.0, VOL_ATR_CAP) if np.isfinite(atr_ratio) else 1.0
    mult = max(mult, VOL_MIN_BASE)
    if VOLUME_GATE_MODE == "full":
        vol_z = float(last.get('vol_z', np.nan))
        body, up, low = candle_body_wicks(last)
        bbp = _bb_prox(last, side=side)
        good_spike = np.isfinite(vol_z) and (vol_z >= VOL_Z_GOOD)
        trap_like = ((up >= TRAP_WICK_MIN and side == "long") or
                     (low >= TRAP_WICK_MIN and side == "short") or
                     (bbp >= TRAP_BB_NEAR))
        if good_spike:
            if side == "long" and (body >= GOOD_BODY_MIN) and (up <= GOOD_UPWICK_MAX):
                mult = min(mult, VOL_RELAX)
            elif side == "short" and (body >= GOOD_BODY_MIN) and (low <= GOOD_DNWICK_MAX):
                mult = min(mult, VOL_RELAX)
            elif trap_like:
                mult *= VOL_TIGHT
        if VOL_OBV_TIGHT > 1.0:
            obv_m = _obv_slope_recent(df, win=OBV_SLOPE_WIN)
            if (side == "long" and obv_m <= 0) or (side == "short" and obv_m >= 0):
                mult *= VOL_OBV_TIGHT
    need = dvol_ref * mult
    ok = (dvol_usd > need)
    return (ok, f"dvol={dvol_usd:.0f} need>{need:.0f} (ref={dvol_ref:.0f}, mult={mult:.2f})")
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
            limit_need = max(150, LOOKBACK_ATR + 80, LOOKBACK_SMI + 40, ADX_PERIOD + 40, SCORING_WIN + 5)
            ohlcv = await fetch_ohlcv_async(symbol, timeframe, limit=limit_need)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            if df is None or df.empty or len(df) < 80:
                logger.warning(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), skip.")
                return
        calc = calculate_indicators(df, symbol, timeframe)
        if not calc or calc[0] is None:
            return
        df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short = calc
        liq_ok = liquidity_ok(df)
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol} {timeframe}), skip.")
            return
        smi_raw = smi_histogram
        atr_for_norm = max(atr_value, 1e-9)
        smi_norm = (smi_raw / atr_for_norm) if np.isfinite(smi_raw) else np.nan
        if SMI_LIGHT_ADAPTIVE:
            smi_norm_series = (df['smi'] / df['atr']).replace([np.inf, -np.inf], np.nan)
            ref = smi_norm_series.iloc[-(SCORING_WIN+1):-1].abs() if len(df) >= SCORING_WIN else smi_norm_series.abs()
            if ref.notna().sum() >= 20:
                pct_val = float(np.nanpercentile(ref, SMI_LIGHT_PCTL * 100))
                SMI_LIGHT_NORM_MAX_EFF = clamp(pct_val, SMI_LIGHT_MAX_MIN, SMI_LIGHT_MAX_MAX)
            else:
                SMI_LIGHT_NORM_MAX_EFF = SMI_LIGHT_NORM_MAX
        else:
            SMI_LIGHT_NORM_MAX_EFF = SMI_LIGHT_NORM_MAX
        if SMI_LIGHT_REQUIRE_SQUEEZE:
            base_long = smi_squeeze_off and (smi_norm > 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
            base_short = smi_squeeze_off and (smi_norm < 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
        else:
            base_long = (smi_norm > 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
            base_short = (smi_norm < 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
        if USE_SMI_SLOPE_CONFIRM and pd.notna(df['smi'].iloc[-3]) and pd.notna(df['smi'].iloc[-2]):
            smi_slope = float(df['smi'].iloc[-2] - df['smi'].iloc[-3])
            slope_ok_long = smi_slope > 0
            slope_ok_short = smi_slope < 0
        else:
            slope_ok_long = slope_ok_short = True
        if USE_FROTH_GUARD and pd.notna(df['ema13'].iloc[-2]):
            ema_gap = abs(float(df['close'].iloc[-2]) - float(df['ema13'].iloc[-2]))
            froth_ok = (ema_gap <= FROTH_GUARD_K_ATR * atr_for_norm)
        else:
            froth_ok = True
        smi_condition_long = base_long and slope_ok_long and froth_ok
        smi_condition_short = base_short and slope_ok_short and froth_ok
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
        adx_ok = adx_condition
        rising = adx_rising(df)
        di_long = di_condition_long
        di_short = di_condition_short
        if SIGNAL_MODE == "2of3":
            dir_long_ok = (int(adx_ok) + int(rising) + int(di_long)) >= 2
            dir_short_ok = (int(adx_ok) + int(rising) + int(di_short)) >= 2
            str_ok = True
        else:
            dir_long_ok, dir_short_ok = di_long, di_short
            str_ok = adx_ok
        ok_l, reason_l = volume_gate(df, side="long", atr_ratio=avg_atr_ratio, symbol=symbol)
        ok_s, reason_s = volume_gate(df, side="short", atr_ratio=avg_atr_ratio, symbol=symbol)
        logger.info(f"{symbol} {timeframe} VOL_LONG {ok_l} | {reason_l}")
        logger.info(f"{symbol} {timeframe} VOL_SHORT {ok_s} | {reason_s}")
        bull_score = compute_trap_scores(df, side="long") if USE_TRAP_SCORING else {"score": 0.0, "label": _risk_label(0.0)}
        bear_score = compute_trap_scores(df, side="short") if USE_TRAP_SCORING else {"score": 0.0, "label": _risk_label(0.0)}
        adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
        eff_trap_max = trap_threshold_by_adx(adx_last)
        logger.info(f"{symbol} {timeframe} trap_dyn_thr:{eff_trap_max:.2f}")
        trap_ok_long = (not TRAP_ONLY_LOW) or (bull_score["score"] < eff_trap_max)
        trap_ok_short = (not TRAP_ONLY_LOW) or (bear_score["score"] < eff_trap_max)
        closed_candle = df.iloc[-2]
        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
        is_green = pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] > closed_candle['open'])
        is_red = pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] < closed_candle['open'])
        buy_condition = (
            liq_ok and
            ema_sma_crossover_buy and ok_l and smi_condition_long and
            str_ok and dir_long_ok and trap_ok_long and is_green and
            (closed_candle['close'] > closed_candle['ema13'] and closed_candle['close'] > closed_candle['sma34'])
        )
        sell_condition = (
            liq_ok and
            ema_sma_crossover_sell and ok_s and smi_condition_short and
            str_ok and dir_short_ok and trap_ok_short and is_red and
            (closed_candle['close'] < closed_candle['ema13'] and closed_candle['close'] < closed_candle['sma34'])
        )
        logger.info(f"{symbol} {timeframe} EMA/SMA buy:{ema_sma_crossover_buy} sell:{ema_sma_crossover_sell}")
        logger.info(f"{symbol} {timeframe} ADX_ok:{adx_ok} rising:{rising} di_long:{di_long} di_short:{di_short}")
        logger.info(f"{symbol} {timeframe} buy:{buy_condition} sell:{sell_condition} riskL:{bull_score['label']} riskS:{bear_score['label']}")
        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
            'remaining_ratio': 1.0, 'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False
        })
        if buy_condition and sell_condition:
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return
        now = datetime.now(tz)
        ema_prev, sma_prev = df['ema13'].iloc[-3], df['sma34'].iloc[-3]
        ema_last, sma_last = df['ema13'].iloc[-2], df['sma34'].iloc[-2]
        exit_cross_long = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev >= sma_prev) and (ema_last < sma_last))
        exit_cross_short = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev <= sma_prev) and (ema_last > sma_last))
        logger.info(f"{symbol} {timeframe} exit_cross_long:{exit_cross_long} exit_cross_short:{exit_cross_short}")
        if (buy_condition or sell_condition) and (current_pos['signal'] is not None):
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] != new_signal:
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: REVERSAL CLOSE 🔁\n"
                    f"Price: {fmt(current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                current_pos = signal_cache[key]
        if buy_condition and current_pos['signal'] != 'buy':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'buy')
            )
            if cooldown_active:
                await enqueue_message(f"{symbol} {timeframe}: BUY atlandı (cooldown {COOLDOWN_MINUTES} dk) 🚫")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price - sl_atr_abs
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    await enqueue_message(f"{symbol} {timeframe}: BUY atlandı (anında SL riski) 🚫")
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                    trap_line = f"\nTrap Risk (Bull): {int(bull_score['score'])}/100 → {bull_score['label']}" if USE_TRAP_SCORING else ""
                    current_pos = {
                        'signal': 'buy', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': entry_price,
                        'lowest_price': None, 'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False
                    }
                    signal_cache[key] = current_pos
                    await enqueue_message(
                        f"{symbol} {timeframe}: BUY (LONG) 🚀\n"
                        f"Entry: {fmt(entry_price)}\n"
                        f"SL: {fmt(sl_price)}\n"
                        f"TP1: {fmt(tp1_price)}\n"
                        f"TP2: {fmt(tp2_price)}"
                        f"{trap_line}"
                    )
        elif sell_condition and current_pos['signal'] != 'sell':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'sell')
            )
            if cooldown_active:
                await enqueue_message(f"{symbol} {timeframe}: SELL atlandı (cooldown {COOLDOWN_MINUTES} dk) 🚫")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price + sl_atr_abs
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    await enqueue_message(f"{symbol} {timeframe}: SELL atlandı (anında SL riski) 🚫")
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                    trap_line = f"\nTrap Risk (Bear): {int(bear_score['score'])}/100 → {bear_score['label']}" if USE_TRAP_SCORING else ""
                    current_pos = {
                        'signal': 'sell', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': None,
                        'lowest_price': entry_price, 'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False
                    }
                    signal_cache[key] = current_pos
                    await enqueue_message(
                        f"{symbol} {timeframe}: SELL (SHORT) 📉\n"
                        f"Entry: {fmt(entry_price)}\n"
                        f"SL: {fmt(sl_price)}\n"
                        f"TP1: {fmt(tp1_price)}\n"
                        f"TP2: {fmt(tp2_price)}"
                        f"{trap_line}"
                    )
        if current_pos['signal'] == 'buy':
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\n"
                    f"Cur: {fmt(current_price)} | TP1: {fmt(current_pos['tp1_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi."
                )
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\n"
                    f"Cur: {fmt(current_price)} | TP2: {fmt(current_pos['tp2_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %40 kapandı, kalan %30 açık."
                )
            if exit_cross_long:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: EMA/SMA EXIT (LONG) 🔁\n"
                    f"Price: {fmt(current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'], 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                return
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP LONG ⛔\n"
                    f"Price: {fmt(current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'], 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                return
            signal_cache[key] = current_pos
        elif current_pos['signal'] == 'sell':
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\n"
                    f"Cur: {fmt(current_price)} | TP1: {fmt(current_pos['tp1_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi."
                )
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\n"
                    f"Cur: {fmt(current_price)} | TP2: {fmt(current_pos['tp2_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %40 kapandı, kalan %30 açık."
                )
            if exit_cross_short:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: EMA/SMA EXIT (SHORT) 🔁\n"
                    f"Price: {fmt(current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'], 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
                }
                return
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP SHORT ⛔\n"
                    f"Price: {fmt(current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'], 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False
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
    symbols = await discover_bybit_symbols(linear_only=LINEAR_ONLY, quote_whitelist=QUOTE_WHITELIST)
    if not symbols:
        raise RuntimeError("Uygun sembol bulunamadı. Permissions/region?")
    logger.info(f"Toplam sembol: {len(symbols)} | N_SHARDS={N_SHARDS} | BATCH_SIZE={BATCH_SIZE}")
    while True:
        loop_start = time.time()
        total_scanned = 0
        for shard_index in range(N_SHARDS):
            shard_symbols = [s for i, s in enumerate(symbols) if (i % N_SHARDS) == shard_index]
            total_scanned += len(shard_symbols)
            logger.info(f"Shard {shard_index+1}/{N_SHARDS} -> {len(shard_symbols)} sembol taranacak")
            tasks = [check_signals(sym, tf) for tf in timeframes for sym in shard_symbols]
            for i in range(0, len(tasks), BATCH_SIZE):
                await asyncio.gather(*tasks[i:i+BATCH_SIZE])
                await asyncio.sleep(INTER_BATCH_SLEEP + random.random()*0.5)
        elapsed = time.time() - loop_start
        sleep_sec = max(0.0, 120.0 - elapsed)
        logger.info(f"Tur bitti, {total_scanned} sembol tarandı, {elapsed:.1f}s sürdü, {sleep_sec:.1f}s bekle...")
        await asyncio.sleep(sleep_sec)
if __name__ == "__main__":
    asyncio.run(main())
