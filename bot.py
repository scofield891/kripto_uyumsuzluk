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

# ---- Sinyal / Risk Parametreleri (SİSTEM AYNI) ----
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8            # SL = 1.8 x ATR
TP_MULTIPLIER1 = 2.0           # TP1 = 2.0 x ATR  (satış %30)
TP_MULTIPLIER2 = 3.5           # TP2 = 3.5 x ATR  (satış %40)
SL_BUFFER = 0.3                # ATR x (SL'e ilave buffer)
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05       # ATR x (entry anında SL'e çok yakınsa atla)
LOOKBACK_CROSSOVER = 30
LOOKBACK_SMI = 20

ADX_PERIOD = 14
ADX_THRESHOLD = 18             # >= 18
APPLY_COOLDOWN_BOTH_DIRECTIONS = True

# ==== SMI Light (Adaptif + Slope teyidi + opsiyonel froth guard) ====
SMI_LIGHT_NORM_MAX = 0.75        # statik fallback/başlangıç
SMI_LIGHT_ADAPTIVE = True
SMI_LIGHT_PCTL = 0.65            # 0.60 agresif, 0.70 muhafazakar
SMI_LIGHT_MAX_MIN = 0.60         # adaptif alt sınır
SMI_LIGHT_MAX_MAX = 1.10         # adaptif üst sınır
SMI_LIGHT_REQUIRE_SQUEEZE = False  # squeeze_off zorunlu değil

USE_SMI_SLOPE_CONFIRM = True     # SMI eğimi yön teyidi (önerilir)
USE_FROTH_GUARD = False          # fiyat EMA13'ten aşırı kopmuşsa sinyali pas geç
FROTH_GUARD_K_ATR = 0.9          # |close-ema13| <= K * ATR

# === ADX Rising (pencere + hibrit hızlı test) ===
ADX_RISE_K = 5                   # ana pencere
ADX_RISE_MIN_NET = 1.0           # son - ilk >= 1.0
ADX_RISE_POS_RATIO = 0.60        # farkların en az %60'ı > eps
ADX_RISE_EPS = 0.00              # minik titreşim eşiği

ADX_RISE_HYBRID = True           # hızlı alternatif
ADX_RISE_K_FAST = 3
ADX_RISE_FAST_EPS = 0.05

# === ADX sinyal modu: "2of3" (önerilen) ===
# Üçlü: (ADX>=18, ADX rising, DI yönü). En az 2 doğruysa yön teyidi geçer.
SIGNAL_MODE = "2of3"

# ---- Rate-limit & tarama pacing ----
MAX_CONCURRENT_FETCHES = 4
RATE_LIMIT_MS = 200
N_SHARDS = 5
BATCH_SIZE = 10
INTER_BATCH_SLEEP = 5.0

# ---- Sembol keşif ----
LINEAR_ONLY = True
QUOTE_WHITELIST = ("USDT",)

# ---- (Opsiyonel) Likidite filtresi ----
USE_LIQ_FILTER = False
LIQ_ROLL_BARS = 60
LIQ_QUANTILE  = 0.70
LIQ_MIN_DVOL_USD = 0

# ==== Trap risk filtresi (yalnız düşük/çok düşük) ====
RISK_FILTER_ACTIVE = True
RISK_ALLOWED_LEVELS = ("Çok düşük risk 🟢", "Düşük risk 🟢")

# ==== Mesaj fiyat hassasiyeti ====
PRICE_PRECISION = 5

# ================== TRAP SKORLAMA (YENİ) ==================
USE_TRAP_SCORING = True           # sadece puanlama; filtre YOK (filtre üstte ayrı)
SCORING_CTX_BARS = 3              # son 3 bar bağlam (wick/vol/RSI medyanı)
SCORING_WIN = 120                 # persentil/z-score penceresi (bar)
# Ağırlıklar (toplam ~100)
W_WICK   = 20.0                   # wick/boy oranı
W_VOL    = 20.0                   # hacim z / vol_ma oranı
W_BBPROX = 15.0                   # BB üst/alt banda yakınlık
W_ATRZ   = 15.0                   # ATR z-score
W_RSI    = 15.0                   # RSI aşırılık
W_MISC   = 5.0                    # ufak bağlam (ADX zayıf / squeeze vb.)
W_FB     = 10.0                   # fitil-bias bonus/ceza (opsiyonel kullanım)
# (Not: W_* toplamı 100)

# TT mesaj etiketleri
def _risk_label(score: float) -> str:
    if score < 20:  return "Çok düşük risk 🟢"
    if score < 40:  return "Düşük risk 🟢"
    if score < 60:  return "Orta risk ⚠️"
    if score < 80:  return "Yüksek risk 🟠"
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

def fmtp(x, p=PRICE_PRECISION):
    try:
        return f"{float(x):.{p}f}"
    except Exception:
        return str(x)

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

# ---- ADX rising (k=5) + hibrit (k=3) ----
def eval_adx_rising(adx_s: pd.Series, k=ADX_RISE_K, min_net=ADX_RISE_MIN_NET,
                    pos_ratio=ADX_RISE_POS_RATIO, eps=ADX_RISE_EPS):
    win = adx_s.iloc[-(k+1):-1]  # t-1'e kadar son k kapalı
    win = win.dropna()
    if len(win) < k:
        return False, {"k": k, "slope": float("nan"), "net": float("nan"), "pos": float("nan")}
    y = win.values.astype(float)
    x = np.arange(len(y))
    slope = float(np.polyfit(x, y, 1)[0])
    net = float(y[-1] - y[0])
    diffs = np.diff(y)
    pos = float((diffs > eps).mean()) if diffs.size else 0.0
    ok = (slope > 0.0) and (net >= min_net) and (pos >= pos_ratio)
    return ok, {"k": k, "slope": slope, "net": net, "pos": pos}

def eval_adx_rising_fast(adx_s: pd.Series, k=ADX_RISE_K_FAST, eps=ADX_RISE_FAST_EPS):
    win = adx_s.iloc[-(k+1):-1]
    win = win.dropna()
    if len(win) < k:
        return False, {"k": k, "slope": float("nan"), "last_diff": float("nan")}
    y = win.values.astype(float)
    x = np.arange(len(y))
    slope = float(np.polyfit(x, y, 1)[0])
    last_diff = float(y[-1] - y[-2]) if len(y) >= 2 else float("nan")
    ok = (slope > 0.0) and (last_diff > eps)
    return ok, {"k": k, "slope": slope, "last_diff": last_diff}

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
        if not m.get('active', True):        continue
        if not m.get('swap', False):         continue
        if linear_only and not m.get('linear', False): continue
        if m.get('quote') not in quote_whitelist:      continue
        syms.append(s)  # "BTC/USDT:USDT"
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
    denom = (1.4826 * df['vol_mad']).replace(0, np.nan)  # MAD -> sigma
    df['vol_z'] = (vol_s - df['vol_med']) / denom
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
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
        df.set_index('timestamp', inplace=True)
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, 13)
    df['sma34'] = calculate_sma(closes, 34)
    df['rsi']   = calculate_rsi(closes, 14)
    df['volume_sma20'] = df['volume'].rolling(20).mean().ffill()
    df = calculate_bb(df)
    df = calculate_kc(df)
    df = calculate_squeeze(df)
    df = calculate_smi_momentum(df)
    df, adx_condition, di_condition_long, di_condition_short = calculate_adx(df, symbol)
    df = ensure_atr(df, period=14)
    df = calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60)
    return df, df['squeeze_off'].iloc[-2], df['smi'].iloc[-2], 'green' if df['smi'].iloc[-2] > 0 else 'red' if df['smi'].iloc[-2] < 0 else 'gray', adx_condition, di_condition_long, di_condition_short

# ================== Likidite Filtresi ==================
def liquidity_ok(df: pd.DataFrame) -> bool:
    if not USE_LIQ_FILTER:
        return True
    if len(df) < LIQ_ROLL_BARS + 2:
        return False
    dv = (df['close'] * df['volume']).astype(float)
    roll = dv.rolling(LIQ_ROLL_BARS, min_periods=LIQ_ROLL_BARS)
    q = roll.apply(lambda x: np.nanquantile(x, LIQ_QUANTILE), raw=True)
    ok_q = bool(dv.iloc[-2] >= q.iloc[-2]) if pd.notna(q.iloc[-2]) else False
    ok_min = True if LIQ_MIN_DVOL_USD <= 0 else bool(dv.iloc[-2] >= LIQ_MIN_DVOL_USD)
    return ok_q and ok_min

# ================== TRAP SKORLAMA HESABI ==================
def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng

def compute_trap_scores(df: pd.DataFrame, side: str = "long") -> dict:
    try:
        ctx = df.iloc[-(SCORING_CTX_BARS+1):-1]  # -2 dahil, -1 hariç
        last = df.iloc[-2]

        body_u_l = ctx.apply(candle_body_wicks, axis=1, result_type='expand')
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
            rsi_sig = clamp((rsi_ctx - 60.0) / 20.0, 0.0, 1.0)
        else:
            rsi_sig = clamp((40.0 - rsi_ctx) / 20.0, 0.0, 1.0)

        misc = 0.0
        if 'adx' in last and np.isfinite(last['adx']) and last['adx'] < ADX_THRESHOLD:
            misc += 0.5
        if 'squeeze_on' in last and bool(last['squeeze_on']):
            misc += 0.5
        misc_sig = clamp(misc / 1.0, 0.0, 1.0)

        # Fitil-bias (FB) – yumuşak bonus
        fb_soft = clamp(((wick_ctx - 0.30) / 0.40), 0.0, 0.30)  # 0..0.3 arası
        fb_sig = max(0.0, fb_soft)

        wick_sig = clamp((wick_ctx - 0.25) / 0.5, 0.0, 1.0)

        score = (
            W_WICK   * wick_sig +
            W_VOL    * vol_sig  +
            W_BBPROX * bb_prox  +
            W_ATRZ   * atr_sig  +
            W_RSI    * rsi_sig  +
            W_MISC   * misc_sig +
            W_FB     * fb_sig
        )
        score = float(clamp(score, 0.0, 100.0))
        return {"score": score, "label": _risk_label(score)}
    except Exception as e:
        logger.warning(f"compute_trap_scores hata: {e}")
        return {"score": 0.0, "label": _risk_label(0.0)}

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

        # --- İndikatörler ---
        df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short = calculate_indicators(df, symbol, timeframe)
        if df is None:
            return

        # --- Likidite ---
        liq_ok = liquidity_ok(df)

        # ATR değerleri (risk ve normalizasyon için)
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol} {timeframe}), skip.")
            return

        # ================== SMI LIGHT (ADAPTİF + SLOPE + OPSİYONELLER) ==================
        smi_raw = smi_histogram
        atr_for_norm = max(atr_value, 1e-9)
        smi_norm = (smi_raw / atr_for_norm) if np.isfinite(smi_raw) else np.nan

        # Adaptif eşik
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

        # Squeeze opsiyonel
        if SMI_LIGHT_REQUIRE_SQUEEZE:
            base_long  = smi_squeeze_off and (smi_norm > 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
            base_short = smi_squeeze_off and (smi_norm < 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
        else:
            base_long  = (smi_norm > 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
            base_short = (smi_norm < 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)

        # Slope teyidi
        if USE_SMI_SLOPE_CONFIRM and pd.notna(df['smi'].iloc[-3]) and pd.notna(df['smi'].iloc[-2]):
            smi_slope = float(df['smi'].iloc[-2] - df['smi'].iloc[-3])
            slope_ok_long  = smi_slope > 0
            slope_ok_short = smi_slope < 0
        else:
            slope_ok_long = slope_ok_short = True

        # (opsiyonel) coşku freni
        if USE_FROTH_GUARD and pd.notna(df['ema13'].iloc[-2]):
            ema_gap = abs(float(df['close'].iloc[-2]) - float(df['ema13'].iloc[-2]))
            froth_ok = (ema_gap <= FROTH_GUARD_K_ATR * atr_for_norm)
        else:
            froth_ok = True

        smi_condition_long  = base_long  and slope_ok_long  and froth_ok
        smi_condition_short = base_short and slope_ok_short and froth_ok

        # ================== ADX rising (pencere + hibrit) ==================
        adx_series = df['adx']
        rising5, parts5 = eval_adx_rising(adx_series)
        adx_rising = rising5
        rising3, parts3 = (False, {})
        if (not adx_rising) and ADX_RISE_HYBRID:
            rising3, parts3 = eval_adx_rising_fast(adx_series)
            adx_rising = rising3

        # ================== Renk/mum gating ==================
        closed_candle = df.iloc[-2]  # kapalı mum
        bullish_close = bool(closed_candle['close'] > closed_candle['open'])
        bearish_close = bool(closed_candle['close'] < closed_candle['open'])

        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan

        logger.info(
            f"{symbol} {timeframe} SMI_norm:{(smi_norm if np.isfinite(smi_norm) else float('nan')):.3f} "
            f"squeeze_off:{smi_squeeze_off} eff_light:{SMI_LIGHT_NORM_MAX_EFF:.2f} "
            f"slope_ok(L/S):{slope_ok_long}/{slope_ok_short} froth_ok:{froth_ok}"
        )
        logger.info(
            f"{symbol} {timeframe} ADX parts k5: slope={parts5.get('slope'):.3f} net={parts5.get('net'):.3f} pos={parts5.get('pos'):.2f} "
            f"| k3: slope={parts3.get('slope', float('nan')):.3f} last={parts3.get('last_diff', float('nan')):.3f} "
            f"=> rising:{adx_rising}"
        )

        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
            'remaining_ratio': 1.0, 'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False
        })

        # --- EMA13 / SMA34 giriş kesişimi (lookback penceresi) ---
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

        # --- Yön teyidi (ADX 2/3) ---
        adx_ok = bool(df['adx'].iloc[-2] >= ADX_THRESHOLD) if pd.notna(df['adx'].iloc[-2]) else False
        di_long = bool(di_condition_long)
        di_short = bool(di_condition_short)

        if SIGNAL_MODE == "2of3":
            dir_long_ok  = (int(adx_ok) + int(adx_rising) + int(di_long))  >= 2
            dir_short_ok = (int(adx_ok) + int(adx_rising) + int(di_short)) >= 2
            str_ok = True
        else:
            dir_long_ok, dir_short_ok = di_long, di_short
            str_ok = adx_ok

        # --- Hacim filtresi (ATR oranına göre multiplier) ---
        volume_multiplier = 1.0 + min(avg_atr_ratio * 3, 0.2) if np.isfinite(avg_atr_ratio) else 1.0
        volume_ok = closed_candle['volume'] > closed_candle['volume_sma20'] * volume_multiplier if pd.notna(closed_candle['volume']) and pd.notna(closed_candle['volume_sma20']) else False

        # --- Trap riskleri (gating için) ---
        bull_score = compute_trap_scores(df, side="long") if USE_TRAP_SCORING else {"label": "Çok düşük risk 🟢", "score": 0}
        bear_score = compute_trap_scores(df, side="short") if USE_TRAP_SCORING else {"label": "Çok düşük risk 🟢", "score": 0}
        risk_ok_long  = (not RISK_FILTER_ACTIVE) or (bull_score["label"] in RISK_ALLOWED_LEVELS)
        risk_ok_short = (not RISK_FILTER_ACTIVE) or (bear_score["label"] in RISK_ALLOWED_LEVELS)

        # --- Al / Sat koşulları ---
        buy_condition = (
            liq_ok and bullish_close and
            ema_sma_crossover_buy and volume_ok and smi_condition_long and
            str_ok and dir_long_ok and risk_ok_long and
            (closed_candle['close'] > closed_candle['ema13'] and closed_candle['close'] > closed_candle['sma34'])
        )
        sell_condition = (
            liq_ok and bearish_close and
            ema_sma_crossover_sell and volume_ok and smi_condition_short and
            str_ok and dir_short_ok and risk_ok_short and
            (closed_candle['close'] < closed_candle['ema13'] and closed_candle['close'] < closed_candle['sma34'])
        )

        logger.info(f"{symbol} {timeframe} ADX_ok:{adx_ok} rising:{adx_rising} di_long:{di_long} di_short:{di_short}")
        logger.info(f"{symbol} {timeframe} buy:{buy_condition} sell:{sell_condition} riskL:{bull_score['label']} riskS:{bear_score['label']}")

        current_pos = signal_cache.get(key, current_pos)
        now = datetime.now(tz)

        if buy_condition and sell_condition:
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return

        # --- EMA/SMA EXIT kesişimleri (son kapalı mum) ---
        ema_prev, sma_prev = df['ema13'].iloc[-3], df['sma34'].iloc[-3]
        ema_last, sma_last = df['ema13'].iloc[-2], df['sma34'].iloc[-2]
        exit_cross_long  = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev >= sma_prev) and (ema_last < sma_last))
        exit_cross_short = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev <= sma_prev) and (ema_last > sma_last))
        logger.info(f"{symbol} {timeframe} exit_cross_long:{exit_cross_long} exit_cross_short:{exit_cross_short}")

        # === Reversal kapama ===
        if (buy_condition or sell_condition) and (current_pos['signal'] is not None):
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] != new_signal:
                if current_pos['signal'] == 'buy':
                    profit_percent = ((float(df['close'].iloc[-1]) - current_pos['entry_price']) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                else:
                    profit_percent = ((current_pos['entry_price'] - float(df['close'].iloc[-1])) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: REVERSAL CLOSE 🔁\n"
                    f"Price: {fmtp(df['close'].iloc[-1])}\n"
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

        # === Pozisyon aç — BUY ===
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
                if float(df['close'].iloc[-1]) <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    await enqueue_message(f"{symbol} {timeframe}: BUY atlandı (anında SL riski) 🚫")
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)

                    trap_line = ""
                    if USE_TRAP_SCORING:
                        trap_line = f"\nTrap Risk (Bull): {bull_score['score']:.0f}/100 → {bull_score['label']}"

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
                        f"Entry: {fmtp(entry_price)}\n"
                        f"SL:    {fmtp(sl_price)}\n"
                        f"TP1:   {fmtp(tp1_price)}\n"
                        f"TP2:   {fmtp(tp2_price)}"
                        f"{trap_line}"
                    )

        # === Pozisyon aç — SELL ===
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
                if float(df['close'].iloc[-1]) >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    await enqueue_message(f"{symbol} {timeframe}: SELL atlandı (anında SL riski) 🚫")
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)

                    trap_line = ""
                    if USE_TRAP_SCORING:
                        trap_line = f"\nTrap Risk (Bear): {bear_score['score']:.0f}/100 → {bear_score['label']}"

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
                        f"Entry: {fmtp(entry_price)}\n"
                        f"SL:    {fmtp(sl_price)}\n"
                        f"TP1:   {fmtp(tp1_price)}\n"
                        f"TP2:   {fmtp(tp2_price)}"
                        f"{trap_line}"
                    )

        # === Pozisyon yönetimi: LONG ===
        if current_pos['signal'] == 'buy':
            cur = float(df['close'].iloc[-1])
            if current_pos['highest_price'] is None or cur > current_pos['highest_price']:
                current_pos['highest_price'] = cur

            # TP1
            if not current_pos['tp1_hit'] and cur >= current_pos['tp1_price']:
                profit_percent = ((cur - current_pos['entry_price']) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']  # BE
                current_pos['tp1_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\n"
                    f"Cur: {fmtp(cur)} | TP1: {fmtp(current_pos['tp1_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi."
                )
            # TP2
            elif not current_pos['tp2_hit'] and cur >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((cur - current_pos['entry_price']) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\n"
                    f"Cur: {fmtp(cur)} | TP2: {fmtp(current_pos['tp2_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %40 kapandı, kalan %30 açık."
                )

            # EMA/SMA exit (bearish cross)
            if exit_cross_long:
                profit_percent = ((cur - current_pos['entry_price']) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: EMA/SMA EXIT (LONG) 🔁\n"
                    f"Price: {fmtp(cur)}\n"
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

            # SL tetik
            if cur <= current_pos['sl_price']:
                profit_percent = ((cur - current_pos['entry_price']) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP LONG ⛔\n"
                    f"Price: {fmtp(cur)}\n"
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

        # === Pozisyon yönetimi: SHORT ===
        elif current_pos['signal'] == 'sell':
            cur = float(df['close'].iloc[-1])
            if current_pos['lowest_price'] is None or cur < current_pos['lowest_price']:
                current_pos['lowest_price'] = cur

            # TP1
            if not current_pos['tp1_hit'] and cur <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - cur) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']  # BE
                current_pos['tp1_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\n"
                    f"Cur: {fmtp(cur)} | TP1: {fmtp(current_pos['tp1_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi."
                )
            # TP2
            elif not current_pos['tp2_hit'] and cur <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - cur) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\n"
                    f"Cur: {fmtp(cur)} | TP2: {fmtp(current_pos['tp2_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %40 kapandı, kalan %30 açık."
                )

            # EMA/SMA exit (bullish cross)
            if exit_cross_short:
                profit_percent = ((current_pos['entry_price'] - cur) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: EMA/SMA EXIT (SHORT) 🔁\n"
                    f"Price: {fmtp(cur)}\n"
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

            # SL tetik
            if cur >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - cur) / current_pos['entry_price']) * 100 if current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP SHORT ⛔\n"
                    f"Price: {fmtp(cur)}\n"
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
