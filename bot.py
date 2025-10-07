import ccxt
import numpy as np
import pandas as pd
import telegram
import logging
import asyncio
import threading
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
import json
from collections import Counter

# ================== Sabit Değerler ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("BOT_TOKEN ve CHAT_ID ortam değişkenlerini ayarla.")
TEST_MODE = False
VERBOSE_LOG = False
HEARTBEAT_ENABLED = False
STARTUP_MSG_ENABLED = True
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0
TP_MULTIPLIER2 = 3.5
SL_BUFFER = 0.3
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05
ADX_PERIOD = 14
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
USE_FROTH_GUARD = True
FROTH_GUARD_K_ATR = 1.2
MAX_CONCURRENT_FETCHES = 4
RATE_LIMIT_MS = 200
N_SHARDS = 5
BATCH_SIZE = 10
INTER_BATCH_SLEEP = 5.0
LINEAR_ONLY = True
QUOTE_WHITELIST = ("USDT",)
VOL_WIN = 60
VOL_Q = 0.60
OBV_SLOPE_WIN = 4
NTX_PERIOD = 14
NTX_K_EFF = 10
NTX_VOL_WIN = 60
NTX_THR_LO, NTX_THR_HI = 44.0, 54.0
NTX_ATRZ_LO, NTX_ATRZ_HI = -1.0, 1.5
NTX_MIN_FOR_HYBRID = 44.0
NTX_RISE_K_STRICT = 5
NTX_RISE_MIN_NET = 1.0
NTX_RISE_POS_RATIO = 0.6
NTX_RISE_EPS = 0.05
NTX_RISE_K_HYBRID = 3
NTX_FROTH_K = 1.0
EMA_FAST = 13
EMA_MID = 34
EMA_SLOW = 89
ADX_SOFT = 21
MIN_BARS = 80
NEW_SYMBOL_COOLDOWN_MIN = 180
ADX_RISE_K = 5
ADX_RISE_MIN_NET = 1.0
ADX_RISE_POS_RATIO = 0.6
ADX_RISE_EPS = 0.0
ADX_RISE_USE_HYBRID = True
REGIME1_BAND_K_DEFAULT = 0.25
REGIME1_SLOPE_WIN = 5
REGIME1_SLOPE_THR_PCT = 0.0
REGIME1_REQUIRE_2CLOSE = True
REGIME1_ADX_ADAPTIVE_BAND = True
RECENCY_K = 6
RECENCY_OPP_K = 2
GRACE_BARS = 8  # 8 mum istisnası için sabit
USE_GATE_V3 = True
G3_BAND_K = 0.25
G3_SLOPE_WIN = 5
G3_SLOPE_THR_PCT = 0.0
G3_MIN_ADX = 22
G3_MIN_ADX_BEAR = 24
G3_NTX_BONUS_BEAR = 6.0
G3_NTX_SLOPE_WIN = 5
G3_USE_RETEST = False
G3_BOS_LOOKBACK = 12
G3_BOS_CONFIRM_BARS = 2
G3_BOS_MIN_BREAK_ATR = 0.20
G3_SWING_WIN = 7
G3_ENTRY_DIST_EMA13_ATR = 0.75
G3_FF_MIN_SCORE = 3
G3_FF_MIN_SCORE_BEAR = 4
USE_ROBUST_SLOPE = True
SCAN_PAUSE_SEC = 120  # her turun sonunda bekleme (2 dk)
# ==== Dynamic mode & profil ====
DYNAMIC_MODE = True
FF_ACTIVE_PROFILE = os.getenv("FF_PROFILE", "garantici")
if FF_ACTIVE_PROFILE == "agresif":
    NTX_LOCAL_WIN = 240
    BANDK_MIN, BANDK_MAX = 0.18, 0.35
    VOL_MA_RATIO_MIN = 1.02
    VOL_Z_MIN = 0.8
    FF_BODY_MIN = 0.40
    FF_UPWICK_MAX = 0.40
    FF_DNWICK_MAX = 0.40
    FF_BB_MIN = 0.20
    G3_BOS_CONFIRM_BARS = max(1, G3_BOS_CONFIRM_BARS - 1)
else:
    NTX_LOCAL_WIN = 300
    BANDK_MIN, BANDK_MAX = 0.22, 0.35
    VOL_MA_RATIO_MIN = 1.05
    VOL_Z_MIN = 1.0
    FF_BODY_MIN = 0.45
    FF_UPWICK_MAX = 0.35
    FF_DNWICK_MAX = 0.35
    FF_BB_MIN = 0.22
NTX_LOCAL_Q = 0.60
NTX_Z_EWMA_ALPHA = 0.02
ntx_local_cache = {}
_ntx_cache_lock = threading.Lock()

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

# INFO seviyesinde sadece shard başlıkları ve FALSE dökümünü geçir
class MinimalInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno != logging.INFO:
            return True  # DEBUG/WARNING/ERROR dokunma
        msg = str(record.getMessage())
        return (
            msg.startswith("Shard ") or
            msg.startswith("Kriter FALSE dökümü") or
            msg.startswith("  (veri yok)") or
            msg.startswith(" - ")
        )

# Logger'a filtreyi ekle
for h in logger.handlers:
    h.addFilter(MinimalInfoFilter())

logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

# ================== Borsa & Bot ==================
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'linear'},
    'timeout': 60000
})
MARKETS = {}
new_symbol_until = {}
_stats_lock = asyncio.Lock()
scan_status = {}
crit_false_counts = Counter()
crit_total_counts = Counter()

async def mark_status(symbol: str, code: str, detail: str = ""):
    async with _stats_lock:
        scan_status[symbol] = {'code': code, 'detail': detail}

async def record_crit_batch(items):
    async with _stats_lock:
        for name, passed in items:
            crit_total_counts[name] += 1
            if not passed:
                crit_false_counts[name] += 1

async def load_markets():
    global MARKETS
    MARKETS = await asyncio.to_thread(exchange.load_markets)

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
STATE_FILE = 'positions.json'
DT_KEYS = {"last_signal_time", "entry_time", "last_bar_time", "last_regime_bar"}

def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)

def _parse_dt(val):
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except Exception:
            return val
    return val

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            for k, v in data.items():
                if isinstance(v, dict):
                    for dk in DT_KEYS:
                        if dk in v:
                            v[dk] = _parse_dt(v[dk])
            return data
        except Exception as e:
            logger.warning(f"State yüklenemedi: {e}")
            return {}
    return {}

def save_state():
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(signal_cache, f, default=_json_default)
    except Exception as e:
        logger.warning(f"State kaydedilemedi: {e}")

signal_cache = load_state()

# ================== Util ==================
def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def ema_smooth(new, old, alpha=0.3):
    if old is None:
        return new
    return old * (1 - alpha) + new * alpha

def rolling_z(series: pd.Series, win: int) -> float:
    s = series.tail(win).astype(float)
    if s.size < 5 or s.std(ddof=0) == 0 or not np.isfinite(s.iloc[-1]):
        return 0.0
    return float((s.iloc[-1] - s.mean()) / (s.std(ddof=0) + 1e-12))

def fmt_sym(symbol, x):
    try:
        prec = MARKETS.get(symbol, {}).get('precision', {}).get('price', 5)
        return f"{float(x):.{prec}f}"
    except Exception:
        return str(x)

def bars_since(mask: pd.Series, idx: int = -2) -> int:
    s = mask.iloc[: idx + 1]
    rev = s.values[::-1]
    return int(np.argmax(rev)) if rev.any() else len(rev)

def format_signal_msg(symbol: str, timeframe: str, side: str,
                     entry: float, sl: float, tp1: float, tp2: float,
                     tz_name: str = "Europe/Istanbul") -> str:
    tz = pytz.timezone(tz_name)
    date_str = datetime.now(tz).strftime("%d.%m.%Y")
    title = "BUY (LONG) 🚀" if side == "buy" else "SELL (SHORT) 📉"
    return (
        f"{symbol} {timeframe}: {title}\n"
        f"Entry: {fmt_sym(symbol, entry)}\n"
        f"SL: {fmt_sym(symbol, sl)}\n"
        f"TP1: {fmt_sym(symbol, tp1)}\n"
        f"TP2: {fmt_sym(symbol, tp2)}\n"
        f"Tarih: {date_str}"
    )

def rising_ema(series: pd.Series, win: int = 5, eps: float = 0.0, pos_ratio_thr: float = 0.55, **kwargs):
    if 'pos_ratio_th' in kwargs:
        try:
            logger.warning("rising_ema(): 'pos_ratio_th' is deprecated; use 'pos_ratio_thr'.")
        except Exception:
            pass
        pos_ratio_thr = kwargs.pop('pos_ratio_th')
    s = pd.to_numeric(series, errors='coerce').dropna()
    if len(s) < win + 2:
        return False, 0.0, 0.0
    ema = s.ewm(span=win, adjust=False).mean()
    slope = float(ema.iloc[-1] - ema.iloc[-2])
    diffs = np.diff(ema.iloc[-(win+1):].values)
    pos_ratio = float((diffs > eps).mean()) if diffs.size else 0.0
    ok = (slope > eps) and (pos_ratio >= pos_ratio_thr)
    return ok, slope, pos_ratio

def robust_up(series: pd.Series, win: int = 5, eps: float = 0.0, pos_ratio_thr: float = 0.55, **kwargs):
    if 'pos_ratio_th' in kwargs:
        try:
            logger.warning("robust_up(): 'pos_ratio_th' is deprecated; use 'pos_ratio_thr'.")
        except Exception:
            pass
        pos_ratio_thr = kwargs.pop('pos_ratio_th')
    s = pd.to_numeric(series, errors='coerce').dropna()
    if len(s) < win + 2:
        return False, 0.0, 0.0
    window_vals = s.iloc[-(win+1):].values
    med_d = float(np.median(np.diff(window_vals)))
    pos_ratio = float((np.diff(window_vals) > eps).mean())
    ok = (med_d > eps) and (pos_ratio >= pos_ratio_thr)
    return ok, med_d, pos_ratio

def _last_true_index(s: pd.Series, upto_idx: int) -> int:
    # upto_idx dahil olacak şekilde sondan ilk True index'i döndürür, yoksa -1
    vals = s.iloc[:upto_idx+1].values
    for i in range(len(vals)-1, -1, -1):
        if bool(vals[i]):
            return i
    return -1

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

# ================== Sembol Keşfi ==================
async def discover_bybit_symbols(linear_only=True, quote_whitelist=("USDT",)):
    markets = await asyncio.to_thread(exchange.load_markets)
    syms = []
    for s, m in markets.items():
        if not m.get('active', True): continue
        if not m.get('swap', False): continue
        if linear_only and not m.get('linear', False): continue
        if m.get('quote') not in quote_whitelist: continue
        syms.append(s)
    syms = sorted(set(syms))
    logger.debug(f"Keşfedilen sembol sayısı: {len(syms)} (linear={linear_only}, quotes={quote_whitelist})")
    return syms

# ================== Volume Kontrol ==================
def simple_volume_ok(df: pd.DataFrame, side: str) -> (bool, str):
    last = df.iloc[-2]
    vol = float(last['volume'])
    vol_ma = float(last['vol_ma']) if pd.notna(last['vol_ma']) else np.nan
    if not (np.isfinite(vol) and np.isfinite(vol_ma) and vol_ma > 0):
        return False, "vol_nan"
    ratio = vol / vol_ma
    vz = float(df['vol_z'].iloc[-2]) if 'vol_z' in df.columns and pd.notna(df['vol_z'].iloc[-2]) else 0.0
    d = float((df['close'] * df['volume']).iloc[-2])
    d_q = float(df['dvol_q'].iloc[-2]) if 'dvol_q' in df.columns and pd.notna(df['dvol_q'].iloc[-2]) else 0.0
    ratio_ok = (ratio >= VOL_MA_RATIO_MIN)
    vz_ok = (vz >= VOL_Z_MIN)
    d_ok = (d >= d_q) if np.isfinite(d_q) and d_q > 0 else True
    ok = bool(ratio_ok and vz_ok and d_ok)
    reason = f"ratio={ratio:.2f}{'✓' if ratio_ok else '✗'}, vz={vz:.2f}{'✓' if vz_ok else '✗'}, dvol>q={d_ok}"
    return ok, reason

# ================== İndikatör Fonksiyonları ==================
def calculate_ema(closes, span):
    k = 2 / (span + 1)
    ema = np.zeros_like(closes, dtype=np.float64)
    ema[0] = closes[0]
    for i in range(1, len(closes)):
        ema[i] = (closes[i] * k) + (ema[i-1] * (1 - k))
    return ema

def calculate_adx(df, symbol, period=ADX_PERIOD):
    df['high_diff'] = df['high'] - df['high'].shift(1)
    df['low_diff'] = df['low'].shift(1) - df['low']
    df['+DM'] = np.where((df['high_diff'] > df['low_diff']) & (df['high_diff'] > 0), df['high_diff'], 0)
    df['-DM'] = np.where((df['low_diff'] > df['high_diff']) & (df['low_diff'] > 0), df['low_diff'], 0)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    df['TR'] = np.maximum(high_low, np.maximum(high_close, low_close))
    alpha = 1.0 / period
    tr_ema = df['TR'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    df['di_plus'] = 100 * (df['+DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    df['di_minus'] = 100 * (df['-DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    denom = (df['di_plus'] + df['di_minus']).clip(lower=1e-9)
    df['DX'] = (100 * (df['di_plus'] - df['di_minus']).abs() / denom).fillna(0)
    df['adx'] = df['DX'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    di_condition_long = df['di_plus'].iloc[-2] > df['di_minus'].iloc[-2] if pd.notna(df['di_plus'].iloc[-2]) and pd.notna(df['di_minus'].iloc[-2]) else False
    di_condition_short = df['di_plus'].iloc[-2] < df['di_minus'].iloc[-2] if pd.notna(df['di_plus'].iloc[-2]) and pd.notna(df['di_minus'].iloc[-2]) else False
    if VERBOSE_LOG:
        logger.debug(f"ADX calculated: {df['adx'].iloc[-2]:.2f} for {symbol} at {df.index[-2]}")
    return df, di_condition_long, di_condition_short

def calculate_bb(df, period=20, mult=2.0):
    df['bb_mid'] = df['close'].rolling(period).mean()
    df['bb_std'] = df['close'].rolling(period).std()
    df['bb_upper'] = df['bb_mid'] + mult * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - mult * df['bb_std']
    return df

def calc_sqzmom_lb(df: pd.DataFrame, length=20, mult_bb=2.0, lengthKC=20, multKC=1.5, use_true_range=True):
    src = df['close'].astype(float)
    basis = src.rolling(length).mean()
    dev = src.rolling(length).std(ddof=0) * mult_bb
    upperBB = basis + dev
    lowerBB = basis - dev
    ma = src.rolling(lengthKC).mean()
    if use_true_range:
        tr1 = (df['high'] - df['low']).astype(float)
        tr2 = (df['high'] - df['close'].shift()).abs().astype(float)
        tr3 = (df['low'] - df['close'].shift()).abs().astype(float)
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    else:
        tr = (df['high'] - df['low']).astype(float)
    rangema = tr.rolling(lengthKC).mean()
    upperKC = ma + rangema * multKC
    lowerKC = ma - rangema * multKC
    sqz_on = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqz_off = (lowerBB < lowerKC) & (upperBB > upperKC)
    no_sqz = (~sqz_on) & (~sqz_off)
    highest = df['high'].rolling(lengthKC).max()
    lowest = df['low'].rolling(lengthKC).min()
    mid1 = (highest + lowest) / 2.0
    mid2 = src.rolling(lengthKC).mean()
    center = (mid1 + mid2) / 2.0
    series = (src - center)
    val = pd.Series(index=df.index, dtype='float64')
    for i in range(lengthKC-1, len(series)):
        y = series.iloc[i-lengthKC+1:i+1].values
        x = np.arange(lengthKC, dtype=float)
        if np.isfinite(y).sum() >= 2:
            m, b = np.polyfit(x, y, 1)
            val.iloc[i] = m*(lengthKC-1) + b
        else:
            val.iloc[i] = np.nan
    df['lb_sqz_val'] = val
    df['lb_sqz_on'] = sqz_on
    df['lb_sqz_off'] = sqz_off
    df['lb_sqz_no'] = no_sqz
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
    denom = (1.4826 * df['vol_mad']).replace(0, np.nan)
    df['vol_z'] = (vol_s - df['vol_med']) / denom
    dvol = (df['close'] * df['volume']).astype(float)
    df['dvol_q'] = dvol.rolling(VOL_WIN).quantile(VOL_Q)
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

def _compute_ntx_z(ntx: pd.Series, win_ema: int = 21, win_q: int = 120) -> pd.Series:
    s = pd.to_numeric(ntx, errors='coerce')
    ema = s.ewm(span=win_ema, adjust=False).mean()
    q50 = s.rolling(win_q, min_periods=max(10, win_q//4)).quantile(0.5)
    q84 = s.rolling(win_q, min_periods=max(10, win_q//4)).quantile(0.84)
    denom = (q84 - q50).replace(0, np.nan)
    z = (ema - q50) / (denom + 1e-12)
    return z.fillna(0.0)

def calculate_indicators(df, symbol, timeframe):
    if len(df) < MIN_BARS:
        logger.debug(f"{symbol}: Yetersiz veri ({len(df)} mum), skip.")
        return None, None, None
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
        df.set_index('timestamp', inplace=True)
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, EMA_FAST)
    df['ema34'] = calculate_ema(closes, EMA_MID)
    df['ema89'] = calculate_ema(closes, EMA_SLOW)
    df = calculate_bb(df)
    df = calc_sqzmom_lb(df, length=20, mult_bb=2.0, lengthKC=20, multKC=1.5, use_true_range=True)
    df, di_condition_long, di_condition_short = calculate_adx(df, symbol)
    df = ensure_atr(df, period=14)
    df = calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60)
    df = calc_ntx(df, period=NTX_PERIOD, k_eff=NTX_K_EFF)
    if 'ntx' in df.columns:
        df['ntx_z'] = _compute_ntx_z(df['ntx'])
    return df, di_condition_long, di_condition_short

def adx_rising_strict(df_adx: pd.Series) -> bool:
    if USE_ROBUST_SLOPE:
        ok, _, _ = robust_up(
            df_adx,
            win=min(ADX_RISE_K + 1, 8),
            eps=ADX_RISE_EPS,
            pos_ratio_thr=ADX_RISE_POS_RATIO
        )
        return ok
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
    if not ADX_RISE_USE_HYBRID or df_adx is None or len(df_adx) < 4:
        return False
    if USE_ROBUST_SLOPE:
        ok, _, _ = robust_up(df_adx, win=4, eps=ADX_RISE_EPS, pos_ratio_thr=0.55)
        last_diff = float(df_adx.iloc[-2] - df_adx.iloc[-3])
        return ok and (last_diff > ADX_RISE_EPS)
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

def calc_ntx(df: pd.DataFrame, period: int = NTX_PERIOD, k_eff: int = NTX_K_EFF) -> pd.DataFrame:
    close = df['close'].astype(float)
    atr = df['atr'].astype(float).replace(0, np.nan)
    ema13 = df['ema13'].astype(float)
    num = (close - close.shift(k_eff)).abs()
    den = close.diff().abs().rolling(k_eff).sum()
    er = (num / (den + 1e-12)).clip(0, 1).fillna(0)
    slope_norm = (ema13 - ema13.shift(k_eff)) / ((atr * k_eff) + 1e-12)
    slope_mag = slope_norm.abs().clip(0, 3) / 3.0
    slope_mag = slope_mag.fillna(0)
    dif = close.diff()
    sign_price = np.sign(dif)
    sign_slope = np.sign(slope_norm.shift(1)).replace(0, np.nan)
    same_dir = (sign_price == sign_slope).astype(float)
    pos_ratio = same_dir.rolling(k_eff).mean().fillna(0)
    vol_ratio = (df['volume'] / df['vol_ma'].replace(0, np.nan)).clip(lower=0).fillna(0)
    vol_sig = np.tanh(np.maximum(0.0, vol_ratio - 1.0)).fillna(0)
    base = (
        0.35 * er +
        0.35 * slope_mag +
        0.15 * pos_ratio +
        0.15 * vol_sig
    ).clip(0, 1)
    df['ntx_raw'] = base
    df['ntx'] = df['ntx_raw'].ewm(alpha=1.0/period, adjust=False).mean() * 100.0
    return df

def ntx_threshold(atr_z: float) -> float:
    a = clamp((atr_z - NTX_ATRZ_LO) / (NTX_ATRZ_HI - NTX_ATRZ_LO + 1e-12), 0.0, 1.0)
    return NTX_THR_LO + a * (NTX_THR_HI - NTX_THR_LO)

def ntx_rising_strict(s: pd.Series, k: int = NTX_RISE_K_STRICT, min_net: float = NTX_RISE_MIN_NET, pos_ratio_th: float = NTX_RISE_POS_RATIO, eps: float = NTX_RISE_EPS) -> bool:
    if USE_ROBUST_SLOPE:
        ok, _, pr = robust_up(s, win=min(k+1, 8), eps=eps, pos_ratio_thr=pos_ratio_th)
        return ok
    if s is None or len(s) < k + 1: return False
    w = s.iloc[-(k+1):-1].astype(float)
    if w.isna().any(): return False
    x = np.arange(len(w)); slope, _ = np.polyfit(x, w.values, 1)
    diffs = np.diff(w.values)
    posr = (diffs > eps).mean() if diffs.size else 0.0
    net = w.iloc[-1] - w.iloc[0]
    return (slope > 0) and (net >= min_net) and (posr >= pos_ratio_th)

def ntx_rising_hybrid_guarded(df: pd.DataFrame, side: str, eps: float = NTX_RISE_EPS, min_ntx: float = NTX_MIN_FOR_HYBRID, k: int = NTX_RISE_K_HYBRID, froth_k: float = NTX_FROTH_K) -> bool:
    s = df['ntx'] if 'ntx' in df.columns else None
    if s is None or len(s) < k + 1:
        return False
    w = s.iloc[-(k+1):-1].astype(float)
    if w.isna().any():
        return False
    if USE_ROBUST_SLOPE:
        ok, _, _ = robust_up(w, win=k, eps=eps, pos_ratio_thr=0.55)
        last_diff = float(w.iloc[-1] - w.iloc[-2])
        if not (ok and last_diff > eps):
            return False
    else:
        x = np.arange(len(w)); slope, _ = np.polyfit(x, w.values, 1)
        last_diff = w.values[-1] - w.values[-2]
        if not (slope > 0 and last_diff > eps):
            return False
    if w.iloc[-1] < min_ntx:
        return False
    close_last = float(df['close'].iloc[-2]); ema13_last = float(df['ema13'].iloc[-2]); atr_value = float(df['atr'].iloc[-2])
    if not (np.isfinite(close_last) and np.isfinite(ema13_last) and np.isfinite(atr_value) and atr_value > 0):
        return False
    return abs(close_last - ema13_last) <= froth_k * atr_value

def ntx_vote(df: pd.DataFrame, ntx_thr: float) -> bool:
    if 'ntx' not in df.columns or 'ema13' not in df.columns:
        return False
    ntx_last = float(df['ntx'].iloc[-2]) if pd.notna(df['ntx'].iloc[-2]) else np.nan
    level_ok = (np.isfinite(ntx_last) and ntx_last >= ntx_thr)
    mom_ok = ntx_rising_strict(df['ntx']) or ntx_rising_hybrid_guarded(df, side="long") or ntx_rising_hybrid_guarded(df, side="short")
    k = NTX_K_EFF
    if len(df) >= k + 3 and pd.notna(df['ema13'].iloc[-2]) and pd.notna(df['ema13'].iloc[-2-k]):
        slope_ok = (df['ema13'].iloc[-2] > df['ema13'].iloc[-2-k])
    else:
        slope_ok = False
    votes = int(level_ok) + int(mom_ok) + int(slope_ok)
    return votes >= 2

def compute_dynamic_band_k(df: pd.DataFrame, adx_last: float) -> float:
    band_k_adx = 0.20 if (np.isfinite(adx_last) and adx_last >= 25) else (0.30 if (np.isfinite(adx_last) and adx_last < 18) else REGIME1_BAND_K_DEFAULT)
    c2 = float(df['close'].iloc[-2]); atr2 = float(df['atr'].iloc[-2])
    atr_pct = atr2 / abs(c2) if (np.isfinite(atr2) and np.isfinite(c2) and c2 != 0) else 0.0
    atr_pct = clamp(atr_pct, 0.0, 0.10)
    band_k_vol = float(np.interp(atr_pct, [0.006, 0.025], [0.20, 0.35]))
    if FF_ACTIVE_PROFILE == "agresif":
        w_adx, w_vol = 0.6, 0.4
    else:
        w_adx, w_vol = 0.4, 0.6
    band_k = w_adx * band_k_adx + w_vol * band_k_vol
    return clamp(band_k, BANDK_MIN, BANDK_MAX)

def compute_ntx_local_thr(df: pd.DataFrame, base_thr: float, symbol: str = None) -> (float, float):
    q_val = None
    tail = df['ntx'].iloc[-(NTX_LOCAL_WIN+2):-2].dropna()
    min_bars = 40 if FF_ACTIVE_PROFILE == "agresif" else 60
    if len(tail) >= min_bars:
        q_val = float(tail.quantile(NTX_LOCAL_Q))
        if symbol is not None:
            with _ntx_cache_lock:
                ntx_local_cache[symbol] = ema_smooth(q_val, ntx_local_cache.get(symbol, q_val))
                q_val = ntx_local_cache[symbol]
    if q_val is None or not np.isfinite(q_val):
        return base_thr, float('nan')
    return max(base_thr, q_val), q_val

def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng

def fake_filter_v2(df: pd.DataFrame, side: str, bear_mode: bool) -> (bool, str):
    if len(df) < max(VOL_WIN + 2, OBV_SLOPE_WIN + 2):
        return False, "data_short"
    last = df.iloc[-2]
    vol_ok, vol_reason = simple_volume_ok(df, side)
    body, up, low = candle_body_wicks(last)
    bb_prox = _bb_prox(last, side=side)
    obv_slope = _obv_slope_recent(df, win=OBV_SLOPE_WIN)
    body_ok = body >= FF_BODY_MIN
    wick_ok = (up <= FF_UPWICK_MAX) if side == "long" else (low <= FF_DNWICK_MAX)
    bb_ok = bb_prox >= FF_BB_MIN
    obv_ok = (obv_slope > 0) if side == "long" else (obv_slope < 0)
    score = int(vol_ok) + int(body_ok) + int(wick_ok) + int(bb_ok) + int(obv_ok)
    ff_min_score = G3_FF_MIN_SCORE_BEAR if bear_mode else G3_FF_MIN_SCORE
    all_ok = score >= ff_min_score
    debug = (
        f"vol={'OK' if vol_ok else 'FAIL'} ({vol_reason}), "
        f"body={body:.2f} ({'OK' if body_ok else 'FAIL'}), "
        f"wick={(up if side=='long' else low):.2f} ({'OK' if wick_ok else 'FAIL'}), "
        f"bb_prox={bb_prox:.2f} ({'OK' if bb_ok else 'FAIL'}), "
        f"obv_slope={obv_slope:.2f} ({'OK' if obv_ok else 'FAIL'})"
    )
    return all_ok, debug

def _bb_prox(last, side="long"):
    if side == "long":
        num = float(last['close'] - last['bb_mid']); den = float(last['bb_upper'] - last['bb_mid'])
    else:
        num = float(last['bb_mid'] - last['close']); den = float(last['bb_mid'] - last['bb_lower'])
    if not (np.isfinite(num) and np.isfinite(den)) or den <= 0: return 0.0
    return clamp(num/den, 0.0, 1.0)

def _obv_slope_recent(df: pd.DataFrame, win=OBV_SLOPE_WIN) -> float:
    s = df['obv'].iloc[-(win+1):-1].astype(float) if 'obv' in df.columns else pd.Series(dtype=float)
    if s.size < 3 or s.isna().any():
        return 0.0
    if USE_ROBUST_SLOPE:
        ok1, slope1, _ = rising_ema(s, win=win, eps=0.0, pos_ratio_thr=0.55)
        ok2, slope2, _ = robust_up(s, win=win, eps=0.0, pos_ratio_thr=0.55)
        if ok1 or ok2:
            return float(slope1 if ok1 else slope2)
        return float(np.median(np.diff(s.values)))
    x = np.arange(len(s))
    m, _ = np.polyfit(x, s.values, 1)
    return float(m)

def _is_swing_high(df, i, win=G3_SWING_WIN):
    if i - win < 0 or i + win >= len(df): return False
    h = df['high'].values
    return h[i] == max(h[i-win:i+win+1])

def _is_swing_low(df, i, win=G3_SWING_WIN):
    if i - win < 0 or i + win >= len(df): return False
    l = df['low'].values
    return l[i] == min(l[i-win:i+win+1])

def _last_swing_levels(df, win=G3_SWING_WIN):
    idx_last = len(df) - 2
    sh = None; sl = None
    for i in range(idx_last-1, max(idx_last-60, 0), -1):
        if sh is None and _is_swing_high(df, i, win=win):
            sh = float(df['high'].iloc[i])
        if sl is None and _is_swing_low(df, i, win=win):
            sl = float(df['low'].iloc[i])
        if sh is not None and sl is not None:
            break
    return sh, sl

def _bos_only(df: pd.DataFrame, side: str,
              lookback: int = G3_BOS_LOOKBACK,
              confirm_bars: int = G3_BOS_CONFIRM_BARS,
              min_break_atr: float = G3_BOS_MIN_BREAK_ATR):
    if len(df) < max(lookback+5, 40) or 'atr' not in df.columns:
        return False, "data_short"
    sh, sl = _last_swing_levels(df)
    if side == 'long' and sh is None: return False, "no_swing_high"
    if side == 'short' and sl is None: return False, "no_swing_low"
    closes = df['close']
    atr = df['atr']
    idx_last = len(df) - 2
    if side == 'long':
        bos_idx = None
        for i in range(idx_last - lookback, idx_last):
            if i <= 0 or i >= len(df): continue
            if not (np.isfinite(closes.iloc[i]) and np.isfinite(atr.iloc[i])): continue
            level = sh + min_break_atr * atr.iloc[i]
            if closes.iloc[i] > level:
                bos_idx = i
                break
        if bos_idx is None:
            return False, "no_bos_long"
        conf_needed = confirm_bars
        ok = 0
        for j in range(bos_idx, min(bos_idx + confirm_bars, idx_last + 1)):
            lvl_j = sh + (min_break_atr * (atr.iloc[j] if np.isfinite(atr.iloc[j]) else 0.0))
            if closes.iloc[j] > lvl_j:
                ok += 1
        return (ok >= conf_needed), f"bos_only_long conf={ok}/{conf_needed}"
    else:
        bos_idx = None
        for i in range(idx_last - lookback, idx_last):
            if i <= 0 or i >= len(df): continue
            if not (np.isfinite(closes.iloc[i]) and np.isfinite(atr.iloc[i])): continue
            level = sl - min_break_atr * atr.iloc[i]
            if closes.iloc[i] < level:
                bos_idx = i
                break
        if bos_idx is None:
            return False, "no_bos_short"
        conf_needed = confirm_bars
        ok = 0
        for j in range(bos_idx, min(bos_idx + confirm_bars, idx_last + 1)):
            lvl_j = sl - (min_break_atr * (atr.iloc[j] if np.isfinite(atr.iloc[j]) else 0.0))
            if closes.iloc[j] < lvl_j:
                ok += 1
        return (ok >= conf_needed), f"bos_only_short conf={ok}/{conf_needed}"

def _trend_ok(df, side, band_k, slope_win, slope_thr_pct):
    c2 = float(df['close'].iloc[-2]); e89 = float(df['ema89'].iloc[-2]); atr2 = float(df['atr'].iloc[-2])
    c3 = float(df['close'].iloc[-3]); e89_3 = float(df['ema89'].iloc[-3]); atr3 = float(df['atr'].iloc[-3])
    if any(map(lambda x: not np.isfinite(x), [c2,e89,atr2,c3,e89_3,atr3])): return False, "nan"
    if side == 'long':
        band_ok = (c2 > e89 + band_k*atr2) and (c3 > e89_3 + band_k*atr3)
    else:
        band_ok = (c2 < e89 - band_k*atr2) and (c3 < e89_3 - band_k*atr3)
    if len(df) > slope_win + 2 and pd.notna(df['ema89'].iloc[-2 - slope_win]):
        e_now = float(df['ema89'].iloc[-2]); e_then = float(df['ema89'].iloc[-2 - slope_win])
        pct_slope = (e_now - e_then) / max(abs(e_then), 1e-12)
    else:
        pct_slope = 0.0
    slope_ok = (pct_slope > slope_thr_pct/100.0) if side=='long' else (pct_slope < -slope_thr_pct/100.0)
    return (band_ok and slope_ok), f"band={band_ok},slope={pct_slope*100:.2f}%"

def _momentum_ok(df, side, adx_last, vote_ntx, ntx_thr, bear_mode):
    adx_min = G3_MIN_ADX_BEAR if bear_mode else G3_MIN_ADX
    adx_gate = np.isfinite(adx_last) and (adx_last >= adx_min)
    ntx_gate = vote_ntx
    s = df['ntx'] if 'ntx' in df.columns else None
    if s is None:
        ntx_trend_ok = False
        ntx_trend_dbg = "no_ntx"
    else:
        ok1, slope1, pr1 = rising_ema(s, win=G3_NTX_SLOPE_WIN, eps=0.0, pos_ratio_thr=0.55)
        ok2, slope2, pr2 = robust_up(s, win=G3_NTX_SLOPE_WIN, eps=0.0, pos_ratio_thr=0.55)
        ntx_trend_ok = ok1 or ok2
        ntx_trend_dbg = f"r_ema={ok1} (s={slope1:.3f},pr={pr1:.2f}) | r_med={ok2} (s={slope2:.3f},pr={pr2:.2f})"
    mom_score = int(adx_gate) + int(ntx_gate) + int(ntx_trend_ok)
    ok = (mom_score >= 2)
    dbg = (f"adx>={adx_min}={adx_gate}, ntx_thr={ntx_thr:.1f}/{float(df['ntx'].iloc[-2]):.1f}->{ntx_gate}, "
           f"ntx_trend={ntx_trend_ok} [{ntx_trend_dbg}]")
    return ok, dbg

def _quality_ok(df, side, bear_mode):
    fk_ok, fk_dbg = fake_filter_v2(df, side=side, bear_mode=bear_mode)
    last = df.iloc[-2]
    ema13 = float(last['ema13']); close = float(last['close']); atrv = float(last['atr'])
    ema13_ok = np.isfinite(atrv) and (abs(close - ema13) <= G3_ENTRY_DIST_EMA13_ATR * atrv)
    return (fk_ok and ema13_ok), f"ff={fk_ok} ({fk_dbg}), ema13_dist_ok={ema13_ok}"

def _structure_ok(df, side):
    ok, why = _bos_only(df, side=side)
    return ok, why

def entry_gate_v3(df, side, adx_last, vote_ntx, ntx_thr, bear_mode, symbol=None):
    band_k = G3_BAND_K
    if bear_mode:
        band_k = max(band_k, 0.30)
    ntx_q = float('nan')
    adx_trend_ok = False
    ntx_trend_ok = False
    ntx_z_last, ntx_z_slope = float('nan'), float('nan')
    if DYNAMIC_MODE:
        band_k_dyn = compute_dynamic_band_k(df, adx_last)
        if bear_mode:
            band_k_dyn = max(band_k_dyn, 0.30)
        band_k = band_k_dyn
        ntx_thr, ntx_q = compute_ntx_local_thr(df, base_thr=ntx_thr, symbol=symbol)
        adx_trend_ok = rising_ema(df['adx'], win=6, pos_ratio_thr=0.6)[0] or robust_up(df['adx'], win=6, pos_ratio_thr=0.6)[0]
        ntx_trend_ok = rising_ema(df['ntx'], win=5, pos_ratio_thr=0.6)[0] or robust_up(df['ntx'], win=5, pos_ratio_thr=0.6)[0]
        vote_ntx_orig = bool(vote_ntx)
        try:
            ntx_z_last = float(df['ntx_z'].iloc[-2]) if 'ntx_z' in df.columns else float('nan')
            if 'ntx_z' in df.columns and len(df['ntx_z']) > G3_NTX_SLOPE_WIN + 2:
                ntx_z_slope = float(df['ntx_z'].iloc[-2] - df['ntx_z'].iloc[-2 - G3_NTX_SLOPE_WIN])
            else:
                ntx_z_slope = float('nan')
        except Exception:
            ntx_z_last, ntx_z_slope = float('nan'), float('nan')
        vote_ntx_dyn = bool(vote_ntx_orig or (
            np.isfinite(ntx_z_last) and (ntx_z_last >= 0.0 or (np.isfinite(ntx_z_slope) and ntx_z_slope > 0.0))
        ))
        vote_ntx = vote_ntx_dyn
    trend_ok, t_dbg = _trend_ok(df, side, band_k, G3_SLOPE_WIN, G3_SLOPE_THR_PCT)
    quality_ok, q_dbg = _quality_ok(df, side, bear_mode)
    mom_ok_base, m_dbg = _momentum_ok(df, side, adx_last, vote_ntx, ntx_thr, bear_mode)
    if DYNAMIC_MODE:
        adx_min = G3_MIN_ADX_BEAR if bear_mode else G3_MIN_ADX
        adx_gate = (np.isfinite(adx_last) and (adx_last >= adx_min))
        mom_ok = bool(mom_ok_base or (adx_trend_ok and (vote_ntx or (ntx_z_slope if np.isfinite(ntx_z_slope) else -1) > 0)) or (ntx_trend_ok and adx_gate))
        m_dbg = f"{m_dbg}, dyn_adx_trend={adx_trend_ok}, dyn_ntx_trend={ntx_trend_ok}, ntx_z_last={ntx_z_last:.2f}, ntx_z_slope={ntx_z_slope:.2f}"
    else:
        mom_ok = mom_ok_base
    structure_ok, s_dbg = _structure_ok(df, side)
    if not trend_ok:
        return False, f"trend_FAIL({t_dbg})"
    if not quality_ok:
        return False, f"quality_FAIL({q_dbg})"
    if bear_mode:
        ok = mom_ok and structure_ok
        dbg = f"bear_mode momentum={mom_ok} ({m_dbg}) structure={structure_ok} ({s_dbg})"
    else:
        ok = mom_ok or structure_ok
        dbg = f"momentum={mom_ok} ({m_dbg}) OR structure={structure_ok} ({s_dbg})"
    if DYNAMIC_MODE and VERBOSE_LOG:
        try:
            dbg_payload = {
                "band_k": round(band_k, 3),
                "ntx_thr": round(ntx_thr, 1),
                "vote_ntx_orig": vote_ntx_orig,
                "vote_ntx_dyn": vote_ntx_dyn,
                "ntx_z_last": None if not np.isfinite(ntx_z_last) else round(ntx_z_last, 2),
                "ntx_z_slope": None if not np.isfinite(ntx_z_slope) else round(ntx_z_slope, 2),
            }
            logger.debug(f"{symbol} DYNDBG " + json.dumps(dbg_payload))
        except Exception:
            pass
        dbg_json = (f"DYNDBG {{band_k:{band_k:.3f}, ntx_thr:{ntx_thr:.1f}, "
                    f"ntx_z_last:{ntx_z_last:.2f}, ntx_z_slope:{ntx_z_slope:.2f}}}")
        dbg = f"{dbg} | {dbg_json}"
    return ok, dbg

def _summarize_coverage(all_symbols):
    total = len(all_symbols)
    # scan_status: {symbol: {'code': <status>, 'detail': ...}}
    codes = {s: scan_status.get(s, {}).get('code') for s in all_symbols}
    ok = sum(1 for c in codes.values() if c == 'completed')
    cooldown = sum(1 for c in codes.values() if c == 'cooldown')
    min_bars = sum(1 for c in codes.values() if c == 'min_bars')
    skip = sum(1 for c in codes.values() if c == 'skip')
    error = sum(1 for c in codes.values() if c == 'error')
    missing = total - sum(1 for c in codes.values() if c is not None)
    return {
        "total": total,
        "ok": ok,
        "cooldown": cooldown,
        "min_bars": min_bars,
        "skip": skip,
        "error": error,
        "missing": missing,
    }

def _log_false_breakdown():
    logger.info("Kriter FALSE dökümü (yüksekten düşüğe):")
    if not crit_total_counts:
        logger.info("  (veri yok)")
        return
    # False sayısına göre azalan sırala
    items = sorted(crit_total_counts.items(),
                   key=lambda kv: crit_false_counts[kv[0]],
                   reverse=True)
    for name, total in items:
        f = crit_false_counts[name]
        pct = (f / total * 100.0) if total else 0.0
        # örnek: " - gate_short: 566/574 (98.6%)"
        logger.info(f" - {name}: {f}/{total} ({pct:.1f}%)")

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe='4h'):
    tz = pytz.timezone('Europe/Istanbul')
    try:
        await mark_status(symbol, "started")
        now = datetime.now(tz)
        until = new_symbol_until.get(symbol)
        if until and now < until:
            await mark_status(symbol, "cooldown", "new_symbol_cooldown")
            return
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.02 * closes
            lows = closes - np.random.rand(200) * 0.02 * closes
            volumes = np.random.rand(200) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            logger.debug(f"Test modu: {symbol} {timeframe}")
        else:
            limit_need = max(150, LOOKBACK_ATR + 80, ADX_PERIOD + 40)
            ohlcv = await fetch_ohlcv_async(symbol, timeframe, limit=limit_need)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            if df is None or df.empty or len(df) < MIN_BARS:
                new_symbol_until[symbol] = now + timedelta(minutes=NEW_SYMBOL_COOLDOWN_MIN)
                await mark_status(symbol, "min_bars", f"bars={len(df) if df is not None else 0}")
                logger.debug(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), cooldown.")
                return
        calc = calculate_indicators(df, symbol, timeframe)
        if not calc or calc[0] is None:
            await mark_status(symbol, "skip", "indicators_failed")
            return
        df, di_condition_long, di_condition_short = calc
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            await mark_status(symbol, "skip", "invalid_atr")
            logger.warning(f"Geçersiz ATR ({symbol} {timeframe}), skip.")
            return
        adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
        atr_z = rolling_z(df['atr'], LOOKBACK_ATR) if 'atr' in df else 0.0
        bear_mode = (adx_last > 25 and df['di_minus'].iloc[-2] > df['di_plus'].iloc[-2]) if pd.notna(adx_last) else False
        rising_adx = adx_rising(df)
        vote_adx = (np.isfinite(adx_last) and adx_last >= ADX_SOFT)
        vote_dir_long = (di_condition_long or rising_adx)
        vote_dir_short = (di_condition_short or rising_adx)
        ntx_thr = ntx_threshold(atr_z)
        vote_ntx = ntx_vote(df, ntx_thr)
        gate_long = (int(vote_adx) + int(vote_dir_long) + int(vote_ntx)) >= 2
        gate_short = (int(vote_adx) + int(vote_dir_short) + int(vote_ntx)) >= 2
        if VERBOSE_LOG:
            logger.debug(f"{symbol} {timeframe} GATE L/S -> ADX:{'✓' if vote_adx else '×'} "
                        f"DIR(L:{'✓' if vote_dir_long else '×'},S:{'✓' if vote_dir_short else '×'}) "
                        f"NTX:{'✓' if vote_ntx else '×'}")
        base_K = FROTH_GUARD_K_ATR
        trend_strong_long = (vote_adx and (di_condition_long or rising_adx))
        trend_strong_short = (vote_adx and (di_condition_short or rising_adx))
        K_long = min(base_K * 1.2, 1.3) if trend_strong_long else base_K
        K_short = min(base_K * 1.2, 1.3) if trend_strong_short else base_K
        ema_gap = abs(float(df['close'].iloc[-2]) - float(df['ema13'].iloc[-2]))
        froth_ok_long = ((ema_gap <= K_long * atr_value) or trend_strong_long) if USE_FROTH_GUARD else True
        froth_ok_short = ((ema_gap <= K_short * atr_value) or trend_strong_short) if USE_FROTH_GUARD else True
        fk_ok_L, fk_dbg_L = fake_filter_v2(df, side="long", bear_mode=bear_mode)
        fk_ok_S, fk_dbg_S = fake_filter_v2(df, side="short", bear_mode=bear_mode)
        if VERBOSE_LOG:
            logger.debug(f"{symbol} {timeframe} FK_LONG {fk_ok_L} | {fk_dbg_L}")
            logger.debug(f"{symbol} {timeframe} FK_SHORT {fk_ok_S} | {fk_dbg_S}")
        if REGIME1_ADX_ADAPTIVE_BAND and np.isfinite(adx_last):
            band_k = 0.20 if adx_last >= 25 else (0.30 if adx_last < 18 else REGIME1_BAND_K_DEFAULT)
        else:
            band_k = REGIME1_BAND_K_DEFAULT
        c2 = float(df['close'].iloc[-2]); c3 = float(df['close'].iloc[-3])
        e89_2 = float(df['ema89'].iloc[-2]); e89_3 = float(df['ema89'].iloc[-3])
        atr2 = float(df['atr'].iloc[-2]); atr3 = float(df['atr'].iloc[-3])
        if REGIME1_REQUIRE_2CLOSE:
            long_band_ok = (c2 > e89_2 + band_k*atr2) and (c3 > e89_3 + band_k*atr3)
            short_band_ok = (c2 < e89_2 - band_k*atr2) and (c3 < e89_3 - band_k*atr3)
        else:
            long_band_ok = (c2 > e89_2 + band_k*atr2)
            short_band_ok = (c2 < e89_2 - band_k*atr2)
        if len(df) > REGIME1_SLOPE_WIN + 2 and pd.notna(df['ema89'].iloc[-2 - REGIME1_SLOPE_WIN]):
            e89_now = float(df['ema89'].iloc[-2])
            e89_then = float(df['ema89'].iloc[-2 - REGIME1_SLOPE_WIN])
            pct_slope = (e89_now - e89_then) / max(abs(e89_then), 1e-12)
        else:
            pct_slope = 0.0
        slope_thr = REGIME1_SLOPE_THR_PCT / 100.0
        only_long = long_band_ok and (pct_slope > slope_thr)
        only_short = short_band_ok and (pct_slope < -slope_thr)
        if VERBOSE_LOG:
            logger.debug(f"{symbol} {timeframe} ADX={adx_last:.2f} band_k={band_k:.2f} "
                        f"LB={long_band_ok} SB={short_band_ok} slope={pct_slope*100:.3f}%")
        e13 = df['ema13']; e34 = df['ema34']
        e13_prev, e34_prev = e13.iloc[-3], e34.iloc[-3]
        e13_last, e34_last = e13.iloc[-2], e34.iloc[-2]
        cross_up_1334 = (pd.notna(e13_prev) and pd.notna(e34_prev) and pd.notna(e13_last) and pd.notna(e34_last)
                         and (e13_prev <= e34_prev) and (e13_last > e34_last))
        cross_dn_1334 = (pd.notna(e13_prev) and pd.notna(e34_prev) and pd.notna(e13_last) and pd.notna(e34_last)
                         and (e13_prev >= e34_prev) and (e13_last < e34_last))
        cross_up_series = (e13.shift(1) <= e34.shift(1)) & (e13 > e34)
        cross_dn_series = (e13.shift(1) >= e34.shift(1)) & (e13 < e34)
        idx_lastbar = len(df) - 2  # karar verilen bar
        close = df['close'].values
        ema89 = df['ema89'].values
        # 1) Son YUKARI kesişimin bar index'i
        idx_up = _last_true_index(cross_up_series, idx_lastbar)
        # 2) Son AŞAĞI kesişimin bar index'i
        idx_dn = _last_true_index(cross_dn_series, idx_lastbar)
        grace_long = False
        if idx_up >= 0:
            # LONG grace: 89 altında 13-34 kesişimi + 8 mum içinde fiyat 89 üstüne
            wrong_side_at_cross = close[idx_up] < ema89[idx_up]
            within = (idx_lastbar - idx_up) <= GRACE_BARS
            if wrong_side_at_cross and within:
                crossed_to_right = np.any(close[idx_up+1:idx_lastbar+1] > ema89[idx_up+1:idx_lastbar+1])
                grace_long = bool(crossed_to_right)
        grace_short = False
        if idx_dn >= 0:
            # SHORT grace: 89 üstünde 13-34 kesişimi + 8 mum içinde fiyat 89 altına
            wrong_side_at_cross = close[idx_dn] > ema89[idx_dn]
            within = (idx_lastbar - idx_dn) <= GRACE_BARS
            if wrong_side_at_cross and within:
                crossed_to_right = np.any(close[idx_dn+1:idx_lastbar+1] < ema89[idx_dn+1:idx_lastbar+1])
                grace_short = bool(crossed_to_right)
        # Rejim + kesişim + istisna (yalın)
        regime_now_long = bool(df['close'].iloc[-2] > df['ema89'].iloc[-2])
        regime_now_short = bool(df['close'].iloc[-2] < df['ema89'].iloc[-2])
        allow_long = (regime_now_long and cross_up_1334) or grace_long
        allow_short = (regime_now_short and cross_dn_1334) or grace_short
        # --- Squeeze Momentum (LazyBear) açık ton ve bar rengi zorunluluğu ---
        smi_val_now = float(df['lb_sqz_val'].iloc[-2]) if pd.notna(df['lb_sqz_val'].iloc[-2]) else np.nan
        smi_val_prev = float(df['lb_sqz_val'].iloc[-3]) if pd.notna(df['lb_sqz_val'].iloc[-3]) else np.nan
        smi_positive = (np.isfinite(smi_val_now) and smi_val_now > 0.0)  # üst yarı
        smi_negative = (np.isfinite(smi_val_now) and smi_val_now < 0.0)  # alt yarı
        smi_up = (np.isfinite(smi_val_now) and np.isfinite(smi_val_prev) and (smi_val_now > smi_val_prev))
        smi_down = (np.isfinite(smi_val_now) and np.isfinite(smi_val_prev) and (smi_val_now < smi_val_prev))
        # Açık tonlar:
        smi_open_green = smi_positive and smi_up  # açık yeşil (long)
        smi_open_red = smi_negative and smi_down  # açık kırmızı (short)
        # Bar rengi:
        is_green = (pd.notna(df['close'].iloc[-2]) and pd.notna(df['open'].iloc[-2]) and (df['close'].iloc[-2] > df['open'].iloc[-2]))
        is_red = (pd.notna(df['close'].iloc[-2]) and pd.notna(df['open'].iloc[-2]) and (df['close'].iloc[-2] < df['open'].iloc[-2]))
        okL, whyL = entry_gate_v3(df, side="long", adx_last=adx_last, vote_ntx=vote_ntx, ntx_thr=ntx_thr, bear_mode=bear_mode, symbol=symbol)
        okS, whyS = entry_gate_v3(df, side="short", adx_last=adx_last, vote_ntx=vote_ntx, ntx_thr=ntx_thr, bear_mode=bear_mode, symbol=symbol)
        # ZORUNLU: LazyBear açık ton + bar rengi
        buy_condition_raw = allow_long and smi_open_green and is_green
        sell_condition_raw = allow_short and smi_open_red and is_red
        buy_condition = gate_long and froth_ok_long and buy_condition_raw and okL
        sell_condition = gate_short and froth_ok_short and sell_condition_raw and okS
        reason = ""
        if buy_condition:
            reason = f"G3 LONG OK | {whyL}"
            if grace_long:
                reason = (reason + " | grace_long_8bar") if reason else "grace_long_8bar"
        elif sell_condition:
            reason = f"G3 SHORT OK | {whyS}"
            if grace_short:
                reason = (reason + " | grace_short_8bar") if reason else "grace_short_8bar"
        if VERBOSE_LOG and (buy_condition or sell_condition):
            logger.debug(f"{symbol} {timeframe} DYNDBG: {reason}")
        criteria = [
            ("gate_long", gate_long),
            ("gate_short", gate_short),
            ("gate_adx", vote_adx),
            ("ntx_vote", vote_ntx),
            ("cross_up_1334", cross_up_1334),
            ("cross_dn_1334", cross_dn_1334),
            ("reg1_long_band_ok", long_band_ok),
            ("reg1_short_band_ok", short_band_ok),
            ("reg1_slope_pos", pct_slope > slope_thr),
            ("reg1_slope_neg", pct_slope < -slope_thr),
            ("grace_long", grace_long),
            ("grace_short", grace_short),
            ("smi_open_green", smi_open_green),
            ("smi_open_red", smi_open_red),
            ("fk_long", fk_ok_L),
            ("fk_short", fk_ok_S),
            ("froth_long", froth_ok_long),
            ("froth_short", froth_ok_short),
            ("is_green", is_green),
            ("is_red", is_red),
            ("regime_now_long", regime_now_long),
            ("regime_now_short", regime_now_short),
            ("allow_long", allow_long),
            ("allow_short", allow_short),
        ]
        await record_crit_batch(criteria)
        if VERBOSE_LOG:
            logger.debug(f"{symbol} {timeframe} buy:{buy_condition} sell:{sell_condition} reason:{reason}")
        if buy_condition and sell_condition:
            await mark_status(symbol, "skip", "conflicting_signals")
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return
        now = datetime.now(tz)
        bar_time = df.index[-2]
        if not isinstance(bar_time, (pd.Timestamp, datetime)):
            bar_time = pd.to_datetime(bar_time, errors="ignore")
        current_pos = signal_cache.get(f"{symbol}_{timeframe}", {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
            'remaining_ratio': 1.0, 'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
            'regime_dir': None, 'last_regime_bar': None
        })
        exit_cross_long = (pd.notna(e13_prev) and pd.notna(e34_prev) and pd.notna(e13_last) and pd.notna(e34_last)
                           and (e13_prev >= e34_prev) and (e13_last < e34_last))
        exit_cross_short = (pd.notna(e13_prev) and pd.notna(e34_prev) and pd.notna(e13_last) and pd.notna(e34_last)
                            and (e13_prev <= e34_prev) and (e13_last > e34_last))
        if (buy_condition or sell_condition) and (current_pos['signal'] is not None):
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] != new_signal:
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(f"{symbol} {timeframe}: REVERSAL CLOSE 🔁 Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': current_pos['signal'], 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                current_pos = signal_cache[f"{symbol}_{timeframe}"]
        if buy_condition and current_pos['signal'] != 'buy':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'buy')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                if VERBOSE_LOG:
                    logger.debug(f"{symbol} {timeframe}: BUY atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price - sl_atr_abs
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    await mark_status(symbol, "skip", "invalid_entry_sl")
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    if VERBOSE_LOG:
                        logger.debug(f"{symbol} {timeframe}: BUY atlandı (anında SL riski) 🚫")
                    await mark_status(symbol, "skip", "instant_sl_risk")
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                    current_pos = {
                        'signal': 'buy',
                        'entry_price': entry_price,
                        'sl_price': sl_price,
                        'tp1_price': tp1_price,
                        'tp2_price': tp2_price,
                        'highest_price': entry_price,
                        'lowest_price': None,
                        'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now,
                        'last_signal_type': 'buy',
                        'entry_time': now,
                        'tp1_hit': False,
                        'tp2_hit': False,
                        'last_bar_time': bar_time,
                        'regime_dir': current_pos.get('regime_dir'),
                        'last_regime_bar': current_pos.get('last_regime_bar')
                    }
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                    await enqueue_message(
                        format_signal_msg(symbol, timeframe, "buy", entry_price, sl_price, tp1_price, tp2_price)
                    )
                    save_state()
        elif sell_condition and current_pos['signal'] != 'sell':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'sell')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                if VERBOSE_LOG:
                    logger.debug(f"{symbol} {timeframe}: SELL atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price + sl_atr_abs
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    await mark_status(symbol, "skip", "invalid_entry_sl")
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    if VERBOSE_LOG:
                        logger.debug(f"{symbol} {timeframe}: SELL atlandı (anında SL riski) 🚫")
                    await mark_status(symbol, "skip", "instant_sl_risk")
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                    current_pos = {
                        'signal': 'sell',
                        'entry_price': entry_price,
                        'sl_price': sl_price,
                        'tp1_price': tp1_price,
                        'tp2_price': tp2_price,
                        'highest_price': None,
                        'lowest_price': entry_price,
                        'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now,
                        'last_signal_type': 'sell',
                        'entry_time': now,
                        'tp1_hit': False,
                        'tp2_hit': False,
                        'last_bar_time': bar_time,
                        'regime_dir': current_pos.get('regime_dir'),
                        'last_regime_bar': current_pos.get('last_regime_bar')
                    }
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                    await enqueue_message(
                        format_signal_msg(symbol, timeframe, "sell", entry_price, sl_price, tp1_price, tp2_price)
                    )
                    save_state()
        if current_pos['signal'] == 'buy':
            current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                await enqueue_message(f"{symbol} {timeframe}: TP1 Hit 🎯 Cur: {fmt_sym(symbol, current_price)} | TP1: {fmt_sym(symbol, current_pos['tp1_price'])} P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi. Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['tp2_hit'] = True
                await enqueue_message(f"{symbol} {timeframe}: TP2 Hit 🎯🎯 Cur: {fmt_sym(symbol, current_price)} | TP2: {fmt_sym(symbol, current_pos['tp2_price'])} P/L: {profit_percent:+.2f}% | %30 kapandı, kalan %40 açık. Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
            if exit_cross_long:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message(f"{symbol} {timeframe}: EMA EXIT (LONG) 🔁 Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message(f"{symbol} {timeframe}: STOP LONG ⛔ Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            signal_cache[f"{symbol}_{timeframe}"] = current_pos
        elif current_pos['signal'] == 'sell':
            current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                await enqueue_message(f"{symbol} {timeframe}: TP1 Hit 🎯 Cur: {fmt_sym(symbol, current_price)} | TP1: {fmt_sym(symbol, current_pos['tp1_price'])} P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi. Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['tp2_hit'] = True
                await enqueue_message(f"{symbol} {timeframe}: TP2 Hit 🎯🎯 Cur: {fmt_sym(symbol, current_price)} | TP2: {fmt_sym(symbol, current_pos['tp2_price'])} P/L: {profit_percent:+.2f}% | %30 kapandı, kalan %40 açık. Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
            if exit_cross_short:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message(f"{symbol} {timeframe}: EMA EXIT (SHORT) 🔁 Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message(f"{symbol} {timeframe}: STOP SHORT ⛔ Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            signal_cache[f"{symbol}_{timeframe}"] = current_pos
        await mark_status(symbol, "completed")
    except Exception as e:
        logger.error(f"{symbol} {timeframe}: Hata: {str(e)}")
        await mark_status(symbol, "error", str(e))

# ================== Ana Döngü (SHARD log + FALSE dökümü, 2 dk bekleme) ==================
async def main():
    # Başlangıç mesajı sadece process başında bir kez gitsin
    if STARTUP_MSG_ENABLED:
        await enqueue_message("Bot başlatıldı! 🚀")
    # Telegram sender task'ini tek sefer başlat
    asyncio.create_task(message_sender())
    # Piyasaları yükle
    await load_markets()
    while True:  # turları sonsuza kadar döndür
        # --- tur başlangıcı: sayaç/situasyon reset ---
        async with _stats_lock:
            scan_status.clear()
        crit_false_counts.clear()
        crit_total_counts.clear()
        try:
            # Sembol setini hazırla ve shard'lara böl
            symbols = await discover_bybit_symbols(linear_only=LINEAR_ONLY, quote_whitelist=QUOTE_WHITELIST)
            random.shuffle(symbols)
            shards = [symbols[i::N_SHARDS] for i in range(N_SHARDS)]
            t_start = time.time()
            # Her shard'ı sırayla çalıştır (tamamlanınca diğerine geç)
            for i, shard in enumerate(shards, start=1):
                # İstediğin formatta tek satır SHARD log'u
                logger.info(f"Shard {i}/{len(shards)} -> {len(shard)} sembol taranacak")
                # Shard içi görevleri topla ve birlikte yürüt
                tasks = [check_signals(symbol, timeframe='4h') for symbol in shard]
                # Bir sembolde hata olursa diğerleri iptal olmasın
                await asyncio.gather(*tasks, return_exceptions=True)
                # Shard'lar arasında küçük nefes payı
                await asyncio.sleep(INTER_BATCH_SLEEP)
            # --- Tur sonu özetleri ---
            cov = _summarize_coverage(symbols)
            logger.debug(
                "Coverage: total={total} | ok={ok} | cooldown={cooldown} | min_bars={min_bars} | "
                "skip={skip} | error={error} | missing={missing}".format(**cov)
            )
            # Kriter FALSE dökümü
            _log_false_breakdown()
            elapsed = time.time() - t_start
            logger.debug(
                "Tur bitti | total={total} | ok={ok} | cooldown={cooldown} | min_bars={min_bars} | "
                "skip={skip} | error={error} | elapsed={elapsed:.1f}s | bekle={wait:.1f}s".format(
                    elapsed=elapsed, wait=float(SCAN_PAUSE_SEC), **cov
                )
            )
        except Exception as e:
            logger.exception(f"Tur genel hatası: {e}")
        # --- Bir sonraki tura kadar 2 dk bekle ---
        await asyncio.sleep(SCAN_PAUSE_SEC)

if __name__ == "__main__":
    asyncio.run(main())
