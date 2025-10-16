import ccxt
import numpy as np
import pandas as pd
import telegram
from telegram.request import HTTPXRequest
from telegram.error import RetryAfter, TimedOut
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
import hashlib
import uuid
import signal
from dataclasses import dataclass, field
from typing import Optional

# ================== Sabitler ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TEST_MODE = False
VERBOSE_LOG = True
STARTUP_MSG_ENABLED = True
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.2
TP_MULTIPLIER2 = 3.5
SL_BUFFER = 0.3
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.04
ADX_PERIOD = 14
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
USE_FROTH_GUARD = False
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
GRACE_BARS = 8
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
G3_ENTRY_DIST_EMA13_ATR = 0.90
G3_FF_MIN_SCORE = 3
G3_FF_MIN_SCORE_BEAR = 4
USE_ROBUST_SLOPE = True
SCAN_PAUSE_SEC = 120
BEAR_ADX_ON = 23
BEAR_ADX_OFF = 20
CLASSIC_MIN_RR = 1.0
# ====== ORDER BLOCK (OB) Ayarları ======
USE_OB_STANDALONE = True
OB_REQUIRE_SMI = False
OB_REQUIRE_G3_GATE = False
OB_MIN_RR = 1.0
OB_FIRST_TOUCH_ONLY = True
OB_LOOKBACK = 30
OB_DISPLACEMENT_ATR = 1.25
OB_BODY_RATIO_MIN = 0.60
OB_RETEST_REQUIRED = False
OB_STOP_ATR_BUFFER = 0.10
OB_HYBRID = False
OB_CONS_ATR_THR = 0.50
OB_CONS_VOL_THR = 1.80
OB_TREND_FILTER = True
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
# ==== R-tabanlı TP/SL planı ====
ADX_TREND_ON = 23
R_MAX_ATR_MULT_RANGE = 2.5
R_MAX_ATR_MULT_TREND = 3.0
TP1_MIN_ATR_GAP_RANGE = 0.8
TP1_MIN_ATR_GAP_TREND = 1.0
# ==== Dip-Tepe Parametreleri ====
DIPTEPE_ATR_LEN = 14
DIPTEPE_K_ATR = 1.25
DIPTEPE_BRK_LEN = 4
DIPTEPE_BRK_BUF_ATR = 0.12
DIPTEPE_SEQ_WIN = 3
DIPTEPE_MIN_SEP = 8
DIPTEPE_BODY_MIN = 0.30
DIPTEPE_A_LOOKBACK = 50
STRONG_TREND_ADX = 28  # 13/34 kesişimi için opsiyonel teyit sınırı
# ==== Zaman dilimi sabiti ====
DEFAULT_TZ = os.getenv("BOT_TZ", "Europe/Istanbul")

# --- MINIMAL RUNTIME PATCH (paste near the top) ---

# Telegram mesaj flood’u için basit throttle ve state kaydı debounce
MESSAGE_THROTTLE_SECS = 20.0
STATE_SAVE_DEBOUNCE_SECS = 2.0

# Kilitler / sayaçlar / cache'ler
_state_lock = threading.Lock()
_stats_lock = asyncio.Lock()
_last_state_save = 0.0

_last_message_hashes = {}        # enqueue_message() için
scan_status = {}                 # tarama durumu özetleri
crit_total_counts = Counter()    # kriter toplam sayılar
crit_false_counts = Counter()    # kriter false sayılar
new_symbol_until = {}            # sembol bazlı cooldown

_ntx_cache_lock = threading.Lock()
ntx_local_cache = {}             # sembol bazlı NTX local threshold cache

# Basit yardımcılar (main/check_signals çağırıyor)
async def mark_status(symbol: str, code: str, detail: Optional[str] = None):
    async with _stats_lock:
        scan_status[symbol] = {"code": code, "detail": detail}

async def record_crit_batch(criteria):
    # criteria: list[ (name:str, ok:bool) ]
    for name, ok in criteria:
        crit_total_counts[name] += 1
        if not ok:
            crit_false_counts[name] += 1
# --- END PATCH ---

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

class MinimalInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno != logging.INFO:
            return True
        msg = str(record.getMessage())
        return (
            msg.startswith("Shard ") or
            msg.startswith("Kriter FALSE dökümü") or
            msg.startswith(" (veri yok)") or
            msg.startswith(" - ")
        )

for h in logger.handlers:
    h.addFilter(MinimalInfoFilter())

logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

# ================== Borsa & Bot ==================
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
    'timeout': 90000
})

MARKETS = {}

_fetch_sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
_rate_lock = asyncio.Lock()
_last_call_ts = 0.0
_rate_penalty_ms = 0.0

async def load_markets():
    global MARKETS
    if not MARKETS:
        MARKETS = await asyncio.to_thread(exchange.load_markets)
    return MARKETS

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
    request=HTTPXRequest(connection_pool_size=20, pool_timeout=30.0)
) if BOT_TOKEN else None

# ================== Global State ==================
signal_cache = {}
message_queue = asyncio.Queue(maxsize=2000)
_fetch_sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
_rate_lock = asyncio.Lock()
_last_call_ts = 0.0
STATE_FILE = 'positions.json'
DT_KEYS = {"last_signal_time", "entry_time", "last_bar_time", "last_regime_bar"}

@dataclass
class PosState:
    signal: Optional[str] = None
    entry_price: Optional[float] = None
    sl_price: Optional[float] = None
    tp1_price: Optional[float] = None
    tp2_price: Optional[float] = None
    highest_price: Optional[float] = None
    lowest_price: Optional[float] = None
    avg_atr_ratio: Optional[float] = None
    remaining_ratio: float = 1.0
    last_signal_time: Optional[datetime] = None
    last_signal_type: Optional[str] = None
    entry_time: Optional[datetime] = None
    tp1_hit: bool = False
    tp2_hit: bool = False
    last_bar_time: Optional[datetime] = None
    regime_dir: Optional[str] = None
    last_regime_bar: Optional[datetime] = None
    trend_on_prev: bool = False
    used_ob_ids: set = field(default_factory=set)
    tp1_pct: float = 0.0
    tp2_pct: float = 0.0
    rest_pct: float = 1.0
    plan_desc: str = ""

def _default_pos_state():
    return PosState().__dict__.copy()

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
                    if 'used_ob_ids' in v and isinstance(v['used_ob_ids'], list):
                        v['used_ob_ids'] = set(v['used_ob_ids'])
            return data
        except Exception as e:
            logger.warning(f"State yüklenemedi: {e}")
            return {}
    return {}

def save_state():
    global _last_state_save
    now = time.time()
    if now - _last_state_save < STATE_SAVE_DEBOUNCE_SECS:
        return
    try:
        with _state_lock:
            payload = dict(signal_cache)
            for k, v in payload.items():
                if isinstance(v, dict) and isinstance(v.get('used_ob_ids'), set):
                    v['used_ob_ids'] = list(v['used_ob_ids'])
            with open(STATE_FILE, 'w') as f:
                json.dump(payload, f, default=_json_default)
            _last_state_save = now
    except Exception as e:
        logger.warning(f"State kaydedilemedi: {e}")

signal_cache = load_state()

# ================== Util ==================
def _safe_tz():
    try:
        return pytz.timezone(DEFAULT_TZ)
    except Exception:
        return pytz.UTC

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
        return exchange.price_to_precision(symbol, float(x))
    except Exception:
        # son çare
        return f"{float(x):.6f}"

def bars_since(mask: pd.Series, idx: int = -2) -> int:
    s = mask.iloc[: idx + 1]
    rev = s.values[::-1]
    return int(np.argmax(rev)) if rev.any() else len(rev)

def format_signal_msg(symbol: str, timeframe: str, side: str,
                     entry: float, sl: float, tp1: float, tp2: float,
                     reason_line: str = "EMA Cross veya BOS veya Order Block",
                     tz_name: str = DEFAULT_TZ) -> str:
    tz = _safe_tz()
    date_str = datetime.now(tz).strftime("%d.%m.%Y")
    title = "BUY (LONG) 🚀" if side == "buy" else "SELL (SHORT) 📉"
    return (
        f"{symbol} {timeframe}: {title}\n"
        f"Sebep: {reason_line}\n"
        f"Entry: {fmt_sym(symbol, entry)}\n"
        f"SL: {fmt_sym(symbol, sl)}\n"
        f"TP1: {fmt_sym(symbol, tp1)}\n"
        f"TP2: {fmt_sym(symbol, tp2)}\n"
        f"Tarih: {date_str}"
    )

def rising_ema(series: pd.Series, win: int = 5, eps: float = 0.0, pos_ratio_thr: float = 0.55, **kwargs):
    if 'pos_ratio_th' in kwargs:
        logger.debug("rising_ema(): 'pos_ratio_th' is deprecated; use 'pos_ratio_thr'.")
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
        logger.debug("robust_up(): 'pos_ratio_th' is deprecated; use 'pos_ratio_thr'.")
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
    vals = s.iloc[:upto_idx+1].values
    for i in range(len(vals)-1, -1, -1):
        if bool(vals[i]):
            return i
    return -1

def regime_mode_from_adx(adx_last: float) -> str:
    return "trend" if (np.isfinite(adx_last) and adx_last >= ADX_TREND_ON) else "range"

def r_tp_plan(mode: str, is_ob: bool, R: float) -> dict:
    if is_ob:
        return dict(tp1_mult=1.0, tp2_mult=None, tp1_pct=0.50, tp2_pct=0.0, rest_pct=0.50, desc="ob")
    if mode == "trend":
        return dict(tp1_mult=1.5, tp2_mult=3.0, tp1_pct=0.30, tp2_pct=0.30, rest_pct=0.40, desc="trend")
    return dict(tp1_mult=1.2, tp2_mult=None, tp1_pct=0.50, tp2_pct=0.0, rest_pct=0.50, desc="range")

def r_plan_guards_ok(mode: str, R: float, atr: float, entry: float, tp1_price: float) -> (bool, str):
    if not all(map(np.isfinite, [R, atr, entry, tp1_price])) or atr <= 0 or R <= 0:
        return False, "nan"
    r_over_atr = R / atr
    if mode == "trend":
        r_cap = R_MAX_ATR_MULT_TREND
        min_gap = TP1_MIN_ATR_GAP_TREND * atr
    else:
        r_cap = R_MAX_ATR_MULT_RANGE
        min_gap = TP1_MIN_ATR_GAP_RANGE * atr
    if r_over_atr > r_cap:
        return False, f"R/ATR>{r_cap:.2f}"
    gap = abs(tp1_price - entry)
    if gap < min_gap:
        return False, f"TP1_gap<{min_gap/atr:.2f}*ATR"
    return True, "ok"

def apply_split_to_state(state: dict, plan: dict):
    state['tp1_pct'] = plan['tp1_pct']
    state['tp2_pct'] = plan.get('tp2_pct', 0.0) or 0.0
    state['rest_pct'] = plan['rest_pct']
    state['plan_desc'] = plan['desc']

def build_reason_text(side: str,
                      cross_up_1334: bool, cross_dn_1334: bool,
                      grace_long: bool, grace_short: bool,
                      structL: bool, structS: bool,
                      obL_ok: bool, obS_ok: bool,
                      dip_recent: bool, top_recent: bool) -> str:
    tags = []
    if side == "buy":
        if cross_up_1334: tags.append("EMA 13/34 Cross (Up)")
        if grace_long:     tags.append("Grace (8 bar)")
        if structL:        tags.append("BOS Long")
        if obL_ok:         tags.append("Order Block Long")
        if dip_recent:     tags.append("Dip onaylı")
    else:
        if cross_dn_1334: tags.append("EMA 13/34 Cross (Down)")
        if grace_short:    tags.append("Grace (8 bar)")
        if structS:        tags.append("BOS Short")
        if obS_ok:         tags.append("Order Block Short")
        if top_recent:     tags.append("Tepe onaylı")
    return " + ".join(tags) if tags else "N/A"

# ================== Mesaj Kuyruğu ==================
async def enqueue_message(text: str, is_retry: bool = False):
    if not BOT_TOKEN or not CHAT_ID or TEST_MODE:
        logger.debug(f"Mesaj atlandı (Telegram kapalı veya TEST_MODE): {text[:50]}...")
        return
    msg_hash = hashlib.md5(text.encode()).hexdigest()
    now = time.time()
    if not is_retry:
        last_sent = _last_message_hashes.get(msg_hash, 0)
        if now - last_sent < MESSAGE_THROTTLE_SECS:
            logger.debug(f"Mesaj throttle edildi: {text[:50]}...")
            return
    try:
        message_queue.put_nowait(text)
        _last_message_hashes[msg_hash] = now
        for h in list(_last_message_hashes.keys()):
            if now - _last_message_hashes[h] > 60.0:
                del _last_message_hashes[h]
    except asyncio.QueueFull:
        logger.warning("Mesaj kuyruğu dolu, mesaj düşürüldü.")

async def message_sender():
    while True:
        message = await message_queue.get()
        try:
            if telegram_bot:
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                await asyncio.sleep(1)
        except (RetryAfter, TimedOut) as e:
            wait_time = getattr(e, 'retry_after', 5) + 2
            logger.warning(f"Telegram: RetryAfter, {wait_time-2}s bekle")
            await asyncio.sleep(wait_time)
            await enqueue_message(message, is_retry=True)
        except Exception as e:
            logger.error(f"Telegram mesaj hatası: {e.__class__.__name__}: {str(e)}")
        message_queue.task_done()

# ================== Rate-limit Dostu Fetch ==================
async def fetch_ohlcv_async(symbol, timeframe, limit):
    """Semaphor'u asla yeniden yaratma; sadece beklemeyi adaptif arttır."""
    global _last_call_ts, _rate_penalty_ms

    max_attempts = 5
    base_ms = RATE_LIMIT_MS  # 200 ms default

    for attempt in range(1, max_attempts + 1):
        try:
            async with _fetch_sem:
                # global hız sınırlaması
                async with _rate_lock:
                    now = asyncio.get_event_loop().time()
                    wait = max(0.0, (_last_call_ts + (base_ms + _rate_penalty_ms)/1000.0) - now)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    _last_call_ts = asyncio.get_event_loop().time()

                # gerçek istek
                data = await asyncio.to_thread(exchange.fetch_ohlcv, symbol, timeframe, None, limit)

            # başarı: ceza kademeli azalsın
            if _rate_penalty_ms > 0:
                _rate_penalty_ms = max(0.0, _rate_penalty_ms * 0.6)  # yumuşak geri sarma
            return data

        except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
            # 429: ceza büyüsün + artan backoff + jitter
            _rate_penalty_ms = min(4000.0, (_rate_penalty_ms * 1.5) + 150.0)
            backoff = 0.8 * attempt + random.random() * 0.6
            await asyncio.sleep(backoff)
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            # ağ hatası: ılımlı backoff + jitter
            backoff = 0.6 * attempt + random.random() * 0.6
            await asyncio.sleep(backoff)

    # tüm denemeler biterse:
    raise ccxt.NetworkError(f"fetch_ohlcv failed after retries: {symbol} {timeframe}")

# ================== Sembol Keşfi ==================
async def discover_bybit_symbols(linear_only=True, quote_whitelist=("USDT",)):
    global MARKETS
    markets = MARKETS or await load_markets()
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
    df['DX'] = (100 * (df['di_plus'] - df['di_minus']).abs() / denom).replace([np.inf, -np.inf], np.nan)
    df['adx'] = df['DX'].ewm(alpha=alpha, adjust=False).mean()
    if VERBOSE_LOG:
        logger.debug(f"ADX calculated: {df['adx'].iloc[-2]:.2f} for {symbol} at {df.index[-2]}")
    return df

def calculate_bb(df, period=20, mult=2.0):
    mid = df['close'].rolling(period).mean()
    std = df['close'].rolling(period).std(ddof=0)
    df['bb_mid']   = mid
    df['bb_std']   = std
    df['bb_upper'] = mid + mult * std
    df['bb_lower'] = mid - mult * std
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
    tr1 = (df['high'] - df['low']).abs()
    tr2 = (df['high'] - df['close'].shift()).abs()
    tr3 = (df['low']  - df['close'].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = tr.ewm(alpha=1/period, adjust=False).mean()
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

def calculate_indicators(df: pd.DataFrame, symbol: str, timeframe: str) -> pd.DataFrame | None:
    if len(df) < MIN_BARS:
        logger.debug(f"{symbol}: Yetersiz veri ({len(df)} mum), skip.")
        return None
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df.set_index('timestamp', inplace=True)
    df.index = df.index.tz_convert(_safe_tz())  # Europe/Istanbul vb.
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, EMA_FAST)
    df['ema34'] = calculate_ema(closes, EMA_MID)
    df['ema89'] = calculate_ema(closes, EMA_SLOW)
    df = calculate_bb(df)
    df = calc_sqzmom_lb(df, length=20, mult_bb=2.0, lengthKC=20, multKC=1.5, use_true_range=True)
    df = calculate_adx(df, symbol)
    df = ensure_atr(df, period=14)
    df = calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60)
    df = calc_ntx(df, period=NTX_PERIOD, k_eff=NTX_K_EFF)
    if 'ntx' in df.columns:
        df['ntx_z'] = _compute_ntx_z(df['ntx'])
    return df

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

def ntx_vote(df: pd.DataFrame, ntx_thr: float) -> bool:
    if 'ntx' not in df.columns:
        return False
    ntx_last = float(df['ntx'].iloc[-2]) if pd.notna(df['ntx'].iloc[-2]) else np.nan
    return bool(np.isfinite(ntx_last) and ntx_last >= ntx_thr)

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

def _ob_trend_filter(df: pd.DataFrame, side: str) -> bool:
    adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
    band_k = compute_dynamic_band_k(df, adx_last)
    c2 = float(df['close'].iloc[-2]); e89 = float(df['ema89'].iloc[-2]); atr2 = float(df['atr'].iloc[-2])
    if not all(map(np.isfinite, [c2, e89, atr2])):
        return False
    return (c2 > e89 + band_k*atr2) if side == "long" else (c2 < e89 - band_k*atr2)

def _momentum_ok(df, side, adx_last, vote_ntx, ntx_thr, bear_mode):
    adx_min = 0 if bear_mode else G3_MIN_ADX
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
    mom_ok_base = (mom_score >= 2)
    dbg = (f"adx>={adx_min}={adx_gate}, ntx_thr={ntx_thr:.1f}/{float(df['ntx'].iloc[-2]):.1f}->{ntx_gate}, "
           f"ntx_trend={ntx_trend_ok} [{ntx_trend_dbg}]")
    if DYNAMIC_MODE:
        adx_min = 0 if bear_mode else G3_MIN_ADX
        adx_gate = (np.isfinite(adx_last) and (adx_last >= adx_min))
        adx_trend_ok = rising_ema(df['adx'], win=6, pos_ratio_thr=0.6)[0] or robust_up(df['adx'], win=6, pos_ratio_thr=0.6)[0]
        ntx_trend_ok = rising_ema(df['ntx'], win=5, pos_ratio_thr=0.6)[0] or robust_up(df['ntx'], win=5, pos_ratio_thr=0.6)[0]
        ntx_z_last = float(df['ntx_z'].iloc[-2]) if 'ntx_z' in df.columns else float('nan')
        ntx_z_slope = float(df['ntx_z'].iloc[-2] - df['ntx_z'].iloc[-2 - G3_NTX_SLOPE_WIN]) if 'ntx_z' in df.columns and len(df['ntx_z']) > G3_NTX_SLOPE_WIN + 2 else float('nan')
        vote_ntx_orig = vote_ntx
        vote_ntx_dyn = bool(vote_ntx_orig or (
            np.isfinite(ntx_z_last) and (ntx_z_last >= 0.0 or (np.isfinite(ntx_z_slope) and ntx_z_slope > 0.0))
        ))
        vote_ntx = vote_ntx_dyn
        mom_ok = bool(mom_ok_base or (adx_trend_ok and (vote_ntx or (ntx_z_slope if np.isfinite(ntx_z_slope) else -1) > 0)) or (ntx_trend_ok and adx_gate))
        dbg = f"{dbg}, dyn_adx_trend={adx_trend_ok}, dyn_ntx_trend={ntx_trend_ok}, ntx_z_last={ntx_z_last:.2f}, ntx_z_slope={ntx_z_slope:.2f}"
    else:
        mom_ok = mom_ok_base
    return mom_ok, dbg

def _quality_ok(df, side, bear_mode):
    fk_ok, fk_dbg = fake_filter_v2(df, side=side, bear_mode=bear_mode)
    last = df.iloc[-2]
    ema13 = float(last['ema13']); close = float(last['close']); atrv = float(last['atr'])
    ema13_ok = np.isfinite(atrv) and (abs(close - ema13) <= G3_ENTRY_DIST_EMA13_ATR * atrv)
    return (fk_ok and ema13_ok), f"ff={fk_ok} ({fk_dbg}), ema13_dist_ok={ema13_ok}"

def _structure_ok(df, side):
    ok, why = _bos_only(df, side=side)
    return ok, why

def _classic_rr_ok(df: pd.DataFrame, side: str, atrv: float, entry: float):
    if not np.isfinite(entry) or not np.isfinite(atrv) or atrv <= 0:
        return False, None, None, None, None
    sl_abs = (SL_MULTIPLIER + SL_BUFFER) * atrv
    if side == "buy":
        sl = entry - sl_abs
        tp1 = entry + (TP_MULTIPLIER1 * atrv)
        tp2 = entry + (TP_MULTIPLIER2 * atrv)
    else:
        sl = entry + sl_abs
        tp1 = entry - (TP_MULTIPLIER1 * atrv)
        tp2 = entry - (TP_MULTIPLIER2 * atrv)
    risk = abs(entry - sl)
    reward = abs(tp1 - entry)
    rr = (reward / (risk + 1e-12)) if risk > 0 else 0.0
    return rr >= CLASSIC_MIN_RR, sl, tp1, tp2, rr

def _tr_series(df):
    high_low = (df['high'] - df['low']).astype(float)
    high_close = (df['high'] - df['close'].shift()).abs().astype(float)
    low_close  = (df['low']  - df['close'].shift()).abs().astype(float)
    return pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

def _true_range(row):
    return max(
        float(row['high'] - row['low']),
        float(abs(row['high'] - row['close_prev'])),
        float(abs(row['low'] - row['close_prev']))
    )

def _ensure_prev_close(df: pd.DataFrame) -> pd.DataFrame:
    if 'close_prev' not in df.columns:
        df['close_prev'] = df['close'].shift(1)
    return df

def _displacement_candle_ok(df: pd.DataFrame, idx: int, side: str) -> bool:
    row = df.iloc[idx]
    if not np.isfinite(row.get('atr', np.nan)):
        return False
    rng = float(row['high'] - row['low'])
    if rng <= 0:
        return False
    body = abs(float(row['close'] - row['open']))
    body_ratio = body / rng
    tr = _true_range(row)
    disp_ok = (tr >= OB_DISPLACEMENT_ATR * float(row['atr'])) and (body_ratio >= OB_BODY_RATIO_MIN)
    if side == 'long':
        return disp_ok and (float(row['close']) > float(row['open']))
    else:
        return disp_ok and (float(row['close']) < float(row['open']))

def _bos_after_displacement(df, side, disp_idx, min_break_atr=G3_BOS_MIN_BREAK_ATR):
    # swing seviyesini kullan
    sh, sl = _last_swing_levels(df)
    atr = df['atr']
    closes = df['close']
    if side == 'long' and sh is not None:
        lvl = sh + min_break_atr * atr.iloc[disp_idx]
        return closes.iloc[disp_idx] > lvl
    if side == 'short' and sl is not None:
        lvl = sl - min_break_atr * atr.iloc[disp_idx]
        return closes.iloc[disp_idx] < lvl
    return False

def _has_imbalance_next(df, side, k):
    # displacement’tan sonraki 1-2 barda karşı wick’e derin dokunmama
    rng = df.iloc[k+1:k+3]
    if rng.empty: return True
    if side == 'long':
        return not (rng['low'] < df['open'].iloc[k])  # kabaca
    else:
        return not (rng['high'] > df['open'].iloc[k])

def _last_opposite_body_zone(df: pd.DataFrame, disp_idx: int, side: str):
    if disp_idx <= 0:
        return None, None, None
    rng = range(disp_idx-1, max(disp_idx-OB_LOOKBACK, -1), -1)
    for i in rng:
        r = df.iloc[i]
        if side == 'long':
            if float(r['close']) < float(r['open']):
                z_high = float(max(r['open'], r['close']))
                z_low = float(min(r['open'], r['close']))
                return z_high, z_low, i
        else:
            if float(r['close']) > float(r['open']):
                z_high = float(max(r['open'], r['close']))
                z_low = float(min(r['open'], r['close']))
                return z_high, z_low, i
    return None, None, None

def _ob_first_touch_reject(df: pd.DataFrame, idx_last: int, side: str, z_high: float, z_low: float) -> bool:
    bar = df.iloc[idx_last]
    o, h, l, c = map(float, (bar['open'], bar['high'], bar['low'], bar['close']))
    if side == 'long':
        touched = (l <= z_high) and (h >= z_low)
        reject = (c > o) and (c > z_high)
        return bool(touched and reject)
    else:
        touched = (h >= z_low) and (l <= z_high)
        reject = (c < o) and (c < z_low)
        return bool(touched and reject)

def _find_displacement_ob(df: pd.DataFrame, side: str):
    if len(df) < max(OB_LOOKBACK+5, 50) or 'atr' not in df.columns:
        return False, "ob_data_short", None, None, None
    df = _ensure_prev_close(df)
    idx_last = len(df) - 2
    for k in range(idx_last-1, max(idx_last-OB_LOOKBACK, 1), -1):
        if _displacement_candle_ok(df, k, side):
            if not _bos_after_displacement(df, side, k):  # k: disp bar index
                continue
            if not _has_imbalance_next(df, side, k):
                continue
            z_high, z_low, opp_idx = _last_opposite_body_zone(df, k, side)
            if z_high is None:
                continue
            if OB_RETEST_REQUIRED:
                post = df.iloc[k+1:idx_last+1]
                if side == 'long' and not (post['low'] <= z_high).any():
                    continue
                if side == 'short' and not (post['high'] >= z_low).any():
                    continue
            if _ob_first_touch_reject(df, idx_last, side, z_high, z_low):
                ob_id = f"{int(df.index[k].value)}_{side}_{round(z_high,6)}_{round(z_low,6)}"
                dbg = f"disp_idx={k}, opp_idx={opp_idx}, zone=[{z_low:.6f},{z_high:.6f}]"
                return True, dbg, z_high, z_low, ob_id
    return False, "no_displacement_ob", None, None, None

def _order_block_cons_fallback(df: pd.DataFrame, side: str, lookback=10) -> (bool, str):
    if not OB_HYBRID:
        return False, "hybrid_off"
    if len(df) < lookback + 3 or 'atr' not in df.columns or 'volume' not in df.columns:
        return False, "cons_data_short"
    idx_last = len(df) - 2
    win = df.iloc[idx_last - lookback:idx_last]
    atr_mean = float(win['atr'].mean())
    vol_mean = float(win['volume'].mean())
    atr_ok = (win['atr'] <= OB_CONS_ATR_THR * atr_mean).mean() >= 0.7
    vol_ok = (win['volume'] >= OB_CONS_VOL_THR * vol_mean).mean() >= 0.7
    if not (atr_ok and vol_ok):
        return False, f"cons_fail atr_ok={atr_ok} vol_ok={vol_ok}"
    c_last = float(df['close'].iloc[idx_last])
    if side == 'long':
        brk = c_last > float(win['high'].max())
    else:
        brk = c_last < float(win['low'].min())
    return bool(brk), f"cons_ok brk={brk}"

def _ob_rr_ok(df: pd.DataFrame, side: str, z_hi: float, z_lo: float):
    if z_hi is None or z_lo is None:
        return False, None, None
    entry = float(df['close'].iloc[-2])
    atrv = float(df['atr'].iloc[-2])
    if not np.isfinite(entry) or not np.isfinite(atrv) or atrv <= 0:
        return False, None, None
    if side == 'long':
        sl = z_lo - OB_STOP_ATR_BUFFER * atrv
        R = abs(entry - sl)
        tp1_r = entry + 1.0 * R
    else:
        sl = z_hi + OB_STOP_ATR_BUFFER * atrv
        R = abs(entry - sl)
        tp1_r = entry - 1.0 * R
    if R <= 0 or not np.isfinite(tp1_r):
        return False, None, None
    reward = abs(tp1_r - entry)
    risk = abs(entry - sl)
    rr = (reward / (risk + 1e-12)) if risk > 0 else 0.0
    return (rr >= OB_MIN_RR), (sl, None, None), (entry, rr)

def _score_side(side, ok_gate, struct_ok, adx_last, ntx_last, ff_ok):
    score = 0
    score += 2 if ok_gate else 0           # G3 gate
    score += 2 if struct_ok else 0         # BOS/OB yapısı
    score += 1 if ((adx_last or 0) >= 23) else 0
    score += 1 if ((ntx_last or 0) >= 50) else 0
    score += 1 if ff_ok else 0             # kalite filtresi
    return score

def _summarize_coverage(all_symbols):
    total = len(all_symbols)
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

def _adaptive_pause(base, errors, ratelims):
    add = min(30, errors*2 + ratelims*4)
    sub = 0 if (errors+ratelims)>0 else 2
    return max(2, base + add - sub)

def _log_false_breakdown():
    logger.info("Kriter FALSE dökümü (yüksekten düşüğe):")
    if not crit_total_counts:
        logger.info(" (veri yok)")
        return
    items = sorted(crit_total_counts.items(),
                   key=lambda kv: crit_false_counts[kv[0]],
                   reverse=True)
    for name, total in items:
        f = crit_false_counts[name]
        pct = (f / total * 100.0) if total else 0.0
        logger.info(f" - {name}: {f}/{total} ({pct:.1f}%)")

# ================== Sinyal Döngüsü ==================
async def entry_gate_v3(df, side, adx_last, vote_ntx, ntx_thr, bear_mode, symbol=None):
    band_k = G3_BAND_K
    ntx_q = float('nan')
    adx_trend_ok = False
    ntx_trend_ok = False
    ntx_z_last, ntx_z_slope = float('nan'), float('nan')
    if DYNAMIC_MODE:
        band_k = compute_dynamic_band_k(df, adx_last)
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
    structure_ok, s_dbg = _structure_ok(df, side)
    if not bear_mode:
        if not trend_ok:
            return False, f"trend_FAIL({t_dbg})"
    if not quality_ok:
        return False, f"quality_FAIL({q_dbg})"
    if bear_mode:
        ok = mom_ok_base and structure_ok
        dbg = f"bear_mode momentum={mom_ok_base} ({m_dbg}) structure={structure_ok} ({s_dbg})"
    else:
        ok = mom_ok_base or structure_ok
        dbg = f"momentum={mom_ok_base} ({m_dbg}) OR structure={structure_ok} ({s_dbg})"
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
                    f"ntx_z_last:{ntx_z_last:.2f}, ntx_z_slope={ntx_z_slope:.2f}}}")
        dbg = f"{dbg} | {dbg_json}"
    return ok, dbg

async def check_signals(symbol: str, timeframe: str = '4h') -> None:
    tz = _safe_tz()
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
        if calc is None:
            await mark_status(symbol, "skip", "indicators_failed")
            return
        df = calc
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            await mark_status(symbol, "skip", "invalid_atr")
            logger.debug(f"Geçersiz ATR ({symbol} {timeframe}), skip.")
            return
        adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
        atr_z = rolling_z(df['atr'], LOOKBACK_ATR) if 'atr' in df else 0.0
        async with _stats_lock:
            trend_prev = signal_cache.get(f"{symbol}_{timeframe}", _default_pos_state()).get('trend_on_prev', False)
        trend_on = (np.isfinite(adx_last) and ((adx_last >= BEAR_ADX_ON) or (trend_prev and adx_last > BEAR_ADX_OFF)))
        bull_mode = bool(trend_on)
        bear_mode = not bull_mode
        cur_key = f"{symbol}_{timeframe}"
        async with _stats_lock:
            st = signal_cache.setdefault(cur_key, _default_pos_state())
            st['used_ob_ids'] = set(st.get('used_ob_ids', []))
            used_set = set(st['used_ob_ids'])  # kopya
            st['trend_on_prev'] = bull_mode
            signal_cache[cur_key] = st
        if VERBOSE_LOG:
            logger.debug(f"MODE bull={bull_mode} | ADX={adx_last:.1f} | ON={BEAR_ADX_ON} OFF={BEAR_ADX_OFF}")
        ntx_thr = ntx_threshold(atr_z)
        vote_ntx = ntx_vote(df, ntx_thr)
        fk_ok_L, fk_dbg_L = fake_filter_v2(df, side="long", bear_mode=bear_mode)
        fk_ok_S, fk_dbg_S = fake_filter_v2(df, side="short", bear_mode=bear_mode)
        smi_val_now = float(df['lb_sqz_val'].iloc[-2]) if pd.notna(df['lb_sqz_val'].iloc[-2]) else np.nan
        smi_val_prev = float(df['lb_sqz_val'].iloc[-3]) if pd.notna(df['lb_sqz_val'].iloc[-3]) else np.nan
        smi_positive = (np.isfinite(smi_val_now) and smi_val_now > 0.0)
        smi_negative = (np.isfinite(smi_val_now) and smi_val_now < 0.0)
        smi_up = (np.isfinite(smi_val_now) and np.isfinite(smi_val_prev) and (smi_val_now > smi_val_prev))
        smi_down = (np.isfinite(smi_val_now) and np.isfinite(smi_val_prev) and (smi_val_now < smi_val_prev))
        smi_open_green = smi_positive and smi_up
        smi_open_red = smi_negative and smi_down
        is_green = (pd.notna(df['close'].iloc[-2]) and pd.notna(df['open'].iloc[-2]) and (df['close'].iloc[-2] > df['open'].iloc[-2]))
        is_red = (pd.notna(df['close'].iloc[-2]) and pd.notna(df['open'].iloc[-2]) and (df['close'].iloc[-2] < df['open'].iloc[-2]))
        obL_ok, obL_dbg, obL_high, obL_low, obL_id = _find_displacement_ob(df, side="long")
        obS_ok, obS_dbg, obS_high, obS_low, obS_id = _find_displacement_ob(df, side="short")
        if OB_HYBRID and not obL_ok:
            hy_ok, hy_dbg = _order_block_cons_fallback(df, side="long", lookback=10)
            if hy_ok:
                obL_ok, obL_dbg = True, f"{obL_dbg}+hybrid({hy_dbg})"
        if OB_HYBRID and not obS_ok:
            hy_ok, hy_dbg = _order_block_cons_fallback(df, side="short", lookback=10)
            if hy_ok:
                obS_ok, obS_dbg = True, f"{obS_dbg}+hybrid({hy_dbg})"
        obL_rr_ok, obL_prices, obL_entry_rr = _ob_rr_ok(df, "long", obL_high, obL_low)
        obS_rr_ok, obS_prices, obS_entry_rr = _ob_rr_ok(df, "short", obS_high, obS_low)
        obL_trend_ok = (not OB_TREND_FILTER) or _ob_trend_filter(df, "long")
        obS_trend_ok = (not OB_TREND_FILTER) or _ob_trend_filter(df, "short")
        obL_smi_ok = (not OB_REQUIRE_SMI) or (smi_open_green and is_green)
        obS_smi_ok = (not OB_REQUIRE_SMI) or (smi_open_red and is_red)
        okL, whyL = await entry_gate_v3(df, side="long", adx_last=adx_last, vote_ntx=vote_ntx, ntx_thr=ntx_thr, bear_mode=bear_mode, symbol=symbol)
        okS, whyS = await entry_gate_v3(df, side="short", adx_last=adx_last, vote_ntx=vote_ntx, ntx_thr=ntx_thr, bear_mode=bear_mode, symbol=symbol)
        obL_gate_ok = (not OB_REQUIRE_G3_GATE) or okL
        obS_gate_ok = (not OB_REQUIRE_G3_GATE) or okS
        obL_touch_ok = (not OB_FIRST_TOUCH_ONLY) or (obL_id and (obL_id not in used_set))
        obS_touch_ok = (not OB_FIRST_TOUCH_ONLY) or (obS_id and (obS_id not in used_set))
        if VERBOSE_LOG:
            logger.debug(f"{symbol} {timeframe} FK_LONG {fk_ok_L} | {fk_dbg_L}")
            logger.debug(f"{symbol} {timeframe} FK_SHORT {fk_ok_S} | {fk_dbg_S}")
            logger.debug(f"{symbol} {timeframe} OB_LONG {obL_ok} | {obL_dbg} | rr_ok={obL_rr_ok} | smi_ok={obL_smi_ok} | gate_ok={obL_gate_ok} | touch_ok={obL_touch_ok} | trend_ok={obL_trend_ok}")
            logger.debug(f"{symbol} {timeframe} OB_SHORT {obS_ok} | {obS_dbg} | rr_ok={obS_rr_ok} | smi_ok={obS_smi_ok} | gate_ok={obS_gate_ok} | touch_ok={obS_touch_ok} | trend_ok={obS_trend_ok}")
        ob_buy_standalone = USE_OB_STANDALONE and obL_ok and obL_rr_ok and obL_smi_ok and obL_gate_ok and obL_touch_ok and obL_trend_ok
        ob_sell_standalone = USE_OB_STANDALONE and obS_ok and obS_rr_ok and obS_smi_ok and obS_gate_ok and obS_touch_ok and obS_trend_ok
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
        e13 = df['ema13']; e34 = df['ema34']
        e13_prev, e34_prev = e13.iloc[-3], e34.iloc[-3]
        e13_last, e34_last = e13.iloc[-2], e34.iloc[-2]
        cross_up_1334 = (pd.notna(e13_prev) and pd.notna(e34_prev) and pd.notna(e13_last) and pd.notna(e34_last)
                         and (e13_prev <= e34_prev) and (e13_last > e34_last))
        cross_dn_1334 = (pd.notna(e13_prev) and pd.notna(e34_prev) and pd.notna(e13_last) and pd.notna(e34_last)
                         and (e13_prev >= e34_prev) and (e13_last < e34_last))
        cross_up_series = (e13.shift(1) <= e34.shift(1)) & (e13 > e34)
        cross_dn_series = (e13.shift(1) >= e34.shift(1)) & (e13 < e34)
        idx_lastbar = len(df) - 2
        close = df['close'].values
        ema89 = df['ema89'].values
        idx_up = _last_true_index(cross_up_series, idx_lastbar)
        idx_dn = _last_true_index(cross_dn_series, idx_lastbar)
        grace_long = False
        if idx_up >= 0:
            wrong_side_at_cross = close[idx_up] < ema89[idx_up]
            within = (idx_lastbar - idx_up) <= GRACE_BARS
            if wrong_side_at_cross and within:
                crossed_to_right = np.any(close[idx_up+1:idx_lastbar+1] > ema89[idx_up+1:idx_lastbar+1])
                grace_long = bool(crossed_to_right)
                if grace_long and VERBOSE_LOG:
                    logger.debug(f"{symbol} {timeframe}: grace_long_8bar aktif")
        grace_short = False
        if idx_dn >= 0:
            wrong_side_at_cross = close[idx_dn] > ema89[idx_dn]
            within = (idx_lastbar - idx_dn) <= GRACE_BARS
            if wrong_side_at_cross and within:
                crossed_to_right = np.any(close[idx_dn+1:idx_lastbar+1] < ema89[idx_dn+1:idx_lastbar+1])
                grace_short = bool(crossed_to_right)
                if grace_short and VERBOSE_LOG:
                    logger.debug(f"{symbol} {timeframe}: grace_short_8bar aktif")
        allow_long = (cross_up_1334) or grace_long
        allow_short = (cross_dn_1334) or grace_short
        structL, _ = _structure_ok(df, side="long")
        structS, _ = _structure_ok(df, side="short")
        entry_price_c = float(df['close'].iloc[-2])
        classic_buy_rr_ok, b_sl, b_tp1, b_tp2, b_rr = _classic_rr_ok(df, "buy", atr_value, entry_price_c)
        classic_sell_rr_ok, s_sl, s_tp1, s_tp2, s_rr = _classic_rr_ok(df, "sell", atr_value, entry_price_c)
        buy_classic = ((allow_long or structL) and smi_open_green and is_green and okL and classic_buy_rr_ok)
        sell_classic = ((allow_short or structS) and smi_open_red and is_red and okS and classic_sell_rr_ok)
        buy_condition = bool(buy_classic or ob_buy_standalone)
        sell_condition = bool(sell_classic or ob_sell_standalone)
        if VERBOSE_LOG:
            if (allow_long or structL) and smi_open_green and is_green and okL and not classic_buy_rr_ok:
                logger.debug(f"{symbol} {timeframe}: RR={b_rr:.2f} < {CLASSIC_MIN_RR} → reddedildi (buy_classic)")
            if (allow_short or structS) and smi_open_red and is_red and okS and not classic_sell_rr_ok:
                logger.debug(f"{symbol} {timeframe}: RR={s_rr:.2f} < {CLASSIC_MIN_RR} → reddedildi (sell_classic)")
            if obL_ok and not obL_rr_ok and obL_entry_rr:
                logger.debug(f"{symbol} {timeframe}: RR={obL_entry_rr[1]:.2f} < {OB_MIN_RR} → reddedildi (ob_buy_standalone)")
            if obS_ok and not obS_rr_ok and obS_entry_rr:
                logger.debug(f"{symbol} {timeframe}: RR={obS_entry_rr[1]:.2f} < {OB_MIN_RR} → reddedildi (ob_sell_standalone)")
        reason = ""
        if buy_condition:
            if ob_buy_standalone:
                reason = f"Order Block (RR={obL_entry_rr[1]:.2f})"
                if OB_REQUIRE_SMI:
                    reason += " + SMI"
                if OB_REQUIRE_G3_GATE:
                    reason += " + G3"
                if OB_TREND_FILTER and obL_trend_ok:
                    reason += " 🟢 Trend"
            else:
                reason = f"EMA Cross veya BOS (RR={b_rr:.2f})"
                if obL_ok:
                    reason += f" veya Order Block (RR={obL_entry_rr[1]:.2f})"
        elif sell_condition:
            if ob_sell_standalone:
                reason = f"Order Block (RR={obS_entry_rr[1]:.2f})"
                if OB_REQUIRE_SMI:
                    reason += " + SMI"
                if OB_REQUIRE_G3_GATE:
                    reason += " + G3"
                if OB_TREND_FILTER and obS_trend_ok:
                    reason += " 🟢 Trend"
            else:
                reason = f"EMA Cross veya BOS (RR={s_rr:.2f})"
                if obS_ok:
                    reason += f" veya Order Block (RR={obS_entry_rr[1]:.2f})"
        if VERBOSE_LOG and (buy_condition or sell_condition):
            logger.debug(f"{symbol} {timeframe} DYNDBG: {reason}")
        criteria = [
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
            ("is_green", is_green),
            ("is_red", is_red),
            ("allow_long", allow_long),
            ("allow_short", allow_short),
            ("order_block_long", obL_ok),
            ("order_block_short", obS_ok),
            ("classic_buy_rr_ok", classic_buy_rr_ok),
            ("classic_sell_rr_ok", classic_sell_rr_ok),
            ("ob_buy_trend_ok", obL_trend_ok),
            ("ob_sell_trend_ok", obS_trend_ok),
        ]
        await record_crit_batch(criteria)
        if buy_condition and sell_condition:
            ntx_last = float(df['ntx'].iloc[-2]) if 'ntx' in df.columns and pd.notna(df['ntx'].iloc[-2]) else 0.0
            buy_score  = _score_side("buy",  okL, structL, adx_last, ntx_last, fk_ok_L)
            sell_score = _score_side("sell", okS, structS, adx_last, ntx_last, fk_ok_S)
            if buy_score != sell_score:
                prefer = "buy" if buy_score > sell_score else "sell"
                buy_condition  = (prefer == "buy")
                sell_condition = (prefer == "sell")
            else:
                # eşitse yine cooldown yap
                new_symbol_until[symbol] = now + timedelta(minutes=NEW_SYMBOL_COOLDOWN_MIN)
                await mark_status(symbol, "skip", "conflicting_signals_tie")
                return
        now = datetime.now(tz)
        bar_time = df.index[-2]
        if not isinstance(bar_time, (pd.Timestamp, datetime)):
            bar_time = pd.to_datetime(bar_time, errors="ignore")
        async with _stats_lock:
            current_pos = signal_cache.setdefault(f"{symbol}_{timeframe}", _default_pos_state())
            base = _default_pos_state()
            for k, v in base.items():
                current_pos.setdefault(k, v)
            if not isinstance(current_pos.get('used_ob_ids'), set):
                current_pos['used_ob_ids'] = set(current_pos.get('used_ob_ids', []))
        same_bar = (pd.Timestamp(current_pos.get('last_bar_time')).value
                    == pd.Timestamp(bar_time).value) if current_pos.get('last_bar_time') else False
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
                async with _stats_lock:
                    signal_cache[f"{symbol}_{timeframe}"] = _default_pos_state()
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_time'] = now
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_type'] = current_pos['signal']
                    signal_cache[f"{symbol}_{timeframe}"]['regime_dir'] = current_pos.get('regime_dir')
                    signal_cache[f"{symbol}_{timeframe}"]['last_regime_bar'] = current_pos.get('last_regime_bar')
                    signal_cache[f"{symbol}_{timeframe}"]['trend_on_prev'] = bull_mode
                    signal_cache[f"{symbol}_{timeframe}"]['used_ob_ids'] = used_set
                save_state()
                async with _stats_lock:
                    current_pos = signal_cache[f"{symbol}_{timeframe}"]
        if buy_condition and current_pos['signal'] != 'buy':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'buy')
            )
            if cooldown_active or same_bar:
                if VERBOSE_LOG:
                    logger.debug(f"{symbol} {timeframe}: BUY atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
                if ob_buy_standalone and obL_prices:
                    sl_price, _, _ = obL_prices
                    if obL_low is not None:
                        sl_price = min(sl_price, obL_low - OB_STOP_ATR_BUFFER * atr_value)
                else:
                    sl_price = b_sl
                R = abs(entry_price - sl_price)
                mode = regime_mode_from_adx(adx_last)
                plan = r_tp_plan(mode=mode, is_ob=ob_buy_standalone, R=R)
                tp1_price = entry_price + plan['tp1_mult'] * R
                tp2_price = (entry_price + plan['tp2_mult'] * R) if plan['tp2_mult'] else None
                ok_guard, why_guard = r_plan_guards_ok(
                    mode=mode, R=R, atr=atr_value, entry=entry_price, tp1_price=tp1_price
                )
                if not ok_guard:
                    if VERBOSE_LOG:
                        logger.debug(f"{symbol} {timeframe}: BUY guard fail: {why_guard}")
                        await enqueue_message(f"{symbol} {timeframe}: BUY reddedildi (guard: {why_guard})")
                    await mark_status(symbol, "skip", f"r_guard_fail: {why_guard}")
                    return
                if not (np.isfinite(entry_price) and np.isfinite(sl_price) and np.isfinite(atr_value)):
                    await mark_status(symbol, "skip", "invalid_entry_sl")
                    logger.debug(f"Geçersiz giriş/SL/ATR ({symbol} {timeframe}), skip.")
                    return
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if not np.isfinite(current_price):
                    await mark_status(symbol, "skip", "invalid_current_price")
                    logger.debug(f"Geçersiz mevcut fiyat ({symbol} {timeframe}), skip.")
                    return
                tr_med = _tr_series(df).rolling(20).median().iloc[-2]
                instant_pad = max(INSTANT_SL_BUFFER * atr_value, 0.4 * tr_med)
                if current_price <= sl_price + instant_pad:
                    if VERBOSE_LOG:
                        logger.debug(f"{symbol} {timeframe}: BUY atlandı (anında SL riski) 🚫")
                    await mark_status(symbol, "skip", "instant_sl_risk")
                    return
                async with _stats_lock:
                    current_pos = _default_pos_state()
                    current_pos.update({
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
                        'last_regime_bar': current_pos.get('last_regime_bar'),
                        'trend_on_prev': bull_mode,
                        'used_ob_ids': used_set
                    })
                    apply_split_to_state(current_pos, plan)
                    if ob_buy_standalone and obL_id:
                        used_set.add(obL_id)
                    st = signal_cache.get(cur_key, _default_pos_state())
                    st.update(current_pos if 'current_pos' in locals() else {})
                    st['used_ob_ids'] = set(used_set)
                    signal_cache[cur_key] = st
                plan_tag = plan['desc']
                msg_reason = ("Order Block" if ob_buy_standalone else build_reason_text(
                    "buy", cross_up_1334, cross_dn_1334, grace_long, grace_short,
                    structL, structS, obL_ok, obS_ok, False, False
                ))
                await enqueue_message(
                    format_signal_msg(symbol, timeframe, "buy",
                                      entry_price, sl_price,
                                      tp1_price, (tp2_price if tp2_price is not None else tp1_price),
                                      reason_line=msg_reason, tz_name=DEFAULT_TZ)
                )
                save_state()
        elif sell_condition and current_pos['signal'] != 'sell':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'sell')
            )
            if cooldown_active or same_bar:
                if VERBOSE_LOG:
                    logger.debug(f"{symbol} {timeframe}: SELL atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
                if ob_sell_standalone and obS_prices:
                    sl_price, _, _ = obS_prices
                    if obS_high is not None:
                        sl_price = max(sl_price, obS_high + OB_STOP_ATR_BUFFER * atr_value)
                else:
                    sl_price = s_sl
                R = abs(entry_price - sl_price)
                mode = regime_mode_from_adx(adx_last)
                plan = r_tp_plan(mode=mode, is_ob=ob_sell_standalone, R=R)
                tp1_price = entry_price - plan['tp1_mult'] * R
                tp2_price = (entry_price - plan['tp2_mult'] * R) if plan['tp2_mult'] else None
                ok_guard, why_guard = r_plan_guards_ok(
                    mode=mode, R=R, atr=atr_value, entry=entry_price, tp1_price=tp1_price
                )
                if not ok_guard:
                    if VERBOSE_LOG:
                        logger.debug(f"{symbol} {timeframe}: SELL guard fail: {why_guard}")
                        await enqueue_message(f"{symbol} {timeframe}: SELL reddedildi (guard: {why_guard})")
                    await mark_status(symbol, "skip", f"r_guard_fail: {why_guard}")
                    return
                if not (np.isfinite(entry_price) and np.isfinite(sl_price) and np.isfinite(atr_value)):
                    await mark_status(symbol, "skip", "invalid_entry_sl")
                    logger.debug(f"Geçersiz giriş/SL/ATR ({symbol} {timeframe}), skip.")
                    return
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if not np.isfinite(current_price):
                    await mark_status(symbol, "skip", "invalid_current_price")
                    logger.debug(f"Geçersiz mevcut fiyat ({symbol} {timeframe}), skip.")
                    return
                tr_med = _tr_series(df).rolling(20).median().iloc[-2]
                instant_pad = max(INSTANT_SL_BUFFER * atr_value, 0.4 * tr_med)
                if current_price >= sl_price - instant_pad:
                    if VERBOSE_LOG:
                        logger.debug(f"{symbol} {timeframe}: SELL atlandı (anında SL riski) 🚫")
                    await mark_status(symbol, "skip", "instant_sl_risk")
                    return
                async with _stats_lock:
                    current_pos = _default_pos_state()
                    current_pos.update({
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
                        'last_regime_bar': current_pos.get('last_regime_bar'),
                        'trend_on_prev': bull_mode,
                        'used_ob_ids': used_set
                    })
                    apply_split_to_state(current_pos, plan)
                    if ob_sell_standalone and obS_id:
                        used_set.add(obS_id)
                    st = signal_cache.get(cur_key, _default_pos_state())
                    st.update(current_pos if 'current_pos' in locals() else {})
                    st['used_ob_ids'] = set(used_set)
                    signal_cache[cur_key] = st
                plan_tag = plan['desc']
                msg_reason = ("Order Block" if ob_sell_standalone else build_reason_text(
                    "sell", cross_up_1334, cross_dn_1334, grace_long, grace_short,
                    structL, structS, obL_ok, obS_ok, False, False
                ))
                await enqueue_message(
                    format_signal_msg(symbol, timeframe, "sell",
                                      entry_price, sl_price,
                                      tp1_price, (tp2_price if tp2_price is not None else tp1_price),
                                      reason_line=msg_reason, tz_name=DEFAULT_TZ)
                )
                save_state()
        if current_pos['signal'] == 'buy':
            current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
            if current_pos['highest_price'] is None or (np.isfinite(current_price) and current_price > current_pos['highest_price']):
                async with _stats_lock:
                    current_pos['highest_price'] = current_price
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
            if not current_pos['tp1_hit'] and np.isfinite(current_price) and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                dec = current_pos.get('tp1_pct', 0.30)
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, current_pos['remaining_ratio'] - dec))
                    current_pos['sl_price'] = current_pos['entry_price']
                    current_pos['tp1_hit'] = True
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯 Cur: {fmt_sym(symbol, current_price)} | "
                    f"TP1: {fmt_sym(symbol, current_pos['tp1_price'])} "
                    f"P/L: {profit_percent:+.2f}% | %{int(dec*100)} kapandı, Stop girişe çekildi. "
                    f"Kalan: %{int(current_pos['remaining_ratio']*100)}"
                )
                save_state()
            if current_pos['tp2_price'] is not None and (not current_pos['tp2_hit']) and np.isfinite(current_price) and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                dec2 = current_pos.get('tp2_pct', 0.30)
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, current_pos['remaining_ratio'] - dec2))
                    current_pos['tp2_hit'] = True
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯 Cur: {fmt_sym(symbol, current_price)} | "
                    f"TP2: {fmt_sym(symbol, current_pos['tp2_price'])} "
                    f"P/L: {profit_percent:+.2f}% | %{int(dec2*100)} kapandı, kalan %{int(current_pos['remaining_ratio']*100)} açık."
                )
                save_state()
            if exit_cross_long:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                    signal_cache[f"{symbol}_{timeframe}"] = _default_pos_state()
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_time'] = now
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_type'] = 'buy'
                    signal_cache[f"{symbol}_{timeframe}"]['regime_dir'] = current_pos.get('regime_dir')
                    signal_cache[f"{symbol}_{timeframe}"]['last_regime_bar'] = current_pos.get('last_regime_bar')
                    signal_cache[f"{symbol}_{timeframe}"]['trend_on_prev'] = bull_mode
                    signal_cache[f"{symbol}_{timeframe}"]['used_ob_ids'] = used_set
                await enqueue_message(f"{symbol} {timeframe}: EMA EXIT (LONG) 🔁 Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
                return
            if np.isfinite(current_price) and current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                    signal_cache[f"{symbol}_{timeframe}"] = _default_pos_state()
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_time'] = now
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_type'] = 'buy'
                    signal_cache[f"{symbol}_{timeframe}"]['regime_dir'] = current_pos.get('regime_dir')
                    signal_cache[f"{symbol}_{timeframe}"]['last_regime_bar'] = current_pos.get('last_regime_bar')
                    signal_cache[f"{symbol}_{timeframe}"]['trend_on_prev'] = bull_mode
                    signal_cache[f"{symbol}_{timeframe}"]['used_ob_ids'] = used_set
                await enqueue_message(f"{symbol} {timeframe}: STOP LONG ⛔ Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
                return
            async with _stats_lock:
                signal_cache[f"{symbol}_{timeframe}"] = current_pos
        elif current_pos['signal'] == 'sell':
            current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
            if current_pos['lowest_price'] is None or (np.isfinite(current_price) and current_price < current_pos['lowest_price']):
                async with _stats_lock:
                    current_pos['lowest_price'] = current_price
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
            if not current_pos['tp1_hit'] and np.isfinite(current_price) and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                dec = current_pos.get('tp1_pct', 0.30)
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, current_pos['remaining_ratio'] - dec))
                    current_pos['sl_price'] = current_pos['entry_price']
                    current_pos['tp1_hit'] = True
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯 Cur: {fmt_sym(symbol, current_price)} | "
                    f"TP1: {fmt_sym(symbol, current_pos['tp1_price'])} "
                    f"P/L: {profit_percent:+.2f}% | %{int(dec*100)} kapandı, Stop girişe çekildi. "
                    f"Kalan: %{int(current_pos['remaining_ratio']*100)}"
                )
                save_state()
            if current_pos['tp2_price'] is not None and (not current_pos['tp2_hit']) and np.isfinite(current_price) and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                dec2 = current_pos.get('tp2_pct', 0.30)
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, current_pos['remaining_ratio'] - dec2))
                    current_pos['tp2_hit'] = True
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯 Cur: {fmt_sym(symbol, current_price)} | "
                    f"TP2: {fmt_sym(symbol, current_pos['tp2_price'])} "
                    f"P/L: {profit_percent:+.2f}% | %{int(dec2*100)} kapandı, kalan %{int(current_pos['remaining_ratio']*100)} açık."
                )
                save_state()
            if exit_cross_short:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                    signal_cache[f"{symbol}_{timeframe}"] = _default_pos_state()
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_time'] = now
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_type'] = 'sell'
                    signal_cache[f"{symbol}_{timeframe}"]['regime_dir'] = current_pos.get('regime_dir')
                    signal_cache[f"{symbol}_{timeframe}"]['last_regime_bar'] = current_pos.get('last_regime_bar')
                    signal_cache[f"{symbol}_{timeframe}"]['trend_on_prev'] = bull_mode
                    signal_cache[f"{symbol}_{timeframe}"]['used_ob_ids'] = used_set
                await enqueue_message(f"{symbol} {timeframe}: EMA EXIT (SHORT) 🔁 Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
                return
            if np.isfinite(current_price) and current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                async with _stats_lock:
                    current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                    signal_cache[f"{symbol}_{timeframe}"] = _default_pos_state()
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_time'] = now
                    signal_cache[f"{symbol}_{timeframe}"]['last_signal_type'] = 'sell'
                    signal_cache[f"{symbol}_{timeframe}"]['regime_dir'] = current_pos.get('regime_dir')
                    signal_cache[f"{symbol}_{timeframe}"]['last_regime_bar'] = current_pos.get('last_regime_bar')
                    signal_cache[f"{symbol}_{timeframe}"]['trend_on_prev'] = bull_mode
                    signal_cache[f"{symbol}_{timeframe}"]['used_ob_ids'] = used_set
                await enqueue_message(f"{symbol} {timeframe}: STOP SHORT ⛔ Price: {fmt_sym(symbol, current_price)} P/L: {profit_percent:+.2f}% Kalan: %{current_pos['remaining_ratio']*100:.0f}")
                save_state()
                return
            async with _stats_lock:
                signal_cache[f"{symbol}_{timeframe}"] = current_pos
        await mark_status(symbol, "completed")
    except Exception as e:
        logger.error(f"{symbol} {timeframe}: Hata: {str(e)}")
        await mark_status(symbol, "error", str(e))

# ================== Ana Döngü ==================
_stop = asyncio.Event()

def _handle_stop():
    _stop.set()

async def main():
    loop = asyncio.get_running_loop()
    try:
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, _handle_stop)
    except NotImplementedError:
        pass

    asyncio.create_task(message_sender())
    if STARTUP_MSG_ENABLED:
        await enqueue_message("Bot başlatıldı! 🚀")
    await load_markets()
    while not _stop.is_set():
        try:
            async with _stats_lock:
                scan_status.clear()
            crit_false_counts.clear()
            crit_total_counts.clear()
            symbols = await discover_bybit_symbols(linear_only=LINEAR_ONLY, quote_whitelist=QUOTE_WHITELIST)
            random.shuffle(symbols)
            shards = [symbols[i::N_SHARDS] for i in range(N_SHARDS)]
            t_start = time.time()
            for i, shard in enumerate(shards, start=1):
                run_id = uuid.uuid4().hex[:6]
                logger.info(f"[{run_id}] Shard {i}/{len(shards)} -> {len(shard)} sembol taranacak")
                tasks = [check_signals(symbol, timeframe='4h') for symbol in shard]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(INTER_BATCH_SLEEP)
                logger.debug(f"[{run_id}] Shard bitti")
            cov = _summarize_coverage(symbols)
            logger.debug(
                "Coverage: total={total} | ok={ok} | cooldown={cooldown} | min_bars={min_bars} | "
                "skip={skip} | error={error} | missing={missing}".format(**cov)
            )
            _log_false_breakdown()
            elapsed = time.time() - t_start
            logger.debug(
                "Tur bitti | total={total} | ok={ok} | cooldown={cooldown} | min_bars={min_bars} | "
                "skip={skip} | error={error} | elapsed={elapsed:.1f}s | bekle={wait:.1f}s".format(
                    elapsed=elapsed, wait=float(SCAN_PAUSE_SEC), **cov
                )
            )
            wait_s = _adaptive_pause(SCAN_PAUSE_SEC, cov['error'], crit_false_counts.get('rate_limit', 0))
            await asyncio.sleep(wait_s)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Tur genel hatası: {e}")
            await asyncio.sleep(SCAN_PAUSE_SEC)
    # kapanış
    await message_queue.join()
    if getattr(exchange, "session", None):
        try: exchange.session.close()
        except: pass
    if telegram_bot:
        try: await telegram_bot.close()
        except: pass
    save_state()
    logger.info("Cleanup tamamlandı, bot kapatıldı.")

if __name__ == "__main__":
    asyncio.run(main())
