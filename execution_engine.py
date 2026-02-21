"""
TCE Execution Engine v1.1
Bybit perpetual futures otomatik al-sat.
Pozisyon: Bakiyenin %10'u margin. TP1 %30 + TP2 %30 + EMA exit %40.
"""

import ccxt, asyncio, logging, time, json, os
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("execution_engine")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(_h)


@dataclass
class ExecConfig:
    api_key: str = ""
    api_secret: str = ""
    testnet: bool = False
    position_size_pct: float = 10.0
    max_open_positions: int = 4
    default_leverage: int = 5
    max_daily_loss_pct: float = 15.0
    max_consecutive_losses: int = 3
    tp1_close_pct: float = 0.30
    tp2_close_pct: float = 0.30
    kill_switch_file: str = "KILL_SWITCH"
    exec_state_file: str = "exec_positions.json"


@dataclass
class LivePosition:
    symbol: str
    side: str
    entry_price: float
    qty: float
    margin_used: float
    sl_price: float
    tp1_price: float
    tp2_price: Optional[float] = None
    sl_order_id: Optional[str] = None
    tp1_order_id: Optional[str] = None
    tp2_order_id: Optional[str] = None
    tp1_hit: bool = False
    tp2_hit: bool = False
    remaining_qty: float = 0.0
    opened_at: str = ""
    signal_reason: str = ""
    plan_desc: str = ""
    tp1_close_pct: float = 0.30
    tp2_close_pct: float = 0.30


class ExecutionEngine:

    def __init__(self, config=None):
        self.config = config or ExecConfig()
        self._load_env()
        self.exchange = self._init_exchange()
        self.positions = {}
        self._load_state()
        self.daily_pnl = 0.0
        self.daily_pnl_reset_date = ""
        self.consecutive_losses = 0
        self._lock = asyncio.Lock()
        mode = "TESTNET" if self.config.testnet else "CANLI"
        logger.info(f"ExecutionEngine | {mode} | margin=%{self.config.position_size_pct} | lev={self.config.default_leverage}x")

    def _load_env(self):
        if not self.config.api_key:
            self.config.api_key = os.getenv("BYBIT_API_KEY", "")
        if not self.config.api_secret:
            self.config.api_secret = os.getenv("BYBIT_API_SECRET", "")

    def _init_exchange(self):
        p = {'apiKey': self.config.api_key, 'secret': self.config.api_secret,
             'enableRateLimit': True, 'options': {'defaultType': 'swap', 'defaultSubType': 'linear'}, 'timeout': 30000}
        if self.config.testnet:
            p['sandbox'] = True
        return ccxt.bybit(p)

    def is_killed(self):
        return os.path.exists(self.config.kill_switch_file)

    def activate_kill_switch(self, reason="manual"):
        with open(self.config.kill_switch_file, 'w') as f:
            f.write(f"KILLED {datetime.now().isoformat()} | {reason}\n")
        logger.critical(f"KILL SWITCH: {reason}")

    def deactivate_kill_switch(self):
        if os.path.exists(self.config.kill_switch_file):
            os.remove(self.config.kill_switch_file)

    def _check_daily_reset(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.daily_pnl_reset_date != today:
            self.daily_pnl = 0.0
            self.daily_pnl_reset_date = today

    def _circuit_breaker_ok(self):
        if self.is_killed():
            return False, "kill_switch"
        self._check_daily_reset()
        if self.daily_pnl < 0 and abs(self.daily_pnl) >= self.config.max_daily_loss_pct:
            self.activate_kill_switch(f"daily_loss_{self.daily_pnl:.1f}%")
            return False, "daily_loss"
        if self.consecutive_losses >= self.config.max_consecutive_losses:
            self.activate_kill_switch(f"consec_loss_{self.consecutive_losses}")
            return False, "consecutive_losses"
        if len(self.positions) >= self.config.max_open_positions:
            return False, "max_positions"
        return True, "ok"

    async def get_balance(self):
        try:
            bal = await asyncio.to_thread(self.exchange.fetch_balance)
            usdt = bal.get('USDT', {})
            free = float(usdt.get('free', 0) or 0)
            total = float(usdt.get('total', 0) or 0)
            logger.info(f"Bakiye: free={free:.2f} total={total:.2f} USDT")
            return free
        except Exception as e:
            logger.error(f"Bakiye hatasi: {e}")
            return 0.0

    def calculate_position_size(self, balance, entry_price, leverage=None):
        lev = leverage or self.config.default_leverage
        margin = balance * (self.config.position_size_pct / 100.0)
        notional = margin * lev
        qty = notional / entry_price if entry_price > 0 else 0.0
        logger.info(f"Pozisyon: {balance:.2f}$ x %{self.config.position_size_pct} = {margin:.2f}$ margin, {notional:.2f}$ notional, qty={qty:.6f}")
        return qty, margin

    def _round_qty(self, symbol, qty):
        try:
            m = self.exchange.market(symbol)
            mi = m.get('limits', {}).get('amount', {}).get('min', 0)
            pr = m.get('precision', {}).get('amount', 8)
            if isinstance(pr, (int, float)) and pr < 1:
                qty = round(qty / pr) * pr
            else:
                qty = round(qty, int(pr))
            return 0.0 if qty < mi else qty
        except Exception:
            return round(qty, 6)

    def _round_price(self, symbol, price):
        try:
            return float(self.exchange.price_to_precision(symbol, price))
        except Exception:
            return round(price, 8)

    async def set_leverage(self, symbol, leverage=None):
        lev = leverage or self.config.default_leverage
        try:
            await asyncio.to_thread(self.exchange.set_leverage, lev, symbol, {'category': 'linear'})
        except Exception as e:
            if "not modified" not in str(e).lower():
                logger.warning(f"Leverage hatasi: {e}")

    async def set_margin_mode(self, symbol, mode="isolated"):
        try:
            await asyncio.to_thread(self.exchange.set_margin_mode, mode, symbol, {'category': 'linear'})
        except Exception as e:
            if "not modified" not in str(e).lower():
                logger.warning(f"Margin hatasi: {e}")

    async def open_long(self, symbol, entry, sl, tp1, tp2=None, reason="", plan_desc="trend", tp1_pct=None, tp2_pct=None):
        """Returns (success: bool, reason: str)"""
        return await self._open_position(symbol, "long", entry, sl, tp1, tp2, reason, plan_desc, tp1_pct, tp2_pct)

    async def open_short(self, symbol, entry, sl, tp1, tp2=None, reason="", plan_desc="trend", tp1_pct=None, tp2_pct=None):
        """Returns (success: bool, reason: str)"""
        return await self._open_position(symbol, "short", entry, sl, tp1, tp2, reason, plan_desc, tp1_pct, tp2_pct)

    async def _open_position(self, symbol, side, entry, sl, tp1, tp2=None, reason="", plan_desc="trend", tp1_pct=None, tp2_pct=None):
        async with self._lock:
            # None/NaN guard
            try:
                entry = float(entry) if entry is not None else 0.0
                sl = float(sl) if sl is not None else 0.0
                tp1 = float(tp1) if tp1 is not None else 0.0
                tp2 = float(tp2) if tp2 is not None else None
            except (TypeError, ValueError) as e:
                return False, f"gecersiz fiyat: {e}"
            if entry <= 0 or sl <= 0 or tp1 <= 0:
                return False, f"fiyat 0/None (entry={entry}, sl={sl}, tp1={tp1})"
            ok, why = self._circuit_breaker_ok()
            if not ok:
                logger.warning(f"BLOCK {symbol}: {why}")
                return False, f"circuit_breaker: {why}"
            if symbol in self.positions:
                logger.warning(f"{symbol}: zaten acik")
                return False, "zaten acik pozisyon var"
            try:
                balance = await self.get_balance()
                if balance <= 0:
                    return False, f"bakiye yetersiz: {balance}"
                await asyncio.to_thread(self.exchange.load_markets)
                await self.set_margin_mode(symbol, "isolated")
                await self.set_leverage(symbol)
                qty, margin_used = self.calculate_position_size(balance, entry)
                qty = self._round_qty(symbol, qty)
                if qty <= 0:
                    return False, f"qty=0 (margin={margin_used:.2f}$, entry={entry})"

                order_side = "buy" if side == "long" else "sell"
                logger.info(f"OPEN {symbol} {side.upper()} qty={qty} entry~{entry} SL={sl} TP1={tp1} TP2={tp2}")
                entry_order = await asyncio.to_thread(self.exchange.create_order, symbol, "market", order_side, qty)
                actual_entry = float(entry_order.get('average') or entry_order.get('price') or entry)
                actual_qty = float(entry_order.get('filled') or qty)

                sl_price = self._round_price(symbol, sl)
                sl_side = "sell" if side == "long" else "buy"
                sl_order = await asyncio.to_thread(
                    self.exchange.create_order, symbol, "market", sl_side, actual_qty, sl_price,
                    {'stopPrice': sl_price, 'triggerPrice': sl_price, 'reduceOnly': True, 'stopLoss': sl_price})
                sl_oid = sl_order.get('id', '')

                _tp1p = tp1_pct or self.config.tp1_close_pct
                tp1_qty = self._round_qty(symbol, actual_qty * _tp1p)
                tp1_pr = self._round_price(symbol, tp1)
                tp1_oid = None
                if tp1_qty > 0:
                    try:
                        r = await asyncio.to_thread(self.exchange.create_order, symbol, "limit", sl_side, tp1_qty, tp1_pr, {'reduceOnly': True})
                        tp1_oid = r.get('id', '')
                    except Exception as e:
                        logger.warning(f"TP1 emir hatasi: {e}")

                _tp2p = tp2_pct or self.config.tp2_close_pct
                tp2_oid = None
                if tp2 is not None:
                    tp2_qty = self._round_qty(symbol, actual_qty * _tp2p)
                    tp2_pr = self._round_price(symbol, tp2)
                    if tp2_qty > 0:
                        try:
                            r = await asyncio.to_thread(self.exchange.create_order, symbol, "limit", sl_side, tp2_qty, tp2_pr, {'reduceOnly': True})
                            tp2_oid = r.get('id', '')
                        except Exception as e:
                            logger.warning(f"TP2 emir hatasi: {e}")

                self.positions[symbol] = LivePosition(
                    symbol=symbol, side=side, entry_price=actual_entry, qty=actual_qty,
                    margin_used=margin_used, sl_price=sl_price, tp1_price=tp1_pr,
                    tp2_price=tp2, sl_order_id=sl_oid, tp1_order_id=tp1_oid,
                    tp2_order_id=tp2_oid, remaining_qty=actual_qty,
                    opened_at=datetime.now().isoformat(), signal_reason=reason,
                    plan_desc=plan_desc, tp1_close_pct=_tp1p, tp2_close_pct=_tp2p)
                self._save_state()
                return True, "ok"
            except Exception as e:
                logger.exception(f"OPEN FAIL {symbol}: {e}")
                await self._emergency_cleanup(symbol)
                return False, str(e)[:150]

    async def handle_tp1_hit(self, symbol):
        async with self._lock:
            pos = self.positions.get(symbol)
            if not pos or pos.tp1_hit:
                return False
            try:
                closed = self._round_qty(symbol, pos.qty * pos.tp1_close_pct)
                pos.remaining_qty = self._round_qty(symbol, pos.qty - closed)
                pos.tp1_hit = True
                if pos.sl_order_id:
                    try:
                        await asyncio.to_thread(self.exchange.cancel_order, pos.sl_order_id, symbol)
                    except Exception:
                        pass
                if pos.remaining_qty > 0:
                    new_sl = self._round_price(symbol, pos.entry_price)
                    sl_side = "sell" if pos.side == "long" else "buy"
                    r = await asyncio.to_thread(
                        self.exchange.create_order, symbol, "market", sl_side, pos.remaining_qty, new_sl,
                        {'stopPrice': new_sl, 'triggerPrice': new_sl, 'reduceOnly': True, 'stopLoss': new_sl})
                    pos.sl_order_id = r.get('id', '')
                    pos.sl_price = new_sl
                    logger.info(f"TP1 {symbol}: SL girise cekildi @ {new_sl}")
                self.consecutive_losses = 0
                self.positions[symbol] = pos
                self._save_state()
                return True
            except Exception as e:
                logger.exception(f"TP1 FAIL {symbol}: {e}")
                return False

    async def handle_tp2_hit(self, symbol):
        async with self._lock:
            pos = self.positions.get(symbol)
            if not pos or pos.tp2_hit or not pos.tp1_hit:
                return False
            try:
                closed = self._round_qty(symbol, pos.qty * pos.tp2_close_pct)
                pos.remaining_qty = self._round_qty(symbol, pos.remaining_qty - closed)
                pos.tp2_hit = True
                if pos.remaining_qty > 0 and pos.sl_order_id:
                    try:
                        await asyncio.to_thread(self.exchange.cancel_order, pos.sl_order_id, symbol)
                        sl_side = "sell" if pos.side == "long" else "buy"
                        new_sl = self._round_price(symbol, pos.entry_price)
                        r = await asyncio.to_thread(
                            self.exchange.create_order, symbol, "market", sl_side, pos.remaining_qty, new_sl,
                            {'stopPrice': new_sl, 'triggerPrice': new_sl, 'reduceOnly': True, 'stopLoss': new_sl})
                        pos.sl_order_id = r.get('id', '')
                    except Exception:
                        pass
                self.positions[symbol] = pos
                self._save_state()
                logger.info(f"TP2 {symbol}: kalan %40={pos.remaining_qty}")
                return True
            except Exception as e:
                logger.exception(f"TP2 FAIL {symbol}: {e}")
                return False

    async def handle_ema_exit(self, symbol, current_price):
        async with self._lock:
            pos = self.positions.get(symbol)
            if not pos:
                return False
            try:
                await self._cancel_all_orders(symbol)
                if pos.remaining_qty > 0:
                    cs = "sell" if pos.side == "long" else "buy"
                    await asyncio.to_thread(self.exchange.create_order, symbol, "market", cs, pos.remaining_qty, None, {'reduceOnly': True})
                    pnl = ((current_price - pos.entry_price) / pos.entry_price * 100) if pos.side == "long" else ((pos.entry_price - current_price) / pos.entry_price * 100)
                    if pnl < 0:
                        self.consecutive_losses += 1
                    else:
                        self.consecutive_losses = 0
                    logger.info(f"EMA EXIT {symbol} @ {current_price} P/L:{pnl:+.2f}%")
                del self.positions[symbol]
                self._save_state()
                return True
            except Exception as e:
                logger.exception(f"EMA EXIT FAIL {symbol}: {e}")
                return False

    async def handle_sl_hit(self, symbol, current_price):
        async with self._lock:
            pos = self.positions.get(symbol)
            if not pos:
                return False
            try:
                await self._cancel_all_orders(symbol)
                pnl = ((current_price - pos.entry_price) / pos.entry_price * 100) if pos.side == "long" else ((pos.entry_price - current_price) / pos.entry_price * 100)
                ratio = pos.remaining_qty / pos.qty if pos.qty > 0 else 0
                self.daily_pnl += pnl * ratio
                if pnl < 0:
                    self.consecutive_losses += 1
                else:
                    self.consecutive_losses = 0
                logger.info(f"SL {symbol} @ {current_price} P/L:{pnl:+.2f}% streak={self.consecutive_losses}")
                del self.positions[symbol]
                self._save_state()
                return True
            except Exception as e:
                logger.exception(f"SL FAIL {symbol}: {e}")
                return False

    async def close_all_positions(self, reason="manual"):
        n = 0
        for sym in list(self.positions.keys()):
            try:
                pos = self.positions[sym]
                await self._cancel_all_orders(sym)
                if pos.remaining_qty > 0:
                    cs = "sell" if pos.side == "long" else "buy"
                    await asyncio.to_thread(self.exchange.create_order, sym, "market", cs, pos.remaining_qty, None, {'reduceOnly': True})
                del self.positions[sym]
                n += 1
            except Exception:
                pass
        self._save_state()
        return n

    async def sync_positions(self):
        for sym in list(self.positions.keys()):
            pos = self.positions[sym]
            try:
                oo = await asyncio.to_thread(self.exchange.fetch_open_orders, sym)
                ids = {o['id'] for o in oo}
                if not pos.tp1_hit and pos.tp1_order_id and pos.tp1_order_id not in ids:
                    await self.handle_tp1_hit(sym)
                if pos.tp1_hit and not pos.tp2_hit and pos.tp2_order_id and pos.tp2_order_id not in ids:
                    await self.handle_tp2_hit(sym)
                try:
                    bp = await asyncio.to_thread(self.exchange.fetch_position, sym)
                    bq = abs(float(bp.get('contracts', 0) or 0))
                    if bq == 0 and sym in self.positions:
                        del self.positions[sym]
                        self._save_state()
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"Sync {sym}: {e}")

    async def _cancel_all_orders(self, symbol):
        try:
            for o in await asyncio.to_thread(self.exchange.fetch_open_orders, symbol):
                try:
                    await asyncio.to_thread(self.exchange.cancel_order, o['id'], symbol)
                except Exception:
                    pass
        except Exception:
            pass

    async def _emergency_cleanup(self, symbol):
        try:
            await self._cancel_all_orders(symbol)
            try:
                bp = await asyncio.to_thread(self.exchange.fetch_position, symbol)
                q = abs(float(bp.get('contracts', 0) or 0))
                if q > 0:
                    cs = "sell" if bp.get('side', '') == "long" else "buy"
                    await asyncio.to_thread(self.exchange.create_order, symbol, "market", cs, q, None, {'reduceOnly': True})
            except Exception:
                pass
        except Exception:
            pass
        self.positions.pop(symbol, None)

    def _save_state(self):
        try:
            data = {s: vars(p) for s, p in self.positions.items()}
            data['_meta'] = {'daily_pnl': self.daily_pnl, 'daily_pnl_reset_date': self.daily_pnl_reset_date,
                             'consecutive_losses': self.consecutive_losses, 'saved_at': datetime.now().isoformat()}
            with open(self.config.exec_state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"State save: {e}")

    def _load_state(self):
        if not os.path.exists(self.config.exec_state_file):
            return
        try:
            with open(self.config.exec_state_file, 'r') as f:
                data = json.load(f)
            meta = data.pop('_meta', {})
            self.daily_pnl = meta.get('daily_pnl', 0.0)
            self.daily_pnl_reset_date = meta.get('daily_pnl_reset_date', '')
            self.consecutive_losses = meta.get('consecutive_losses', 0)
            for s, d in data.items():
                self.positions[s] = LivePosition(**d)
        except Exception:
            pass

    def get_status(self):
        return (f"Pos:{len(self.positions)}/{self.config.max_open_positions} "
                f"PnL:{self.daily_pnl:+.1f}% Streak:{self.consecutive_losses}")
