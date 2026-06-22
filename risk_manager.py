from __future__ import annotations

"""
risk_manager.py
===============
Javítás [#D]: date.today() → us_trade_date() minden érintett helyen.

Korábban a DayTradeLedger._trade_date() és a RiskManager.__init__()
a gép lokális dátumát (date.today()) használta. Magyarországból futtatva
a CET/CEST időzóna 1–6 órával megelőzi az US/Eastern időzónát, ami azt
eredményezi, hogy a „napi" számlálók az US piaci nap előtt resetelnek,
vagy a zárás körül rossz naphoz rendelik a fill eseményeket.

Megoldás: az us_trade_date() függvény minden esetben az America/New_York
zónában meghatározott dátumot adja vissza (zoneinfo → pytz → UTC-offset
fallback sorrendben).
"""

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from modules.models import RiskDecision
from modules.trading_calendar import us_trade_date  # [#D]


class PositionSizer:
    """Csak méretez; döntést nem hoz."""

    @staticmethod
    def calc_entry_qty(cfg: dict, equity: float, limit_price: float) -> int:
        if limit_price <= 0:
            return 0
        risk_amount = equity * cfg["max_risk_per_trade"]
        risk_per_share = limit_price * cfg["stop_loss_pct"]
        if risk_per_share <= 0:
            return 0
        qty_by_risk = math.floor(risk_amount / risk_per_share)
        qty_by_alloc = math.floor(cfg["allocation_usd"] / limit_price)
        return max(0, min(qty_by_risk, qty_by_alloc))


@dataclass
class DayTradeLedger:
    """
    Konzervatív, long-only day-trade nyilvántartás.

    Javítás [#D]: A _trade_date() statikus metódus mostantól az
    us_trade_date() függvényt hívja ts=None esetén (azaz az aktuális
    ET dátumot adja vissza, nem date.today()-t). Fill timestamp
    esetén az UTC időpontból számít ET dátumot, így kelet-európai
    időzónából futtatva sem csúszik el a nap határán.
    """

    opened_qty_by_day_symbol:   dict[str, dict[str, float]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(float))
    )
    closed_qty_by_day_symbol:   dict[str, dict[str, float]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(float))
    )
    counted_day_trade_symbols:  dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    day_trades_by_date:         dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    total_trades_by_date:       dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )

    @staticmethod
    def _trade_date(ts: datetime | date | str | None = None) -> str:
        """[#D] US/Eastern kereskedési dátum — NEM date.today().

        ts=None → aktuális ET dátum (us_trade_date())
        ts=datetime → az adott UTC időponthoz tartozó ET dátum
        ts=date | str → formátum-normalizálás, feltételezzük, hogy ET
        """
        if ts is None:
            return us_trade_date()                         # [#D]
        if isinstance(ts, datetime):
            # Timezone-aware → UTC-ből ET-be konvertálva
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return us_trade_date(as_of_utc=ts)            # [#D]
        if isinstance(ts, date):
            return str(ts)
        return str(ts)[:10]

    @staticmethod
    def _business_day_window(end_date: str, days: int = 5) -> set[str]:
        end = date.fromisoformat(end_date)
        window: set[str] = set()
        cursor = end
        while len(window) < days:
            if cursor.weekday() < 5:
                window.add(str(cursor))
            cursor -= timedelta(days=1)
        return window

    def register_fill(
        self,
        symbol: str,
        side: str,
        qty: float,
        ts: datetime | date | str | None = None,
    ) -> bool:
        """Fill esemény regisztrálása.

        Visszatérés: True, ha a fill új day-trade eseményt eredményezett.
        """
        if qty <= 0:
            return False

        trade_date = self._trade_date(ts)
        normalized_side   = str(side).lower()
        normalized_symbol = str(symbol).upper()
        self.total_trades_by_date[trade_date] += 1

        if normalized_side == "buy":
            self.opened_qty_by_day_symbol[trade_date][normalized_symbol] += qty
            return False

        if normalized_side != "sell":
            return False

        self.closed_qty_by_day_symbol[trade_date][normalized_symbol] += qty

        opened_today    = self.opened_qty_by_day_symbol[trade_date].get(normalized_symbol, 0.0)
        already_counted = normalized_symbol in self.counted_day_trade_symbols[trade_date]
        if opened_today > 0 and not already_counted:
            self.counted_day_trade_symbols[trade_date].add(normalized_symbol)
            self.day_trades_by_date[trade_date] += 1
            return True
        return False

    def rolling_day_trade_count(self, as_of: str | None = None, business_days: int = 5) -> int:
        as_of = as_of or us_trade_date()                   # [#D]
        window = self._business_day_window(as_of, business_days)
        return sum(v for d, v in self.day_trades_by_date.items() if d in window)

    def rolling_total_trade_count(self, as_of: str | None = None, business_days: int = 5) -> int:
        as_of = as_of or us_trade_date()                   # [#D]
        window = self._business_day_window(as_of, business_days)
        return sum(v for d, v in self.total_trades_by_date.items() if d in window)

    def reset_current_day(self, trade_date: str) -> None:
        self.opened_qty_by_day_symbol.setdefault(trade_date, defaultdict(float))
        self.closed_qty_by_day_symbol.setdefault(trade_date, defaultdict(float))
        self.counted_day_trade_symbols.setdefault(trade_date, set())
        self.day_trades_by_date.setdefault(trade_date, 0)
        self.total_trades_by_date.setdefault(trade_date, 0)


class RiskPolicy:
    def __init__(
        self,
        pdt_safety_margin: float = 0.15,
        *,
        pdt_enabled: bool = True,
        broker_pdt_active: bool = True,
        intraday_margin_safety_margin: float = 0.10,
    ):
        # --- LEGACY (2026-06 előtt) -------------------------------------------
        # A PDT szabályt az Alpaca 2026 közepén megszüntette; a guard
        # alapértelmezetten kikapcsolt (pdt_enabled=False).
        self.pdt_safety_margin = max(0.0, min(float(pdt_safety_margin), 0.75))
        self.pdt_enabled       = pdt_enabled
        self.broker_pdt_active = broker_pdt_active

        # --- ÚJ: Intraday Margin -----------------------------------------------
        self.intraday_margin_safety_margin = max(
            0.0, min(float(intraday_margin_safety_margin), 0.9)
        )

    @staticmethod
    def check_daily_loss(equity: float, start_equity: float | None) -> bool:
        if not start_equity:
            return False
        return (equity - start_equity) / start_equity <= -0.03

    def can_open_intraday(
        self,
        account,
        day_trade_count: int,
        is_live: bool,
        *,
        rolling_day_trade_count: int | None = None,
        rolling_total_trade_count: int | None = None,
    ) -> tuple[bool, str]:
        """LEGACY PDT guard. Alapértelmezetten kikapcsolt (pdt_enabled=False)."""
        if not self.pdt_enabled:
            return True, "pdt_guard_disabled"
        if not self.broker_pdt_active:
            return True, "broker_pdt_inactive"
        if not is_live:
            return True, "paper"
        if float(account.equity) >= 25_000:
            return True, "equity>=25k"

        api_dtc    = getattr(account, "daytrade_count", None)
        broker_dtc = int(api_dtc) if api_dtc is not None else 0
        local_dtc  = rolling_day_trade_count if rolling_day_trade_count is not None else day_trade_count
        dtc        = max(int(local_dtc), broker_dtc)

        total_trades   = rolling_total_trade_count
        projected_dtc  = dtc + 1
        projected_total = (int(total_trades) + 1) if total_trades is not None else None
        projected_ratio = (
            projected_dtc / projected_total
            if projected_total and projected_total > 0
            else None
        )

        block_threshold    = max(1, math.floor(4 * (1 - self.pdt_safety_margin)))
        ratio_would_trigger = projected_ratio is None or projected_ratio > 0.06
        if dtc >= block_threshold and ratio_would_trigger:
            ratio_txt = "unknown" if projected_ratio is None else f"{projected_ratio:.2%}"
            return False, (
                f"pdt_guard dtc={dtc} threshold={block_threshold} "
                f"projected_ratio={ratio_txt}"
            )

        return True, "ok"

    def can_open_intraday_margin(
        self,
        account,
        order_value: float,
    ) -> tuple[bool, str]:
        """ÚJ Intraday Margin guard (Alpaca 2026-os PDT-lecserélés óta)."""
        buying_power = getattr(account, "buying_power", None)
        if buying_power is None:
            return True, "buying_power_unavailable"

        bp = float(buying_power)
        if bp <= 0:
            return False, "buying_power<=0"

        allowed = bp * (1 - self.intraday_margin_safety_margin)
        if order_value > allowed:
            return False, (
                f"intraday_margin_guard order_value={order_value:.2f} "
                f"allowed={allowed:.2f} buying_power={bp:.2f}"
            )

        return True, "ok"


class RiskManager:
    """Koordinátor: méretezés + risk policy + tartós állapot."""

    def __init__(
        self,
        logger,
        state_manager,
        pdt_safety_margin: float = 0.15,
        *,
        is_live: bool = False,
        pdt_enabled: bool = False,
        broker_pdt_active: bool = False,
        intraday_margin_safety_margin: float = 0.10,
    ):
        self.logger  = logger
        self.state   = state_manager
        self.sizer   = PositionSizer()
        self.policy  = RiskPolicy(
            pdt_safety_margin,
            pdt_enabled=pdt_enabled,
            broker_pdt_active=broker_pdt_active,
            intraday_margin_safety_margin=intraday_margin_safety_margin,
        )
        self.is_live        = is_live
        self.day_trade_ledger = DayTradeLedger()

        self.start_equity:          float | None = None
        self.day_trade_count:       int          = 0
        self.circuit_breaker_active: bool        = False
        self.realized_pnl:          float        = 0.0
        self.unrealized_pnl:        float        = 0.0
        # [#3] Nap közbeni max drawdown nyomkövetés
        # equity_high: a nap során elért legmagasabb equity érték
        # max_drawdown_pct: az equity_high-hoz képest a legnagyobb visszaesés
        self.equity_high:           float        = 0.0
        self.max_drawdown_pct:      float        = 0.0

        # [#D] Induláskor ET dátum, nem date.today()
        self.trade_date: str = us_trade_date()

        self._restore_state()

    def _restore_state(self) -> None:
        rs = self.state.load_risk_state(self.trade_date)
        if rs:
            self.start_equity           = rs["start_equity"]
            self.day_trade_count        = rs["day_trade_count"]
            self.circuit_breaker_active = bool(rs["circuit_breaker_active"])
            self.realized_pnl           = float(rs.get("realized_pnl", 0) or 0)
            self.unrealized_pnl         = float(rs.get("unrealized_pnl", 0) or 0)
            self.day_trade_ledger.day_trades_by_date[self.trade_date] = int(
                rs.get("day_trade_count", 0) or 0
            )
            # Drawdown adatok visszatöltése: restart után a napi high és
            # max drawdown folytatódik, nem nulláról indul.
            self.equity_high      = float(rs.get("equity_high", 0) or 0)
            self.max_drawdown_pct = float(rs.get("max_drawdown_pct", 0) or 0)
            # Ha az equity_high még 0 (régi DB sor migráció után),
            # inicializáljuk a start_equity értékével.
            if self.equity_high == 0 and self.start_equity:
                self.equity_high = self.start_equity

    def _persist(self) -> None:
        self.state.save_risk_state(
            self.trade_date,
            self.start_equity or 0,
            self.realized_pnl,
            self.unrealized_pnl,
            self.day_trade_count,
            self.circuit_breaker_active,
            equity_high=self.equity_high,
            max_drawdown_pct=self.max_drawdown_pct,
        )

    def set_start_equity(self, equity: float) -> None:
        if self.start_equity is None:
            self.start_equity = equity
            self.equity_high  = equity  # [#3] kezdeti high = nyitó equity
            self._persist()

    def daily_reset(self, trade_date: str, equity: float) -> None:
        self.trade_date             = trade_date
        self.start_equity           = equity
        self.day_trade_count        = 0
        self.circuit_breaker_active = False
        self.realized_pnl           = 0.0
        self.unrealized_pnl         = 0.0
        self.equity_high            = equity  # [#3] új nap, reset
        self.max_drawdown_pct       = 0.0     # [#3]
        self.day_trade_ledger.reset_current_day(trade_date)
        self._persist()

    def update_equity(self, current_equity: float) -> None:
        """[#3] Nap közbeni equity frissítés — max drawdown nyomkövetés.

        Minden account lekérdezéskor hívható (on_bar, circuit breaker loop).
        Frissíti az equity_high-t és kiszámítja az aktuális max_drawdown_pct-et.

        [#1-fix] Ha az equity_high vagy a max_drawdown_pct ténylegesen
        változott, _persist()-et hív, hogy crash/restart esetén ne vesszen
        el a legfrissebb drawdown-adat. A feltétel (changed flag) megakadályozza
        a felesleges DB-írást, ha az értékek nem változtak.
        """
        changed = False

        if current_equity > self.equity_high:
            self.equity_high = current_equity
            changed = True

        if self.equity_high > 0:
            drawdown = (self.equity_high - current_equity) / self.equity_high * 100
            if drawdown > self.max_drawdown_pct:
                self.max_drawdown_pct = drawdown
                changed = True

        if changed:
            self._persist()

    def evaluate_entry(self, symbol: str, cfg: dict, signal, account) -> RiskDecision:
        equity   = float(account.equity)
        warnings: list[str] = []

        if self.circuit_breaker_active:
            return RiskDecision.block("circuit_breaker_active")

        if self.policy.check_daily_loss(equity, self.start_equity):
            self.circuit_breaker_active = True
            self._persist()
            return RiskDecision.block("daily_loss_-3pct")

        if cfg["mode"] == 0:
            rolling_dtc   = self.day_trade_ledger.rolling_day_trade_count(self.trade_date)
            rolling_total = self.day_trade_ledger.rolling_total_trade_count(self.trade_date)
            ok, reason = self.policy.can_open_intraday(
                account,
                self.day_trade_count,
                self.is_live,
                rolling_day_trade_count=rolling_dtc,
                rolling_total_trade_count=rolling_total,
            )
            if not ok:
                return RiskDecision.block(reason)
            if reason != "ok":
                warnings.append(reason)

        qty = self.sizer.calc_entry_qty(cfg, equity, signal.limit_price)
        if qty <= 0:
            return RiskDecision.block("qty<=0")

        if cfg["mode"] in (0, 2):
            order_value = qty * float(signal.limit_price)
            ok, reason  = self.policy.can_open_intraday_margin(account, order_value)
            if not ok:
                return RiskDecision.block(reason)
            if reason != "ok":
                warnings.append(reason)

        return RiskDecision.allow(max_qty=qty, warnings=warnings)

    def evaluate_circuit_breaker(self, account) -> bool:
        if self.policy.check_daily_loss(float(account.equity), self.start_equity):
            if not self.circuit_breaker_active:
                self.circuit_breaker_active = True
                self._persist()
            return True
        return False

    def on_fill(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float | None = None,
        ts: datetime | date | str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> bool:
        """Fill-alapú PDT frissítés.

        [#D] ts paraméter mostantól az us_trade_date(as_of_utc=ts) útján
        lesz kereskedési dátummá konvertálva — kelet-európai időzónából
        futtatva sem csúszik el a nap határán.
        """
        created_day_trade = self.day_trade_ledger.register_fill(
            symbol, side, float(qty), ts
        )
        self.day_trade_count = self.day_trade_ledger.rolling_day_trade_count(
            self.trade_date
        )
        if created_day_trade:
            self._persist()
        return created_day_trade

    def register_day_trade_round_trip(self) -> None:
        """Backward-compatible kézi regisztráció."""
        self.day_trade_count += 1
        self.day_trade_ledger.day_trades_by_date[self.trade_date] = self.day_trade_count
        self._persist()
