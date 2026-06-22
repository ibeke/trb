from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class OrderState(str, Enum):
    """Lokális, broker-független order állapotok.

    Értékek és Alpaca státusz → lokális leképezés:

      INTENT          — létrehozva, de még nem küldtük el a brokernek
      SUBMITTED       — elküldve / aktív / különféle "nyitott" Alpaca állapotok:
                          new, accepted, pending_new, pending_replace,
                          accepted_for_bidding, stopped, replaced,
                          pending_cancel (*),  pending_review (*), held (*)
      PARTIALLY_FILLED — részlegesen teljesült, még nyitott
      FILLED          — teljesen teljesült (záró állapot)
      CANCELED        — törölt (záró állapot)
      REJECTED        — elutasított (záró állapot)
      EXPIRED         — lejárt / done_for_day (záró állapot)
      UNKNOWN         — ismeretlen / nem mappelt Alpaca státusz

    (*) [#3-fix] Korábban hiányzó állapotok — részletes indoklás lent.
    """

    UNKNOWN          = "unknown"
    INTENT           = "intent"
    SUBMITTED        = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED           = "filled"
    CANCELED         = "canceled"
    REJECTED         = "rejected"
    EXPIRED          = "expired"

    @classmethod
    def from_alpaca(cls, status) -> "OrderState":
        """Alpaca order státusz → lokális OrderState.

        [#2-fix] A bemeneti status paraméter mostantól normalize_order_status()
        függvényen megy keresztül, mielőtt a mapping-be kerül. Ez biztosítja,
        hogy az Alpaca SDK enum típusú visszatérési értéke (pl. OrderStatus.NEW)
        éppúgy kezelt legyen, mint a string "new":

            str(OrderStatus.NEW).lower()  →  "orderstatus.new"  →  UNKNOWN ✗
            normalize_order_status(OrderStatus.NEW)  →  "new"   →  SUBMITTED ✓

        A normalize_order_status() előbb a .value attribútumot próbálja
        (enum esetén ez adja a "new" string értéket), majd az utolsó
        pont utáni részt veszi (str repr fallback).

        Ez a javítás egyszerre véd minden hívási helyen:
          - main.py trade update stream
          - execution_engine.py order refresh és reconciliation
          - state_manager.py broker recovery
          - bracket child order feldolgozás

        Leképezések:
          Nyitott:         new, accepted, pending_new, pending_replace,
                           accepted_for_bidding, stopped, replaced,
                           pending_cancel, pending_review, held  → SUBMITTED
          Részleges fill:  partially_filled                       → PARTIALLY_FILLED
          Záró:            filled                                 → FILLED
                           canceled / cancelled                   → CANCELED
                           rejected                               → REJECTED
                           expired / done_for_day                 → EXPIRED
          Ismeretlen:      suspended, calculated, egyéb           → UNKNOWN
        """
        # normalize_order_status() egyformán kezeli:
        #   - str inputot:   "new" → "new", "OrderStatus.NEW" → "new"
        #   - enum inputot:  OrderStatus.NEW (.value = "new") → "new"
        normalized = normalize_order_status(status)
        mapping = {
            # ── Nyitott / aktív állapotok ──────────────────────────────────
            "new":                   cls.SUBMITTED,
            "accepted":              cls.SUBMITTED,
            "pending_new":           cls.SUBMITTED,
            "pending_replace":       cls.SUBMITTED,
            "accepted_for_bidding":  cls.SUBMITTED,
            "stopped":               cls.SUBMITTED,
            "replaced":              cls.SUBMITTED,
            "pending_cancel":        cls.SUBMITTED,
            "pending_review":        cls.SUBMITTED,
            "held":                  cls.SUBMITTED,
            # ── Teljesülési állapotok ───────────────────────────────────────
            "partially_filled":      cls.PARTIALLY_FILLED,
            "filled":                cls.FILLED,
            # ── Záró állapotok ──────────────────────────────────────────────
            "canceled":              cls.CANCELED,
            "cancelled":             cls.CANCELED,
            "rejected":              cls.REJECTED,
            "expired":               cls.EXPIRED,
            "done_for_day":          cls.EXPIRED,
            # ── Speciális / átmeneti állapotok ──────────────────────────────
            "suspended":             cls.UNKNOWN,
            "calculated":            cls.UNKNOWN,
        }
        return mapping.get(normalized, cls.UNKNOWN)

    @property
    def is_terminal(self) -> bool:
        """True, ha az order már nem változhat (végállapot)."""
        return self in {
            OrderState.FILLED,
            OrderState.CANCELED,
            OrderState.REJECTED,
            OrderState.EXPIRED,
        }

    @property
    def is_open(self) -> bool:
        """True, ha az order még aktív (nyitott pozíció vagy pending fill).

        pending_cancel / pending_review / held mind SUBMITTED-nek mappolódik,
        ezért ezek is nyitottnak minősülnek — amíg a broker végleges
        záró státuszt nem küld, a rendszer nem törli őket helyileg.
        """
        return self in {OrderState.SUBMITTED, OrderState.PARTIALLY_FILLED}


def normalize_order_status(raw) -> str:
    """Alpaca order státusz normalizálása megbízható lowercase stringgé.

    [#2-fix] Az Alpaca SDK verziótól függően az order.status mező
    lehet:
      - str:          "new", "filled", "canceled" stb.  (régebbi SDK)
      - OrderStatus enum: OrderStatus.NEW, OrderStatus.FILLED stb.
        amelynek str() eredménye "OrderStatus.NEW" — NEM "new"

    Ez a függvény mindkét formát kezeli:
      str("new")            → "new"
      str(OrderStatus.NEW)  → "OrderStatus.NEW" → ".new" → "new"
      getattr(.value)       → "new" (ha van .value attribútum)

    Minden _OPEN_STATUSES és hasonló összehasonlítás ezt hívja,
    így enum/string inkonzisztenciából nem keletkezhet silent bug.
    """
    if raw is None:
        return ""
    # Enum esetén: ha van .value attribútum, az mindig a string érték
    value = getattr(raw, "value", None)
    if value is not None:
        return str(value).lower()
    # String eset: "OrderStatus.NEW" → "new"; "new" → "new"
    s = str(raw).lower()
    if "." in s:
        # pl. "orderstatus.new" → "new"
        s = s.rsplit(".", 1)[-1]
    return s


# Nyitott Alpaca order státuszok kanonikus, centrális definíciója.
#
# Minden cancel/preclose/reconciliation útvonalnak ezt kell importálnia és
# használnia a helyi, egyedi definíciók helyett — így egyetlen helyen
# tartható karban a lista.
#
# Miért tartalmaz minden státuszt:
#   partially_filled  — az order részlegesen teljesült, de még aktív;
#                       kill switch és preclose alatt törlendő!
#   pending_replace   — módosítási kérelem folyamatban, order még él
#   replaced          — módosítás után az order új állapotban aktív
#   pending_cancel    — törlés kérve, de még nem végleges; az order él
#   pending_review    — compliance ellenőrzés, nyitott állapot
#   stopped           — market maker által megállított, de nem zárult
#   held              — piac zárva / extended-hours szünet
OPEN_ALPACA_STATUSES: frozenset[str] = frozenset({
    "new",
    "accepted",
    "pending_new",
    "pending_replace",
    "accepted_for_bidding",
    "stopped",
    "replaced",
    "pending_cancel",
    "pending_review",
    "held",
    "partially_filled",
})


@dataclass
class Signal:
    """Stratégia kimenete; pre-trade előtt is perzisztálható."""

    symbol: str
    side: str
    limit_price: float
    strategy: str
    mode: int
    ts: datetime
    indicators: dict = field(default_factory=dict)
    signal_id: str = ""

    def __post_init__(self) -> None:
        if not self.signal_id:
            self.signal_id = self._generate_id()

    def _generate_id(self) -> str:
        ts_key = self.ts.replace(microsecond=0).isoformat()
        raw = f"{self.symbol}|{ts_key}|{self.strategy}|{self.side}|{self.limit_price}"
        return hashlib.sha1(raw.encode()).hexdigest()[:16]


@dataclass
class OrderIntent:
    """Megbízási szándék; még nem broker order."""

    symbol: str
    side: str
    qty: int
    entry_type: str
    limit_price: float
    tp_price: float
    sl_price: float
    tif: str
    strategy: str
    signal_id: str
    order_role: str = "entry"
    parent_client_order_id: Optional[str] = None
    client_order_id: str = ""
    run_id: str = ""

    def __post_init__(self) -> None:
        if not self.client_order_id:
            self.client_order_id = self._generate_coid()

    def _generate_coid(self) -> str:
        raw = (
            f"{self.run_id}|{self.symbol}|{self.signal_id}|{self.strategy}|"
            f"{self.limit_price}|{self.order_role}|{self.parent_client_order_id or ''}"
        )
        digest = hashlib.sha1(raw.encode()).hexdigest()[:24]
        return f"bot-{digest}"


@dataclass
class OrderRecord:
    """A megbízás lokálisan tárolt broker állapota."""

    client_order_id: str
    symbol: str
    side: str
    qty: int
    state: OrderState
    signal_id: str = ""
    broker_order_id: Optional[str] = None
    filled_qty: float = 0.0
    avg_fill_price: Optional[float] = None
    limit_price: Optional[float] = None
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None
    tif: str = "day"
    strategy: Optional[str] = None
    order_role: str = "entry"
    parent_client_order_id: Optional[str] = None
    submitted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    reject_reason: Optional[str] = None

    @classmethod
    def from_intent(cls, intent: OrderIntent) -> "OrderRecord":
        return cls(
            client_order_id=intent.client_order_id,
            symbol=intent.symbol,
            side=intent.side,
            qty=intent.qty,
            state=OrderState.INTENT,
            signal_id=intent.signal_id,
            limit_price=intent.limit_price,
            tp_price=intent.tp_price,
            sl_price=intent.sl_price,
            tif=intent.tif,
            strategy=intent.strategy,
            order_role=intent.order_role,
            parent_client_order_id=intent.parent_client_order_id,
            submitted_at=datetime.now(timezone.utc),
        )


@dataclass
class RiskDecision:
    """Risk policy kimenete."""

    allowed: bool
    reason: str
    max_qty: int = 0
    warnings: list[str] = field(default_factory=list)

    @classmethod
    def block(cls, reason: str, warnings: Optional[list[str]] = None) -> "RiskDecision":
        return cls(allowed=False, reason=reason, max_qty=0, warnings=warnings or [])

    @classmethod
    def allow(
        cls,
        max_qty: int,
        reason: str = "ok",
        warnings: Optional[list[str]] = None,
    ) -> "RiskDecision":
        return cls(allowed=True, reason=reason, max_qty=max_qty, warnings=warnings or [])

    def to_dict(self) -> dict:
        return asdict(self)
