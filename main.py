from __future__ import annotations

"""
main.py — Alpaca Hibrid Trading Bot
====================================
Indítás:
  python main.py

Leállítás: CTRL+C

Környezeti változók (.env):
  ALPACA_API_KEY       — Alpaca API kulcs (kötelező)
  ALPACA_SECRET_KEY    — Alpaca titkos kulcs (kötelező)
  ALPACA_LIVE          — "true" esetén éles mód; egyébként paper (alapértelmezett)
  I_UNDERSTAND_LIVE_TRADING_RISK
                       — ALPACA_LIVE=true esetén kötelező "true" értékre állítani.
                         Véletlen éles indítás elleni védelem.
  MAX_LIVE_NOTIONAL_USD
                       — Maximális order nominális érték ($) éles módban.
                         Éles módban blokkol, paper módban csak figyelmeztet.
                         0 = nincs limit (default: 1000)
  PDT_SAFETY_MARGIN    — [LEGACY] PDT biztonsági margó (alapértelmezett: 0.15).
                         Csak akkor releváns, ha PDT_GUARD_ENABLED=true.
                         Az Alpaca 2026 közepén megszüntette a PDT szabályt,
                         lásd alpaca_new.txt.
  PDT_GUARD_ENABLED    — "true" esetén bekapcsolja a régi PDT guardot
                         (alapértelmezett: false — már nincs PDT szabály)
  INTRADAY_MARGIN_SAFETY_MARGIN
                       — Az új Intraday Margin guard biztonsági margója,
                         a buying_power mennyi százalékát tartjuk vissza
                         pufferként (alapértelmezett: 0.10)
  LOG_DIR              — Napló könyvtár (alapértelmezett: logs/)
  KILL_SWITCH_PATH     — Kill switch fájl (default: data/KILL_SWITCH)
  KILL_SWITCH_SCOPE    — "intraday" | "all" (default: intraday)
  KILL_SWITCH_CANCEL_ALL_ORDERS — "true" | "false" (default: false)
  KILL_SWITCH_CLOSE_ALL_POSITIONS — "true" | "false" (default: false)

Javítások (2024-06-14):
  #1  volume mező hozzáadva a warmup és az on_bar() gyertya-pufferéhez
      → a BreakoutStrategy (mode 2) mostantól valóban generál jeleket
  #2  Mode 1 (trend following) saját óra-aggregátort kap a perces streamből
      → az EMA/ADX indikátor konzisztens hourly idősoron fut
  #3  Mode 2 (breakout) DAY TIF-et kap (nem GTC)
      → az execution_engine.py create_entry_intent() javítva
  #4  close_intraday_positions() a pozíciók zárása ELŐTT törli a nyitott
      intraday ordereket is
  #5  Folyamatos circuit breaker monitor (_circuit_breaker_loop) 60 mp-enként
      fut, nem csupán új jel érkezésekor
  #6  stream.run_in_executor() — publikus stream.run() thread-poolban fut,
      így asyncio.gather()-rel párhuzamosítható anélkül, hogy event loop
      ütközés keletkezne (a privát _run_forever() kiváltva)
  #7  Az on_bar() async handlerben a szinkron REST/SQLite hívások
      asyncio.to_thread() -be kerültek, így nem blokkolják az event loopot
"""

import asyncio
import hashlib
import json
import os
import signal
import sys
import time
import uuid
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from modules.trading_calendar import us_trade_date, trading_date_from_clock  # [#D]

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream
from dotenv import load_dotenv

from modules.broker_cache import BrokerSnapshotCache
from modules.config_loader import get_symbols, load_config
from modules.config_validator import validate_config, ConfigError
from modules.daily_reporter import DailyReporter
from modules.execution_engine import ExecutionEngine, ExecutionError, _get_orders_nested
from modules.logger import get_logger, log_event
from modules.models import Signal, OrderState, normalize_order_status, OPEN_ALPACA_STATUSES
from modules.pretrade import PreTradePolicy
from modules.risk_manager import RiskManager
from modules.state_manager import StateManager, canonical_fill_id  # [#3-fix]
from modules.strategy_engine import StrategyEngine

# ── Konfiguráció betöltése ──────────────────────────────────────────────────────
load_dotenv()

API_KEY: str = os.getenv("ALPACA_API_KEY", "")
SECRET: str = os.getenv("ALPACA_SECRET_KEY", "")
IS_LIVE: bool = os.getenv("ALPACA_LIVE", "false").lower() == "true"

# Live safety flags — kötelező explicit megerősítés éles módban
#
# I_UNDERSTAND_LIVE_TRADING_RISK=true
#   Kötelező visszaigazolás: a felhasználó tisztában van azzal, hogy
#   éles pénzzel kereskedik. ALPACA_LIVE=true esetén enélkül a bot
#   nem indul el. Paper módban nincs hatása.
#
# MAX_LIVE_NOTIONAL_USD=1000
#   Maximális nominális érték ($) egyetlen order esetén éles módban.
#   Ha egy entry intent qty * limit_price meghaladja ezt, az order
#   blokkolódik és naplózásra kerül. 0 = nincs limit (nem ajánlott).
#   Paper módban ellenőrizzük, de csak figyelmeztetünk — nem blokkol.
I_UNDERSTAND_LIVE_TRADING_RISK: bool = (
    os.getenv("I_UNDERSTAND_LIVE_TRADING_RISK", "false").lower() == "true"
)
MAX_LIVE_NOTIONAL_USD: float = float(os.getenv("MAX_LIVE_NOTIONAL_USD", "1000"))

# ── Live safety validáció (modul-szinten, indulás előtt) ──────────────────
if IS_LIVE and not I_UNDERSTAND_LIVE_TRADING_RISK:
    print(
        "HIBA: ALPACA_LIVE=true, de I_UNDERSTAND_LIVE_TRADING_RISK nincs beállítva.\n"
        "  Éles kereskedés indításához explicit megerősítés szükséges.\n"
        "  Add hozzá a .env fájlhoz:\n"
        "    I_UNDERSTAND_LIVE_TRADING_RISK=true\n"
        "    MAX_LIVE_NOTIONAL_USD=1000  # max order érték USD-ben\n"
        "  Ez a védelmi mechanizmus megakadályozza a véletlen éles indítást.",
        file=sys.stderr,
    )
    sys.exit(1)

PDT_MARGIN: float = float(os.getenv("PDT_SAFETY_MARGIN", "0.15"))
PDT_GUARD_ENABLED: bool = os.getenv("PDT_GUARD_ENABLED", "false").lower() == "true"
INTRADAY_MARGIN_SAFETY_MARGIN: float = float(
    os.getenv("INTRADAY_MARGIN_SAFETY_MARGIN", "0.10")
)
MAX_BARS: int = 300
CONFIG_PATH: str = os.getenv("CONFIG_PATH", "config/stcks.json")

# Exponenciális backoff lista (másodpercek)
RECONNECT_BACKOFFS = (1, 2, 4, 8, 16, 30, 60)

# Circuit breaker monitor intervalluma (másodperc)  [#5]
CIRCUIT_BREAKER_POLL_SEC: int = 60

# Kill switch fájl elérési útja — ha létezik, emergency stop indul
KILL_SWITCH_PATH: str = os.getenv("KILL_SWITCH_PATH", "data/KILL_SWITCH")
# Kill switch monitor intervalluma (másodperc)
KILL_SWITCH_POLL_SEC: int = int(os.getenv("KILL_SWITCH_POLL_SEC", "5"))

# Kill switch hatókör-beállítások
# KILL_SWITCH_SCOPE: "intraday" | "all"
#   intraday — csak mode 0 és mode 2 pozíciók záródnak (swing/trend pozíciók védve maradnak)
#   all      — minden pozíció záródik
KILL_SWITCH_SCOPE: str = os.getenv("KILL_SWITCH_SCOPE", "intraday")

# KILL_SWITCH_CANCEL_ALL_ORDERS: true | false
#   true  — minden nyitott order törlése (beleértve a mode 1 bracket védőordereit)
#   false — csak az érintett hatókörbe tartozó szimbólumok orderei törlődnek
#           (mode 1 / trend following bracket védőorderei megmaradnak)
KILL_SWITCH_CANCEL_ALL_ORDERS: bool = (
    os.getenv("KILL_SWITCH_CANCEL_ALL_ORDERS", "false").lower() == "true"
)

# KILL_SWITCH_CLOSE_ALL_POSITIONS: true | false
#   true  — minden pozíció zárása (beleértve a swing/trend pozíciókat)
#   false — csak a KILL_SWITCH_SCOPE szerinti pozíciók záródnak
KILL_SWITCH_CLOSE_ALL_POSITIONS: bool = (
    os.getenv("KILL_SWITCH_CLOSE_ALL_POSITIONS", "false").lower() == "true"
)

# ── Kill switch env validáció (modul-szinten, bot indulása előtt) ─────────
# [#3] Veszélyes kombináció detektálása:
#   CANCEL_ALL_ORDERS=true + CLOSE_ALL_POSITIONS=false + SCOPE=intraday
#   → minden order törlődik, de csak intraday pozíciók záródnak
#   → mode 1 swing pozíciók védőorder nélkül maradnak
# [#4] SCOPE értékkészlet validáció: csak "intraday" és "all" megengedett.
_VALID_KS_SCOPES = {"intraday", "all"}
_raw_ks_scope = os.getenv("KILL_SWITCH_SCOPE", "intraday").lower().strip()

if _raw_ks_scope not in _VALID_KS_SCOPES:
    print(
        f"HIBA: KILL_SWITCH_SCOPE='{_raw_ks_scope}' érvénytelen értékkészlet.\n"
        f"  Megengedett értékek: {sorted(_VALID_KS_SCOPES)}\n"
        f"  Ellenőrizd a .env fájlt (pl. elgépelés: 'intrday' helyett 'intraday').",
        file=sys.stderr,
    )
    sys.exit(1)

# A validált scope felülírja az eredeti konstanst
KILL_SWITCH_SCOPE: str = _raw_ks_scope  # type: ignore[assignment]  # noqa: F811

if (
    KILL_SWITCH_CANCEL_ALL_ORDERS
    and not KILL_SWITCH_CLOSE_ALL_POSITIONS
    and KILL_SWITCH_SCOPE == "intraday"
):
    print(
        "HIBA: Veszélyes kill switch kombináció detektálva:\n"
        "  KILL_SWITCH_CANCEL_ALL_ORDERS=true + "
        "KILL_SWITCH_CLOSE_ALL_POSITIONS=false + "
        "KILL_SWITCH_SCOPE=intraday\n"
        "  Ez törölné a mode 1 (trend following) bracket védőordereket,\n"
        "  miközben azok pozíciói nyitva maradnának — fedezetlen kockázat.\n"
        "  Javítási lehetőségek:\n"
        "    1) KILL_SWITCH_CANCEL_ALL_ORDERS=false  (csak intraday orderek törlése)\n"
        "    2) KILL_SWITCH_CLOSE_ALL_POSITIONS=true  (minden pozíció zárása)\n"
        "    3) KILL_SWITCH_SCOPE=all  (minden szimbólum érintett)",
        file=sys.stderr,
    )
    sys.exit(1)


# ── TradingBot ──────────────────────────────────────────────────────────────────

class TradingBot:
    """
    Fő vezérlő osztály.
    Felelős a WebSocket-alapú esemény-feldolgozásért, stratégiai döntésekért
    és a napi CSV riportért.
    """

    def __init__(self) -> None:
        self.logger = get_logger()
        self.cfg = load_config(CONFIG_PATH)
        self.symbols = get_symbols(self.cfg)
        self.run_id = uuid.uuid4().hex[:12]

        # Alpaca kliensek
        self.trading = TradingClient(API_KEY, SECRET, paper=not IS_LIVE)
        self.hist = StockHistoricalDataClient(API_KEY, SECRET)
        self.stream = StockDataStream(API_KEY, SECRET)
        self.trading_stream = TradingStream(API_KEY, SECRET, paper=not IS_LIVE)

        # Alrendszerek
        self.state = StateManager("state.db")
        self.cache = BrokerSnapshotCache(self.trading)
        self.strategy = StrategyEngine()
        self.risk = RiskManager(
            self.logger,
            self.state,
            PDT_MARGIN,
            is_live=IS_LIVE,
            pdt_enabled=PDT_GUARD_ENABLED,
            broker_pdt_active=PDT_GUARD_ENABLED,
            intraday_margin_safety_margin=INTRADAY_MARGIN_SAFETY_MARGIN,
        )
        self.execution = ExecutionEngine(
            self.trading, self.logger, self.state, self.run_id
        )
        self.pretrade = PreTradePolicy(self.cache, self.state)
        self.reporter = DailyReporter(report_dir="data", filename="daily_summary.csv")

        # Gyertya pufferek (mode 0 és mode 2: perces; mode 1: KÜLÖN óra-puffer)
        self.candles: dict = defaultdict(lambda: deque(maxlen=MAX_BARS))

        # [#2] Mode 1 szimbólumokhoz óra-aggregátor állapot
        # Kulcs: szimbólum  →  az éppen épülő órai gyertya adatai
        self._hourly_builder: dict[str, dict] = {}

        # Intraday szimbólumok (mode 0 és mode 2): zárás előtt le kell zárni
        # - mode 0 (mean reversion): DAY order, automatikusan lejár, de aktív pozíciót zárunk
        # - mode 2 (breakout): szintén intraday, napon belüli zárás szükséges [#3 javítva]
        # - mode 1 (trend following): GTC, nem zárjuk napon belül
        self.intraday_symbols: list[str] = [
            s for s, c in self.cfg.items() if c["mode"] in (0, 2)
        ]

        # Futásidejű állapot
        self.running: bool = True
        self.preclose_actions_done: bool = False
        self.daily_summary_done: bool = False
        self.in_flight_symbols: set[str] = set()
        self.processed_fill_qty: dict[str, float] = {}

        # Napi nyitó equity (CSV-hez)
        self._day_open_equity: float = 0.0
        self._trade_date: str = us_trade_date()  # [#D] ET dátum, nem lokális

    # ── Indítás ────────────────────────────────────────────────────────────────

    def startup(self) -> None:
        # [#3 konfig] Teljes séma- és broker-validáció indítás előtt.
        # ConfigError esetén a bot nem indul el.
        try:
            validate_config(self.cfg, strict=True, trading_client=self.trading)
        except ConfigError as exc:
            self.logger.error(
                "Konfiguráció validációs hiba — bot nem indul el:\n%s", exc
            )
            raise SystemExit(1) from exc

        cfg_hash = hashlib.sha1(
            json.dumps(self.cfg, sort_keys=True).encode()
        ).hexdigest()[:12]
        self.state.start_run(self.run_id, "live" if IS_LIVE else "paper", cfg_hash)

        acct = self.cache.get_account()
        equity = float(acct.equity)
        self._day_open_equity = equity

        self.risk.set_start_equity(equity)
        self.state.recover_from_broker(self.trading)
        self.execution.reconcile_orders()
        self._warmup_candles()

        log_event(
            self.logger,
            "startup",
            run_id=self.run_id,
            live=IS_LIVE,
            equity=equity,
            symbols=self.symbols,
            config=CONFIG_PATH,
            mode="LIVE" if IS_LIVE else "PAPER",
        )

    def _warmup_candles(self) -> None:
        """Történeti gyertyák betöltése indításkor.

        [#1] volume mező hozzáadva minden gyertyához — a BreakoutStrategy igényli.
        [#2] Mode 1 szimbólumokhoz órás gyertyákat töltünk (konzisztencia),
             mode 0/2-höz perceset.
        """
        for sym, sym_cfg in self.cfg.items():
            tf = TimeFrame.Hour if sym_cfg["mode"] == 1 else TimeFrame.Minute
            try:
                req = StockBarsRequest(
                    symbol_or_symbols=sym, timeframe=tf, limit=MAX_BARS
                )
                bars = self.hist.get_stock_bars(req).data.get(sym, [])
                for bar in bars:
                    candle = {
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": getattr(bar, "volume", 0) or 0,  # [#1]
                        "ts": bar.timestamp,
                    }
                    self.candles[sym].append(candle)
                log_event(
                    self.logger,
                    "warmup",
                    symbol=sym,
                    bars_loaded=len(bars),
                    timeframe=str(tf),
                )
            except Exception as exc:
                log_event(
                    self.logger,
                    "warmup_error",
                    level="WARNING",
                    symbol=sym,
                    detail=str(exc),
                )

    # ── WebSocket eseménykezelők ───────────────────────────────────────────────

    async def on_bar(self, bar) -> None:
        """Minden bejövő perces gyertyát feldolgoz.

        [#1] volume mező felveszi az aktuális bar.volume értékét.
        [#2] Mode 1 szimbólumokhoz az alacsonyabb szintű perces adatból
             saját óra-aggregátor épít hourly gyertyát.
        [#7] A szinkron REST/SQLite hívásokat asyncio.to_thread()-be tesszük,
             hogy ne blokkolják az event loopot.
        """
        # Kill switch vagy shutdown után érkező bar-ok eldobása.
        # A stream.stop() és self.running = False között rövid ablak nyílik,
        # amikor a WebSocket még küldhet eseményeket — ezeket nem dolgozzuk fel.
        if not self.running:
            return

        sym = bar.symbol
        sym_cfg = self.cfg.get(sym)
        if sym_cfg is None:
            return

        if sym_cfg["mode"] == 1:
            # [#2] Perces bar érkezik → aggregálunk órai gyertyává
            completed_hourly = self._aggregate_hourly(sym, bar)
            if completed_hourly is None:
                return  # Az óra még nem zárult le → nincs mit értékelni
            self.candles[sym].append(completed_hourly)
        else:
            # Mode 0 / mode 2: perces gyertya közvetlenül a pufferbe kerül
            candle = {
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": getattr(bar, "volume", 0) or 0,  # [#1]
                "ts": bar.timestamp,
            }
            self.candles[sym].append(candle)

        # [#7] Szinkron műveletek futtatása thread-poolban
        await asyncio.to_thread(self._check_daily_reset)
        await asyncio.to_thread(self.cache.invalidate_clock)  # clock frissítés előkészítése

        await self.process_symbol(sym)

    def _aggregate_hourly(self, sym: str, bar) -> dict | None:
        """[#2] Perces bar → órai gyertya aggregátor.

        Az Alpaca perces streamből épít konzisztens hourly gyertyákat a mode 1
        szimbólumokhoz, hogy az EMA/ADX indikátor ne keveredjen különböző
        idősíkok adataival.

        Visszatérés: lezárt hourly gyertya dict-je, vagy None ha az óra még folyik.
        """
        ts: datetime = bar.timestamp
        # Óra határát a timestamp csonkításával határozzuk meg
        hour_key = ts.replace(minute=0, second=0, microsecond=0)

        builder = self._hourly_builder.get(sym)

        if builder is None:
            # Első bar ebben a szimbólumban
            self._hourly_builder[sym] = {
                "hour": hour_key,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": getattr(bar, "volume", 0) or 0,
                "ts": hour_key,
            }
            return None

        if hour_key == builder["hour"]:
            # Ugyanabba az órába tartozik → frissítjük a running gyertyát
            builder["high"] = max(builder["high"], bar.high)
            builder["low"] = min(builder["low"], bar.low)
            builder["close"] = bar.close
            builder["volume"] = builder["volume"] + (getattr(bar, "volume", 0) or 0)
            return None

        # Új óra kezdődik → az előző óra lezárult, visszaadjuk
        completed = {
            "open": builder["open"],
            "high": builder["high"],
            "low": builder["low"],
            "close": builder["close"],
            "volume": builder["volume"],
            "ts": builder["ts"],
        }
        # Új builder az aktuális órához
        self._hourly_builder[sym] = {
            "hour": hour_key,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": getattr(bar, "volume", 0) or 0,
            "ts": hour_key,
        }
        return completed

    async def on_trade_update(self, data) -> None:
        """Kereskedési események (filled, rejected, canceled stb.) feldolgozása.

        [#7] SQLite műveletek asyncio.to_thread()-be kerültek.
        """
        event = getattr(data, "event", "")
        order = getattr(data, "order", None)
        if order is None:
            log_event(self.logger, "broker_trade_update_skip", level="WARNING", reason="missing_order")
            return

        symbol = str(getattr(order, "symbol", "") or "")
        broker_order_id = str(getattr(order, "id", "") or "")
        client_order_id = str(getattr(order, "client_order_id", "") or "")
        order_key = client_order_id or broker_order_id

        log_event(
            self.logger,
            "broker_trade_update",
            symbol=symbol,
            event=event,
            order_id=client_order_id,
            broker_order_id=broker_order_id,
        )

        if not order_key:
            log_event(self.logger, "broker_trade_update_skip", level="WARNING", symbol=symbol, reason="missing_order_ids")
            return

        local_state = OrderState.from_alpaca(getattr(order, "status", event))
        filled_qty  = self._safe_float(getattr(order, "filled_qty", 0), default=0.0)
        avg_price   = self._safe_float(getattr(order, "filled_avg_price", None), default=None)

        # [#C] Bracket child order felismerés a trade update stream-en.
        # Az Alpaca a TP/SL child fill event-eken is küld trade update-et.
        # Ha a lokális DB-ben az order már ismert (submit vagy reconcile során
        # import_broker_order mentette helyes szerepsel), csak állapotot frissítünk.
        # Ha még nem ismert (pl. nagyon korai fill a reconcile előtt), megpróbáljuk
        # a parent kapcsolatot kinyerni az order.order_class + order.legs mezőkből.
        order_class  = normalize_order_status(getattr(order, "order_class", ""))  # [#2-fix] enum-safe
        # order.order_id = az Alpaca broker-oldali PARENT order UUID-je.
        # Ez NEM a lokális client_order_id — a round-trip PnL JOIN csak
        # client_order_id alapú parent_client_order_id mezőn működik.
        # Feloldás: DB lookup broker_order_id → client_order_id.
        # Ha nem találjuk (pl. korai fill, parent még nem importálva),
        # a raw broker UUID kerül be ideiglenes értékként; a
        # repair_parent_client_order_ids() reconcile-kor kijavítja.
        broker_parent_oid = str(getattr(order, "order_id", "") or "")
        if broker_parent_oid:
            resolved = await asyncio.to_thread(
                self.state.get_client_order_id_by_broker_id, broker_parent_oid
            )
            parent_oid = resolved or broker_parent_oid  # fallback: raw broker UUID
            if resolved is None:
                log_event(
                    self.logger, "parent_lookup_miss",
                    level="DEBUG",
                    child_client_order_id=client_order_id,
                    broker_parent_oid=broker_parent_oid,
                    note="repair_parent_client_order_ids() javítja reconcile-kor",
                )
        else:
            parent_oid = ""
        # order_type: 'limit' → tp, 'stop'/'stop_limit' → sl, 'market' → entry
        order_type   = normalize_order_status(getattr(order, "order_type", ""))  # [#2-fix]
        _CHILD_ROLES = {"limit": "tp", "stop": "sl", "stop_limit": "sl"}
        is_child = bool(parent_oid) and order_class not in ("bracket", "")
        derived_role = _CHILD_ROLES.get(order_type, "entry") if is_child else "entry"

        # [#7] SQLite írás thread-poolban
        if client_order_id:
            if is_child and not await asyncio.to_thread(self.state.order_exists, client_order_id):
                # Korábban nem ismert child order → mentjük helyes szereppel
                from modules.models import OrderRecord
                child_rec = OrderRecord(
                    client_order_id=client_order_id,
                    broker_order_id=broker_order_id or None,
                    signal_id="",
                    symbol=symbol,
                    side=self._normalize_order_side(getattr(order, "side", "sell")),
                    qty=int(float(getattr(order, "qty", 0) or 0)),
                    filled_qty=filled_qty or 0.0,
                    avg_fill_price=avg_price,
                    tif=str(getattr(order, "time_in_force", "day")).lower(),
                    order_role=derived_role,
                    parent_client_order_id=parent_oid or None,
                    state=local_state,
                )
                await asyncio.to_thread(self.state.import_broker_order, child_rec)
            else:
                await asyncio.to_thread(
                    self.state.update_order_state,
                    client_order_id=client_order_id,
                    state=local_state,
                    filled_qty=filled_qty,
                    avg_price=avg_price,
                )
        else:
            log_event(
                self.logger,
                "broker_trade_update_unmatched",
                level="WARNING",
                symbol=symbol,
                broker_order_id=broker_order_id,
                reason="missing_client_order_id",
            )

        fill_delta = self._fill_delta(order_key, filled_qty)
        if fill_delta > 0:
            await asyncio.to_thread(
                self._record_fill_and_update_risk,
                symbol=symbol,
                side=self._normalize_order_side(getattr(order, "side", "")),
                qty=fill_delta,
                price=avg_price,
                broker_order_id=broker_order_id or order_key,
                client_order_id=client_order_id or order_key,
                cumulative_filled_qty=filled_qty,
            )

        self.cache.invalidate_positions()
        self.cache.invalidate_orders()

        if self.preclose_actions_done:
            await asyncio.to_thread(self._maybe_write_daily_summary_after_preclose)

    @staticmethod
    def _safe_float(value, default: float | None = 0.0) -> float | None:
        """Broker mezők robusztus float-konverziója None/üres string esetére."""
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _fill_delta(self, order_key: str, cumulative_filled_qty: float) -> float:
        """Partial fill esetén csak az újonnan megjelent fill mennyiséget adja vissza."""
        previous = self.processed_fill_qty.get(order_key, 0.0)
        delta = max(0.0, cumulative_filled_qty - previous)
        if cumulative_filled_qty > previous:
            self.processed_fill_qty[order_key] = cumulative_filled_qty
        return delta

    @staticmethod
    def _normalize_order_side(side) -> str:
        """Alpaca enum/string oldalelem normalizálása: buy/sell."""
        value = getattr(side, "value", side)
        text = str(value or "").lower()
        if text.endswith(".buy"):
            return "buy"
        if text.endswith(".sell"):
            return "sell"
        return text

    def _record_fill_and_update_risk(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        price: float | None,
        broker_order_id: str,
        client_order_id: str,
        cumulative_filled_qty: float,
    ) -> None:
        """Fill audit trail + PDT/day-trade lifecycle frissítés."""
        fill_price = price if price is not None else 0.0
        # [#3-fix] Kanonikus fill_id — azonos a refresh_order_state() által generálttal,
        # így restart/reconnect után sem keletkezik duplikát fill rekord.
        fill_id = canonical_fill_id(broker_order_id, cumulative_filled_qty, fill_price)

        # [#1-fix] apply_fill() visszajelzi, hogy tényleg új rekord keletkezett-e.
        # Ha False (már létező fill_id → IGNORE), a risk/day-trade ledgert NEM
        # frissítjük — így reconnect/restart után sem duplázódik on_fill() hívás.
        fill_was_new: bool = False
        try:
            fill_was_new = self.state.apply_fill(
                fill_id=fill_id,
                broker_order_id=broker_order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                qty=qty,
                price=fill_price,
            )
        except Exception as exc:
            log_event(
                self.logger,
                "fill_persist_error",
                level="WARNING",
                symbol=symbol,
                broker_order_id=broker_order_id,
                client_order_id=client_order_id,
                detail=str(exc),
            )

        if not fill_was_new:
            log_event(
                self.logger,
                "fill_duplicate_skipped",
                level="DEBUG",
                symbol=symbol,
                fill_id=fill_id,
                reason="already_in_db",
            )
            return

        try:
            created_day_trade = self.risk.on_fill(
                symbol=symbol,
                side=side,
                qty=qty,
                price=fill_price,
                ts=datetime.now(timezone.utc),
                extra={
                    "broker_order_id": broker_order_id,
                    "client_order_id": client_order_id,
                },
            )
            log_event(
                self.logger,
                "fill_lifecycle_update",
                symbol=symbol,
                side=side,
                qty=qty,
                price=fill_price,
                day_trade_created=created_day_trade,
                day_trade_count=self.risk.day_trade_count,
            )
        except AttributeError:
            log_event(
                self.logger,
                "fill_lifecycle_skip",
                level="WARNING",
                symbol=symbol,
                reason="risk_manager_has_no_on_fill",
            )
        except Exception as exc:
            log_event(
                self.logger,
                "fill_lifecycle_error",
                level="WARNING",
                symbol=symbol,
                detail=str(exc),
            )

    # ── Háttérfeladatok ────────────────────────────────────────────────────────

    async def _market_close_failsafe_loop(self) -> None:
        """Független, időalapú háttérfeladat a piaci zárás kényszerítésére."""
        while self.running:
            try:
                clock = await asyncio.to_thread(self.cache.get_clock)  # [#7]
                if clock.is_open:
                    now = datetime.now(timezone.utc)
                    if (clock.next_close - now) <= timedelta(minutes=5):
                        if not self.preclose_actions_done:
                            log_event(self.logger, "failsafe_preclose_triggered", reason="Time-based trigger")
                            await asyncio.to_thread(
                                self.execution.close_intraday_positions,
                                self.intraday_symbols,
                                "failsafe_preclose",
                            )
                            self.cache.invalidate_positions()
                            self.cache.invalidate_orders()
                            self.preclose_actions_done = True
                        await asyncio.to_thread(self._maybe_write_daily_summary_after_preclose)
            except Exception as exc:
                log_event(self.logger, "failsafe_loop_error", level="WARNING", detail=str(exc))

            await asyncio.sleep(30)

    async def _circuit_breaker_loop(self) -> None:
        """[#5] Folyamatos circuit breaker monitor.

        Az eredeti megközelítés szerint a -3%-os napi veszteség ellenőrzése csak
        akkor futott, amikor új signal/pre-trade folyamat indult. Ha már volt pozíció
        és nem érkezett új bar-jel, a bot nem reagált azonnal a veszteségre.

        Ez a háttérfeladat CIRCUIT_BREAKER_POLL_SEC (alapértelmezett: 60 mp)
        időközönként lekéri a számlát, és ha a napi veszteség eléri a -3%-ot,
        azonnal zár minden pozíciót — függetlenül attól, hogy jön-e új bar.
        """
        while self.running:
            await asyncio.sleep(CIRCUIT_BREAKER_POLL_SEC)
            if self.risk.circuit_breaker_active:
                continue  # Már aktív, nincs teendő
            try:
                account = await asyncio.to_thread(self.cache.get_account, 0)  # friss adat
                self.risk.update_equity(float(account.equity))  # [#3] drawdown nyomkövetés
                if self.risk.evaluate_circuit_breaker(account):
                    log_event(
                        self.logger,
                        "circuit_breaker_monitor_triggered",
                        level="WARNING",
                        reason="daily_loss_-3pct_continuous_check",
                        equity=float(account.equity),
                    )
                    await asyncio.to_thread(
                        self.execution.close_all_positions, "circuit_breaker_monitor"
                    )
                    self.cache.invalidate_positions()
                    self.cache.invalidate_orders()
            except Exception as exc:
                log_event(
                    self.logger,
                    "circuit_breaker_monitor_error",
                    level="WARNING",
                    detail=str(exc),
                )

    # ── Szimbólum feldolgozás ─────────────────────────────────────────────────

    async def _kill_switch_loop(self) -> None:
        """Kill switch monitor háttérfeladat.

        KILL_SWITCH_POLL_SEC (default: 5 mp) időközönként ellenőrzi, hogy
        létezik-e a KILL_SWITCH_PATH fájl (default: data/KILL_SWITCH).

        A hatókör KILL_SWITCH_SCOPE, KILL_SWITCH_CANCEL_ALL_ORDERS és
        KILL_SWITCH_CLOSE_ALL_POSITIONS env. változókkal finomítható:

        ┌──────────────────────────────────────────────────────────────────┐
        │ KILL_SWITCH_SCOPE=intraday  (default)                           │
        │   Csak mode 0 és mode 2 szimbólumok érintettek.                 │
        │   Mode 1 (trend following) swing pozíciók és védőordereik       │
        │   érintetlenek maradnak — a bracket SL/TP gyermek orderek nem   │
        │   törlődnek véletlenül.                                         │
        │                                                                  │
        │ KILL_SWITCH_SCOPE=all                                           │
        │   Minden szimbólum pozíciója záródik.                           │
        ├──────────────────────────────────────────────────────────────────┤
        │ KILL_SWITCH_CANCEL_ALL_ORDERS=false  (default)                  │
        │   Csak a hatókörbe eső szimbólumok nyitott orderei törlődnek.   │
        │   Mode 1 bracket védőorderei megmaradnak.                       │
        │                                                                  │
        │ KILL_SWITCH_CANCEL_ALL_ORDERS=true                              │
        │   Minden nyitott order törlése (trading.cancel_orders()).       │
        │   Veszélyes: mode 1 bracket SL/TP is törlődik, a pozíció       │
        │   védelem nélkül marad. Csak tudatos döntés esetén alkalmazd.   │
        ├──────────────────────────────────────────────────────────────────┤
        │ KILL_SWITCH_CLOSE_ALL_POSITIONS=false  (default)                │
        │   Csak a KILL_SWITCH_SCOPE szerinti pozíciók záródnak.          │
        │                                                                  │
        │ KILL_SWITCH_CLOSE_ALL_POSITIONS=true                            │
        │   Minden pozíció azonnali zárása (execution.close_all()).       │
        └──────────────────────────────────────────────────────────────────┘

        Sorrend (order cancel ELŐBB, mint position close — #4 javítás elve):
          1. Orderek törlése (hatókör szerint)
          2. Pozíciók zárása (hatókör szerint)
          3. Fájl átnevezése .triggered-re (audit trail + nem indul újra)
          4. Bot leállítása

        Aktiválás:
            touch data/KILL_SWITCH          # Unix/Linux/macOS
            New-Item data/KILL_SWITCH       # PowerShell
            type nul > data/KILL_SWITCH     # Windows cmd

        Env. változók:
            KILL_SWITCH_PATH              — fájl (default: data/KILL_SWITCH)
            KILL_SWITCH_POLL_SEC          — polling időköz mp-ben (default: 5)
            KILL_SWITCH_SCOPE             — "intraday" | "all" (default: intraday)
            KILL_SWITCH_CANCEL_ALL_ORDERS — "true" | "false" (default: false)
            KILL_SWITCH_CLOSE_ALL_POSITIONS — "true" | "false" (default: false)
        """
        import pathlib
        kill_path = pathlib.Path(KILL_SWITCH_PATH)

        while self.running:
            await asyncio.sleep(KILL_SWITCH_POLL_SEC)
            if not kill_path.exists():
                continue

            # ── Kill switch aktiválva ─────────────────────────────────────────
            scope = KILL_SWITCH_SCOPE.lower()
            cancel_all = KILL_SWITCH_CANCEL_ALL_ORDERS
            close_all  = KILL_SWITCH_CLOSE_ALL_POSITIONS

            # Ha scope=intraday: csak az intraday_symbols érintett
            # Ha scope=all: minden szimbólum listája (config alapján)
            target_symbols: list[str] = (
                list(self.cfg.keys())
                if scope == "all"
                else self.intraday_symbols
            )

            log_event(
                self.logger, "kill_switch_triggered",
                level="WARNING",
                path=str(kill_path),
                scope=scope,
                cancel_all_orders=cancel_all,
                close_all_positions=close_all,
                target_symbols=target_symbols,
            )

            # ── 1. Order törlés ───────────────────────────────────────────────
            if cancel_all:
                # Minden nyitott order törlése (beleértve mode 1 védőordereit!)
                try:
                    await asyncio.to_thread(self.trading.cancel_orders)
                    log_event(
                        self.logger, "kill_switch_all_orders_canceled",
                        level="WARNING",
                    )
                except Exception as exc:
                    log_event(
                        self.logger, "kill_switch_cancel_all_error",
                        level="ERROR", detail=str(exc),
                    )
            else:
                # Csak a hatókörbe eső szimbólumok orderei törlődnek.
                # Mode 1 bracket SL/TP gyermek orderei érintetlenek maradnak,
                # mivel azok nem target_symbols scope-ban vannak (scope=intraday
                # esetén mode 1 szimbólumok nem intraday_symbols tagjai).
                try:
                    await asyncio.to_thread(
                        self._cancel_orders_for_symbols,
                        target_symbols,
                        "kill_switch",
                    )
                    log_event(
                        self.logger, "kill_switch_scoped_orders_canceled",
                        level="WARNING", symbols=target_symbols,
                    )
                except Exception as exc:
                    log_event(
                        self.logger, "kill_switch_cancel_scoped_error",
                        level="ERROR", detail=str(exc),
                    )

            # ── 2. Pozíció zárás ──────────────────────────────────────────────
            if close_all:
                # Minden pozíció zárása
                try:
                    await asyncio.to_thread(
                        self.execution.close_all_positions, "kill_switch_all"
                    )
                    log_event(
                        self.logger, "kill_switch_all_positions_closed",
                        level="WARNING",
                    )
                except Exception as exc:
                    log_event(
                        self.logger, "kill_switch_close_all_error",
                        level="ERROR", detail=str(exc),
                    )
            else:
                # Csak a target_symbols pozíciói záródnak
                try:
                    await asyncio.to_thread(
                        self.execution.close_intraday_positions,
                        target_symbols,
                        "kill_switch",
                    )
                    log_event(
                        self.logger, "kill_switch_scoped_positions_closed",
                        level="WARNING", symbols=target_symbols,
                    )
                except Exception as exc:
                    log_event(
                        self.logger, "kill_switch_close_scoped_error",
                        level="ERROR", detail=str(exc),
                    )

            self.cache.invalidate_positions()
            self.cache.invalidate_orders()

            # ── 3. Fájl átnevezése .triggered-re ─────────────────────────────
            try:
                kill_path.rename(kill_path.with_suffix(".triggered"))
            except Exception:
                pass

            # ── 4. Stream explicit leállítás — ELŐBB, mint self.running = False
            #
            # Indok: a stream task-ok run_in_executor() thread-ben blokkoló
            # stream.run()-ként futnak. Ha csak self.running = False értéket
            # állítunk be, az asyncio.wait() nem feltétlenül tér vissza,
            # mert a blokkoló szál tovább él. A stream.stop() felszabadítja
            # a WebSocket kapcsolatot → a run() visszatér → a task done lesz
            # → az asyncio.wait(FIRST_EXCEPTION) visszatér → reconnect-loop
            # észleli a leállást és normál shutdown-ként kezeli.
            for _stream_obj, _stream_label in [
                (self.stream,         "data_stream"),
                (self.trading_stream, "trade_stream"),
            ]:
                try:
                    _stream_obj.stop()
                except Exception as _stop_exc:
                    log_event(
                        self.logger, "kill_switch_stream_stop_error",
                        level="WARNING",
                        stream=_stream_label,
                        detail=str(_stop_exc),
                    )

            # ── 5. Bot leállítása ─────────────────────────────────────────────
            self.running = False
            log_event(self.logger, "kill_switch_shutdown", level="WARNING")
            return

    def _cancel_orders_for_symbols(
        self, symbols: list[str], reason: str = ""
    ) -> None:
        """Csak a megadott szimbólumok nyitott ordereinek törlése.

        A kill switch scoped (nem CANCEL_ALL) üzemmódjában hívja a
        _kill_switch_loop(). Szándékosan csak a megadott szimbólumok
        orderei törlődnek — mode 1 bracket SL/TP gyermek orderei
        érintetlenek maradnak, ha azok szimbólumai nincsenek a listában.
        """
        _OPEN_STATUSES = OPEN_ALPACA_STATUSES  # centrális definíció, models.py
        target_set = set(symbols)
        try:
            for order in _get_orders_nested(self.trading):
                sym  = str(getattr(order, "symbol", "") or "")
                stat = normalize_order_status(getattr(order, "status", ""))  # [#2-fix]
                if sym in target_set and stat in _OPEN_STATUSES:
                    oid = str(getattr(order, "id", "") or "")
                    if oid:
                        try:
                            self.trading.cancel_order_by_id(oid)
                        except Exception as exc:
                            log_event(
                                self.logger,
                                "kill_switch_cancel_order_error",
                                level="WARNING",
                                symbol=sym, order_id=oid,
                                reason=reason, detail=str(exc),
                            )
        except Exception as exc:
            log_event(
                self.logger, "kill_switch_cancel_orders_fetch_error",
                level="ERROR", reason=reason, detail=str(exc),
            )

    async def process_symbol(self, sym: str) -> None:
        # 1. Versenyhelyzet védelem
        if sym in self.in_flight_symbols:
            log_event(self.logger, "execution_skip", level="DEBUG", symbol=sym, reason="order_already_in_flight")
            return

        if not self._guard_circuit_breaker():
            return

        # [#7] Szinkron cache/REST hívások thread-poolban
        is_open, next_close = await asyncio.to_thread(self._guard_market_open)
        if not is_open:
            return
        if await asyncio.to_thread(self._handle_preclose, next_close):
            return

        signal = await asyncio.to_thread(self._get_signal, sym)
        if not signal:
            return

        self.in_flight_symbols.add(sym)
        try:
            decision = await asyncio.to_thread(self._pretrade_check, sym, signal)
            if not decision:
                await asyncio.to_thread(
                    self.state.update_signal_status,
                    signal.signal_id, "blocked", "pretrade_or_risk_block",
                )
                return

            await asyncio.to_thread(self._submit_entry, sym, signal, decision.max_qty)
        finally:
            self.in_flight_symbols.discard(sym)

    # ── Guard metódusok ───────────────────────────────────────────────────────

    def _guard_circuit_breaker(self) -> bool:
        return not self.risk.circuit_breaker_active

    def _guard_market_open(self) -> tuple[bool, datetime]:
        clock = self.cache.get_clock()
        return clock.is_open, clock.next_close

    def _handle_preclose(self, next_close, minutes: int = 5) -> bool:
        """
        Kereskedési nap vége előtt 5 perccel:
        - Mode 0 és mode 2 pozíciók + nyitott orderek zárása  [#4]
        - Napi CSV összesítő csak broker-visszaigazolt zárás után
        """
        now = datetime.now(timezone.utc)
        if (next_close - now) <= timedelta(minutes=minutes):
            if not self.preclose_actions_done:
                self.execution.close_intraday_positions(
                    self.intraday_symbols, reason="preclose"
                )
                self.cache.invalidate_positions()
                self.cache.invalidate_orders()
                self.preclose_actions_done = True

            self._maybe_write_daily_summary_after_preclose()
            return True
        return False

    def _has_open_intraday_exposure(self) -> bool:
        """Igaz, ha broker oldalon még van intraday pozíció vagy nyitott intraday order."""
        try:
            positions = self.cache.get_positions(max_age_sec=0)
            for pos in positions:
                if getattr(pos, "symbol", None) in self.intraday_symbols and float(getattr(pos, "qty", 0) or 0) != 0:
                    return True
        except Exception as exc:
            log_event(self.logger, "preclose_position_check_error", level="WARNING", detail=str(exc))
            return True

        try:
            open_orders = self.cache.get_open_orders(max_age_sec=0)
            for order in open_orders:
                if getattr(order, "symbol", None) in self.intraday_symbols:
                    return True
        except Exception as exc:
            log_event(self.logger, "preclose_order_check_error", level="WARNING", detail=str(exc))
            return True

        return False

    def _maybe_write_daily_summary_after_preclose(self) -> None:
        """Napi riport csak akkor készül, ha az intraday kitettség már ténylegesen lezárult."""
        if self.daily_summary_done:
            return
        if self._has_open_intraday_exposure():
            log_event(
                self.logger,
                "daily_summary_deferred",
                level="INFO",
                reason="intraday_exposure_still_open",
            )
            return
        self._write_daily_summary()

    # ── Jel generálás ─────────────────────────────────────────────────────────

    def _get_signal(self, sym: str) -> Signal | None:
        raw = self.strategy.evaluate(self.cfg[sym], list(self.candles[sym]))
        if not raw:
            return None
        sig = Signal(
            symbol=sym,
            side="buy",
            limit_price=raw["limit_price"],
            strategy=self.cfg[sym]["strategy"],
            mode=self.cfg[sym]["mode"],
            ts=datetime.now(timezone.utc),
            indicators=raw["indicators"],
        )
        self.state.save_signal(sig, status="new")
        return sig

    # ── Pre-trade és kockázat ellenőrzés ──────────────────────────────────────

    def _pretrade_check(self, sym: str, signal: Signal):
        ok, reason = self.pretrade.can_enter(sym)
        if not ok:
            log_event(
                self.logger, "pretrade_block", level="INFO", symbol=sym, reason=reason
            )
            self.state.update_signal_status(signal.signal_id, "blocked", reason)
            return None

        account = self.cache.get_account()
        if self.risk.evaluate_circuit_breaker(account):
            self.execution.close_all_positions(reason="daily_loss")
            log_event(
                self.logger,
                "circuit_breaker",
                level="WARNING",
                reason="daily_loss_-3pct",
                equity=float(account.equity),
            )
            self.state.update_signal_status(
                signal.signal_id, "blocked", "daily_loss_-3pct"
            )
            return None

        decision = self.risk.evaluate_entry(sym, self.cfg[sym], signal, account)
        if not decision.allowed:
            log_event(
                self.logger,
                "risk_block",
                level="WARNING",
                symbol=sym,
                reason=decision.reason,
            )
            self.state.update_signal_status(
                signal.signal_id, "blocked", decision.reason
            )
            return None
        return decision

    # ── Order benyújtás ───────────────────────────────────────────────────────

    def _submit_entry(self, sym: str, signal: Signal, qty: int) -> None:
        intent = self.execution.create_entry_intent(sym, self.cfg[sym], qty, signal)

        # [#5] Notional limit ellenőrzés éles (és paper) módban.
        # MAX_LIVE_NOTIONAL_USD=0 → nincs limit (nem ajánlott éles módban).
        if MAX_LIVE_NOTIONAL_USD > 0:
            notional = (intent.qty or 0) * (intent.limit_price or 0)
            if notional > MAX_LIVE_NOTIONAL_USD:
                if IS_LIVE:
                    # Éles módban blokkoljuk az ordert
                    log_event(
                        self.logger, "live_notional_limit_block",
                        level="WARNING",
                        symbol=sym,
                        notional=round(notional, 2),
                        max_allowed=MAX_LIVE_NOTIONAL_USD,
                        reason="notional > MAX_LIVE_NOTIONAL_USD",
                    )
                    self.state.update_signal_status(
                        signal.signal_id, "blocked", "live_notional_limit"
                    )
                    return
                else:
                    # Paper módban csak figyelmeztetünk
                    log_event(
                        self.logger, "paper_notional_limit_warning",
                        level="INFO",
                        symbol=sym,
                        notional=round(notional, 2),
                        max_would_block=MAX_LIVE_NOTIONAL_USD,
                        note="paper módban nem blokkol, éles módban blokkolna",
                    )

        try:
            record = self.execution.submit_intent(intent)
        except ExecutionError as exc:
            self.state.update_signal_status(signal.signal_id, "blocked", str(exc))
            log_event(
                self.logger,
                "submit_skip",
                level="INFO",
                symbol=sym,
                reason=str(exc),
            )
            return

        self.state.update_signal_status(
            signal.signal_id, "submitted", record.client_order_id
        )
        self._post_submit_update(sym, signal, record)

    def _post_submit_update(self, sym: str, signal: Signal, record) -> None:
        self.cache.invalidate_orders()
        log_event(
            self.logger,
            "trade_entry",
            symbol=sym,
            side="buy",
            qty=record.qty,
            limit_price=record.limit_price,
            tp_price=record.tp_price,
            sl_price=record.sl_price,
            client_order_id=record.client_order_id,
            broker_order_id=record.broker_order_id,
            strategy=signal.strategy,
            signal_id=signal.signal_id,
            indicators=signal.indicators,
            mode=self.cfg[sym]["mode"],
        )

    # ── Napi összesítő (CSV) ──────────────────────────────────────────────────

    def _write_daily_summary(self) -> None:
        """[#3] Bővített napi CSV riport generálása.

        Összegyűjti az összes szükséges adatot a három forrásból:
          - broker (account equity, open positions, unrealized PnL)
          - state_manager (order statisztikák: trade/win/loss/rejected/canceled)
          - risk_manager (realized PnL, max drawdown, circuit breaker flag)
        """
        if self.daily_summary_done:
            return

        # ── Equity adatok ─────────────────────────────────────────────────────
        try:
            acct = self.cache.get_account(max_age_sec=0)
            close_equity = float(acct.equity)
        except Exception:
            close_equity = self._day_open_equity

        # Drawdown frissítés a záróárral
        self.risk.update_equity(close_equity)

        # ── Nyitott pozíciók ──────────────────────────────────────────────────
        positions = []
        unrealized_pnl_total = 0.0
        try:
            broker_positions = self.cache.get_positions(max_age_sec=0)
            for p in broker_positions:
                upl = float(getattr(p, "unrealized_pl", 0) or 0)
                unrealized_pnl_total += upl
                positions.append({
                    "symbol":       p.symbol,
                    "qty":          float(p.qty),
                    "unrealized_pl": round(upl, 2),
                })
        except Exception:
            unrealized_pnl_total = self.risk.unrealized_pnl

        # ── DB-alapú order statisztikák ───────────────────────────────────────
        order_stats = {"trade_count": 0, "win_count": 0, "loss_count": 0,
                       "rejected_count": 0, "canceled_count": 0,
                       "net_cash_flow": 0.0, "realized_pnl": 0.0}
        try:
            order_stats = self.state.get_daily_order_stats(self._trade_date)
        except Exception:
            pass

        # risk_manager realized_pnl az autoritatív forrás, ha a DB-alapú
        # számítás 0 (pl. fills hiányos startup után)
        realized_pnl = order_stats["realized_pnl"]
        if realized_pnl == 0.0 and self.risk.realized_pnl != 0.0:
            realized_pnl = self.risk.realized_pnl

        trade_date = date.fromisoformat(self._trade_date)

        # ── CSV írás ──────────────────────────────────────────────────────────
        self.reporter.write_day(
            trade_date=trade_date,
            open_equity=self._day_open_equity,
            close_equity=close_equity,
            net_cash_flow=order_stats.get("net_cash_flow", 0.0),
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl_total,
            trade_count=order_stats["trade_count"],
            win_count=order_stats["win_count"],
            loss_count=order_stats["loss_count"],
            max_drawdown_pct=self.risk.max_drawdown_pct,
            circuit_breaker=self.risk.circuit_breaker_active,
            rejected_orders=order_stats["rejected_count"],
            canceled_orders=order_stats["canceled_count"],
            open_positions=len(positions),
        )

        pnl     = close_equity - self._day_open_equity
        pnl_pct = (pnl / self._day_open_equity * 100) if self._day_open_equity else 0.0

        log_event(
            self.logger,
            "daily_summary",
            date=str(trade_date),
            open_equity=round(self._day_open_equity, 2),
            close_equity=round(close_equity, 2),
            daily_pnl=round(pnl, 2),
            daily_pnl_pct=round(pnl_pct, 4),
            net_cash_flow=round(order_stats.get("net_cash_flow", 0.0), 4),
            realized_pnl=round(realized_pnl, 4),
            unrealized_pnl=round(unrealized_pnl_total, 4),
            trade_count=order_stats["trade_count"],
            win_count=order_stats["win_count"],
            loss_count=order_stats["loss_count"],
            max_drawdown_pct=round(self.risk.max_drawdown_pct, 4),
            circuit_breaker=self.risk.circuit_breaker_active,
            rejected_orders=order_stats["rejected_count"],
            canceled_orders=order_stats["canceled_count"],
            open_positions=positions,
            day_trade_count=self.risk.day_trade_count,
            csv_path=str(self.reporter.path),
        )
        self.daily_summary_done = True

    # ── Napi reset (éjfél után) ───────────────────────────────────────────────

    def _check_daily_reset(self) -> None:
        # [#D] Az Alpaca clock.timestamp mezőjéből olvassuk az ET dátumot,
        # ha elérhető — ez a leghitelesebb forrás. Fallback: us_trade_date().
        try:
            clock = self.cache.get_clock()
            today = trading_date_from_clock(clock)  # [#D]
        except Exception:
            today = us_trade_date()  # [#D] fallback
        if today != self._trade_date:
            try:
                acct = self.cache.get_account(max_age_sec=0)
                new_equity = float(acct.equity)
            except Exception:
                new_equity = self._day_open_equity

            self.risk.daily_reset(today, new_equity)
            self._trade_date = today
            self._day_open_equity = new_equity
            self.preclose_actions_done = False
            self.daily_summary_done = False
            # [#2] Óra-aggregátor törlése napi reset-nél
            self._hourly_builder.clear()

            log_event(
                self.logger,
                "daily_reset",
                trade_date=today,
                start_equity=round(new_equity, 2),
            )

    # ── WebSocket feliratkozás ────────────────────────────────────────────────

    def _subscribe(self) -> None:
        # [#6] stream.subscribe_bars() — publikus API, nincs _run_forever()
        for symbol in self.symbols:
            self.stream.subscribe_bars(self.on_bar, symbol)
        self.trading_stream.subscribe_trade_updates(self.on_trade_update)
        log_event(self.logger, "subscribed", symbols=self.symbols)

    # ── Főciklus exponenciális backoff-fal ────────────────────────────────────

    def run(self) -> None:
        self.startup()
        backoff_idx = 0

        while self.running:
            try:
                self._subscribe()

                async def start_streams():
                    # [#2-fix v2] Stream indítási stratégia.
                    #
                    # Az Alpaca StockDataStream és TradingStream publikus API-ja
                    # két metódust dokumentál párhuzamos futáshoz:
                    #
                    #   stream.run()   — szinkron, blokkoló; belül asyncio.run()
                    #                   hívja a _run_forever()-t. Ebből következően
                    #                   NEM awaitelható és asyncio.gather()-rel sem
                    #                   használható (saját event loop-ot nyit).
                    #
                    #   stream.stop_ws() / stream.stop() — leállítja a streamet.
                    #
                    # Mivel a két stream.run() hívás egymást blokkolná, és a
                    # _run_forever() egy implementációs részlet (privát metódus,
                    # jövőbeli verzióban eltűnhet), a helyes megközelítés:
                    # mindkét streamet asyncio.Task-ként, thread-poolban futtatjuk,
                    # így a publikus API-ra támaszkodunk, és az event loop szabadon
                    # kezeli a háttérfeladatokat is.
                    loop = asyncio.get_event_loop()

                    # [#5] Háttérfeladatok
                    loop.create_task(self._circuit_breaker_loop())
                    loop.create_task(self._market_close_failsafe_loop())
                    loop.create_task(self._kill_switch_loop())  # [#2]

                    # Mindkét stream.run() (szinkron, blokkoló) futtatása
                    # külön thread-ben — így az event loop nem blokkolódik,
                    # a két stream valóban párhuzamosan fut, és a publikus
                    # API-t használjuk a privát _run_forever() helyett.
                    data_task  = loop.run_in_executor(None, self.stream.run)
                    trade_task = loop.run_in_executor(None, self.trading_stream.run)

                    # [#2 stream] asyncio.wait() FIRST_EXCEPTION stratégiával.
                    #
                    # Probléma az asyncio.gather(return_exceptions=True) megoldással:
                    #   gather() akkor tér vissza, ha MINDKÉT task befejeződött.
                    #   Ha az egyik stream hibával leáll, a másik thread-ben futó
                    #   stream.run() tovább blokkolhat — a reconnect logika addig
                    #   nem aktiválódik, amíg a másik stream is le nem áll
                    #   (ami soha nem történik meg magától hálózati leállás esetén).
                    #
                    # Megoldás: asyncio.wait(return_when=FIRST_EXCEPTION)
                    #   - Visszatér, amint az ELSŐ task kivétellel zár (vagy mind kész).
                    #   - A pending (még futó) taskokat explicit stream.stop()-pal
                    #     leállítjuk, hogy a blokkoló szál felszabaduljon.
                    #   - A done task kivételét propagáljuk a reconnect-loopba.
                    #   - Ha mindkét stream normálisan zárt (pl. stop() hívásra),
                    #     kivétel nélkül tér vissza → szabályos leállás.
                    done, pending = await asyncio.wait(
                        {data_task, trade_task},
                        return_when=asyncio.FIRST_EXCEPTION,
                    )

                    # Ha van pending task: az egyik stream leállt, a másikat
                    # explicit stream.stop()-pal kérjük le — ez felszabadítja
                    # a run_in_executor thread-ben blokkoló stream.run()-t.
                    if pending:
                        for stream_obj, label in [
                            (self.stream,         "data_stream"),
                            (self.trading_stream, "trade_stream"),
                        ]:
                            try:
                                stream_obj.stop()
                            except Exception as stop_exc:
                                log_event(
                                    self.logger, "stream_stop_on_peer_error",
                                    level="WARNING",
                                    stream=label, detail=str(stop_exc),
                                )
                        # Megvárjuk, hogy a pending task(ok) valóban leálljanak
                        # (rövid timeout: ha stop() nem elég, a reconnect-loop
                        # úgyis új stream objektumokat hoz létre)
                        await asyncio.wait(pending, timeout=5.0)

                    # Done task-ok vizsgálata: ha valamelyik kivétellel zárt,
                    # propagáljuk a reconnect-loopba.
                    for task in done:
                        if not task.cancelled():
                            exc = task.exception()
                            if exc is not None:
                                task_label = (
                                    "data_stream"
                                    if task is data_task
                                    else "trade_stream"
                                )
                                log_event(
                                    self.logger, "stream_task_error",
                                    level="WARNING",
                                    stream=task_label, detail=str(exc),
                                )
                                raise exc  # reconnect-loop fogja el

                asyncio.run(start_streams())
                break  # Szabályos leállás
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                wait = RECONNECT_BACKOFFS[
                    min(backoff_idx, len(RECONNECT_BACKOFFS) - 1)
                ]
                log_event(
                    self.logger,
                    "reconnect",
                    level="WARNING",
                    detail=str(exc),
                    wait_sec=wait,
                    attempt=backoff_idx + 1,
                )
                self.cache.invalidate_account()
                self.cache.invalidate_orders()
                self.cache.invalidate_positions()
                self.state.recover_from_broker(self.trading)
                self.execution.reconcile_orders()
                repaired = self.state.repair_parent_client_order_ids()
                if repaired:
                    log_event(
                        self.logger, "parent_ids_repaired_reconnect",
                        level="INFO", count=repaired,
                    )
                time.sleep(wait)
                backoff_idx += 1

                # Új stream objektumok az újracsatlakozáshoz
                self.stream = StockDataStream(API_KEY, SECRET)
                self.trading_stream = TradingStream(API_KEY, SECRET, paper=not IS_LIVE)

    # ── Leállítás ─────────────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """Szabályos leállítás: stream stop → riport → DB lezárás.

        Idempotens: többszöri hívás (finally blokk + kill switch + CTRL+C)
        nem okoz dupla log/DB műveletet. Az első hívás beállítja
        _shutdown_done=True, a további hívások azonnal visszatérnek.

        [#1-stream] Az Alpaca stream.stop() explicit hívása szükséges,
        mert a run_in_executor()-ban futó stream.run() blokkoló szálat
        önmagától nem állítja le sem CTRL+C, sem SIGTERM esetén.
        """
        if getattr(self, "_shutdown_done", False):
            return
        self._shutdown_done = True
        self.running = False

        # Stream leállítás (publikus Alpaca API)
        for stream_obj, label in [
            (self.stream,          "data_stream"),
            (self.trading_stream,  "trade_stream"),
        ]:
            try:
                stream_obj.stop()
            except Exception as stop_exc:
                log_event(
                    self.logger, "stream_stop_error",
                    level="WARNING", stream=label, detail=str(stop_exc),
                )

        if not self.daily_summary_done:
            self._write_daily_summary()
        self.state.end_run(self.run_id)
        self.state.close()
        log_event(self.logger, "shutdown", run_id=self.run_id)


# ── Belépési pont ──────────────────────────────────────────────────────────────

def _signal_handler(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    if not API_KEY or not SECRET:
        print(
            "HIBA: Hiányzó Alpaca API kulcsok!\n"
            "Hozzon létre .env fájlt az alábbi tartalommal:\n"
            "  ALPACA_API_KEY=your_key\n"
            "  ALPACA_SECRET_KEY=your_secret\n"
            "  ALPACA_LIVE=false  # paper mód (alapértelmezett)\n",
            file=sys.stderr,
        )
        sys.exit(1)

    signal.signal(signal.SIGTERM, _signal_handler)

    mode_label = "LIVE" if IS_LIVE else "PAPER"
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Bot indul — mód: {mode_label}")
    print(f"  Konfiguráció: {CONFIG_PATH}")
    print(f"  Szimbólumok: {', '.join(get_symbols(load_config(CONFIG_PATH)))}")
    print(f"  Leállítás: CTRL+C\n")

    bot = TradingBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n[Bot leállítás — CTRL+C]")
        log_event(bot.logger, "shutdown", reason="keyboard_interrupt")
    except Exception as exc:
        log_event(bot.logger, "fatal_error", level="ERROR", detail=str(exc))
        raise
    finally:
        # [#1-fix] A finally blokk garantálja, hogy shutdown() mindig lefut:
        #   - normál visszatérés (kill switch utáni szabályos leállás)
        #   - KeyboardInterrupt (CTRL+C)
        #   - váratlan kivétel
        # Így soha nem maradhat el a napi summary, state.end_run(),
        # SQLite kapcsolat lezárása és a végső shutdown log.
        bot.shutdown()
