from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from alpaca.trading.requests import GetOrdersRequest  # [#2-fix]
from typing import Any, Optional

from modules.models import OrderRecord, OrderState, Signal

SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    stopped_at TEXT,
    mode TEXT,
    config_hash TEXT
);
CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    strategy TEXT,
    side TEXT,
    payload_json TEXT,
    status TEXT DEFAULT 'new',
    status_reason TEXT
);
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    broker_order_id TEXT,
    signal_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT,
    qty INTEGER,
    filled_qty REAL DEFAULT 0,
    avg_fill_price REAL,
    limit_price REAL,
    tp_price REAL,
    sl_price REAL,
    tif TEXT,
    strategy TEXT,
    order_role TEXT DEFAULT 'entry',
    parent_client_order_id TEXT,
    state TEXT NOT NULL,
    reject_reason TEXT,
    submitted_at TEXT,
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    broker_order_id TEXT,
    client_order_id TEXT,
    symbol TEXT NOT NULL,
    qty REAL,
    price REAL,
    ts TEXT
);
CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT PRIMARY KEY,
    qty REAL,
    avg_entry_price REAL,
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS risk_state (
    trade_date TEXT PRIMARY KEY,
    start_equity REAL,
    realized_pnl REAL DEFAULT 0,
    unrealized_pnl REAL DEFAULT 0,
    day_trade_count INTEGER DEFAULT 0,
    circuit_breaker_active INTEGER DEFAULT 0,
    equity_high REAL DEFAULT 0,
    max_drawdown_pct REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_state ON orders(state);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_fill_id(broker_order_id: str, cumulative_qty: float, avg_price: float) -> str:
    """[#3-fix] Kanonikus fill_id — az egyetlen engedélyezett formátum.

    Minden fill-rögzítő útvonalnak (trade-update stream, reconcile polling)
    ezt a függvényt kell hívnia, így ugyanaz a broker fill-esemény mindig
    ugyanazt a fill_id-t kapja, függetlenül attól, hogy melyik útvonalon
    érkezik. A fills tábla 'INSERT OR IGNORE' szemantikája garantálja, hogy
    a duplikát rekord csendben elveszik — az első bejegyzés autoritatív.

    Formátum: "<broker_order_id>@<cumulative_qty>@<avg_price>"
    (A ':' helyett '@' elválasztó, mert a broker_order_id néha tartalmaz
    ':'-t uuid4 reprezentációkban egyes Alpaca verziókban.)
    """
    return f"{broker_order_id}@{cumulative_qty}@{avg_price}"


class StateManager:
    def __init__(self, path: str = "state.db"):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        with self._conn:
            self._conn.executescript(SCHEMA)
            self._ensure_column("signals", "status_reason", "TEXT")
            self._ensure_column("orders", "strategy", "TEXT")
            self._ensure_column("orders", "order_role", "TEXT DEFAULT 'entry'")
            self._ensure_column("orders", "parent_client_order_id", "TEXT")
            self._ensure_column("orders", "reject_reason", "TEXT")
            # Drawdown mezők migrálása meglévő DB-ekhez
            self._ensure_column("risk_state", "equity_high", "REAL DEFAULT 0")
            self._ensure_column("risk_state", "max_drawdown_pct", "REAL DEFAULT 0")

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        cols = {
            row["name"]
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in cols:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    @contextmanager
    def _tx(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def start_run(self, run_id: str, mode: str, config_hash: str) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT OR REPLACE INTO bot_runs(run_id, started_at, mode, config_hash) VALUES (?,?,?,?)",
                (run_id, _utc_now(), mode, config_hash),
            )

    def end_run(self, run_id: str) -> None:
        with self._tx() as c:
            c.execute("UPDATE bot_runs SET stopped_at=? WHERE run_id=?", (_utc_now(), run_id))

    def save_signal(self, sig: Signal, status: str = "new", reason: str | None = None) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO signals(signal_id, symbol, ts, strategy, side, payload_json, status, status_reason) VALUES (?,?,?,?,?,?,?,?)",
                (
                    sig.signal_id,
                    sig.symbol,
                    sig.ts.isoformat(),
                    sig.strategy,
                    sig.side,
                    json.dumps(sig.indicators),
                    status,
                    reason,
                ),
            )

    def update_signal_status(self, signal_id: str, status: str, reason: str | None = None) -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE signals SET status=?, status_reason=? WHERE signal_id=?",
                (status, reason, signal_id),
            )

    def save_order_intent(self, rec: OrderRecord) -> None:
        with self._tx() as c:
            c.execute(
                """
                INSERT OR IGNORE INTO orders(
                    client_order_id, broker_order_id, signal_id, symbol, side, qty,
                    filled_qty, avg_fill_price, limit_price, tp_price, sl_price,
                    tif, strategy, order_role, parent_client_order_id, state,
                    reject_reason, submitted_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    rec.client_order_id,
                    rec.broker_order_id,
                    rec.signal_id,
                    rec.symbol,
                    rec.side,
                    rec.qty,
                    rec.filled_qty,
                    rec.avg_fill_price,
                    rec.limit_price,
                    rec.tp_price,
                    rec.sl_price,
                    rec.tif,
                    rec.strategy,
                    rec.order_role,
                    rec.parent_client_order_id,
                    rec.state.value,
                    rec.reject_reason,
                    _utc_now(),
                    _utc_now(),
                ),
            )

    def import_broker_order(self, rec: OrderRecord) -> None:
        with self._tx() as c:
            c.execute(
                """
                INSERT INTO orders(
                    client_order_id, broker_order_id, signal_id, symbol, side, qty,
                    filled_qty, avg_fill_price, limit_price, tp_price, sl_price,
                    tif, strategy, order_role, parent_client_order_id, state,
                    reject_reason, submitted_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(client_order_id) DO UPDATE SET
                    broker_order_id=excluded.broker_order_id,
                    signal_id=excluded.signal_id,
                    symbol=excluded.symbol,
                    side=excluded.side,
                    qty=excluded.qty,
                    filled_qty=excluded.filled_qty,
                    avg_fill_price=excluded.avg_fill_price,
                    limit_price=excluded.limit_price,
                    tp_price=excluded.tp_price,
                    sl_price=excluded.sl_price,
                    tif=excluded.tif,
                    strategy=excluded.strategy,
                    order_role=excluded.order_role,
                    parent_client_order_id=excluded.parent_client_order_id,
                    state=excluded.state,
                    reject_reason=excluded.reject_reason,
                    updated_at=excluded.updated_at
                """,
                (
                    rec.client_order_id,
                    rec.broker_order_id,
                    rec.signal_id,
                    rec.symbol,
                    rec.side,
                    rec.qty,
                    rec.filled_qty,
                    rec.avg_fill_price,
                    rec.limit_price,
                    rec.tp_price,
                    rec.sl_price,
                    rec.tif,
                    rec.strategy,
                    rec.order_role,
                    rec.parent_client_order_id,
                    rec.state.value,
                    rec.reject_reason,
                    _utc_now(),
                    _utc_now(),
                ),
            )

    def mark_order_submitted(self, client_order_id: str, broker_order_id: str | None) -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE orders SET broker_order_id=?, state=?, updated_at=? WHERE client_order_id=?",
                (broker_order_id, OrderState.SUBMITTED.value, _utc_now(), client_order_id),
            )

    def mark_order_rejected(self, client_order_id: str, reason: str = "") -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE orders SET state=?, reject_reason=?, updated_at=? WHERE client_order_id=?",
                (OrderState.REJECTED.value, reason, _utc_now(), client_order_id),
            )

    def update_order_state(
        self,
        client_order_id: str,
        state: OrderState,
        filled_qty: float = 0,
        avg_price: Optional[float] = None,
        reject_reason: str | None = None,
    ) -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE orders SET state=?, filled_qty=?, avg_fill_price=?, reject_reason=COALESCE(?, reject_reason), updated_at=? WHERE client_order_id=?",
                (state.value, filled_qty, avg_price, reject_reason, _utc_now(), client_order_id),
            )

    def mark_missing_open_orders_closed(self, broker_open_client_order_ids: set[str]) -> None:
        with self._tx() as c:
            rows = c.execute(
                "SELECT client_order_id FROM orders WHERE state IN (?, ?)",
                (OrderState.SUBMITTED.value, OrderState.PARTIALLY_FILLED.value),
            ).fetchall()
            for row in rows:
                coid = row["client_order_id"]
                if coid not in broker_open_client_order_ids:
                    c.execute(
                        "UPDATE orders SET state=?, updated_at=? WHERE client_order_id=?",
                        (OrderState.CANCELED.value, _utc_now(), coid),
                    )

    def order_exists(self, client_order_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
        return row is not None

    def get_open_orders(self) -> list[dict[str, Any]]:
        states = (OrderState.SUBMITTED.value, OrderState.PARTIALLY_FILLED.value)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM orders WHERE state IN (?, ?)", states
            ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_pending_orders(self, symbol: str, max_age_sec: int = 120) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max_age_sec)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM orders WHERE symbol=? AND state IN (?, ?, ?) AND updated_at >= ?",
                (
                    symbol,
                    OrderState.INTENT.value,
                    OrderState.SUBMITTED.value,
                    OrderState.PARTIALLY_FILLED.value,
                    cutoff,
                ),
            ).fetchall()
        return [dict(r) for r in rows]

    def apply_fill(
        self,
        fill_id: str,
        broker_order_id: str,
        client_order_id: str,
        symbol: str,
        qty: float,
        price: float,
    ) -> bool:
        """Fill esemény perzisztálása, idempotens módon.

        [#1-fix] Visszatérési érték:
          True  — a fill ténylegesen bekerült az adatbázisba (új rekord)
          False — a fill már létezett (INSERT OR IGNORE silent-skip)

        A hívó köteles ezt ellenőrizni, mielőtt risk/day-trade ledgert
        frissít, hogy elkerülje a dupla on_fill() hívást restart/reconnect
        és párhuzamos útvonal esetén egyaránt.
        """
        with self._tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO fills(fill_id, broker_order_id, client_order_id, symbol, qty, price, ts) VALUES (?,?,?,?,?,?,?)",
                (fill_id, broker_order_id, client_order_id, symbol, qty, price, _utc_now()),
            )
            return c.rowcount > 0  # 1 = tényleges insert; 0 = IGNORE (már létezett)

    def upsert_position(self, symbol: str, qty: float, avg_entry: float) -> None:
        with self._tx() as c:
            if qty == 0:
                c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            else:
                c.execute(
                    """
                    INSERT INTO positions(symbol, qty, avg_entry_price, updated_at)
                    VALUES (?,?,?,?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        qty=excluded.qty,
                        avg_entry_price=excluded.avg_entry_price,
                        updated_at=excluded.updated_at
                    """,
                    (symbol, qty, avg_entry, _utc_now()),
                )

    def replace_positions_from_broker(self, positions: list[tuple[str, float, float]]) -> None:
        with self._tx() as c:
            c.execute("DELETE FROM positions")
            for symbol, qty, avg_entry in positions:
                if qty != 0:
                    c.execute(
                        "INSERT INTO positions(symbol, qty, avg_entry_price, updated_at) VALUES (?,?,?,?)",
                        (symbol, qty, avg_entry, _utc_now()),
                    )

    def get_open_positions(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM positions").fetchall()
        return {r["symbol"]: dict(r) for r in rows}

    def get_client_order_id_by_broker_id(self, broker_order_id: str) -> str | None:
        """Broker order ID (UUID) → lokális client_order_id feloldás.

        [#2-fix] A trade update stream-en érkező child order event-ek
        order.order_id mezője az Alpaca broker-oldali parent UUID-t adja —
        nem a lokális client_order_id-t. Ez a lookup DB-ből oldja fel a
        kapcsolatot, hogy a parent_client_order_id mező helyesen kerüljön
        mentésre és a round-trip PnL JOIN működjön.

        Visszatér: client_order_id string ha megtalálta, None ha nem.
        """
        if not broker_order_id:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT client_order_id FROM orders WHERE broker_order_id = ? LIMIT 1",
                (broker_order_id,),
            ).fetchone()
        return row["client_order_id"] if row else None

    def repair_parent_client_order_ids(self) -> int:
        """Sérült parent_client_order_id mezők javítása reconnect/restart után.

        [#2-fix] Akkor fordulhat elő, hogy a parent_client_order_id mező
        broker UUID-t tartalmaz (nem lokális client_order_id-t):
          - Korai child fill érkezett, mielőtt a parent entry order
            mentésre került (parent broker_order_id-ból lookup sikertelen).
          - Reconnect után a child importálódott, de a parent még nem volt
            a DB-ben.

        Ez a metódus megkeresi azokat a child ordereket, ahol a
        parent_client_order_id NEM szerepel az orders tábla
        client_order_id oszlopában (tehát broker UUID-t tartalmaz),
        majd a broker_order_id → client_order_id lookup alapján
        helyesbíti őket.

        Visszatér: javított sorok száma (0 = semmi sérült nem volt).
        """
        with self._lock:
            # Child orderek, ahol parent_client_order_id nem ismert lokális ID
            broken = self._conn.execute(
                """
                SELECT client_order_id, parent_client_order_id
                FROM orders
                WHERE parent_client_order_id IS NOT NULL
                  AND parent_client_order_id NOT IN (
                      SELECT client_order_id FROM orders
                  )
                """,
            ).fetchall()

        fixed = 0
        for row in broken:
            child_coid  = row["client_order_id"]
            broken_pid  = row["parent_client_order_id"]
            # Keresés: van-e order amelynek broker_order_id = broken_pid?
            with self._lock:
                parent_row = self._conn.execute(
                    "SELECT client_order_id FROM orders WHERE broker_order_id = ? LIMIT 1",
                    (broken_pid,),
                ).fetchone()
            if parent_row:
                correct_parent_coid = parent_row["client_order_id"]
                with self._lock:
                    self._conn.execute(
                        "UPDATE orders SET parent_client_order_id = ? WHERE client_order_id = ?",
                        (correct_parent_coid, child_coid),
                    )
                    self._conn.commit()
                fixed += 1
        return fixed

    def get_daily_order_stats(self, trade_date: str) -> dict[str, Any]:
        """Napi order statisztikák CSV riporthoz.

        Visszatérési értékek:
          trade_count    — összes benyújtott entry order
          win_count      — TP-en zárt order (filled, role='tp')
          loss_count     — SL-en zárt order (filled, role='sl')
          rejected_count — elutasított orderek száma
          canceled_count — törölt orderek száma

          net_cash_flow  — az aznapi összes fill nettó pénzmozgása:
                           SUM(sell_fills) − SUM(buy_fills)
                           Negatív = többet vásároltunk mint eladtunk.
                           Ez NEM azonos a realizált PnL-lel, ha nyitott
                           pozíciók maradnak nap végén (ld. lent).

          realized_pnl   — csak lezárt round-trip alapján számolt PnL.
                           Feltétel: a szimbólumhoz mindkét láb (entry buy
                           fill + TP/SL sell fill) aznap megjelent a fills
                           táblában. Nyitott, el nem zárt pozíciók entry
                           buy fillje NEM számít bele — így a mode 1
                           (trend following) nap végén nyitva maradó
                           pozíciói nem torzítják a riportot negatív
                           irányba (nem kezeli őket „realizált veszteségként").

        [#2 PnL-fix] A korábbi verzió buy/sell összegzéssel számolt
        realized_pnl-t, ami félrevezető volt: ha egy entry buy fill
        aznap keletkezett, de a zárási sell fill nem, a buy összeg
        negatívként jelent meg a riportban (mintha veszteség lett volna),
        holott az csak egy nyitott pozíció cash-flow-ja.
        """
        with self._lock:
            # ── Darabszámok ───────────────────────────────────────────────────
            trade_count = self._conn.execute(
                "SELECT COUNT(*) FROM orders WHERE order_role='entry' AND DATE(submitted_at)=?",
                (trade_date,),
            ).fetchone()[0] or 0

            win_count = self._conn.execute(
                """SELECT COUNT(*) FROM orders
                   WHERE order_role='tp' AND state='filled'
                   AND DATE(updated_at)=?""",
                (trade_date,),
            ).fetchone()[0] or 0

            loss_count = self._conn.execute(
                """SELECT COUNT(*) FROM orders
                   WHERE order_role='sl' AND state='filled'
                   AND DATE(updated_at)=?""",
                (trade_date,),
            ).fetchone()[0] or 0

            rejected_count = self._conn.execute(
                """SELECT COUNT(*) FROM orders
                   WHERE state='rejected' AND DATE(updated_at)=?""",
                (trade_date,),
            ).fetchone()[0] or 0

            canceled_count = self._conn.execute(
                """SELECT COUNT(*) FROM orders
                   WHERE state='canceled' AND DATE(updated_at)=?""",
                (trade_date,),
            ).fetchone()[0] or 0

            # ── Net cash flow (minden fill) ────────────────────────────────────
            # Minden aznapi fill pénzmozgása — buy + sell együtt.
            # Ez a szám megmutatja a nap bruttó cash-igényét, de NEM
            # azonos a realizált PnL-lel, ha nyitott pozíciók maradnak.
            all_fills = self._conn.execute(
                """SELECT o.side, o.order_role, o.symbol, f.qty, f.price
                   FROM fills f
                   JOIN orders o ON f.client_order_id = o.client_order_id
                   WHERE DATE(f.ts) = ?""",
                (trade_date,),
            ).fetchall()

            # ── Realizált PnL: csak lezárt round-trip-ek ──────────────────────
            # Lezárt round-trip = van entry fill ÉS van TP/SL fill ugyanahhoz
            # a szimbólumhoz aznap. A szimbólum szintű aggregáción belül
            # csak azokat az entry buy összegeket vonjuk le a sell bevételből,
            # amelyekhez van párjuk (záró sell fill).
            #
            # SQL megközelítés: az orders táblán belül a parent-child reláció
            # alapján párosítunk (entry ↔ tp/sl), majd a fills-ből összegzünk.
            closed_roundtrips = self._conn.execute(
                """
                SELECT
                    e_f.qty   AS entry_qty,
                    e_f.price AS entry_price,
                    x_f.qty   AS exit_qty,
                    x_f.price AS exit_price,
                    x_o.order_role AS exit_role
                FROM orders e_o                            -- entry order
                JOIN fills  e_f ON e_f.client_order_id = e_o.client_order_id
                JOIN orders x_o ON x_o.parent_client_order_id = e_o.client_order_id
                                AND x_o.state = 'filled'
                                AND x_o.order_role IN ('tp', 'sl')
                JOIN fills  x_f ON x_f.client_order_id = x_o.client_order_id
                WHERE e_o.order_role = 'entry'
                  AND DATE(x_f.ts) = ?
                """,
                (trade_date,),
            ).fetchall()

        # Net cash flow számítás
        buy_total  = sum(r["qty"] * r["price"] for r in all_fills if r["side"] == "buy")
        sell_total = sum(r["qty"] * r["price"] for r in all_fills if r["side"] == "sell")
        net_cash_flow = round(sell_total - buy_total, 4)

        # Realizált PnL: matched_qty * (exit_price − entry_price)
        #
        # [#1-fix] Részleges zárásnál a korábbi képlet hibát adott:
        #   exit_qty * exit_price − entry_qty * entry_price
        #   pl. entry 10×100, exit 5×110 → 550 − 1000 = −450 USD  ✗
        #
        # Helyes: matched_qty * (exit_price − entry_price)
        #   min(10, 5) × (110 − 100) = 5 × 10 = +50 USD  ✓
        #
        # matched_qty = min(entry_qty, exit_qty): csak a ténylegesen
        # lezárt mennyiségre számolunk PnL-t. Ha az entry részlegesen
        # teljesült (entry_qty < teljes lot), vagy a TP/SL részlegesen
        # zárt (exit_qty < entry_qty), mindig a kisebbet vesszük.
        # Ez egy FIFO/átlagár-kompatibilis egyszerűsített modell.
        realized_pnl = round(
            sum(
                min(r["entry_qty"], r["exit_qty"]) * (r["exit_price"] - r["entry_price"])
                for r in closed_roundtrips
            ),
            4,
        )

        return {
            "trade_count":     trade_count,
            "win_count":       win_count,
            "loss_count":      loss_count,
            "rejected_count":  rejected_count,
            "canceled_count":  canceled_count,
            "net_cash_flow":   net_cash_flow,
            "realized_pnl":    realized_pnl,
        }

    def load_risk_state(self, trade_date: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM risk_state WHERE trade_date=?",
                (trade_date,),
            ).fetchone()
        return dict(row) if row else None

    def save_risk_state(
        self,
        trade_date: str,
        start_equity: float,
        realized_pnl: float,
        unrealized_pnl: float,
        day_trade_count: int,
        circuit_breaker: bool,
        equity_high: float = 0.0,
        max_drawdown_pct: float = 0.0,
    ) -> None:
        with self._tx() as c:
            c.execute(
                """
                INSERT INTO risk_state(
                    trade_date, start_equity, realized_pnl, unrealized_pnl,
                    day_trade_count, circuit_breaker_active,
                    equity_high, max_drawdown_pct
                ) VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(trade_date) DO UPDATE SET
                    start_equity=excluded.start_equity,
                    realized_pnl=excluded.realized_pnl,
                    unrealized_pnl=excluded.unrealized_pnl,
                    day_trade_count=excluded.day_trade_count,
                    circuit_breaker_active=excluded.circuit_breaker_active,
                    equity_high=excluded.equity_high,
                    max_drawdown_pct=excluded.max_drawdown_pct
                """,
                (
                    trade_date, start_equity, realized_pnl, unrealized_pnl,
                    day_trade_count, int(circuit_breaker),
                    equity_high, max_drawdown_pct,
                ),
            )

    # child order_type → order_role mappa (Alpaca bracket leg-ek)
    _CHILD_ORDER_ROLES: dict[str, str] = {
        "limit":      "tp",
        "stop":       "sl",
        "stop_limit": "sl",
    }

    def recover_from_broker(self, trading_client) -> dict[str, dict[str, Any]]:
        """Broker állapot visszatöltése indításkor.

        [#C] Bracket child leg-ek felismerése: az order.order_class == 'bracket'
        parent orderekből kinyerjük a legs listát és a child-eket helyes
        order_role ('tp'/'sl') + parent_client_order_id értékkel importáljuk,
        nem hamis 'entry' szerepként.
        """
        positions = trading_client.get_all_positions()
        normalized_positions = [
            (p.symbol, float(p.qty), float(p.avg_entry_price)) for p in positions
        ]
        self.replace_positions_from_broker(normalized_positions)

        # [#2-fix] nested=True: bracket child leg-ek a parent order .legs
        #   mezőjében jelennek meg, nem külön top-level entitásként.
        try:
            broker_orders = trading_client.get_orders(GetOrdersRequest(nested=True))
        except TypeError:
            broker_orders = trading_client.get_orders()  # fallback régi SDK-ra
        broker_open_ids: set[str] = set()
        known_child_ids: set[str] = set()

        # 1. pass: parent orderek + child leg-ek mentése
        for order in broker_orders:
            parent_coid = str(getattr(order, "client_order_id", "") or "")
            if not parent_coid:
                continue

            order_class = str(getattr(order, "order_class", "") or "").lower()
            state       = OrderState.from_alpaca(getattr(order, "status", "unknown"))
            lp          = getattr(order, "limit_price", None)
            avg_p       = getattr(order, "filled_avg_price", None)

            parent_record = OrderRecord(
                client_order_id=parent_coid,
                broker_order_id=str(getattr(order, "id", "") or "") or None,
                signal_id="",
                symbol=str(getattr(order, "symbol", "") or ""),
                side=str(getattr(order, "side", "buy")).lower(),
                qty=int(float(getattr(order, "qty", 0) or 0)),
                filled_qty=float(getattr(order, "filled_qty", 0) or 0),
                avg_fill_price=float(avg_p) if avg_p is not None else None,
                limit_price=float(lp) if lp is not None else None,
                tif=str(getattr(order, "time_in_force", "day")).lower(),
                order_role="entry",
                parent_client_order_id=None,
                state=state,
                submitted_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            self.import_broker_order(parent_record)
            if state.is_open:
                broker_open_ids.add(parent_coid)

            # Bracket child leg-ek
            if order_class == "bracket":
                legs = getattr(order, "legs", None) or []
                for leg in legs:
                    child_coid      = str(getattr(leg, "client_order_id", "") or "")
                    child_broker_id = str(getattr(leg, "id", "") or "") or None
                    if not child_coid:
                        child_coid = f"child-{child_broker_id or 'unknown'}"

                    known_child_ids.add(child_coid)
                    child_state = OrderState.from_alpaca(getattr(leg, "status", "new"))
                    leg_ot      = str(getattr(leg, "order_type", "") or "").lower()
                    role        = self._CHILD_ORDER_ROLES.get(leg_ot, "child")
                    leg_lp      = getattr(leg, "limit_price", None)
                    leg_stop    = getattr(leg, "stop_price", None)
                    leg_avg     = getattr(leg, "filled_avg_price", None)

                    child_record = OrderRecord(
                        client_order_id=child_coid,
                        broker_order_id=child_broker_id,
                        signal_id="",
                        symbol=str(getattr(leg, "symbol", "") or ""),
                        side=str(getattr(leg, "side", "sell")).lower(),
                        qty=int(float(getattr(leg, "qty", 0) or 0)),
                        filled_qty=float(getattr(leg, "filled_qty", 0) or 0),
                        avg_fill_price=float(leg_avg) if leg_avg is not None else None,
                        limit_price=float(leg_lp) if leg_lp is not None else None,
                        sl_price=float(leg_stop) if leg_stop is not None else None,
                        tif=str(getattr(leg, "time_in_force", "day")).lower(),
                        order_role=role,
                        parent_client_order_id=parent_coid,
                        state=child_state,
                        submitted_at=None,
                        updated_at=datetime.now(timezone.utc),
                    )
                    self.import_broker_order(child_record)
                    if child_state.is_open:
                        broker_open_ids.add(child_coid)

        self.mark_missing_open_orders_closed(broker_open_ids)
        return self.get_open_positions()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
