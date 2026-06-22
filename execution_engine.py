from __future__ import annotations

"""
execution_engine.py
===================
Javítások:
  #3  mode 2 → DAY TIF (nem GTC)                               [előző kör]
  #4  close_intraday_positions(): order cancel előbb, zárás utóbb [előző kör]
  #B  refresh_order_state() delta-alapú fill kezelés             [EZ A KÖR]
  #C  reconcile_orders(): bracket child order felismerés,
      order_role + parent_client_order_id helyes beállítása      [EZ A KÖR]
"""

from datetime import datetime, timezone

from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import (
    GetOrdersRequest,
    LimitOrderRequest,
    StopLossRequest,
    TakeProfitRequest,
)

from modules.models import OrderIntent, OrderRecord, OrderState, normalize_order_status, OPEN_ALPACA_STATUSES


def _get_orders_nested(client, **kwargs):
    """[#2-fix] get_orders() hívás nested=True paraméterrel.

    A GetOrdersRequest nested=True paramétere biztosítja, hogy az Alpaca
    multi-leg (bracket) orderek legs mezőjükön adják vissza a child leg-eket,
    ahelyett, hogy azokat is külön top-level entitásként listáznák.

    Ha a telepített alpaca-py verzió nem támogatja a nested paramétert
    (régebbi SDK), graceful fallback: sima get_orders() hívás.
    """
    try:
        req = GetOrdersRequest(nested=True, **kwargs)
        return client.get_orders(req)
    except TypeError:
        # Régi alpaca-py: GetOrdersRequest nem fogad nested paramétert
        return client.get_orders()
from modules.state_manager import canonical_fill_id  # [#3-fix]


class ExecutionError(Exception):
    """Nem-fatális végrehajtási hiba."""


class ExecutionEngine:
    MIN_PRICE = 0.01
    MIN_STOP_DISTANCE_PCT = 0.001

    # Az Alpaca bracket child-order típusok, amelyek TP/SL szerepet töltenek be.
    # Az order.order_class + order.order_type kombinációból olvasható ki.
    _CHILD_ROLES: dict[str, str] = {
        # (order_class, order_type) → order_role
        "limit":     "tp",   # take_profit child: limit order
        "stop":      "sl",   # stop_loss child: stop order
        "stop_limit": "sl",  # stop_loss child stop_limit variáns
    }

    def __init__(self, trading_client, logger, state_manager, run_id: str):
        self.client = trading_client
        self.logger = logger
        self.state = state_manager
        self.run_id = run_id

        # [#B] fill delta tracker: client_order_id → eddig feldolgozott cumulative qty
        # Ugyanaz a logika, mint a main.py-ban lévő processed_fill_qty, de
        # a refresh_order_state() pollozási útvonalon.
        self._refresh_fill_seen: dict[str, float] = {}

    # ── Intent létrehozás ──────────────────────────────────────────────────────

    def create_entry_intent(self, symbol, cfg, qty, signal) -> OrderIntent:
        limit_price = round(signal.limit_price, 2)
        tp_price    = round(limit_price * (1 + cfg["take_profit_pct"]), 2)
        sl_price    = round(limit_price * (1 - cfg["stop_loss_pct"]), 2)
        # [#3] mode 1 (trend following) → GTC; minden más (mode 0, 2) → DAY
        tif = "gtc" if cfg["mode"] == 1 else "day"
        return OrderIntent(
            symbol=symbol,
            side="buy",
            qty=qty,
            entry_type="bracket",
            limit_price=limit_price,
            tp_price=tp_price,
            sl_price=sl_price,
            tif=tif,
            strategy=cfg["strategy"],
            signal_id=signal.signal_id,
            order_role="entry",
            run_id=self.run_id,
        )

    # ── Validáció ──────────────────────────────────────────────────────────────

    def validate_intent(self, intent: OrderIntent) -> tuple[bool, str]:
        if intent.qty <= 0:
            return False, "qty<=0"
        if intent.limit_price <= self.MIN_PRICE:
            return False, "limit_price<=min"
        if intent.sl_price <= self.MIN_PRICE:
            return False, "sl_price<=min"
        if intent.tp_price <= self.MIN_PRICE:
            return False, "tp_price<=min"
        if not (intent.sl_price < intent.limit_price < intent.tp_price):
            return False, "bracket_order_invalid (SL<Limit<TP sérül)"
        stop_dist = (intent.limit_price - intent.sl_price) / intent.limit_price
        if stop_dist < self.MIN_STOP_DISTANCE_PCT:
            return False, f"stop_too_close ({stop_dist:.4f})"
        notional = intent.qty * intent.limit_price
        if notional <= 0 or notional > 1_000_000:
            return False, f"notional_out_of_range ({notional})"
        return True, "ok"

    # ── Order benyújtás ────────────────────────────────────────────────────────

    def submit_intent(self, intent: OrderIntent) -> OrderRecord:
        if self.state.order_exists(intent.client_order_id):
            raise ExecutionError(f"duplicate intent {intent.client_order_id}")

        ok, reason = self.validate_intent(intent)
        if not ok:
            raise ExecutionError(f"invalid_intent: {reason}")

        record = OrderRecord.from_intent(intent)
        self.state.save_order_intent(record)

        tif = TimeInForce.DAY if intent.tif == "day" else TimeInForce.GTC
        req = LimitOrderRequest(
            symbol=intent.symbol,
            qty=intent.qty,
            side=OrderSide.BUY,
            time_in_force=tif,
            limit_price=intent.limit_price,
            client_order_id=intent.client_order_id,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=intent.tp_price),
            stop_loss=StopLossRequest(stop_price=intent.sl_price),
        )

        try:
            order = self.client.submit_order(req)
        except Exception as exc:
            self.state.mark_order_rejected(intent.client_order_id, str(exc))
            raise ExecutionError(f"broker_reject: {exc}") from exc

        broker_id = str(getattr(order, "id", "") or "") or None
        self.state.mark_order_submitted(intent.client_order_id, broker_id)
        record.broker_order_id = broker_id
        record.state = OrderState.SUBMITTED
        record.updated_at = datetime.now(timezone.utc)

        # [#C] A broker_order válasz tartalmazza a child leg-eket — rögtön perzisztáljuk őket.
        self._persist_child_legs(order, parent_client_order_id=intent.client_order_id)

        return record

    # ── [#C] Child leg helper ──────────────────────────────────────────────────

    def _derive_child_role(self, order) -> str:
        """Meghatározza, hogy egy bracket child order TP vagy SL szerepű-e.

        Az Alpaca az order.order_type mezőn 'limit' (TP) ill. 'stop' / 'stop_limit'
        (SL) értéket ad a child leg-ekre. Ha nem dönthető el egyértelműen, 'child'
        értéket adunk vissza.
        """
        ot = str(getattr(order, "order_type", "") or "").lower()
        return self._CHILD_ROLES.get(ot, "child")

    def _persist_child_legs(self, parent_broker_order, parent_client_order_id: str) -> None:
        """[#C] Bracket order child leg-ek (TP/SL) lokális DB-be mentése.

        Az Alpaca bracket order benyújtás válasza tartalmazza a child leg-eket
        egy `legs` listában. Ezeket teljes entitásként mentjük, hogy:
          - audit trail ne legyen lyukas
          - reconciliation ne importálja őket hamis 'entry' szerepként
          - PnL rekonstrukció a záró fill-t a helyes szerephez tudja kötni

        A child leg client_order_id-ja az Alpaca-generált uuid alapú string,
        amelyre a `parent_order_id` visszamutat a parent-re.
        """
        legs = getattr(parent_broker_order, "legs", None) or []
        for leg in legs:
            child_client_oid = str(getattr(leg, "client_order_id", "") or "")
            child_broker_oid = str(getattr(leg, "id", "") or "") or None
            if not child_client_oid:
                # Ha nincs client_order_id, broker_order_id-t használunk generált kulcsként
                child_client_oid = f"child-{child_broker_oid or 'unknown'}"

            role = self._derive_child_role(leg)
            state = OrderState.from_alpaca(getattr(leg, "status", "new"))
            qty   = int(float(getattr(leg, "qty", 0) or 0))
            limit_p = getattr(leg, "limit_price", None)
            stop_p  = getattr(leg, "stop_price", None)

            record = OrderRecord(
                client_order_id=child_client_oid,
                broker_order_id=child_broker_oid,
                signal_id="",                          # nem kötődik közvetlen signalhoz
                symbol=str(getattr(leg, "symbol", "") or ""),
                side=str(getattr(leg, "side", "sell")).lower(),
                qty=qty,
                state=state,
                tif=str(getattr(leg, "time_in_force", "day")).lower(),
                order_role=role,
                parent_client_order_id=parent_client_order_id,
                limit_price=float(limit_p) if limit_p is not None else None,
                sl_price=float(stop_p)  if stop_p  is not None else None,
                submitted_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            self.state.import_broker_order(record)

    # ── [#B] refresh_order_state — delta-alapú fill ────────────────────────────

    def refresh_order_state(self, client_order_id: str) -> OrderState | None:
        """Lekéri az order broker-oldali állapotát és szinkronizálja a lokális DB-be.

        [#B] Delta-alapú fill kezelés:
          Korábban a cumulative filled_qty-t írta fill-ként, ami partial fill
          növekedésnél (pl. 5 → 8) 8-at rögzített új fill eseményként, nem a
          delta 3-at. A fill_id ütközésvédelem miatt ez nem duplikált azonos
          állapotra, de ha a fill_id tartalmaz qty-t (pl. "broker_id:8:12.50"),
          egy korábbi "broker_id:5:12.50" fill_id-val együtt kétszer számolt
          mennyiséget eredményezhet PnL kalkulációban.

          Javítás: _refresh_fill_seen[client_order_id] nyomon követi az eddig
          látott cumulative qty-t, és csak a delta (ha pozitív) kerül fillként
          az adatbázisba.
        """
        try:
            order = self.client.get_order_by_client_id(client_order_id)
        except Exception:
            return None

        state         = OrderState.from_alpaca(getattr(order, "status", "unknown"))
        cum_filled    = float(getattr(order, "filled_qty", 0) or 0)
        avg           = getattr(order, "filled_avg_price", None)
        avg_price     = float(avg) if avg else None
        reject_reason = getattr(order, "failed_at", None)
        reject_text   = str(reject_reason) if reject_reason else None
        broker_oid    = str(getattr(order, "id", "") or "") or client_order_id

        # Állapot frissítése (cumulative qty a state táblában marad — az helyes)
        self.state.update_order_state(client_order_id, state, cum_filled, avg_price, reject_text)

        # [#B] Delta számítás
        if cum_filled > 0 and avg_price is not None:
            prev_seen = self._refresh_fill_seen.get(client_order_id, 0.0)
            delta_qty = cum_filled - prev_seen
            if delta_qty > 1e-9:                      # lebegőpontos epsilon
                self._refresh_fill_seen[client_order_id] = cum_filled
                # [#3-fix] Kanonikus fill_id — azonos a main.py trade-update
                # útvonalon generálttal, restart után nem duplikál.
                fill_id = canonical_fill_id(broker_oid, cum_filled, avg_price)
                # [#1-fix] apply_fill() bool-t ad vissza; ha False (már létező
                # rekord), a _refresh_fill_seen-t visszaállítjuk, hogy a delta
                # tracker ne csússzon el valódi jövőbeli fill-ek esetén sem.
                inserted = self.state.apply_fill(
                    fill_id=fill_id,
                    broker_order_id=broker_oid,
                    client_order_id=client_order_id,
                    symbol=getattr(order, "symbol", ""),
                    qty=delta_qty,          # ← csak az új delta
                    price=avg_price,
                )
                if not inserted:
                    # Visszaállítjuk a seen-t az előző értékre: a next poll
                    # ugyanezt a deltát újra megpróbálhatja, de az IGNORE
                    # megvédi a fills táblát. Így nem veszítünk el valódi
                    # jövőbeli partial fill-eket sem.
                    self._refresh_fill_seen[client_order_id] = prev_seen

        # [#C] Child leg-ek frissítése, ha az order broker-válasza tartalmazza őket
        legs = getattr(order, "legs", None) or []
        for leg in legs:
            child_coid = str(getattr(leg, "client_order_id", "") or "")
            if not child_coid:
                child_coid = f"child-{str(getattr(leg, 'id', '') or 'unknown')}"
            child_state  = OrderState.from_alpaca(getattr(leg, "status", "unknown"))
            child_filled = float(getattr(leg, "filled_qty", 0) or 0)
            child_avg    = getattr(leg, "filled_avg_price", None)
            child_avg_f  = float(child_avg) if child_avg else None

            # Állapot szinkron
            self.state.update_order_state(child_coid, child_state, child_filled, child_avg_f)

            # [#B] Delta fill a child leg-re is
            if child_filled > 0 and child_avg_f is not None:
                child_broker_oid = str(getattr(leg, "id", "") or "") or child_coid
                prev_child = self._refresh_fill_seen.get(child_coid, 0.0)
                child_delta = child_filled - prev_child
                if child_delta > 1e-9:
                    self._refresh_fill_seen[child_coid] = child_filled
                    child_fill_id = canonical_fill_id(child_broker_oid, child_filled, child_avg_f)  # [#3-fix]
                    child_inserted = self.state.apply_fill(  # [#1-fix] bool guard
                        fill_id=child_fill_id,
                        broker_order_id=child_broker_oid,
                        client_order_id=child_coid,
                        symbol=getattr(leg, "symbol", ""),
                        qty=child_delta,
                        price=child_avg_f,
                    )
                    if not child_inserted:
                        self._refresh_fill_seen[child_coid] = prev_child

        return state

    # ── [#C] Reconciliation — bracket child felismerés ─────────────────────────

    def reconcile_orders(self) -> None:
        """Broker ↔ lokális DB szinkron, bracket child-order felismeréssel.

        [#C] Korábban a reconciliation minden broker ordert 'entry' szerepként
        importált, parent_client_order_id nélkül. Ez hamis képet adott az
        adatbázisban: a TP/SL child order-ek belső entry-ként szerepeltek,
        audit trail és PnL rekonstrukció szempontból félrevezető volt.

        Javítás: Az Alpaca bracket order-ek `legs` listáját a parent ordereken
        keresztül bejárjuk, és a child-eket helyes order_role ('tp'/'sl') és
        parent_client_order_id értékkel mentjük. A flat loop végén csak azok
        az orderek kerülnek alapértelmezett 'entry' importba, amelyeket
        korábban nem fedtünk fel child-ként.
        """
        broker_open_ids: set[str] = set()
        # child_client_oid → parent_client_oid mapping a flat loop kizárásához
        known_child_coids: set[str] = set()

        try:
            broker_orders = _get_orders_nested(self.client)
        except Exception:
            broker_orders = []

        # 1. pass: szülő orderek + child leg-ek importálása
        for order in broker_orders:
            parent_coid = str(getattr(order, "client_order_id", "") or "")
            if not parent_coid:
                continue

            order_class = str(getattr(order, "order_class", "") or "").lower()
            state = OrderState.from_alpaca(getattr(order, "status", "unknown"))

            # Parent entry order rekord összeállítása
            parent_record = self._build_order_record(
                order,
                client_order_id=parent_coid,
                order_role="entry",
                parent_client_order_id=None,
                state=state,
            )
            self.state.import_broker_order(parent_record)
            if state.is_open:
                broker_open_ids.add(parent_coid)

            # Ha bracket parent → child leg-ek feldolgozása
            if order_class == "bracket":
                legs = getattr(order, "legs", None) or []
                for leg in legs:
                    child_coid = str(getattr(leg, "client_order_id", "") or "")
                    child_broker_oid = str(getattr(leg, "id", "") or "") or None
                    if not child_coid:
                        child_coid = f"child-{child_broker_oid or 'unknown'}"

                    known_child_coids.add(child_coid)
                    role = self._derive_child_role(leg)
                    child_state = OrderState.from_alpaca(getattr(leg, "status", "new"))

                    child_record = self._build_order_record(
                        leg,
                        client_order_id=child_coid,
                        order_role=role,
                        parent_client_order_id=parent_coid,
                        state=child_state,
                    )
                    self.state.import_broker_order(child_record)
                    if child_state.is_open:
                        broker_open_ids.add(child_coid)

        # 2. pass: lokálisan nyitott orderek állapot-frissítése (delta-fill logikával)
        for row in self.state.get_open_orders():
            self.refresh_order_state(row["client_order_id"])

        self.state.mark_missing_open_orders_closed(broker_open_ids)

    def _build_order_record(
        self,
        order,
        *,
        client_order_id: str,
        order_role: str,
        parent_client_order_id: str | None,
        state: OrderState,
    ) -> OrderRecord:
        """Alpaca order objektumból OrderRecord épít; újrafelhasználható helper."""
        limit_p = getattr(order, "limit_price", None)
        stop_p  = getattr(order, "stop_price", None)
        avg_p   = getattr(order, "filled_avg_price", None)
        return OrderRecord(
            client_order_id=client_order_id,
            broker_order_id=str(getattr(order, "id", "") or "") or None,
            signal_id="",
            symbol=str(getattr(order, "symbol", "") or ""),
            side=str(getattr(order, "side", "buy")).lower(),
            qty=int(float(getattr(order, "qty", 0) or 0)),
            filled_qty=float(getattr(order, "filled_qty", 0) or 0),
            avg_fill_price=float(avg_p) if avg_p is not None else None,
            limit_price=float(limit_p) if limit_p is not None else None,
            sl_price=float(stop_p) if stop_p is not None else None,
            tif=str(getattr(order, "time_in_force", "day")).lower(),
            strategy=None,
            order_role=order_role,
            parent_client_order_id=parent_client_order_id,
            state=state,
            submitted_at=None,
            updated_at=datetime.now(timezone.utc),
        )

    # ── Pozíció zárás ──────────────────────────────────────────────────────────

    def close_all_positions(self, reason: str = "") -> None:
        self.client.close_all_positions(cancel_orders=True)

    def close_intraday_positions(self, intraday_symbols, reason: str = "") -> None:
        """[#4] Cancel nyitott intraday orderek → close pozíciók."""
        intraday_set = set(intraday_symbols)
        _OPEN_STATUSES = OPEN_ALPACA_STATUSES  # centrális definíció, models.py

        # 1. lépés: nyitott intraday orderek törlése
        try:
            for order in _get_orders_nested(self.client):
                sym    = str(getattr(order, "symbol", "") or "")
                status = normalize_order_status(getattr(order, "status", ""))  # [#2-fix]
                if sym in intraday_set and status in _OPEN_STATUSES:
                    order_id = str(getattr(order, "id", "") or "")
                    if order_id:
                        try:
                            self.client.cancel_order_by_id(order_id)
                        except Exception as cancel_exc:
                            self.logger.warning(
                                "cancel_order_error sym=%s id=%s reason=%s err=%s",
                                sym, order_id, reason, cancel_exc,
                            )
        except Exception as exc:
            self.logger.warning("cancel_intraday_orders_error reason=%s err=%s", reason, exc)

        # 2. lépés: nyitott intraday pozíciók zárása
        try:
            all_positions = self.client.get_all_positions()
        except Exception:
            all_positions = []

        for p in all_positions:
            if p.symbol in intraday_set:
                try:
                    self.client.close_position(p.symbol)
                except Exception as close_exc:
                    self.logger.warning(
                        "close_position_error sym=%s reason=%s err=%s",
                        p.symbol, reason, close_exc,
                    )

    def close_symbol(self, symbol: str, reason: str = "") -> None:
        self.client.close_position(symbol)
