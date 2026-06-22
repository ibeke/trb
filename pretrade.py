from __future__ import annotations


class PreTradePolicy:
    """Belépési policy; executiontől függetlenül dönti el a belépés tiltását."""

    LOCAL_PENDING_TTL_SEC = 120

    def __init__(self, cache, state_manager):
        self.cache = cache
        self.state = state_manager

    def has_open_position(self, symbol: str) -> bool:
        return any(p.symbol == symbol for p in self.cache.get_positions())

    def has_open_order(self, symbol: str) -> bool:
        broker_open = [o for o in self.cache.get_open_orders() if o.symbol == symbol]
        if broker_open:
            return True
        recent_local = self.state.get_recent_pending_orders(
            symbol,
            max_age_sec=self.LOCAL_PENDING_TTL_SEC,
        )
        return len(recent_local) > 0

    def can_enter(self, symbol: str) -> tuple[bool, str]:
        if self.has_open_position(symbol):
            return False, "position_already_open"
        if self.has_open_order(symbol):
            return False, "order_already_open"
        return True, "ok"
