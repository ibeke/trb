from __future__ import annotations

"""
config_validator.py — stcks.json validáció
============================================

Indítás előtt hívja meg a TradingBot.startup(), így hibás konfiguráció
esetén a bot nem indul el, és nem keletkeznek éles hibás orderek.

Használat:
    from modules.config_validator import validate_config
    validate_config(cfg)           # kivételt dob, ha valami hibás
    validate_config(cfg, strict=False)  # csak figyelmeztetések, nem áll meg

A validate_config() három szintű ellenőrzést végez:
  1. Séma-ellenőrzés   — kötelező mezők, típusok, értékkészletek
  2. Kereszt-ellenőrzés — mezők közötti logikai összefüggések
  3. Broker-ellenőrzés  — csak ha trading_client megadva (opcionális):
       kereskedhető-e a szimbólum, van-e elég buying power

A ConfigError kivétel az összes hibát összegyűjti, nem áll meg az
elsőnél — így egyszerre látható minden javítandó paraméter.
"""

import math
from typing import Any


# ── Kivétel ────────────────────────────────────────────────────────────────────

class ConfigError(ValueError):
    """Konfigurációs validációs hiba.

    Az összes hibát összegyűjti és egyszerre jeleníti meg,
    hogy az összes sérült paramétert egyszerre lehessen javítani.
    """

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__(
            f"{len(errors)} konfigurációs hiba:\n" +
            "\n".join(f"  [{i+1}] {e}" for i, e in enumerate(errors))
        )


# ── Mode → stratégia megfeleltetés ────────────────────────────────────────────

_MODE_STRATEGY: dict[int, str] = {
    0: "mean_reversion",
    1: "trend_following",
    2: "breakout",
}

# Minden mode-hoz kötelező paraméterek
_REQUIRED_BY_MODE: dict[int, list[str]] = {
    0: ["bb_period", "bb_std", "atr_period"],
    1: ["ema_fast", "ema_slow", "adx_period", "adx_threshold"],
    2: ["breakout_period", "volume_ma_period", "volume_factor"],
}

# Közös kötelező paraméterek minden mode-ban
_REQUIRED_COMMON = [
    "mode", "strategy",
    "stop_loss_pct", "take_profit_pct",
    "max_risk_per_trade", "limit_offset_pct",
    "allocation_usd",
]


# ── Segédfüggvények ────────────────────────────────────────────────────────────

def _is_positive_int(v: Any) -> bool:
    return isinstance(v, int) and v > 0


def _is_positive_float(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0


def _is_positive_pct(v: Any) -> bool:
    """0 < v < 1 tartomány (százalék mint tört)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool) and 0 < v < 1


# ── Fő validátor ──────────────────────────────────────────────────────────────

def validate_config(
    cfg: dict,
    *,
    strict: bool = True,
    trading_client=None,
) -> list[str]:
    """stcks.json tartalom validálása.

    Args:
        cfg:            load_config() által visszaadott dict.
        strict:         True esetén ConfigError-t dob hiba esetén.
                        False esetén csak a hibákat adja vissza listában
                        (figyelmeztetés mód — pl. teszteléskor hasznos).
        trading_client: Ha megadva (Alpaca TradingClient), broker-szintű
                        ellenőrzéseket is elvégez (szimbólum elérhetőség,
                        buying power). Indítási overhead miatt opcionális.

    Returns:
        list[str]: Hibaüzenetek listája (üres = minden rendben).

    Raises:
        ConfigError: Ha strict=True és van legalább egy hiba.
    """
    errors: list[str] = []

    if not cfg:
        errors.append("A konfiguráció üres — nincs szimbólum definiálva.")
        if strict:
            raise ConfigError(errors)
        return errors

    for sym, sym_cfg in cfg.items():
        _validate_symbol(sym, sym_cfg, errors)

    if trading_client is not None:
        _validate_broker(cfg, trading_client, errors)

    if strict and errors:
        raise ConfigError(errors)
    return errors


# ── Szimbólum szintű ellenőrzések ─────────────────────────────────────────────

def _validate_symbol(sym: str, c: dict, errors: list[str]) -> None:
    """Egyetlen szimbólum konfigurációjának teljes ellenőrzése."""
    pfx = f"{sym}"   # hibaüzenet prefixhez

    # ── 1. Mode ────────────────────────────────────────────────────────────────
    mode = c.get("mode")
    if mode not in (0, 1, 2):
        errors.append(
            f"{pfx}: mode={mode!r} érvénytelen — csak 0, 1, 2 megengedett."
        )
        mode = None   # többi ellenőrzés kihagyja a mode-függő részeket

    # ── 2. Strategy ────────────────────────────────────────────────────────────
    strategy = c.get("strategy", "")
    if mode is not None:
        expected = _MODE_STRATEGY[mode]
        if strategy != expected:
            errors.append(
                f"{pfx}: strategy={strategy!r} nem illeszkedik mode={mode}-hoz "
                f"(elvárt: '{expected}')."
            )

    # ── 3. Közös kötelező mezők meglét ─────────────────────────────────────────
    for key in _REQUIRED_COMMON:
        if key not in c:
            errors.append(f"{pfx}: hiányzó kötelező mező '{key}'.")

    # ── 4. Mode-specifikus kötelező mezők ──────────────────────────────────────
    if mode is not None:
        for key in _REQUIRED_BY_MODE.get(mode, []):
            if key not in c:
                errors.append(
                    f"{pfx}: mode={mode} esetén kötelező '{key}' mező hiányzik."
                )

    # ── 5. stop_loss_pct ───────────────────────────────────────────────────────
    sl = c.get("stop_loss_pct")
    if sl is not None:
        if not _is_positive_pct(sl):
            errors.append(
                f"{pfx}: stop_loss_pct={sl} érvénytelen — 0 < érték < 1 szükséges."
            )

    # ── 6. take_profit_pct > stop_loss_pct ────────────────────────────────────
    tp = c.get("take_profit_pct")
    if tp is not None:
        if not _is_positive_pct(tp):
            errors.append(
                f"{pfx}: take_profit_pct={tp} érvénytelen — 0 < érték < 1 szükséges."
            )
        elif sl is not None and isinstance(sl, (int, float)) and tp <= sl:
            errors.append(
                f"{pfx}: take_profit_pct={tp} ≤ stop_loss_pct={sl} — "
                f"a célár kisebbel van, mint a stop, a bracket order sohasem zárhat nyereséggel."
            )

    # ── 7. max_risk_per_trade ─────────────────────────────────────────────────
    mr = c.get("max_risk_per_trade")
    if mr is not None:
        if not (isinstance(mr, (int, float)) and not isinstance(mr, bool) and 0 < mr <= 0.5):
            errors.append(
                f"{pfx}: max_risk_per_trade={mr} érvénytelen — (0, 0.5] tartomány szükséges."
            )

    # ── 8. allocation_usd ─────────────────────────────────────────────────────
    alloc = c.get("allocation_usd")
    if alloc is not None:
        if not _is_positive_float(alloc):
            errors.append(
                f"{pfx}: allocation_usd={alloc} érvénytelen — pozitív szám szükséges."
            )

    # ── 9. limit_offset_pct ───────────────────────────────────────────────────
    off = c.get("limit_offset_pct")
    if off is not None:
        if not (isinstance(off, (int, float)) and not isinstance(off, bool) and 0 <= off < 0.1):
            errors.append(
                f"{pfx}: limit_offset_pct={off} érvénytelen — [0, 0.1) tartomány szükséges."
            )

    # ── 10. Mode 0 specifikus ─────────────────────────────────────────────────
    if mode == 0:
        bb_period = c.get("bb_period")
        if bb_period is not None and not _is_positive_int(bb_period):
            errors.append(f"{pfx}: bb_period={bb_period} nem pozitív egész.")

        bb_std = c.get("bb_std")
        if bb_std is not None:
            if not (isinstance(bb_std, (int, float)) and not isinstance(bb_std, bool) and bb_std > 0):
                errors.append(f"{pfx}: bb_std={bb_std} nem pozitív szám.")

        atr_period = c.get("atr_period")
        if atr_period is not None and not _is_positive_int(atr_period):
            errors.append(f"{pfx}: atr_period={atr_period} nem pozitív egész.")

    # ── 11. Mode 1 specifikus ─────────────────────────────────────────────────
    if mode == 1:
        ema_fast = c.get("ema_fast")
        ema_slow = c.get("ema_slow")
        adx_p    = c.get("adx_period")
        adx_th   = c.get("adx_threshold")

        if ema_fast is not None and not _is_positive_int(ema_fast):
            errors.append(f"{pfx}: ema_fast={ema_fast} nem pozitív egész.")
        if ema_slow is not None and not _is_positive_int(ema_slow):
            errors.append(f"{pfx}: ema_slow={ema_slow} nem pozitív egész.")

        # ema_fast < ema_slow feltétel
        if (ema_fast is not None and ema_slow is not None
                and isinstance(ema_fast, int) and isinstance(ema_slow, int)):
            if ema_fast >= ema_slow:
                errors.append(
                    f"{pfx}: ema_fast={ema_fast} ≥ ema_slow={ema_slow} — "
                    f"a gyors EMA-nak kisebb periódusúnak kell lennie."
                )

        if adx_p is not None and not _is_positive_int(adx_p):
            errors.append(f"{pfx}: adx_period={adx_p} nem pozitív egész.")
        if adx_th is not None:
            if not (isinstance(adx_th, (int, float)) and not isinstance(adx_th, bool) and adx_th > 0):
                errors.append(f"{pfx}: adx_threshold={adx_th} nem pozitív szám.")

    # ── 12. Mode 2 specifikus ─────────────────────────────────────────────────
    if mode == 2:
        bp  = c.get("breakout_period")
        vmp = c.get("volume_ma_period")
        vf  = c.get("volume_factor")

        if bp is not None and not _is_positive_int(bp):
            errors.append(f"{pfx}: breakout_period={bp} nem pozitív egész.")
        if vmp is not None and not _is_positive_int(vmp):
            errors.append(f"{pfx}: volume_ma_period={vmp} nem pozitív egész.")
        if vf is not None:
            if not (isinstance(vf, (int, float)) and not isinstance(vf, bool) and vf >= 1.0):
                errors.append(
                    f"{pfx}: volume_factor={vf} érvénytelen — legalább 1.0 szükséges "
                    f"(értéke a volume_ma szorzója)."
                )


# ── Broker szintű ellenőrzések (opcionális) ────────────────────────────────────

def _validate_broker(cfg: dict, trading_client, errors: list[str]) -> None:
    """Alpaca broker-szintű ellenőrzések (opcionális, csak ha trading_client megadva).

    Ellenőrzi:
    - Minden szimbólum kereskedhető-e Alpaca alatt (tradable=True)
    - Minden szimbólum támogatja-e a bracket ordert (fractionable ellenőrzés
      nem szükséges — a bracket order integer qty esetén mindig elérhető)
    - Van-e elegendő buying power az összes allocation_usd összegéhez képest
    """
    # Szimbólum kereskedhetőség
    symbols = list(cfg.keys())
    try:
        assets = {
            a.symbol: a
            for a in trading_client.get_all_assets()
            if hasattr(a, "symbol")
        }
    except Exception as exc:
        errors.append(f"Broker szimbólum ellenőrzés sikertelen: {exc}")
        assets = {}

    for sym in symbols:
        asset = assets.get(sym)
        if asset is None:
            errors.append(
                f"{sym}: az Alpaca nem ismeri ezt a szimbólumot, "
                f"vagy nem elérhető paper/live módban."
            )
            continue
        if not getattr(asset, "tradable", False):
            errors.append(
                f"{sym}: az Alpaca-n a szimbólum tradable=False — "
                f"jelenleg nem kereskedhető (halt, delisted, stb.)."
            )
        # Bracket order: limit order szükséges, fractional assets ezt nem feltétlenül
        # támogatják. Jelezzük, ha fractionable=True de shortable=False együtt fennáll.
        if getattr(asset, "fractionable", False) and not getattr(asset, "shortable", True):
            errors.append(
                f"{sym}: fractionable=True, shortable=False — "
                f"bracket order viselkedése korlátozott lehet ennél a szimbólumnál."
            )

    # Buying power ellenőrzés
    total_alloc = sum(
        c.get("allocation_usd", 0)
        for c in cfg.values()
        if isinstance(c.get("allocation_usd", 0), (int, float))
    )
    if total_alloc > 0:
        try:
            acct = trading_client.get_account()
            bp = float(getattr(acct, "buying_power", 0) or 0)
            if bp < total_alloc:
                errors.append(
                    f"Elégtelen buying power: {bp:.2f} USD elérhető, "
                    f"de az összes allocation_usd összege {total_alloc:.2f} USD. "
                    f"Csökkentse az allocation_usd értékeket, vagy töltsön fel tőkét."
                )
        except Exception as exc:
            errors.append(f"Buying power ellenőrzés sikertelen: {exc}")
