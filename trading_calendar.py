from __future__ import annotations

"""
trading_calendar.py — US keleti piaci nap segédeszközök
========================================================

Probléma: `date.today()` a gép lokális (pl. CET/CEST) dátumát adja, ami
a New York-i kereskedési nappal akár -1 napot is eltérhet (pl. 17:00 CET
= 11:00 ET = még ugyanaz a tőzsdei nap; 00:30 CET = 18:30 ET előző nap).

Megoldás: minden "kereskedési nap" hivatkozás az
`us_trade_date()` függvényen keresztül az America/New_York zónához
kötött dátumot adja vissza. A pytz/zoneinfo csomag nem feltétlenül
elérhető a célrendszeren, ezért UTC offset-alapú approximációt is
biztosítunk tartalékként, a pontos offset az UTC-hez képest
konfigurálható (.env: MARKET_UTC_OFFSET, alapértelmezett: -4, azaz EDT).

Ha az `alpaca-py` TradingClient elérhető, a `trading_date_from_clock()`
közvetlenül az Alpaca clock API `timestamp` mezőjét használja — ez a
leghitelesebb forrás, mert az Alpaca saját naptárán alapul.

Importálás:
    from modules.trading_calendar import us_trade_date, trading_date_from_clock
"""

import os
from datetime import date, datetime, timedelta, timezone

# Konfigurálható UTC offset (fallback, ha a zoneinfo nem érhető el)
# EDT = -4, EST = -5; a legtöbb kereskedési időszakban EDT (-4)
_MARKET_UTC_OFFSET_HOURS: int = int(os.getenv("MARKET_UTC_OFFSET", "-4"))

# Megpróbáljuk a zoneinfo / pytz csomagot betölteni a pontos DST kezeléshez
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    _TZ_EASTERN = ZoneInfo("America/New_York")
    _HAS_ZONEINFO = True
except Exception:
    _TZ_EASTERN = None  # type: ignore[assignment]
    _HAS_ZONEINFO = False

try:
    import pytz as _pytz  # type: ignore[import]
    _TZ_PYTZ = _pytz.timezone("America/New_York")
    _HAS_PYTZ = True
except Exception:
    _TZ_PYTZ = None  # type: ignore[assignment]
    _HAS_PYTZ = False


def _now_eastern() -> datetime:
    """Visszaadja az aktuális időt US/Eastern zónában.

    Prioritás: zoneinfo → pytz → UTC + statikus offset (fallback).
    """
    utc_now = datetime.now(timezone.utc)

    if _HAS_ZONEINFO and _TZ_EASTERN is not None:
        return utc_now.astimezone(_TZ_EASTERN)

    if _HAS_PYTZ and _TZ_PYTZ is not None:
        return utc_now.astimezone(_TZ_PYTZ)

    # Fallback: statikus UTC offset
    offset = timezone(timedelta(hours=_MARKET_UTC_OFFSET_HOURS))
    return utc_now.astimezone(offset)


def us_trade_date(as_of_utc: datetime | None = None) -> str:
    """Visszaadja az aktuális US/Eastern kereskedési dátumot 'YYYY-MM-DD' formában.

    Args:
        as_of_utc: Ha megadjuk, ehhez az UTC időponthoz számítja a dátumot
                   (pl. fill timestamp). Ha None, az aktuális UTC időt használja.

    Returns:
        str: 'YYYY-MM-DD' formátumú kereskedési dátum.

    Példa (CET nyárban = UTC+2):
        UTC 22:00 → ET 18:00 → kereskedési nap = aznap
        UTC 23:30 → ET 19:30 → kereskedési nap = aznap (piac már zárva)
        UTC 00:30 → ET 20:30 → kereskedési nap = előző nap (!)
    """
    if as_of_utc is not None:
        # Biztosítjuk, hogy timezone-aware legyen
        if as_of_utc.tzinfo is None:
            as_of_utc = as_of_utc.replace(tzinfo=timezone.utc)
        utc_ts = as_of_utc
    else:
        utc_ts = datetime.now(timezone.utc)

    if _HAS_ZONEINFO and _TZ_EASTERN is not None:
        eastern = utc_ts.astimezone(_TZ_EASTERN)
        return str(eastern.date())

    if _HAS_PYTZ and _TZ_PYTZ is not None:
        eastern = utc_ts.astimezone(_TZ_PYTZ)
        return str(eastern.date())

    # Fallback: statikus offset
    offset = timezone(timedelta(hours=_MARKET_UTC_OFFSET_HOURS))
    eastern = utc_ts.astimezone(offset)
    return str(eastern.date())


def trading_date_from_clock(clock) -> str:
    """Alpaca Clock objektumból nyeri ki a kereskedési dátumot.

    Az Alpaca `get_clock()` válasza tartalmaz egy `timestamp` mezőt, amely
    az Alpaca saját naptárán alapul — ez az elsődleges, legmegbízhatóbb
    forrás az aktuális kereskedési napra.

    Ha a `timestamp` mező nem érhető el (pl. régi SDK verzió), visszaesik
    az `us_trade_date()` fallbackre.

    Args:
        clock: Alpaca TradingClient.get_clock() visszatérési értéke.

    Returns:
        str: 'YYYY-MM-DD' formátumú kereskedési dátum.
    """
    ts = getattr(clock, "timestamp", None)
    if ts is not None:
        # Az Alpaca clock.timestamp timezone-aware datetime (US/Eastern)
        if isinstance(ts, datetime):
            return str(ts.date())
        # String esetén az első 10 karakter a dátum
        return str(ts)[:10]

    # Fallback: saját keleti idő alapú számítás
    return us_trade_date()


def today_et() -> date:
    """Visszaadja az aktuális US/Eastern dátumot `date` objektumként."""
    return date.fromisoformat(us_trade_date())
