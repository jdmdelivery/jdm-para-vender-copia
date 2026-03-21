# -*- coding: utf-8 -*-
"""
Zona horaria de negocio: República Dominicana (America/Santo_Domingo).

- Mostrar siempre en hora local RD.
- Persistir en BD como UTC naive (TIMESTAMP sin TZ), interpretando naive como UTC al mostrar.
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

ZONE_RD = ZoneInfo("America/Santo_Domingo")
FORMAT_RD_DATETIME = "%d/%m/%Y %I:%M %p"


def get_current_time_rd() -> datetime:
    """Fecha y hora actual en Santo Domingo (datetime con tzinfo)."""
    return datetime.now(ZONE_RD)


def today_rd() -> date:
    """Fecha calendario actual en RD (para «hoy» del negocio)."""
    return get_current_time_rd().date()


def utc_now_for_db() -> datetime:
    """
    Marca de tiempo UTC **naive** para columnas DateTime sin zona en PostgreSQL.
    Equivalente a guardar UTC y documentar que es UTC.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def as_utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_santo_domingo(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return as_utc_aware(dt).astimezone(ZONE_RD)


def format_dt_rd(
    dt: datetime | date | None,
    fmt: str = FORMAT_RD_DATETIME,
    *,
    naive_storage_is_utc: bool = True,
) -> str:
    """
    Formatea para UI. Si `dt` es naive datetime, se asume UTC (datos de BD).
    Las `date` puras se formatean sin conversión de zona.
    """
    if dt is None:
        return "—"
    if isinstance(dt, date) and not isinstance(dt, datetime):
        return dt.strftime("%d/%m/%Y")
    if not isinstance(dt, datetime):
        return str(dt)
    if dt.tzinfo is None and naive_storage_is_utc:
        dt = dt.replace(tzinfo=timezone.utc)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZONE_RD)
    return dt.astimezone(ZONE_RD).strftime(fmt)


def format_payment_receipt_when(p: dict) -> str:
    """Texto de fecha/hora para recibos de pago (impresión y pantalla)."""
    ts = p.get("created_at")
    if ts is not None and isinstance(ts, datetime):
        return format_dt_rd(ts) + " RD"
    d = p.get("date")
    if isinstance(d, datetime):
        return format_dt_rd(d) + " RD"
    if isinstance(d, date):
        return d.strftime("%d/%m/%Y") + " · RD"
    return str(d or "—") + " RD"


def combine_date_at_rd_midnight(d: date) -> datetime:
    """Útil si se necesita un datetime RD a medianoche para un día calendario."""
    return datetime.combine(d, time(0, 0), tzinfo=ZONE_RD)
