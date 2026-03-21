# -*- coding: utf-8 -*-
"""
Capa de repositorio: acceso a datos compatible con la lógica actual.
Retorna dicts para minimizar cambios en app.py.
"""
from datetime import datetime, date
from decimal import Decimal

from rd_time import today_rd

from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session

from .models import (
    Admin, Usuario, Cliente, Prestamo, Banco, Pago, Gasto, Atraso,
    PagoAdmin, Auditoria, Cierre,
)
from .database import get_session, session_scope


def _row_to_dict(row, model_class):
    if row is None:
        return None
    if hasattr(row, "to_dict"):
        return row.to_dict()
    return dict(row._mapping)


# --- Admins ---
def get_admin(session: Session, admin_id: int) -> dict | None:
    a = session.get(Admin, admin_id)
    return a.to_dict() if a else None


def get_admins_all(session: Session) -> list:
    rows = session.execute(select(Admin)).scalars().all()
    return [r.to_dict() for r in rows]


# --- Usuarios ---
def get_usuario(session: Session, user_id: int) -> dict | None:
    u = session.get(Usuario, user_id)
    return u.to_dict() if u else None


def get_usuario_by_username(session: Session, username: str) -> dict | None:
    u = session.execute(select(Usuario).where(Usuario.username == username)).scalar_one_or_none()
    return u.to_dict() if u else None


def get_usuarios_dict(session: Session) -> dict:
    """Retorna {id: dict} como store.users. Admins también por admin_id (tenant lookup)."""
    rows = session.execute(select(Usuario)).scalars().all()
    d = {r.id: r.to_dict() for r in rows}
    for r in rows:
        if r.role == "admin" and r.admin_id:
            d[r.admin_id] = r.to_dict()  # store.users.get(tenant_id)
    return d


def get_usuarios_by_admin(session: Session, admin_id: int) -> list:
    rows = session.execute(select(Usuario).where(Usuario.admin_id == admin_id)).scalars().all()
    return [r.to_dict() for r in rows]


def save_usuario(session: Session, data: dict) -> int:
    u = Usuario(
        admin_id=data.get("admin_id") or data.get("organization_id"),
        username=data["username"],
        password_hash=data["password_hash"],
        role=data["role"],
        name=data.get("name"),
        email=data.get("email"),
        phone=data.get("phone") or "",
        account_status=data.get("account_status", "activo"),
        fecha_inicio=data.get("fecha_inicio"),
        fecha_fin=data.get("fecha_fin") or data.get("subscription_end"),
        subscription_end=data.get("subscription_end"),
    )
    session.add(u)
    session.flush()
    return u.id


def update_usuario(session: Session, user_id: int, data: dict):
    u = session.get(Usuario, user_id)
    if not u:
        return
    for k, v in data.items():
        if hasattr(u, k):
            setattr(u, k, v)


# --- Clientes ---
def get_cliente(session: Session, client_id: int) -> dict | None:
    c = session.get(Cliente, client_id)
    return c.to_dict() if c else None


def get_clientes_by_admin(session: Session, admin_id: int) -> list:
    rows = session.execute(select(Cliente).where(Cliente.admin_id == admin_id)).scalars().all()
    return [r.to_dict() for r in rows]


def get_clientes_dict(session: Session, admin_ids: set | list | None = None) -> dict:
    """Retorna {id: dict}. Si admin_ids, filtra por esos tenants."""
    q = select(Cliente)
    if admin_ids is not None:
        q = q.where(Cliente.admin_id.in_(admin_ids))
    rows = session.execute(q).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def save_cliente(session: Session, data: dict) -> int:
    c = Cliente(
        admin_id=data["admin_id"],
        first_name=data["first_name"],
        last_name=data.get("last_name") or "",
        document_id=data.get("document_id") or "",
        phone=data.get("phone") or "",
        address=data.get("address") or "",
        route=data.get("route") or "",
        created_by=data.get("created_by"),
    )
    session.add(c)
    session.flush()
    return c.id


def update_cliente(session: Session, client_id: int, data: dict):
    c = session.get(Cliente, client_id)
    if not c:
        return
    for k, v in data.items():
        if hasattr(c, k):
            setattr(c, k, v)


def delete_cliente(session: Session, client_id: int):
    session.execute(Cliente.__table__.delete().where(Cliente.id == client_id))


# --- Préstamos ---
def get_prestamo(session: Session, loan_id: int) -> dict | None:
    p = session.get(Prestamo, loan_id)
    return p.to_dict() if p else None


def get_prestamos_by_admin(session: Session, admin_id: int) -> list:
    rows = session.execute(select(Prestamo).where(Prestamo.admin_id == admin_id)).scalars().all()
    return [r.to_dict() for r in rows]


def get_prestamos_dict(session: Session, admin_ids: set | list | None = None) -> dict:
    """Retorna {id: dict} como store.loans."""
    q = select(Prestamo)
    if admin_ids is not None:
        q = q.where(Prestamo.admin_id.in_(admin_ids))
    rows = session.execute(q).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def save_prestamo(session: Session, data: dict) -> int:
    p = Prestamo(
        admin_id=data["admin_id"],
        client_id=data["client_id"],
        amount=data["amount"],
        rate=data.get("rate", 0),
        frequency=data.get("frequency", "semanal"),
        start_date=data["start_date"],
        next_payment_date=data.get("next_payment_date"),
        term_count=data.get("term_count", 1),
        remaining=data.get("remaining", data["amount"]),
        remaining_capital=data.get("remaining_capital", data["amount"]),
        total_interest=data.get("total_interest", 0),
        total_to_pay=data["total_to_pay"],
        upfront_percent=data.get("upfront_percent", 0),
        installment_amount=data["installment_amount"],
        total_interest_paid=data.get("total_interest_paid", 0),
        status=data.get("status", "ACTIVO"),
        created_by=data.get("created_by"),
        discount_banco_id=data.get("discount_cash_report_id") or data.get("discount_banco_id"),
        disbursement_banco_id=data.get("disbursement_cash_report_id") or data.get("disbursement_banco_id"),
    )
    session.add(p)
    session.flush()
    return p.id


def update_prestamo(session: Session, loan_id: int, data: dict):
    p = session.get(Prestamo, loan_id)
    if not p:
        return
    for k, v in data.items():
        if k in ("discount_cash_report_id", "disbursement_cash_report_id"):
            k = "discount_banco_id" if k == "discount_cash_report_id" else "disbursement_banco_id"
        if hasattr(p, k):
            setattr(p, k, v)


def delete_prestamo(session: Session, loan_id: int):
    session.execute(Prestamo.__table__.delete().where(Prestamo.id == loan_id))


# --- Banco (ledger) ---
def get_banco_sum(session: Session, admin_id: int) -> float:
    r = session.execute(
        select(func.coalesce(func.sum(Banco.amount), 0)).where(Banco.admin_id == admin_id)
    ).scalar()
    return float(r or 0)


def get_banco_movements_dict(session: Session, admin_id: int) -> dict:
    rows = session.execute(select(Banco).where(Banco.admin_id == admin_id)).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def get_starting_bank(session: Session, admin_id: int) -> float:
    a = session.get(Admin, admin_id)
    return float(a.starting_bank) if a else 0.0


def add_banco_movement(session: Session, data: dict) -> int:
    b = Banco(
        admin_id=data["admin_id"],
        user_id=data.get("user_id"),
        collector_id=data.get("collector_id"),
        amount=data["amount"],
        movement_type=data["movement_type"],
        note=data.get("note"),
        mov_date=data.get("date") or data.get("mov_date") or today_rd(),
    )
    session.add(b)
    session.flush()
    return b.id


def delete_banco_movement(session: Session, mov_id: int):
    session.execute(Banco.__table__.delete().where(Banco.id == mov_id))


# --- Pagos ---
def get_pagos_by_loan(session: Session, loan_id: int) -> list:
    rows = session.execute(select(Pago).where(Pago.loan_id == loan_id)).scalars().all()
    return [r.to_dict() for r in rows]


def get_pagos_dict(session: Session, admin_id: int | None = None) -> dict:
    q = select(Pago)
    if admin_id is not None:
        q = q.where(Pago.admin_id == admin_id)
    rows = session.execute(q).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def save_pago(session: Session, data: dict) -> int:
    p = Pago(
        admin_id=data["admin_id"],
        loan_id=data["loan_id"],
        amount=data["amount"],
        type=data.get("type", "cuota"),
        pago_date=data.get("date") or data.get("pago_date") or today_rd(),
        capital=data.get("capital", 0),
        interest=data.get("interest", 0),
        status=data.get("status", "OK"),
        weeks_advanced=data.get("weeks_advanced"),
        created_by=data.get("created_by"),
    )
    session.add(p)
    session.flush()
    return p.id


def delete_pago(session: Session, payment_id: int):
    session.execute(Pago.__table__.delete().where(Pago.id == payment_id))


# --- Gastos ---
def get_gastos_dict(session: Session, admin_id: int) -> dict:
    rows = session.execute(select(Gasto).where(Gasto.admin_id == admin_id)).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def save_gasto(session: Session, data: dict) -> int:
    g = Gasto(
        admin_id=data["admin_id"],
        route=data.get("route", ""),
        kind=data.get("kind", "otros"),
        expense_type=data.get("expense_type", "gasto_ruta"),
        amount=data["amount"],
        note=data.get("note", "—"),
        user_id=data.get("user_id"),
        cash_report_id=data.get("cash_report_id"),
    )
    session.add(g)
    session.flush()
    return g.id


# --- Atrasos ---
def get_atrasos_dict(session: Session, admin_id: int) -> dict:
    rows = session.execute(select(Atraso).where(Atraso.admin_id == admin_id)).scalars().all()
    return {r.id: r.to_dict() for r in rows}


# --- Pagos Admin ---
def get_pagos_admin_list(session: Session, admin_ids: set | None = None) -> list:
    q = select(PagoAdmin)
    if admin_ids is not None:
        q = q.where(PagoAdmin.admin_id.in_(admin_ids))
    rows = session.execute(q).scalars().all()
    return [r.to_dict() for r in rows]


def save_pago_admin(session: Session, data: dict) -> int:
    pa = PagoAdmin(
        admin_id=data["admin_id"],
        amount=data["amount"],
        payment_date=data["payment_date"],
        method=data.get("method", ""),
    )
    session.add(pa)
    session.flush()
    return pa.id


# --- Auditoría ---
def save_auditoria(session: Session, data: dict) -> int:
    a = Auditoria(
        user_id=data.get("user_id"),
        admin_id=data.get("admin_id"),
        user_name=data.get("user_name"),
        role=data.get("role"),
        raw_role=data.get("raw_role"),
        action=data["action"],
        module=data.get("module", ""),
        detail=data.get("detail", ""),
        ip=data.get("ip"),
        device=data.get("device"),
    )
    session.add(a)
    session.flush()
    return a.id


# --- Cierres ---
def get_cierres_list(session: Session, admin_id: int) -> list:
    rows = session.execute(
        select(Cierre).where(Cierre.admin_id == admin_id).order_by(Cierre.closed_at.desc()).limit(50)
    ).scalars().all()
    return [r.to_dict() for r in rows]


def save_cierre(session: Session, data: dict) -> int:
    c = Cierre(
        admin_id=data["admin_id"],
        user_id=data.get("user_id"),
        notas=data.get("notas", ""),
        cobrado_hoy_snapshot=data.get("cobrado_hoy_snapshot", 0),
        total_por_cobrar_snapshot=data.get("total_por_cobrar_snapshot", 0),
        n_activos=data.get("n_activos", 0),
    )
    session.add(c)
    session.flush()
    return c.id


def delete_cierre(session: Session, cierre_id: int, admin_id: int):
    session.execute(Cierre.__table__.delete().where(
        and_(Cierre.id == cierre_id, Cierre.admin_id == admin_id)
    ))


# --- Deposit history (usamos banco con movement_type=deposito_banco) ---
def get_deposits_list(session: Session, admin_id: int) -> list:
    rows = session.execute(
        select(Banco).where(
            and_(Banco.admin_id == admin_id, Banco.movement_type == "deposito_banco")
        ).order_by(Banco.created_at.desc()).limit(200)
    ).scalars().all()
    return [
        {
            "id": r.id,
            "organization_id": r.admin_id,
            "user_id": r.user_id,
            "amount": float(r.amount),
            "note": r.note,
            "at": r.created_at,
        }
        for r in rows
    ]
