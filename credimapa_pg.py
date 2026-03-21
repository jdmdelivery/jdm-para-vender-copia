# -*- coding: utf-8 -*-
"""
PostgreSQL para CREDIMAPA — un solo archivo (Render/GitHub no pierden carpetas).
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, date
from decimal import Decimal

from flask import session as flask_session
from sqlalchemy import (
    Column, Integer, String, Text, Numeric, Boolean, Date, DateTime,
    ForeignKey, Index, create_engine, select, func, and_,
)
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session

# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------
Base = declarative_base()


def _to_dict(obj, exclude=None):
    if obj is None:
        return None
    exclude = set(exclude or []) | {"_sa_instance_state"}
    d = {}
    for c in obj.__table__.columns:
        if c.name in exclude:
            continue
        v = getattr(obj, c.name)
        if isinstance(v, Decimal):
            v = float(v)
        d[c.name] = v
    return d


class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(180), nullable=True)
    account_status = Column(String(40), nullable=False, default="activo")
    fecha_inicio = Column(DateTime, nullable=True)
    fecha_fin = Column(DateTime, nullable=True)
    subscription_end = Column(DateTime, nullable=True)
    starting_bank = Column(Numeric(14, 2), default=0, nullable=False)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        d = _to_dict(self)
        if d.get("fecha_inicio"):
            d["subscription_start"] = d["fecha_inicio"]
        if d.get("fecha_fin"):
            d["subscription_end"] = d.get("subscription_end") or d["fecha_fin"]
        d["organization_id"] = d["id"]
        d["role"] = "admin"
        return d


class Usuario(Base):
    __tablename__ = "usuarios"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=True)
    username = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(40), nullable=False)
    name = Column(String(180), nullable=True)
    email = Column(String(180), nullable=True)
    phone = Column(String(60), nullable=True, default="")
    account_status = Column(String(40), nullable=False, default="activo")
    fecha_inicio = Column(Date, nullable=True)
    fecha_fin = Column(Date, nullable=True)
    subscription_end = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_usuarios_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d.get("admin_id")
        if d.get("fecha_fin"):
            d["subscription_end"] = d.get("subscription_end") or d["fecha_fin"]
        if d.get("fecha_inicio"):
            d["subscription_start"] = d["fecha_inicio"]
        return d


class Cliente(Base):
    __tablename__ = "clientes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    first_name = Column(String(120), nullable=False)
    last_name = Column(String(120), nullable=True)
    document_id = Column(String(80), nullable=True)
    phone = Column(String(60), nullable=True)
    address = Column(Text, nullable=True)
    route = Column(String(120), nullable=True)
    created_by = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_clientes_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        return d


class Prestamo(Base):
    __tablename__ = "prestamos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    client_id = Column(Integer, ForeignKey("clientes.id", ondelete="RESTRICT"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    rate = Column(Numeric(8, 2), default=0)
    frequency = Column(String(40), default="semanal")
    start_date = Column(Date, nullable=False)
    next_payment_date = Column(Date, nullable=True)
    term_count = Column(Integer, default=1)
    remaining = Column(Numeric(14, 2), nullable=False)
    remaining_capital = Column(Numeric(14, 2), nullable=True)
    total_interest = Column(Numeric(14, 2), default=0)
    total_to_pay = Column(Numeric(14, 2), nullable=False)
    upfront_percent = Column(Numeric(6, 2), default=0)
    installment_amount = Column(Numeric(14, 2), nullable=False)
    total_interest_paid = Column(Numeric(14, 2), default=0)
    status = Column(String(40), default="ACTIVO")
    created_by = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    discount_banco_id = Column(Integer, nullable=True)
    disbursement_banco_id = Column(Integer, nullable=True)
    signature_b64 = Column(Text, nullable=True)
    id_photo_b64 = Column(Text, nullable=True)
    id_photo_back_b64 = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_prestamos_admin_id", "admin_id"), Index("ix_prestamos_client_id", "client_id"))

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        d["discount_cash_report_id"] = d.get("discount_banco_id")
        d["disbursement_cash_report_id"] = d.get("disbursement_banco_id")
        return d


class Banco(Base):
    __tablename__ = "banco"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    collector_id = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    amount = Column(Numeric(14, 2), nullable=False)
    movement_type = Column(String(80), nullable=False)
    note = Column(Text, nullable=True)
    mov_date = Column(Date, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_banco_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["date"] = d.get("mov_date")
        d["organization_id"] = d["admin_id"]
        return d


class Pago(Base):
    __tablename__ = "pagos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    loan_id = Column(Integer, ForeignKey("prestamos.id", ondelete="CASCADE"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    type = Column(String(40), default="cuota")
    pago_date = Column(Date, nullable=False)
    capital = Column(Numeric(14, 2), default=0)
    interest = Column(Numeric(14, 2), default=0)
    status = Column(String(20), default="OK")
    weeks_advanced = Column(Integer, nullable=True)
    created_by = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_pagos_admin_id", "admin_id"), Index("ix_pagos_loan_id", "loan_id"))

    def to_dict(self):
        d = _to_dict(self)
        d["date"] = d.get("pago_date")
        d["organization_id"] = d["admin_id"]
        return d


class Gasto(Base):
    __tablename__ = "gastos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    route = Column(String(120), nullable=True)
    kind = Column(String(60), default="otros")
    expense_type = Column(String(60), default="gasto_ruta")
    amount = Column(Numeric(14, 2), nullable=False)
    note = Column(Text, nullable=True)
    user_id = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    cash_report_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_gastos_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        return d


class Atraso(Base):
    __tablename__ = "atrasos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    loan_id = Column(Integer, ForeignKey("prestamos.id", ondelete="CASCADE"), nullable=False)
    paid = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_atrasos_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        return d


class PagoAdmin(Base):
    __tablename__ = "pagos_admin"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    payment_date = Column(Date, nullable=False)
    method = Column(String(80), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_pagos_admin_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        return d


class Descuento(Base):
    __tablename__ = "descuentos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    loan_id = Column(Integer, ForeignKey("prestamos.id", ondelete="CASCADE"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    banco_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_descuentos_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        return d


class Cierre(Base):
    __tablename__ = "cierres"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    closed_at = Column(DateTime, default=datetime.utcnow)
    notas = Column(Text, nullable=True)
    cobrado_hoy_snapshot = Column(Numeric(14, 2), default=0)
    total_por_cobrar_snapshot = Column(Numeric(14, 2), default=0)
    n_activos = Column(Integer, default=0)
    __table_args__ = (Index("ix_cierres_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        return d


class Auditoria(Base):
    __tablename__ = "auditoria"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    admin_id = Column(Integer, ForeignKey("admins.id"), nullable=True)
    user_name = Column(String(180), nullable=True)
    role = Column(String(60), nullable=True)
    raw_role = Column(String(60), nullable=True)
    action = Column(String(120), nullable=False)
    module = Column(String(80), nullable=True)
    detail = Column(Text, nullable=True)
    ip = Column(String(60), nullable=True)
    device = Column(String(280), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_auditoria_admin_id", "admin_id"), Index("ix_auditoria_user_id", "user_id"))


# ---------------------------------------------------------------------------
# Engine / sesión
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or "postgresql://localhost/jdm_cash"
if str(DATABASE_URL).lower() in ("", "null", "none"):
    DATABASE_URL = "postgresql://localhost/jdm_cash"
if DATABASE_URL and str(DATABASE_URL).startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=bool(os.getenv("SQL_ECHO")),
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_factory = scoped_session(SessionLocal)


def get_session():
    return session_factory()


@contextmanager
def session_scope():
    s = get_session()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def init_db(drop_all=False):
    if drop_all:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def init_app(app):
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        try:
            if exception is None:
                session_factory().commit()
        except Exception:
            session_factory().rollback()
        finally:
            session_factory.remove()


# ---------------------------------------------------------------------------
# Repositorio (funciones)
# ---------------------------------------------------------------------------
def get_prestamo(sess, loan_id: int):
    p = sess.get(Prestamo, loan_id)
    return p.to_dict() if p else None


def delete_pago(sess, payment_id: int):
    sess.execute(Pago.__table__.delete().where(Pago.id == payment_id))


def get_usuario(sess, user_id: int):
    u = sess.get(Usuario, user_id)
    return u.to_dict() if u else None


def get_usuario_by_username(sess, username: str):
    u = sess.execute(select(Usuario).where(Usuario.username == username)).scalar_one_or_none()
    return u.to_dict() if u else None


def get_usuarios_dict(sess):
    rows = sess.execute(select(Usuario)).scalars().all()
    d = {r.id: r.to_dict() for r in rows}
    for r in rows:
        if r.role == "admin" and r.admin_id:
            d[r.admin_id] = r.to_dict()
    return d


def get_clientes_dict(sess, admin_ids=None):
    q = select(Cliente)
    if admin_ids is not None:
        q = q.where(Cliente.admin_id.in_(list(admin_ids)))
    rows = sess.execute(q).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def get_prestamos_dict(sess, admin_ids=None):
    q = select(Prestamo)
    if admin_ids is not None:
        q = q.where(Prestamo.admin_id.in_(list(admin_ids)))
    rows = sess.execute(q).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def get_pagos_dict(sess, admin_ids=None):
    q = select(Pago)
    if admin_ids is not None:
        if isinstance(admin_ids, (list, set, tuple)):
            q = q.where(Pago.admin_id.in_(list(admin_ids)))
        else:
            q = q.where(Pago.admin_id == admin_ids)
    rows = sess.execute(q).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def get_starting_bank(sess, admin_id: int) -> float:
    a = sess.get(Admin, admin_id)
    return float(a.starting_bank) if a else 0.0


def get_banco_sum(sess, admin_id: int) -> float:
    r = sess.execute(
        select(func.coalesce(func.sum(Banco.amount), 0)).where(Banco.admin_id == admin_id)
    ).scalar()
    return float(r or 0)


def _repo_add_banco(sess, data: dict) -> int:
    b = Banco(
        admin_id=data["admin_id"],
        user_id=data.get("user_id"),
        collector_id=data.get("collector_id"),
        amount=data["amount"],
        movement_type=data["movement_type"],
        note=data.get("note"),
        mov_date=data.get("date") or data.get("mov_date") or date.today(),
    )
    sess.add(b)
    sess.flush()
    return b.id


def delete_banco_movement(sess, mov_id: int):
    sess.execute(Banco.__table__.delete().where(Banco.id == mov_id))


def get_atrasos_dict(sess, admin_id: int):
    rows = sess.execute(select(Atraso).where(Atraso.admin_id == admin_id)).scalars().all()
    return {r.id: r.to_dict() for r in rows}


def get_pagos_admin_list(sess, admin_ids=None):
    q = select(PagoAdmin)
    if admin_ids is not None:
        q = q.where(PagoAdmin.admin_id.in_(list(admin_ids)))
    rows = sess.execute(q).scalars().all()
    return [r.to_dict() for r in rows]


def save_auditoria(sess, data: dict) -> int:
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
    sess.add(a)
    sess.flush()
    return a.id


def get_admins_all(sess):
    rows = sess.execute(select(Admin)).scalars().all()
    return [r.to_dict() for r in rows]


# ---------------------------------------------------------------------------
# API de alto nivel (antes appdb.ops)
# ---------------------------------------------------------------------------
def get_users():
    return get_usuarios_dict(get_session())


def get_user(user_id):
    return get_usuario(get_session(), user_id)


def get_user_by_username(username):
    return get_usuario_by_username(get_session(), username)


def get_clients(admin_ids=None):
    return get_clientes_dict(get_session(), admin_ids)


def get_loans(admin_ids=None):
    return get_prestamos_dict(get_session(), admin_ids)


def get_payments(admin_ids=None):
    return get_pagos_dict(get_session(), admin_ids)


def get_bank_available(org_id=None):
    oid = org_id if org_id is not None else flask_session.get("org_id") or 1
    s = get_session()
    base = get_starting_bank(s, oid)
    total = get_banco_sum(s, oid)
    result = float(base) + float(total)
    if abs(result) < 1e-9:
        result = 0.0
    return round(result, 2)


def add_banco_movement(movement_type, amount, note, user_id=None, org_id=None, collector_id=None):
    oid = org_id if org_id is not None else flask_session.get("org_id") or 1
    amt = float(amount or 0)
    if abs(amt) < 1e-9:
        return None
    avail = get_bank_available(oid)
    if avail + amt < -1e-9:
        raise ValueError(f"Banco insuficiente. Disponible: {avail} | Requerido adicional: {abs(amt)}")
    rid = _repo_add_banco(get_session(), {
        "admin_id": oid,
        "user_id": user_id,
        "collector_id": collector_id,
        "amount": round(amt, 2),
        "movement_type": movement_type,
        "note": note or movement_type,
        "date": date.today(),
    })
    get_session().flush()
    return rid


def pop_banco_movement(mov_id):
    delete_banco_movement(get_session(), mov_id)
    get_session().flush()


def get_loan_arrears(org_id):
    return get_atrasos_dict(get_session(), org_id) if org_id else {}


def get_admin_payments(admin_ids=None):
    return get_pagos_admin_list(get_session(), admin_ids)


def save_audit(data):
    save_auditoria(get_session(), data)
    get_session().flush()


def get_admins():
    return get_admins_all(get_session())
