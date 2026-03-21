# -*- coding: utf-8 -*-
"""
PostgreSQL para CREDIMAPA — un solo archivo (Render/GitHub no pierden carpetas).
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from decimal import Decimal

from rd_time import today_rd, utc_now_for_db

from flask import session as flask_session
from sqlalchemy import (
    Column, Integer, String, Text, Numeric, Boolean, Date, DateTime,
    ForeignKey, Index, create_engine, select, func, and_, text, update,
)
from sqlalchemy.exc import IntegrityError
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
    created_at = Column(DateTime, default=utc_now_for_db)

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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
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
    closed_at = Column(DateTime, default=utc_now_for_db)
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
    created_at = Column(DateTime, default=utc_now_for_db)
    __table_args__ = (Index("ix_auditoria_admin_id", "admin_id"), Index("ix_auditoria_user_id", "user_id"))


# ---------------------------------------------------------------------------
# Engine / sesión  (solo PostgreSQL; sin SQLite ni URL local por defecto)
# ---------------------------------------------------------------------------
_raw_pg = (os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or "").strip()
if not _raw_pg or str(_raw_pg).lower() in ("none", "null"):
    raise RuntimeError(
        "credimapa_pg: defina DATABASE_URL o POSTGRES_URL apuntando a PostgreSQL (p. ej. en Render)."
    )
DATABASE_URL = _raw_pg
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
# Driver explícito para Render / psycopg2-binary
_pg_prefix = "postgresql://"
if DATABASE_URL.startswith(_pg_prefix) and "psycopg2" not in DATABASE_URL.split("://", 1)[0]:
    DATABASE_URL = "postgresql+psycopg2://" + DATABASE_URL[len(_pg_prefix) :]

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


def _sync_pg_id_sequences(connection) -> None:
    """Tras INSERT con id fijo, alinea serial de PostgreSQL para próximos autoincrement."""
    connection.execute(
        text(
            "SELECT setval(pg_get_serial_sequence('usuarios', 'id'), "
            "COALESCE((SELECT MAX(id) FROM usuarios), 1))"
        )
    )
    connection.execute(
        text(
            "SELECT setval(pg_get_serial_sequence('admins', 'id'), "
            "COALESCE((SELECT MAX(id) FROM admins), 1))"
        )
    )


def _seed_defaults_if_empty() -> None:
    """
    Si la BD está vacía (p. ej. Postgres nuevo en Render), crea super_admin y tenant admin.
    Misma lógica que init_db.py — no hace falta ejecutar el script a mano.
    """
    from werkzeug.security import generate_password_hash

    super_user = os.getenv("SUPER_ADMIN_USERNAME", "super_admin")
    super_pass = os.getenv("SUPER_ADMIN_PASSWORD", "super_admin")
    tenant_days = int(os.getenv("DEFAULT_TENANT_SUBSCRIPTION_DAYS", "30"))
    starting_bank = float(os.getenv("STARTING_BANK", "0") or 0)

    try:
        with session_scope() as session:
            n = session.scalar(select(func.count()).select_from(Usuario))
            if n:
                return

            created = utc_now_for_db()
            sub_end = created + timedelta(days=tenant_days)
            sub_end_d = sub_end.date()

            session.add(
                Usuario(
                    id=1,
                    admin_id=None,
                    username=super_user,
                    password_hash=generate_password_hash(super_pass),
                    role="super_admin",
                    phone="",
                    account_status="activo",
                    fecha_inicio=created.date(),
                    fecha_fin=sub_end_d,
                    subscription_end=sub_end_d,
                    created_at=created,
                )
            )
            session.flush()

            session.add(
                Admin(
                    id=2,
                    account_status="activo",
                    fecha_inicio=created,
                    fecha_fin=sub_end,
                    subscription_end=sub_end,
                    starting_bank=starting_bank,
                    is_default=True,
                    created_at=created,
                )
            )
            session.flush()

            session.add(
                Usuario(
                    id=2,
                    admin_id=2,
                    username="admin",
                    password_hash=generate_password_hash("admin"),
                    role="admin",
                    phone="",
                    account_status="activo",
                    fecha_inicio=created.date(),
                    fecha_fin=sub_end_d,
                    subscription_end=sub_end_d,
                    created_at=created,
                )
            )
            session.flush()

            _sync_pg_id_sequences(session.connection())
    except IntegrityError:
        # Otro worker de Gunicorn sembró al mismo tiempo
        pass


def mask_database_url(url: str) -> str:
    if not url:
        return "(vacío)"
    if "@" not in url:
        return url
    try:
        scheme, rest = url.split("://", 1)
        if "@" in rest:
            _creds, hostpart = rest.rsplit("@", 1)
            return f"{scheme}://***:***@{hostpart}"
    except Exception:
        pass
    return "***"


def init_app(app):
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    app.logger.info("PostgreSQL OK (SELECT 1). URI activa: %s", mask_database_url(DATABASE_URL))
    init_db()
    try:
        _seed_defaults_if_empty()
    except Exception as e:
        app.logger.warning("PostgreSQL bootstrap: %s", e)

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


def sum_banco_amount(
    sess,
    admin_id: int,
    *,
    movement_type: str | None = None,
    movement_types: tuple[str, ...] | list[str] | None = None,
    user_id: int | None = None,
    banco_ids: set[int] | frozenset[int] | list[int] | None = None,
) -> float:
    """Suma de `amount` en `banco` para el tenant, con filtros opcionales."""
    q = select(func.coalesce(func.sum(Banco.amount), 0)).where(Banco.admin_id == admin_id)
    if movement_type is not None:
        q = q.where(Banco.movement_type == movement_type)
    elif movement_types is not None:
        q = q.where(Banco.movement_type.in_(list(movement_types)))
    if user_id is not None:
        q = q.where(Banco.user_id == user_id)
    if banco_ids is not None:
        ids = list(banco_ids)
        if not ids:
            return 0.0
        q = q.where(Banco.id.in_(ids))
    r = sess.execute(q).scalar()
    return float(r or 0)


def sum_banco_abs_amount(
    sess,
    admin_id: int,
    movement_types: tuple[str, ...] | list[str],
    *,
    user_id: int | None = None,
) -> float:
    """SUM(ABS(amount)) para gastos u otros tipos donde el signo no importa para el total mostrado."""
    q = select(func.coalesce(func.sum(func.abs(Banco.amount)), 0)).where(Banco.admin_id == admin_id)
    q = q.where(Banco.movement_type.in_(list(movement_types)))
    if user_id is not None:
        q = q.where(Banco.user_id == user_id)
    r = sess.execute(q).scalar()
    return float(r or 0)


def list_banco_descuentos_iniciales(
    sess,
    admin_id: int,
    *,
    banco_ids: set[int] | frozenset[int] | None = None,
    limit: int = 500,
) -> list[dict]:
    q = select(Banco).where(
        Banco.admin_id == admin_id,
        Banco.movement_type == "descuento_inicial",
    )
    if banco_ids is not None:
        ids = list(banco_ids)
        if not ids:
            return []
        q = q.where(Banco.id.in_(ids))
    q = q.order_by(Banco.created_at.desc()).limit(limit)
    rows = sess.execute(q).scalars().all()
    return [r.to_dict() for r in rows]


def _repo_add_banco(sess, data: dict) -> int:
    b = Banco(
        admin_id=data["admin_id"],
        user_id=data.get("user_id"),
        collector_id=data.get("collector_id"),
        amount=data["amount"],
        movement_type=data["movement_type"],
        note=data.get("note"),
        mov_date=data.get("date") or data.get("mov_date") or today_rd(),
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


def list_tenant_usuarios(admin_id: int):
    """Usuarios del tenant (sin duplicados; no incluye super_admin)."""
    s = get_session()
    rows = s.execute(select(Usuario).where(Usuario.admin_id == admin_id)).scalars().all()
    return [r.to_dict() for r in rows]


def username_exists(username: str) -> bool:
    u = (username or "").strip()
    if not u:
        return False
    return get_usuario_by_username(get_session(), u) is not None


def create_tenant_usuario(
    admin_id: int,
    username: str,
    password_hash: str,
    role: str,
    phone: str,
    account_status: str,
    fecha_inicio=None,
    fecha_fin=None,
) -> int:
    s = get_session()
    row = Usuario(
        admin_id=admin_id,
        username=(username or "").strip(),
        password_hash=password_hash,
        role=role,
        phone=phone or "",
        account_status=account_status,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        subscription_end=fecha_fin,
    )
    s.add(row)
    s.flush()
    new_id = row.id
    s.commit()
    return new_id


def _clear_usuario_fks(sess, uid: int) -> None:
    sess.execute(update(Cliente).where(Cliente.created_by == uid).values(created_by=None))
    sess.execute(update(Prestamo).where(Prestamo.created_by == uid).values(created_by=None))
    sess.execute(update(Banco).where(Banco.user_id == uid).values(user_id=None))
    sess.execute(update(Banco).where(Banco.collector_id == uid).values(collector_id=None))
    sess.execute(update(Pago).where(Pago.created_by == uid).values(created_by=None))
    sess.execute(update(Gasto).where(Gasto.user_id == uid).values(user_id=None))
    sess.execute(update(Cierre).where(Cierre.user_id == uid).values(user_id=None))
    sess.execute(update(Auditoria).where(Auditoria.user_id == uid).values(user_id=None))


def delete_tenant_usuario(user_id: int, admin_id: int) -> bool:
    s = get_session()
    row = s.get(Usuario, user_id)
    if not row or row.admin_id != admin_id or row.role == "super_admin":
        return False
    _clear_usuario_fks(s, user_id)
    s.delete(row)
    s.commit()
    return True


def get_clients(admin_ids=None):
    return get_clientes_dict(get_session(), admin_ids)


def get_client(client_id: int):
    """Un cliente como dict (equiv. a Cliente.query.get(id))."""
    row = get_session().get(Cliente, client_id)
    return row.to_dict() if row else None


def create_client(
    admin_id: int,
    created_by,
    first_name: str,
    last_name: str = "",
    document_id: str = "",
    phone: str = "",
    address: str = "",
    route: str = "",
) -> int:
    """
    Inserta cliente en PostgreSQL (equiv. a db.session.add(cliente); db.session.commit()).
    """
    s = get_session()
    row = Cliente(
        admin_id=admin_id,
        first_name=(first_name or "").strip(),
        last_name=((last_name or "").strip() or None),
        document_id=((document_id or "").strip() or None),
        phone=((phone or "").strip() or None),
        address=((address or "").strip() or None),
        route=((route or "").strip() or None),
        created_by=created_by,
    )
    s.add(row)
    s.flush()
    new_id = row.id
    s.commit()
    if os.getenv("DEBUG_CLIENTS"):
        n = s.scalar(
            select(func.count()).select_from(Cliente).where(Cliente.admin_id == admin_id)
        )
        print(f"[DEBUG_CLIENTS] create_client committed id={new_id} admin_id={admin_id} org_total={n}")
    return new_id


def update_client(
    client_id: int,
    admin_id: int,
    first_name: str,
    last_name: str = "",
    phone: str = "",
    address: str = "",
    document_id: str = "",
    route: str = "",
) -> bool:
    s = get_session()
    row = s.get(Cliente, client_id)
    if not row or row.admin_id != admin_id:
        return False
    row.first_name = (first_name or "").strip()
    row.last_name = ((last_name or "").strip() or None)
    row.phone = ((phone or "").strip() or None)
    row.address = ((address or "").strip() or None)
    row.document_id = ((document_id or "").strip() or None)
    row.route = ((route or "").strip() or None)
    s.add(row)
    s.commit()
    return True


def delete_prestamo_cascade(sess, loan_id: int) -> None:
    """Elimina préstamo; FK ON DELETE CASCADE limpia pagos, atrasos, descuentos."""
    sess.execute(Prestamo.__table__.delete().where(Prestamo.id == loan_id))


def delete_client_db(client_id: int, admin_id: int, loan_ids: list) -> bool:
    s = get_session()
    row = s.get(Cliente, client_id)
    if not row or row.admin_id != admin_id:
        return False
    for lid in loan_ids:
        delete_prestamo_cascade(s, int(lid))
    s.delete(row)
    s.commit()
    return True


def get_loan(loan_id: int):
    return get_prestamo(get_session(), loan_id)


def create_prestamo(
    admin_id: int,
    client_id: int,
    created_by: int,
    amount: float,
    rate: float,
    frequency: str,
    start_date,
    next_payment_date,
    term_count: int,
    remaining: float,
    total_interest: float,
    total_to_pay: float,
    upfront_percent: float,
    installment_amount: float,
    discount_banco_id=None,
    disbursement_banco_id=None,
) -> int:
    """Inserta préstamo y hace commit (persistente en PostgreSQL)."""
    s = get_session()
    row = Prestamo(
        admin_id=admin_id,
        client_id=client_id,
        amount=amount,
        rate=rate,
        frequency=frequency or "semanal",
        start_date=start_date,
        next_payment_date=next_payment_date,
        term_count=term_count,
        remaining=remaining,
        remaining_capital=remaining,
        total_interest=total_interest,
        total_to_pay=total_to_pay,
        upfront_percent=upfront_percent,
        installment_amount=installment_amount,
        total_interest_paid=0,
        status="ACTIVO",
        created_by=created_by,
        discount_banco_id=discount_banco_id,
        disbursement_banco_id=disbursement_banco_id,
    )
    s.add(row)
    s.flush()
    lid = row.id
    s.commit()
    return lid


def update_prestamo_simple(loan_id: int, admin_id: int, rate=None, remaining=None) -> bool:
    s = get_session()
    row = s.get(Prestamo, loan_id)
    if not row or row.admin_id != admin_id:
        return False
    if rate is not None:
        row.rate = rate
    if remaining is not None:
        row.remaining = remaining
        row.remaining_capital = remaining
    s.commit()
    return True


def delete_loan_row(loan_id: int, admin_id: int) -> bool:
    s = get_session()
    row = s.get(Prestamo, loan_id)
    if not row or row.admin_id != admin_id:
        return False
    delete_prestamo_cascade(s, loan_id)
    s.commit()
    return True


def save_prestamo_from_loan_dict(d: dict) -> bool:
    """Persiste campos mutables del préstamo desde un dict (como en memoria)."""
    s = get_session()
    row = s.get(Prestamo, d.get("id"))
    oid = d.get("organization_id")
    if not row or row.admin_id != oid:
        return False
    row.remaining = float(d.get("remaining") or 0)
    rc = d.get("remaining_capital")
    if rc is not None:
        row.remaining_capital = float(rc)
    row.next_payment_date = d.get("next_payment_date")
    st = d.get("status")
    if st is not None:
        row.status = str(st)
    s.commit()
    return True


def insert_pago_and_sync_loan(
    admin_id: int,
    loan_id: int,
    payment: dict,
    loan_snapshot: dict,
) -> int:
    """Equiv. a db.session.add(Pago); actualizar Prestamo; commit."""
    s = get_session()
    pay_date = payment.get("date")
    if hasattr(pay_date, "date"):
        pay_date = pay_date.date()
    rowp = Pago(
        admin_id=admin_id,
        loan_id=loan_id,
        amount=float(payment.get("amount") or 0),
        type=(payment.get("type") or "cuota"),
        pago_date=pay_date,
        capital=float(payment.get("capital") or 0),
        interest=float(payment.get("interest") or 0),
        status=payment.get("status") or "OK",
        weeks_advanced=payment.get("weeks_advanced"),
        created_by=payment.get("created_by"),
    )
    s.add(rowp)
    rowl = s.get(Prestamo, loan_id)
    if rowl and rowl.admin_id == admin_id:
        rowl.remaining = float(loan_snapshot.get("remaining") or 0)
        rc = loan_snapshot.get("remaining_capital")
        if rc is not None:
            rowl.remaining_capital = float(rc)
        rowl.next_payment_date = loan_snapshot.get("next_payment_date")
        st = loan_snapshot.get("status")
        if st is not None:
            rowl.status = str(st)
    s.flush()
    pid = rowp.id
    s.commit()
    return pid


def get_payment_row(payment_id: int):
    row = get_session().get(Pago, payment_id)
    return row.to_dict() if row else None


def delete_pago_by_id(payment_id: int, admin_id: int) -> bool:
    s = get_session()
    row = s.get(Pago, payment_id)
    if not row or row.admin_id != admin_id:
        return False
    s.delete(row)
    s.commit()
    return True


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
        "date": today_rd(),
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
