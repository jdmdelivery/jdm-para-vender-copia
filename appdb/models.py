# -*- coding: utf-8 -*-
"""
Modelos SQLAlchemy para JDM Cash Now - multi-tenant SaaS.
Todo filtrado por admin_id (organization/tenant).
"""
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Numeric, Boolean, Date, DateTime,
    ForeignKey, Index, UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def _to_dict(obj, exclude=None):
    """Convierte un modelo a dict compatible con la lógica actual."""
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
        elif isinstance(v, date) and not isinstance(v, datetime):
            v = v
        d[c.name] = v
    return d


class Admin(Base):
    """Tenants (negocios). Configuración por organización. Sin login directo."""
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
    """Usuarios (cobradores, supervisores, cajero). super_admin en admin_id=NULL."""
    __tablename__ = "usuarios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=True)  # null = super_admin
    username = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(40), nullable=False)  # super_admin, admin, supervisor, cobrador, cajero
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
    """Clientes del tenant."""
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
    """Préstamos."""
    __tablename__ = "prestamos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    client_id = Column(Integer, ForeignKey("clientes.id", ondelete="RESTRICT"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)  # capital aprobado
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
    discount_banco_id = Column(Integer, nullable=True)  # FK banco.id
    disbursement_banco_id = Column(Integer, nullable=True)  # FK banco.id
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
    """Ledger principal. Movimientos de caja."""
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
    """Pagos de préstamos (cuota, adelanto, capital, interes)."""
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
    """Gastos de ruta."""
    __tablename__ = "gastos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.id", ondelete="CASCADE"), nullable=False)
    route = Column(String(120), nullable=True)
    kind = Column(String(60), default="otros")
    expense_type = Column(String(60), default="gasto_ruta")
    amount = Column(Numeric(14, 2), nullable=False)
    note = Column(Text, nullable=True)
    user_id = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    cash_report_id = Column(Integer, nullable=True)  # banco.id
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("ix_gastos_admin_id", "admin_id"),)

    def to_dict(self):
        d = _to_dict(self)
        d["organization_id"] = d["admin_id"]
        return d


class Atraso(Base):
    """Atrasos de préstamos."""
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
    """Pagos de suscripción de admins (super admin)."""
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
    """Descuentos iniciales (referencia a préstamo y banco)."""
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
    """Cierres de semana."""
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
        d["notas"] = d.get("notas")
        return d


class Auditoria(Base):
    """Log de auditoría."""
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


