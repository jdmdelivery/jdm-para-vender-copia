#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para inicializar la base de datos PostgreSQL.
Uso: python init_db.py
Variables de entorno: DATABASE_URL o POSTGRES_URL
"""
import os
import sys

# Añadir el directorio del proyecto al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from werkzeug.security import generate_password_hash
from datetime import datetime, timedelta

from credimapa_pg import init_db, session_scope, Admin, Usuario

# Config desde env (igual que app.py)
SUPER_ADMIN_USERNAME = os.getenv("SUPER_ADMIN_USERNAME", "super_admin")
SUPER_ADMIN_PASSWORD = os.getenv("SUPER_ADMIN_PASSWORD", "super_admin")
DEFAULT_TENANT_SUBSCRIPTION_DAYS = int(os.getenv("DEFAULT_TENANT_SUBSCRIPTION_DAYS", "30"))
STARTING_BANK = float(os.getenv("STARTING_BANK", "0") or 0)


def seed_initial_data():
    """Inserta super_admin y tenant por defecto si no existen."""
    with session_scope() as session:
        # Super admin (usuario id=1)
        if session.get(Usuario, 1) is None:
            created = datetime.utcnow()
            sub_end = created + timedelta(days=DEFAULT_TENANT_SUBSCRIPTION_DAYS)
            super_admin = Usuario(
                id=1,
                admin_id=None,
                username=SUPER_ADMIN_USERNAME,
                password_hash=generate_password_hash(SUPER_ADMIN_PASSWORD),
                role="super_admin",
                phone="",
                account_status="activo",
                fecha_inicio=created,
                fecha_fin=sub_end,
                subscription_end=sub_end,
                created_at=created,
            )
            session.add(super_admin)
            session.flush()
            print("Super admin creado.")

        # Tenant por defecto (admin id=2) + usuario admin (id=2)
        if session.get(Admin, 2) is None:
            created = datetime.utcnow()
            sub_end = created + timedelta(days=DEFAULT_TENANT_SUBSCRIPTION_DAYS)
            admin_tenant = Admin(
                id=2,
                account_status="activo",
                fecha_inicio=created,
                fecha_fin=sub_end,
                subscription_end=sub_end,
                starting_bank=STARTING_BANK,
                is_default=True,
                created_at=created,
            )
            session.add(admin_tenant)
            session.flush()

            admin_user = Usuario(
                id=2,
                admin_id=2,
                username="admin",
                password_hash=generate_password_hash("admin"),
                role="admin",
                phone="",
                account_status="activo",
                fecha_inicio=created,
                fecha_fin=sub_end,
                subscription_end=sub_end,
                created_at=created,
            )
            session.add(admin_user)
            session.flush()
            print("Tenant por defecto y admin creados.")


def main():
    drop = "--drop" in sys.argv
    if drop:
        print("Eliminando tablas existentes...")
    init_db(drop_all=drop)
    print("Tablas creadas.")
    try:
        seed_initial_data()
        print("Seed aplicado.")
    except Exception as e:
        print(f"Seed (puede fallar si ya existe): {e}")
    print("Listo. Ejecute la app con DATABASE_URL definido.")


if __name__ == "__main__":
    main()
