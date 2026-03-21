# -*- coding: utf-8 -*-
"""
Operaciones de datos para PostgreSQL. Solo DB; sin circular import con app.
"""
from flask import session

from rd_time import today_rd

from .database import get_session
from . import repository as repo


def _session():
    return get_session()


def get_users():
    return repo.get_usuarios_dict(_session())


def get_user(user_id):
    return repo.get_usuario(_session(), user_id)


def get_user_by_username(username):
    return repo.get_usuario_by_username(_session(), username)


def get_clients(admin_ids=None):
    return repo.get_clientes_dict(_session(), admin_ids)


def get_loans(admin_ids=None):
    return repo.get_prestamos_dict(_session(), admin_ids)


def get_payments(admin_ids=None):
    return repo.get_pagos_dict(_session(), admin_ids)


def get_bank_available(org_id=None):
    oid = org_id if org_id is not None else session.get("org_id") or 1
    base = repo.get_starting_bank(_session(), oid)
    total = repo.get_banco_sum(_session(), oid)
    result = float(base) + float(total)
    if abs(result) < 1e-9:
        result = 0.0
    return round(result, 2)


def add_banco_movement(movement_type, amount, note, user_id=None, org_id=None, collector_id=None):
    oid = org_id if org_id is not None else session.get("org_id") or 1
    amt = float(amount or 0)
    if abs(amt) < 1e-9:
        return None
    avail = get_bank_available(oid)
    if avail + amt < -1e-9:
        raise ValueError(f"Banco insuficiente. Disponible: {avail} | Requerido adicional: {abs(amt)}")
    rid = repo.add_banco_movement(_session(), {
        "admin_id": oid,
        "user_id": user_id,
        "collector_id": collector_id,
        "amount": round(amt, 2),
        "movement_type": movement_type,
        "note": note or movement_type,
        "date": today_rd(),
    })
    _session().flush()
    return rid


def pop_banco_movement(mov_id):
    repo.delete_banco_movement(_session(), mov_id)
    _session().flush()


def get_cash_reports(org_id):
    return repo.get_banco_movements_dict(_session(), org_id)


def get_loan_arrears(org_id):
    return repo.get_atrasos_dict(_session(), org_id) if org_id else {}


def get_admin_payments(admin_ids=None):
    return repo.get_pagos_admin_list(_session(), admin_ids)


def save_audit(data):
    repo.save_auditoria(_session(), data)
    _session().flush()


def get_admins():
    return repo.get_admins_all(_session())


def get_starting_bank(org_id):
    return repo.get_starting_bank(_session(), org_id)
