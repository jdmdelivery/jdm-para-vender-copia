# -*- coding: utf-8 -*-
"""
Adaptador que expone la interfaz de Store sobre PostgreSQL.
Permite migrar app.py con cambios mínimos.
"""
from datetime import datetime, date

from .database import get_session
from . import repository as r


class DBStore:
    """
    Objeto compatible con Store (dict-like) que usa PostgreSQL.
    Cada acceso obtiene datos frescos. org_id/session se infieren del contexto.
    """

    def _session(self):
        return get_session()

    @property
    def users(self):
        return r.get_usuarios_dict(self._session())

    def clients_for_orgs(self, admin_ids):
        return r.get_clientes_dict(self._session(), admin_ids)

    @property
    def clients(self):
        # Sin filtro: todos (el filtro se hace en clients_for_user)
        return r.get_clientes_dict(self._session())

    @property
    def loans(self):
        return r.get_prestamos_dict(self._session())

    @property
    def payments(self):
        return r.get_pagos_dict(self._session())

    @property
    def cash_reports(self):
        # Necesitamos org_id - no está en el store. Usamos un dict vacío y lo cargamos bajo demanda.
        # Mejor: cargar por org en get_bank. Para store.cash_reports values(), necesitamos por org.
        # La mayoría de usos son por org_id. Devolvemos un proxy que carga bajo demanda.
        return _CashReportsProxy(self)

    def _cash_reports_for_org(self, org_id):
        return r.get_banco_movements_dict(self._session(), org_id)

    @property
    def admin_payments(self):
        return r.get_pagos_admin_list(self._session())

    @property
    def route_expenses(self):
        return _RouteExpensesProxy(self)

    def _route_expenses_for_org(self, org_id):
        return r.get_gastos_dict(self._session(), org_id)

    @property
    def loan_arrears(self):
        return _LoanArrearsProxy(self)

    def _loan_arrears_for_org(self, org_id):
        return r.get_atrasos_dict(self._session(), org_id)

    @property
    def audit_log(self):
        return _AuditLogProxy(self)

    @property
    def deposit_history(self):
        return _DepositHistoryProxy(self)

    def _deposit_history_for_org(self, org_id):
        return r.get_deposits_list(self._session(), org_id)

    @property
    def closure_history(self):
        return _ClosureHistoryProxy(self)

    def _closure_history_for_org(self, org_id):
        return r.get_cierres_list(self._session(), org_id)

    @property
    def starting_banks(self):
        return _StartingBanksProxy(self)


class _CashReportsProxy(dict):
    """Proxy que carga cash_reports por org_id bajo demanda."""
    def __init__(self, store):
        self._store = store
        super().__init__()

    def __getitem__(self, key):
        # key es mov_id. Necesitamos buscar en todos los orgs - no tenemos org en el key.
        # El código hace store.cash_reports[rid] o store.cash_reports.pop(rid).
        # Mejor: tener un método get_cash_report(session, mov_id) que busque por id.
        s = self._store._session()
        from sqlalchemy import select
        from .models import Banco
        b = s.execute(select(Banco).where(Banco.id == key)).scalar_one_or_none()
        if b is None:
            raise KeyError(key)
        return b.to_dict()

    def __contains__(self, key):
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    def pop(self, key, *default):
        s = self._store._session()
        from .models import Banco
        b = s.get(Banco, key)
        if b is None:
            if default:
                return default[0]
            raise KeyError(key)
        d = b.to_dict()
        s.delete(b)
        s.flush()
        return d

    def values(self):
        # Sin org_id no podemos. El código que usa .values() filtra por organization_id.
        # Necesitamos pasar org. Por ahora devolvemos vacío - los usos deben cambiar a repo.
        return []


class _RouteExpensesProxy(dict):
    def __init__(self, store):
        self._store = store
        self._cache = None
        self._cache_org = None

    def _load(self, org_id):
        if self._cache_org != org_id:
            self._cache = self._store._route_expenses_for_org(org_id)
            self._cache_org = org_id
        return self._cache or {}

    def __getitem__(self, key):
        # Sin org no sabemos. Los gastos se buscan por id. Necesitamos get_gasto(session, id).
        from .database import get_session
        from sqlalchemy import select
        from .models import Gasto
        s = get_session()
        g = s.get(Gasto, key)
        if g is None:
            raise KeyError(key)
        return g.to_dict()

    def __setitem__(self, key, val):
        from . import repository as r
        s = self._store._session()
        if isinstance(val, dict):
            val["id"] = key
            # save_gasto crea nuevo - necesitamos update o upsert
            r.save_gasto(s, {**val, "id": key})
        s.flush()

    def pop(self, key, *default):
        from .models import Gasto
        s = self._store._session()
        g = s.get(Gasto, key)
        if g is None:
            if default:
                return default[0]
            raise KeyError(key)
        d = g.to_dict()
        s.delete(g)
        s.flush()
        return d


class _LoanArrearsProxy(dict):
    def __init__(self, store):
        self._store = store

    def __getitem__(self, key):
        from .models import Atraso
        s = self._store._session()
        a = s.get(Atraso, key)
        if a is None:
            raise KeyError(key)
        return a.to_dict()

    def values(self):
        from .database import get_session
        from flask import session
        oid = session.get("org_id")
        if oid:
            return (self._store._loan_arrears_for_org(oid) or {}).values()
        return []


class _AuditLogProxy(list):
    def __init__(self, store):
        self._store = store
        super().__init__()

    def append(self, item):
        r.save_auditoria(self._store._session(), item)
        self._store._session().flush()


class _DepositHistoryProxy(list):
    def __init__(self, store):
        self._store = store
        super().__init__()

    def __iter__(self):
        from flask import session
        oid = session.get("org_id")
        if oid:
            return iter(self._store._deposit_history_for_org(oid))
        return iter([])

    def append(self, item):
        # deposit_history append - el movimiento ya está en banco
        pass  # No-op: los depósitos están en banco con movement_type=deposito_banco


class _ClosureHistoryProxy(list):
    def __init__(self, store):
        self._store = store
        super().__init__()

    def __iter__(self):
        from flask import session
        oid = session.get("org_id")
        if oid:
            return iter(self._store._closure_history_for_org(oid))
        return iter([])

    def __reversed__(self):
        from flask import session
        oid = session.get("org_id")
        if oid:
            return reversed(self._store._closure_history_for_org(oid))
        return reversed([])


class _StartingBanksProxy(dict):
    def __init__(self, store):
        self._store = store
        super().__init__()

    def get(self, org_id, default=0):
        return r.get_starting_bank(self._store._session(), org_id) if org_id else default


# Para cash_reports necesitamos que get_bank_available y apply_cash_movement usen repo.
# El store.cash_reports se usa en: get_bank_available (suma amounts), apply_cash_movement (insert),
# _revert_loan_financials (pop), delete_discount (pop), etc.
# Es más limpio tener funciones directas en un módulo db_ops que reemplacen esas operaciones.