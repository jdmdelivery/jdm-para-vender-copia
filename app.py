# -*- coding: utf-8 -*-
# JDM Cash Now — datos en memoria (sin base de datos persistente). Se pierden al reiniciar el servidor.
from __future__ import annotations

import os
import json
import base64
import html
import secrets
from datetime import datetime, date, timedelta
from functools import wraps

from werkzeug.security import generate_password_hash, check_password_hash
from flask import (
    Flask,
    request,
    redirect,
    url_for,
    render_template_string,
    session,
    flash,
    get_flashed_messages,
    send_from_directory,
    jsonify,
    Response,
)

APP_BRAND = "DEMO"
# Service worker mínimo si no hay archivo en static/ (p. ej. deploy sin carpeta static).
SW_JS_MINIMAL = """/* Minimal service worker */
self.addEventListener('install', (e) => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));
"""
ADMIN_PIN = os.getenv("ADMIN_PIN", "5555")
CURRENCY = "RD$"
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "3128565688")
# Recibo térmico (impresión); sobreescribibles en producción.
RECEIPT_BUSINESS_NAME = os.getenv("RECEIPT_BUSINESS_NAME", "JDMCASHNOW")
RECEIPT_SUBTITLE = os.getenv("RECEIPT_SUBTITLE", "LA FACTORIA DEL POZO")
RECEIPT_COMPANY_TEL = os.getenv("RECEIPT_COMPANY_TEL", "8495788819")
CARTERA_ADMIN_USER_ID = 60
ORG_ID = 1
SUPER_ADMIN_USERNAME = os.getenv("SUPER_ADMIN_USERNAME", "super_admin")
SUPER_ADMIN_PASSWORD = os.getenv("SUPER_ADMIN_PASSWORD", "super_admin")
DEFAULT_TENANT_SUBSCRIPTION_DAYS = int(os.getenv("DEFAULT_TENANT_SUBSCRIPTION_DAYS", "30"))
PAYMENT_EXTENSION_DAYS = int(os.getenv("PAYMENT_EXTENSION_DAYS", "30"))

ACCOUNT_PENDING = "pendiente"
ACCOUNT_ACTIVE = "activo"
ACCOUNT_SUSPENDED = "suspendido"

ROLES = ("admin", "supervisor", "cobrador", "super_admin")


def fmt_money(val):
    try:
        v = float(val or 0)
    except (TypeError, ValueError):
        v = 0.0
    return f"{CURRENCY} {v:,.2f}"


def receipt_cuotas_label(p, L):
    """Etiqueta tipo 3/10 para el recibo (solo cuenta pagos tipo cuota)."""
    loan_id = L.get("id") or p.get("loan_id")
    term_count = int(L.get("term_count") or 1)
    if str(p.get("type") or "").strip().lower() != "cuota":
        return f"—/{term_count}"
    cuotas = [
        x
        for x in store.payments.values()
        if x.get("loan_id") == loan_id
        and (x.get("status") or "OK") != "ANULADO"
        and str(x.get("type") or "").strip().lower() == "cuota"
    ]
    cuotas.sort(key=lambda x: (x.get("date") or date.min, x.get("id") or 0))
    for i, x in enumerate(cuotas, start=1):
        if x.get("id") == p.get("id"):
            return f"{i}/{term_count}"
    return f"—/{term_count}"


def receipt_payment_when(p):
    """Fecha/hora para el recibo (DD/MM/YYYY HH:MM AM/PM RD)."""
    ts = p.get("created_at")
    if ts and hasattr(ts, "strftime"):
        return ts.strftime("%d/%m/%Y %I:%M %p") + " RD"
    d = p.get("date")
    if hasattr(d, "strftime"):
        return d.strftime("%d/%m/%Y") + " 12:00 PM RD"
    return str(d or "—") + " RD"


def freq_interval_days(freq):
    """
    Intervalo aproximado (en días) por frecuencia.
    - semanal: 7
    - diario: 1
    - quincenal: 15
    - mensual: 30
    """
    s = str(freq or "").strip().lower()
    if "diar" in s:
        return 1
    if "quinc" in s:
        return 15
    if "mens" in s:
        return 30
    return 7


def loan_frequency_label(freq):
    """Etiqueta para listados (ej. Quincenal, Semanal)."""
    s = str(freq or "").strip().lower()
    if "quinc" in s:
        return "Quincenal"
    if "seman" in s:
        return "Semanal"
    if "diar" in s:
        return "Diario"
    if "mens" in s:
        return "Mensual"
    if not s:
        return "—"
    return str(freq).title()


def proximo_sabado_cobro(d=None):
    """Sábado objetivo de la ruta: hoy si ya es sábado; si no, el próximo sábado."""
    today = d or date.today()
    ahead = (5 - today.weekday()) % 7
    return today if ahead == 0 else today + timedelta(days=ahead)


def loans_cobro_sabado_semanal(org_id, user, ref_date=None):
    """
    Préstamos semanales activos cuya próxima cuota vence a más tardar el sábado de cobro.
    Misma criterio que la lista de cobro de sábados.
    """
    today = ref_date or date.today()
    prox_sab = proximo_sabado_cobro(today)
    rows = []
    for L in loans_for_user(org_id, user):
        if str(L.get("status", "")).upper() != "ACTIVO":
            continue
        if "semanal" not in str(L.get("frequency") or "").lower():
            continue
        npd = L.get("next_payment_date")
        if npd and npd <= prox_sab:
            cid = L.get("client_id")
            c = store.clients.get(cid, {})
            nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or f"Cliente #{cid}"
            rows.append((npd, L, nm))
    rows.sort(key=lambda x: x[0] or date.max)
    return prox_sab, rows


# Tipos de gasto de ruta (iconos + etiqueta para UI).
ROUTE_EXPENSE_KINDS = {
    "gasolina": ("Gasolina", "⛽"),
    "peaje": ("Peaje", "🛣️"),
    "comida": ("Comida", "🍔"),
    "otros": ("Otros", "⚙️"),
}


def route_expense_kind_info(kind):
    k = (kind or "otros").strip().lower()
    return ROUTE_EXPENSE_KINDS.get(k, ROUTE_EXPENSE_KINDS["otros"])


def fmt_expense_datetime(dt):
    if not dt:
        return "—"
    try:
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return str(dt)


def fmt_advance_datetime(dt):
    """Fecha/hora para lista de adelantos (DD/MM/YYYY HH:MM AM/PM)."""
    if not dt:
        return "—"
    try:
        return dt.strftime("%d/%m/%Y %I:%M %p")
    except Exception:
        return str(dt)


def get_theme():
    return session.get("theme", "light")


def is_cartera_admin(user):
    return user and user.get("id") == CARTERA_ADMIN_USER_ID


BASE_STYLE = """
<style>
*,*::before,*::after{box-sizing:border-box}
html,body{width:100%;max-width:100%;overflow-x:hidden;margin:0;padding:0}
body{margin:0;font-family:system-ui,sans-serif}
body.theme-light{background:#ecfdf3;color:#022c22}
body.theme-dark{background:linear-gradient(135deg,#06131a,#022c22,#111827);color:#f9fafb}
header.topbar{display:flex;align-items:center;justify-content:center;padding:14px;background:linear-gradient(135deg,#166534,#22c55e);color:#fff;position:relative;box-shadow:0 10px 28px rgba(0,0,0,.25)}
.topbar-title{font-weight:900;font-size:18px}
.container{width:100%;padding:12px}
.card{padding:14px;border-radius:16px;margin-bottom:12px;background:rgba(255,255,255,.95)}
body.theme-dark .card{background:rgba(15,23,42,.96)}
table{width:100%;border-collapse:collapse;font-size:.9rem}
th,td{padding:9px 10px;border-bottom:1px solid rgba(148,163,184,.4)}
th{background:#ecfdf3;text-align:left}
body.theme-dark th{background:rgba(30,64,175,.25)}
.btn{padding:8px 16px;border-radius:999px;border:none;cursor:pointer;font-weight:700;font-size:.9rem;text-decoration:none;display:inline-block}
.btn-primary{background:#16a34a;color:#fff}
.btn-secondary{background:#e5e7eb;color:#0f172a}
body.theme-dark .btn-secondary{background:#334155;color:#e5e7eb}
.table-scroll{overflow-x:auto}
</style>
"""

TPL_LAYOUT = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>{{ app_brand }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes">
  <link rel="manifest" href="/manifest.json">
  <meta name="theme-color" content="#16a34a">
  """ + BASE_STYLE + """
<style>
.premium-btn{position:absolute;left:14px;top:50%;transform:translateY(-50%);width:56px;height:56px;border-radius:18px;border:none;cursor:pointer;background:rgba(255,255,255,.78);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px}
body.theme-dark .premium-btn{background:rgba(15,23,42,.65);color:#e5e7eb}
.side-menu{position:fixed;top:0;left:0;width:260px;height:100%;padding:18px;background:linear-gradient(180deg,#0f172a,#020617);transform:translateX(-100%);transition:transform .35s;z-index:9999}
.side-menu.open{transform:translateX(0)}
.side-menu a{display:block;padding:12px;margin-bottom:6px;border-radius:14px;color:#e5e7eb;text-decoration:none;font-weight:600}
.menu-overlay{position:fixed;inset:0;background:rgba(0,0,0,.35);display:none;z-index:9998}
.menu-overlay.show{display:block}
.menu-user{margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid rgba(255,255,255,.15);color:#ecfdf3}
</style>
</head>
<body class="theme-{{ theme or 'light' }}">
<header class="topbar">
{% if user %}
<button type="button" class="premium-btn" onclick="toggleMenu()"><span>☰</span><span style="font-size:11px;font-weight:700">Menú</span></button>
{% endif %}
<div class="topbar-title">{{ app_brand }}</div>
</header>
{% if user %}
<div id="menuOverlay" class="menu-overlay" onclick="closeMenu()"></div>
<div id="sideMenu" class="side-menu">
  <div class="menu-user">👤 {{ user.username }}<br><small>{{ user.role }}</small></div>
  <a href="{{ url_for('index') }}">🏠 Inicio</a>
  <a href="{{ url_for('clients') }}">👥 Clientes</a>
  <a href="{{ url_for('loans') }}">💳 Préstamos</a>
  <a href="{{ url_for('bank_home') }}">🏦 Banco</a>
  {% if user.role in ['admin','supervisor'] %}
  <a href="{{ url_for('reportes') }}">📊 Reportes</a>
  <a href="{{ url_for('audit') }}">🧾 Auditoría</a>
  <a href="{{ url_for('users') }}">👤 Usuarios</a>
  <a href="{{ url_for('reassign_clients') }}">🛣️ Rutas</a>
  {% endif %}
  <a href="{{ url_for('toggle_theme') }}">{% if theme == 'dark' %}🌙 Oscuro{% else %}☀️ Claro{% endif %}</a>
  <a href="{{ url_for('logout') }}">🚪 Salir</a>
</div>
{% endif %}
<div class="container">
{% if flashes %}{% for cat,msg in flashes %}<div class="card" style="background:#fef9c3;color:#713f12">{{ msg }}</div>{% endfor %}{% endif %}
{{ body|safe }}
</div>
<script>
function toggleMenu(){document.getElementById('sideMenu').classList.toggle('open');document.getElementById('menuOverlay').classList.toggle('show');}
function closeMenu(){document.getElementById('sideMenu').classList.remove('open');document.getElementById('menuOverlay').classList.remove('show');}
if("serviceWorker" in navigator){navigator.serviceWorker.register("/sw.js").catch(function(){});}
</script>
</body>
</html>
"""

TPL_LOGIN = """<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"/><title>{{ app_brand }}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{background:#e8f5e9;font-family:system-ui;margin:0}.card{background:#fff;width:90%;max-width:400px;margin:40px auto;padding:25px;border-radius:15px;box-shadow:0 4px 10px rgba(0,0,0,.15)}
input{width:100%;padding:10px;margin:6px 0;border-radius:8px;border:1px solid #a5d6a7}button{width:100%;padding:12px;background:#2e7d32;color:#fff;border:none;border-radius:10px;font-weight:700;margin-top:8px}</style>
</head><body><div class="card"><h1 style="text-align:center">{{ app_brand }}</h1>
{% for cat,msg in flashes %}<p style="color:#b71c1c">{{ msg }}</p>{% endfor %}
<form method="post"><label>Usuario</label><input name="username" required><label>Contraseña</label><input type="password" name="password" required>
<button>Entrar</button></form><p style="text-align:center"><a href="{{ url_for('forgot_password') }}">¿Olvidó su contraseña?</a></p>
<p style="text-align:center"><a href="https://wa.me/{{ admin_whatsapp }}" target="_blank">WhatsApp {{ admin_whatsapp }}</a></p></div></body></html>"""


class Store:
    __slots__ = (
        "users", "organizations", "clients", "loans", "payments", "loan_arrears",
        "cash_reports", "audit_log", "route_expenses", "initial_discounts", "gps_positions",
        "weekly_closures", "deposit_history", "closure_history", "starting_banks", "starting_bank_default",
        "admin_payments", "_seq",
    )

    def __init__(self):
        self.users = {}
        self.organizations = {1: {"id": 1, "name": "Principal", "slug": "principal"}}
        self.clients = {}
        self.loans = {}
        self.payments = {}
        self.loan_arrears = {}
        self.cash_reports = {}
        self.audit_log = []
        self.route_expenses = {}
        self.initial_discounts = {}
        self.gps_positions = {}
        self.weekly_closures = {}
        self.deposit_history = []
        self.closure_history = []
        self.admin_payments = []  # pagos de suscripción de admins (historial)
        # Saldo inicial del banco (por tenant/admin).
        try:
            self.starting_bank_default = float(os.getenv("STARTING_BANK", "0") or 0)
        except ValueError:
            self.starting_bank_default = 0.0
        self.starting_banks = {}
        self._seq = {
            "users": 1,
            "clients": 0,
            "loans": 0,
            "payments": 0,
            "admin_payments": 0,
            "arrears": 0,
            "cash": 0,
            "route_expenses": 0,
            "re": 0,
            "disc": 0,
            "cierre": 0,
            "audit": 0,
        }
        self._seed()

    def nid(self, key):
        self._seq[key] += 1
        return self._seq[key]

    def _seed(self):
        """Solo administrador inicial; clientes, préstamos y demás usuarios se crean desde la app."""
        created = datetime.utcnow()
        sub_end = created + timedelta(days=DEFAULT_TENANT_SUBSCRIPTION_DAYS)

        # SUPER ADMIN (plataforma).
        self.users[1] = {
            "id": 1,
            "username": SUPER_ADMIN_USERNAME,
            "password_hash": generate_password_hash(SUPER_ADMIN_PASSWORD),
            "role": "super_admin",
            "phone": "",
            "organization_id": None,
            "created_at": created,
            "name": None,
            "account_status": ACCOUNT_ACTIVE,
            "fecha_inicio": created,
            "fecha_fin": sub_end,
            "subscription_end": sub_end,  # compatibilidad
        }

        # ADMÍN TENANT inicial (su propio sistema).
        tenant_admin_id = 2
        self.users[tenant_admin_id] = {
            "id": tenant_admin_id,
            "username": "admin",
            "password_hash": generate_password_hash("admin"),
            "role": "admin",
            "phone": "",
            "organization_id": tenant_admin_id,  # Tenant = su propio admin.
            "created_at": created,
            "name": None,
            "account_status": ACCOUNT_ACTIVE,
            "fecha_inicio": created,
            "fecha_fin": sub_end,
            "subscription_end": sub_end,  # compatibilidad
        }
        self.starting_banks[tenant_admin_id] = self.starting_bank_default
        self._seq["users"] = tenant_admin_id

    def reset_all(self):
        self.__init__()


store = Store()
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(16))


def current_user():
    uid = session.get("user_id")
    return store.users.get(uid) if uid else None


def login_required(fn):
    @wraps(fn)
    def w(*a, **k):
        if not session.get("user_id"):
            flash("Debe iniciar sesión primero.", "warning")
            return redirect(url_for("login"))
        return fn(*a, **k)
    return w


def role_required(*roles):
    def deco(fn):
        @wraps(fn)
        def w(*a, **k):
            u = current_user()
            if not u or u.get("role") not in roles:
                flash("No tiene permiso.", "danger")
                return redirect(url_for("index"))
            return fn(*a, **k)
        return w
    return deco


def admin_required(fn):
    return role_required("admin")(fn)


def super_admin_required(fn):
    return role_required("super_admin")(fn)


def log_action(user_id, action, detail=""):
    store.audit_log.append({
        "id": store.nid("audit"), "user_id": user_id, "action": action, "detail": detail, "created_at": datetime.utcnow(),
    })


def ensure_org():
    # Para multi-tenant: org_id se define en el login según el usuario.
    # Si por alguna razón no existe, se intenta derivar desde el usuario actual.
    if session.get("org_id") is None:
        u = current_user()
        if u and u.get("role") != "super_admin":
            session["org_id"] = u.get("organization_id") or ORG_ID
        else:
            session["org_id"] = ORG_ID


def calc_client_score(client_id, org_id):
    prestamos_pagados = sum(
        1 for L in store.loans.values()
        if L.get("client_id") == client_id and L.get("organization_id") == org_id
        and str(L.get("status", "")).upper() in ("CERRADO", "PAGADO", "FINALIZADO")
    )
    atrasos = sum(
        1 for A in store.loan_arrears.values()
        if not A.get("paid") and store.loans.get(A.get("loan_id"), {}).get("client_id") == client_id
    )
    base = 55
    score = base + min(prestamos_pagados, 8) * 8 - min(atrasos, 10) * 12
    score = max(0, min(100, int(score)))
    nivel = "A — Excelente" if score >= 80 else "B — Bueno" if score >= 60 else "C — Regular" if score >= 40 else "D — Riesgo alto"
    return {"score": score, "nivel": nivel, "prestamos_pagados": prestamos_pagados, "atrasos": atrasos}


def calc_max_credito(prestamos_pagados, score):
    mult = max(score, 20) / 100.0
    floor_amt = 3000 + prestamos_pagados * 2500
    return round(max(floor_amt, 5000) * mult, -2)


def evaluate_loan(score, prestamos_activos):
    if prestamos_activos >= 3:
        return {"status": "warning", "status_label": "Varios préstamos activos.", "max_amount": round(max(3000, score * 80), -2)}
    if score < 40:
        return {"status": "denied", "status_label": "Score bajo.", "max_amount": round(max(1000, score * 50), -2)}
    if score < 60:
        return {"status": "warning", "status_label": "Reservas.", "max_amount": round(max(5000, score * 120), -2)}
    return {"status": "approved", "status_label": "Aprobado.", "max_amount": round(max(8000, score * 200), -2)}


def page(body, user=None):
    return render_template_string(
        TPL_LAYOUT, body=body, user=user if user is not None else current_user(),
        app_brand=APP_BRAND, flashes=get_flashed_messages(with_categories=True), theme=get_theme(),
    )


def stub_page(title, extra=""):
    return page(
        f'<div class="card"><h2>{title}</h2><p>Modo <b>memoria</b> (sin base de datos). {extra}</p>'
        f'<p><a class="btn btn-secondary" href="{url_for("bank_home")}">Volver al banco</a> '
        f'<a class="btn btn-primary" href="{url_for("dashboard")}">Dashboard</a></p></div>'
    )


def loans_for_user(org_id, user):
    rows = [L for L in store.loans.values() if L.get("organization_id") == org_id]
    if user.get("role") == "cobrador" and not is_cartera_admin(user):
        rows = [L for L in rows if L.get("created_by") == user["id"]]
    return rows


def clients_for_user(org_id, user):
    rows = [c for c in store.clients.values() if c.get("organization_id") == org_id]
    if user.get("role") == "cobrador" and not is_cartera_admin(user):
        rows = [c for c in rows if c.get("created_by") == user["id"]]
    return rows


def bank_org_id(org_id=None):
    return org_id if org_id is not None else session.get("org_id") or ORG_ID


def get_bank_available(org_id=None):
    oid = bank_org_id(org_id)
    total = float(getattr(store, "starting_banks", {}).get(oid, 0.0) or 0.0)
    for cr in store.cash_reports.values():
        if cr.get("organization_id") == oid:
            total += float(cr.get("amount") or 0)
    # Evitar valores tipo -0.00 por redondeos.
    if abs(total) < 1e-9:
        total = 0.0
    return round(total, 2)


def apply_cash_movement(movement_type, amount, note, user_id=None, org_id=None, collector_id=None):
    """
    Aplica un movimiento al banco (memoria) y valida que nunca quede negativo.
    movement_type: string informativo (deposito_banco, prestamo_entregado, descuento_inicial, pago_prestamo, gasto_ruta, etc.)
    collector_id: opcional (p. ej. entrega de efectivo al cobrador antes de la ruta).
    """
    oid = bank_org_id(org_id)
    amt = float(amount or 0)
    if abs(amt) < 1e-9:
        return None

    projected = get_bank_available(oid) + amt
    if projected < -1e-9:
        raise ValueError(f"Banco insuficiente. Disponible: {get_bank_available(oid)} | Requerido adicional: {abs(amt)}")
    # Evitar -0.00
    if abs(projected) < 1e-9:
        projected = 0.0

    rid = store.nid("cash")
    rec = {
        "id": rid,
        "user_id": user_id,
        "date": date.today(),
        "amount": round(amt, 2),
        "note": note or movement_type,
        "created_at": datetime.utcnow(),
        "organization_id": oid,
        "movement_type": movement_type,
    }
    if collector_id is not None:
        rec["collector_id"] = int(collector_id)
    store.cash_reports[rid] = rec
    return rid


def loan_ids_visible(org_id, user):
    return {L["id"] for L in loans_for_user(org_id, user)}


def payments_in_scope(org_id, user):
    lids = loan_ids_visible(org_id, user)
    return [p for p in store.payments.values() if p.get("loan_id") in lids]


def count_loan_cuota_payments(loan_id):
    """Pagos tipo cuota válidos (para numerar la cuota vencida siguiente)."""
    return sum(
        1
        for p in store.payments.values()
        if p.get("loan_id") == loan_id
        and (p.get("status") or "OK") != "ANULADO"
        and str(p.get("type") or "").strip().lower() == "cuota"
    )


def compute_financial_kpis(org_id, user):
    """Métricas alineadas al panel «Resumen financiero» (memoria)."""
    L = loans_for_user(org_id, user)
    P = payments_in_scope(org_id, user)
    ok = [p for p in P if (p.get("status") or "OK") != "ANULADO"]

    capital_prestado = sum(float(x.get("amount") or 0) for x in L)
    capital_cobrado = sum(float(p.get("capital") or 0) for p in ok)
    activos = [x for x in L if str(x.get("status", "")).upper() == "ACTIVO"]
    capital_pendiente = sum(float(x.get("remaining") or 0) for x in activos)

    interes_total = sum(float(x.get("total_interest") or 0) for x in L)
    interes_cobrado = sum(float(p.get("interest") or 0) for p in ok)
    interes_pendiente = max(interes_total - interes_cobrado, 0)
    total_por_cobrar = capital_pendiente + interes_pendiente

    today = date.today()
    cobrado_hoy = sum(float(p.get("amount") or 0) for p in ok if p.get("date") == today)
    interes_hoy = sum(float(p.get("interest") or 0) for p in ok if p.get("date") == today)

    atrasados = 0
    for x in activos:
        npd = x.get("next_payment_date")
        if npd and npd < today:
            atrasados += 1

    n_cobradores = sum(1 for u in store.users.values() if u.get("role") == "cobrador" and u.get("organization_id") == org_id)

    return {
        "capital_prestado": capital_prestado,
        "capital_cobrado": capital_cobrado,
        "capital_pendiente": capital_pendiente,
        "interes_total": interes_total,
        "interes_cobrado": interes_cobrado,
        "interes_pendiente": interes_pendiente,
        "total_por_cobrar": total_por_cobrar,
        "cobrado_hoy": cobrado_hoy,
        "interes_hoy": interes_hoy,
        "atrasados": atrasados,
        "n_prestamos": len(L),
        "n_clientes": len(clients_for_user(org_id, user)),
        "n_cobradores": n_cobradores,
        "n_activos": len(activos),
    }


def nav_subfooter():
    u = current_user()
    if u and u.get("role") == "super_admin":
        return f'<p style="margin-top:20px"><a class="btn btn-secondary" href="{url_for("super_admin_panel")}">← Panel Super Admin</a></p>'
    return (
        f'<p style="margin-top:20px"><a class="btn btn-secondary" href="{url_for("dashboard")}">← Dashboard</a> '
        f'<a class="btn btn-secondary" href="{url_for("bank_home")}">Banco</a></p>'
    )


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        user = next((u for u in store.users.values() if u["username"] == username), None)
        if not user or not check_password_hash(user["password_hash"], password):
            flash("Usuario o contraseña incorrectos.", "danger")
            return render_template_string(
                TPL_LOGIN, flashes=get_flashed_messages(with_categories=True), app_brand=APP_BRAND, admin_whatsapp=ADMIN_WHATSAPP
            )

        session.clear()

        # Bloqueos por estado/aprobación/suscripción.
        role = user.get("role")
        if role == "super_admin":
            session["user_id"] = user["id"]
            session["role"] = "super_admin"
            session["org_id"] = None
        else:
            tenant_id = user.get("organization_id") or ORG_ID
            tenant_admin = store.users.get(tenant_id, {}) if tenant_id else user
            now = datetime.utcnow()

            def to_date(x):
                if not x:
                    return None
                if isinstance(x, datetime):
                    return x.date()
                try:
                    # datetime -> date
                    if hasattr(x, "date") and not isinstance(x, date):
                        return x.date()
                except Exception:
                    pass
                if isinstance(x, date):
                    return x
                return None

            now_d = now.date()
            u_status = user.get("account_status", ACCOUNT_ACTIVE)
            tenant_status = tenant_admin.get("account_status", ACCOUNT_ACTIVE)

            # Expiración por fecha: se bloquea con mensaje requerido.
            u_fin = to_date(user.get("fecha_fin")) or to_date(user.get("subscription_end"))
            tenant_fin = to_date(tenant_admin.get("fecha_fin")) or to_date(tenant_admin.get("subscription_end"))
            if u_fin and now_d > u_fin:
                flash("Acceso vencido", "danger")
                return render_template_string(
                    TPL_LOGIN,
                    flashes=get_flashed_messages(with_categories=True),
                    app_brand=APP_BRAND,
                    admin_whatsapp=ADMIN_WHATSAPP,
                )
            if tenant_fin and now_d > tenant_fin:
                flash("Acceso vencido", "danger")
                return render_template_string(
                    TPL_LOGIN,
                    flashes=get_flashed_messages(with_categories=True),
                    app_brand=APP_BRAND,
                    admin_whatsapp=ADMIN_WHATSAPP,
                )

            # Pendiente de aprobación.
            if u_status == ACCOUNT_PENDING or tenant_status == ACCOUNT_PENDING:
                flash("Usuario pendiente de aprobación por el sistema", "warning")
                return render_template_string(
                    TPL_LOGIN,
                    flashes=get_flashed_messages(with_categories=True),
                    app_brand=APP_BRAND,
                    admin_whatsapp=ADMIN_WHATSAPP,
                )

            # Suspensión por falta de pago.
            if u_status == ACCOUNT_SUSPENDED or tenant_status == ACCOUNT_SUSPENDED:
                flash("Usuario suspendido por el sistema", "danger")
                return render_template_string(
                    TPL_LOGIN,
                    flashes=get_flashed_messages(with_categories=True),
                    app_brand=APP_BRAND,
                    admin_whatsapp=ADMIN_WHATSAPP,
                )

            if tenant_status != ACCOUNT_ACTIVE:
                flash("Usuario suspendido por el sistema", "danger")
                return render_template_string(
                    TPL_LOGIN,
                    flashes=get_flashed_messages(with_categories=True),
                    app_brand=APP_BRAND,
                    admin_whatsapp=ADMIN_WHATSAPP,
                )

            # Alerta automática por suscripción: si vence en 3 días.
            effective_fin = u_fin or tenant_fin
            if effective_fin and hasattr(effective_fin, "strftime"):
                days_left = (effective_fin - now_d).days
                if days_left == 3:
                    flash("Te quedan 3 días", "warning")
                elif 1 <= days_left <= 2:
                    flash(f"Te quedan {days_left} días", "warning")

            session["user_id"] = user["id"]
            session["role"] = role
            session["org_id"] = tenant_id

        try:
            log_action(user["id"], "login", "login")
        except Exception:
            pass
        flash(f"Bienvenido, {user['username']}", "success")
        return redirect(url_for("index"))
    return render_template_string(
        TPL_LOGIN, flashes=get_flashed_messages(with_categories=True), app_brand=APP_BRAND, admin_whatsapp=ADMIN_WHATSAPP
    )


@app.route("/register-admin", methods=["GET", "POST"])
def register_admin():
    """
    Registro de nuevos admins (tenants).
    Quedan en estado "pendiente" hasta aprobación del super admin.
    """
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        days = request.form.get("subscription_days", type=int) or DEFAULT_TENANT_SUBSCRIPTION_DAYS

        if not username or not password:
            flash("Complete usuario y contraseña.", "danger")
            return redirect(url_for("register_admin"))
        if any(u.get("username") == username for u in store.users.values()):
            flash("Usuario ya existe.", "danger")
            return redirect(url_for("register_admin"))
        if days < 1:
            days = DEFAULT_TENANT_SUBSCRIPTION_DAYS

        created = datetime.utcnow()
        sub_end = created + timedelta(days=days)

        uid = store.nid("users")
        store.users[uid] = {
            "id": uid,
            "username": username,
            "password_hash": generate_password_hash(password),
            "role": "admin",
            "phone": phone,
            "organization_id": uid,  # Tenant = su propio admin.
            "created_at": created,
            "name": None,
            "account_status": ACCOUNT_PENDING,
            "fecha_inicio": created,
            "fecha_fin": sub_end,
            "subscription_end": sub_end,  # compatibilidad
        }
        store.starting_banks[uid] = store.starting_bank_default
        flash("Registro recibido. Queda pendiente de aprobación por el sistema.", "success")
        return redirect(url_for("login"))

    body = (
        f'<div class="card" style="padding:16px;max-width:520px;margin:20px auto;">'
        f'<h2 style="margin:0 0 12px 0;color:#14532d;">Registro de nuevo Admin</h2>'
        f'<form method="post">'
        f'<label>Usuario</label><input name="username" required>'
        f'<label>Contraseña</label><input type="password" name="password" required>'
        f'<label>Teléfono (opcional)</label><input name="phone" placeholder="809...">'
        f'<label>Días de suscripción</label><input name="subscription_days" type="number" min="1" value="{DEFAULT_TENANT_SUBSCRIPTION_DAYS}">'
        f'<button class="btn btn-primary" type="submit" style="width:auto;margin-top:12px">Registrar (pendiente)</button>'
        f'</form>'
        f'<p style="margin-top:10px"><a class="btn btn-secondary" href="{url_for("login")}">Volver al login</a></p>'
        f'</div>'
    )
    return page(body, user=None)


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "success")
    return redirect(url_for("login"))


@app.route("/")
def index():
    # Compatibilidad: si Render o un monitor hace health-check contra "/",
    # evitamos redirects a login y respondemos 200.
    ua = (request.headers.get("User-Agent") or "").lower()
    if request.method in ("GET", "HEAD") and "go-http-client" in ua:
        return Response("ok\n", status=200, mimetype="text/plain; charset=utf-8")
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/toggle-theme")
@login_required
def toggle_theme():
    session["theme"] = "dark" if session.get("theme", "light") == "light" else "light"
    return redirect(request.referrer or url_for("index"))


@app.route("/manifest.json")
def manifest():
    return jsonify({
        "name": APP_BRAND, "short_name": "DEMO",
        "description": "Gestión de préstamos y cobros.",
        "start_url": "/dashboard", "scope": "/", "display": "standalone",
        "theme_color": "#16a34a", "background_color": "#ecfdf3",
    })


@app.route("/healthz", methods=["GET", "HEAD"])
def healthz():
    """Sin auth ni redirects — Render debe usar esto como Health Check Path."""
    hdrs = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "X-Content-Type-Options": "nosniff",
    }
    if request.method == "HEAD":
        return Response("", status=200, mimetype="text/plain; charset=utf-8", headers=hdrs)
    return Response("ok\n", status=200, mimetype="text/plain; charset=utf-8", headers=hdrs)


@app.route("/sw.js")
def service_worker():
    folder = app.static_folder
    path = os.path.join(folder, "sw.js") if folder else ""
    if folder and os.path.isfile(path):
        return send_from_directory(folder, "sw.js", mimetype="application/javascript")
    return Response(SW_JS_MINIMAL, mimetype="application/javascript")


@app.route("/api/notification-check")
@login_required
def api_notification_check():
    ensure_org()
    org_id = session.get("org_id")
    user = current_user()
    morosos = sum(
        1 for A in store.loan_arrears.values()
        if not A.get("paid") and store.loans.get(A.get("loan_id"), {}).get("organization_id") == org_id
    )
    hoy = sum(
        1 for L in store.loans.values()
        if L.get("organization_id") == org_id and str(L.get("status", "")).upper() == "ACTIVO" and L.get("next_payment_date") == date.today()
    )
    if user and user.get("role") == "cobrador" and not is_cartera_admin(user):
        morosos = sum(
            1 for A in store.loan_arrears.values()
            if not A.get("paid") and store.loans.get(A.get("loan_id"), {}).get("created_by") == user["id"]
        )
        hoy = sum(
            1 for L in store.loans.values()
            if L.get("created_by") == user["id"] and str(L.get("status", "")).upper() == "ACTIVO" and L.get("next_payment_date") == date.today()
        )
    if morosos > 0:
        return jsonify({"show": True, "title": "Atrasos", "body": f"{morosos} registro(s)"})
    if hoy > 0:
        return jsonify({"show": True, "title": "Cobros hoy", "body": f"{hoy} préstamo(s)"})
    return jsonify({"show": False})


@app.route("/dashboard")
@login_required
def dashboard():
    user = current_user()
    if user["role"] not in ROLES:
        flash("Acceso no permitido", "danger")
        return redirect(url_for("index"))
    if user.get("role") == "super_admin":
        return redirect(url_for("super_admin_panel"))
    ensure_org()
    org_id = session.get("org_id")
    k = compute_financial_kpis(org_id, user)

    def fin_card(label, value, bg):
        return (
            f'<div class="fin-mini" style="background:{bg}"><span class="fin-mini-k">{label}</span>'
            f'<span class="fin-mini-v">{value}</span></div>'
        )

    fin_row = (
        fin_card("Capital prestado", fmt_money(k["capital_prestado"]), "linear-gradient(135deg,#0d9488,#14b8a6)")
        + fin_card("Capital cobrado", fmt_money(k["capital_cobrado"]), "linear-gradient(135deg,#15803d,#22c55e)")
        + fin_card("Capital pendiente", fmt_money(k["capital_pendiente"]), "linear-gradient(135deg,#1d4ed8,#3b82f6)")
        + fin_card("Interés total", fmt_money(k["interes_total"]), "linear-gradient(135deg,#7c3aed,#a855f7)")
        + fin_card("Interés cobrado", fmt_money(k["interes_cobrado"]), "linear-gradient(135deg,#166534,#4ade80)")
        + fin_card("Interés pendiente", fmt_money(k["interes_pendiente"]), "linear-gradient(135deg,#c2410c,#fb923c)")
        + fin_card("Total por cobrar", fmt_money(k["total_por_cobrar"]), "linear-gradient(135deg,#b91c1c,#ef4444)")
    )

    daily_row = (
        f'<div class="daily-card g"><span class="dk">Cobrado hoy</span><span class="dv">{fmt_money(k["cobrado_hoy"])}</span></div>'
        f'<div class="daily-card g"><span class="dk">Interés hoy</span><span class="dv">{fmt_money(k["interes_hoy"])}</span></div>'
        f'<a class="daily-card r" href="{url_for("bank_late")}" style="text-decoration:none;color:inherit;display:block;cursor:pointer">'
        f'<span class="dk">⚠️ Atrasados</span><span class="dv">{k["atrasados"]}</span></a>'
    )

    def op_tile(href, icon, title, subtitle="", badge=""):
        sub = f'<span class="op-sub">{subtitle}</span>' if subtitle else ""
        bd = f'<span class="op-badge">{badge}</span>' if badge else ""
        return (
            f'<a class="op-tile" href="{href}"><span class="op-ic">{icon}</span><span class="op-tit">{title}</span>{sub}{bd}</a>'
        )

    ops = (
        op_tile(url_for("loans"), "📄", "Préstamos", "", str(k["n_prestamos"]))
        + op_tile(url_for("clients"), "👤", "Clientes", "", str(k["n_clientes"]))
        + op_tile(url_for("new_loan"), "➕", "Nuevo préstamo", "Evaluación crédito", "")
        + op_tile(url_for("credit_history"), "📁", "Historial de crédito", "", "")
        + op_tile(url_for("client_scores"), "⭐", "Score de clientes", "", "")
        + op_tile(url_for("cobro_sabado"), "💰", "Cobro sábado", "", "")
        + op_tile(url_for("bank_late"), "⚠️", "Cuotas atrasadas", "", str(k["atrasados"]))
        + op_tile(url_for("bank_ranking"), "📉", "Ranking morosos", "", "")
        + op_tile(url_for("bank_resumen"), "📊", "Resumen financiero", "", "")
        + op_tile(url_for("check_client"), "🔍", "Consultar por cédula", "", "")
        + op_tile(url_for("prestamos_pagados"), "✅", "Préstamos pagados", "", "")
        + op_tile(url_for("employees"), "👥", "Empleados", "", str(k["n_cobradores"]))
    )

    ctrl = (
        f'<a class="ctrl-big g" href="{url_for("cierre_semanal")}"><span class="c-ic">📅</span> Cierre semanal</a>'
        f'<a class="ctrl-big o" href="{url_for("historial_cierres")}"><span class="c-ic">✔️</span> Cuadres cerrados</a>'
        f'<a class="ctrl-big r" href="{url_for("agregar_dinero_banco")}"><span class="c-ic">🏦</span> Agregar dinero banco</a>'
    )

    extra_ops = ""
    if user.get("role") in ("admin", "supervisor"):
        extra_ops = (
            '<p class="dash-h2" style="margin-top:18px">Administración</p><div class="ops-grid">'
            + op_tile(url_for("users"), "🔑", "Usuarios", "", "")
            + op_tile(url_for("reportes"), "📈", "Reportes", "", "")
            + op_tile(url_for("audit"), "🧾", "Auditoría", "", "")
            + op_tile(url_for("reassign_clients"), "🛣️", "Rutas", "", "")
            + op_tile(url_for("bank_home"), "🏦", "Menú banco", "", "")
            + "</div>"
        )

    body = f"""
<style>
.dash-wrap {{ max-width: 900px; margin: 0 auto; }}
.dash-top {{
  background: linear-gradient(135deg,#14532d,#22c55e);
  color: #fff;
  padding: 20px 16px;
  border-radius: 20px;
  text-align: center;
  margin-bottom: 16px;
  box-shadow: 0 12px 28px rgba(22,163,74,.3);
}}
.dash-top h1 {{ margin: 0; font-size: 1.15rem; font-weight: 900; }}
.dash-top p {{ margin: 6px 0 0; opacity: .95; font-size: 13px; }}
.dash-h2 {{
  font-size: 14px;
  font-weight: 800;
  color: #14532d;
  margin: 16px 0 10px 4px;
  border-left: 4px solid #22c55e;
  padding-left: 8px;
}}
body.theme-dark .dash-h2 {{ color: #86efac; }}
.fin-strip {{
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 8px;
}}
.fin-mini {{
  flex: 1 1 calc(50% - 8px);
  min-width: 140px;
  border-radius: 14px;
  padding: 10px 8px;
  color: #fff;
  text-align: center;
  box-shadow: 0 4px 12px rgba(0,0,0,.12);
}}
@media(min-width:600px){{ .fin-mini{{ flex: 1 1 calc(25% - 8px); }} }}
.fin-mini-k {{ display:block; font-size: 10px; font-weight: 700; opacity: .95; text-transform: uppercase; }}
.fin-mini-v {{ display:block; font-size: 14px; font-weight: 800; margin-top: 4px; }}
.daily-row {{ display: grid; grid-template-columns: repeat(3,1fr); gap: 8px; margin-bottom: 8px; }}
.daily-card {{
  border-radius: 14px;
  padding: 12px 8px;
  text-align: center;
  color: #fff;
  font-weight: 800;
}}
.daily-card.g {{ background: linear-gradient(135deg,#15803d,#4ade80); }}
.daily-card.r {{ background: linear-gradient(135deg,#b91c1c,#f87171); }}
.daily-card .dk {{ display:block; font-size:10px; opacity:.95; }}
.daily-card .dv {{ display:block; font-size:1.1rem; margin-top:4px; }}
.ops-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
  padding: 8px 4px 14px;
}}
.op-tile {{
  width: 100%;
  min-height: 118px;
  border-radius: 16px;
  padding: 10px 6px;
  text-decoration: none !important;
  color: #fff !important;
  text-align: center;
  font-weight: 800;
  font-size: 11px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  box-shadow: 0 6px 16px rgba(0,0,0,.18);
  position: relative;
  box-sizing: border-box;
  transition: transform .12s ease, opacity .12s ease;
}}
.op-tile:hover {{
  transform: translateY(-2px);
  opacity: .98;
}}
.op-tile:focus {{
  outline: 2px solid rgba(255,255,255,.75);
  outline-offset: 2px;
}}
.op-tile:nth-child(6n+1) {{ background: linear-gradient(180deg,#2563eb,#3b82f6); }}
.op-tile:nth-child(6n+2) {{ background: linear-gradient(180deg,#15803d,#22c55e); }}
.op-tile:nth-child(6n+3) {{ background: linear-gradient(180deg,#0d9488,#2dd4bf); }}
.op-tile:nth-child(6n+4) {{ background: linear-gradient(180deg,#65a30d,#84cc16); }}
.op-tile:nth-child(6n+5) {{ background: linear-gradient(180deg,#7c3aed,#a78bfa); }}
.op-tile:nth-child(6n+0) {{ background: linear-gradient(180deg,#c2410c,#fb923c); }}
.op-ic {{ font-size: 22px; margin-bottom: 6px; }}
.op-tit {{ line-height: 1.15; }}
.op-sub {{ font-size: 9px; font-weight: 700; opacity: .9; margin-top: 4px; line-height: 1.1; }}
.op-badge {{
  position: absolute;
  top: 6px; right: 6px;
  background: rgba(0,0,0,.25);
  border-radius: 999px;
  padding: 2px 7px;
  font-size: 10px;
}}
.ctrl-stack {{ display: flex; flex-direction: column; gap: 10px; margin-top: 8px; }}
.ctrl-big {{
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 16px;
  border-radius: 16px;
  color: #fff !important;
  font-weight: 900;
  text-decoration: none !important;
  font-size: 15px;
  box-shadow: 0 8px 20px rgba(0,0,0,.15);
}}
.ctrl-big.g {{ background: linear-gradient(135deg,#15803d,#22c55e); }}
.ctrl-big.o {{ background: linear-gradient(135deg,#c2410c,#f59e0b); }}
.ctrl-big.r {{ background: linear-gradient(135deg,#b91c1c,#ef4444); }}
.c-ic {{ font-size: 22px; }}
</style>

<div class="dash-wrap">
  <div class="dash-top">
    <h1>💵 {APP_BRAND} 💵</h1>
    <p>Panel principal</p>
  </div>

  <p class="dash-h2">Resumen financiero</p>
  <div class="fin-strip">{fin_row}</div>

  <p class="dash-h2">Métricas diarias</p>
  <div class="daily-row">{daily_row}</div>

  <p class="dash-h2">Operaciones</p>
  <div class="ops-grid">{ops}</div>

  <p class="dash-h2">Control</p>
  <div class="ctrl-stack">{ctrl}</div>
  {extra_ops}
</div>
"""
    return page(body, user)


@app.route("/users")
@login_required
@role_required("admin", "supervisor")
def users():
    ensure_org()
    oid = session.get("org_id")
    rows = "".join(
        f"<tr><td>{u['username']}</td><td>{u['role']}</td><td>{u.get('phone') or ''}</td>"
        f"""<td><form method="post" action="{url_for('delete_user', user_id=u['id'])}" style="display:inline" """
        f"""onsubmit="return confirm('¿Eliminar usuario?');"><button class="btn btn-secondary" type="submit">Borrar</button></form></td></tr>"""
        for u in store.users.values()
        if u.get("organization_id") == oid and u.get("role") != "super_admin"
    )
    body = (
        f'<div class="card"><h2>Usuarios</h2><div class="table-scroll"><table><tr><th>Usuario</th><th>Rol</th><th>Tel</th><th></th></tr>{rows}</table></div>'
        f'<a class="btn btn-primary" href="{url_for("new_user")}">Nuevo usuario</a></div>'
    )
    return page(body)


@app.route("/employees")
@login_required
def employees():
    ensure_org()
    org_id = session.get("org_id")
    rows = "".join(
        f"<tr><td>{u['username']}</td><td>{u.get('phone') or '—'}</td><td>{u['role']}</td></tr>"
        for u in store.users.values()
        if u.get("organization_id") == org_id and u.get("role") == "cobrador"
    )
    body = (
        f'<div class="card"><h2>👥 Empleados / cobradores</h2>'
        f'<div class="table-scroll"><table><tr><th>Usuario</th><th>Teléfono</th><th>Rol</th></tr>{rows or "<tr><td colspan=3>Sin cobradores</td></tr>"}</table></div>'
        f"{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/users/new", methods=["GET", "POST"])
@login_required
@admin_required
def new_user():
    ensure_org()
    if request.method == "POST":
        if request.form.get("pin") != ADMIN_PIN:
            flash("PIN incorrecto.", "danger")
            return redirect(url_for("new_user"))
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        role = request.form.get("role") or "cobrador"
        phone = (request.form.get("phone") or "").strip()
        if not username or not password:
            flash("Datos incompletos.", "danger")
            return redirect(url_for("new_user"))
        if role == "admin":
            flash("No puede crear usuarios con rol 'admin'.", "danger")
            return redirect(url_for("new_user"))
        if any(u["username"] == username for u in store.users.values()):
            flash("Usuario ya existe.", "danger")
            return redirect(url_for("new_user"))
        org_id = session.get("org_id")
        uid = store.nid("users")
        status = ACCOUNT_PENDING if role == "cobrador" else ACCOUNT_ACTIVE

        # Fechas de acceso (solo cobradores). Se guardan incluso si queda pendiente.
        fecha_inicio = None
        fecha_fin = None
        if role == "cobrador":
            raw_inicio = (request.form.get("fecha_inicio") or "").strip()
            raw_fin = (request.form.get("fecha_fin") or "").strip()
            today = date.today()
            default_inicio = today
            default_fin = today + timedelta(days=30)
            try:
                fecha_inicio = (
                    datetime.strptime(raw_inicio, "%Y-%m-%d").date() if raw_inicio else default_inicio
                )
                fecha_fin = datetime.strptime(raw_fin, "%Y-%m-%d").date() if raw_fin else default_fin
            except ValueError:
                flash("Fechas inválidas. Usa el selector de fechas.", "danger")
                return redirect(url_for("new_user"))
            if fecha_fin < fecha_inicio:
                flash("La fecha fin no puede ser menor que la fecha inicio.", "danger")
                return redirect(url_for("new_user"))

        store.users[uid] = {
            "id": uid, "username": username, "password_hash": generate_password_hash(password),
            "role": role,
            "phone": phone,
            "organization_id": org_id,
            "created_at": datetime.utcnow(),
            "name": None,
            "account_status": status,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
        }
        flash("Usuario creado.", "success")
        return redirect(url_for("users"))
    body = (
        f'<div class="card"><h2>Nuevo usuario</h2><form method="post">'
        f'<label>Usuario</label><input name="username" required>'
        f'<label>Contraseña</label><input type="password" name="password" required>'
        f'<label>Teléfono</label><input name="phone">'
        f'<label>Rol</label><select name="role"><option value="cobrador">Cobrador (queda pendiente)</option><option value="supervisor">Supervisor</option></select>'
        f'<label>Fecha inicio (cobradores)</label><input name="fecha_inicio" type="date" value="{date.today().strftime("%Y-%m-%d")}">'
        f'<label>Fecha fin (cobradores)</label><input name="fecha_fin" type="date" value="{(date.today()+timedelta(days=30)).strftime("%Y-%m-%d")}">'
        f'<label>PIN admin</label><input name="pin" required>'
        f'<button class="btn btn-primary" type="submit">Crear</button></form></div>'
    )
    return page(body)


@app.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_user(user_id):
    if user_id == session.get("user_id"):
        flash("No puede borrarse a sí mismo.", "danger")
        return redirect(url_for("users"))
    ensure_org()
    oid = session.get("org_id")
    u = store.users.get(user_id)
    if not u or u.get("organization_id") != oid:
        flash("Sin acceso para eliminar ese usuario.", "danger")
        return redirect(url_for("users"))
    store.users.pop(user_id, None)
    flash("Usuario eliminado.", "success")
    return redirect(url_for("users"))


@app.route("/reassign", methods=["GET", "POST"])
@login_required
@role_required("admin", "supervisor")
def reassign_clients():
    if request.method == "POST":
        flash("Reasignación guardada.", "info")
        return redirect(url_for("clients"))
    ensure_org()
    oid = session.get("org_id")
    opts = "".join(
        f"<option value='{u['id']}'>{u['username']}</option>"
        for u in store.users.values()
        if u.get("role") == "cobrador" and u.get("organization_id") == oid
    )
    body = f'<div class="card"><h2>Reasignar rutas</h2><select>{opts}</select><p><a class="btn btn-secondary" href="{url_for("clients")}">Volver</a></p></div>'
    return page(body)


@app.route("/clients/<int:client_id>/reassign", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def reassign_single_client(client_id):
    ensure_org()
    oid = session.get("org_id")
    c = store.clients.get(client_id)
    if not c or c.get("organization_id") != oid:
        flash("Cliente no encontrado.", "danger")
        return redirect(url_for("clients"))

    collector_id = request.form.get("collector_id", type=int)
    if not collector_id:
        flash("Seleccione un cobrador.", "danger")
        return redirect(url_for("client_detail", client_id=client_id))

    new_collector = store.users.get(collector_id)
    if not new_collector or new_collector.get("role") != "cobrador" or new_collector.get("organization_id") != oid:
        flash("Cobrador inválido.", "danger")
        return redirect(url_for("client_detail", client_id=client_id))

    # Reasignar el "dueño" del cliente y también los préstamos existentes.
    c["created_by"] = collector_id
    for L in store.loans.values():
        if L.get("client_id") == client_id and L.get("organization_id") == oid:
            L["created_by"] = collector_id

    flash("Cliente y préstamos reasignados.", "success")
    return redirect(url_for("client_detail", client_id=client_id))


@app.route("/clients")
@login_required
def clients():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    rows = clients_for_user(org_id, user)
    t = "".join(
        f"<tr><td>{c['first_name']} {c.get('last_name') or ''}</td><td>{c.get('phone') or ''}</td>"
        f"<td><a class='btn btn-secondary' href='{url_for('client_detail', client_id=c['id'])}'>Ver</a></td></tr>"
        for c in rows
    )
    body = (
        f'<div class="card"><h2>Clientes</h2><div class="table-scroll"><table><tr><th>Nombre</th><th>Tel</th><th></th></tr>{t}</table></div>'
        f'<a class="btn btn-primary" href="{url_for("new_client")}">Nuevo cliente</a></div>'
    )
    return page(body)


@app.route("/clients/new", methods=["GET", "POST"])
@login_required
def new_client():
    user = current_user()
    ensure_org()
    if request.method == "POST":
        first = (request.form.get("first_name") or "").strip()
        if not first:
            flash("Nombre obligatorio.", "danger")
            return redirect(url_for("new_client"))
        cid = store.nid("clients")
        store.clients[cid] = {
            "id": cid,
            "first_name": first,
            "last_name": (request.form.get("last_name") or "").strip(),
            "document_id": (request.form.get("document_id") or "").strip(),
            "phone": (request.form.get("phone") or "").strip(),
            "address": (request.form.get("address") or "").strip(),
            "route": (request.form.get("route") or "").strip(),
            "created_by": user["id"],
            "organization_id": session.get("org_id"),
            "created_at": datetime.utcnow(),
        }
        flash("Cliente creado.", "success")
        return redirect(url_for("clients"))
    body = (
        f'<div class="card"><h2>Nuevo cliente</h2><form method="post">'
        f'<label>Nombre</label><input name="first_name" required>'
        f'<label>Apellido</label><input name="last_name">'
        f'<label>Documento</label><input name="document_id">'
        f'<label>Teléfono</label><input name="phone">'
        f'<label>Dirección</label><input name="address">'
        f'<label>Ruta</label><input name="route">'
        f'<button class="btn btn-primary" type="submit">Guardar</button></form></div>'
    )
    return page(body)


@app.route("/clients/<int:client_id>/edit", methods=["GET", "POST"])
@login_required
def edit_client(client_id):
    c = store.clients.get(client_id)
    if not c:
        flash("No encontrado.", "danger")
        return redirect(url_for("clients"))
    user = current_user()
    if user["role"] == "cobrador" and c.get("created_by") != user["id"] and not is_cartera_admin(user):
        flash("Sin acceso.", "danger")
        return redirect(url_for("clients"))
    if request.method == "POST":
        c["first_name"] = (request.form.get("first_name") or "").strip()
        c["last_name"] = (request.form.get("last_name") or "").strip()
        c["phone"] = (request.form.get("phone") or "").strip()
        c["address"] = (request.form.get("address") or "").strip()
        c["document_id"] = (request.form.get("document_id") or "").strip()
        c["route"] = (request.form.get("route") or "").strip()
        flash("Guardado.", "success")
        return redirect(url_for("client_detail", client_id=client_id))
    fn = c.get("first_name") or ""
    body = (
        f'<div class="card"><h2>Editar cliente</h2><form method="post">'
        f'<label>Nombre</label><input name="first_name" value="{fn}" required>'
        f'<label>Apellido</label><input name="last_name" value="{c.get("last_name") or ""}">'
        f'<label>Teléfono</label><input name="phone" value="{c.get("phone") or ""}">'
        f'<label>Dirección</label><input name="address" value="{c.get("address") or ""}">'
        f'<label>Documento</label><input name="document_id" value="{c.get("document_id") or ""}">'
        f'<label>Ruta</label><input name="route" value="{c.get("route") or ""}">'
        f'<button class="btn btn-primary" type="submit">Guardar</button></form></div>'
    )
    return page(body)


@app.route("/clients/<int:client_id>/delete", methods=["POST"])
@login_required
def delete_client(client_id):
    store.clients.pop(client_id, None)
    for lid in [x for x, L in store.loans.items() if L.get("client_id") == client_id]:
        store.loans.pop(lid, None)
    flash("Cliente eliminado.", "success")
    return redirect(url_for("clients"))


@app.route("/clients/<int:client_id>")
@login_required
def client_detail(client_id):
    ensure_org()
    c = store.clients.get(client_id)
    if not c:
        flash("No encontrado.", "danger")
        return redirect(url_for("clients"))
    user = current_user()
    can_admin = user.get("role") == "admin" or is_cartera_admin(user)
    if user["role"] == "cobrador" and c.get("created_by") != user["id"] and not is_cartera_admin(user):
        flash("Sin acceso.", "danger")
        return redirect(url_for("clients"))
    org_id = session.get("org_id")
    sd = calc_client_score(client_id, org_id)
    mx = calc_max_credito(sd["prestamos_pagados"], sd["score"])
    score_html = (
        f'<div class="card" style="margin-bottom:12px;">'
        f'<h3 style="margin-top:0;margin-bottom:8px">🔎 Score de crédito</h3>'
        f'<p style="margin:0;"><b>Score:</b> {sd["score"]} &nbsp; <b>Nivel:</b> {sd["nivel"]}</p>'
        f'<p style="margin:6px 0 0 0;"><b>Préstamos pagados:</b> {sd["prestamos_pagados"]} &nbsp; <b>Atrasos:</b> {sd["atrasos"]}</p>'
        f'<p style="margin:10px 0 0 0;"><b>Crédito recomendado:</b> {fmt_money(mx)}</p>'
        f'</div>'
    )
    loans = [L for L in store.loans.values() if L.get("client_id") == client_id]

    def fmt_date(d):
        return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)

    def fmt_date_ddmmyyyy(d):
        return d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(d)

    # Tabla de préstamos (alineada al formato de tu otro sistema).
    loan_rows = []
    for L in sorted(loans, key=lambda x: -x.get("id", 0)):
        paid_interest = round(
            sum(float(p.get("interest") or 0) for p in store.payments.values() if p.get("loan_id") == L.get("id")),
            2,
        )
        upfront_pct = float(L.get("upfront_percent") or 0.0)
        inicio = fmt_date_ddmmyyyy(L.get("start_date"))

        del_form = (
            f"<form method='post' action='{url_for('delete_loan', loan_id=L['id'])}' "
            f"style='margin:0' onsubmit=\"return confirm('¿Eliminar préstamo #{L['id']}?');\">"
            f"<button class='btn btn-secondary' type='submit' style='padding:6px 10px'>Eliminar</button>"
            f"</form>"
        )

        if can_admin:
            actions = (
                f"<a class='btn btn-secondary' style='padding:6px 10px' href='{url_for('loan_detail', loan_id=L['id'])}'>Ver</a> "
                f"<a class='btn btn-primary' style='padding:6px 10px' href='{url_for('edit_loan', loan_id=L['id'])}'>Editar</a> "
                f"{del_form}"
            )
        else:
            actions = (
                f"<a class='btn btn-secondary' style='padding:6px 10px' href='{url_for('loan_detail', loan_id=L['id'])}'>Ver</a>"
            )

        loan_rows.append(
            "<tr>"
            f"<td>{L['id']}</td>"
            f"<td>{fmt_money(L.get('amount'))}</td>"
            f"<td>{fmt_money(L.get('remaining'))}</td>"
            f"<td>{upfront_pct:.2f}%</td>"
            f"<td>{L.get('frequency') or '—'}</td>"
            f"<td>{inicio}</td>"
            f"<td>{fmt_money(paid_interest)}</td>"
            f"<td>{L.get('status') or '—'}</td>"
            f"<td>{actions}</td>"
            "</tr>"
        )

    lr = "".join(loan_rows) if loan_rows else "<tr><td colspan='9' style='opacity:.85; text-align:center'>Sin préstamos</td></tr>"

    # Dropdown para reasignar cobrador.
    collectors_opts = "".join(
        f"<option value='{u['id']}'{' selected' if u['id'] == c.get('created_by') else ''}>{u.get('username')}</option>"
        for u in store.users.values()
        if u.get("role") == "cobrador" and u.get("organization_id") == org_id
    )

    header = (
        f'<div class="card" style="margin-bottom:12px;">'
        f'<h2 style="margin-top:0;margin-bottom:10px">{c["first_name"]} {c.get("last_name") or ""}</h2>'
        f"<p style='margin:6px 0;'><b>Tel:</b> {c.get('phone') or '—'}</p>"
        f"<p style='margin:6px 0;'><b>Dirección:</b> {c.get('address') or '—'}</p>"
        f"<p style='margin:6px 0;'><b>Documento:</b> {c.get('document_id') or '—'}</p>"
        f"<p style='margin:6px 0;'><b>Ruta:</b> {c.get('route') or '—'}</p>"
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;margin-bottom:10px;">'
        f'<a class="btn btn-secondary" href="{url_for("edit_client", client_id=client_id)}">Editar</a> '
        f'<form method="post" action="{url_for("delete_client", client_id=client_id)}" style="margin:0" onsubmit="return confirm(\'¿Borrar cliente?\');">'
        f'<button class="btn btn-secondary" type="submit">Eliminar</button></form>'
        f"</div>"
        f"<div style='border-top:1px solid rgba(148,163,184,.35); padding-top:10px;'>"
        f"<form method='post' action='{url_for('reassign_single_client', client_id=client_id)}' style='display:flex;gap:8px;flex-wrap:wrap;align-items:center;'>"
        f"<label style='margin:0;'><b>Reasignar cobrador:</b></label>"
        f"<select name='collector_id'>{collectors_opts}</select>"
        f"<button class='btn btn-primary' type='submit'>Mover cliente</button>"
        f"</form>"
        f"</div>"
        f"</div>"
    )

    body = (
        header
        + score_html
        + f'<div class="card">'
        f'<a class="btn btn-primary" href="{url_for("new_loan")}?client_id={client_id}">Nuevo préstamo</a>'
        f'<div class="table-scroll" style="margin-top:12px;">'
        f"<table><tr><th>ID</th><th>Monto</th><th>Restante</th><th>%</th><th>Frecuencia</th><th>Inicio</th><th>Interés pagado</th><th>Estado</th><th>Acciones</th></tr>{lr}</table>"
        f"</div></div>"
    )
    return page(body)


@app.route("/loans")
@login_required
def loans():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    rows_all = loans_for_user(org_id, user)
    show_filter = user.get("role") in ("admin", "supervisor") or is_cartera_admin(user)
    filter_id = request.args.get("prestamista", type=int)

    if show_filter and filter_id:
        rows = [L for L in rows_all if L.get("created_by") == filter_id]
    else:
        rows = list(rows_all)

    by_coll = {}
    for L in rows_all:
        uid = L.get("created_by")
        by_coll.setdefault(uid, []).append(L)

    def prestamista_name(uid):
        if uid is None:
            return "—"
        u = store.users.get(uid, {})
        return u.get("name") or u.get("username") or f"#{uid}"

    sum_rows = []
    for uid in sorted(by_coll.keys(), key=lambda i: str(prestamista_name(i)).lower()):
        lg = by_coll[uid]
        n = len(lg)
        pagados = sum(
            1 for x in lg if str(x.get("status", "")).upper() == "CERRADO" or float(x.get("remaining") or 0) <= 0
        )
        total_prest = sum(float(x.get("amount") or 0) for x in lg)
        cap_activo = sum(float(x.get("remaining") or 0) for x in lg if str(x.get("status", "")).upper() == "ACTIVO")
        nm = html.escape(str(prestamista_name(uid)))
        sum_rows.append(
            "<tr>"
            f"<td>{nm}</td><td>{n}</td><td>{pagados}</td>"
            f"<td>{html.escape(fmt_money(total_prest))}</td>"
            f"<td>{html.escape(fmt_money(cap_activo))}</td>"
            "</tr>"
        )
    sum_body = "".join(sum_rows) if sum_rows else "<tr><td colspan='5' style='text-align:center;opacity:.85'>Sin préstamos</td></tr>"

    ids_loans = {L.get("created_by") for L in rows_all if L.get("created_by")}
    collector_map = {}
    for u in store.users.values():
        if u.get("organization_id") != org_id:
            continue
        if u.get("role") == "cobrador" or u["id"] in ids_loans:
            collector_map[u["id"]] = u
    collector_options = sorted(
        collector_map.values(), key=lambda u: str(u.get("name") or u.get("username") or "").lower()
    )
    opt_parts = ['<option value="">-- TODOS --</option>']
    for u in collector_options:
        sel = " selected" if filter_id == u["id"] else ""
        lab = html.escape(str(u.get("name") or u.get("username") or ""))
        opt_parts.append(f'<option value="{u["id"]}"{sel}>{lab}</option>')
    opts_html = "".join(opt_parts)

    filter_block = ""
    if show_filter:
        filter_block = (
            f'<form class="loans-filter-form" method="get" action="{url_for("loans")}">'
            f'<span class="loans-filter-lbl">👤 Ver préstamos por prestamista</span>'
            f'<select name="prestamista" class="loans-filter-select" onchange="this.form.submit()">{opts_html}</select>'
            f"</form>"
        )

    cards = []
    for L in sorted(rows, key=lambda x: -x["id"]):
        cid = L.get("client_id")
        cl = store.clients.get(cid, {})
        nm_raw = f"{cl.get('first_name','')} {cl.get('last_name') or ''}".strip() or f"Cliente #{cid}"
        nm = html.escape(nm_raw)
        freq = loan_frequency_label(L.get("frequency"))
        st = str(L.get("status") or "").upper()
        if st == "CERRADO" or float(L.get("remaining") or 0) <= 0:
            badge = '<span class="loan-card-badge loan-card-badge-done">Cerrado</span>'
        else:
            badge = '<span class="loan-card-badge">Activo</span>'
        sub = html.escape(f"Préstamo #{L['id']} - {freq}")
        amt = html.escape(fmt_money(L.get("remaining")))
        href = url_for("loan_detail", loan_id=L["id"])
        cards.append(
            f'<a class="loan-card-link" href="{href}">'
            f'<div class="loan-card">'
            f'<div class="loan-card-ic" aria-hidden="true">💵</div>'
            f'<div class="loan-card-mid">'
            f'<div class="loan-card-name">{nm}</div>'
            f'<div class="loan-card-sub">{sub}</div>'
            f"{badge}"
            f"</div>"
            f'<div class="loan-card-amt">{amt}</div>'
            f"</div></a>"
        )
    cards_html = "".join(cards) if cards else '<p class="loans-empty">No hay préstamos en esta vista.</p>'

    body = f"""
<style>
.loans-page-wrap {{ max-width: 720px; margin: 0 auto; }}
.loans-page-title {{
  text-align: center;
  font-size: 1.35rem;
  font-weight: 900;
  color: #14532d;
  margin: 8px 0 16px;
}}
.loans-toolbar {{
  display: flex;
  flex-direction: column;
  gap: 14px;
  margin-bottom: 18px;
}}
.btn-loan-new {{
  display: inline-flex;
  align-items: center;
  gap: 8px;
  align-self: flex-start;
  padding: 12px 20px;
  border-radius: 999px;
  font-weight: 900;
  font-size: 15px;
  text-decoration: none !important;
  color: #fff !important;
  background: linear-gradient(135deg,#15803d,#22c55e);
  box-shadow: 0 8px 22px rgba(22,163,74,.35);
}}
.btn-loan-new:hover {{ filter: brightness(1.05); }}
.loans-filter-form {{
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  font-size: 14px;
  color: #166534;
  font-weight: 600;
}}
.loans-filter-select {{
  padding: 8px 12px;
  border-radius: 12px;
  border: 1px solid rgba(22,101,52,.25);
  background: #fff;
  font-weight: 600;
  min-width: 200px;
}}
.loans-summary {{
  background: rgba(255,255,255,.92);
  border-radius: 18px;
  padding: 16px;
  margin-bottom: 20px;
  border: 1px solid rgba(22,163,74,.15);
  box-shadow: 0 6px 20px rgba(0,0,0,.06);
}}
.loans-summary h3 {{
  margin: 0 0 12px 0;
  font-size: 1.05rem;
  font-weight: 900;
  color: #14532d;
}}
.loans-summary table {{ width: 100%; font-size: 13px; }}
.loans-summary th {{
  text-align: left;
  padding: 8px 6px;
  color: #14532d;
  border-bottom: 2px solid rgba(22,163,74,.2);
}}
.loans-summary td {{ padding: 8px 6px; border-bottom: 1px solid rgba(148,163,184,.25); }}
.loan-card-link {{ text-decoration: none !important; color: inherit; display: block; }}
.loan-card {{
  display: flex;
  align-items: center;
  gap: 14px;
  background: #fff;
  border-radius: 18px;
  padding: 16px 18px;
  margin-bottom: 12px;
  box-shadow: 0 8px 24px rgba(0,0,0,.07);
  border: 1px solid rgba(22,163,74,.12);
  transition: transform .12s ease, box-shadow .12s ease;
}}
.loan-card:hover {{ transform: translateY(-2px); box-shadow: 0 12px 28px rgba(0,0,0,.09); }}
.loan-card-ic {{ font-size: 28px; line-height: 1; flex-shrink: 0; }}
.loan-card-mid {{ flex: 1; min-width: 0; }}
.loan-card-name {{ font-weight: 900; font-size: 16px; color: #0f172a; }}
.loan-card-sub {{ font-size: 13px; color: #64748b; margin-top: 4px; }}
.loan-card-badge {{
  display: inline-block;
  margin-top: 8px;
  padding: 4px 12px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 800;
  background: #dc2626;
  color: #fff;
  letter-spacing: .02em;
}}
.loan-card-badge-done {{ background: #16a34a; }}
.loan-card-amt {{ font-weight: 900; font-size: 17px; color: #15803d; white-space: nowrap; flex-shrink: 0; }}
.loans-empty {{ text-align: center; opacity: .85; padding: 24px; }}
</style>
<div class="loans-page-wrap">
  <h1 class="loans-page-title">📋 Lista de Préstamos</h1>
  <div class="loans-toolbar">
    <a class="btn-loan-new" href="{url_for("new_loan")}">👤➕ Nuevo préstamo</a>
    {filter_block}
  </div>
  <div class="loans-summary">
    <h3>📌 Resumen por prestamista</h3>
    <div class="table-scroll">
      <table>
        <tr><th>Prestamista</th><th># Préstamos</th><th>Pagados</th><th>Total prestado</th><th>Capital activo</th></tr>
        {sum_body}
      </table>
    </div>
  </div>
  <div class="loans-cards">{cards_html}</div>
</div>
"""
    return page(body)


@app.route("/loans/new", methods=["GET", "POST"])
@login_required
def new_loan():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    clist = clients_for_user(org_id, user)
    if request.method == "POST":
        client_id = request.form.get("client_id", type=int)
        amount = request.form.get("amount", type=float)
        rate = request.form.get("rate", type=float) or 0
        upfront_percent = request.form.get("upfront_percent", type=float) or 0
        upfront_percent = max(0.0, min(float(upfront_percent), 100.0))
        freq = request.form.get("frequency") or "semanal"
        start_str = (request.form.get("start_date") or "").strip()
        term_count = request.form.get("term_count", type=int) or 1
        if not client_id or amount is None or not start_str:
            flash("Complete cliente, monto y fecha.", "danger")
            return redirect(url_for("new_loan"))
        if amount <= 0:
            flash("El monto debe ser mayor que 0.", "danger")
            return redirect(url_for("new_loan"))
        if rate < 0:
            flash("La tasa no puede ser negativa.", "danger")
            return redirect(url_for("new_loan"))
        if term_count < 1:
            flash("Cuotas inválidas.", "danger")
            return redirect(url_for("new_loan"))
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        interval_days = freq_interval_days(freq)
        next_payment_date = start_date + timedelta(days=interval_days)

        discount_amount = round(amount * upfront_percent / 100.0, 2)
        monto_entregado = round(amount - discount_amount, 2)
        if monto_entregado < 0:
            monto_entregado = 0.0

        total_interest = round((amount * rate / 100) * term_count, 2)
        total_to_pay = round(monto_entregado + total_interest, 2)
        installment_amount = round(total_to_pay / max(term_count, 1), 2)

        discount_cash_id = None
        disbursement_cash_id = None

        # 1) Descuento inicial entra al sistema y SUMA al banco.
        # 2) Se entrega al cliente SOLO el monto neto (amount - descuento) y RESTA del banco.
        try:
            if discount_amount > 0:
                discount_cash_id = apply_cash_movement(
                    movement_type="descuento_inicial",
                    amount=discount_amount,
                    note=f"Descuento inicial préstamo cliente #{client_id}",
                    user_id=user["id"],
                    org_id=org_id,
                )
            if monto_entregado > 0:
                disbursement_cash_id = apply_cash_movement(
                    movement_type="prestamo_entregado",
                    amount=-monto_entregado,
                    note=f"Préstamo entregado cliente #{client_id}",
                    user_id=user["id"],
                    org_id=org_id,
                )
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("new_loan"))

        lid = store.nid("loans")
        store.loans[lid] = {
            "id": lid,
            "client_id": client_id,
            "amount": amount,  # capital aprobado
            "rate": rate,
            "frequency": freq,
            "start_date": start_date,
            "next_payment_date": next_payment_date,
            "created_by": user["id"],
            "remaining": monto_entregado,  # capital pendiente (neto entregado)
            "remaining_capital": monto_entregado,
            # IDs del libro de movimientos para poder corregir descuento.
            "discount_cash_report_id": discount_cash_id,
            "disbursement_cash_report_id": disbursement_cash_id,
            "total_interest_paid": 0,
            "status": "ACTIVO",
            "term_count": term_count,
            "organization_id": org_id,
            "total_interest": total_interest,
            "total_to_pay": total_to_pay,
            "upfront_percent": upfront_percent,
            "installment_amount": installment_amount,
            "signature_b64": None,
            "id_photo_b64": None,
            "id_photo_back_b64": None,
        }
        flash("Préstamo creado.", "success")
        return redirect(url_for("loan_detail", loan_id=lid))
    opts = "".join(f"<option value='{c['id']}'{' selected' if request.args.get('client_id', type=int)==c['id'] else ''}>{c['first_name']}</option>" for c in clist)
    body = (
        f'<div class="card"><h2>Nuevo préstamo</h2>'
        f'<p style="margin-top:6px; opacity:.9;"><b>Banco disponible:</b> {fmt_money(get_bank_available(org_id))}</p>'
        f'<form method="post">'
        f'<label>Cliente</label><select name="client_id" required>{opts}</select>'
        f'<label>Monto</label><input name="amount" type="number" step="0.01" required>'
        f'<label>Tasa %</label><input name="rate" type="number" step="0.01" value="10">'
        f'<label>Descuento inicial (%)</label><input name="upfront_percent" type="number" step="0.01" value="0" min="0" max="100">'
        f'<label>Frecuencia</label><select name="frequency"><option>semanal</option><option>diario</option><option>quincenal</option><option>mensual</option></select>'
        f'<label>Inicio</label><input name="start_date" type="date" value="{date.today().isoformat()}" required>'
        f'<label>Cuotas</label><input name="term_count" type="number" value="10" min="1">'
        f'<button class="btn btn-primary" type="submit">Crear</button></form></div>'
    )
    return page(body)


@app.route("/loans/<int:loan_id>/delete", methods=["POST"])
@login_required
def delete_loan(loan_id):
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    L = store.loans.get(loan_id)
    if not L or L.get("organization_id") != oid:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))
    if user.get("role") != "admin" and not is_cartera_admin(user):
        flash("Solo admin puede borrar préstamos.", "danger")
        return redirect(url_for("loans"))

    try:
        # 1) Revertir pagos asociados (no podemos borrar cash_reports porque no guardamos sus IDs en el payment).
        payments = [p for p in store.payments.values() if p.get("loan_id") == loan_id]
        for p in payments:
            amt = float(p.get("amount") or 0)
            if abs(amt) < 1e-9:
                continue
            apply_cash_movement(
                movement_type="reverso_pago_prestamo",
                amount=-amt,
                note=f"Reverso por eliminación préstamo #{loan_id} (pago #{p.get('id')})",
                user_id=user["id"],
                org_id=oid,
            )
            store.payments.pop(p.get("id"), None)

        # 2) Revertir movimientos iniciales del préstamo (aquí sí tenemos los IDs en el loan).
        disc_id = L.get("discount_cash_report_id")
        if disc_id is not None:
            store.cash_reports.pop(disc_id, None)
        disb_id = L.get("disbursement_cash_report_id")
        if disb_id is not None:
            store.cash_reports.pop(disb_id, None)

        store.loans.pop(loan_id, None)
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    flash("Préstamo eliminado y movimientos del banco revertidos.", "success")
    return redirect(url_for("loans"))


@app.route("/loans/<int:loan_id>/edit", methods=["GET", "POST"])
@login_required
def edit_loan(loan_id):
    L = store.loans.get(loan_id)
    if not L:
        flash("No encontrado.", "danger")
        return redirect(url_for("loans"))
    user = current_user()
    if user.get("role") != "admin" and not is_cartera_admin(user):
        flash("Solo admin puede editar préstamos.", "danger")
        return redirect(url_for("loan_detail", loan_id=loan_id))
    if request.method == "POST":
        L["rate"] = request.form.get("rate", type=float) or L.get("rate")
        L["remaining"] = request.form.get("remaining", type=float)
        flash("Actualizado.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))
    body = (
        f'<div class="card"><h2>Editar préstamo #{loan_id}</h2><form method="post">'
        f'<label>Tasa</label><input name="rate" value="{L.get("rate")}" type="number" step="0.01">'
        f'<label>Saldo</label><input name="remaining" value="{L.get("remaining")}" type="number" step="0.01">'
        f'<button class="btn btn-primary" type="submit">Guardar</button></form></div>'
    )
    return page(body)


@app.route("/loan/<int:loan_id>")
@login_required
def loan_detail(loan_id):
    L = store.loans.get(loan_id)
    if not L:
        flash("No encontrado.", "danger")
        return redirect(url_for("loans"))
    client_id = L.get("client_id")
    client = store.clients.get(client_id, {})
    pays = [p for p in store.payments.values() if p.get("loan_id") == loan_id]
    u = current_user()
    can_admin = u.get("role") == "admin" or is_cartera_admin(u)
    edit_btn = (
        f'<a class="btn btn-secondary" href="{url_for("edit_loan", loan_id=loan_id)}">✏️ Editar</a>'
        if can_admin
        else ""
    )

    # =========================
    # Métricas/Resumen
    # =========================
    capital_aprobado = float(L.get("amount") or 0)
    upfront_percent = float(L.get("upfront_percent") or 0)
    descuento_inicial = round(capital_aprobado * upfront_percent / 100.0, 2)
    monto_entregado = round(capital_aprobado - descuento_inicial, 2)
    rate = float(L.get("rate") or 0)
    interes_total = round(float(L.get("total_interest") or 0), 2)
    total_a_pagar = round(float(L.get("total_to_pay") or (capital_aprobado + interes_total)), 2)

    # Cuotas estimadas
    term_count = int(L.get("term_count") or 1)
    pagos_cuotas = [p for p in pays if (p.get("type") or "").lower() == "cuota"]
    pagadas = len(pagos_cuotas)
    restantes = max(term_count - pagadas, 0)
    cuota_label = L.get("frequency") or "semanal"

    # Siguiente pago (simplificado)
    next_pago = L.get("next_payment_date") or date.today()
    def fmt_date(d):
        return d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(d)
    next_pago_disp = fmt_date(next_pago)

    estado_pago = "Pendiente"
    if str(L.get("status") or "").lower() == "cerrado" or float(L.get("remaining") or 0) <= 0:
        estado_pago = "Cerrado"

    # Etiqueta % sin decimales cuando sea entero (ej: 0% en vez de 0.0%)
    if abs(upfront_percent - round(upfront_percent)) < 1e-9:
        upfront_pct_label = f"{int(round(upfront_percent))}%"
    else:
        upfront_pct_label = f"{upfront_percent:.1f}%"

    # Nombre legible para frecuencia
    freq = str(L.get("frequency") or "").strip().lower()
    if "quinc" in freq:
        freq_label = "quincena"
    elif "seman" in freq:
        freq_label = "semana"
    elif "diar" in freq:
        freq_label = "día"
    elif "mens" in freq:
        freq_label = "mes"
    else:
        freq_label = freq or "cuota"

    # Monto programado de la cuota (para que salga aun si no hay pagos)
    scheduled_payment = float(L.get("installment_amount") or 0)
    if scheduled_payment <= 0 and term_count:
        scheduled_payment = float(total_a_pagar) / float(term_count)

    # Historial tabla
    hist_rows = ""
    for idx, p in enumerate(sorted(pays, key=lambda x: (x.get("date") or date.min)), start=1):
        hist_rows += (
            f"<tr>"
            f"<td>{idx}</td>"
            f"<td>{fmt_money(p.get('amount'))}</td>"
            f"<td>{fmt_money(p.get('capital'))}</td>"
            f"<td>{fmt_money(p.get('interest'))}</td>"
            f"<td>{p.get('date')}</td>"
            f"<td><a class='btn btn-secondary' style='padding:6px 10px' href='{url_for('print_payment', payment_id=p.get('id'))}' target='_blank' rel='noopener'>Imprimir recibo</a></td>"
            f"</tr>"
        )

    if not hist_rows:
        hist_rows = (
            "<tr><td colspan='6' style='opacity:.85; text-align:center;'>Sin pagos</td></tr>"
        )

    # =========================
    # Calendario de cuotas (10/12 semanas)
    # =========================
    interval_days = freq_interval_days(L.get("frequency"))
    first_due = next_pago if hasattr(next_pago, "strftime") else date.today()

    cuota_items_html = ""
    for i in range(1, term_count + 1):
        due_i = first_due + timedelta(days=interval_days * (i - 1))
        cuota_estado = "Pagada" if i <= pagadas else "Pendiente"
        color = "#16a34a" if i <= pagadas else "#d97706"
        cuota_items_html += (
            f"<div class='card' style='margin:0; padding:12px; min-width:170px; flex:1 1 170px; background:#f8fafc;'>"
            f"<div style='font-weight:900; margin-bottom:6px;'>Cuota {i}</div>"
            f"<div><b>{fmt_date(due_i)}</b></div>"
            f"<div style='margin-top:6px; opacity:.9; font-weight:900;'>Estado: <span style='color:{color}'>{cuota_estado}</span></div>"
            f"</div>"
        )

    # Siguiente pago = primera cuota pendiente
    if pagadas < term_count:
        next_idx = pagadas + 1
        next_due = first_due + timedelta(days=interval_days * (next_idx - 1))
        next_status = "Pendiente"
    else:
        next_idx = term_count
        next_due = first_due + timedelta(days=interval_days * (term_count - 1))
        next_status = "Cerrado"

    siguiente_pago_html = (
        f"<div style='margin-top:12px;'>"
        f"<div style='font-weight:900; margin-bottom:6px;'>Siguiente pago</div>"
        f"<div><b>Cuota {next_idx}</b></div>"
        f"<div style='margin-top:6px; opacity:.95;'>Fecha: <b>{fmt_date(next_due)}</b></div>"
        f"<div style='margin-top:6px; opacity:.95;'>Monto: <b>{fmt_money(scheduled_payment)}</b></div>"
        f"<div style='margin-top:6px; opacity:.95;'>Estado: <b style='color:#d97706'>{next_status}</b></div>"
        f"</div>"
    )

    # =========================
    # WhatsApp
    # =========================
    phone = (client.get("phone") or "").strip()
    wa_target = phone if phone else ADMIN_WHATSAPP
    wa_text = (
        f"Hola {client.get('first_name') or 'cliente'} 👋\n"
        f"Le recordamos su próximo pago del préstamo #{loan_id}.\n"
        f"Fecha: {next_pago}\n"
        f"Estado: {estado_pago}"
    )
    from urllib.parse import quote_plus
    wa_link = f"https://wa.me/{wa_target}?text={quote_plus(wa_text)}"

    body = f"""
<style>
.loan-wrap {{ padding: 8px 0; }}
.loan-title {{ margin: 0 0 8px 0; font-size: 1.35rem; font-weight: 900; }}
.loan-sub {{ opacity: .9; margin: 0 0 12px 0; font-size: 13px; }}
.loan-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }}
.loan-card {{
  border-radius: 16px; padding: 12px 10px; color: #fff; box-shadow: 0 8px 20px rgba(0,0,0,.10);
  min-height: 76px; box-sizing: border-box;
}}
.loan-k {{ display:block; font-size: 10px; font-weight: 900; opacity: .95; text-transform: uppercase; letter-spacing: .04em; margin-bottom: 6px; }}
.loan-v {{ display:block; font-size: 16px; font-weight: 1000; line-height: 1.2; }}
.loan-actions {{ display: flex; gap: 10px; flex-wrap: wrap; margin: 12px 0 10px 0; }}
.loan-actions .btn {{ flex: 1 1 160px; }}
.loan-section {{ margin-top: 14px; }}
.loan-table th, .loan-table td {{ padding: 10px 8px; }}
.loan-details summary {{ font-weight: 900; cursor: pointer; }}
.loan-details details {{ margin-top: 10px; }}
.loan-pill {{ display:inline-block; padding: 3px 10px; border-radius: 999px; background: rgba(255,255,255,.18); font-weight: 800; font-size: 11px; margin-left: 8px; }}
</style>

<div class="card loan-wrap">
  <h2 class="loan-title">📄 Préstamo #{loan_id}</h2>
  <p class="loan-sub">
    Cliente: <b>{client.get('first_name') or '—'}</b>
    <span class="loan-pill">Estado: {estado_pago}</span>
  </p>

  <div class="loan-actions">
    <a class="btn btn-secondary" href="{wa_link}" target="_blank" rel="noopener">📲 Recordar por WhatsApp</a>
    <a class="btn btn-primary" href="{url_for('new_payment', loan_id=loan_id)}">➕ Registrar pago</a>
    {edit_btn}
  </div>

  <div class="loan-section">
    <details class="loan-details" open>
      <summary>✅ Resumen</summary>
      <div class="loan-grid" style="margin-top:10px;">
        <div class="loan-card" style="background: linear-gradient(135deg,#15803d,#22c55e);">
          <span class="loan-k">Capital aprobado</span><span class="loan-v">{fmt_money(capital_aprobado)}</span>
        </div>
        <div class="loan-card" style="background: linear-gradient(135deg,#7c3aed,#a855f7);">
          <span class="loan-k">Descuento inicial ({upfront_pct_label})</span><span class="loan-v">-{fmt_money(descuento_inicial)}</span>
        </div>
        <div class="loan-card" style="background: linear-gradient(135deg,#0d9488,#14b8a6);">
          <span class="loan-k">Monto entregado</span><span class="loan-v">{fmt_money(monto_entregado)}</span>
        </div>
        <div class="loan-card" style="background: linear-gradient(135deg,#1d4ed8,#3b82f6);">
          <span class="loan-k">Interés total ({rate:.1f}%)</span><span class="loan-v">{fmt_money(interes_total)}</span>
        </div>
        <div class="loan-card" style="background: linear-gradient(135deg,#b91c1c,#ef4444); grid-column: span 1;">
          <span class="loan-k">Total a pagar</span><span class="loan-v">{fmt_money(total_a_pagar)}</span>
        </div>
      </div>
    </details>
  </div>

  <div class="loan-section">
    <details class="loan-details" open>
      <summary>💰 Pagos</summary>
      <div class="loan-grid" style="margin-top:10px;">
        <div class="loan-card" style="background: linear-gradient(135deg,#4f8df7,#3b82f6);">
          <span class="loan-k">Pago por {freq_label}</span>
          <span class="loan-v">{fmt_money(scheduled_payment)}</span>
        </div>
        <div class="loan-card" style="background: linear-gradient(135deg,#16a34a,#22c55e);">
          <span class="loan-k">Cuotas</span>
          <span class="loan-v">{pagadas} de {term_count}</span>
          <span style="display:block; font-size:12px; margin-top:6px; opacity:.95; font-weight:900;">
            {pagadas} pagadas • {restantes} restantes
          </span>
        </div>
      </div>
    </details>
  </div>

  <div class="loan-section">
    <details class="loan-details" open>
      <summary>📅 Calendario de pagos</summary>
      <div style="margin-top:10px;">
        <div style="display:flex; flex-wrap:wrap; gap:10px; align-items:stretch;">
          {cuota_items_html}
        </div>
        {siguiente_pago_html}
      </div>
    </details>
  </div>

  <div class="loan-section">
    <details class="loan-details">
      <summary>📜 Historial de pagos</summary>
      <div class="table-scroll" style="margin-top:10px;">
        <table class="loan-table">
          <tr>
            <th>#</th><th>Monto</th><th>Capital</th><th>Interés</th><th>Fecha</th><th>Recibo</th>
          </tr>
          {hist_rows}
        </table>
      </div>
    </details>
  </div>

</div>
"""
    return page(body)


@app.route("/payment/new/<int:loan_id>", methods=["GET", "POST"])
@login_required
def new_payment(loan_id):
    L = store.loans.get(loan_id)
    if not L:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))
    # Monto recomendado para "cuota" (para precargar el formulario).
    term_count = int(L.get("term_count") or 1)
    total_to_pay = float(L.get("total_to_pay") or 0)
    scheduled_payment = float(L.get("installment_amount") or 0)
    if scheduled_payment <= 0 and term_count:
        scheduled_payment = total_to_pay / float(term_count)
    scheduled_payment = round(max(scheduled_payment, 0.0), 2)
    if request.method == "POST":
        amt = request.form.get("amount", type=float)
        typ = (request.form.get("type") or "cuota").strip()
        if amt is None or amt <= 0:
            flash("Monto inválido.", "danger")
            return redirect(url_for("new_payment", loan_id=loan_id))

        typ_l = str(typ or "").strip().lower()
        weeks_adv = None
        if typ_l == "adelanto":
            weeks_adv = request.form.get("weeks_advanced", type=int) or 1
            weeks_adv = max(1, min(int(weeks_adv), 52))

        pay_note = f"Pago préstamo #{loan_id}"
        if typ_l == "adelanto":
            pay_note = f"Pago adelantado préstamo #{loan_id} ({weeks_adv} sem.)"

        # Registrar movimiento en el banco (siempre suma).
        try:
            apply_cash_movement(
                movement_type="pago_prestamo",
                amount=amt,
                note=pay_note,
                user_id=current_user()["id"],
                org_id=session.get("org_id"),
            )
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("loan_detail", loan_id=loan_id))

        pid = store.nid("payments")
        store.payments[pid] = {
            "id": pid, "loan_id": loan_id, "amount": amt, "type": typ, "date": date.today(),
            "created_by": current_user()["id"], "capital": amt * 0.5, "interest": amt * 0.5,
            "status": "OK", "weeks_advanced": weeks_adv,
            "created_at": datetime.utcnow(),
        }
        rem = float(L.get("remaining") or 0) - amt
        L["remaining"] = max(0, rem)
        if L["remaining"] <= 0:
            L["status"] = "cerrado"
        # Si se registra una cuota, avanzamos la próxima fecha automáticamente.
        if typ_l == "cuota":
            interval_days = freq_interval_days(L.get("frequency"))
            current_due = L.get("next_payment_date") or date.today()
            if hasattr(current_due, "strftime"):
                L["next_payment_date"] = current_due + timedelta(days=interval_days)
        elif typ_l == "adelanto" and weeks_adv:
            interval_days = freq_interval_days(L.get("frequency"))
            current_due = L.get("next_payment_date") or date.today()
            if hasattr(current_due, "strftime"):
                L["next_payment_date"] = current_due + timedelta(days=interval_days * weeks_adv)
        flash("Pago registrado.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))
    body = (
        f'<div class="card"><h2>Pago — préstamo #{loan_id}</h2><form method="post" id="payForm">'
        f'<p style="margin:6px 0 10px 0; opacity:.95;"><b>Cuota recomendada:</b> {fmt_money(scheduled_payment)}</p>'
        f'<label>Monto</label><input name="amount" type="number" step="0.01" value="{scheduled_payment}" required>'
        f'<label>Tipo</label><select name="type" id="payType"><option value="cuota">Cuota</option>'
        f'<option value="adelanto">Adelanto</option><option value="capital">Capital</option><option value="interes">Interés</option></select>'
        f'<div id="weeksWrap" style="display:none;margin-top:8px"><label>Semanas adelantadas</label>'
        f'<input name="weeks_advanced" type="number" min="1" max="52" value="1" id="weeksInp"></div>'
        f'<button class="btn btn-primary" type="submit">Registrar</button></form>'
        f"<script>document.getElementById('payType').addEventListener('change',function(){{"
        f"document.getElementById('weeksWrap').style.display=this.value==='adelanto'?'block':'none';}});"
        f"document.getElementById('weeksWrap').style.display=document.getElementById('payType').value==='adelanto'?'block':'none';</script></div>"
    )
    return page(body)


@app.route("/payment/<int:payment_id>/print")
@login_required
def print_payment(payment_id):
    ensure_org()
    user = current_user()
    oid = session.get("org_id")
    p = store.payments.get(payment_id)
    if not p:
        return "Pago no encontrado", 404
    loan_id = p.get("loan_id")
    L = store.loans.get(loan_id) if loan_id is not None else None
    if not L:
        return "Préstamo no encontrado", 404
    if L.get("organization_id") != oid:
        return "Sin acceso.", 403
    if user.get("role") == "cobrador" and not is_cartera_admin(user) and L.get("created_by") != user.get("id"):
        return "Sin acceso.", 403

    cid = L.get("client_id")
    client = store.clients.get(cid, {})
    cli_name = html.escape(
        f"{client.get('first_name', '')} {client.get('last_name') or ''}".strip() or f"Cliente #{cid}"
    )
    cli_phone = html.escape(str(client.get("phone") or "—"))

    cob_uid = L.get("created_by")
    cob = store.users.get(cob_uid, {}) if cob_uid else {}
    cob_name = html.escape(str(cob.get("name") or cob.get("username") or "—"))
    cob_phone = html.escape(str(cob.get("phone") or RECEIPT_COMPANY_TEL or "—"))

    capital_prest = float(L.get("amount") or 0)
    interes_total = float(L.get("total_interest") or 0)
    total_prest = float(L.get("total_to_pay") or (capital_prest + interes_total))
    term_count = int(L.get("term_count") or 1)
    cuota_prog = float(L.get("installment_amount") or 0)
    if cuota_prog <= 0 and total_prest and term_count:
        cuota_prog = total_prest / float(term_count)

    head1 = html.escape(RECEIPT_BUSINESS_NAME.upper())
    head2 = html.escape(RECEIPT_SUBTITLE)
    tel_emp = html.escape(str(RECEIPT_COMPANY_TEL))
    dash = "--------------------------------"

    cuotas_lbl = receipt_cuotas_label(p, L)
    when = html.escape(receipt_payment_when(p))

    amt_m = html.escape(fmt_money(p.get("amount")))
    cap_pay = html.escape(fmt_money(p.get("capital")))
    int_pay = html.escape(fmt_money(p.get("interest")))

    sep = f'<div class="rc-sep">{dash}</div>'

    html_page = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Recibo #{payment_id}</title>
<style>
html,body{{margin:0;padding:0;background:#fff}}
.rc-wrap{{font-family:'Courier New',Courier,monospace;font-size:13px;max-width:320px;margin:0 auto;padding:16px 12px 24px;color:#111;box-sizing:border-box}}
.rc-wrap *{{box-sizing:border-box}}
.rc-c{{text-align:center;margin:2px 0;line-height:1.35}}
.rc-sep{{text-align:center;margin:10px 0;font-size:11px;letter-spacing:0;overflow:hidden;white-space:nowrap;color:#222}}
.rc-h{{font-weight:700;text-align:center;margin:8px 0 6px;text-transform:uppercase}}
.rc-row{{display:flex;justify-content:space-between;gap:8px;margin:3px 0;align-items:baseline}}
.rc-row span:first-child{{flex:0 1 auto}}
.rc-row span:last-child{{flex:0 0 auto;text-align:right;font-weight:700}}
@media print{{
body{{background:#fff}}
.rc-wrap{{max-width:100%;padding:8px}}
.no-print{{display:none!important}}
}}
</style>
</head>
<body>
<div class="rc-wrap">
  <div class="rc-c" style="font-weight:800;font-size:14px">{head1}</div>
  <div class="rc-c">{head2}</div>
  <div class="rc-c">TEL EMPRESA: {tel_emp}</div>
  {sep}
  <div>Cliente: {cli_name}</div>
  <div>Teléfono: {cli_phone}</div>
  <div>Fecha: {when}</div>
  {sep}
  <div>Cobrador: {cob_name}</div>
  <div>Tel Cobrador: {cob_phone}</div>
  {sep}
  <div class="rc-h">RESUMEN DEL PRESTAMO</div>
  <div class="rc-row"><span>Capital</span><span>{html.escape(fmt_money(capital_prest))}</span></div>
  <div class="rc-row"><span>Interés</span><span>{html.escape(fmt_money(interes_total))}</span></div>
  <div class="rc-row"><span>TOTAL</span><span>{html.escape(fmt_money(total_prest))}</span></div>
  <div class="rc-row"><span>Cuota</span><span>{html.escape(fmt_money(cuota_prog))}</span></div>
  <div class="rc-row"><span>Cuotas</span><span>{html.escape(cuotas_lbl)}</span></div>
  {sep}
  <div class="rc-h">PAGO REALIZADO</div>
  <div class="rc-row"><span>Monto</span><span>{amt_m}</span></div>
  <div class="rc-row"><span>Capital</span><span>{cap_pay}</span></div>
  <div class="rc-row"><span>Interés</span><span>{int_pay}</span></div>
  {sep}
  <div class="rc-c" style="font-weight:700">GRACIAS POR SU PAGO</div>
  <div class="rc-c" style="margin-top:4px">{head1}</div>
</div>
<p class="no-print" style="text-align:center;margin-top:12px"><button type="button" onclick="window.print()">Imprimir</button></p>
</body>
</html>"""
    return html_page


@app.route("/payment/delete/<int:payment_id>", methods=["POST"])
@login_required
def delete_payment(payment_id):
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    p = store.payments.get(payment_id)
    if not p:
        flash("Pago no encontrado.", "danger")
        return redirect(url_for("loans"))
    loan_id = p.get("loan_id")
    L = store.loans.get(loan_id) if loan_id is not None else None
    if not L or L.get("organization_id") != oid:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))
    if user.get("role") != "admin" and not is_cartera_admin(user):
        flash("Solo admin puede eliminar pagos.", "danger")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    typ_l = str(p.get("type") or "").strip().lower()
    weeks = int(p.get("weeks_advanced") or 0)
    if typ_l == "adelanto" and weeks < 1:
        weeks = 1

    amt = float(p.get("amount") or 0)
    try:
        apply_cash_movement(
            movement_type="reverso_pago_prestamo",
            amount=-amt,
            note=f"Reverso pago #{payment_id} préstamo #{loan_id}",
            user_id=user["id"],
            org_id=oid,
        )
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    # Ajustar préstamo (saldo y fechas) para que no quede inconsistente.
    L["remaining"] = float(L.get("remaining") or 0) + amt
    if "remaining_capital" in L:
        L["remaining_capital"] = float(L.get("remaining_capital") or 0) + amt
    if float(L.get("remaining") or 0) > 0:
        L["status"] = "ACTIVO"
    else:
        L["status"] = "cerrado"

    interval_days = freq_interval_days(L.get("frequency"))
    current_due = L.get("next_payment_date")
    if hasattr(current_due, "strftime"):
        if typ_l == "cuota":
            L["next_payment_date"] = current_due - timedelta(days=interval_days)
        elif typ_l == "adelanto" and weeks > 0:
            L["next_payment_date"] = current_due - timedelta(days=interval_days * weeks)

    store.payments.pop(payment_id, None)
    flash("Pago eliminado y banco revertido.", "success")
    return redirect(url_for("loan_detail", loan_id=loan_id))


@app.route("/payment/undo/<int:loan_id>", methods=["POST"])
@login_required
def undo_payment(loan_id):
    flash("Deshacer: sin lógica detallada en memoria.", "info")
    return redirect(url_for("loan_detail", loan_id=loan_id))


@app.route("/bank")
@login_required
def bank_home():
    ensure_org()
    user = current_user()
    tiles = (
        f'<a href="{url_for("bank_daily_list")}" class="bank-tile blue">🗓️ Lista diaria</a>'
        f'<a href="{url_for("bank_delivery")}" class="bank-tile green2">💰 Entrega</a>'
        f'<a href="{url_for("bank_expenses")}" class="bank-tile red">📓 Gastos</a>'
        f'<a href="{url_for("bank_acta")}" class="bank-tile yellow">💸 Descuento inicial</a>'
        f'<a href="{url_for("bank_routes_list")}" class="bank-tile teal">🏦 Capital por ruta</a>'
        f'<a href="{url_for("bank_advance")}" class="bank-tile lavender">💵 Adelantos</a>'
        f'<a href="{url_for("bank_legal_list")}" class="bank-tile purple">📜 Documento legal</a>'
        f'<a href="{url_for("bank_late")}" class="bank-tile orange">🔥 Atrasos</a>'
    )
    bottom = (
        f'<a href="{url_for("collector_map")}" class="bank-tile bank-tile-full teal2">📍 Ver ubicación cobrador</a>'
    )
    destroy = ""
    if user.get("role") == "admin":
        destroy = (
            f'<form method="post" action="{url_for("admin_clear_all")}" style="margin:14px 0 0 0;" '
            f'onsubmit="return confirm(\'¿BORRAR TODO EL SISTEMA? Se eliminarán todos los datos en memoria y se cerrará la sesión. Esta acción no se puede deshacer.\');">'
            f'<button type="submit" class="bank-tile bank-tile-full bank-destroy">🗑️ BORRAR TODO EL SISTEMA</button></form>'
        )
    body = (
        f'<h2 style="text-align:center;margin:8px 0 6px 0;font-size:1.5rem;font-weight:900;color:#14532d">🏛️ Banco</h2>'
        f"<style>"
        f".bank-wrap{{max-width:520px;margin:0 auto;padding:6px 4px 20px}}"
        f".bank-menu{{display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:stretch}}"
        f"@media(max-width:420px){{.bank-menu{{grid-template-columns:1fr}}}}"
        f".bank-tile{{display:flex;align-items:center;justify-content:center;text-align:center;padding:18px 14px;min-height:64px;border-radius:18px;color:#fff!important;font-weight:900;text-decoration:none!important;font-size:15px;box-shadow:0 10px 26px rgba(0,0,0,.16);transition:transform .12s ease,filter .12s ease;box-sizing:border-box;line-height:1.25}}"
        f".bank-tile:hover{{transform:translateY(-2px);filter:brightness(1.02)}}"
        f".bank-tile:focus{{outline:2px solid rgba(255,255,255,.85);outline-offset:2px}}"
        f".bank-tile-full{{grid-column:1/-1}}"
        f".blue{{background:linear-gradient(135deg,#4f8df7,#60a5fa)}}"
        f".red{{background:linear-gradient(135deg,#f87171,#fb7185)}}"
        f".orange{{background:linear-gradient(135deg,#fb923c,#fdba74)}}"
        f".purple{{background:linear-gradient(135deg,#a855f7,#c084fc)}}"
        f".lavender{{background:linear-gradient(135deg,#6366f1,#818cf8)}}"
        f".teal{{background:linear-gradient(135deg,#0d9488,#14b8a6)}}"
        f".teal2{{background:linear-gradient(135deg,#0f766e,#2dd4bf)}}"
        f".yellow{{background:linear-gradient(135deg,#ca8a04,#eab308)}}"
        f".green2{{background:linear-gradient(135deg,#15803d,#22c55e)}}"
        f".bank-destroy{{background:linear-gradient(135deg,#dc2626,#ef4444)!important;border:2px solid rgba(255,255,255,.35);cursor:pointer;width:100%;font:inherit}}"
        f"</style>"
        f'<div class="bank-wrap"><div class="bank-menu">{tiles}{bottom}</div>{destroy}</div>'
    )
    return page(body)


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        flash("Contacte al administrador.", "info")
        return redirect(url_for("login"))
    return page('<div class="card"><h2>Recuperar</h2><p>Contacte al admin.</p><a class="btn btn-primary" href="' + url_for("login") + '">Volver</a></div>', user=None)


@app.route("/admin-force-create")
def admin_force_create():
    store.reset_all()
    return "Datos reiniciados. Usuario: admin / Contraseña: admin"


@app.route("/admin/clear-all", methods=["GET", "POST"])
@login_required
@admin_required
def admin_clear_all():
    if request.method == "POST":
        store.reset_all()
        session.clear()
        flash("Datos reiniciados.", "success")
        return redirect(url_for("login"))
    return page(f'<div class="card"><h2>Reiniciar todo</h2><form method="post"><button class="btn btn-primary" type="submit">Confirmar</button></form></div>')


@app.route("/audit")
@login_required
@role_required("admin", "supervisor")
def audit():
    ensure_org()
    oid = session.get("org_id")
    rows = "".join(
        f"<tr><td>{a.get('created_at')}</td><td>{a.get('action')}</td><td>{a.get('detail')}</td></tr>"
        for a in store.audit_log[-200:]
        if store.users.get(a.get("user_id"), {}).get("organization_id") == oid
    )
    return page(f'<div class="card"><h2>Auditoría</h2><div class="table-scroll"><table><tr><th>Fecha</th><th>Acción</th><th>Detalle</th></tr>{rows}</table></div></div>')


def compute_super_admin_stats(date_from=None, date_to=None):
    """
    KPIs y datos para el panel SaaS:
    - Ganancias del sistema = pagos de suscripción de admins.
    - Aplicar filtros por rango de fechas a gráficas, estadísticas y ganancias.
    """
    admins = [u for u in store.users.values() if u.get("role") == "admin"]
    tenant_ids = {a.get("id") for a in admins if a.get("id") is not None}

    today = date.today()
    if date_from is None:
        date_from = today - timedelta(days=29)
    if date_to is None:
        date_to = today
    if isinstance(date_from, str):
        try:
            date_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        except ValueError:
            date_from = today - timedelta(days=29)
    if isinstance(date_to, str):
        try:
            date_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        except ValueError:
            date_to = today
    if date_from is None:
        date_from = today - timedelta(days=29)
    if date_to is None:
        date_to = today
    if date_from > date_to:
        date_from, date_to = date_to, date_from

    # Helper para convertir datetime->date sin depender de otra función.
    def _to_date(x):
        if not x:
            return None
        # Nota: datetime hereda de date, así que hay que manejar primero datetime.
        if isinstance(x, datetime):
            return x.date()
        if isinstance(x, date):
            return x
        try:
            if hasattr(x, "date"):
                return x.date()
        except Exception:
            return None
        return None

    # Ganancias basadas en pagos de admins dentro del rango.
    range_payments = [
        p
        for p in store.admin_payments
        if p.get("admin_id") in tenant_ids
        and _to_date(p.get("payment_date")) is not None
        and date_from <= _to_date(p.get("payment_date")) <= date_to
        and float(p.get("amount") or 0) >= 0
    ]

    gain_total_range = round(sum(float(p.get("amount") or 0) for p in range_payments), 2)

    # Ganancia total (todo el tiempo).
    gain_total_all_time = round(
        sum(
            float(p.get("amount") or 0)
            for p in store.admin_payments
            if p.get("admin_id") in tenant_ids and float(p.get("amount") or 0) >= 0
        ),
        2,
    )

    # Ganancia mensual: mes que contiene `date_to`, respetando el filtro.
    month_start = date(date_to.year, date_to.month, 1)
    month_end = (date_to.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    gain_monthly_current = round(
        sum(
            float(p.get("amount") or 0)
            for p in store.admin_payments
            if p.get("admin_id") in tenant_ids
            and _to_date(p.get("payment_date")) is not None
            and max(date_from, month_start) <= _to_date(p.get("payment_date")) <= min(date_to, month_end)
            and float(p.get("amount") or 0) >= 0
        ),
        2,
    )

    # Serie para gráfica (diaria o semanal si el rango es grande).
    range_days = (date_to - date_from).days + 1
    income_series = []
    labels = []

    if range_days <= 45:
        # Diaria.
        cursor = date_from
        while cursor <= date_to:
            s = sum(
                float(p.get("amount") or 0)
                for p in range_payments
                if _to_date(p.get("payment_date")) == cursor
            )
            labels.append(cursor.strftime("%d/%m"))
            income_series.append(round(s, 2))
            cursor += timedelta(days=1)
    else:
        # Semanal.
        cursor = date_from
        while cursor <= date_to:
            end = min(cursor + timedelta(days=6), date_to)
            s = sum(
                float(p.get("amount") or 0)
                for p in range_payments
                if cursor <= _to_date(p.get("payment_date")) <= end
            )
            labels.append(f"{cursor.strftime('%d/%m')}-{end.strftime('%d/%m')}")
            income_series.append(round(s, 2))
            cursor = end + timedelta(days=1)

    # Serie mensual para gráfica (barra).
    month_labels = []
    monthly_series = []
    m = date(date_from.year, date_from.month, 1)
    end_m = date(date_to.year, date_to.month, 1)
    while m <= end_m:
        next_m = (m + timedelta(days=32)).replace(day=1)
        m_end = next_m - timedelta(days=1)
        month_labels.append(m.strftime("%m/%Y"))
        s = sum(
            float(p.get("amount") or 0)
            for p in range_payments
            if m <= _to_date(p.get("payment_date")) <= m_end
        )
        monthly_series.append(round(s, 2))
        m = next_m

    # Estado de admins por fechas (activo/vencido) basado en fecha_fin.
    admins_active_count = 0
    admins_expired_count = 0
    pending_admins_count = 0

    expired_admins = []
    soon_admins = []
    upcoming_days = 3
    now = datetime.utcnow()
    for a in admins:
        a_status = a.get("account_status", ACCOUNT_ACTIVE)
        f_ini = _to_date(a.get("fecha_inicio") or a.get("subscription_start") or None)
        f_fin = _to_date(a.get("fecha_fin") or a.get("subscription_end") or None)
        if a_status == ACCOUNT_PENDING:
            pending_admins_count += 1
        if f_fin and today > f_fin:
            admins_expired_count += 1
            expired_admins.append(a)
        elif f_fin and f_ini and f_ini <= today <= f_fin and a_status == ACCOUNT_ACTIVE:
            admins_active_count += 1
        elif f_fin and f_ini and f_ini <= today <= f_fin and a_status != ACCOUNT_ACTIVE and a_status != ACCOUNT_PENDING:
            # Si está dentro del rango pero está suspendido, no cuenta como "activo".
            pass

        # Alertas próximas a vencer (solo los activos).
        if f_fin and a_status == ACCOUNT_ACTIVE and today <= f_fin:
            if (f_fin - today).days <= upcoming_days:
                soon_admins.append(a)

    pending_collectors = sum(
        1 for u in store.users.values() if u.get("role") == "cobrador" and u.get("account_status") == ACCOUNT_PENDING
    )

    # Historial de pagos (para tabla). Orden por fecha desc.
    payment_history = sorted(
        range_payments,
        key=lambda x: (_to_date(x.get("payment_date")) or date.min, x.get("id") or 0),
        reverse=True,
    )[:150]

    return {
        "range_from": date_from.isoformat(),
        "range_to": date_to.isoformat(),
        "gain_total_all_time": gain_total_all_time,
        "gain_monthly_current": gain_monthly_current,
        "gain_total_range": gain_total_range,
        "labels": labels,
        "income_series": income_series,
        "month_labels": month_labels,
        "monthly_series": monthly_series,
        "admins_active_count": admins_active_count,
        "admins_expired_count": admins_expired_count,
        "admins_pending_count": pending_admins_count,
        "alerts": {
            "expired_admins_count": len(expired_admins),
            "soon_admins_count": len(soon_admins),
            "pending_collectors_count": pending_collectors,
            "expired_admins": [{"id": a.get("id"), "username": a.get("username")} for a in expired_admins[:10]],
            "soon_admins": [{"id": a.get("id"), "username": a.get("username")} for a in soon_admins[:10]],
        },
        "payment_history": [
            {
                "id": p.get("id"),
                "admin_id": p.get("admin_id"),
                "admin_username": store.users.get(p.get("admin_id"), {}).get("username"),
                "amount": round(float(p.get("amount") or 0), 2),
                "payment_date": _to_date(p.get("payment_date")).isoformat() if _to_date(p.get("payment_date")) else None,
                "method": p.get("method") or "",
            }
            for p in payment_history
        ],
    }


@app.route("/super-admin", methods=["GET", "POST"])
@login_required
@super_admin_required
def super_admin_panel():
    ensure_org()  # no-op para super_admin (solo para no romper otras dependencias)
    user = current_user()
    now = datetime.utcnow()

    # Filtro por fecha (aplica a gráficas, estadísticas y ganancias).
    today_d = date.today()
    df = today_d - timedelta(days=29)
    dt = today_d
    raw_desde = request.args.get("desde", type=str)
    raw_hasta = request.args.get("hasta", type=str)
    if raw_desde:
        try:
            df = datetime.strptime(raw_desde, "%Y-%m-%d").date()
        except ValueError:
            flash("Fecha «Desde» inválida. Se usará el rango por defecto.", "warning")
    if raw_hasta:
        try:
            dt = datetime.strptime(raw_hasta, "%Y-%m-%d").date()
        except ValueError:
            flash("Fecha «Hasta» inválida. Se usará el rango por defecto.", "warning")
    if df > dt:
        df, dt = dt, df

    def fmt_sub_end(x):
        if not x:
            return "—"
        try:
            return x.strftime("%d/%m/%Y")
        except Exception:
            return str(x)

    if request.method == "POST":
        act = (request.form.get("action") or "").strip()
        target_id = request.form.get("target_id", type=int)
        tenant_id = request.form.get("tenant_id", type=int)

        u = store.users.get(target_id) if target_id else None
        if not u:
            flash("Usuario no encontrado.", "danger")
            return redirect(url_for("super_admin_panel"))

        if act == "approve_admin":
            if u.get("role") != "admin":
                flash("Solo se aprueban admins.", "danger")
                return redirect(url_for("super_admin_panel"))
            u["account_status"] = ACCOUNT_ACTIVE
            flash("Admin aprobado y activado.", "success")
            return redirect(url_for("super_admin_panel"))

        if act == "toggle_admin":
            if u.get("role") != "admin":
                flash("Solo se pueden activar/desactivar admins.", "danger")
                return redirect(url_for("super_admin_panel"))
            new_status = request.form.get("new_status", type=str)
            if new_status not in (ACCOUNT_ACTIVE, ACCOUNT_SUSPENDED):
                flash("Estado inválido.", "danger")
                return redirect(url_for("super_admin_panel"))
            u["account_status"] = new_status
            flash("Estado de admin actualizado.", "success")
            return redirect(url_for("super_admin_panel"))

        if act == "approve_cobrador":
            if u.get("role") != "cobrador":
                flash("Solo se aprueban cobradores.", "danger")
                return redirect(url_for("super_admin_panel"))
            # Si el cobrador no pertenece a un tenant/admin, no se aprueba.
            tid = u.get("organization_id")
            if tid is None:
                flash("Cobrador sin tenant asociado.", "danger")
                return redirect(url_for("super_admin_panel"))
            u["account_status"] = ACCOUNT_ACTIVE
            flash("Cobrador aprobado.", "success")
            return redirect(url_for("super_admin_panel"))

        if act == "toggle_cobrador":
            if u.get("role") != "cobrador":
                flash("Solo se puede controlar cobradores.", "danger")
                return redirect(url_for("super_admin_panel"))
            new_status = request.form.get("new_status", type=str)
            if new_status not in (ACCOUNT_ACTIVE, ACCOUNT_SUSPENDED):
                flash("Estado inválido.", "danger")
                return redirect(url_for("super_admin_panel"))
            # No permitir pasar de "pendiente" a "activo" sin aprobación.
            if u.get("account_status") == ACCOUNT_PENDING and new_status == ACCOUNT_ACTIVE:
                flash("Primero apruebe el cobrador.", "warning")
                return redirect(url_for("super_admin_panel"))
            u["account_status"] = new_status
            flash("Estado del cobrador actualizado.", "success")
            return redirect(url_for("super_admin_panel"))

        if act == "register_admin_payment":
            # Registrar pago de suscripción y extender fecha_fin.
            admin_id = request.form.get("admin_id", type=int)
            amount = request.form.get("amount", type=float)
            payment_date_raw = (request.form.get("payment_date") or "").strip()
            method = (request.form.get("method") or "").strip()

            if not admin_id:
                flash("Admin inválido.", "danger")
                return redirect(url_for("super_admin_panel"))
            if amount is None or amount < 0:
                flash("Monto inválido.", "danger")
                return redirect(url_for("super_admin_panel"))
            if amount == 0:
                flash("El monto debe ser mayor que 0.", "danger")
                return redirect(url_for("super_admin_panel"))

            try:
                payment_date = datetime.strptime(payment_date_raw, "%Y-%m-%d").date()
            except ValueError:
                flash("Fecha de pago inválida.", "danger")
                return redirect(url_for("super_admin_panel"))

            admin_u = store.users.get(admin_id)
            if not admin_u or admin_u.get("role") != "admin":
                flash("Admin no encontrado.", "danger")
                return redirect(url_for("super_admin_panel"))

            # Extender fecha_fin + PAYMENT_EXTENSION_DAYS.
            base_date = admin_u.get("fecha_fin") or admin_u.get("subscription_end")
            base_d = base_date
            if hasattr(base_d, "date") and not isinstance(base_d, date):
                base_d = base_d.date()
            if isinstance(base_d, date) and base_d >= payment_date:
                extend_from = base_d
            else:
                extend_from = payment_date

            if extend_from < date.today():
                extend_from = date.today()

            new_fin = extend_from + timedelta(days=PAYMENT_EXTENSION_DAYS)
            if not admin_u.get("fecha_inicio"):
                admin_u["fecha_inicio"] = extend_from
            # Si fechas están invertidas por edición manual, reacomodar.
            if admin_u.get("fecha_inicio") and isinstance(admin_u["fecha_inicio"], date) and admin_u["fecha_inicio"] > extend_from:
                admin_u["fecha_inicio"] = extend_from

            admin_u["fecha_fin"] = new_fin
            admin_u["subscription_end"] = new_fin  # compatibilidad
            admin_u["account_status"] = ACCOUNT_ACTIVE  # activar si estaba suspendido

            pid = store.nid("admin_payments")
            store.admin_payments.append(
                {
                    "id": pid,
                    "admin_id": admin_id,
                    "amount": round(float(amount), 2),
                    "payment_date": payment_date,
                    "method": method,
                    "created_at": datetime.utcnow(),
                }
            )
            try:
                log_action(current_user()["id"], "admin_payment", str(pid))
            except Exception:
                pass

            flash("Pago registrado. Suscripción extendida.", "success")
            return redirect(url_for("super_admin_panel"))

        if act == "set_admin_subscription_dates":
            admin_id = request.form.get("target_id", type=int) or request.form.get("admin_id", type=int)
            admin_u = store.users.get(admin_id) if admin_id else None
            if not admin_u or admin_u.get("role") != "admin":
                flash("Admin inválido.", "danger")
                return redirect(url_for("super_admin_panel"))
            raw_inicio = (request.form.get("fecha_inicio") or "").strip()
            raw_fin = (request.form.get("fecha_fin") or "").strip()
            try:
                fecha_inicio = datetime.strptime(raw_inicio, "%Y-%m-%d").date()
                fecha_fin = datetime.strptime(raw_fin, "%Y-%m-%d").date()
            except ValueError:
                flash("Fechas inválidas.", "danger")
                return redirect(url_for("super_admin_panel"))
            if fecha_fin < fecha_inicio:
                flash("La fecha fin no puede ser menor que la fecha inicio.", "danger")
                return redirect(url_for("super_admin_panel"))
            admin_u["fecha_inicio"] = fecha_inicio
            admin_u["fecha_fin"] = fecha_fin
            admin_u["subscription_end"] = fecha_fin  # compatibilidad
            admin_u["subscription_start"] = fecha_inicio
            admin_u["account_status"] = ACCOUNT_ACTIVE
            flash("Suscripción del admin actualizada.", "success")
            return redirect(url_for("super_admin_panel"))

        if act == "set_cobrador_dates":
            cob_id = request.form.get("target_id", type=int)
            cob_u = store.users.get(cob_id) if cob_id else None
            if not cob_u or cob_u.get("role") != "cobrador":
                flash("Cobrador inválido.", "danger")
                return redirect(url_for("super_admin_panel"))
            raw_inicio = (request.form.get("fecha_inicio") or "").strip()
            raw_fin = (request.form.get("fecha_fin") or "").strip()
            try:
                fecha_inicio = datetime.strptime(raw_inicio, "%Y-%m-%d").date()
                fecha_fin = datetime.strptime(raw_fin, "%Y-%m-%d").date()
            except ValueError:
                flash("Fechas inválidas.", "danger")
                return redirect(url_for("super_admin_panel"))
            if fecha_fin < fecha_inicio:
                flash("La fecha fin no puede ser menor que la fecha inicio.", "danger")
                return redirect(url_for("super_admin_panel"))
            cob_u["fecha_inicio"] = fecha_inicio
            cob_u["fecha_fin"] = fecha_fin
            cob_u["subscription_end"] = fecha_fin  # compatibilidad
            cob_u["subscription_start"] = fecha_inicio
            flash("Fechas del cobrador actualizadas.", "success")
            return redirect(url_for("super_admin_panel"))

        flash("Acción inválida.", "danger")
        return redirect(url_for("super_admin_panel"))

    stats = compute_super_admin_stats(df, dt)
    admins = [u for u in store.users.values() if u.get("role") == "admin"]
    admins.sort(key=lambda x: x.get("created_at") or datetime.min, reverse=True)

    tenant_ids = {a.get("id") for a in admins if a.get("id") is not None}
    total_clients = sum(1 for c in store.clients.values() if c.get("organization_id") in tenant_ids)
    total_loans = sum(1 for L in store.loans.values() if L.get("organization_id") in tenant_ids)

    def esc(x):
        return html.escape(str(x or "—"))

    def status_badge(status):
        status = status or ACCOUNT_ACTIVE
        if status == ACCOUNT_ACTIVE:
            return '<span class="sa-badge sa-ok">activo</span>'
        if status == ACCOUNT_PENDING:
            return '<span class="sa-badge sa-warn">pendiente</span>'
        if status == ACCOUNT_SUSPENDED:
            return '<span class="sa-badge sa-bad">suspendido</span>'
        return f'<span class="sa-badge">{esc(status)}</span>'

    def to_date_only(x):
        if not x:
            return None
        if isinstance(x, datetime):
            return x.date()
        if isinstance(x, date):
            return x
        try:
            if hasattr(x, "date"):
                return x.date()
        except Exception:
            return None
        return None

    def date_input_val(x):
        d = to_date_only(x)
        return d.strftime("%Y-%m-%d") if d else ""

    def subscription_state_badge(account_status, fecha_fin):
        a_status = account_status or ACCOUNT_ACTIVE
        fin_d = to_date_only(fecha_fin)
        if fin_d and today_d > fin_d:
            return '<span class="sa-badge sa-bad">vencido</span>'
        if a_status == ACCOUNT_PENDING:
            return '<span class="sa-badge sa-warn">pendiente</span>'
        if a_status == ACCOUNT_SUSPENDED:
            return '<span class="sa-badge sa-bad">suspendido</span>'
        return '<span class="sa-badge sa-ok">activo</span>'

    def role_badge(role):
        role = (role or "").strip().lower()
        if role == "admin":
            return '<span class="sa-pill sa-ink">admin</span>'
        if role == "cobrador":
            return '<span class="sa-pill sa-amber">cobrador</span>'
        return f'<span class="sa-pill sa-muted">{esc(role)}</span>'

    # Tabla de admins (tenants)
    admin_rows = ""
    for a in admins:
        aid = a.get("id")
        status = a.get("account_status", ACCOUNT_ACTIVE)
        sub_end = a.get("fecha_fin") or a.get("subscription_end")
        pending_c = sum(
            1
            for u in store.users.values()
            if u.get("role") == "cobrador"
            and u.get("organization_id") == aid
            and u.get("account_status") == ACCOUNT_PENDING
        )
        users_total = sum(1 for u in store.users.values() if u.get("organization_id") == aid)
        clients_total = sum(1 for c in store.clients.values() if c.get("organization_id") == aid)
        loans_total = sum(1 for L in store.loans.values() if L.get("organization_id") == aid)

        actions = ""
        if status == ACCOUNT_PENDING:
            actions = (
                f"<form method='post' action='{url_for('super_admin_panel')}' style='display:inline'>"
                f"<input type='hidden' name='action' value='approve_admin'>"
                f"<input type='hidden' name='target_id' value='{aid}'>"
                f"<button class='sa-btn sa-btn-primary' type='submit'>Aprobar</button>"
                f"</form>"
            )
        elif status == ACCOUNT_ACTIVE:
            actions = (
                f"<form method='post' action='{url_for('super_admin_panel')}' style='display:inline'>"
                f"<input type='hidden' name='action' value='toggle_admin'>"
                f"<input type='hidden' name='target_id' value='{aid}'>"
                f"<input type='hidden' name='new_status' value='{ACCOUNT_SUSPENDED}'>"
                f"<button class='sa-btn sa-btn-danger' type='submit'>Suspender</button>"
                f"</form>"
            )
        else:
            actions = (
                f"<form method='post' action='{url_for('super_admin_panel')}' style='display:inline'>"
                f"<input type='hidden' name='action' value='toggle_admin'>"
                f"<input type='hidden' name='target_id' value='{aid}'>"
                f"<input type='hidden' name='new_status' value='{ACCOUNT_ACTIVE}'>"
                f"<button class='sa-btn sa-btn-primary' type='submit'>Activar</button>"
                f"</form>"
            )

        admin_dates_editor = (
            f"<details style='display:inline-block;margin-left:8px;vertical-align:top'>"
            f"<summary style='cursor:pointer;font-weight:900;opacity:.85'>Fechas</summary>"
            f"<form method='post' action='{url_for('super_admin_panel')}' style='margin-top:8px'>"
            f"<input type='hidden' name='action' value='set_admin_subscription_dates'>"
            f"<input type='hidden' name='target_id' value='{aid}'>"
            f"<div style='display:flex;gap:10px;flex-wrap:wrap'>"
            f"<div><div style='font-size:11px;opacity:.8;font-weight:800;margin-bottom:6px'>Inicio</div>"
            f"<input name='fecha_inicio' type='date' value='{date_input_val(a.get('fecha_inicio') or a.get('subscription_start'))}' required style='padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9)'/></div>"
            f"<div><div style='font-size:11px;opacity:.8;font-weight:800;margin-bottom:6px'>Fin</div>"
            f"<input name='fecha_fin' type='date' value='{date_input_val(sub_end)}' required style='padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9)'/></div>"
            f"</div>"
            f"<button class='sa-btn sa-btn-ghost' type='submit' style='margin-top:8px;width:100%'>Guardar</button>"
            f"</form>"
            f"</details>"
        )
        actions = f"{actions}{admin_dates_editor}"

        admin_rows += (
            "<tr>"
            f"<td>{esc(a.get('username'))}</td>"
            f"<td>{subscription_state_badge(status, sub_end)}</td>"
            f"<td>{fmt_sub_end(sub_end)}</td>"
            f"<td style='text-align:right'>{users_total}</td>"
            f"<td style='text-align:right'>{clients_total}</td>"
            f"<td style='text-align:right'>{loans_total}</td>"
            f"<td style='text-align:right'>{pending_c}</td>"
            f"<td>{actions}</td>"
            "</tr>"
        )

    # Tabla global de usuarios (admins + cobradores)
    all_users = [u for u in store.users.values() if u.get("role") in ("admin", "cobrador") and u.get("organization_id") in tenant_ids]
    all_users.sort(key=lambda u: u.get("created_at") or datetime.min, reverse=True)
    users_rows = ""
    for u in all_users[:280]:
        tid = u.get("organization_id")
        tname = store.users.get(tid, {}).get("username") or f"Admin #{tid}"
        users_rows += (
            "<tr>"
            f"<td>{esc(tname)}</td>"
            f"<td>{esc(u.get('username'))}</td>"
            f"<td>{role_badge(u.get('role'))}</td>"
            f"<td>{subscription_state_badge(u.get('account_status', ACCOUNT_ACTIVE), u.get('fecha_fin') or u.get('subscription_end'))}</td>"
            f"<td>{esc(u.get('phone') or '—')}</td>"
            f"<td>{esc(u.get('created_at'))}</td>"
            "</tr>"
        )

    # Sección de cobradores por admin (tenant)
    collectors_by_admin_html = ""
    for a in admins:
        aid = a.get("id")
        a_name = a.get("username") or f"Admin #{aid}"
        collectors = [
            u
            for u in store.users.values()
            if u.get("role") == "cobrador" and u.get("organization_id") == aid
        ]
        collectors.sort(key=lambda u: u.get("created_at") or datetime.min, reverse=True)
        if not collectors:
            continue

        rows = ""
        for c in collectors[:200]:
            c_status = c.get("account_status", ACCOUNT_PENDING)
            c_fin = c.get("fecha_fin") or c.get("subscription_end")
            actions = ""
            if c_status == ACCOUNT_PENDING:
                actions = (
                    f"<form method='post' action='{url_for('super_admin_panel')}' style='display:inline'>"
                    f"<input type='hidden' name='action' value='approve_cobrador'>"
                    f"<input type='hidden' name='target_id' value='{c.get('id')}'>"
                    f"<button class='sa-btn sa-btn-primary' type='submit'>Aprobar</button>"
                    f"</form>"
                )
            elif c_status == ACCOUNT_ACTIVE:
                actions = (
                    f"<form method='post' action='{url_for('super_admin_panel')}' style='display:inline'>"
                    f"<input type='hidden' name='action' value='toggle_cobrador'>"
                    f"<input type='hidden' name='target_id' value='{c.get('id')}'>"
                    f"<input type='hidden' name='new_status' value='{ACCOUNT_SUSPENDED}'>"
                    f"<button class='sa-btn sa-btn-danger' type='submit'>Suspender</button>"
                    f"</form>"
                )
            else:
                actions = (
                    f"<form method='post' action='{url_for('super_admin_panel')}' style='display:inline'>"
                    f"<input type='hidden' name='action' value='toggle_cobrador'>"
                    f"<input type='hidden' name='target_id' value='{c.get('id')}'>"
                    f"<input type='hidden' name='new_status' value='{ACCOUNT_ACTIVE}'>"
                    f"<button class='sa-btn sa-btn-primary' type='submit'>Activar</button>"
                    f"</form>"
                )

            c_dates_editor = (
                f"<details style='display:inline-block;margin-left:8px;vertical-align:top'>"
                f"<summary style='cursor:pointer;font-weight:900;opacity:.85'>Fechas</summary>"
                f"<form method='post' action='{url_for('super_admin_panel')}' style='margin-top:8px'>"
                f"<input type='hidden' name='action' value='set_cobrador_dates'>"
                f"<input type='hidden' name='target_id' value='{c.get('id')}'>"
                f"<div style='display:flex;gap:10px;flex-wrap:wrap'>"
                f"<div><div style='font-size:11px;opacity:.8;font-weight:800;margin-bottom:6px'>Inicio</div>"
                f"<input name='fecha_inicio' type='date' value='{date_input_val(c.get('fecha_inicio') or c.get('subscription_start'))}' required style='padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9)'/></div>"
                f"<div><div style='font-size:11px;opacity:.8;font-weight:800;margin-bottom:6px'>Fin</div>"
                f"<input name='fecha_fin' type='date' value='{date_input_val(c_fin)}' required style='padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9)'/></div>"
                f"</div>"
                f"<button class='sa-btn sa-btn-ghost' type='submit' style='margin-top:8px;width:100%'>Guardar</button>"
                f"</form>"
                f"</details>"
            )
            actions = f"{actions}{c_dates_editor}"

            rows += (
                "<tr>"
                f"<td>{esc(c.get('username'))}</td>"
                f"<td>{subscription_state_badge(c_status, c_fin)}</td>"
                f"<td>{esc(a_name)}</td>"
                f"<td>{esc(c.get('phone') or '—')}</td>"
                f"<td>{esc(c.get('created_at'))}</td>"
                f"<td>{actions}</td>"
                "</tr>"
            )

        collectors_by_admin_html += (
            f'<div class="sa-card sa-section">'
            f'<div class="sa-section-head"><div><h3 class="sa-h3">Cobradores de {esc(a_name)}</h3><p class="sa-sub">Control por estado y acceso</p></div></div>'
            f'<div class="table-scroll"><table><tr><th>Cobrador</th><th>Estado</th><th>Admin</th><th>Tel</th><th>Fecha reg.</th><th></th></tr>{rows}</table></div>'
            f'</div>'
        )

    expired_admins = stats.get("alerts", {}).get("expired_admins", [])
    soon_admins = stats.get("alerts", {}).get("soon_admins", [])
    pending_collectors_count = stats.get("alerts", {}).get("pending_collectors_count", 0)
    expired_list = ", ".join(esc(a.get("username")) for a in expired_admins) if expired_admins else "Sin admins vencidos"
    soon_list = ", ".join(esc(a.get("username")) for a in soon_admins) if soon_admins else "Nadie próximo a vencer"

    payment_admin_opts = "".join(
        f"<option value='{a.get('id')}'>{esc(a.get('username'))}</option>"
        for a in admins
    )
    payments_rows = "".join(
        "<tr>"
        f"<td>{esc(p.get('admin_username') or '—')}</td>"
        f"<td style='text-align:right'>{fmt_money(p.get('amount') or 0)}</td>"
        f"<td>{esc(p.get('payment_date') or '—')}</td>"
        f"<td>{esc(p.get('method') or '—')}</td>"
        "</tr>"
        for p in (stats.get("payment_history") or [])
    )

    initial_json = json.dumps(stats)
    stats_url = url_for(
        "super_admin_stats",
        desde=df.isoformat(),
        hasta=dt.isoformat(),
    )
    charts_js = """
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const statsUrl = "STATS_URL";
const initialData = INITIAL_DATA;

let incomeChart = null;
let prestadoChart = null;

function money(v){
  const n = Number(v || 0);
  return 'RD$ ' + n.toLocaleString('es-DO', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function buildCharts(){
  const incomeCtx = document.getElementById('chartIncome');
  const prestCtx = document.getElementById('chartPrestadoCobrado');

  incomeChart = new Chart(incomeCtx, {
    type: 'line',
    data: {
      labels: initialData.labels || [],
      datasets: [{
        label: 'Ingresos (intereses cobrados)',
        data: initialData.income_series || [],
        borderColor: 'rgba(99,102,241,1)',
        backgroundColor: 'rgba(99,102,241,.2)',
        tension: 0.35,
        fill: true,
        pointRadius: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: true } },
      scales: { y: { beginAtZero: true } }
    }
  });

  prestadoChart = new Chart(prestCtx, {
    type: 'bar',
    data: {
      labels: initialData.month_labels || [],
      datasets: [{
        label: 'Ganancia por mes',
        data: initialData.monthly_series || [],
        backgroundColor: 'rgba(22,163,74,.35)',
        borderColor: 'rgba(22,163,74,1)',
        borderWidth: 1
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } }
    }
  });
}

function updateFrom(data){
  document.getElementById('gainMonthly').textContent = money(data.gain_monthly_current);
  document.getElementById('gainGlobal').textContent = money(data.gain_total_all_time);
  document.getElementById('gainRange').textContent = money(data.gain_total_range);
  document.getElementById('adminsActiveCount').textContent = data.admins_active_count ?? 0;
  document.getElementById('adminsExpiredCount').textContent = data.admins_expired_count ?? 0;

  if (incomeChart && data.labels && data.income_series){
    incomeChart.data.labels = data.labels;
    incomeChart.data.datasets[0].data = data.income_series;
    incomeChart.update();
  }
  if (prestadoChart && data.month_labels && data.monthly_series){
    prestadoChart.data.labels = data.month_labels;
    prestadoChart.data.datasets[0].data = data.monthly_series;
    prestadoChart.update();
  }

  const alerts = data.alerts || {};
  document.getElementById('alertExpiredCount').textContent = alerts.expired_admins_count || 0;
  document.getElementById('alertSoonCount').textContent = alerts.soon_admins_count || 0;
  document.getElementById('alertPendingCollectorsCount').textContent = alerts.pending_collectors_count || 0;
}

async function refreshStats(){
  try{
    const r = await fetch(statsUrl, { credentials: 'same-origin', cache: 'no-store' });
    if(!r.ok) return;
    const data = await r.json();
    updateFrom(data);
  } catch(e){}
}

window.addEventListener('load', () => {
  buildCharts();
  updateFrom(initialData);
  refreshStats();
  setInterval(refreshStats, 8000);
});
</script>
"""
    charts_js = charts_js.replace("STATS_URL", stats_url).replace("INITIAL_DATA", initial_json)

    sa_css = """
    .sa-wrap .sa-top{
      background: linear-gradient(135deg, rgba(22,163,74,.18), rgba(59,130,246,.14));
      border: 1px solid rgba(16,185,129,.20);
      padding: 16px;
      border-radius: 18px;
      margin-bottom: 14px;
    }
    .sa-top h2{ margin:0; font-size: 20px; font-weight: 900; color:#14532d; letter-spacing:-.01em;}
    .sa-top p{ margin:6px 0 0 0; opacity:.9;}

    .sa-grid-2{ display:grid; grid-template-columns: 1.15fr .85fr; gap: 14px; align-items:start; }
    .sa-grid-3{ display:grid; grid-template-columns: repeat(3,1fr); gap: 12px; }
    .sa-metric{ background: rgba(255,255,255,.92); border:1px solid rgba(148,163,184,.25); border-radius: 16px; padding: 12px; box-shadow: 0 10px 26px rgba(0,0,0,.06); }
    .sa-metric .k{ font-size: 12px; opacity:.75; margin-bottom: 6px; }
    .sa-metric .v{ font-size: 20px; font-weight: 950; color:#0f172a; }

    .sa-card{
      background: rgba(255,255,255,.95);
      border: 1px solid rgba(148,163,184,.25);
      border-radius: 18px;
      box-shadow: 0 10px 28px rgba(0,0,0,.06);
      overflow:hidden;
    }
    .sa-section{ padding: 14px; margin-bottom: 14px; }
    .sa-section-head{ display:flex; align-items:flex-start; justify-content:space-between; gap: 12px; margin-bottom: 12px; }
    .sa-h3{ margin:0; font-size: 15px; font-weight: 900; color:#0f766e; }
    .sa-sub{ margin:6px 0 0 0; font-size: 12px; opacity:.78; }

    .sa-badge{
      display:inline-block; padding: 4px 10px; border-radius: 999px; font-weight: 800; font-size: 12px;
      border: 1px solid rgba(148,163,184,.3);
      background: rgba(148,163,184,.10);
      color:#111827;
      white-space:nowrap;
    }
    .sa-ok{ background: rgba(34,197,94,.15); border-color: rgba(34,197,94,.35); color: #166534; }
    .sa-warn{ background: rgba(245,158,11,.18); border-color: rgba(245,158,11,.35); color: #92400e; }
    .sa-bad{ background: rgba(239,68,68,.14); border-color: rgba(239,68,68,.35); color: #b91c1c; }

    .sa-pill{
      display:inline-block; padding: 4px 10px; border-radius: 999px; font-weight: 900; font-size: 12px;
      border: 1px solid rgba(148,163,184,.25);
      background: rgba(148,163,184,.08);
      color:#111827;
      white-space:nowrap;
    }
    .sa-ink{ background: rgba(59,130,246,.12); border-color: rgba(59,130,246,.28); color:#1d4ed8; }
    .sa-amber{ background: rgba(245,158,11,.14); border-color: rgba(245,158,11,.30); color:#b45309; }
    .sa-muted{ background: rgba(148,163,184,.12); border-color: rgba(148,163,184,.25); color:#334155; }

    .sa-btn{
      padding: 8px 12px; border-radius: 12px; font-weight: 900; border: 1px solid rgba(0,0,0,.08);
      cursor:pointer; display:inline-block; font-size: 13px; text-decoration:none;
    }
    .sa-btn-primary{ background: #16a34a; border-color: rgba(22,163,74,.65); color:#fff; }
    .sa-btn-danger{ background: #ef4444; border-color: rgba(239,68,68,.65); color:#fff; }
    .sa-btn-ghost{ background: rgba(148,163,184,.10); border-color: rgba(148,163,184,.25); color:#0f172a; }

    .sa-table table{ width:100%; border-collapse:collapse; font-size: 13px; }
    .sa-table th{ background: rgba(236,253,245,.9); padding: 10px; text-align:left; font-weight: 950; color:#064e3b; border-bottom: 1px solid rgba(148,163,184,.25);}
    .sa-table td{ padding: 10px; border-bottom: 1px solid rgba(148,163,184,.18); vertical-align: middle; }
    .sa-charts{ display:grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 14px; }
    .sa-chart-card{ padding: 14px; }
    .sa-chart-wrap{ height: 280px; position: relative; }

    .sa-alerts{ display:grid; grid-template-columns: repeat(3,1fr); gap: 12px; margin-top: 12px; }
    .sa-alert-item{ padding: 12px; border-radius: 16px; border:1px solid rgba(148,163,184,.25); background: rgba(255,255,255,.9); box-shadow: 0 10px 24px rgba(0,0,0,.04); }
    .sa-alert-item .t{ font-size: 12px; opacity:.78; }
    .sa-alert-item .n{ font-size: 22px; font-weight: 1000; margin-top: 4px; }
    .sa-alert-item .d{ font-size: 12px; opacity:.82; margin-top: 6px; }

    @media(max-width: 980px){
      .sa-grid-2{ grid-template-columns: 1fr; }
      .sa-charts{ grid-template-columns: 1fr; }
      .sa-alerts{ grid-template-columns: 1fr; }
      .sa-grid-3{ grid-template-columns: 1fr; }
    }
    """

    body = f"""
<div class="sa-wrap">
  <style>{sa_css}</style>

  <div class="sa-top">
    <h2>🛡️ Panel Super Admin</h2>
    <p>Control multi-tenant con aprobaciones, suspensión y métricas tipo SaaS (memoria).</p>
    <form method="get" style="margin-top:12px">
      <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end">
        <div style="min-width:160px">
          <div style="font-size:12px;opacity:.8;font-weight:800;margin-bottom:6px">Desde</div>
          <input type="date" name="desde" value="{df.isoformat()}" style="width:100%;padding:9px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);"/>
        </div>
        <div style="min-width:160px">
          <div style="font-size:12px;opacity:.8;font-weight:800;margin-bottom:6px">Hasta</div>
          <input type="date" name="hasta" value="{dt.isoformat()}" style="width:100%;padding:9px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);"/>
        </div>
        <button class="sa-btn sa-btn-ghost" type="submit">Filtrar</button>
      </div>
    </form>
    <div class="sa-alerts">
      <div class="sa-alert-item">
        <div class="t">Admins con suscripción vencida</div>
        <div class="n"><span id="alertExpiredCount">{stats.get('alerts', {}).get('expired_admins_count', 0)}</span></div>
        <div class="d">{expired_list}</div>
      </div>
      <div class="sa-alert-item">
        <div class="t">Admins próximos a vencer (3 días)</div>
        <div class="n" style="color:#92400e;"><span id="alertSoonCount">{stats.get('alerts', {}).get('soon_admins_count', 0)}</span></div>
        <div class="d">{soon_list}</div>
      </div>
      <div class="sa-alert-item">
        <div class="t">Cobradores pendientes de aprobación</div>
        <div class="n" style="color:#1d4ed8;"><span id="alertPendingCollectorsCount">{pending_collectors_count}</span></div>
        <div class="d">Aprobación manual requerida antes de iniciar sesión.</div>
      </div>
    </div>
  </div>

  <div class="sa-grid-3">
    <div class="sa-metric">
      <div class="k">💰 Ganancia mensual</div>
      <div class="v" id="gainMonthly">{fmt_money(stats.get('gain_monthly_current', 0))}</div>
    </div>
    <div class="sa-metric">
      <div class="k">💰 Ganancia total</div>
      <div class="v" id="gainGlobal">{fmt_money(stats.get('gain_total_all_time', 0))}</div>
    </div>
    <div class="sa-metric">
      <div class="k">💰 Ganancia por rango</div>
      <div class="v" id="gainRange">{fmt_money(stats.get('gain_total_range', 0))}</div>
    </div>
  </div>

  <div class="sa-grid-2" style="margin-top:14px;">
    <div class="sa-metric">
      <div class="k">👥 Admins activos</div>
      <div class="v" id="adminsActiveCount">{stats.get('admins_active_count', 0)}</div>
    </div>
    <div class="sa-metric">
      <div class="k">⛔ Admins vencidos</div>
      <div class="v" id="adminsExpiredCount">{stats.get('admins_expired_count', 0)}</div>
    </div>
  </div>

  <div class="sa-charts">
    <div class="sa-card sa-chart-card">
      <div class="sa-section-head">
        <div>
          <h3 class="sa-h3">Ganancia del sistema (rango)</h3>
          <p class="sa-sub">Pagos de admins agrupados por fecha.</p>
        </div>
      </div>
      <div class="sa-chart-wrap"><canvas id="chartIncome"></canvas></div>
    </div>
    <div class="sa-card sa-chart-card">
      <div class="sa-section-head">
        <div>
          <h3 class="sa-h3">Ganancias mensuales</h3>
          <p class="sa-sub">Total por mes dentro del rango.</p>
        </div>
      </div>
      <div style="display:flex;gap:12px;align-items:flex-end;margin-bottom:10px;flex-wrap:wrap">
        <div style="font-size:12px;opacity:.78">Meses: <b>{len(stats.get('month_labels', []) or [])}</b></div>
      </div>
      <div class="sa-chart-wrap"><canvas id="chartPrestadoCobrado"></canvas></div>
    </div>
  </div>

  <div class="sa-grid-2">
    <div class="sa-card sa-section sa-table">
      <div class="sa-section-head">
        <div>
          <h3 class="sa-h3">Registrar pago (admins)</h3>
          <p class="sa-sub">Renueva suscripción y activa la cuenta si estaba suspendida.</p>
        </div>
      </div>
      <form method="post" style="margin-top:8px">
        <input type="hidden" name="action" value="register_admin_payment">
        <label style="display:block;font-size:12px;opacity:.8;font-weight:800;margin-top:6px">Admin</label>
        <select name="admin_id" required style="width:100%;padding:10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);">
          {payment_admin_opts}
        </select>
        <label style="display:block;font-size:12px;opacity:.8;font-weight:800;margin-top:10px">Monto pagado</label>
        <input name="amount" type="number" step="0.01" min="0" required style="width:100%;padding:10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);">
        <label style="display:block;font-size:12px;opacity:.8;font-weight:800;margin-top:10px">Fecha de pago</label>
        <input name="payment_date" type="date" value="{dt.isoformat()}" required style="width:100%;padding:10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);">
        <label style="display:block;font-size:12px;opacity:.8;font-weight:800;margin-top:10px">Método (opcional)</label>
        <input name="method" type="text" placeholder="Ej. Transferencia, Efectivo" style="width:100%;padding:10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);">
        <button class="sa-btn sa-btn-primary" type="submit" style="margin-top:12px;width:100%">Guardar pago</button>
      </form>
    </div>

    <div class="sa-card sa-section sa-table">
      <div class="sa-section-head">
        <div>
          <h3 class="sa-h3">Historial de pagos</h3>
          <p class="sa-sub">Filtrado por el rango seleccionado.</p>
        </div>
      </div>
      <div class="table-scroll" style="margin-top:8px">
        <table>
          <tr><th>Admin</th><th style="text-align:right">Monto</th><th>Fecha</th><th>Método</th></tr>
          {payments_rows or "<tr><td colspan='4' style='text-align:center;opacity:.85'>Sin pagos en el rango</td></tr>"}
        </table>
      </div>
    </div>
  </div>

  <div class="sa-grid-2">
    <div class="sa-card sa-section sa-table">
      <div class="sa-section-head">
        <div>
          <h3 class="sa-h3">Tabla de admins (tenants)</h3>
          <p class="sa-sub">Activación, suspensión y aprobación.</p>
        </div>
      </div>
      <div class="table-scroll">
        <table>
          <tr>
            <th>Admin</th><th>Estado</th><th>Expira</th><th style="text-align:right">Usuarios</th><th style="text-align:right">Clientes</th><th style="text-align:right">Préstamos</th><th style="text-align:right">Pend. cobradores</th><th></th>
          </tr>
          {admin_rows or "<tr><td colspan='8' style='text-align:center;opacity:.85'>Sin admins</td></tr>"}
        </table>
      </div>
    </div>

    <div class="sa-card sa-section sa-table">
      <div class="sa-section-head">
        <div>
          <h3 class="sa-h3">Tabla de usuarios</h3>
          <p class="sa-sub">Vista global por tenant (admin/cobrador).</p>
        </div>
      </div>
      <div class="table-scroll">
        <table>
          <tr><th>Tenant</th><th>Usuario</th><th>Rol</th><th>Estado</th><th>Tel</th><th>Fecha</th></tr>
          {users_rows or "<tr><td colspan='6' style='text-align:center;opacity:.85'>Sin usuarios</td></tr>"}
        </table>
      </div>
    </div>
  </div>

  {collectors_by_admin_html or "<div class='sa-card sa-section'><h3 class='sa-h3'>Cobradores</h3><p class='sa-sub'>No hay cobradores aún.</p></div>"}

  {nav_subfooter()}
  {charts_js}
</div>
"""
    return page(body)


@app.route("/super-admin/stats")
@login_required
@super_admin_required
def super_admin_stats():
    raw_from = request.args.get("desde", type=str)
    raw_to = request.args.get("hasta", type=str)
    df = None
    dt = None
    if raw_from:
        try:
            df = datetime.strptime(raw_from, "%Y-%m-%d").date()
        except ValueError:
            df = None
    if raw_to:
        try:
            dt = datetime.strptime(raw_to, "%Y-%m-%d").date()
        except ValueError:
            dt = None
    stats = compute_super_admin_stats(df, dt)
    return jsonify(stats)


@app.route("/reportes", methods=["GET", "POST"])
@login_required
@role_required("admin", "supervisor")
def reportes():
    ensure_org()
    oid = session.get("org_id")
    total_gastos_ruta = round(
        sum(float(e.get("amount") or 0) for e in store.route_expenses.values() if e.get("organization_id") == oid),
        2,
    )
    banco_disp = get_bank_available(oid)
    descuento_income = round(
        sum(
            float(cr.get("amount") or 0)
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == oid and cr.get("movement_type") == "descuento_inicial"
        ),
        2,
    )
    body = (
        f'<div class="card"><h2>📊 Reportes</h2>'
        f'<a class="btn btn-secondary" href="{url_for("reportes_cobradores")}">Por cobrador</a> '
        f'<a class="btn btn-secondary" href="{url_for("dashboard")}">Dashboard</a>'
        f'<div style="margin-top:12px">'
        f'<p><b>Banco disponible:</b> {fmt_money(banco_disp)}</p>'
        f'<p><b>Gastos de ruta (total):</b> {fmt_money(total_gastos_ruta)}</p>'
        f'<p><b>Ingreso por descuentos iniciales:</b> {fmt_money(descuento_income)}</p>'
        f'</div>'
        f"<p>Préstamos: {len(store.loans)} · Clientes: {len(store.clients)}</p></div>"
    )
    return page(body)


@app.route("/reportes/cobradores", methods=["GET", "POST"])
@login_required
@role_required("admin", "supervisor")
def reportes_cobradores():
    return stub_page("Reportes por cobrador")


@app.route("/ruta/resumen")
@login_required
def ruta_resumen():
    return stub_page("Resumen de ruta")


@app.route("/prestamos/pagados")
@login_required
def prestamos_pagados():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    rows = [
        L for L in loans_for_user(org_id, user)
        if str(L.get("status", "")).lower() == "cerrado"
    ]
    t = "".join(
        f"<tr><td>#{L['id']}</td><td>{fmt_money(L.get('amount'))}</td>"
        f"<td><a class='btn btn-secondary' href='{url_for('loan_detail', loan_id=L['id'])}'>Ver</a></td></tr>"
        for L in rows
    )
    return page(
        f'<div class="card"><h2>Préstamos pagados / cerrados</h2><div class="table-scroll">'
        f'<table><tr><th>ID</th><th>Monto original</th><th></th></tr>{t or "<tr><td colspan=3>Ninguno</td></tr>"}</table></div>{nav_subfooter()}</div>'
    )


@app.route("/gps/update", methods=["POST"])
@login_required
def gps_update():
    uid = session.get("user_id")
    store.gps_positions[uid] = {
        "lat": request.form.get("lat"), "lng": request.form.get("lng"), "ts": datetime.utcnow(),
    }
    return jsonify({"ok": True})


@app.route("/bank/collector-map")
@login_required
def collector_map():
    ensure_org()
    oid = session.get("org_id")
    user = current_user()

    def fmt_ts(ts):
        if not ts:
            return "—"
        try:
            return ts.strftime("%d/%m/%Y %I:%M %p")
        except Exception:
            return str(ts)

    # La geolocalización se registra para el usuario logueado (cada cobrador debe abrir esta pantalla).
    me_pos = store.gps_positions.get(user.get("id"))

    role = (user.get("role") or "").strip().lower()
    is_adminish = role in ("admin", "supervisor", "super_admin") or is_cartera_admin(user)
    if is_adminish:
        if role == "super_admin":
            collectors = [
                u
                for u in store.users.values()
                if u.get("role") == "cobrador"
            ]
        else:
            collectors = [
                u
                for u in store.users.values()
                if u.get("organization_id") == oid and u.get("role") == "cobrador"
            ]
        collectors.sort(key=lambda u: str(u.get("name") or u.get("username") or "").lower())
        rows = ""
        for c in collectors:
            p = store.gps_positions.get(c.get("id"))
            lat = p.get("lat") if p else None
            lng = p.get("lng") if p else None
            rows += (
                "<tr>"
                f"<td>{html.escape(str(c.get('name') or c.get('username') or '—'))}</td>"
                f"<td>{html.escape(str(lat) if lat else '—')}</td>"
                f"<td>{html.escape(str(lng) if lng else '—')}</td>"
                f"<td>{fmt_ts(p.get('ts')) if p else '—'}</td>"
                "</tr>"
            )
        table = (
            "<div class='table-scroll'>"
            "<table><tr><th>Cobrador</th><th>Lat</th><th>Lng</th><th>Actualizado</th></tr>"
            f"{rows or '<tr><td colspan=4 style=\"text-align:center;opacity:.85\">No hay cobradores con posiciones aún</td></tr>'}"
            "</table></div>"
        )
        extra = (
            "<p style='margin:0 0 10px 0;opacity:.9;font-size:14px'>"
            "Para ver ubicaciones, cada cobrador debe abrir esta pantalla con su GPS activo."
            "</p>"
            f"{table}"
        )
    else:
        extra = f"<p>{('Última posición registrada: ' + str(me_pos.get('ts')) ) if me_pos else 'Sin posición (activa GPS en el navegador).'} </p>" \
            f"<p style='margin-top:6px;opacity:.9;font-size:14px'>Abre el mapa para registrar tu ubicación automáticamente.</p>"

    js = """
    <script>
      async function sendPos(pos){
        if(!pos || !pos.coords) return;
        const lat = pos.coords.latitude;
        const lng = pos.coords.longitude;
        const body = new URLSearchParams();
        body.append('lat', lat);
        body.append('lng', lng);
        try{
          await fetch('/gps/update', {
            method: 'POST',
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            body: body.toString()
          });
        }catch(e){}
      }

      function startGps(){
        if(!navigator.geolocation) return;
        function tick(){
          navigator.geolocation.getCurrentPosition(
            (p)=>sendPos(p),
            ()=>{},
            {enableHighAccuracy:true, maximumAge:15000, timeout:8000}
          );
        }
        tick();
        setInterval(tick, 60000);
      }
      window.addEventListener('load', startGps);
    </script>
    """

    body = (
        "<div class='card'>"
        "<h2>Mapa cobrador</h2>"
        f"{extra}"
        f"{js}"
        "</div>"
    )
    return page(body)


@app.route("/advance/delete/<int:payment_id>", methods=["POST"])
@login_required
def delete_advance(payment_id):
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    p = store.payments.get(payment_id)
    if not p:
        flash("Pago no encontrado.", "danger")
        return redirect(url_for("bank_advance"))
    loan_id = p.get("loan_id")
    L = store.loans.get(loan_id) if loan_id else None
    if L and L.get("organization_id") != oid:
        flash("Sin acceso.", "danger")
        return redirect(url_for("bank_advance"))
    if L and loan_id not in loan_ids_visible(oid, user):
        flash("Sin acceso.", "danger")
        return redirect(url_for("bank_advance"))
    if user.get("role") != "admin" and not is_cartera_admin(user):
        flash("Solo admin puede eliminar adelantos.", "danger")
        return redirect(url_for("bank_advance"))

    is_adv = str(p.get("type") or "").strip().lower() == "adelanto" or int(p.get("weeks_advanced") or 0) > 0
    if not is_adv:
        flash("Este registro no es un adelanto.", "danger")
        return redirect(url_for("bank_advance"))

    amt = float(p.get("amount") or 0)
    weeks = int(p.get("weeks_advanced") or 0)
    if str(p.get("type") or "").strip().lower() == "adelanto" and weeks < 1:
        weeks = 1

    # Revertir ingreso al banco por ese pago.
    try:
        apply_cash_movement(
            movement_type="reverso_adelanto",
            amount=-amt,
            note=f"Reverso adelanto pago #{payment_id} préstamo #{loan_id}",
            user_id=user["id"],
            org_id=oid,
        )
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("bank_advance"))

    if L:
        L["remaining"] = float(L.get("remaining") or 0) + amt
        if str(L.get("status") or "").lower() == "cerrado" and L["remaining"] > 0:
            L["status"] = "ACTIVO"
        if weeks > 0 and L.get("next_payment_date") and hasattr(L.get("next_payment_date"), "strftime"):
            interval = freq_interval_days(L.get("frequency"))
            L["next_payment_date"] = L["next_payment_date"] - timedelta(days=weeks * interval)

    store.payments.pop(payment_id, None)
    flash("Adelanto eliminado.", "success")
    return redirect(url_for("bank_advance"))


# --- Vistas banco / informes (memoria; sin persistencia extra) ---
@app.route("/bank/legal")
@login_required
def bank_legal():
    return redirect(url_for("bank_legal_list"))


@app.route("/bank/legal/list")
@login_required
def bank_legal_list():
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    rows = []

    for L in sorted(loans_for_user(oid, user), key=lambda x: -x.get("id", 0))[:200]:
        client = store.clients.get(L.get("client_id"), {})
        cobrador = store.users.get(L.get("created_by"), {}).get("username") or "—"
        firmado = bool(L.get("signature_b64"))
        status_label = "Firmado" if firmado else "Pendiente"
        status_color = "#16a34a" if firmado else "#d97706"
        nm = f"{client.get('first_name','')} {client.get('last_name') or ''}".strip() or f"Cliente #{client.get('id')}"
        rows.append(
            "<div class='legal-card' style='background:#ffffffb8;border-radius:18px;padding:12px;box-shadow:0 8px 22px rgba(0,0,0,.06);'>"
            f"<div style='display:flex;justify-content:space-between;gap:10px;align-items:flex-start;'>"
            f"<div style='font-weight:900;line-height:1.2'>{nm}</div>"
            f"<div style='font-weight:900;color:{status_color}'>{status_label}</div>"
            f"</div>"
            f"<div style='opacity:.85;margin-top:6px;font-size:12px;'>Cobrador: {cobrador}</div>"
            f"<div style='opacity:.85;margin-top:3px;font-size:12px;'>Prestamo: #{L.get('id')}</div>"
            f"<div style='margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;'>"
            f"<a class='btn btn-secondary' style='padding:7px 12px' href='{url_for('view_legal_document', loan_id=L.get('id'))}'>Ver</a>"
            f"</div>"
            f"</div>"
        )

    cards = "".join(rows) if rows else "<div style='opacity:.85;padding:12px'>Sin documentos legales</div>"
    body = f"""
<div class="card" style="padding:16px;">
  <h2 style="margin:0 0 10px 0;">Documento legal</h2>
  <style>
    .legal-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;}}
  </style>
  <div class="legal-grid">{cards}</div>
  {nav_subfooter()}
</div>
"""
    return page(body)


@app.route("/bank/legal/view/<int:loan_id>")
@login_required
def view_legal_document(loan_id):
    ensure_org()
    oid = session.get("org_id")
    loan = store.loans.get(loan_id)
    if not loan or loan.get("organization_id") != oid:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("bank_legal_list"))
    # Control de acceso para cobradores.
    if current_user().get("role") == "cobrador" and not (is_cartera_admin(current_user()) or loan.get("created_by") == current_user()["id"]):
        flash("Sin acceso.", "danger")
        return redirect(url_for("bank_legal_list"))

    client = store.clients.get(loan.get("client_id"), {})
    cob_u = store.users.get(loan.get("created_by"), {})
    cobrador = cob_u.get("name") or cob_u.get("username") or "—"

    capital_aprobado = float(loan.get("amount") or 0)
    interes_total = float(loan.get("total_interest") or 0)
    start_date = loan.get("start_date")
    doc_raw = (client.get("document_id") or "").strip()
    cedula = doc_raw if doc_raw else "No tiene"
    cliente_nombre = f"{client.get('first_name','')} {client.get('last_name') or ''}".strip() or "—"

    # Mostrar previews si ya subieron fotos/firmaron.
    sig = loan.get("signature_b64")
    id_front = loan.get("id_photo_b64")
    id_back = loan.get("id_photo_back_b64")

    preview_sig = (
        f"<div style='margin-top:10px;'><img alt='Firma' style='max-width:260px;border:1px solid rgba(0,0,0,.08);border-radius:12px;background:#fff;padding:8px;' src='{sig}'/></div>"
        if sig else ""
    )

    preview_front = (
        f"<div style='margin-top:6px;'><img alt='ID frente' style='width:140px;height:92px;object-fit:cover;border:1px solid rgba(0,0,0,.08);border-radius:10px;background:#fff;' src='{id_front}'/></div>"
        if id_front else ""
    )
    preview_back = (
        f"<div style='margin-top:6px;'><img alt='ID atrás' style='width:140px;height:92px;object-fit:cover;border:1px solid rgba(0,0,0,.08);border-radius:10px;background:#fff;' src='{id_back}'/></div>"
        if id_back else ""
    )

    signature_script = """
<script>
  const canvas = document.getElementById('sigCanvas');
  const ctx = canvas.getContext('2d');
  let drawing = false;
  function pos(e){
    const r = canvas.getBoundingClientRect();
    const x = (e.clientX - r.left) * (canvas.width / r.width);
    const y = (e.clientY - r.top) * (canvas.height / r.height);
    return {x,y};
  }
  function resize(){
    const ratio = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = Math.floor(rect.width * ratio);
    canvas.height = Math.floor(rect.height * ratio);
    ctx.fillStyle = '#fff';
    ctx.fillRect(0,0,canvas.width,canvas.height);
    ctx.lineWidth = 3 * ratio;
    ctx.lineCap = 'round';
    ctx.strokeStyle = '#000';
  }
  window.addEventListener('resize', resize);
  resize();
  canvas.addEventListener('mousedown', e=>{drawing=true; const p=pos(e); ctx.beginPath(); ctx.moveTo(p.x,p.y);});
  canvas.addEventListener('mouseup', ()=>{drawing=false; ctx.closePath();});
  canvas.addEventListener('mousemove', e=>{if(!drawing) return; const p=pos(e); ctx.lineTo(p.x,p.y); ctx.stroke();});
  canvas.addEventListener('touchstart', e=>{drawing=true; const t=e.touches[0]; const p=pos(t); ctx.beginPath(); ctx.moveTo(p.x,p.y); e.preventDefault();});
  canvas.addEventListener('touchend', ()=>{drawing=false; ctx.closePath();});
  canvas.addEventListener('touchmove', e=>{if(!drawing) return; const t=e.touches[0]; const p=pos(t); ctx.lineTo(p.x,p.y); ctx.stroke(); e.preventDefault();});
  function clearSig(){ctx.clearRect(0,0,canvas.width,canvas.height); ctx.fillStyle='#fff'; ctx.fillRect(0,0,canvas.width,canvas.height);}

  const signForm = document.querySelector(\"form[action*='/bank/legal/sign/']\");
  if (signForm) {
    signForm.addEventListener('submit', ()=>{document.getElementById('signature_b64').value = canvas.toDataURL('image/png');});
  }
  function legalScrollFirma() {{
    const c = document.getElementById('sigCanvas');
    if (c) c.scrollIntoView({{behavior:'smooth', block:'center'}});
  }}
</script>
"""

    cliente_nombre_esc = html.escape(cliente_nombre)
    cedula_esc = html.escape(str(cedula))
    cobrador_esc = html.escape(str(cobrador))
    fecha_inicio_txt = (
        start_date.strftime("%Y-%m-%d")
        if start_date and hasattr(start_date, "strftime")
        else (str(start_date) if start_date else "—")
    )
    fecha_inicio_esc = html.escape(fecha_inicio_txt)

    body = f"""
<style>
.legal-contract-wrap {{
  background: linear-gradient(180deg,#ecfdf5 0%,#f0fdf4 48%,#ecfdf5 100%);
  border-radius: 22px;
  padding: 18px 16px 26px;
  border: 1px solid rgba(22,163,74,.2);
  box-shadow: 0 10px 32px rgba(0,0,0,.07);
  margin-bottom: 14px;
}}
.legal-back {{
  display: inline-block;
  background: #e5e7eb;
  color: #374151;
  padding: 9px 18px;
  border-radius: 999px;
  text-decoration: none !important;
  font-weight: 800;
  font-size: 14px;
  margin-bottom: 14px;
  border: none;
  box-shadow: 0 2px 8px rgba(0,0,0,.06);
}}
.legal-back:hover {{ filter: brightness(0.97); }}
.legal-title {{
  margin: 0 0 16px 0;
  font-size: 1.45rem;
  font-weight: 900;
  color: #14532d;
  letter-spacing: -0.02em;
}}
.legal-dl {{ font-size: 14px; color: #166534; line-height: 1.65; margin: 0 0 18px 0; }}
.legal-dl div {{ margin: 2px 0; }}
.legal-dl b {{ color: #14532d; font-weight: 800; min-width: 9rem; display: inline-block; }}
.legal-sep {{ height: 1px; background: rgba(22,101,52,.2); margin: 16px 0; border: 0; }}
.legal-h3 {{
  margin: 0 0 10px 0;
  font-size: 1.05rem;
  font-weight: 900;
  color: #14532d;
}}
.legal-compromiso p {{
  margin: 0 0 10px 0;
  font-size: 14px;
  line-height: 1.55;
  color: #1e293b;
}}
.legal-compromiso p:last-child {{ margin-bottom: 0; }}
.legal-id-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  margin-top: 12px;
}}
@media (max-width: 560px) {{ .legal-id-grid {{ grid-template-columns: 1fr; }} }}
.legal-upload-form {{ margin: 0; }}
.legal-upload-label {{
  display: block;
  cursor: pointer;
  margin: 0;
}}
.legal-upload-btn {{
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  min-height: 72px;
  padding: 14px 12px;
  background: linear-gradient(180deg,#64748b,#475569);
  color: #fff !important;
  font-weight: 800;
  font-size: 14px;
  border-radius: 16px;
  box-shadow: 0 8px 22px rgba(15,23,42,.22);
  transition: transform .12s ease, filter .12s ease;
  border: none;
}}
.legal-upload-btn:hover {{ filter: brightness(1.06); transform: translateY(-1px); }}
.legal-upload-hint {{ font-size: 12px; opacity: .85; margin-top: 8px; color: #166534; text-align: center; }}
.legal-firma-actions {{
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  justify-content: center;
  margin: 14px 0 18px 0;
}}
.legal-btn-sign {{
  border: none;
  cursor: pointer;
  padding: 14px 22px;
  border-radius: 999px;
  font-weight: 900;
  font-size: 15px;
  color: #fff;
  background: linear-gradient(135deg,#2563eb,#3b82f6);
  box-shadow: 0 10px 26px rgba(37,99,235,.45);
  display: inline-flex;
  align-items: center;
  gap: 8px;
}}
.legal-btn-sign:hover {{ filter: brightness(1.05); }}
.legal-btn-print {{
  border: none;
  cursor: pointer;
  padding: 14px 22px;
  border-radius: 999px;
  font-weight: 900;
  font-size: 15px;
  color: #fff;
  background: linear-gradient(135deg,#15803d,#22c55e);
  box-shadow: 0 10px 26px rgba(22,163,74,.4);
  display: inline-flex;
  align-items: center;
  gap: 8px;
}}
.legal-btn-print:hover {{ filter: brightness(1.05); }}
.legal-sig-box {{
  background: #fff;
  border-radius: 16px;
  border: 1px solid rgba(22,101,52,.15);
  padding: 14px;
  box-shadow: inset 0 1px 0 rgba(255,255,255,.8);
}}
.legal-sig-box .hint {{ font-size: 12px; color: #64748b; margin-bottom: 8px; }}
@media print {{
  .no-print-legal, header.topbar, .premium-btn, .menu-overlay, .side-menu, nav, .container > .card:first-child {{ display: none !important; }}
  .legal-contract-wrap {{ box-shadow: none !important; border: none !important; background: #fff !important; }}
}}
</style>
<div class="legal-contract-wrap">
  <a class="legal-back no-print-legal" href="{url_for('bank_legal_list')}">← Volver</a>
  <h1 class="legal-title">Contrato de Préstamo</h1>
  <div class="legal-dl">
    <div><b>Cliente:</b> {cliente_nombre_esc}</div>
    <div><b>Cédula:</b> {cedula_esc}</div>
    <div><b>Capital aprobado:</b> {html.escape(fmt_money(capital_aprobado))}</div>
    <div><b>Interés total:</b> {html.escape(fmt_money(interes_total))}</div>
    <div><b>Fecha inicio:</b> {fecha_inicio_esc}</div>
    <div><b>Cobrador:</b> {cobrador_esc}</div>
  </div>

  <hr class="legal-sep">
  <div class="legal-compromiso">
    <h3 class="legal-h3">📄 Compromiso de Pago</h3>
    <p>
      El cliente <b>{cliente_nombre_esc}</b> reconoce haber recibido el capital del préstamo y se compromete de manera expresa, voluntaria e irrevocable a pagar la totalidad de la deuda a <b>JDM CASH NOW</b>, incluyendo capital, intereses, cargos y penalidades aplicables, en los plazos establecidos.
    </p>
    <p>
      El incumplimiento de este compromiso autoriza a <b>JDM CASH NOW</b> a iniciar las acciones legales correspondientes conforme a la ley vigente.
    </p>
  </div>

  <hr class="legal-sep">
  <h3 class="legal-h3">Cédula del cliente</h3>
  <div class="legal-id-grid no-print-legal">
    <form class="legal-upload-form" method="post" enctype="multipart/form-data" action="{url_for('upload_id_front', loan_id=loan_id)}">
      <label class="legal-upload-label">
        <span class="legal-upload-btn">Subir cédula (frente)</span>
        <input name="id_front" type="file" accept="image/*" required style="display:none" onchange="if(this.files.length)this.form.submit()">
      </label>
    </form>
    <form class="legal-upload-form" method="post" enctype="multipart/form-data" action="{url_for('upload_id_back', loan_id=loan_id)}">
      <label class="legal-upload-label">
        <span class="legal-upload-btn">Subir cédula (atrás)</span>
        <input name="id_back" type="file" accept="image/*" required style="display:none" onchange="if(this.files.length)this.form.submit()">
      </label>
    </form>
  </div>
  <div style="display:flex;gap:14px;flex-wrap:wrap;justify-content:center;margin-top:12px;">
    {preview_front}
    {preview_back}
  </div>

  <hr class="legal-sep">
  <section id="firmaCliente">
    <h3 class="legal-h3">Firma del cliente</h3>
    <div class="legal-firma-actions no-print-legal">
      <button type="button" class="legal-btn-sign" onclick="legalScrollFirma()">🖊 Firmar documento</button>
      <button type="button" class="legal-btn-print" onclick="window.print()">🖨 Imprimir contrato</button>
    </div>
    <form method="post" action="{url_for('sign_legal_document', loan_id=loan_id)}">
      <div class="legal-sig-box">
        <div class="hint">Dibuje su firma aquí (mouse o dedo) y pulse Guardar firma.</div>
        <canvas id="sigCanvas" style="width:100%;max-width:520px;height:130px;border:1px dashed rgba(21,83,45,.35);border-radius:14px;background:#fafafa;display:block;margin:0 auto;"></canvas>
        <div style="display:flex;gap:10px;flex-wrap:wrap;justify-content:center;margin-top:12px;">
          <button class="btn btn-secondary" type="button" onclick="clearSig()">Limpiar</button>
          <button class="btn btn-primary" type="submit">Guardar firma</button>
        </div>
      </div>
      <input type="hidden" name="signature_b64" id="signature_b64">
    </form>
    <div style="text-align:center;margin-top:12px;">{preview_sig}</div>
  </section>
</div>
{signature_script}
<p class="no-print-legal" style="margin-top:20px"><a class="btn btn-secondary" href="{url_for("dashboard")}">← Dashboard</a>
<a class="btn btn-secondary" href="{url_for("bank_home")}">Banco</a></p>
"""
    return page(body)


@app.route("/bank/legal/upload-id-front/<int:loan_id>", methods=["POST"])
@login_required
def upload_id_front(loan_id):
    ensure_org()
    oid = session.get("org_id")
    loan = store.loans.get(loan_id)
    if not loan or loan.get("organization_id") != oid:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("bank_legal_list"))
    if current_user().get("role") == "cobrador" and not (is_cartera_admin(current_user()) or loan.get("created_by") == current_user()["id"]):
        flash("Sin acceso.", "danger")
        return redirect(url_for("bank_legal_list"))

    f = request.files.get("id_front")
    if not f:
        flash("Debe seleccionar la imagen del frente.", "danger")
        return redirect(url_for("view_legal_document", loan_id=loan_id))

    raw = f.read()
    if not raw:
        flash("Archivo vacío.", "danger")
        return redirect(url_for("view_legal_document", loan_id=loan_id))

    b64 = base64.b64encode(raw).decode("ascii")
    loan["id_photo_b64"] = f"data:image/png;base64,{b64}"
    flash("ID (frente) guardada en memoria.", "success")
    return redirect(url_for("view_legal_document", loan_id=loan_id))


@app.route("/bank/legal/upload-id-back/<int:loan_id>", methods=["POST"])
@login_required
def upload_id_back(loan_id):
    ensure_org()
    oid = session.get("org_id")
    loan = store.loans.get(loan_id)
    if not loan or loan.get("organization_id") != oid:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("bank_legal_list"))
    if current_user().get("role") == "cobrador" and not (is_cartera_admin(current_user()) or loan.get("created_by") == current_user()["id"]):
        flash("Sin acceso.", "danger")
        return redirect(url_for("bank_legal_list"))

    f = request.files.get("id_back")
    if not f:
        flash("Debe seleccionar la imagen de atrás.", "danger")
        return redirect(url_for("view_legal_document", loan_id=loan_id))

    raw = f.read()
    if not raw:
        flash("Archivo vacío.", "danger")
        return redirect(url_for("view_legal_document", loan_id=loan_id))

    b64 = base64.b64encode(raw).decode("ascii")
    loan["id_photo_back_b64"] = f"data:image/png;base64,{b64}"
    flash("ID (atrás) guardada en memoria.", "success")
    return redirect(url_for("view_legal_document", loan_id=loan_id))


@app.route("/bank/legal/sign/<int:loan_id>", methods=["GET", "POST"])
@login_required
def sign_legal_document(loan_id):
    # Nota: mostramos el formulario completo en `view_legal_document`.
    # Este endpoint solo guarda la firma al hacer POST.
    if request.method == "POST":
        ensure_org()
        oid = session.get("org_id")
        loan = store.loans.get(loan_id)
        if not loan or loan.get("organization_id") != oid:
            flash("Préstamo no encontrado.", "danger")
            return redirect(url_for("bank_legal_list"))

        if current_user().get("role") == "cobrador" and not (is_cartera_admin(current_user()) or loan.get("created_by") == current_user()["id"]):
            flash("Sin acceso.", "danger")
            return redirect(url_for("bank_legal_list"))

        sig = request.form.get("signature_b64")
        if not sig or not str(sig).startswith("data:"):
            flash("Firma inválida.", "danger")
            return redirect(url_for("view_legal_document", loan_id=loan_id))

        loan["signature_b64"] = sig
        flash("Firma guardada en memoria.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    # GET: redirigir a la vista completa del contrato.
    return redirect(url_for("view_legal_document", loan_id=loan_id))


@app.route("/bank/advance", methods=["GET", "POST"])
@login_required
def bank_advance():
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    can_del = user.get("role") == "admin" or is_cartera_admin(user)

    def payment_ts(p):
        ts = p.get("created_at")
        if ts:
            return fmt_advance_datetime(ts)
        d = p.get("date")
        if hasattr(d, "strftime"):
            return d.strftime("%d/%m/%Y") + " 12:00 AM"
        return "—"

    advances = []
    for p in payments_in_scope(oid, user):
        if (p.get("status") or "OK") == "ANULADO":
            continue
        typ = str(p.get("type") or "").strip().lower()
        wk = int(p.get("weeks_advanced") or 0)
        if typ != "adelanto" and wk <= 0:
            continue
        advances.append(p)

    advances.sort(key=lambda x: (x.get("created_at") or datetime.min, x.get("id", 0)), reverse=True)

    rows = []
    for p in advances[:300]:
        lid = p.get("loan_id")
        L = store.loans.get(lid, {})
        cid = L.get("client_id")
        cl = store.clients.get(cid, {})
        nm = f"{cl.get('first_name','')} {cl.get('last_name') or ''}".strip() or f"Cliente #{cid}"
        wk_disp = int(p.get("weeks_advanced") or 1)
        del_form = (
            f'<form method="post" action="{url_for("delete_advance", payment_id=p["id"])}" style="display:inline; margin:0" '
            f'onsubmit="return confirm(\'¿Eliminar este adelanto?\');">'
            f'<button type="submit" class="btn-adv-del">🗑 Eliminar</button></form>'
            if can_del
            else "<span style='opacity:.55'>—</span>"
        )
        rows.append(
            "<tr>"
            f"<td>{lid}</td>"
            f"<td>{nm}</td>"
            f"<td>{wk_disp}</td>"
            f"<td class='adv-monto'>{fmt_money(p.get('amount'))}</td>"
            f"<td class='adv-fecha'>{payment_ts(p)}</td>"
            f"<td>{del_form}</td>"
            "</tr>"
        )

    tbody = "".join(rows) if rows else (
        "<tr><td colspan='6' style='text-align:center;opacity:.85;padding:16px'>Sin pagos adelantados</td></tr>"
    )

    body = f"""
<div class="card adv-wrap" style="padding:16px;background:#ecfdf5;border:1px solid rgba(22,163,74,.18);">
  <style>
    .adv-wrap h2{{margin:0 0 14px 0;font-size:1.35rem;font-weight:900;color:#14532d}}
    .adv-table{{width:100%;border-collapse:collapse;background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 4px 18px rgba(0,0,0,.06)}}
    .adv-table th{{text-align:left;padding:12px 14px;background:#dcfce7;color:#14532d;font-weight:900;font-size:13px}}
    .adv-table td{{padding:11px 14px;border-bottom:1px solid rgba(148,163,184,.2);font-size:14px}}
    .adv-table tr:last-child td{{border-bottom:none}}
    .adv-monto{{color:#16a34a;font-weight:900}}
    .adv-fecha{{font-variant-numeric:tabular-nums;color:#334155}}
    .btn-adv-del{{background:#dc2626;color:#fff;border:none;border-radius:999px;padding:8px 14px;font-weight:800;cursor:pointer;font-size:13px}}
    .btn-adv-del:hover{{filter:brightness(1.05)}}
  </style>
  <h2>⏩ Pagos adelantados</h2>
  <div class="table-scroll">
    <table class="adv-table">
      <tr><th>Préstamo</th><th>Cliente</th><th>Semanas adelantadas</th><th>Monto</th><th>Fecha</th><th>Acción</th></tr>
      {tbody}
    </table>
  </div>
  {nav_subfooter()}
</div>
"""
    return page(body)


@app.route("/ruta/agregar-capital", methods=["POST"])
@login_required
def agregar_capital_ruta():
    flash("Capital agregado (memoria).", "success")
    return redirect(url_for("bank_home"))


@app.route("/bank/daily-list", methods=["GET", "POST"])
@login_required
def bank_daily_list():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    prox_sab, pack = loans_cobro_sabado_semanal(org_id, user)
    tr = ""
    for npd, L, nm in pack:
        nm_e = html.escape(nm)
        pay_href = url_for("new_payment", loan_id=L["id"])
        tr += (
            f"<tr><td>{nm_e}</td><td>#{L['id']}</td><td>{npd}</td><td>{html.escape(fmt_money(L.get('installment_amount')))}</td>"
            f"<td>{html.escape(fmt_money(L.get('remaining')))}</td>"
            f"<td><a class='btn btn-primary' href='{pay_href}' data-pay-href='{pay_href}' onclick='return openPayConfirm(this)'>Pagar</a></td></tr>"
        )
    pay_confirm_modal = """
    <div id="payConfirmModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:99999">
      <div style="background:#fff;max-width:420px;margin:18vh auto;padding:16px;border-radius:16px;box-shadow:0 14px 40px rgba(0,0,0,.25)">
        <h3 style="margin:0 0 8px 0;color:#14532d;font-weight:900">Confirmar pago</h3>
        <p style="margin:0 0 12px 0;opacity:.92">¿Aceptar y abrir el formulario de pago para este préstamo?</p>
        <div style="display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap">
          <button type="button" class="btn btn-secondary" onclick="closePayConfirm()">Cancelar</button>
          <button type="button" class="btn btn-primary" onclick="acceptPayConfirm()">Aceptar</button>
        </div>
      </div>
    </div>
    <script>
      let payConfirmHref = null;
      function openPayConfirm(el){
        payConfirmHref = el.getAttribute('data-pay-href') || el.getAttribute('href');
        const m = document.getElementById('payConfirmModal');
        if (m) m.style.display = 'block';
        return false;
      }
      function closePayConfirm(){
        const m = document.getElementById('payConfirmModal');
        if (m) m.style.display = 'none';
        payConfirmHref = null;
      }
      function acceptPayConfirm(){
        if (payConfirmHref) window.location = payConfirmHref;
      }
    </script>
    """
    body = (
        f'<div class="card" style="padding:16px;background:#ecfdf5;border:1px solid rgba(22,163,74,.15);">'
        f'<h2 style="margin:0 0 8px 0;color:#14532d;">📋 Lista diaria — cobro sábados</h2>'
        f'<p style="margin:0 0 14px 0;opacity:.92;font-size:14px;color:#166534;">'
        f"Préstamos <b>semanales</b> activos con cuota a cobrar a más tardar el sábado <b>{prox_sab.strftime('%d/%m/%Y')}</b> "
        f"(incluye hoy si es sábado). Disponible cualquier día de la semana."
        f"</p>"
        f'<div class="table-scroll"><table><tr>'
        f"<th>Cliente</th><th>Prést.</th><th>Próx. pago</th><th>Cuota est.</th><th>Saldo</th><th></th></tr>"
        f"{tr or '<tr><td colspan=6 style=\"text-align:center;opacity:.85\">Ninguno para este sábado de cobro</td></tr>'}"
        f"</table></div>{pay_confirm_modal}{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/bank/expenses", methods=["GET", "POST"])
@login_required
def bank_expenses():
    ensure_org()
    oid = session.get("org_id")

    kind_options = "".join(
        f'<option value="{k}">{ico} {lbl}</option>' for k, (lbl, ico) in ROUTE_EXPENSE_KINDS.items()
    )

    expense_list = sorted(
        (e for e in store.route_expenses.values() if e.get("organization_id") == oid),
        key=lambda x: x.get("created_at") or datetime.min,
        reverse=True,
    )

    tbody_rows = []
    for idx, e in enumerate(expense_list):
        kind = e.get("kind") or "otros"
        lbl, ico = route_expense_kind_info(kind)
        row_bg = "#ecfdf5" if idx % 2 == 0 else "#ffffff"
        edit_btn = f'<a class="route-act route-act-edit" href="{url_for("edit_expense", expense_id=e["id"])}" title="Editar">✏</a>'
        del_form = (
            f'<form method="post" action="{url_for("delete_route_expense", expense_id=e["id"])}" style="display:inline" '
            f'onsubmit="return confirm(\'¿Eliminar este gasto?\');">'
            f'<button type="submit" class="route-act route-act-del" title="Eliminar">🗑</button></form>'
        )
        tbody_rows.append(
            f"<tr style='background:{row_bg};'>"
            f"<td>{e.get('route') or '—'}</td>"
            f"<td><span class='route-tipo'>{ico} {lbl}</span></td>"
            f"<td class='route-monto'>💵 {fmt_money(e.get('amount'))}</td>"
            f"<td>{e.get('note') or '—'}</td>"
            f"<td class='route-fecha'>{fmt_expense_datetime(e.get('created_at'))}</td>"
            f"<td style='white-space:nowrap'>{edit_btn} {del_form}</td>"
            f"</tr>"
        )

    rows = "".join(tbody_rows) if tbody_rows else (
        "<tr><td colspan='6' style='opacity:.85;text-align:center;padding:16px'>Sin gastos registrados</td></tr>"
    )

    body = f"""
<div class="card route-exp-wrap" style="padding:16px;background:linear-gradient(180deg,#ecfdf5,#f8fffc);border:1px solid rgba(22,163,74,.2);">
  <style>
    .route-exp-wrap h2{{margin:0 0 4px 0;font-size:1.25rem;font-weight:900;color:#14532d}}
    .route-exp-wrap h3{{margin:18px 0 10px 0;font-size:1.05rem;font-weight:900;color:#14532d}}
    .route-form-row{{display:flex;flex-wrap:wrap;gap:12px 16px;align-items:flex-end;margin-top:10px}}
    .route-field{{flex:1 1 140px;min-width:120px}}
    .route-field label{{display:block;font-size:12px;font-weight:800;color:#166534;margin-bottom:4px}}
    .route-field input,.route-field select{{width:100%;padding:10px 12px;border-radius:12px;border:1px solid rgba(22,101,52,.25);background:#fff;font-size:14px}}
    .route-save{{flex:0 0 auto;padding:11px 20px;border-radius:999px;border:none;background:#16a34a;color:#fff;font-weight:900;cursor:pointer;box-shadow:0 4px 14px rgba(22,163,74,.35)}}
    .route-save:hover{{filter:brightness(1.05)}}
    .route-table{{width:100%;border-collapse:collapse;font-size:14px;margin-top:6px}}
    .route-table th{{text-align:left;padding:10px 12px;background:#dcfce7;color:#14532d;font-weight:900;border-bottom:2px solid rgba(22,163,74,.25)}}
    .route-table td{{padding:10px 12px;border-bottom:1px solid rgba(148,163,184,.25);vertical-align:middle}}
    .route-monto{{color:#16a34a;font-weight:900}}
    .route-fecha{{font-variant-numeric:tabular-nums;color:#334155}}
    .route-act{{display:inline-flex;align-items:center;justify-content:center;width:36px;height:36px;border-radius:50%;border:none;cursor:pointer;text-decoration:none;font-size:16px;line-height:1}}
    .route-act-edit{{background:#fb923c;color:#fff}}
    .route-act-del{{background:#d946ef;color:#fff}}
  </style>

  <h2>📋 Registrar gasto de ruta</h2>
  <form method="post" action="{url_for("add_route_expense")}">
    <div class="route-form-row">
      <div class="route-field">
        <label>Ruta</label>
        <input name="route" type="text" placeholder="Ej: Ruta Norte" autocomplete="off">
      </div>
      <div class="route-field">
        <label>Tipo de gasto</label>
        <select name="tipo">{kind_options}</select>
      </div>
      <div class="route-field">
        <label>Monto</label>
        <input name="amount" type="number" step="0.01" min="0.01" required placeholder="0.00">
      </div>
      <div class="route-field" style="flex:2 1 200px">
        <label>Nota</label>
        <input name="note" type="text" placeholder="Opcional" autocomplete="off">
      </div>
      <div class="route-field" style="flex:0 0 auto">
        <label style="opacity:0">.</label>
        <button class="route-save" type="submit">➕ Guardar gasto</button>
      </div>
    </div>
  </form>

  <h3>📄 Gastos registrados</h3>
  <div class="table-scroll">
    <table class="route-table">
      <tr><th>Ruta</th><th>Tipo</th><th>Monto</th><th>Nota</th><th>Fecha</th><th></th></tr>
      {rows}
    </table>
  </div>
  {nav_subfooter()}
</div>
"""
    return page(body)


@app.route("/bank/expenses/delete/<int:expense_id>", methods=["POST"])
@login_required
def delete_route_expense(expense_id):
    exp = store.route_expenses.get(expense_id)
    if not exp:
        flash("Gasto no encontrado.", "danger")
        return redirect(url_for("bank_expenses"))
    cash_id = exp.get("cash_report_id")
    if cash_id is not None:
        store.cash_reports.pop(cash_id, None)
    store.route_expenses.pop(expense_id, None)
    flash("Gasto eliminado y banco actualizado.", "success")
    return redirect(url_for("bank_expenses"))


@app.route("/bank/expenses/edit/<int:expense_id>", methods=["GET", "POST"])
@login_required
def edit_expense(expense_id):
    ensure_org()
    oid = session.get("org_id")
    exp = store.route_expenses.get(expense_id)
    if not exp or exp.get("organization_id") != oid:
        flash("Gasto no encontrado.", "danger")
        return redirect(url_for("bank_expenses"))

    if request.method == "POST":
        route = (request.form.get("route") or "").strip()
        note = (request.form.get("note") or "").strip()
        kind = (request.form.get("tipo") or "otros").strip().lower()
        if kind not in ROUTE_EXPENSE_KINDS:
            kind = "otros"
        lbl, ico = route_expense_kind_info(kind)
        new_amt = request.form.get("amount", type=float)
        if new_amt is None or new_amt <= 0:
            flash("Monto inválido.", "danger")
            return redirect(url_for("edit_expense", expense_id=expense_id))

        old_amt = float(exp.get("amount") or 0)
        old_cash_id = exp.get("cash_report_id")

        projected = get_bank_available(oid) + old_amt - new_amt
        if projected < -1e-9:
            flash("Banco insuficiente para este cambio de monto.", "danger")
            return redirect(url_for("edit_expense", expense_id=expense_id))

        old_entry = store.cash_reports.get(old_cash_id) if old_cash_id is not None else None
        if old_cash_id is not None:
            store.cash_reports.pop(old_cash_id, None)

        cash_note = f"{ico} {lbl} — {route or '—'}" + (f" · {note}" if note else "")
        try:
            new_cash_id = apply_cash_movement(
                movement_type="gasto_ruta",
                amount=-new_amt,
                note=cash_note + " (editado)",
                user_id=current_user()["id"],
                org_id=oid,
            )
        except ValueError as e:
            if old_entry is not None and old_cash_id is not None:
                store.cash_reports[old_cash_id] = old_entry
            flash(str(e), "danger")
            return redirect(url_for("edit_expense", expense_id=expense_id))

        exp["route"] = route
        exp["kind"] = kind
        exp["amount"] = round(float(new_amt), 2)
        exp["note"] = note or "—"
        exp["cash_report_id"] = new_cash_id
        flash("Gasto actualizado.", "success")
        return redirect(url_for("bank_expenses"))

    cur_kind = exp.get("kind") or "otros"
    kind_options = "".join(
        f"<option value='{k}'{' selected' if k == cur_kind else ''}>{ico} {html.escape(lbl)}</option>"
        for k, (lbl, ico) in ROUTE_EXPENSE_KINDS.items()
    )
    body = (
        f'<div class="card route-exp-wrap" style="padding:16px;background:#ecfdf5;">'
        f"<h2 style='margin-top:0'>Editar gasto #{expense_id}</h2>"
        f"<form method='post'>"
        f"<label>Ruta</label><input name='route' value='{html.escape(exp.get('route') or '')}'>"
        f"<label>Tipo de gasto</label><select name='tipo'>{kind_options}</select>"
        f"<label>Monto</label><input name='amount' type='number' step='0.01' value='{exp.get('amount')}'>"
        f"<label>Nota</label><input name='note' value='{html.escape((exp.get('note') or '') if exp.get('note') != '—' else '')}'>"
        f"<p style='margin-top:12px'><button class='btn btn-primary' type='submit'>Guardar</button> "
        f"<a class='btn btn-secondary' href='{url_for('bank_expenses')}'>Volver</a></p>"
        f"</form></div>"
    )
    return page(body)


@app.route("/route/expenses/new", methods=["POST"])
@login_required
def add_route_expense():
    ensure_org()
    org_id = session.get("org_id")
    route = (request.form.get("route") or "").strip()
    note = (request.form.get("note") or "").strip()
    kind = (request.form.get("tipo") or "otros").strip().lower()
    if kind not in ROUTE_EXPENSE_KINDS:
        kind = "otros"
    lbl, ico = route_expense_kind_info(kind)
    exp_amount = request.form.get("amount", type=float)
    if exp_amount is None or exp_amount <= 0:
        flash("Monto inválido.", "danger")
        return redirect(url_for("bank_expenses"))

    cash_note = f"{ico} {lbl} — {route or '—'}" + (f" · {note}" if note else "")
    # Movimiento negativo del banco (gasto).
    try:
        cash_id = apply_cash_movement(
            movement_type="gasto_ruta",
            amount=-exp_amount,
            note=cash_note,
            user_id=current_user()["id"],
            org_id=org_id,
        )
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("bank_expenses"))

    eid = store.nid("route_expenses")
    store.route_expenses[eid] = {
        "id": eid,
        "route": route,
        "kind": kind,
        "expense_type": "gasto_ruta",
        "amount": round(float(exp_amount), 2),
        "note": note or "—",
        "user_id": current_user()["id"],
        "created_at": datetime.utcnow(),
        "organization_id": org_id,
        "cash_report_id": cash_id,
    }
    flash("Gasto registrado. Banco actualizado.", "success")
    return redirect(url_for("bank_expenses"))


@app.route("/bank/discount/delete/<int:discount_id>", methods=["POST"])
@login_required
def delete_discount(discount_id):
    ensure_org()
    oid = session.get("org_id")

    # El descuento se guarda como movement_type="descuento_inicial" en cash_reports.
    # También guardamos el cash_report_id en el préstamo para poder corregir totales.
    target = None
    for L in store.loans.values():
        if L.get("organization_id") == oid and L.get("discount_cash_report_id") == discount_id:
            target = L
            break
    if not target:
        flash("Descuento no encontrado.", "danger")
        return redirect(url_for("bank_acta"))

    amount_total = float(target.get("amount") or 0)
    old_upfront_percent = float(target.get("upfront_percent") or 0)
    old_discount_amount = round(amount_total * old_upfront_percent / 100.0, 2)
    if old_discount_amount < 0:
        old_discount_amount = 0.0

    # Validación contable: el banco cambia en 2*(nuevo_desc - viejo_desc).
    current_bank = get_bank_available(oid)
    final_bank = current_bank - 2 * old_discount_amount
    if final_bank < -1e-9:
        flash("No se puede eliminar el descuento: banco insuficiente.", "danger")
        return redirect(url_for("bank_acta"))

    client_id = target.get("client_id")
    user_id = current_user()["id"]

    # Revertir movimientos existentes.
    store.cash_reports.pop(discount_id, None)
    old_disbursement_id = target.get("disbursement_cash_report_id")
    if old_disbursement_id is not None:
        store.cash_reports.pop(old_disbursement_id, None)

    # Actualizar préstamo: descuento => 0 (monto entregado = amount_total).
    old_net_initial = float(target.get("remaining_capital") or 0)
    new_upfront_percent = 0.0
    new_net_initial = round(amount_total - (amount_total * new_upfront_percent / 100.0), 2)
    delta = new_net_initial - old_net_initial

    target["upfront_percent"] = new_upfront_percent
    target["remaining_capital"] = new_net_initial
    target["remaining"] = max(0.0, float(target.get("remaining") or 0) + delta)

    total_interest = round(float(target.get("total_interest") or 0), 2)
    term_count = int(target.get("term_count") or 1)
    target["total_to_pay"] = round(new_net_initial + total_interest, 2)
    target["installment_amount"] = round(target["total_to_pay"] / max(term_count, 1), 2)

    target["discount_cash_report_id"] = None
    target["disbursement_cash_report_id"] = None

    # Crear movimiento de entrega con descuento=0.
    if amount_total > 0:
        try:
            new_disbursement_id = apply_cash_movement(
                movement_type="prestamo_entregado",
                amount=-amount_total,
                note=f"Préstamo entregado cliente #{client_id} (sin descuento)",
                user_id=user_id,
                org_id=oid,
            )
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("bank_acta"))
        target["disbursement_cash_report_id"] = new_disbursement_id

    if float(target.get("remaining") or 0) <= 0:
        target["status"] = "cerrado"

    flash("Descuento inicial eliminado y banco actualizado.", "success")
    return redirect(url_for("bank_acta"))


@app.route("/bank/discount/edit/<int:discount_id>", methods=["POST"])
@login_required
def edit_discount(discount_id):
    ensure_org()
    oid = session.get("org_id")

    target = None
    for L in store.loans.values():
        if L.get("organization_id") == oid and L.get("discount_cash_report_id") == discount_id:
            target = L
            break
    if not target:
        flash("Préstamo/Descuento no encontrado.", "danger")
        return redirect(url_for("bank_acta"))

    new_upfront_percent = request.form.get("upfront_percent", type=float)
    if new_upfront_percent is None:
        flash("Por favor ingrese un %.", "danger")
        return redirect(url_for("bank_acta"))
    new_upfront_percent = max(0.0, min(float(new_upfront_percent), 100.0))

    amount_total = float(target.get("amount") or 0)
    old_upfront_percent = float(target.get("upfront_percent") or 0)
    old_discount_amount = round(amount_total * old_upfront_percent / 100.0, 2)
    new_discount_amount = round(amount_total * new_upfront_percent / 100.0, 2)
    old_net_initial = float(target.get("remaining_capital") or 0)
    new_net_initial = round(amount_total - new_discount_amount, 2)
    if new_net_initial < 0:
        new_net_initial = 0.0

    if abs(new_discount_amount - old_discount_amount) < 1e-9:
        flash("No hay cambios en el descuento.", "info")
        return redirect(url_for("bank_acta"))

    # Validación contable: banco cambia en 2*(nuevo_desc - viejo_desc).
    current_bank = get_bank_available(oid)
    final_bank = current_bank + 2 * (new_discount_amount - old_discount_amount)
    if final_bank < -1e-9:
        flash("No se puede editar el descuento: banco insuficiente.", "danger")
        return redirect(url_for("bank_acta"))

    client_id = target.get("client_id")
    user_id = current_user()["id"]

    # Revertir movimientos existentes.
    store.cash_reports.pop(discount_id, None)
    old_disbursement_id = target.get("disbursement_cash_report_id")
    if old_disbursement_id is not None:
        store.cash_reports.pop(old_disbursement_id, None)

    # Actualizar campos del préstamo.
    delta = new_net_initial - old_net_initial
    target["upfront_percent"] = new_upfront_percent
    target["remaining_capital"] = new_net_initial
    target["remaining"] = max(0.0, float(target.get("remaining") or 0) + delta)

    total_interest = round(float(target.get("total_interest") or 0), 2)
    term_count = int(target.get("term_count") or 1)
    target["total_to_pay"] = round(new_net_initial + total_interest, 2)
    target["installment_amount"] = round(target["total_to_pay"] / max(term_count, 1), 2)

    target["discount_cash_report_id"] = None
    target["disbursement_cash_report_id"] = None

    # Aplicar nuevos movimientos al banco.
    try:
        if new_discount_amount > 0:
            target["discount_cash_report_id"] = apply_cash_movement(
                movement_type="descuento_inicial",
                amount=new_discount_amount,
                note=f"Descuento inicial préstamo cliente #{client_id} (editado)",
                user_id=user_id,
                org_id=oid,
            )
        if new_net_initial > 0:
            target["disbursement_cash_report_id"] = apply_cash_movement(
                movement_type="prestamo_entregado",
                amount=-new_net_initial,
                note=f"Préstamo entregado cliente #{client_id} (neto editado)",
                user_id=user_id,
                org_id=oid,
            )
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("bank_acta"))

    if float(target.get("remaining") or 0) <= 0:
        target["status"] = "cerrado"
    else:
        target["status"] = target.get("status") if target.get("status") != "cerrado" else "ACTIVO"

    flash("Descuento inicial editado y banco actualizado.", "success")
    return redirect(url_for("bank_acta"))


@app.route("/bank/routes/history")
@login_required
def bank_routes_history():
    return stub_page("Historial por ruta")


@app.route("/bank/delivery", methods=["GET", "POST"])
@login_required
def bank_delivery():
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    puede_registrar_entrega = user.get("role") in ("admin", "supervisor") or is_cartera_admin(user)
    puede_registrar_devolucion = user.get("role") in ("admin", "supervisor", "cobrador") or is_cartera_admin(user)

    if request.method == "POST":
        form_type = (request.form.get("form_type") or "entrega").strip().lower()

        if form_type == "entrega":
            if not puede_registrar_entrega:
                flash("Solo administración puede registrar entregas.", "danger")
                return redirect(url_for("bank_delivery"))
            collector_id = request.form.get("collector_id", type=int)
            amt = request.form.get("amount", type=float)
            note = (request.form.get("note") or "").strip()
            cob = store.users.get(collector_id) if collector_id else None
            if not cob or cob.get("organization_id") != oid or cob.get("role") != "cobrador":
                flash("Seleccione un cobrador válido.", "danger")
                return redirect(url_for("bank_delivery"))
            if amt is None or amt <= 0:
                flash("Monto inválido.", "danger")
                return redirect(url_for("bank_delivery"))
            try:
                apply_cash_movement(
                    movement_type="entrega_cobrador",
                    amount=-float(amt),
                    note=note or f"Entrega efectivo para ruta — {cob.get('username')}",
                    user_id=user["id"],
                    org_id=oid,
                    collector_id=collector_id,
                )
            except ValueError as e:
                flash(str(e), "danger")
                return redirect(url_for("bank_delivery"))
            flash(f"Entrega registrada: {fmt_money(amt)} para {cob.get('username')}.", "success")
            return redirect(url_for("bank_delivery"))

        if form_type == "devolucion":
            if not puede_registrar_devolucion:
                flash("No tiene permiso para registrar devoluciones.", "danger")
                return redirect(url_for("bank_delivery"))

            amt = request.form.get("amount", type=float)
            note = (request.form.get("note") or "").strip()

            # Un cobrador solo puede registrar devolución a su nombre (si no es admin/outsider).
            if user.get("role") == "cobrador" and not is_cartera_admin(user):
                collector_id = user["id"]
            else:
                collector_id = request.form.get("collector_id", type=int)

            cob = store.users.get(collector_id) if collector_id else None
            if not cob or cob.get("organization_id") != oid or cob.get("role") != "cobrador":
                flash("Seleccione un cobrador válido.", "danger")
                return redirect(url_for("bank_delivery"))
            if amt is None or amt <= 0:
                flash("Monto inválido.", "danger")
                return redirect(url_for("bank_delivery"))

            try:
                apply_cash_movement(
                    movement_type="devolucion_capital",
                    amount=float(amt),
                    note=note or f"Devolución de capital — {cob.get('username')}",
                    user_id=user["id"],
                    org_id=oid,
                    collector_id=collector_id,
                )
            except ValueError as e:
                flash(str(e), "danger")
                return redirect(url_for("bank_delivery"))

            flash(f"Devolución registrada: {fmt_money(amt)} para {cob.get('username')}.", "success")
            return redirect(url_for("bank_delivery"))

        flash("Tipo de formulario inválido.", "danger")
        return redirect(url_for("bank_delivery"))

    entregas = sorted(
        (
            cr
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == oid and cr.get("movement_type") == "entrega_cobrador"
        ),
        key=lambda x: x.get("created_at") or datetime.min,
        reverse=True,
    )
    if user.get("role") == "cobrador" and not is_cartera_admin(user):
        entregas = [e for e in entregas if e.get("collector_id") == user["id"]]

    devoluciones = sorted(
        (
            cr
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == oid and cr.get("movement_type") == "devolucion_capital"
        ),
        key=lambda x: x.get("created_at") or datetime.min,
        reverse=True,
    )
    if user.get("role") == "cobrador" and not is_cartera_admin(user):
        devoluciones = [d for d in devoluciones if d.get("collector_id") == user["id"]]

    cobradores = sorted(
        [u for u in store.users.values() if u.get("organization_id") == oid and u.get("role") == "cobrador"],
        key=lambda u: (u.get("name") or u.get("username") or "").lower(),
    )
    opts = "".join(
        f'<option value="{u["id"]}">{html.escape(u.get("name") or u.get("username") or "")}</option>' for u in cobradores
    )
    tr = ""
    for cr in entregas[:150]:
        col_uid = cr.get("collector_id")
        col = store.users.get(col_uid, {})
        col_nm = html.escape(col.get("name") or col.get("username") or "—")
        reg_by = html.escape(store.users.get(cr.get("user_id"), {}).get("username") or "—")
        m = abs(float(cr.get("amount") or 0))
        fecha = cr.get("created_at")
        fecha_s = fecha.strftime("%d/%m/%Y %I:%M %p") if fecha and hasattr(fecha, "strftime") else str(fecha or "—")
        tr += (
            f"<tr><td>{html.escape(fecha_s)}</td><td>{col_nm}</td>"
            f"<td style='text-align:right;font-weight:800;color:#15803d'>{html.escape(fmt_money(m))}</td>"
            f"<td>{html.escape(cr.get('note') or '')}</td><td>{reg_by}</td></tr>"
        )

    tr_dev = ""
    for cr in devoluciones[:150]:
        col_uid = cr.get("collector_id")
        col = store.users.get(col_uid, {})
        col_nm = html.escape(col.get("name") or col.get("username") or "—")
        reg_by = html.escape(store.users.get(cr.get("user_id"), {}).get("username") or "—")
        m = float(cr.get("amount") or 0)
        fecha = cr.get("created_at")
        fecha_s = fecha.strftime("%d/%m/%Y %I:%M %p") if fecha and hasattr(fecha, "strftime") else str(fecha or "—")
        tr_dev += (
            f"<tr><td>{html.escape(fecha_s)}</td><td>{col_nm}</td>"
            f"<td style='text-align:right;font-weight:800;color:#15803d'>{html.escape(fmt_money(m))}</td>"
            f"<td>{html.escape(cr.get('note') or '')}</td><td>{reg_by}</td></tr>"
        )

    formulario = ""
    if puede_registrar_entrega:
        formulario = (
            f'<div class="card" style="padding:16px;margin-bottom:14px;background:#ecfdf5;border:1px solid rgba(22,163,74,.2)">'
            f'<h3 style="margin:0 0 8px 0;color:#14532d">Registrar entrega</h3>'
            f'<p style="margin:0 0 12px 0;font-size:14px;opacity:.92">Efectivo que sale del banco hacia el cobrador antes de la ruta. '
            f'Los cobros del día siguen sumando al banco; el <a href="{url_for("cierre_semanal")}">cierre semanal</a> sirve para cuadrar.</p>'
            f'<p style="margin:0 0 10px 0"><b>Banco disponible:</b> {html.escape(fmt_money(get_bank_available(oid)))}</p>'
            f'<form method="post">'
            f'<label>Cobrador</label><select name="collector_id" required><option value="">— Elegir —</option>{opts}</select>'
            f'<label>Monto entregado</label><input name="amount" type="number" step="0.01" min="0.01" required>'
            f'<label>Nota (opcional)</label><input name="note" type="text" placeholder="Ej. Ruta norte 22/03">'
            f'<button class="btn btn-primary" type="submit" style="margin-top:10px">Guardar entrega</button>'
            f"</form></div>"
        )
    else:
        formulario = (
            f'<div class="card" style="padding:14px;margin-bottom:14px;opacity:.95">'
            f'<p style="margin:0">Aquí ves las entregas registradas a tu nombre. Solo administración registra nuevas entregas.</p></div>'
        )

    formulario_dev = ""
    if puede_registrar_devolucion:
        if user.get("role") == "cobrador" and not is_cartera_admin(user):
            # Cobrador: la devolución va siempre a su propio ID.
            my_name = html.escape(user.get("name") or user.get("username") or "—")
            formulario_dev = (
                f'<div class="card" style="padding:16px;margin-bottom:14px;background:#f0f9ff;border:1px solid rgba(56,189,248,.25);">'
                f'<h3 style="margin:0 0 8px 0;color:#075985">Devolución de capital</h3>'
                f'<p style="margin:0 0 10px 0;font-size:14px;opacity:.92">Cuando el {my_name} regresa con efectivo no utilizado, se registra aquí para cuadrar el banco.</p>'
                f'<p style="margin:0 0 10px 0"><b>Banco disponible:</b> {html.escape(fmt_money(get_bank_available(oid)))}</p>'
                f'<form method="post">'
                f'<input type="hidden" name="form_type" value="devolucion">'
                f'<input type="hidden" name="collector_id" value="{user["id"]}">'
                f'<label>Monto devuelto</label><input name="amount" type="number" step="0.01" min="0.01" required>'
                f'<label>Nota (opcional)</label><input name="note" type="text" placeholder="Ej. Devolución ruta / cierre">'
                f'<button class="btn btn-primary" type="submit" style="margin-top:10px">Guardar devolución</button>'
                f'</form></div>'
            )
        else:
            formulario_dev = (
                f'<div class="card" style="padding:16px;margin-bottom:14px;background:#f0fdf4;border:1px solid rgba(16,185,129,.2);">'
                f'<h3 style="margin:0 0 8px 0;color:#14532d">Devolución de capital</h3>'
                f'<p style="margin:0 0 10px 0;font-size:14px;opacity:.92">El dinero devuelto por el prestamista/cobrador se suma al banco para cuadrar al terminar la ruta.</p>'
                f'<p style="margin:0 0 10px 0"><b>Banco disponible:</b> {html.escape(fmt_money(get_bank_available(oid)))}</p>'
                f'<form method="post">'
                f'<input type="hidden" name="form_type" value="devolucion">'
                f'<label>Cobrador</label><select name="collector_id" required><option value="">— Elegir —</option>{opts}</select>'
                f'<label>Monto devuelto</label><input name="amount" type="number" step="0.01" min="0.01" required>'
                f'<label>Nota (opcional)</label><input name="note" type="text" placeholder="Ej. Ruta norte 22/03">'
                f'<button class="btn btn-primary" type="submit" style="margin-top:10px">Guardar devolución</button>'
                f'</form></div>'
            )
    else:
        formulario_dev = (
            f'<div class="card" style="padding:14px;margin-bottom:14px;opacity:.95">'
            f'<p style="margin:0">Aquí ves la devolución de capital registrada para el banco.</p></div>'
        )

    body = (
        f"{formulario}"
        f"{formulario_dev}"
        f'<div class="card" style="padding:16px">'
        f'<h2 style="margin:0 0 6px 0;color:#14532d">💰 Entrega de efectivo</h2>'
        f'<p style="margin:0 0 14px 0;font-size:14px">Historial de entregas al cobrador para salir a cobrar.</p>'
        f'<div class="table-scroll"><table><tr>'
        f"<th>Fecha</th><th>Cobrador</th><th>Monto</th><th>Nota</th><th>Registró</th></tr>"
        f"{tr or '<tr><td colspan=5 style=\"text-align:center;opacity:.85\">Sin entregas registradas</td></tr>'}"
        "</table></div>"
        f"</div>"
        f'<div class="card" style="padding:16px;margin-top:14px">'
        f'<h2 style="margin:0 0 6px 0;color:#14532d">🧾 Devolución de capital</h2>'
        f'<p style="margin:0 0 14px 0;font-size:14px">Efectivo devuelto por el cobrador/“prestamista” para cuadrar el banco.</p>'
        f'<div class="table-scroll"><table><tr>'
        f"<th>Fecha</th><th>Cobrador</th><th>Monto</th><th>Nota</th><th>Registró</th></tr>"
        f"{tr_dev or '<tr><td colspan=5 style=\"text-align:center;opacity:.85\">Sin devoluciones registradas</td></tr>'}"
        "</table></div>"
        f"{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/bank/delivery/edit/<int:delivery_id>", methods=["GET", "POST"])
@login_required
def bank_delivery_edit(delivery_id):
    return stub_page("Editar entrega")


@app.route("/bank/delivery/delete/<int:delivery_id>", methods=["POST"])
@login_required
def bank_delivery_delete(delivery_id):
    flash("Eliminado (memoria).", "info")
    return redirect(url_for("bank_delivery"))


@app.route("/bank/acta", methods=["GET", "POST"])
@login_required
def bank_acta():
    ensure_org()
    oid = session.get("org_id")
    # ============================================================
    # Vista tipo "Acta global" (alineada al otro sistema)
    # - Caja global: depósitos + banco inicial (sin incluir préstamos/pagos)
    # - Descuento total: movimientos "descuento_inicial"
    # - Gastos realizados: movimientos "gasto_ruta"
    # - Disponible: saldo real del banco (starting_bank + todos los cash_reports)
    # ============================================================
    caja_global = float(getattr(store, "starting_banks", {}).get(oid, 0.0) or 0.0) + sum(
        float(cr.get("amount") or 0)
        for cr in store.cash_reports.values()
        if cr.get("organization_id") == oid and cr.get("movement_type") == "deposito_banco"
    )
    descuento_total = round(
        sum(
            float(cr.get("amount") or 0)
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == oid and cr.get("movement_type") == "descuento_inicial"
        ),
        2,
    )
    gastos_realizados = round(
        sum(
            abs(float(cr.get("amount") or 0))
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == oid and cr.get("movement_type") == "gasto_ruta"
        ),
        2,
    )
    disponible = get_bank_available(oid)

    def fmt_dt(dt):
        if not dt:
            return ""
        try:
            return dt.strftime("%d/%m/%Y %I:%M %p")
        except Exception:
            return str(dt)

    # Listar descuentos registrados como movimientos del libro.
    discount_moves = sorted(
        (
            cr
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == oid and cr.get("movement_type") == "descuento_inicial"
        ),
        key=lambda x: x.get("created_at") or datetime.min,
        reverse=True,
    )

    rows = ""
    for cr in discount_moves[:200]:
        discount_id = cr.get("id")
        loan = None
        for L in store.loans.values():
            if L.get("organization_id") == oid and L.get("discount_cash_report_id") == discount_id:
                loan = L
                break
        client = store.clients.get((loan or {}).get("client_id"), {}) if loan else {}
        user_id = cr.get("user_id")
        cobrador = store.users.get(user_id, {}).get("username") or "—"
        ruta = client.get("route") or "—"
        monto = float(cr.get("amount") or 0)

        del_form = (
            f"<form method='post' action='{url_for('delete_discount', discount_id=discount_id)}' "
            f"onsubmit=\"return confirm('¿Eliminar este descuento y ajustar el banco?');\" style='margin:0'>"
            f"<button class='btn btn-secondary' type='submit' title='Eliminar'>🗑</button></form>"
        )

        rows += (
            "<tr>"
            f"<td>{fmt_dt(cr.get('created_at'))}</td>"
            f"<td>{cobrador}</td>"
            f"<td>{ruta}</td>"
            f"<td style='text-align:right'>{fmt_money(-monto)}</td>"
            f"<td>{del_form}</td>"
            "</tr>"
        )

    if not rows:
        rows = "<tr><td colspan='5' style='opacity:.85; text-align:center'>Sin descuentos registrados</td></tr>"

    body = f"""
<div class="card" style="padding:16px;">
  <h2 style="margin:0 0 12px 0;">🧾 Acta global</h2>
  <style>
    .acta-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin:0 0 14px 0}}
    .acta-pill{{border-radius:16px;padding:12px 12px;color:#fff;box-shadow:0 6px 16px rgba(0,0,0,.10);min-height:82px}}
    .acta-k{{display:block;font-size:12px;font-weight:900;opacity:.95;text-transform:uppercase;letter-spacing:.03em;margin-bottom:6px}}
    .acta-v{{display:block;font-size:16px;font-weight:1000;line-height:1.15}}
    table td,table th{{vertical-align:middle}}
  </style>
  <div class="acta-grid">
    <div class="acta-pill" style="background:linear-gradient(135deg,#0d9488,#14b8a6)"><span class="acta-k">Caja global</span><span class="acta-v">{fmt_money(caja_global)}</span></div>
    <div class="acta-pill" style="background:linear-gradient(135deg,#15803d,#22c55e)"><span class="acta-k">Descuento total</span><span class="acta-v">{fmt_money(descuento_total)}</span></div>
    <div class="acta-pill" style="background:linear-gradient(135deg,#c2410c,#fb923c)"><span class="acta-k">Gastos realizados</span><span class="acta-v">{fmt_money(gastos_realizados)}</span></div>
    <div class="acta-pill" style="background:linear-gradient(135deg,#334155,#0f172a)"><span class="acta-k">Disponible</span><span class="acta-v">{fmt_money(disponible)}</span></div>
  </div>

  <div style="margin-top:8px;">
    <h3 style="margin:0 0 10px 0;">Descuentos registrados</h3>
    <div class="table-scroll">
      <table>
        <tr>
          <th>Fecha</th>
          <th>Cobrador</th>
          <th>Ruta</th>
          <th>Monto</th>
          <th></th>
        </tr>
        {rows}
      </table>
    </div>
  </div>
</div>
{nav_subfooter()}
"""
    return page(body)


@app.route("/bank/routes", methods=["GET", "POST"])
@login_required
def bank_routes_list():
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    by_route = {}
    for L in loans_for_user(oid, user):
        if str(L.get("status", "")).upper() != "ACTIVO":
            continue
        c = store.clients.get(L.get("client_id"), {})
        ruta = (c.get("route") or "").strip() or "Sin ruta"
        if ruta not in by_route:
            by_route[ruta] = {"remaining": 0.0, "n": 0, "cids": set()}
        by_route[ruta]["remaining"] += float(L.get("remaining") or 0)
        by_route[ruta]["n"] += 1
        cb = L.get("created_by")
        if cb:
            by_route[ruta]["cids"].add(cb)

    tr = ""
    for ruta in sorted(by_route.keys(), key=lambda x: x.lower()):
        info = by_route[ruta]
        names = []
        for uid in info["cids"]:
            u = store.users.get(uid, {})
            names.append(u.get("name") or u.get("username") or str(uid))
        pres = html.escape(", ".join(sorted(names)) if names else "—")
        tr += (
            f"<tr><td>{html.escape(ruta)}</td><td style='text-align:center'>{info['n']}</td>"
            f"<td style='text-align:right;font-weight:800;color:#15803d'>{html.escape(fmt_money(info['remaining']))}</td>"
            f"<td>{pres}</td></tr>"
        )

    total_rest = sum(float(x["remaining"]) for x in by_route.values())
    total_n = sum(int(x["n"]) for x in by_route.values())

    body = (
        f'<div class="card" style="padding:16px;background:#ecfdf5;border:1px solid rgba(22,163,74,.18)">'
        f'<h2 style="margin:0 0 8px 0;color:#14532d">🏦 Capital por ruta</h2>'
        f'<p style="margin:0 0 14px 0;font-size:14px">Saldo vivo (<b>capital pendiente</b>) agrupado por la ruta del cliente.</p>'
        f'<p style="margin:0 0 12px 0"><b>Total préstamos activos:</b> {total_n} · '
        f"<b>Capital activo total:</b> {html.escape(fmt_money(total_rest))}</p>"
        f'<div class="table-scroll"><table>'
        f"<tr><th>Ruta</th><th># Préstamos</th><th>Capital activo</th><th>Prestamistas</th></tr>"
        f"{tr or '<tr><td colspan=4 style=\"text-align:center;opacity:.85\">Sin préstamos activos con ruta</td></tr>'}"
        f"</table></div>{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/bank/late")
@login_required
def bank_late():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    today = date.today()

    rows_pack = []
    for L in loans_for_user(org_id, user):
        if str(L.get("status", "")).upper() != "ACTIVO":
            continue
        if float(L.get("remaining") or 0) <= 0:
            continue
        npd = L.get("next_payment_date")
        if not npd or not hasattr(npd, "strftime"):
            continue
        if npd >= today:
            continue
        days_late = (today - npd).days
        cuota_n = count_loan_cuota_payments(L["id"]) + 1
        rows_pack.append((days_late, L, cuota_n, npd))

    rows_pack.sort(key=lambda x: -x[0])

    rows = []
    for days_late, L, cuota_n, npd in rows_pack[:400]:
        cid = L.get("client_id")
        c = store.clients.get(cid, {})
        nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or f"Cliente #{cid}"
        phone = (c.get("phone") or "").strip() or "—"
        due_s = npd.strftime("%d/%m/%Y")
        pay_href = url_for("new_payment", loan_id=L["id"])
        rows.append(
            "<tr>"
            f"<td>{nm}</td>"
            f"<td>Cuota atrasada #{cuota_n}</td>"
            f"<td>{phone}</td>"
            f"<td class='late-days'><b>{days_late} días</b></td>"
            f"<td class='late-saldo'>{fmt_money(L.get('remaining'))}</td>"
            f"<td class='late-fecha'>{due_s}</td>"
            f"<td><a class='btn-late-pay' href='{pay_href}'>💸 Pagar</a></td>"
            "</tr>"
        )

    tbody = "".join(rows) if rows else (
        "<tr><td colspan='7' style='text-align:center;opacity:.85;padding:16px'>Sin cuotas atrasadas</td></tr>"
    )

    body = f"""
<div class="card late-wrap" style="padding:16px;background:#fffbeb;border:1px solid rgba(217,119,6,.22);">
  <style>
    .late-wrap h2{{margin:0 0 14px 0;font-size:1.35rem;font-weight:900;color:#78350f}}
    .late-table{{width:100%;border-collapse:collapse;background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 4px 18px rgba(0,0,0,.06)}}
    .late-table th{{text-align:left;padding:12px 14px;background:#fef3c7;color:#78350f;font-weight:900;font-size:13px}}
    .late-table td{{padding:11px 14px;border-bottom:1px solid rgba(148,163,184,.2);font-size:14px}}
    .late-table tr:last-child td{{border-bottom:none}}
    .late-days{{color:#dc2626;font-variant-numeric:tabular-nums}}
    .late-saldo{{color:#16a34a;font-weight:900}}
    .late-fecha{{font-variant-numeric:tabular-nums;color:#334155}}
    .btn-late-pay{{background:#dc2626;color:#fff;border-radius:999px;padding:8px 14px;font-weight:800;font-size:13px;text-decoration:none;display:inline-block}}
    .btn-late-pay:hover{{filter:brightness(1.06)}}
  </style>
  <h2>⚠️ Cuotas Atrasadas</h2>
  <div class="table-scroll">
    <table class="late-table">
      <tr><th>Cliente</th><th>Cuota</th><th>Teléfono</th><th>Días atraso</th><th>Saldo</th><th>Fecha vencida</th><th>Acción</th></tr>
      {tbody}
    </table>
  </div>
  {nav_subfooter()}
</div>
"""
    return page(body)


@app.route("/bank/ranking")
@login_required
def bank_ranking():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    today = date.today()
    scored = []
    for L in loans_for_user(org_id, user):
        if str(L.get("status", "")).upper() != "ACTIVO":
            continue
        npd = L.get("next_payment_date")
        days_late = (today - npd).days if npd and npd < today else 0
        rem = float(L.get("remaining") or 0)
        scored.append((days_late, rem, L))
    scored.sort(key=lambda x: (-x[0], -x[1]))
    rows = ""
    for days_late, rem, L in scored[:50]:
        cid = L.get("client_id")
        c = store.clients.get(cid, {})
        nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or f"#{cid}"
        rows += (
            f"<tr><td>{nm}</td><td>#{L['id']}</td><td>{days_late} d</td><td>{fmt_money(rem)}</td>"
            f"<td><a class='btn btn-secondary' href='{url_for('loan_detail', loan_id=L['id'])}'>Ver</a></td></tr>"
        )
    body = (
        f'<div class="card"><h2>Ranking morosos</h2><p>Activos ordenados por días de atraso y saldo.</p>'
        f'<div class="table-scroll"><table><tr><th>Cliente</th><th>Prést.</th><th>Atraso</th><th>Saldo</th><th></th></tr>'
        f"{rows or '<tr><td colspan=5>Sin datos</td></tr>'}</table></div>{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/bank/credit-history")
@login_required
def credit_history():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    lids = loan_ids_visible(org_id, user)
    by_client = {}
    for L in store.loans.values():
        if L["id"] not in lids:
            continue
        cid = L["client_id"]
        by_client.setdefault(cid, []).append(L)
    rows = ""
    for cid, lst in sorted(by_client.items(), key=lambda x: store.clients.get(x[0], {}).get("first_name", "")):
        c = store.clients.get(cid, {})
        nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or f"#{cid}"
        doc = c.get("document_id") or "—"
        n = len(lst)
        cerr = sum(1 for x in lst if str(x.get("status", "")).lower() == "cerrado")
        cap = sum(float(x.get("amount") or 0) for x in lst)
        rows += f"<tr><td>{nm}</td><td>{doc}</td><td>{n}</td><td>{cerr}</td><td>{fmt_money(cap)}</td>"
        rows += f"<td><a class='btn btn-secondary' href='{url_for('client_detail', client_id=cid)}'>Ver</a></td></tr>"
    body = (
        f'<div class="card"><h2>Historial de crédito</h2><p>Por cliente (préstamos en tu alcance).</p>'
        f'<div class="table-scroll"><table><tr><th>Cliente</th><th>Cédula</th><th>Prést.</th><th>Cerrados</th><th>Capital total</th><th></th></tr>'
        f"{rows or '<tr><td colspan=6>Sin datos</td></tr>'}</table></div>{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/bank/client-scores")
@login_required
def client_scores():
    ensure_org()
    org_id = session.get("org_id")
    rows = []
    for c in store.clients.values():
        if c.get("organization_id") != org_id:
            continue
        sd = calc_client_score(c["id"], org_id)
        mc = calc_max_credito(sd["prestamos_pagados"], sd["score"])
        rows.append(f"<tr><td>{c['first_name']}</td><td>{sd['score']}</td><td>{fmt_money(mc)}</td></tr>")
    t = "".join(rows)
    return page(
        f'<div class="card"><h2>⭐ Score de clientes</h2><div class="table-scroll">'
        f'<table><tr><th>Cliente</th><th>Score</th><th>Crédito sug.</th></tr>{t}</table></div>{nav_subfooter()}</div>'
    )


@app.route("/bank/check-client", methods=["GET", "POST"])
@login_required
def check_client():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    result_html = ""
    if request.method == "POST":
        q = (request.form.get("cedula") or "").strip().upper().replace(" ", "")
        found = None
        for c in clients_for_user(org_id, user):
            doc = (c.get("document_id") or "").strip().upper().replace(" ", "")
            if doc and q and q in doc:
                found = c
                break
        if found:
            loans_c = [L for L in store.loans.values() if L.get("client_id") == found["id"]]
            lr = "".join(
                f"<tr><td>#{L['id']}</td><td>{L.get('status')}</td><td>{fmt_money(L.get('remaining'))}</td></tr>"
                for L in loans_c
            )
            result_html = (
                f"<h3>Resultado</h3><p><b>{found.get('first_name')} {found.get('last_name') or ''}</b> · {found.get('document_id')}</p>"
                f'<div class="table-scroll"><table><tr><th>Préstamo</th><th>Estado</th><th>Saldo</th></tr>{lr}</table></div>'
                f'<p><a class="btn btn-primary" href="{url_for("client_detail", client_id=found["id"])}">Ficha cliente</a></p>'
            )
        else:
            result_html = "<p>No se encontró ningún cliente con esa cédula en tu cartera.</p>"
    body = (
        f'<div class="card"><h2>🔍 Consultar por cédula</h2>'
        f'<form method="post"><label>Cédula / documento</label><input name="cedula" required placeholder="001-0000000-0">'
        f'<button class="btn btn-primary" type="submit">Buscar</button></form>{result_html}{nav_subfooter()}</div>'
    )
    return page(body)


@app.route("/bank/risk-clients")
@login_required
def risk_clients():
    return stub_page("Clientes riesgo")


@app.route("/bank/cobro-sabado")
@login_required
def cobro_sabado():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    prox_sab, rows = loans_cobro_sabado_semanal(org_id, user)
    tr = ""
    for npd, L, nm in rows:
        nm_e = html.escape(nm or f"Cliente #{L.get('client_id')}")
        pay_href = url_for("new_payment", loan_id=L["id"])
        tr += (
            f"<tr><td>{nm_e}</td><td>#{L['id']}</td><td>{npd}</td><td>{html.escape(fmt_money(L.get('installment_amount')))}</td>"
            f"<td><a class='btn btn-primary' href='{pay_href}' data-pay-href='{pay_href}' onclick='return openPayConfirm(this)'>Pagar</a></td></tr>"
        )
    body = (
        f'<div class="card"><h2>💰 Cobro sábado</h2>'
        f"<p>Préstamos <b>semanales</b> con cuota a cobrar a más tardar el sábado "
        f"<b>{prox_sab.strftime('%d/%m/%Y')}</b> (hoy si ya es sábado).</p>"
        f'<div class="table-scroll"><table><tr><th>Cliente</th><th>Prést.</th><th>Próx. pago</th><th>Cuota est.</th><th></th></tr>'
        f"{tr or '<tr><td colspan=5>Ninguno programado para este ciclo</td></tr>'}</table></div>"
        """
<div id="payConfirmModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:99999">
  <div style="background:#fff;max-width:420px;margin:18vh auto;padding:16px;border-radius:16px;box-shadow:0 14px 40px rgba(0,0,0,.25)">
    <h3 style="margin:0 0 8px 0;color:#14532d;font-weight:900">Confirmar pago</h3>
    <p style="margin:0 0 12px 0;opacity:.92">¿Aceptar y abrir el formulario de pago para este préstamo?</p>
    <div style="display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap">
      <button type="button" class="btn btn-secondary" onclick="closePayConfirm()">Cancelar</button>
      <button type="button" class="btn btn-primary" onclick="acceptPayConfirm()">Aceptar</button>
    </div>
  </div>
</div>
<script>
  let payConfirmHref = null;
  function openPayConfirm(el){
    payConfirmHref = el.getAttribute('data-pay-href') || el.getAttribute('href');
    const m = document.getElementById('payConfirmModal');
    if (m) m.style.display = 'block';
    return false;
  }
  function closePayConfirm(){
    const m = document.getElementById('payConfirmModal');
    if (m) m.style.display = 'none';
    payConfirmHref = null;
  }
  function acceptPayConfirm(){
    if (payConfirmHref) window.location = payConfirmHref;
  }
</script>
"""
        f"{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/bank/resumen")
@login_required
def bank_resumen():
    ensure_org()
    oid = session.get("org_id")
    k = compute_financial_kpis(oid, current_user())
    banco_disp = get_bank_available(oid)
    total_gastos_ruta = round(
        sum(
            float(e.get("amount") or 0)
            for e in store.route_expenses.values()
            if e.get("organization_id") == oid
        ),
        2,
    )
    body = (
        f"""
<div class="card" style="padding:16px;">
  <h2 style="margin:0 0 10px 0;">📊 Resumen financiero</h2>

  <style>
    .res-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin: 10px 0 18px 0;
    }}
    .res-card {{
      border-radius: 16px;
      padding: 12px 10px;
      color: #fff;
      box-shadow: 0 6px 16px rgba(0,0,0,.14);
      min-height: 86px;
    }}
    .res-k {{
      display:block;
      font-size: 10px;
      font-weight: 800;
      opacity: .95;
      text-transform: uppercase;
      letter-spacing: .04em;
      margin-bottom: 6px;
    }}
    .res-v {{
      display:block;
      font-size: 16px;
      font-weight: 900;
      line-height: 1.2;
    }}
    .res-daily-title {{
      font-size: 14px;
      font-weight: 900;
      margin: 6px 0 10px 0;
      color: #14532d;
    }}
    body.theme-dark .res-daily-title {{ color: #86efac; }}
  </style>

  <div class="res-grid">
    <div class="res-card" style="background: linear-gradient(135deg,#0d9488,#14b8a6);">
      <span class="res-k">Capital prestado</span>
      <span class="res-v">{fmt_money(k['capital_prestado'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#15803d,#22c55e);">
      <span class="res-k">Capital cobrado</span>
      <span class="res-v">{fmt_money(k['capital_cobrado'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#1d4ed8,#3b82f6);">
      <span class="res-k">Capital pendiente</span>
      <span class="res-v">{fmt_money(k['capital_pendiente'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#7c3aed,#a855f7);">
      <span class="res-k">Interés total</span>
      <span class="res-v">{fmt_money(k['interes_total'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#166534,#4ade80);">
      <span class="res-k">Interés cobrado</span>
      <span class="res-v">{fmt_money(k['interes_cobrado'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#c2410c,#fb923c);">
      <span class="res-k">Interés pendiente</span>
      <span class="res-v">{fmt_money(k['interes_pendiente'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#b91c1c,#ef4444);">
      <span class="res-k">Total por cobrar</span>
      <span class="res-v">{fmt_money(k['total_por_cobrar'])}</span>
    </div>

    <div class="res-card" style="background: linear-gradient(135deg,#0f172a,#334155);">
      <span class="res-k">Banco disponible</span>
      <span class="res-v">{fmt_money(banco_disp)}</span>
    </div>

    <div class="res-card" style="background: linear-gradient(135deg,#ef4444,#f59e0b);">
      <span class="res-k">Gastos de ruta</span>
      <span class="res-v">{fmt_money(total_gastos_ruta)}</span>
    </div>
  </div>

  <div class="res-daily-title">Métricas diarias</div>
  <div class="res-grid" style="margin-bottom:0;">
    <div class="res-card" style="background: linear-gradient(135deg,#4f8df7,#3b82f6);">
      <span class="res-k">Cobrado hoy</span>
      <span class="res-v">{fmt_money(k['cobrado_hoy'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#7c3aed,#a78bfa);">
      <span class="res-k">Interés hoy</span>
      <span class="res-v">{fmt_money(k['interes_hoy'])}</span>
    </div>
    <div class="res-card" style="background: linear-gradient(135deg,#fb923c,#f59e0b);">
      <span class="res-k">Atrasados</span>
      <span class="res-v">{k['atrasados']}</span>
    </div>
  </div>

  <div style="margin-top:14px;">{nav_subfooter()}</div>
</div>
"""
    )
    return page(body)


@app.route("/bank/cierre-semanal", methods=["GET"])
@login_required
def cierre_semanal():
    ensure_org()
    org_id = session.get("org_id")
    user = current_user()

    # Rango semanal tipo lunes->domingo.
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)

    # Pagos de préstamos del tenant dentro del rango.
    def loan_org_ok(loan_id):
        L = store.loans.get(loan_id) if loan_id is not None else None
        return bool(L and L.get("organization_id") == org_id)

    payments_week = [
        p
        for p in store.payments.values()
        if p.get("date") is not None
        and week_start <= p.get("date") <= week_end
        and loan_org_ok(p.get("loan_id"))
        and (p.get("status") or "OK") != "ANULADO"
    ]
    payments_week.sort(key=lambda p: (p.get("created_by") or 0, p.get("id") or 0))

    total_cobrado = round(sum(float(p.get("amount") or 0) for p in payments_week), 2)

    payments_rows = ""
    for p in payments_week:
        uid = p.get("created_by")
        u = store.users.get(uid, {})
        name = (u.get("name") or u.get("username") or "—") if u else "—"
        payments_rows += (
            f"<tr>"
            f"<td style='padding-right:12px'>{html.escape(str(name))}</td>"
            f"<td style='text-align:right;font-weight:900'>{fmt_money(p.get('amount') or 0)}</td>"
            f"</tr>"
        )
    if not payments_rows:
        payments_rows = "<tr><td colspan=2 style='opacity:.78'>Sin pagos</td></tr>"

    # Préstamos entregados (cash_movements prestamo_entregado) dentro del rango.
    prestamos_entregados = [
        cr
        for cr in store.cash_reports.values()
        if cr.get("organization_id") == org_id
        and cr.get("movement_type") == "prestamo_entregado"
        and cr.get("date") is not None
        and week_start <= cr.get("date") <= week_end
    ]
    prestamos_entregados.sort(key=lambda cr: (cr.get("user_id") or 0, cr.get("id") or 0))

    prestado_total = round(sum(float(cr.get("amount") or 0) for cr in prestamos_entregados), 2)
    prestamos_rows = ""
    for cr in prestamos_entregados:
        uid = cr.get("user_id")
        u = store.users.get(uid, {})
        name = (u.get("name") or u.get("username") or "—") if u else "—"
        prestamos_rows += (
            "<tr>"
            f"<td style='padding-right:12px'>{html.escape(str(name))}</td>"
            f"<td style='text-align:right;font-weight:900'>{fmt_money(cr.get('amount') or 0)}</td>"
            "</tr>"
        )
    if not prestamos_rows:
        prestamos_rows = "<tr><td colspan=2 style='opacity:.78'>Sin préstamos</td></tr>"

    # Descuentos informativos.
    descuentos = [
        cr
        for cr in store.cash_reports.values()
        if cr.get("organization_id") == org_id
        and cr.get("movement_type") == "descuento_inicial"
        and cr.get("date") is not None
        and week_start <= cr.get("date") <= week_end
    ]
    descuentos_total = round(sum(float(cr.get("amount") or 0) for cr in descuentos), 2)
    descuentos_rows = ""
    for cr in descuentos:
        uid = cr.get("user_id")
        u = store.users.get(uid, {})
        name = (u.get("name") or u.get("username") or "—") if u else "—"
        descuentos_rows += (
            "<tr>"
            f"<td style='padding-right:12px'>{html.escape(str(name))}</td>"
            f"<td style='text-align:right;font-weight:900'>-{fmt_money(cr.get('amount') or 0)}</td>"
            "</tr>"
        )
    if not descuentos_rows:
        descuentos_rows = "<tr><td colspan=2 style='opacity:.78'>Sin descuentos</td></tr>"

    # Gastos ruta.
    gastos = [
        e
        for e in store.route_expenses.values()
        if e.get("organization_id") == org_id
        and e.get("created_at") is not None
        and week_start <= e.get("created_at").date() <= week_end
    ]
    gastos_rows = ""
    gastos_total = round(sum(float(e.get("amount") or 0) for e in gastos), 2)
    for e in sorted(gastos, key=lambda x: x.get("created_at") or datetime.min):
        uid = e.get("user_id")
        u = store.users.get(uid, {})
        name = (u.get("name") or u.get("username") or "—") if u else "—"
        # Nota: mostramos como en el sistema: nombre + monto.
        gastos_rows += (
            "<tr>"
            f"<td style='padding-right:12px'>{html.escape(str(name))}</td>"
            f"<td style='text-align:right;font-weight:900'>{fmt_money(e.get('amount') or 0)}</td>"
            "</tr>"
        )
    if not gastos_rows:
        gastos_rows = "<tr><td colspan=2 style='opacity:.78'>Sin gastos</td></tr>"

    balance_negocio = round(total_cobrado - prestado_total - gastos_total, 2)

    body = (
        f'<div class="card">'
        f'<h2>🧾 Cierre semanal</h2>'
        f"<div style='opacity:.85;font-weight:800;margin-bottom:10px'>Semana: {week_start.isoformat()} → {week_end.isoformat()}</div>"
        f"<hr style='opacity:.18;margin:12px 0'/>"
        f"<div style='font-weight:950;margin:10px 0 6px 0'>💰 Pagos</div>"
        f"<div class='table-scroll'><table><tbody>{payments_rows}</tbody></table></div>"
        f"<div style='font-weight:950;margin:14px 0 6px 0'>💸 Préstamos entregados</div>"
        f"<div class='table-scroll'><table><tbody>{prestamos_rows}</tbody></table></div>"
        f"<div style='font-weight:950;margin:14px 0 6px 0'>📉 Descuentos (informativo)</div>"
        f"<div class='table-scroll'><table><tbody>{descuentos_rows}</tbody></table></div>"
        f"<div style='font-weight:950;margin:14px 0 6px 0'>🚗 Gastos ruta</div>"
        f"<div class='table-scroll'><table><tbody>{gastos_rows}</tbody></table></div>"
        f"<div style='font-weight:950;margin:14px 0 6px 0'>📊 Totales</div>"
        f"<div style='padding:10px 12px;background:rgba(236,253,245,.55);border:1px solid rgba(148,163,184,.25);border-radius:14px'>"
        f"<div style='display:flex;justify-content:space-between'><span style='opacity:.88'>Total cobrado</span><b>{fmt_money(total_cobrado)}</b></div>"
        f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Prestado</span><b>{fmt_money(prestado_total)}</b></div>"
        f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Gastos</span><b>{fmt_money(gastos_total)}</b></div>"
        f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Balance negocio</span><b>{fmt_money(balance_negocio)}</b></div>"
        f"</div>"
        f'<form method="post" action="{url_for("cerrar_semana")}" style="margin-top:14px">'
        f'<button class="btn btn-primary" type="submit" style="width:100%;font-size:16px;padding:12px 10px">✅ CERRAR CUADRE</button>'
        f"</form>"
        f"{nav_subfooter()}"
        f"</div>"
    )
    return page(body)


@app.route("/bank/cerrar-semana", methods=["POST"])
@login_required
def cerrar_semana():
    ensure_org()
    org_id = session.get("org_id")
    user = current_user()
    k = compute_financial_kpis(org_id, user)
    rec = {
        "id": store.nid("cierre"),
        "organization_id": org_id,
        "closed_at": datetime.utcnow(),
        "user_id": user["id"],
        "notas": (request.form.get("notas") or "").strip(),
        "cobrado_hoy_snapshot": k["cobrado_hoy"],
        "total_por_cobrar_snapshot": k["total_por_cobrar"],
        "n_activos": k["n_activos"],
    }
    store.closure_history.append(rec)
    try:
        log_action(user["id"], "cierre_semana", str(rec["id"]))
    except Exception:
        pass
    flash("Cierre de semana registrado.", "success")
    return redirect(url_for("historial_cierres"))


@app.route("/bank/historial-cierres")
@login_required
def historial_cierres():
    ensure_org()
    oid = session.get("org_id")
    rows = ""
    for rec in reversed([r for r in store.closure_history if r.get("organization_id") == oid][-50:]):
        cid = rec.get("id")
        del_form = (
            f'<form method="post" action="{url_for("borrar_cierre", cierre_id=cid)}" style="display:inline" '
            f'onsubmit="return confirm(\'¿Eliminar este registro?\');"><button type="submit" class="btn btn-secondary">Borrar</button></form>'
        )
        rows += (
            f"<tr><td>{rec.get('closed_at')}</td><td>{fmt_money(rec.get('cobrado_hoy_snapshot'))}</td>"
            f"<td>{fmt_money(rec.get('total_por_cobrar_snapshot'))}</td>"
            f"<td>{rec.get('n_activos')}</td><td>{rec.get('notas') or '—'}</td><td>{del_form}</td></tr>"
        )
    body = (
        f'<div class="card"><h2>✔️ Cuadres cerrados</h2><p>Últimos cierres semanales guardados en memoria.</p>'
        f'<div class="table-scroll"><table><tr><th>Fecha</th><th>Cobrado hoy (snap)</th><th>Total por cobrar</th><th>Activos</th><th>Notas</th><th></th></tr>'
        f"{rows or '<tr><td colspan=6>Sin cierres aún</td></tr>'}</table></div>{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/bank/pagar/<int:loan_id>", methods=["POST"])
@login_required
def pagar_prestamo(loan_id):
    flash("Use «Registrar pago» en el detalle del préstamo.", "info")
    return redirect(url_for("loan_detail", loan_id=loan_id))


@app.route("/bank/borrar-cierre/<int:cierre_id>", methods=["POST"])
@login_required
def borrar_cierre(cierre_id):
    ensure_org()
    oid = session.get("org_id")
    store.closure_history = [
        c
        for c in store.closure_history
        if not (c.get("id") == cierre_id and c.get("organization_id") == oid)
    ]
    flash("Cierre eliminado (memoria).", "info")
    return redirect(url_for("historial_cierres"))


@app.route("/bank/agregar-dinero", methods=["GET", "POST"])
@login_required
def agregar_dinero_banco():
    ensure_org()
    org_id = session.get("org_id")
    if request.method == "POST":
        amt = request.form.get("amount", type=float)
        note = (request.form.get("note") or "Depósito banco").strip()
        if amt is None or amt <= 0:
            flash("Monto inválido.", "danger")
            return redirect(url_for("agregar_dinero_banco"))
        rid = None
        try:
            rid = apply_cash_movement(
                movement_type="deposito_banco",
                amount=amt,
                note=note,
                user_id=current_user()["id"],
                org_id=org_id,
            )
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("agregar_dinero_banco"))

        store.deposit_history.append(
            {"id": rid, "organization_id": org_id, "amount": amt, "note": note, "at": datetime.utcnow()}
        )
        flash(f"Ingresado {fmt_money(amt)} al flujo de caja (memoria).", "success")
        return redirect(url_for("historial_depositos"))
    body = (
        f'<div class="card"><h2>🏦 Agregar dinero al banco</h2>'
        f'<form method="post"><label>Monto (RD$)</label><input name="amount" type="number" step="0.01" required>'
        f'<label>Nota</label><input name="note" placeholder="Ej. Ingreso efectivo">'
        f'<button class="btn btn-primary" type="submit">Guardar</button></form>'
        f'<p><a class="btn btn-secondary" href="{url_for("historial_depositos")}">Ver historial de depósitos</a></p>{nav_subfooter()}</div>'
    )
    return page(body)


@app.route("/bank/historial-depositos")
@login_required
def historial_depositos():
    ensure_org()
    oid = session.get("org_id")
    rows = "".join(
        f"<tr><td>{d.get('at')}</td><td>{fmt_money(d.get('amount'))}</td><td>{d.get('note') or '—'}</td></tr>"
        for d in reversed([d for d in store.deposit_history if d.get("organization_id") == oid][-100:])
    )
    body = (
        f'<div class="card"><h2>Historial de depósitos</h2>'
        f'<div class="table-scroll"><table><tr><th>Fecha</th><th>Monto</th><th>Nota</th></tr>'
        f"{rows or '<tr><td colspan=3>Sin depósitos</td></tr>'}</table></div>{nav_subfooter()}</div>"
    )
    return page(body)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"[DEMO — memoria] http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
