# -*- coding: utf-8 -*-
# CREDIMAPA — PostgreSQL multi-tenant SaaS. Con fallback a memoria si no hay DATABASE_URL.
from __future__ import annotations

import os
import json
import math
import base64
import html
import secrets
from urllib.parse import urlencode
from datetime import datetime, date, timedelta
from functools import wraps

from rd_time import (
    FORMAT_RD_DATETIME,
    combine_date_at_rd_midnight,
    format_dt_rd,
    format_payment_receipt_when,
    get_current_time_rd,
    today_rd,
    utc_now_for_db,
)

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

APP_BRAND = "CREDIMAPA"
# Service worker mínimo si no hay archivo en static/ (p. ej. deploy sin carpeta static).
SW_JS_MINIMAL = """/* Minimal service worker */
self.addEventListener('install', (e) => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));
"""
ADMIN_PIN = os.getenv("ADMIN_PIN", "5555")
CURRENCY = "RD$"
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "3128565688")
# Recibo térmico (impresión); sobreescribibles en producción.
RECEIPT_BUSINESS_NAME = os.getenv("RECEIPT_BUSINESS_NAME", "CREDIMAPA")
RECEIPT_SUBTITLE = os.getenv("RECEIPT_SUBTITLE", "Calle central 31, los jardines, 10116")
RECEIPT_COMPANY_TEL = os.getenv("RECEIPT_COMPANY_TEL", "(829) 924-8121")
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
    oid = L.get("organization_id")
    if str(p.get("type") or "").strip().lower() != "cuota":
        return f"—/{term_count}"
    if USE_DATABASE:
        cuotas = [
            x
            for x in payments_for_loan(loan_id, oid)
            if (x.get("status") or "OK") != "ANULADO"
            and str(x.get("type") or "").strip().lower() == "cuota"
        ]
    else:
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
    """Fecha/hora para el recibo en hora de Santo Domingo (UTC en BD → RD al mostrar)."""
    return format_payment_receipt_when(p)


def freq_interval_days(freq):
    """
    Intervalo aproximado (en días) por frecuencia.
    - semanal: 7
    - diario: 1
    - quincenal: 15
    - mensual: 30
    - custom: 1 (pagos diarios según rango de fechas)
    """
    s = str(freq or "").strip().lower()
    if "custom" in s:
        return 1
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
    if "custom" in s:
        return "Personalizado"
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
    today = d or today_rd()
    ahead = (5 - today.weekday()) % 7
    return today if ahead == 0 else today + timedelta(days=ahead)


def loans_cobro_sabado_semanal(org_id, user, ref_date=None):
    """
    Préstamos semanales activos cuya próxima cuota vence a más tardar el sábado de cobro.
    Misma criterio que la lista de cobro de sábados.
    """
    today = ref_date or today_rd()
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
            c = client_dict_by_id(cid, org_id) or {}
            nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or (f"Cliente #{cid}" if cid else "Sin nombre")
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
        return format_dt_rd(dt, "%Y-%m-%d %I:%M %p")
    except Exception:
        return str(dt)


def fmt_advance_datetime(dt):
    """Fecha/hora para lista de adelantos (America/Santo_Domingo)."""
    if not dt:
        return "—"
    try:
        return format_dt_rd(dt, FORMAT_RD_DATETIME)
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
body{margin:0;font-family:system-ui,sans-serif;min-height:100dvh;-webkit-text-size-adjust:100%}
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
form{width:100%}
form label{display:block;font-weight:900;font-size:12px;opacity:.92}
form input,form select,form textarea{width:100%;max-width:100%;padding:10px 12px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:#fff;font-size:14px}
form textarea{min-height:96px;resize:vertical}
a,button{-webkit-tap-highlight-color:transparent}
@media(max-width:420px){
  .container{padding:8px}
  .card{padding:12px;margin-bottom:10px}
  header.topbar{padding:12px}
  .topbar-title{font-size:16px}
  th,td{padding:7px 8px}
}
</style>
"""

TPL_LAYOUT = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>{{ app_brand }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="default">
  <meta name="mobile-web-app-capable" content="yes">
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
{% if user %}
<div style="position:absolute;right:14px;top:50%;transform:translateY(-50%);display:flex;align-items:center;gap:14px">
  {% if nav_balance %}
  <div style="font-weight:900;font-size:13px;color:#16a34a">{{ nav_balance }}</div>
  {% endif %}
  <div style="font-size:10px;opacity:.92;text-align:right;line-height:1.25">
    <div>Hora RD</div>
    <div style="font-weight:800">{{ time_rd_label }}</div>
  </div>
</div>
{% endif %}
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
        created = utc_now_for_db()
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

        # ADMÍN TENANT inicial (de fábrica). Se puede eliminar si existe otro admin activo.
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
            "is_default": True,
        }
        self.starting_banks[tenant_admin_id] = self.starting_bank_default
        self._seq["users"] = tenant_admin_id

    def reset_all(self):
        self.__init__()


store = Store()
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(16))

# --- Base de datos (Render / producción) ---
# Misma URL que usaría Flask-SQLAlchemy; el motor real está en credimapa_pg (SQLAlchemy 2).
# En Render debe existir DATABASE_URL (PostgreSQL). SQLite en el filesystem del contenedor
# NO persiste entre redeploys/reinicios: no lo uses para datos reales en Render.
_raw_db_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
SQLALCHEMY_DATABASE_URI = (
    _raw_db_url.replace("postgres://", "postgresql://", 1) if _raw_db_url else ""
)

USE_DATABASE = bool(os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL"))
if USE_DATABASE:
    import credimapa_pg

    # Compatibilidad con convención Flask-SQLAlchemy (no usamos extensión; el motor es credimapa_pg).
    app.config["SQLALCHEMY_DATABASE_URI"] = credimapa_pg.DATABASE_URL
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    credimapa_pg.init_app(app)


def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    if USE_DATABASE:
        from credimapa_pg import get_user
        return get_user(uid)
    return store.users.get(uid)


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


def log_action(user_id, action, detail="", module=""):
    """
    Auditoría. En DB: tabla auditoria. En memoria: audit_log.
    """
    try:
        if USE_DATABASE and user_id:
            from credimapa_pg import get_user
            u = get_user(user_id) or {}
        else:
            u = store.users.get(user_id, {}) if user_id else {}
        user_name = u.get("name") or u.get("username") or "—"
        raw_role = (u.get("role") or "").strip().lower()
        role_group = "admin" if raw_role in ("admin", "supervisor", "super_admin") else "prestamista"

        ip = None
        device = None
        try:
            ip = (request.headers.get("X-Forwarded-For") or request.remote_addr) if request else None
            ua = (request.headers.get("User-Agent") or "") if request else ""
            device = (ua[:220] if ua else None)
        except Exception:
            pass

        oid = session.get("org_id")
        data = {
            "user_id": user_id,
            "admin_id": oid,
            "user_name": user_name,
            "role": role_group,
            "raw_role": raw_role,
            "action": action,
            "module": module or "",
            "detail": detail or "",
            "ip": ip,
            "device": device,
        }
        if USE_DATABASE:
            from credimapa_pg import save_audit
            save_audit(data)
        else:
            data["id"] = store.nid("audit")
            data["created_at"] = utc_now_for_db()
            store.audit_log.append(data)
    except Exception:
        return


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
    if USE_DATABASE:
        from credimapa_pg import get_loans, get_loan_arrears

        loans = list(get_loans([org_id]).values())
        prestamos_pagados = sum(
            1
            for L in loans
            if L.get("client_id") == client_id
            and str(L.get("status", "")).upper() in ("CERRADO", "PAGADO", "FINALIZADO")
        )
        arr = get_loan_arrears(org_id) or {}
        by_loan = {L["id"]: L for L in loans}
        atrasos = sum(
            1
            for A in arr.values()
            if not A.get("paid")
            and by_loan.get(A.get("loan_id"), {}).get("client_id") == client_id
        )
    else:
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
    u = user if user is not None else current_user()
    nav_balance = ""
    if u and session.get("org_id") is not None:
        try:
            bal = get_bank_available(session.get("org_id"))
            nav_balance = f"RD$ {bal:,.2f}" if bal is not None else "RD$ 0.00"
        except Exception:
            nav_balance = "RD$ —"
    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=u,
        app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme(),
        time_rd_label=format_dt_rd(get_current_time_rd()),
        time_rd_zone="America/Santo_Domingo",
        nav_balance=nav_balance,
    )


def stub_page(title, extra=""):
    return page(
        f'<div class="card"><h2>{title}</h2><p>Modo <b>memoria</b> (sin base de datos). {extra}</p>'
        f'<p><a class="btn btn-secondary" href="{url_for("bank_home")}">Volver al banco</a> '
        f'<a class="btn btn-primary" href="{url_for("dashboard")}">Dashboard</a></p></div>'
    )


def loans_for_user(org_id, user):
    if USE_DATABASE:
        from credimapa_pg import get_loans
        rows = list(get_loans([org_id]).values())
    else:
        rows = [L for L in store.loans.values() if L.get("organization_id") == org_id]
    if is_cajero_role(user) and not is_cartera_admin(user):
        rows = [L for L in rows if L.get("created_by") == user["id"]]
    return rows


def clients_for_user(org_id, user):
    if USE_DATABASE:
        from credimapa_pg import get_clients
        # Equivalente a Cliente.query.filter_by(admin_id=org_id).all() → dicts para las plantillas HTML.
        rows = list(get_clients([org_id]).values())
    else:
        rows = [c for c in store.clients.values() if c.get("organization_id") == org_id]
    if is_cajero_role(user) and not is_cartera_admin(user):
        rows = [c for c in rows if c.get("created_by") == user["id"]]
    return rows


def client_dict_by_id(client_id, org_id=None):
    """Cliente como dict (store en memoria o fila PostgreSQL)."""
    oid = org_id if org_id is not None else session.get("org_id")
    if USE_DATABASE:
        from credimapa_pg import get_client as _db_get_client

        c = _db_get_client(client_id)
        if not c or c.get("organization_id") != oid:
            return None
        return c
    c = store.clients.get(client_id)
    if not c:
        return None
    if oid is not None and c.get("organization_id") != oid:
        return None
    return c


def loan_dict_by_id(loan_id, org_id=None):
    """Préstamo como dict (memoria o PostgreSQL)."""
    oid = org_id if org_id is not None else session.get("org_id")
    if USE_DATABASE:
        from credimapa_pg import get_loan as _db_get_loan

        L = _db_get_loan(loan_id)
        if not L or L.get("organization_id") != oid:
            return None
        return L
    L = store.loans.get(loan_id)
    if not L:
        return None
    if oid is not None and L.get("organization_id") != oid:
        return None
    return L


def payments_for_loan(loan_id, org_id=None):
    oid = org_id if org_id is not None else session.get("org_id")
    if USE_DATABASE:
        from credimapa_pg import get_payments

        return [p for p in get_payments([oid]).values() if p.get("loan_id") == loan_id]
    return [p for p in store.payments.values() if p.get("loan_id") == loan_id]


def user_is_cobrador_limited(user):
    """
    Un cobrador (collector) no debe ver datos de otros usuarios.
    Excepción: cartera admin (CARTERA_ADMIN_USER_ID) y super_admin.
    """
    role = (user.get("role") or "").strip().lower()
    return bool(user) and role in ("cobrador", "cajero") and not is_cartera_admin(user) and role != "super_admin"


def can_admin_actions(user):
    """
    Admin-level: puede hacer cualquier operación crítica.
    Mapeo actual (por compatibilidad con roles ya existentes):
    - `admin` y `supervisor`: admin-level
    - `super_admin`: también admin-level (panel global)
    - `is_cartera_admin(user)`: excepción histórica del sistema
    """
    if not user:
        return False
    role = (user.get("role") or "").strip().lower()
    return role in ("admin", "supervisor", "super_admin") or is_cartera_admin(user)


def is_cajero_role(user):
    role = (user.get("role") or "").strip().lower()
    return role in ("cobrador", "cajero")


def scope_owns_loan(user, loan):
    if not user_is_cobrador_limited(user):
        return True
    return bool(loan) and loan.get("created_by") == user.get("id")


def scope_owns_client(user, client):
    if not user_is_cobrador_limited(user):
        return True
    return bool(client) and client.get("created_by") == user.get("id")


def scope_owns_payment(user, payment):
    if not user_is_cobrador_limited(user):
        return True
    return bool(payment) and payment.get("created_by") == user.get("id")


def scope_owns_cash_report(user, cash_report):
    if not user_is_cobrador_limited(user):
        return True
    return bool(cash_report) and cash_report.get("user_id") == user.get("id")


def scope_owns_route_expense(user, route_expense):
    if not user_is_cobrador_limited(user):
        return True
    return bool(route_expense) and route_expense.get("user_id") == user.get("id")


def bank_org_id(org_id=None):
    return org_id if org_id is not None else session.get("org_id") or ORG_ID


def get_bank_available(org_id=None):
    oid = bank_org_id(org_id)
    if USE_DATABASE:
        from credimapa_pg import get_bank_available as _db_bank
        return _db_bank(oid)
    total = float(getattr(store, "starting_banks", {}).get(oid, 0.0) or 0.0)
    for cr in store.cash_reports.values():
        if cr.get("organization_id") == oid:
            total += float(cr.get("amount") or 0)
    if abs(total) < 1e-9:
        total = 0.0
    return round(total, 2)


def apply_cash_movement(movement_type, amount, note, user_id=None, org_id=None, collector_id=None):
    """
    Aplica un movimiento al banco (ledger). Valida que nunca quede negativo.
    """
    if USE_DATABASE:
        from credimapa_pg import add_banco_movement as _db_add
        rid = _db_add(movement_type, amount, note, user_id, org_id, collector_id)
        try:
            oid = org_id if org_id is not None else session.get("org_id")
            if oid:
                nuevo_balance = get_bank_available(oid)
                log_action(
                    user_id or (current_user() or {}).get("id"),
                    "movimiento banco",
                    module="banco",
                    detail=f"{movement_type} | RD$ {float(amount or 0):,.2f} | balance: RD$ {float(nuevo_balance or 0):,.2f}",
                )
        except Exception:
            pass
        return rid
    oid = bank_org_id(org_id)
    amt = float(amount or 0)
    if abs(amt) < 1e-9:
        return None
    projected = get_bank_available(oid) + amt
    if projected < -1e-9:
        raise ValueError(f"Banco insuficiente. Disponible: {get_bank_available(oid)} | Requerido adicional: {abs(amt)}")
    rid = store.nid("cash")
    rec = {
        "id": rid, "user_id": user_id, "date": today_rd(),
        "amount": round(amt, 2), "note": note or movement_type,
        "created_at": utc_now_for_db(), "organization_id": oid,
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
    if USE_DATABASE:
        from credimapa_pg import get_payments
        pmts = get_payments([org_id])
        return [p for p in (pmts or {}).values() if p.get("loan_id") in lids]
    return [p for p in store.payments.values() if p.get("loan_id") in lids]


def _revert_loan_financials(loan_id, oid, user_id):
    """
    Revierte todos los movimientos financieros de un préstamo.
    """
    if USE_DATABASE:
        from credimapa_pg import get_session, get_prestamo
        L = get_prestamo(get_session(), loan_id)
    else:
        L = store.loans.get(loan_id)
    if not L:
        return
    if USE_DATABASE:
        from credimapa_pg import get_payments
        payments_src = list(get_payments([oid]).values())
    else:
        payments_src = list(store.payments.values())
    payments = [p for p in payments_src if p and p.get("loan_id") == loan_id]
    for p in payments:
        amt = float(p.get("amount") or 0)
        if abs(amt) >= 1e-9:
            try:
                apply_cash_movement(
                    movement_type="reverso_pago_prestamo",
                    amount=-amt,
                    note=f"Reverso por eliminación préstamo #{loan_id} (pago #{p.get('id')})",
                    user_id=user_id,
                    org_id=oid,
                )
            except ValueError:
                pass
        if USE_DATABASE:
            from credimapa_pg import delete_pago, get_session as _gs
            delete_pago(_gs(), p.get("id"))
        else:
            store.payments.pop(p.get("id"), None)
    disc_id = L.get("discount_cash_report_id") or L.get("discount_banco_id")
    disb_id = L.get("disbursement_cash_report_id") or L.get("disbursement_banco_id")
    if disc_id:
        if USE_DATABASE:
            from credimapa_pg import pop_banco_movement
            pop_banco_movement(disc_id)
        else:
            store.cash_reports.pop(disc_id, None)
    if disb_id:
        if USE_DATABASE:
            from credimapa_pg import pop_banco_movement
            pop_banco_movement(disb_id)
        else:
            store.cash_reports.pop(disb_id, None)


def count_loan_cuota_payments(loan_id):
    """Pagos tipo cuota válidos (para numerar la cuota vencida siguiente)."""
    if USE_DATABASE:
        from credimapa_pg import get_loan, get_payments

        L = get_loan(loan_id)
        if not L:
            return 0
        oid = L.get("organization_id")
        pm = get_payments([oid]) if oid is not None else {}
        return sum(
            1
            for p in pm.values()
            if p.get("loan_id") == loan_id
            and (p.get("status") or "OK") != "ANULADO"
            and str(p.get("type") or "").strip().lower() == "cuota"
        )
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

    today = today_rd()
    cobrado_hoy = sum(float(p.get("amount") or 0) for p in ok if p.get("date") == today)
    interes_hoy = sum(float(p.get("interest") or 0) for p in ok if p.get("date") == today)

    atrasados = 0
    clientes_atrasados = set()
    for x in activos:
        npd = x.get("next_payment_date")
        rem = float(x.get("remaining") or 0)
        if npd and npd < today and rem > 0:
            atrasados += 1
            clientes_atrasados.add(x.get("client_id"))

    if USE_DATABASE:
        from credimapa_pg import get_users
        users_iter = get_users().values()
    else:
        users_iter = store.users.values()
    n_cobradores = sum(1 for u in users_iter if u and u.get("role") == "cobrador" and u.get("organization_id") == org_id)

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
        "clientes_atrasados": len(clientes_atrasados),
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
        if USE_DATABASE:
            from credimapa_pg import get_user_by_username
            user = get_user_by_username(username)
        else:
            user = next((u for u in store.users.values() if u.get("username") == username), None)
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
            tenant_id = user.get("organization_id") or user.get("admin_id") or ORG_ID
            if USE_DATABASE:
                from credimapa_pg import get_users
                tenant_admin = get_users().get(tenant_id, user)
            else:
                tenant_admin = store.users.get(tenant_id, user) if tenant_id else user
            now = utc_now_for_db()

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

            now_d = today_rd()
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
            log_action(user["id"], "login", detail="login", module="auth")
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

        created = utc_now_for_db()
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
            "is_default": False,
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
    u = current_user()
    try:
        if u:
            log_action(u.get("id"), "logout", detail="logout", module="auth")
    except Exception:
        pass
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
        "name": APP_BRAND, "short_name": "CREDIMAPA",
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
    if USE_DATABASE:
        from credimapa_pg import get_loans, get_loan_arrears
        _loans = get_loans([org_id]) if org_id else {}
        _arrears = get_loan_arrears(org_id) or {}
        morosos = sum(
            1 for A in _arrears.values()
            if not A.get("paid") and _loans.get(A.get("loan_id"), {}).get("organization_id") == org_id
        )
        hoy = sum(
            1 for L in _loans.values()
            if L.get("organization_id") == org_id and str(L.get("status", "")).upper() == "ACTIVO" and L.get("next_payment_date") == today_rd()
        )
        if user_is_cobrador_limited(user):
            morosos = sum(
                1 for A in _arrears.values()
                if not A.get("paid") and _loans.get(A.get("loan_id"), {}).get("created_by") == user["id"]
            )
            hoy = sum(
                1 for L in _loans.values()
                if L.get("created_by") == user["id"] and str(L.get("status", "")).upper() == "ACTIVO" and L.get("next_payment_date") == today_rd()
            )
    else:
        morosos = sum(
            1 for A in store.loan_arrears.values()
            if not A.get("paid") and store.loans.get(A.get("loan_id"), {}).get("organization_id") == org_id
        )
        hoy = sum(
            1 for L in store.loans.values()
            if L.get("organization_id") == org_id and str(L.get("status", "")).upper() == "ACTIVO" and L.get("next_payment_date") == today_rd()
        )
        if user_is_cobrador_limited(user):
            morosos = sum(
                1 for A in store.loan_arrears.values()
                if not A.get("paid") and store.loans.get(A.get("loan_id"), {}).get("created_by") == user["id"]
            )
            hoy = sum(
                1 for L in store.loans.values()
                if L.get("created_by") == user["id"] and str(L.get("status", "")).upper() == "ACTIVO" and L.get("next_payment_date") == today_rd()
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
        f'<span class="dk">⚠️ Préstamos en atraso</span><span class="dv">{k["atrasados"]}</span></a>'
        f'<a class="daily-card r" href="{url_for("bank_late")}" style="text-decoration:none;color:inherit;display:block;cursor:pointer">'
        f'<span class="dk">👤 Clientes en atraso</span><span class="dv">{k.get("clientes_atrasados", k["atrasados"])}</span></a>'
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
.daily-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr)); gap: 8px; margin-bottom: 8px; }}
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
@role_required("admin", "supervisor", "super_admin")
def users():
    ensure_org()
    oid = session.get("org_id")
    current_uid = session.get("user_id")
    current_u = current_user()
    can_manage = current_u and current_u.get("role") == "admin"
    n_admins = _count_active_admins_in_org(oid)
    rows = []
    for u in _org_users_list(oid):
        is_self = u.get("id") == current_uid
        is_last_admin = u.get("role") == "admin" and n_admins <= 1
        can_delete = can_manage and not is_self and not is_last_admin
        badge = " (fábrica)" if _is_factory_admin_user(u) else ""
        del_btn = ""
        if can_delete:
            del_btn = (
                f'<form method="post" action="{url_for("delete_user", user_id=u["id"])}" style="display:inline" '
                f'onsubmit="return confirm(\'¿Eliminar usuario {html.escape(u["username"])}?\');">'
                f'<button class="btn btn-secondary" type="submit">Borrar</button></form>'
            )
        else:
            if is_last_admin:
                del_btn = '<span style="opacity:.7;font-size:12px">Requiere otro admin</span>'
        rows.append(
            f"<tr><td>{html.escape(u['username'])}{badge}</td><td>{html.escape(u['role'])}</td>"
            f"<td>{html.escape(u.get('phone') or '')}</td><td>{del_btn}</td></tr>"
        )
    rows_html = "".join(rows)
    body = (
        f'<div class="card"><h2>Usuarios</h2>'
        f'<p style="opacity:.9;font-size:13px">Puede crear admins y eliminar el de fábrica una vez exista otro admin activo.</p>'
        f'<div class="table-scroll"><table><tr><th>Usuario</th><th>Rol</th><th>Tel</th><th></th></tr>{rows_html or "<tr><td colspan=4 style=\"text-align:center;opacity:.85\">Sin usuarios</td></tr>"}</table></div>'
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
        if u.get("organization_id") == org_id and is_cajero_role(u)
    )
    body = (
        f'<div class="card"><h2>👥 Empleados / cobradores</h2>'
        f'<div class="table-scroll"><table><tr><th>Usuario</th><th>Teléfono</th><th>Rol</th></tr>{rows or "<tr><td colspan=3>Sin cobradores</td></tr>"}</table></div>'
        f"{nav_subfooter()}</div>"
    )
    return page(body)


@app.route("/users/new", methods=["GET", "POST"])
@login_required
@role_required("admin", "super_admin")
def new_user():
    ensure_org()
    user = current_user()
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
        # Crear otro admin: autorizado con PIN admin (ya validado arriba), no hace falta ser super_admin.
        if role == "admin" and user.get("role") not in ("admin", "super_admin"):
            flash("Solo un administrador de la empresa puede crear otro admin.", "danger")
            return redirect(url_for("new_user"))
        org_id = session.get("org_id")
        if USE_DATABASE:
            from credimapa_pg import username_exists, create_tenant_usuario

            if username_exists(username):
                flash("Usuario ya existe.", "danger")
                return redirect(url_for("new_user"))
        elif any(u["username"] == username for u in store.users.values()):
            flash("Usuario ya existe.", "danger")
            return redirect(url_for("new_user"))
        status = ACCOUNT_ACTIVE if role in ("admin", "supervisor") else (
            ACCOUNT_PENDING if role in ("cobrador", "cajero") else ACCOUNT_ACTIVE
        )

        # Fechas de acceso (solo cobradores/cajeros). Admins/supervisores no las requieren.
        fecha_inicio = None
        fecha_fin = None
        if role in ("cobrador", "cajero"):
            raw_inicio = (request.form.get("fecha_inicio") or "").strip()
            raw_fin = (request.form.get("fecha_fin") or "").strip()
            today = today_rd()
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

        if USE_DATABASE:
            uid = create_tenant_usuario(
                admin_id=org_id,
                username=username,
                password_hash=generate_password_hash(password),
                role=role,
                phone=phone,
                account_status=status,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
            )
        else:
            uid = store.nid("users")
            store.users[uid] = {
                "id": uid,
                "username": username,
                "password_hash": generate_password_hash(password),
                "role": role,
                "phone": phone,
                "organization_id": org_id,
                "created_at": utc_now_for_db(),
                "name": None,
                "account_status": status,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "is_default": False,
            }
        flash("Usuario creado.", "success")
        return redirect(url_for("users"))
    role_opts = ""
    if user.get("role") == "super_admin":
        role_opts = (
            f'<option value="admin">Admin</option>'
            f'<option value="supervisor">Supervisor</option>'
            f'<option value="cobrador">Cobrador (queda pendiente)</option>'
            f'<option value="cajero">Cajero (queda pendiente)</option>'
        )
    else:
        role_opts = (
            f'<option value="admin">Admin (requiere PIN admin)</option>'
            f'<option value="supervisor">Supervisor</option>'
            f'<option value="cobrador">Cobrador (queda pendiente)</option>'
            f'<option value="cajero">Cajero (queda pendiente)</option>'
        )
    hint = ""
    if user.get("role") != "super_admin":
        hint = (
            '<p style="opacity:.88;font-size:13px;margin-bottom:12px">'
            "Para crear un <b>Admin</b> adicional, elija ese rol e ingrese el <b>PIN admin</b> correcto. "
            "Cuando haya más de un admin activo, podrá borrar el de fábrica (<code>admin</code>) desde la lista."
            "</p>"
        )
    body = (
        f'<div class="card"><h2>Nuevo usuario</h2>'
        f'{hint}'
        f'<form method="post">'
        f'<label>Usuario</label><input name="username" required>'
        f'<label>Contraseña</label><input type="password" name="password" required>'
        f'<label>Teléfono</label><input name="phone">'
        f'<label>Rol</label><select name="role">{role_opts}</select>'
        f'<label>Fecha inicio (cobradores/cajeros)</label><input name="fecha_inicio" type="date" value="{today_rd().strftime("%Y-%m-%d")}">'
        f'<label>Fecha fin (cobradores/cajeros)</label><input name="fecha_fin" type="date" value="{(today_rd()+timedelta(days=30)).strftime("%Y-%m-%d")}">'
        f'<label>PIN admin</label><input name="pin" required>'
        f'<button class="btn btn-primary" type="submit">Crear</button></form></div>'
    )
    return page(body)


def _org_users_list(oid):
    """Usuarios del tenant (PostgreSQL o memoria), sin super_admin."""
    if USE_DATABASE:
        from credimapa_pg import list_tenant_usuarios

        return list_tenant_usuarios(oid)
    return [
        u
        for u in store.users.values()
        if u.get("organization_id") == oid and u.get("role") != "super_admin"
    ]


def _count_active_admins_in_org(oid):
    """Cuenta admins activos en la org (rol admin). Siempre debe haber al menos 1."""
    return sum(
        1
        for u in _org_users_list(oid)
        if u.get("role") == "admin"
        and u.get("account_status", ACCOUNT_ACTIVE) == ACCOUNT_ACTIVE
    )


def _is_factory_admin_user(u):
    """Admin sembrado por defecto (badge «fábrica»)."""
    if u.get("is_default"):
        return True
    return u.get("id") == 2 and (u.get("username") or "").strip().lower() == "admin"


@app.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_user(user_id):
    ensure_org()
    oid = session.get("org_id")
    if USE_DATABASE:
        from credimapa_pg import get_user as _db_gu

        u = _db_gu(user_id)
        if not u or u.get("organization_id") != oid:
            flash("Sin acceso para eliminar ese usuario.", "danger")
            return redirect(url_for("users"))
    else:
        u = store.users.get(user_id)
        if not u or u.get("organization_id") != oid:
            flash("Sin acceso para eliminar ese usuario.", "danger")
            return redirect(url_for("users"))
    if u.get("role") == "super_admin":
        flash("No se puede eliminar al super administrador.", "danger")
        return redirect(url_for("users"))
    if user_id == session.get("user_id"):
        flash("No puede borrarse a sí mismo.", "danger")
        return redirect(url_for("users"))
    # Solo admin puede eliminar admin; validar que no quede el sistema sin admin.
    if u.get("role") == "admin":
        n_admins = _count_active_admins_in_org(oid)
        if n_admins <= 1:
            flash("No se puede eliminar el único admin. Cree otro admin primero.", "danger")
            return redirect(url_for("users"))
    try:
        current_u = current_user()
        log_action(
            current_u.get("id"),
            "eliminar usuario",
            module="usuarios",
            detail=f"{u.get('username')} (rol: {u.get('role')})",
        )
    except Exception:
        pass
    if USE_DATABASE:
        from credimapa_pg import delete_tenant_usuario

        if not delete_tenant_usuario(user_id, oid):
            flash("No se pudo eliminar el usuario en la base de datos.", "danger")
            return redirect(url_for("users"))
    else:
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
    old_owner_id = c.get("created_by")
    old_owner_u = store.users.get(old_owner_id, {}) if old_owner_id is not None else {}
    old_owner_name = old_owner_u.get("username") or old_owner_u.get("name") or "—"
    c["created_by"] = collector_id
    for L in store.loans.values():
        if L.get("client_id") == client_id and L.get("organization_id") == oid:
            L["created_by"] = collector_id

    try:
        u = current_user()
        new_owner = new_collector.get("username") or new_collector.get("name") or "—"
        full_nm = f"{c.get('first_name') or ''} {c.get('last_name') or ''}".strip()
        log_action(
            u.get("id"),
            "reasignar cliente",
            module="clientes",
            detail=f"{full_nm} (de {old_owner_name} a {new_owner})",
        )
    except Exception:
        pass
    flash("Cliente y préstamos reasignados.", "success")
    return redirect(url_for("client_detail", client_id=client_id))


@app.route("/clients")
@login_required
def clients():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    rows = clients_for_user(org_id, user)
    if os.getenv("DEBUG_CLIENTS"):
        app.logger.info(
            "clients() org_id=%s count=%s ids=%s",
            org_id,
            len(rows),
            [c.get("id") for c in rows],
        )
    t = "".join(
        f"<tr><td>{c['first_name']} {c.get('last_name') or ''}</td><td>{c.get('phone') or ''}</td>"
        f"<td><a class='btn btn-secondary' href='{url_for('client_detail', client_id=c['id'])}'>Ver</a></td></tr>"
        for c in rows
    )
    new_client_btn = ""
    if can_admin_actions(user) or is_cajero_role(user):
        new_client_btn = f'<a class="btn btn-primary" href="{url_for("new_client")}">Nuevo cliente</a>'
    body = (
        f'<div class="card"><h2>Clientes</h2><div class="table-scroll"><table><tr><th>Nombre</th><th>Tel</th><th></th></tr>{t}</table></div>'
        + new_client_btn
        + "</div>"
    )
    return page(body)


@app.route("/clients/new", methods=["GET", "POST"])
@login_required
def new_client():
    user = current_user()
    ensure_org()
    if not (can_admin_actions(user) or is_cajero_role(user)):
        flash("Acción restringida.", "danger")
        return redirect(url_for("clients"))
    if request.method == "POST":
        first = (request.form.get("first_name") or "").strip()
        if not first:
            flash("Nombre obligatorio.", "danger")
            return redirect(url_for("new_client"))
        route = (request.form.get("route") or "").strip()
        oid = session.get("org_id")
        last = (request.form.get("last_name") or "").strip()
        if USE_DATABASE:
            from credimapa_pg import create_client as _db_create_client, get_clients

            try:
                new_id = _db_create_client(
                    admin_id=oid,
                    created_by=user["id"],
                    first_name=first,
                    last_name=last,
                    document_id=(request.form.get("document_id") or "").strip(),
                    phone=(request.form.get("phone") or "").strip(),
                    address=(request.form.get("address") or "").strip(),
                    route=route,
                )
                if os.getenv("DEBUG_CLIENTS"):
                    snap = list(get_clients([oid]).values())
                    app.logger.info(
                        "new_client DB committed id=%s org=%s total_in_org=%s snapshot=%s",
                        new_id,
                        oid,
                        len(snap),
                        [(x.get("id"), x.get("first_name")) for x in snap],
                    )
            except Exception:
                app.logger.exception("new_client PostgreSQL")
                flash("Error al guardar el cliente en la base de datos.", "danger")
                return redirect(url_for("new_client"))
        else:
            cid = store.nid("clients")
            store.clients[cid] = {
                "id": cid,
                "first_name": first,
                "last_name": last,
                "document_id": (request.form.get("document_id") or "").strip(),
                "phone": (request.form.get("phone") or "").strip(),
                "address": (request.form.get("address") or "").strip(),
                "route": route,
                "created_by": user["id"],
                "organization_id": oid,
                "created_at": utc_now_for_db(),
            }
            if os.getenv("DEBUG_CLIENTS"):
                app.logger.info(
                    "new_client memory id=%s store_clients=%s",
                    cid,
                    list(store.clients.keys()),
                )
        try:
            full_nm = f"{first} {last}".strip()
            log_action(
                user["id"],
                "crear cliente",
                detail=f"{full_nm} (Ruta: {route or '—'})",
                module="clientes",
            )
        except Exception:
            pass
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
    user = current_user()
    ensure_org()
    oid = session.get("org_id")
    c = client_dict_by_id(client_id, oid)
    if not c:
        flash("No encontrado.", "danger")
        return redirect(url_for("clients"))
    if not can_admin_actions(user):
        flash("Acción restringida: solo admin puede editar clientes.", "danger")
        return redirect(url_for("clients"))
    if request.method == "POST":
        fn = (request.form.get("first_name") or "").strip()
        if not fn:
            flash("Nombre obligatorio.", "danger")
            return redirect(url_for("edit_client", client_id=client_id))
        if USE_DATABASE:
            from credimapa_pg import update_client as _db_update_client

            if not _db_update_client(
                client_id,
                oid,
                first_name=fn,
                last_name=(request.form.get("last_name") or "").strip(),
                phone=(request.form.get("phone") or "").strip(),
                address=(request.form.get("address") or "").strip(),
                document_id=(request.form.get("document_id") or "").strip(),
                route=(request.form.get("route") or "").strip(),
            ):
                flash("No se pudo guardar.", "danger")
                return redirect(url_for("edit_client", client_id=client_id))
        else:
            c["first_name"] = fn
            c["last_name"] = (request.form.get("last_name") or "").strip()
            c["phone"] = (request.form.get("phone") or "").strip()
            c["address"] = (request.form.get("address") or "").strip()
            c["document_id"] = (request.form.get("document_id") or "").strip()
            c["route"] = (request.form.get("route") or "").strip()
        try:
            full_nm = f"{fn} {(request.form.get('last_name') or '').strip()}".strip()
            log_action(
                user["id"],
                "editar cliente",
                module="clientes",
                detail=f"{full_nm} (ID: {client_id})",
            )
        except Exception:
            pass
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
@admin_required
def delete_client(client_id):
    ensure_org()
    oid = session.get("org_id")
    c = client_dict_by_id(client_id, oid)
    if not c:
        flash("Cliente no encontrado.", "danger")
        return redirect(url_for("clients"))
    u = current_user()

    # Prevenir eliminar si tiene préstamos activos (remaining > 0)
    activos_con_saldo = [
        (lid, L) for lid, L in client_loans
        if float(L.get("remaining") or 0) > 0
    ]
    if activos_con_saldo:
        flash("No se puede eliminar: el cliente tiene préstamos activos con saldo pendiente.", "danger")
        return redirect(url_for("client_detail", client_id=client_id))

    # Revertir movimientos financieros de todos los préstamos del cliente.
    if USE_DATABASE:
        from credimapa_pg import get_loans

        loans_map = get_loans([oid])
        client_loans = [
            (L["id"], L)
            for L in loans_map.values()
            if L.get("organization_id") == oid and L.get("client_id") == client_id
        ]
    else:
        client_loans = [
            (lid, L) for lid, L in list(store.loans.items())
            if L.get("organization_id") == oid and L.get("client_id") == client_id
        ]
    try:
        for lid, L in client_loans:
            _revert_loan_financials(lid, oid, u.get("id"))
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("client_detail", client_id=client_id))

    if USE_DATABASE:
        from credimapa_pg import delete_client_db

        if not delete_client_db(client_id, oid, [lid for lid, _ in client_loans]):
            flash("No se pudo eliminar el cliente en la base de datos.", "danger")
            return redirect(url_for("client_detail", client_id=client_id))
    else:
        store.clients.pop(client_id, None)
        for lid, _ in client_loans:
            store.loans.pop(lid, None)
    try:
        full_nm = f"{c.get('first_name') or ''} {c.get('last_name') or ''}".strip()
        log_action(
            u.get("id"),
            "eliminar cliente",
            module="clientes",
            detail=f"{full_nm} (ID: {client_id}) — banco ajustado",
        )
    except Exception:
        pass
    flash("Cliente eliminado. Banco ajustado correctamente.", "success")
    return redirect(url_for("clients"))


@app.route("/clients/<int:client_id>")
@login_required
def client_detail(client_id):
    ensure_org()
    oid = session.get("org_id")
    c = client_dict_by_id(client_id, oid)
    if not c:
        flash("No encontrado.", "danger")
        return redirect(url_for("clients"))
    user = current_user()
    can_admin = can_admin_actions(user)
    if is_cajero_role(user) and c.get("created_by") != user["id"] and not is_cartera_admin(user):
        flash("Sin acceso.", "danger")
        return redirect(url_for("clients"))
    try:
        full_nm = f"{c.get('first_name') or ''} {c.get('last_name') or ''}".strip() or f"Cliente #{client_id}"
        log_action(
            user.get("id"),
            "ver cliente",
            module="clientes",
            detail=full_nm,
        )
    except Exception:
        pass
    org_id = session.get("org_id")
    sd = calc_client_score(client_id, org_id)
    mx = calc_max_credito(sd["prestamos_pagados"], sd["score"])
    score = int(sd.get("score") or 0)
    nivel = sd.get("nivel") or "—"
    nivel_letter = (str(nivel).split("—", 1)[0] or "—").strip()
    if score >= 80:
        score_color = "#16a34a"
        score_label_color = "#16a34a"
    elif score >= 60:
        score_color = "#fbbf24"
        score_label_color = "#a16207"
    else:
        score_color = "#ef4444"
        score_label_color = "#dc2626"

    score_html = (
        f"""
        <div class="client-card client-score" data-animate="1">
          <div class="client-score-head">
            <div class="client-score-ico" aria-hidden="true">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M12 2v4"/>
                <path d="M12 18v4"/>
                <path d="M4.93 4.93l2.83 2.83"/>
                <path d="M16.24 16.24l2.83 2.83"/>
                <path d="M2 12h4"/>
                <path d="M18 12h4"/>
                <path d="M4.93 19.07l2.83-2.83"/>
                <path d="M16.24 7.76l2.83-2.83"/>
                <circle cx="12" cy="12" r="3"/>
              </svg>
            </div>
            <div class="client-score-headtxt">
              <div class="client-score-title">Score de crédito</div>
              <div class="client-score-levelwrap">
                <span class="level-pill" style="color:{score_label_color}; border-color: rgba(148,163,184,.35);">{nivel_letter}</span>
              </div>
            </div>
          </div>

          <div class="client-score-metric">
            <div class="client-score-score" style="color:{score_label_color};">{score}</div>
            <div class="client-score-nivel">{html.escape(str(nivel))}</div>
          </div>

          <div class="client-progress" role="progressbar" aria-valuenow="{score}" aria-valuemin="0" aria-valuemax="100">
            <div class="client-progress-fill" style="width:{score}%; background:{score_color};"></div>
          </div>

          <div class="client-score-substats">
            <div><b>Pagados:</b> {sd.get("prestamos_pagados")}</div>
            <div><b>Atrasos:</b> {sd.get("atrasos")}</div>
          </div>

          <div class="client-score-reco"><b>Crédito recomendado:</b> {fmt_money(mx)}</div>
        </div>
        """
    )
    loans = [L for L in loans_for_user(oid, user) if L.get("client_id") == client_id]

    def fmt_date(d):
        return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)

    def fmt_date_ddmmyyyy(d):
        return d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(d)

    # Tabla de préstamos (alineada al formato de tu otro sistema).
    loan_rows = []
    for L in sorted(loans, key=lambda x: -x.get("id", 0)):
        pays = payments_for_loan(L.get("id"), oid)
        paid_interest = round(sum(float(p.get("interest") or 0) for p in pays), 2)
        upfront_pct = float(L.get("upfront_percent") or 0.0)
        inicio = fmt_date_ddmmyyyy(L.get("start_date"))

        term_count_l = int(L.get("term_count") or 1)
        cuota_count = count_loan_cuota_payments(L.get("id"))
        loan_is_closed = cuota_count >= term_count_l
        status_badge = (
            "<span class='status-badge status-closed'>Cerrado</span>"
            if loan_is_closed
            else "<span class='status-badge status-active'>Activo</span>"
        )

        del_form = (
            f"<form method='post' action='{url_for('delete_loan', loan_id=L['id'])}' "
            f"style='margin:0' onsubmit=\"return confirm('¿Seguro que deseas eliminar este préstamo? Se revertirán los movimientos financieros.');\">"
            f"<button class='btn btn-danger btn-action' type='submit'>"
            f"<span class='btn-ic' aria-hidden='true'>"
            f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='18' height='18' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>"
            f"<path d='M3 6h18' /><path d='M8 6V4h8v2' /><path d='M19 6l-1 14H6L5 6' />"
            f"</svg>"
            f"</span>Eliminar</button>"
            f"</form>"
        )

        if can_admin:
            actions = (
                f"<a class='btn btn-secondary btn-action' href='{url_for('loan_detail', loan_id=L['id'])}'>"
                f"<span class='btn-ic' aria-hidden='true'>"
                f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='18' height='18' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>"
                f"<path d='M2 12s4-8 10-8 10 8 10 8-4 8-10 8S2 12 2 12z' /><circle cx='12' cy='12' r='3' />"
                f"</svg>"
                f"</span>Ver</a> "
                f"<a class='btn btn-primary btn-action' href='{url_for('edit_loan', loan_id=L['id'])}'>"
                f"<span class='btn-ic' aria-hidden='true'>"
                f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='18' height='18' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>"
                f"<path d='M12 20h9' /><path d='M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z' />"
                f"</svg>"
                f"</span>Editar</a> "
                f"{del_form}"
            )
        else:
            actions = (
                f"<a class='btn btn-secondary btn-action' href='{url_for('loan_detail', loan_id=L['id'])}'>"
                f"<span class='btn-ic' aria-hidden='true'>"
                f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='18' height='18' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>"
                f"<path d='M2 12s4-8 10-8 10 8 10 8-4 8-10 8S2 12 2 12z' /><circle cx='12' cy='12' r='3' />"
                f"</svg>"
                f"</span>Ver</a>"
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
            f"<td>{status_badge}</td>"
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

    style_block = """
<style>
  .client-shell{max-width:1040px;margin:0 auto;padding:16px 10px 30px}
  .client-top-grid{display:grid;gap:14px}
  @media(min-width:860px){.client-top-grid{grid-template-columns: 1.4fr .9fr; align-items:start}}

  .client-card{
    background: rgba(255,255,255,.92);
    border: 1px solid rgba(148,163,184,.35);
    border-radius: 18px;
    box-shadow: 0 10px 24px rgba(0,0,0,.06);
    padding: 16px;
  }
  body.theme-dark .client-card{
    background: rgba(15,23,42,.92);
    border-color: rgba(148,163,184,.25);
    box-shadow: 0 14px 34px rgba(0,0,0,.35);
  }

  [data-animate]{opacity:0;transform: translateY(10px);transition: opacity .35s ease, transform .35s ease}
  [data-animate].is-visible{opacity:1;transform: translateY(0)}

  .client-hero{display:flex;gap:14px;justify-content:space-between;flex-wrap:wrap;align-items:flex-start}
  .client-hero-left{display:flex;gap:14px;align-items:center}
  .client-avatar{
    width:56px;height:56px;border-radius:16px;
    background: linear-gradient(135deg, rgba(22,163,74,.95), rgba(99,102,241,.85));
    color:#fff;font-weight:1000;font-size:22px;
    display:flex;align-items:center;justify-content:center;
    box-shadow: 0 12px 26px rgba(22,163,74,.18);
  }
  .client-name{font-size:26px;font-weight:1000;line-height:1.15;margin:0 0 6px}
  .client-lines{display:grid;gap:8px;margin-top:6px}
  .client-line{display:flex;gap:10px;align-items:flex-start;color:#0f172a}
  body.theme-dark .client-line{color:#e5e7eb}
  .client-line .ic{flex:0 0 auto;margin-top:2px;opacity:.9}
  .client-line .txt{font-size:14px;opacity:.95}
  .client-actions{display:flex;flex-direction:column;gap:10px;min-width:280px}

  .btn-action{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:14px;border:1px solid transparent;transition: transform .12s ease, box-shadow .12s ease, filter .12s ease}
  .btn-ic{display:inline-flex;align-items:center;justify-content:center;opacity:.95}
  .btn-action:hover{transform: translateY(-1px);box-shadow: 0 10px 24px rgba(0,0,0,.08);filter: brightness(1.02)}
  .btn-action:active{transform: translateY(0) scale(.99);box-shadow:none}

  .btn{cursor:pointer}
  .btn-primary.btn-action{background: rgba(16,185,129,.14); border-color: rgba(16,185,129,.35); color:#047857}
  body.theme-dark .btn-primary.btn-action{background: rgba(16,185,129,.12); border-color: rgba(16,185,129,.25); color:#34d399}
  .btn-secondary.btn-action{background: rgba(59,130,246,.12); border-color: rgba(59,130,246,.25); color:#1d4ed8}
  body.theme-dark .btn-secondary.btn-action{background: rgba(59,130,246,.10); border-color: rgba(59,130,246,.22); color:#60a5fa}
  .btn-danger.btn-action{background: rgba(239,68,68,.10); border-color: rgba(239,68,68,.30); color:#dc2626}
  body.theme-dark .btn-danger.btn-action{background: rgba(239,68,68,.08); border-color: rgba(239,68,68,.22); color:#fb7185}

  .client-cta-row{display:flex;justify-content:flex-end;gap:10px;flex-wrap:wrap}
  .client-move{border-top: 1px solid rgba(148,163,184,.35); padding-top: 10px; margin-top: 8px}
  body.theme-dark .client-move{border-top-color: rgba(148,163,184,.22)}
  .client-move label{font-weight:900; margin-right: 6px; opacity:.95}
  .client-move select{
    padding: 9px 10px; border-radius: 12px; border: 1px solid rgba(148,163,184,.35);
    background: rgba(255,255,255,.8); color:#0f172a;
  }
  body.theme-dark .client-move select{background: rgba(2,6,23,.55); color:#e5e7eb}

  .client-score{position:relative}
  .client-score-head{display:flex;gap:12px;align-items:flex-start}
  .client-score-ico{width:34px;height:34px;border-radius:12px;background: rgba(59,130,246,.12); border:1px solid rgba(59,130,246,.22); display:flex;align-items:center;justify-content:center; color:#1d4ed8}
  body.theme-dark .client-score-ico{background: rgba(59,130,246,.10); border-color: rgba(59,130,246,.18)}
  .client-score-title{font-weight:1000}
  .client-score-ico svg{display:block}
  .client-score-headtxt{flex:1}
  .client-score-title{font-weight:1000}
  .client-score-levelwrap{margin-top:6px}
  .level-pill{
    display:inline-flex;align-items:center;justify-content:center;
    width:30px;height:30px;border-radius: 999px;
    border:1px solid rgba(148,163,184,.35);
    background: rgba(255,255,255,.35);
    font-weight:1000;
    font-size: 13px;
  }
  .client-score-metric{display:flex;align-items:baseline;gap:14px;margin-top:12px}
  .client-score-score{font-size: 44px; font-weight:1000; line-height:1}
  .client-score-nivel{font-weight:900; opacity:.9}
  .client-score-substats{display:flex;justify-content:space-between;gap:10px;margin-top:10px;opacity:.95; font-size:13px}
  .client-score-reco{margin-top: 10px; padding-top: 10px; border-top:1px solid rgba(148,163,184,.25); font-weight:900; font-size:14px}

  .client-progress{
    height: 12px;
    border-radius: 999px;
    background: rgba(148,163,184,.22);
    overflow:hidden;
    margin-top: 14px;
    border: 1px solid rgba(148,163,184,.25);
  }
  .client-progress-fill{height:100%; border-radius: 999px; transition: width .6s ease}

  .status-badge{
    display:inline-flex;align-items:center;gap:8px;
    padding: 6px 10px;border-radius: 999px;
    font-size: 12px; font-weight: 1000;
    border: 1px solid transparent;
  }
  .status-active{background: rgba(16,185,129,.12); color:#047857; border-color: rgba(16,185,129,.30)}
  .status-closed{background: rgba(59,130,246,.10); color:#1d4ed8; border-color: rgba(59,130,246,.25)}
  body.theme-dark .status-closed{background: rgba(59,130,246,.08); color:#60a5fa; border-color: rgba(59,130,246,.22)}

  .client-loans{margin-top:14px}
  .client-loans-head{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px}
  .client-loans-title{font-size:18px;font-weight:1000}
  .client-table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px}
  .client-table thead th{
    text-align:left;padding:10px 10px;
    border-bottom:2px solid rgba(148,163,184,.25);
    color: rgba(15,23,42,.92);
    background: rgba(2,132,199,.06);
    position: sticky; top: 0;
  }
  body.theme-dark .client-table thead th{color: rgba(229,231,235,.95); background: rgba(59,130,246,.08); border-bottom-color: rgba(148,163,184,.18)}
  .client-table tbody td{padding:12px 10px;border-bottom:1px solid rgba(148,163,184,.18); vertical-align: middle}
  body.theme-dark .client-table tbody td{border-bottom-color: rgba(148,163,184,.14)}

  .table-scroll{overflow:auto;border-radius:14px}
  .fade-soft{animation: fadeSoft .35s ease both}
  @keyframes fadeSoft{from{opacity:.0; transform: translateY(6px)} to{opacity:1; transform: translateY(0)}}
</style>
"""

    full_name = f"{c.get('first_name') or ''} {c.get('last_name') or ''}".strip() or f"Cliente #{client_id}"
    avatar_letter = (c.get("first_name") or full_name[:1] or "?").strip()[:1].upper()

    # Acciones solo para admin (cajero/cobrador no debe ver operaciones críticas).
    client_actions_html = ""
    if can_admin:
        client_actions_html = f"""
          <div class="client-actions">
            <div class="client-cta-row">
              <a class="btn btn-secondary btn-action" href="{url_for("edit_client", client_id=client_id)}">
                <span class="btn-ic" aria-hidden="true">
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg>
                </span>Editar</a>
              <form method="post" action="{url_for("delete_client", client_id=client_id)}" style="margin:0" onsubmit="return confirm('¿Seguro que deseas eliminar este cliente? Se revertirán los movimientos financieros.');">
                <button class="btn btn-danger btn-action" type="submit">
                  <span class="btn-ic" aria-hidden="true">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M19 6l-1 14H6L5 6"/></svg>
                  </span>Eliminar</button>
              </form>
            </div>

            <div class="client-move">
              <form method="post" action="{url_for('reassign_single_client', client_id=client_id)}" style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
                <label>Reasignar cobrador:</label>
                <select name="collector_id">{collectors_opts}</select>
                <button class="btn btn-primary btn-action" type="submit">
                  <span class="btn-ic" aria-hidden="true">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 11H3"/><path d="M4 11l7-7"/><path d="M4 11l7 7"/></svg>
                  </span>Mover cliente</button>
              </form>
            </div>
          </div>
        """
    else:
        client_actions_html = (
            '<div class="client-actions">'
            '<div style="opacity:.85;font-weight:900;font-size:13px;">'
            'Solo admin puede editar/eliminar o mover clientes.'
            '</div>'
            '</div>'
        )

    header = (
        f"""
        <div class="client-hero client-card data-animate" data-animate="1">
          <div class="client-hero-left">
            <div class="client-avatar" aria-hidden="true">{html.escape(avatar_letter)}</div>
            <div>
              <div class="client-name">{html.escape(full_name)}</div>
              <div class="client-lines">
                <div class="client-line"><span class="ic" aria-hidden="true">
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2A19.8 19.8 0 0 1 3 5.18 2 2 0 0 1 5.11 3h3a2 2 0 0 1 2 1.72c.12.86.31 1.7.57 2.5a2 2 0 0 1-.45 2.11L9.09 10.91a16 16 0 0 0 4 4l1.58-1.12a2 2 0 0 1 2.11-.45c.8.26 1.64.45 2.5.57A2 2 0 0 1 22 16.92z"/></svg>
                </span><span class="txt"><b>Tel:</b> {html.escape(str(c.get('phone') or '—'))}</span></div>
                <div class="client-line"><span class="ic" aria-hidden="true">
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 21s-7-4.35-7-11a7 7 0 0 1 14 0c0 6.65-7 11-7 11z"/><circle cx="12" cy="10" r="2"/></svg>
                </span><span class="txt"><b>Dirección:</b> {html.escape(str(c.get('address') or '—'))}</span></div>
                <div class="client-line"><span class="ic" aria-hidden="true">
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15V3a2 2 0 0 0-2-2H7a2 2 0 0 0-2 2v18a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2z"/><path d="M3 15h18"/><path d="M8 7h5"/><path d="M8 11h7"/></svg>
                </span><span class="txt"><b>Documento:</b> {html.escape(str(c.get('document_id') or '—'))}</span></div>
                <div class="client-line"><span class="ic" aria-hidden="true">
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 17l6-6 4 4 8-8"/><path d="M14 5h7v7"/></svg>
                </span><span class="txt"><b>Ruta:</b> {html.escape(str(c.get('route') or '—'))}</span></div>
              </div>
            </div>
          </div>

          {client_actions_html}
        </div>
        """
    )

    new_loan_href = url_for("new_loan") + "?client_id=" + str(client_id)
    new_loan_btn_html = ""
    if can_admin:
        new_loan_btn_html = (
            "<a class=\"btn btn-primary btn-action\" href=\"" + new_loan_href + "\">"
            "<span class=\"btn-ic\" aria-hidden=\"true\">"
            "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 24 24\" width=\"18\" height=\"18\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M12 5v14\"/><path d=\"M5 12h14\"/></svg>"
            "</span>Nuevo préstamo</a>"
        )
    toggle_href = url_for("toggle_theme")
    theme_now = get_theme()
    toggle_label = "Claro" if theme_now == "dark" else "Oscuro"
    script_block = (
        "<script>"
        "document.addEventListener('DOMContentLoaded', () => "
        "document.querySelectorAll('[data-animate]').forEach(el => el.classList.add('is-visible'))"
        ");"
        "</script>"
    )

    body = (
        style_block
        + "<div class=\"client-shell\">"
        + "<div class=\"client-top-grid\">"
        + header
        + score_html
        + "</div>"
        + "<div class=\"client-card client-loans data-animate\" data-animate=\"1\">"
        + "<div class=\"client-loans-head\">"
        + "<div><div class=\"client-loans-title\">Préstamos del cliente</div>"
        + "<div style=\"font-size:13px;opacity:.9;margin-top:4px\">Historial, estado y acciones.</div></div>"
        + "<a class=\"btn btn-secondary btn-action\" href=\"" + toggle_href + "\">"
        + "<span class=\"btn-ic\" aria-hidden=\"true\">"
        + "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 24 24\" width=\"18\" height=\"18\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z\"/></svg>"
        + "</span>" + toggle_label + "</a>"
        + new_loan_btn_html
        + "</div>"
        + "<div class=\"table-scroll\" style=\"margin-top:12px;\">"
        + "<table class=\"client-table\">"
        + "<thead><tr>"
        + "<th>ID</th><th>Monto</th><th>Restante</th><th>%</th><th>Frecuencia</th><th>Inicio</th><th>Interés pagado</th><th>Estado</th><th>Acciones</th>"
        + "</tr></thead>"
        + "<tbody>" + lr + "</tbody>"
        + "</table></div></div></div>"
        + script_block
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
    can_edit = user.get("role") == "admin" or is_cartera_admin(user)
    for L in sorted(rows, key=lambda x: -x["id"]):
        cid = L.get("client_id")
        cl = client_dict_by_id(cid, org_id) or {}
        nm_raw = f"{cl.get('first_name','')} {cl.get('last_name') or ''}".strip() or "Sin nombre"
        nm = html.escape(nm_raw)
        freq = loan_frequency_label(L.get("frequency"))
        term_count = int(L.get("term_count") or 1)
        pagadas = count_loan_cuota_payments(L["id"])
        if pagadas >= term_count:
            badge = '<span class="loan-card-badge loan-card-badge-done">Cerrado</span>'
        else:
            badge = '<span class="loan-card-badge">Activo</span>'
        sub = html.escape(f"Préstamo #{L['id']} - {freq}")
        amt = html.escape(fmt_money(L.get("remaining")))
        href = url_for("loan_detail", loan_id=L["id"])
        edit_href = url_for("edit_loan", loan_id=L["id"])
        edit_btn = (
            f'<a class="loan-card-edit" href="{edit_href}" onclick="event.stopPropagation();">Editar</a>'
            if can_edit else ""
        )
        cards.append(
            f'<div class="loan-card-wrap">'
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
            f"{edit_btn}"
            f"</div>"
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
.loan-card-wrap {{ position: relative; margin-bottom: 12px; }}
.loan-card-link {{ text-decoration: none !important; color: inherit; display: block; }}
.loan-card-edit {{
  position: absolute; top: 10px; right: 10px;
  font-size: 12px; font-weight: 700; padding: 4px 10px;
  border-radius: 8px; background: rgba(59,130,246,.15); color: #1d4ed8;
  text-decoration: none !important; z-index: 2;
}}
.loan-card-edit:hover {{ background: rgba(59,130,246,.3); }}
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
  background: #16a34a;
  color: #fff;
  letter-spacing: .02em;
}}
.loan-card-badge-done {{ background: #dc2626; }}
.loan-card-amt {{ font-weight: 900; font-size: 17px; color: #15803d; white-space: nowrap; flex-shrink: 0; }}
.loans-empty {{ text-align: center; opacity: .85; padding: 24px; }}
</style>
<div class="loans-page-wrap">
  <h1 class="loans-page-title">📋 Lista de Préstamos</h1>
  <div class="loans-toolbar">
    {('<a class="btn-loan-new" href="' + url_for("new_loan") + '">👤➕ Nuevo préstamo</a>') if (can_admin_actions(user) or is_cajero_role(user)) else ''}
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
    if not (can_admin_actions(user) or is_cajero_role(user)):
        flash("Acción restringida.", "danger")
        return redirect(url_for("loans"))
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
        end_str = (request.form.get("end_date") or "").strip()
        term_count = request.form.get("term_count", type=int) or 1
        if not client_id or amount is None or not start_str:
            flash("Complete cliente, monto y fecha.", "danger")
            return redirect(url_for("new_loan"))
        if is_cajero_role(user):
            # Validación backend: el cajero solo puede crear préstamos para sus clientes.
            c = client_dict_by_id(client_id, org_id)
            if not c or c.get("created_by") != user.get("id"):
                flash("Sin acceso al cliente seleccionado.", "danger")
                return redirect(url_for("new_loan"))
        if amount <= 0:
            flash("El monto debe ser mayor que 0.", "danger")
            return redirect(url_for("new_loan"))
        if rate < 0:
            flash("La tasa no puede ser negativa.", "danger")
            return redirect(url_for("new_loan"))

        if str(freq).strip().lower() == "custom":
            if not end_str:
                flash("Para frecuencia personalizada indique fecha de fin.", "danger")
                return redirect(url_for("new_loan"))
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            days = (end_date - start_date).days
            if days <= 0:
                flash("Rango de fechas inválido. La fecha fin debe ser posterior a la fecha inicio.", "danger")
                return redirect(url_for("new_loan"))
            term_count = days
            total_interest = round(amount * (rate / 100), 2)
            total_to_pay = round(amount + total_interest, 2)
            installment_amount = round(total_to_pay / days, 2)
            interval_days = 1
            next_payment_date = start_date
        else:
            if term_count < 1:
                flash("Cuotas inválidas.", "danger")
                return redirect(url_for("new_loan"))
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            interval_days = freq_interval_days(freq)
            next_payment_date = start_date + timedelta(days=interval_days)
            total_interest = round((amount * rate / 100) * term_count, 2)
            total_to_pay = round(amount + total_interest, 2)
            installment_amount = round(total_to_pay / max(term_count, 1), 2)

        discount_amount = round(amount * upfront_percent / 100.0, 2)
        monto_entregado = round(amount - discount_amount, 2)
        if monto_entregado < 0:
            monto_entregado = 0.0

        discount_cash_id = None
        disbursement_cash_id = None

        # Fórmula: caja = caja - capital + descuento
        # 1) Se resta el capital completo del banco (préstamo entregado).
        # 2) El descuento inicial vuelve a caja (se suma).
        try:
            if amount > 0:
                disbursement_cash_id = apply_cash_movement(
                    movement_type="prestamo_entregado",
                    amount=-float(amount),
                    note=f"Préstamo entregado cliente #{client_id}",
                    user_id=user["id"],
                    org_id=org_id,
                )
            if discount_amount > 0:
                discount_cash_id = apply_cash_movement(
                    movement_type="descuento_inicial",
                    amount=discount_amount,
                    note=f"Descuento inicial préstamo cliente #{client_id}",
                    user_id=user["id"],
                    org_id=org_id,
                )
                try:
                    cl = client_dict_by_id(client_id, org_id) or {}
                    client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{client_id}"
                    log_action(
                        user["id"],
                        "registrar descuento inicial",
                        module="descuentos",
                        detail=f"{fmt_money(discount_amount)} para {client_nm}",
                    )
                except Exception:
                    pass
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("new_loan"))

        if USE_DATABASE:
            from credimapa_pg import create_prestamo as _db_create_prestamo

            lid = _db_create_prestamo(
                admin_id=org_id,
                client_id=client_id,
                created_by=user["id"],
                amount=float(amount),
                rate=float(rate),
                frequency=freq,
                start_date=start_date,
                next_payment_date=next_payment_date,
                term_count=term_count,
                remaining=float(amount),
                total_interest=float(total_interest),
                total_to_pay=float(total_to_pay),
                upfront_percent=float(upfront_percent),
                installment_amount=float(installment_amount),
                discount_banco_id=discount_cash_id,
                disbursement_banco_id=disbursement_cash_id,
            )
        else:
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
                "remaining": amount,  # capital pendiente = capital aprobado (el descuento no lo reduce)
                "remaining_capital": amount,
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
        try:
            cl = client_dict_by_id(client_id, org_id) or {}
            client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{client_id}"
            log_action(
                user["id"],
                "crear préstamo",
                module="préstamos",
                detail=f"{fmt_money(amount)} para {client_nm} (cuotas: {term_count})",
            )
        except Exception:
            pass
        flash("Préstamo creado.", "success")
        return redirect(url_for("loan_detail", loan_id=lid))
    opts = "".join(f"<option value='{c['id']}'{' selected' if request.args.get('client_id', type=int)==c['id'] else ''}>{c['first_name']}</option>" for c in clist)
    today_iso = today_rd().isoformat()
    body = (
        f'<div class="card"><h2>Nuevo préstamo</h2>'
        f'<p style="margin-top:6px; opacity:.9;"><b>Banco disponible:</b> {fmt_money(get_bank_available(org_id))}</p>'
        f'<form method="post" id="newLoanForm">'
        f'<label>Cliente</label><select name="client_id" required>{opts}</select>'
        f'<label>Monto</label><input name="amount" type="number" step="0.01" required>'
        f'<label>Tasa %</label><input name="rate" type="number" step="0.01" value="10">'
        f'<label>Descuento inicial (%)</label><input name="upfront_percent" type="number" step="0.01" value="0" min="0" max="100">'
        f'<label>Frecuencia</label><select name="frequency" id="freqSelect">'
        f'<option value="semanal">semanal</option><option value="diario">diario</option>'
        f'<option value="quincenal">quincenal</option><option value="mensual">mensual</option>'
        f'<option value="custom">Personalizado (por fecha)</option></select>'
        f'<label>Fecha inicio</label><input name="start_date" type="date" value="{today_iso}" required>'
        f'<div id="freqStandard">'
        f'<label>Cuotas</label><input name="term_count" type="number" value="10" min="1">'
        f'</div>'
        f'<div id="freqCustom" style="display:none">'
        f'<p style="padding:10px;margin:10px 0;background:rgba(59,130,246,.1);border-radius:10px;font-size:13px;">'
        f'Este préstamo será calculado según las fechas seleccionadas.</p>'
        f'<label>Fecha fin</label><input name="end_date" type="date">'
        f'</div>'
        f'<button class="btn btn-primary" type="submit">Crear</button></form></div>'
        '<script>'
        "(function(){"
        "var sel=document.getElementById('freqSelect');"
        "var std=document.getElementById('freqStandard');"
        "var custom=document.getElementById('freqCustom');"
        "function toggle(){"
        "var isCustom=sel.value==='custom';"
        "std.style.display=isCustom?'none':'block';"
        "custom.style.display=isCustom?'block':'none';"
        "var endInp=custom.querySelector('input[name=end_date]');"
        "if(endInp)endInp.required=isCustom;"
        "}"
        "sel.addEventListener('change',toggle);"
        "toggle();"
        "})();"
        '</script>'
    )
    return page(body)


@app.route("/loans/<int:loan_id>/delete", methods=["POST"])
@login_required
def delete_loan(loan_id):
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    if user.get("role") != "admin" and not is_cartera_admin(user):
        flash("Solo admin puede borrar préstamos.", "danger")
        return redirect(url_for("loans"))
    L = loan_dict_by_id(loan_id, oid)
    if not L:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))

    try:
        _revert_loan_financials(loan_id, oid, user.get("id"))
        if USE_DATABASE:
            from credimapa_pg import delete_loan_row

            if not delete_loan_row(loan_id, oid):
                flash("No se pudo eliminar el préstamo en la base de datos.", "danger")
                return redirect(url_for("loan_detail", loan_id=loan_id))
        else:
            store.loans.pop(loan_id, None)
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    try:
        user = current_user()
        cl = client_dict_by_id(L.get("client_id"), oid) if L else {}
        client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{L.get('client_id') if L else '—'}"
        loan_amt = float(L.get("amount") or 0) if L else 0
        log_action(
            user.get("id"),
            "eliminar préstamo",
            module="préstamos",
            detail=f"#{loan_id} • {fmt_money(loan_amt)} para {client_nm} — banco ajustado",
        )
    except Exception:
        pass
    flash("Préstamo eliminado. Banco ajustado correctamente.", "success")
    return redirect(url_for("loans"))


@app.route("/loans/<int:loan_id>/edit", methods=["GET", "POST"])
@login_required
def edit_loan(loan_id):
    ensure_org()
    oid = session.get("org_id")
    L = loan_dict_by_id(loan_id, oid)
    if not L:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))
    user = current_user()
    if user.get("role") != "admin" and not is_cartera_admin(user):
        flash("Solo admin puede editar préstamos.", "danger")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    pays = payments_for_loan(loan_id, oid)
    pagos_cuotas = [p for p in pays if (p.get("type") or "").lower() == "cuota"]
    has_payments = len(pagos_cuotas) > 0
    allow_amount_edit = not has_payments

    if request.method == "POST":
        amount_raw = request.form.get("amount", type=float)
        rate_raw = request.form.get("rate", type=float)
        term_count_raw = request.form.get("term_count", type=int)
        installment_raw = request.form.get("installment_amount", type=float)
        start_str = (request.form.get("start_date") or "").strip()

        amount = amount_raw if amount_raw is not None else float(L.get("amount") or 0)
        rate = rate_raw if rate_raw is not None else float(L.get("rate") or 0)
        term_count = term_count_raw if term_count_raw is not None else int(L.get("term_count") or 1)
        if not allow_amount_edit:
            amount = float(L.get("amount") or 0)
        if amount <= 0:
            flash("El monto debe ser mayor que 0.", "danger")
            return redirect(url_for("edit_loan", loan_id=loan_id))
        if rate < 0:
            flash("La tasa no puede ser negativa.", "danger")
            return redirect(url_for("edit_loan", loan_id=loan_id))
        if term_count < 1:
            flash("Cuotas inválidas.", "danger")
            return redirect(url_for("edit_loan", loan_id=loan_id))
        if not start_str:
            flash("Fecha de inicio requerida.", "danger")
            return redirect(url_for("edit_loan", loan_id=loan_id))

        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        freq = L.get("frequency") or "semanal"
        is_custom = str(freq).strip().lower() == "custom"
        interval_days = freq_interval_days(freq)
        if is_custom:
            total_interest = round(amount * (rate / 100), 2)
        else:
            total_interest = round((amount * rate / 100) * term_count, 2)
        total_to_pay = round(amount + total_interest, 2)
        installment_amount = round(total_to_pay / max(term_count, 1), 2)
        if installment_raw is not None and installment_raw > 0:
            installment_amount = round(installment_raw, 2)

        if has_payments:
            capital_pagado = sum(float(p.get("capital") or 0) for p in pagos_cuotas)
            remaining = round(amount - capital_pagado, 2)
            remaining = max(0.0, remaining)
            if is_custom:
                next_payment_date = start_date + timedelta(days=len(pagos_cuotas))
            else:
                next_payment_date = start_date + timedelta(days=interval_days * (len(pagos_cuotas) + 1))
        else:
            remaining = amount
            next_payment_date = start_date if is_custom else start_date + timedelta(days=interval_days)

        if USE_DATABASE:
            from credimapa_pg import update_prestamo_edit

            if not update_prestamo_edit(
                loan_id,
                oid,
                amount=amount,
                rate=rate,
                term_count=term_count,
                installment_amount=installment_amount,
                start_date=start_date,
                total_interest=total_interest,
                total_to_pay=total_to_pay,
                remaining=remaining,
                next_payment_date=next_payment_date,
            ):
                flash("No se pudo guardar.", "danger")
                return redirect(url_for("edit_loan", loan_id=loan_id))
        else:
            L["amount"] = amount
            L["rate"] = rate
            L["term_count"] = term_count
            L["installment_amount"] = installment_amount
            L["start_date"] = start_date
            L["total_interest"] = total_interest
            L["total_to_pay"] = total_to_pay
            L["remaining"] = remaining
            L["remaining_capital"] = remaining
            L["next_payment_date"] = next_payment_date

        try:
            cl = client_dict_by_id(L.get("client_id"), oid) or {}
            client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{L.get('client_id') or '—'}"
            log_action(
                user.get("id"),
                "editar préstamo",
                module="préstamos",
                detail=f"#{loan_id} • {fmt_money(amount)} • cliente {client_nm}",
            )
        except Exception:
            pass
        flash("Préstamo actualizado correctamente.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    amount_val = L.get("amount")
    rate_val = L.get("rate")
    term_val = L.get("term_count")
    inst_val = L.get("installment_amount")
    start_val = L.get("start_date")
    if hasattr(start_val, "strftime"):
        start_str_val = start_val.strftime("%Y-%m-%d")
    else:
        start_str_val = str(start_val)[:10] if start_val else today_rd().isoformat()

    amount_disabled = ' disabled' if not allow_amount_edit else ''
    amount_title = ' (bloqueado: ya hay pagos)' if not allow_amount_edit else ''
    amount_hidden = f'<input type="hidden" name="amount" value="{amount_val}">' if not allow_amount_edit else ''
    warning_html = (
        '<div class="alert alert-warning" style="padding:12px;margin-bottom:16px;border-radius:12px;'
        'background:rgba(245,158,11,.15);border:1px solid rgba(245,158,11,.4);">'
        '<strong>⚠️ Advertencia:</strong> Editar préstamo puede afectar cálculos existentes. '
        'El monto no se puede modificar si ya hay pagos registrados.'
        '</div>'
    )
    body = (
        f'<div class="card"><h2>Editar préstamo #{loan_id}</h2>'
        f'{warning_html}'
        f'<form method="post">'
        f'{amount_hidden}'
        f'<label>Monto{amount_title}</label>'
        f'<input name="amount" value="{amount_val}" type="number" step="0.01" required{amount_disabled}>'
        f'<label>Tasa %</label><input name="rate" value="{rate_val}" type="number" step="0.01" required>'
        f'<label>Cuotas</label><input name="term_count" value="{term_val}" type="number" min="1" required>'
        f'<label>Cuota programada</label><input name="installment_amount" value="{inst_val}" type="number" step="0.01">'
        f'<label>Fecha inicio</label><input name="start_date" type="date" value="{start_str_val}" required>'
        f'<button class="btn btn-primary" type="submit">Guardar</button>'
        f'<a class="btn btn-secondary" href="{url_for("loan_detail", loan_id=loan_id)}" style="margin-left:8px;">Cancelar</a>'
        f'</form></div>'
    )
    return page(body)


@app.route("/loan/<int:loan_id>")
@login_required
def loan_detail(loan_id):
    ensure_org()
    oid = session.get("org_id")
    L = loan_dict_by_id(loan_id, oid)
    if not L:
        flash("No encontrado.", "danger")
        return redirect(url_for("loans"))
    u = current_user()
    if not scope_owns_loan(u, L):
        flash("Sin acceso.", "danger")
        return redirect(url_for("loans"))
    try:
        _cid = L.get("client_id")
        _c = client_dict_by_id(_cid, oid) or {}
        client_nm = f"{_c.get('first_name') or ''} {_c.get('last_name') or ''}".strip() or "Cliente"
        log_action(
            u.get("id"),
            "ver préstamo",
            module="préstamos",
            detail=f"#{loan_id} • {client_nm}",
        )
    except Exception:
        pass
    client_id = L.get("client_id")
    client = client_dict_by_id(client_id, oid) or {}
    pays = payments_for_loan(loan_id, oid)
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
    # Siempre: total = capital aprobado + interés (el descuento no reduce lo que debe)
    total_a_pagar = round(capital_aprobado + interes_total, 2)

    # Cuotas estimadas
    term_count = int(L.get("term_count") or 1)
    pagos_cuotas = [p for p in pays if (p.get("type") or "").lower() == "cuota"]
    pagadas = len(pagos_cuotas)
    restantes = max(term_count - pagadas, 0)
    cuota_label = L.get("frequency") or "semanal"

    # Siguiente pago (simplificado)
    next_pago = L.get("next_payment_date") or today_rd()
    def fmt_date(d):
        return d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(d)
    next_pago_disp = fmt_date(next_pago)

    estado_pago = "Pendiente"
    # Regla de cierre por cuotas: solo cerramos cuando completa todas las cuotas.
    # Si por datos previos el campo `status` quedó en `cerrado`, esto evita
    # que se bloquee el cobro antes de completar `term_count`.
    loan_is_closed = pagadas >= term_count
    if loan_is_closed:
        estado_pago = "Cerrado"

    # Etiqueta % sin decimales cuando sea entero (ej: 0% en vez de 0.0%)
    if abs(upfront_percent - round(upfront_percent)) < 1e-9:
        upfront_pct_label = f"{int(round(upfront_percent))}%"
    else:
        upfront_pct_label = f"{upfront_percent:.1f}%"

    # Nombre legible para frecuencia
    freq = str(L.get("frequency") or "").strip().lower()
    if "custom" in freq:
        freq_label = "día (personalizado)"
    elif "quinc" in freq:
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
        amt_p = p.get("amount") or 0
        del_btn = ""
        if can_admin:
            del_btn = (
                f" <form method='post' action='{url_for('delete_payment', payment_id=p.get('id'))}' "
                f"style='display:inline' onsubmit=\"return confirm('¿Seguro que deseas eliminar este pago de {fmt_money(amt_p)}? Esto ajustará el banco automáticamente.');\">"
                f"<button type='submit' class='btn btn-danger' style='padding:6px 10px'>Eliminar</button></form>"
            )
        hist_rows += (
            f"<tr>"
            f"<td>{idx}</td>"
            f"<td>{fmt_money(p.get('amount'))}</td>"
            f"<td>{fmt_money(p.get('capital'))}</td>"
            f"<td>{fmt_money(p.get('interest'))}</td>"
            f"<td>{p.get('date')}</td>"
            f"<td><a class='btn btn-secondary' style='padding:6px 10px' href='{url_for('print_payment', payment_id=p.get('id'))}' target='_blank' rel='noopener'>Imprimir recibo</a>{del_btn}</td>"
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
    first_due = next_pago if hasattr(next_pago, "strftime") else today_rd()
    # Normalizar a `date` para evitar comparaciones datetime vs date.
    first_due_d = first_due.date() if hasattr(first_due, "hour") else first_due

    cuota_items_html = ""
    for i in range(1, term_count + 1):
        due_i = first_due_d + timedelta(days=interval_days * (i - 1))
        if i <= pagadas:
            cuota_estado = "Pagada"
            state_class = "is-pagada"
        else:
            # Si la fecha ya pasó y no está pagada, se considera atrasada.
            if due_i < today_rd():
                cuota_estado = "Atrasada"
                state_class = "is-atrasada"
            else:
                cuota_estado = "Pendiente"
                state_class = "is-pendiente"
        cuota_items_html += (
            f"<div class='cuota {state_class}'>"
            f"<div class='cuota-k'>Cuota {i}</div>"
            f"<div><b>{fmt_date(due_i)}</b></div>"
            f"<div class='cuota-estado'>Estado: {cuota_estado}</div>"
            f"</div>"
        )

    # Siguiente pago = primera cuota pendiente
    if pagadas < term_count:
        next_idx = pagadas + 1
        next_due = first_due_d + timedelta(days=interval_days * (next_idx - 1))
        next_status = "Pendiente"
    else:
        next_idx = term_count
        next_due = first_due_d + timedelta(days=interval_days * (term_count - 1))
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

    registrar_pago_btn = (
        f'<a class="btn btn-primary" href="{url_for("new_payment", loan_id=loan_id)}">➕ Registrar pago</a>'
        if not loan_is_closed
        else ""
    )

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

/* Calendario cuotas (estilo premium) */
.cuota {{
  margin: 0;
  padding: 12px;
  min-width: 170px;
  flex: 1 1 170px;
  border-radius: 16px;
  box-shadow: 0 8px 20px rgba(0,0,0,.05);
  border: 1px solid rgba(148,163,184,.35);
  transition: transform .12s ease, box-shadow .12s ease;
  background: #f8fafc;
  color: #0f172a;
}}
.cuota:hover {{
  transform: translateY(-1px) scale(1.02);
  box-shadow: 0 12px 26px rgba(0,0,0,.08);
}}
.cuota:nth-child(5n+1) {{ background: #d4edda; }}
.cuota:nth-child(5n+2) {{ background: #d1ecf1; }}
.cuota:nth-child(5n+3) {{ background: #fff3cd; }}
.cuota:nth-child(5n+4) {{ background: #e2d6f3; }}
.cuota:nth-child(5n+5) {{ background: #f8d7da; }}

.cuota.is-pagada {{ border: 2px solid #16a34a; }}
.cuota.is-pendiente {{ border: 2px solid #fbbf24; }}
.cuota.is-atrasada {{ border: 2px solid #ef4444; }}

.cuota-k {{ font-weight: 900; margin-bottom: 6px; }}
.cuota-estado {{ margin-top: 6px; opacity: .9; font-weight: 900; }}
.cuota.is-pagada .cuota-estado {{ color: #16a34a; }}
.cuota.is-pendiente .cuota-estado {{ color: #a16207; }}
.cuota.is-atrasada .cuota-estado {{ color: #dc2626; }}
</style>

<div class="card loan-wrap">
  <h2 class="loan-title">📄 Préstamo #{loan_id}</h2>
  <p class="loan-sub">
    Cliente: <b>{client.get('first_name') or '—'}</b>
    <span class="loan-pill">Estado: {estado_pago}</span>
  </p>

  <div class="loan-actions">
    <a class="btn btn-secondary" href="{wa_link}" target="_blank" rel="noopener">📲 Recordar por WhatsApp</a>
    {registrar_pago_btn}
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
    ensure_org()
    oid = session.get("org_id")
    L = loan_dict_by_id(loan_id, oid)
    if not L:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))
    u = current_user()
    if not scope_owns_loan(u, L):
        flash("Sin acceso.", "danger")
        return redirect(url_for("loans"))
    # Monto recomendado para "cuota" (para precargar el formulario).
    term_count = int(L.get("term_count") or 1)

    # Regla: cuando se alcanzan las cuotas (ej. 10), el préstamo se cierra
    # y no se permite registrar más pagos.
    cuota_count = count_loan_cuota_payments(loan_id)
    if cuota_count >= term_count:
        flash("Préstamo cerrado. No se permite registrar más pagos.", "warning")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    # Total = capital + interés (descuento no lo reduce)
    capital_prest = float(L.get("amount") or 0)
    interes_loan = float(L.get("total_interest") or 0)
    total_to_pay = float(L.get("total_to_pay") or (capital_prest + interes_loan))
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

        # Separar capital e interés para que `remaining` (capital pendiente)
        # no llegue a 0 antes de completar todas las cuotas.
        total_interest = float(L.get("total_interest") or 0)
        interest_per_cuota = round(total_interest / max(term_count, 1), 2)
        if typ_l in ("cuota", "adelanto"):
            # En esta versión el interés por cuota es constante (modelo simple).
            adv_mult = int(weeks_adv or 1) if typ_l == "adelanto" else 1
            interest_part = round(min(amt, interest_per_cuota * adv_mult), 2)
            capital_part = round(amt - interest_part, 2)
        elif typ_l == "capital":
            capital_part = round(amt, 2)
            interest_part = 0.0
        elif typ_l == "interes":
            capital_part = 0.0
            interest_part = round(amt, 2)
        else:
            # Fallback: reparto 50/50
            capital_part = round(amt * 0.5, 2)
            interest_part = round(amt - capital_part, 2)

        if USE_DATABASE:
            L_state = dict(L)
        else:
            L_state = store.loans[loan_id]

        pay_payload = {
            "amount": amt,
            "type": typ,
            "date": today_rd(),
            "created_by": current_user()["id"],
            "capital": capital_part,
            "interest": interest_part,
            "status": "OK",
            "weeks_advanced": weeks_adv,
        }

        try:
            cl = client_dict_by_id(L.get("client_id"), oid) or {}
            client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{L.get('client_id')}"
            user_id = current_user()["id"]
            if typ_l == "adelanto":
                log_action(
                    user_id,
                    "registrar adelanto",
                    module="adelantos",
                    detail=f"{fmt_money(amt)} ({weeks_adv} semanas) — {client_nm}",
                )
            else:
                log_action(
                    user_id,
                    "registrar pago",
                    module="pagos",
                    detail=f"{fmt_money(amt)} ({typ_l}) — {client_nm}",
                )
        except Exception:
            pass

        rem = float(L_state.get("remaining") or 0) - capital_part
        L_state["remaining"] = max(0, rem)
        if "remaining_capital" in L_state:
            L_state["remaining_capital"] = max(0, float(L_state.get("remaining_capital") or 0) - capital_part)
        # Si se registra una cuota, avanzamos la próxima fecha automáticamente.
        if typ_l == "cuota":
            interval_days = freq_interval_days(L_state.get("frequency"))
            current_due = L_state.get("next_payment_date") or today_rd()
            if hasattr(current_due, "strftime"):
                L_state["next_payment_date"] = current_due + timedelta(days=interval_days)
        elif typ_l == "adelanto" and weeks_adv:
            interval_days = freq_interval_days(L_state.get("frequency"))
            current_due = L_state.get("next_payment_date") or today_rd()
            if hasattr(current_due, "strftime"):
                L_state["next_payment_date"] = current_due + timedelta(days=interval_days * weeks_adv)

        # Regla: al alcanzar el número de cuotas, cerrar el préstamo (incluye el pago que acabamos de registrar).
        if typ_l == "cuota":
            cuota_count_after = count_loan_cuota_payments(loan_id) + 1
            if cuota_count_after >= term_count:
                L_state["status"] = "cerrado"
                L_state["next_payment_date"] = None
            else:
                L_state["status"] = "ACTIVO"

        if USE_DATABASE:
            from credimapa_pg import insert_pago_and_sync_loan

            insert_pago_and_sync_loan(oid, loan_id, pay_payload, L_state)
        else:
            pid = store.nid("payments")
            pay_payload["id"] = pid
            pay_payload["loan_id"] = loan_id
            pay_payload["created_at"] = utc_now_for_db()
            store.payments[pid] = pay_payload

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
    if USE_DATABASE:
        from credimapa_pg import get_payment_row

        p = get_payment_row(payment_id)
    else:
        p = store.payments.get(payment_id)
    if not p:
        return "Pago no encontrado", 404
    loan_id = p.get("loan_id")
    L = loan_dict_by_id(loan_id, oid) if loan_id is not None else None
    if not L:
        return "Préstamo no encontrado", 404
    if user_is_cobrador_limited(user) and L.get("created_by") != user.get("id"):
        return "Sin acceso.", 403

    cid = L.get("client_id")
    client = client_dict_by_id(cid, oid) or {}
    cli_name = html.escape(
        f"{client.get('first_name', '')} {client.get('last_name') or ''}".strip() or f"Cliente #{cid}"
    )
    cli_phone = html.escape(str(client.get("phone") or "—"))

    cob_uid = L.get("created_by")
    if USE_DATABASE and cob_uid:
        from credimapa_pg import get_user as _db_gu

        cob = _db_gu(cob_uid) or {}
    else:
        cob = store.users.get(cob_uid, {}) if cob_uid else {}
    cob_name = html.escape(str(cob.get("name") or cob.get("username") or "—"))
    cob_phone = html.escape(str(cob.get("phone") or RECEIPT_COMPANY_TEL or "—"))

    capital_prest = float(L.get("amount") or 0)
    interes_total = float(L.get("total_interest") or 0)
    # Total = capital + interés (descuento no reduce lo que debe el cliente)
    total_prest = round(capital_prest + interes_total, 2)
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
    if USE_DATABASE:
        from credimapa_pg import get_payment_row

        p = get_payment_row(payment_id)
    else:
        p = store.payments.get(payment_id)
    if not p:
        flash("Pago no encontrado.", "danger")
        return redirect(url_for("loans"))
    loan_id = p.get("loan_id")
    L = loan_dict_by_id(loan_id, oid) if loan_id is not None else None
    if not L:
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
    # `remaining` representa capital pendiente. Al revertir, sumamos el capital
    # que se había descontado originalmente (no el monto total).
    capital_back = float(p.get("capital") or 0)
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

    if USE_DATABASE:
        L_adj = dict(L)
    else:
        L_adj = store.loans[loan_id]

    # Ajustar préstamo (saldo y fechas) para que no quede inconsistente.
    L_adj["remaining"] = float(L_adj.get("remaining") or 0) + capital_back
    if "remaining_capital" in L_adj:
        L_adj["remaining_capital"] = float(L_adj.get("remaining_capital") or 0) + capital_back
    term_count = int(L_adj.get("term_count") or 1)
    if USE_DATABASE:
        from credimapa_pg import get_payments

        pm = list(get_payments([oid]).values())
        cuota_count_after = len(
            [
                x
                for x in pm
                if x.get("loan_id") == loan_id
                and (x.get("type") or "").lower() == "cuota"
                and int(x.get("id") or -1) != int(payment_id)
            ]
        )
    else:
        cuota_count_after = len(
            [
                x
                for x in store.payments.values()
                if x.get("loan_id") == loan_id
                and (x.get("type") or "").lower() == "cuota"
                and int(x.get("id") or -1) != int(payment_id)
            ]
        )
    if cuota_count_after >= term_count:
        L_adj["status"] = "cerrado"
    else:
        L_adj["status"] = "ACTIVO"

    interval_days = freq_interval_days(L_adj.get("frequency"))
    current_due = L_adj.get("next_payment_date")
    if hasattr(current_due, "strftime"):
        if typ_l == "cuota":
            L_adj["next_payment_date"] = current_due - timedelta(days=interval_days)
        elif typ_l == "adelanto" and weeks > 0:
            L_adj["next_payment_date"] = current_due - timedelta(days=interval_days * weeks)

    if USE_DATABASE:
        from credimapa_pg import delete_pago_by_id, save_prestamo_from_loan_dict

        if not delete_pago_by_id(payment_id, oid):
            flash("No se pudo eliminar el pago en la base de datos.", "danger")
            return redirect(url_for("loan_detail", loan_id=loan_id))
        save_prestamo_from_loan_dict(L_adj)
    else:
        store.payments.pop(payment_id, None)
    try:
        cl = client_dict_by_id(L_adj.get("client_id"), oid) if L_adj else {}
        client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{L_adj.get('client_id') if L_adj else '—'}"
        loan_amt = float(L_adj.get("amount") or 0) if L_adj else 0
        log_action(
            user.get("id"),
            "eliminar recibo de pago",
            module="pagos",
            detail=f"Usuario eliminó recibo de {fmt_money(amt)} — Préstamo #{loan_id} • {client_nm}",
        )
    except Exception:
        pass
    flash("Recibo eliminado. Banco ajustado correctamente.", "success")
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
    adminish = can_admin_actions(user)
    tiles_base = (
        f'<a href="{url_for("bank_daily_list")}" class="bank-tile blue">🗓️ Lista diaria</a>'
        f'<a href="{url_for("bank_late")}" class="bank-tile orange">🔥 Atrasos</a>'
    )
    tiles_prestamista = (
        f'<a href="{url_for("bank_expenses")}" class="bank-tile red">📓 Gastos de ruta</a>'
        f'<a href="{url_for("bank_acta")}" class="bank-tile yellow">💸 Descuento inicial</a>'
        f'<a href="{url_for("bank_routes_list")}" class="bank-tile teal">🏦 Capital por ruta</a>'
        f'<a href="{url_for("bank_advance")}" class="bank-tile lavender">💵 Adelantos</a>'
        f'<a href="{url_for("bank_legal_list")}" class="bank-tile purple">📜 Documento legal</a>'
    ) if is_cajero_role(user) or is_cartera_admin(user) else ""

    tiles_admin = (
        f'<a href="{url_for("bank_delivery")}" class="bank-tile green2">💰 Entrega</a>'
        f'<a href="{url_for("bank_expenses")}" class="bank-tile red">📓 Gastos de ruta</a>'
        f'<a href="{url_for("bank_acta")}" class="bank-tile yellow">💸 Descuento inicial</a>'
        f'<a href="{url_for("bank_routes_list")}" class="bank-tile teal">🏦 Capital por ruta</a>'
        f'<a href="{url_for("bank_advance")}" class="bank-tile lavender">💵 Adelantos</a>'
        f'<a href="{url_for("bank_legal_list")}" class="bank-tile purple">📜 Documento legal</a>'
    ) if adminish else ""

    tiles = tiles_base + (tiles_admin if adminish else tiles_prestamista)
    bottom = (
        f'<a href="{url_for("collector_map")}" class="bank-tile bank-tile-full teal2">📍 Ver ubicación cobrador</a>'
    )
    # Solo super_admin puede ver/usar "Borrar todo el sistema".
    destroy = ""
    if user.get("role") == "super_admin":
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
@super_admin_required
def admin_clear_all():
    if request.method == "POST":
        store.reset_all()
        session.clear()
        flash("Datos reiniciados.", "success")
        return redirect(url_for("login"))
    return page(f'<div class="card"><h2>Reiniciar todo</h2><form method="post"><button class="btn btn-primary" type="submit">Confirmar</button></form></div>')


@app.route("/audit")
@login_required
def audit():
    ensure_org()
    oid = session.get("org_id")
    u = current_user()
    # Seguridad: solo admin (tenant) y super_admin ven auditoría completa.
    is_adminish = bool(u) and u.get("role") in ("admin", "super_admin")

    # Filtros (query params)
    q_uid = request.args.get("user_id", type=int)
    q_action = (request.args.get("accion") or "").strip().lower()
    q_from = request.args.get("desde") or ""
    q_to = request.args.get("hasta") or ""

    def parse_d(s):
        s = (s or "").strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None

    d_from = parse_d(q_from)
    d_to = parse_d(q_to)

    filtered = []
    if USE_DATABASE:
        try:
            from credimapa_pg import list_auditoria, list_tenant_usuarios, get_users, session_scope
            with session_scope() as sess:
                filtered = list_auditoria(
                    sess,
                    admin_id=oid,
                    user_id=q_uid,
                    action_like=q_action if q_action else None,
                    d_from=d_from,
                    d_to=d_to,
                    limit=600,
                )
            # Cobrador/cajero solo ve su propio historial
            if not is_adminish and u.get("id"):
                filtered = [a for a in filtered if a.get("user_id") == u.get("id")]
        except Exception:
            filtered = []
    else:
        for a in store.audit_log:
            au = store.users.get(a.get("user_id"), {})
            if u.get("role") != "super_admin" and au.get("organization_id") != oid:
                continue

            # Seguridad: prestamista solo ve su propio historial.
            if not is_adminish and a.get("user_id") != u.get("id"):
                continue

            if q_uid is not None and a.get("user_id") != q_uid:
                continue
            if q_action and q_action not in str(a.get("action") or "").lower() and q_action not in str(a.get("module") or "").lower():
                continue

            created_dt = a.get("created_at")
            created_date = created_dt.date() if hasattr(created_dt, "date") else None
            if d_from and (not created_date or created_date < d_from):
                continue
            if d_to and (not created_date or created_date > d_to):
                continue

            filtered.append(a)

    filtered.sort(key=lambda x: x.get("created_at") or datetime.min, reverse=True)
    filtered = filtered[:600]

    def severity(a):
        act = str(a.get("action") or "").lower()
        mod = str(a.get("module") or "").lower()
        red_words = (
            "eliminar",
            "borrar",
            "delete",
            "reverso",
            "editar",
            "reasignar",
            "suspender",
            "subir foto",
            "firmar",
            "entrega de dinero",
            "devolución",
            "banco insuficiente",
        )
        blue_words = ("ver ", "ver información", "información", "vista", "consulta", "ver préstamo", "ver cliente", "ver documento")
        if any(w in act for w in red_words) or "crítico" in act:
            return "red"
        if any(w in act for w in blue_words) or "ver " in act:
            return "blue"
        return "green"

    color_map = {
        "red": ("#fee2e2", "#991b1b"),
        "green": ("#dcfce7", "#166534"),
        "blue": ("#dbeafe", "#1d4ed8"),
    }

    style = """
<style>
  .audit-wrap{max-width:1080px;margin:0 auto;padding:12px 0 26px}
  .audit-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap}
  .audit-filters{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
  .audit-filters label{font-size:12px;font-weight:900;opacity:.9}
  .audit-input{padding:10px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:#fff;font-weight:800}
  body.theme-dark .audit-input{background:rgba(2,6,23,.55);color:#e5e7eb}
  .audit-btn{padding:10px 16px;border-radius:999px;border:none;background:#16a34a;color:#fff;font-weight:900;cursor:pointer}
  body.theme-dark .audit-btn{background:#22c55e}
  .audit-table{width:100%;border-collapse:separate;border-spacing:0}
  .audit-table th{text-align:left;padding:12px 10px;border-bottom:2px solid rgba(148,163,184,.25);background:rgba(2,132,199,.06);position:sticky;top:0}
  .audit-table td{padding:10px 10px;border-bottom:1px solid rgba(148,163,184,.16);vertical-align:top}
  .audit-row{transition: transform .12s ease, filter .12s ease}
  .audit-row:hover{transform: translateY(-1px);filter:brightness(1.01)}
</style>
"""

    # Tabla
    rows_html = ""
    for a in filtered:
        sev = severity(a)
        bg, fg = color_map[sev]
        created_dt = a.get("created_at")
        dt_txt = format_dt_rd(created_dt) if isinstance(created_dt, datetime) else str(created_dt or "")
        rows_html += (
            "<tr class='audit-row' style='background:{};'>"
            "<td style='font-variant-numeric:tabular-nums;white-space:nowrap;color:{}'>{}</td>"
            "<td>{}</td>"
            "<td>{}</td>"
            "<td>{}</td>"
            "<td style='max-width:520px;word-break:break-word'>{}</td>"
            "<td style='white-space:nowrap;opacity:.85'>{}</td>"
            "</tr>".format(
                bg,
                fg,
                html.escape(dt_txt),
                html.escape(str(a.get("user_name") or "—")),
                html.escape(str(a.get("role") or a.get("raw_role") or "—")),
                html.escape(str(a.get("module") or "—")),
                html.escape(str(a.get("action") or "—")),
                html.escape(str(a.get("detail") or "")),
                html.escape(str(a.get("ip") or "—")),
            )
        )

    if not rows_html:
        rows_html = "<tr><td colspan='7' style='opacity:.75;text-align:center;padding:18px'>Sin auditoría para estos filtros</td></tr>"

    user_filter_html = ""
    if is_adminish:
        # Opciones por usuario (en el tenant actual)
        if USE_DATABASE:
            try:
                from credimapa_pg import list_tenant_usuarios, get_users
                if u.get("role") == "super_admin":
                    users_src = [{"id": k, **(v or {})} for k, v in get_users().items()]
                else:
                    users_src = list_tenant_usuarios(oid)
            except Exception:
                users_src = []
        else:
            users_src = [
                {"id": uid, **uobj} for uid, uobj in store.users.items()
                if u.get("role") == "super_admin" or uobj.get("organization_id") == oid
            ]
        opts = "<option value=''>Todos</option>" + "".join(
            f"<option value='{uobj.get('id')}'{' selected' if q_uid == uobj.get('id') else ''}>{html.escape(uobj.get('username') or uobj.get('name') or str(uobj.get('id', '')))}</option>"
            for uobj in (users_src if isinstance(users_src, list) else list(users_src))
        )
        user_filter_html = f"<select name='user_id' class='audit-input' style='min-width:190px'>{opts}</select>"
    else:
        user_filter_html = f"<input type='hidden' name='user_id' value='{u.get('id')}'/>"

    body = (
        style
        + "<div class='audit-wrap'>"
        + "<div class='card' style='padding:16px;'>"
        + "<div class='audit-head'>"
        + "<div>"
        + "<h2 style='margin:0 0 6px 0'>🧾 Auditoría</h2>"
        + "<div style='opacity:.88;font-weight:800;font-size:13px'>Registra acciones críticas y consultas.</div>"
        + "</div>"
        + "<div class='audit-filters'>"
        + "<form method='get' style='display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end'>"
        + "<div><label>Usuario</label><br>" + user_filter_html + "</div>"
        + "<div><label>Acción / módulo</label><br><input class='audit-input' name='accion' placeholder='ej: crear préstamo' value='" + html.escape(q_action) + "'></div>"
        + "<div><label>Desde</label><br><input class='audit-input' type='date' name='desde' value='" + html.escape(q_from) + "'></div>"
        + "<div><label>Hasta</label><br><input class='audit-input' type='date' name='hasta' value='" + html.escape(q_to) + "'></div>"
        + "<div><button class='audit-btn' type='submit'>Filtrar</button></div>"
        + "</form>"
        + "</div>"
        + "</div>"
        + "<div style='margin-top:14px'>"
        + "<div class='table-scroll'>"
        + "<table class='audit-table'>"
        + "<thead><tr>"
        + "<th>Fecha</th><th>Usuario</th><th>Rol</th><th>Módulo</th><th>Acción</th><th>Detalle</th><th>IP</th>"
        + "</tr></thead>"
        + "<tbody>" + rows_html + "</tbody>"
        + "</table>"
        + "</div>"
        + "</div>"
        + "</div>"
        + "</div>"
    )
    return page(body)


def compute_super_admin_stats(date_from=None, date_to=None):
    """
    KPIs y datos para el panel SaaS:
    - Ganancias del sistema = pagos de suscripción de admins.
    - Aplicar filtros por rango de fechas a gráficas, estadísticas y ganancias.
    """
    admins = [u for u in store.users.values() if u.get("role") == "admin"]
    tenant_ids = {a.get("id") for a in admins if a.get("id") is not None}

    today = today_rd()
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
    now = utc_now_for_db()
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

    def _admin_display_name(uid):
        u = store.users.get(uid, {})
        return u.get("name") or u.get("username") or "—"

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
                "admin_username": _admin_display_name(p.get("admin_id")),
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
    now = utc_now_for_db()

    # Filtro por fecha (aplica a gráficas, estadísticas y ganancias).
    today_d = today_rd()
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

        # No exigir target_id global: register_admin_payment usa admin_id; fechas usan su propio lookup.

        if act == "register_admin_payment":
            # Registrar pago de suscripción y extender fecha_fin.
            admin_id = request.form.get("admin_id", type=int)
            amount_raw = (request.form.get("amount") or "").strip()
            try:
                amount = float(amount_raw.replace(",", ".")) if amount_raw else None
            except ValueError:
                amount = None
            payment_date_raw = (request.form.get("payment_date") or "").strip()
            method = (request.form.get("method") or "").strip()

            if not admin_id:
                flash("Admin inválido.", "danger")
                return redirect(url_for("super_admin_panel"))
            if amount is None or amount < 0 or not math.isfinite(amount):
                flash("Indique un monto válido (mayor que 0).", "danger")
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
            # datetime es subclase de date: hay que convertir a date antes de comparar con payment_date.
            base_date = admin_u.get("fecha_fin") or admin_u.get("subscription_end")
            base_d = None
            if base_date is not None:
                if isinstance(base_date, datetime):
                    base_d = base_date.date()
                elif isinstance(base_date, date):
                    base_d = base_date
                elif hasattr(base_date, "date"):
                    try:
                        base_d = base_date.date()
                    except Exception:
                        base_d = None

            if base_d is not None and base_d >= payment_date:
                extend_from = base_d
            else:
                extend_from = payment_date

            if extend_from < today_rd():
                extend_from = today_rd()

            new_fin = extend_from + timedelta(days=PAYMENT_EXTENSION_DAYS)
            if not admin_u.get("fecha_inicio"):
                admin_u["fecha_inicio"] = extend_from
            # Si fechas están invertidas por edición manual, reacomodar.
            fi = admin_u.get("fecha_inicio")
            if fi is not None:
                fi_d = fi.date() if isinstance(fi, datetime) else fi if isinstance(fi, date) else None
                if fi_d is not None and fi_d > extend_from:
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
                    "created_at": utc_now_for_db(),
                }
            )
            try:
                log_action(current_user()["id"], "admin_payment", str(pid))
            except Exception:
                pass

            amt_txt = fmt_money(round(float(amount), 2))
            flash(
                f"Pago #{pid} guardado por {amt_txt}. Suscripción extendida hasta {new_fin.strftime('%d/%m/%Y')}.",
                "success",
            )
            fd = (request.form.get("desde") or raw_desde or "").strip()
            fh = (request.form.get("hasta") or raw_hasta or "").strip()
            qp = []
            if fd:
                qp.append(f"desde={fd}")
            if fh:
                qp.append(f"hasta={fh}")
            q = ("?" + "&".join(qp)) if qp else ""
            return redirect(url_for("super_admin_panel") + q)

        if act == "edit_admin":
            aid = request.form.get("admin_id", type=int)
            admin_u = store.users.get(aid) if aid else None
            if not admin_u or admin_u.get("role") != "admin":
                flash("Admin no encontrado.", "danger")
                return redirect(url_for("super_admin_panel"))
            display_name = (request.form.get("display_name") or "").strip()
            new_username = (request.form.get("new_username") or "").strip()
            new_email = (request.form.get("email") or "").strip()
            new_password = (request.form.get("new_password") or "").strip()
            if not display_name:
                flash("El nombre no puede estar vacío.", "danger")
                return redirect(url_for("super_admin_panel"))
            # Nombre visible duplicado entre admins (misma etiqueta)
            dn_lower = display_name.lower()
            for u in store.users.values():
                if u.get("role") != "admin" or u.get("id") == aid:
                    continue
                other = (u.get("name") or u.get("username") or "").strip().lower()
                if other == dn_lower:
                    flash("Ya existe otro admin con ese nombre.", "danger")
                    return redirect(url_for("super_admin_panel"))
            admin_u["name"] = display_name
            if new_email:
                admin_u["email"] = new_email
            else:
                admin_u.pop("email", None)
            if new_username:
                if new_username != admin_u.get("username"):
                    if any(
                        x.get("username") == new_username and x.get("id") != aid
                        for x in store.users.values()
                    ):
                        flash("Ese usuario (login) ya está en uso.", "danger")
                        return redirect(url_for("super_admin_panel"))
                    admin_u["username"] = new_username
            if new_password:
                admin_u["password_hash"] = generate_password_hash(new_password)
            try:
                log_action(current_user()["id"], "edit_admin", f"id={aid}")
            except Exception:
                pass
            flash("Datos del admin actualizados.", "success")
            fd = (request.form.get("desde") or raw_desde or "").strip()
            fh = (request.form.get("hasta") or raw_hasta or "").strip()
            qp = []
            if fd:
                qp.append(f"desde={fd}")
            if fh:
                qp.append(f"hasta={fh}")
            q = ("?" + "&".join(qp)) if qp else ""
            return redirect(url_for("super_admin_panel") + q)

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
            if u.get("account_status") == ACCOUNT_PENDING and new_status == ACCOUNT_ACTIVE:
                flash("Primero apruebe el cobrador.", "warning")
                return redirect(url_for("super_admin_panel"))
            u["account_status"] = new_status
            flash("Estado del cobrador actualizado.", "success")
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
        if role in ("cobrador", "cajero"):
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
            if u.get("role") in ("cobrador", "cajero")
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
        dn_raw = a.get("name") or a.get("username") or ""
        un_raw = a.get("username") or ""
        em_raw = (a.get("email") or "").strip()
        admin_edit_form = (
            f"<details style='display:inline-block;margin-left:8px;vertical-align:top'>"
            f"<summary style='cursor:pointer;font-weight:900;opacity:.85'>Editar admin</summary>"
            f"<form method='post' action='{url_for('super_admin_panel')}' style='margin-top:8px;min-width:240px'>"
            f"<input type='hidden' name='action' value='edit_admin'>"
            f"<input type='hidden' name='admin_id' value='{aid}'>"
            f"<input type='hidden' name='desde' value='{df.isoformat()}'>"
            f"<input type='hidden' name='hasta' value='{dt.isoformat()}'>"
            f"<div style='font-size:11px;opacity:.8;font-weight:800;margin-bottom:6px'>Nombre visible</div>"
            f"<input name='display_name' type='text' value={json.dumps(dn_raw)} required maxlength='120' style='width:100%;padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);box-sizing:border-box'/>"
            f"<div style='font-size:11px;opacity:.8;font-weight:800;margin:10px 0 6px 0'>Usuario (login)</div>"
            f"<input name='new_username' type='text' value={json.dumps(un_raw)} placeholder='Opcional: nuevo usuario' maxlength='80' style='width:100%;padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);box-sizing:border-box'/>"
            f"<div style='font-size:11px;opacity:.8;font-weight:800;margin:10px 0 6px 0'>Correo (opcional)</div>"
            f"<input name='email' type='email' value={json.dumps(em_raw)} maxlength='120' style='width:100%;padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);box-sizing:border-box'/>"
            f"<div style='font-size:11px;opacity:.8;font-weight:800;margin:10px 0 6px 0'>Nueva contraseña (opcional)</div>"
            f"<input name='new_password' type='password' placeholder='Dejar vacío para no cambiar' autocomplete='new-password' style='width:100%;padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);box-sizing:border-box'/>"
            f"<button class='sa-btn sa-btn-primary' type='submit' style='margin-top:10px;width:100%'>Guardar cambios</button>"
            f"</form>"
            f"</details>"
        )
        actions = f"{actions}{admin_dates_editor}{admin_edit_form}"

        admin_rows += (
            "<tr>"
            f"<td>{esc(a.get('name') or a.get('username'))}</td>"
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
        tname = (
            store.users.get(tid, {}).get("name")
            or store.users.get(tid, {}).get("username")
            or f"Admin #{tid}"
        )
        users_rows += (
            "<tr>"
            f"<td>{esc(tname)}</td>"
            f"<td>{esc(u.get('username'))}</td>"
            f"<td>{role_badge(u.get('role'))}</td>"
            f"<td>{subscription_state_badge(u.get('account_status', ACCOUNT_ACTIVE), u.get('fecha_fin') or u.get('subscription_end'))}</td>"
            f"<td>{esc(u.get('phone') or '—')}</td>"
            f"<td>{esc(format_dt_rd(u.get('created_at')))}</td>"
            "</tr>"
        )

    # Sección de cobradores por admin (tenant)
    collectors_by_admin_html = ""
    for a in admins:
        aid = a.get("id")
        a_name = a.get("name") or a.get("username") or f"Admin #{aid}"
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
                f"<td>{esc(format_dt_rd(c.get('created_at')))}</td>"
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
        f"<option value='{a.get('id')}'>{esc(a.get('name') or a.get('username'))}</option>"
        for a in admins
    )

    def fmt_ph_date_cell(iso_s):
        if not iso_s:
            return "—"
        try:
            return datetime.strptime(str(iso_s)[:10], "%Y-%m-%d").date().strftime("%d/%m/%Y")
        except ValueError:
            return str(iso_s)

    payments_rows = "".join(
        "<tr>"
        f"<td>{esc(p.get('admin_username') or '—')}</td>"
        f"<td style='text-align:right'>{fmt_money(p.get('amount') or 0)}</td>"
        f"<td>{esc(fmt_ph_date_cell(p.get('payment_date')))}</td>"
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

function escText(s){
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function formatPayDateIso(iso){
  if(!iso) return '—';
  const p = String(iso).slice(0,10).split('-');
  if(p.length !== 3) return escText(iso);
  return escText(`${p[2]}/${p[1]}/${p[0]}`);
}

function renderPaymentHistoryBody(rows){
  const tb = document.getElementById('saPaymentHistoryBody');
  if(!tb) return;
  const list = Array.isArray(rows) ? rows : [];
  if(!list.length){
    tb.innerHTML = "<tr><td colspan='4' style='text-align:center;opacity:.85'>Sin pagos en el rango</td></tr>";
    return;
  }
  tb.innerHTML = list.map(p => {
    const who = escText(p.admin_username || '—');
    const amt = money(p.amount || 0);
    const when = formatPayDateIso(p.payment_date);
    const how = escText(p.method || '—');
    return `<tr><td>${who}</td><td style="text-align:right">${amt}</td><td>${when}</td><td>${how}</td></tr>`;
  }).join('');
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

  renderPaymentHistoryBody(data.payment_history);
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
        <input type="hidden" name="desde" value="{df.isoformat()}">
        <input type="hidden" name="hasta" value="{dt.isoformat()}">
        <label style="display:block;font-size:12px;opacity:.8;font-weight:800;margin-top:6px">Admin</label>
        <select name="admin_id" required style="width:100%;padding:10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);">
          {payment_admin_opts}
        </select>
        <label style="display:block;font-size:12px;opacity:.8;font-weight:800;margin-top:10px">Monto pagado</label>
        <input name="amount" type="number" step="0.01" min="0.01" required style="width:100%;padding:10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.9);">
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
          <thead><tr><th>Admin</th><th style="text-align:right">Monto</th><th>Fecha</th><th>Método</th></tr></thead>
          <tbody id="saPaymentHistoryBody">
          {payments_rows or "<tr><td colspan='4' style='text-align:center;opacity:.85'>Sin pagos en el rango</td></tr>"}
          </tbody>
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
    today = today_rd()

    def parse_date(raw):
        raw = (raw or "").strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None

    hoy_filter = request.args.get("hoy") == "1"
    cobrador_id = request.args.get("cobrador_id", type=int) or request.form.get("cobrador_id", type=int)

    if hoy_filter:
        dt_from = today
        dt_to = today
    else:
        dt_from = parse_date(request.args.get("desde"))
        dt_to = parse_date(request.args.get("hasta"))
        if dt_from is None and dt_to is None:
            dt_from = today
            dt_to = today

    # Helpers para bucket por día o mes.
    days_span = (dt_to - dt_from).days if (dt_from and dt_to) else 0
    bucket_by = "day" if days_span <= 31 else "month"

    def bucket_key_day(d):
        return d.isoformat()

    def bucket_key_month(d):
        return f"{d.year}-{d.month:02d}"

    def bkey(d):
        return bucket_key_day(d) if bucket_by == "day" else bucket_key_month(d)

    def build_series(map_amounts, labels):
        return [round(float(map_amounts.get(lb) or 0), 2) for lb in labels]

    # Today summary (Total cobrado, Interés, Capital)
    total_cobrado_hoy = 0.0
    total_interes_hoy = 0.0
    total_capital_hoy = 0.0
    if USE_DATABASE:
        try:
            from credimapa_pg import sums_pagos_report_range, session_scope

            with session_scope() as sess:
                total_cobrado_hoy, total_interes_hoy, total_capital_hoy = sums_pagos_report_range(
                    sess, oid, today, today, created_by=cobrador_id
                )
            total_cobrado_hoy = round(float(total_cobrado_hoy or 0), 2)
            total_interes_hoy = round(float(total_interes_hoy or 0), 2)
            total_capital_hoy = round(float(total_capital_hoy or 0), 2)
        except Exception as e:
            total_cobrado_hoy = 0.0
            total_interes_hoy = 0.0
            total_capital_hoy = 0.0
    else:
        for p in store.payments.values():
            pd = p.get("date") or p.get("pago_date") or p.get("fecha")
            if pd != today:
                continue
            L = store.loans.get(p.get("loan_id")) if p.get("loan_id") else None
            if not L or L.get("organization_id") != oid:
                continue
            if cobrador_id is not None and p.get("created_by") != cobrador_id:
                continue
            total_cobrado_hoy += float(p.get("amount") or p.get("monto") or 0)
            total_interes_hoy += float(p.get("interest") or p.get("interes") or 0)
            total_capital_hoy += float(p.get("capital") or 0)
        total_cobrado_hoy = round(total_cobrado_hoy, 2)
        total_interes_hoy = round(total_interes_hoy, 2)
        total_capital_hoy = round(total_capital_hoy, 2)

    # Alcance por tenant (org)
    if USE_DATABASE:
        try:
            from credimapa_pg import (
                get_loans,
                get_clients,
                list_pagos_cierre_semanal,
                list_banco_cierre_by_type,
                list_banco_cierre_gastos,
                list_tenant_usuarios,
                session_scope,
            )

            loans_dict = get_loans([oid]) if oid else {}
            clients_dict = get_clients([oid]) if oid else {}
            loans_tenant = list(loans_dict.values()) if loans_dict else []
            clients_tenant = list(clients_dict.values()) if clients_dict else []
            total_prestamos = len(loans_tenant)
            total_clientes = len(clients_tenant)

            with session_scope() as sess:
                pagos_list = list_pagos_cierre_semanal(
                    sess, oid, dt_from, dt_to,
                    restrict=(cobrador_id is not None), user_id=cobrador_id,
                )
                descuentos_list = list_banco_cierre_by_type(sess, oid, "descuento_inicial", dt_from, dt_to)
                prestamos_ent_list = list_banco_cierre_by_type(sess, oid, "prestamo_entregado", dt_from, dt_to)
                gastos_list = list_banco_cierre_gastos(sess, oid, dt_from, dt_to)
                cobradores_list = list_tenant_usuarios(oid) if oid else []

            route_expenses = [
                {**g, "created_at": g.get("created_at"), "amount": g.get("amount")}
                for g in gastos_list
            ]
            gastos_ruta_total = round(sum(float(g.get("amount") or 0) for g in gastos_list), 2)
            descuento_income_total = round(sum(float(d.get("amount") or 0) for d in descuentos_list), 2)

            ingresos_by_bucket = {}
            for d in descuentos_list:
                md = d.get("mov_date") or d.get("date")
                if md:
                    k = bkey(md) if isinstance(md, date) else bkey(datetime.strptime(str(md)[:10], "%Y-%m-%d").date())
                    ingresos_by_bucket[k] = ingresos_by_bucket.get(k, 0) + float(d.get("amount") or 0)

            gastos_by_bucket = {}
            for g in gastos_list:
                md = g.get("mov_date") or g.get("date")
                if md:
                    d_val = md if isinstance(md, date) else datetime.strptime(str(md)[:10], "%Y-%m-%d").date()
                    gastos_by_bucket[bkey(d_val)] = gastos_by_bucket.get(bkey(d_val), 0) + float(g.get("amount") or 0)

            prestamos_by_bucket = {}
            for p in prestamos_ent_list:
                md = p.get("mov_date") or p.get("date")
                if md:
                    d_val = md if isinstance(md, date) else datetime.strptime(str(md)[:10], "%Y-%m-%d").date()
                    prestamos_by_bucket[bkey(d_val)] = prestamos_by_bucket.get(bkey(d_val), 0) + float(p.get("amount") or 0)

            loan_ids_tenant = {L.get("id") for L in loans_tenant}
            cobros_by_bucket = {}
            for p in pagos_list:
                pd = p.get("pago_date") or p.get("date")
                if pd is None:
                    continue
                if p.get("loan_id") not in loan_ids_tenant:
                    continue
                amt = float(p.get("amount") or p.get("monto") or 0)
                d_val = pd if isinstance(pd, date) else datetime.strptime(str(pd)[:10], "%Y-%m-%d").date()
                cobros_by_bucket[bkey(d_val)] = cobros_by_bucket.get(bkey(d_val), 0) + amt
        except Exception:
            loans_tenant = []
            clients_tenant = []
            total_prestamos = 0
            total_clientes = 0
            cobradores_list = []
            pagos_list = []
            gastos_ruta_total = 0.0
            descuento_income_total = 0.0
            ingresos_by_bucket = {}
            gastos_by_bucket = {}
            prestamos_by_bucket = {}
            cobros_by_bucket = {}
    else:
        loans_tenant = [L for L in store.loans.values() if L.get("organization_id") == oid]
        clients_tenant = [c for c in store.clients.values() if c.get("organization_id") == oid]
        total_prestamos = len(loans_tenant)
        total_clientes = len(clients_tenant)
        cobradores_list = [
            {"id": u.get("id"), "username": u.get("username") or u.get("name")}
            for u in store.users.values()
            if u.get("organization_id") == oid and u.get("role") in ("cobrador", "admin", "supervisor")
        ]

        route_expenses = [
            e
            for e in store.route_expenses.values()
            if e.get("organization_id") == oid
            and e.get("created_at") is not None
            and (dt_from <= e.get("created_at").date() <= dt_to)
        ]
        gastos_ruta_total = round(sum(float(e.get("amount") or 0) for e in route_expenses), 2)

        descuento_income_total = round(
            sum(
                float(cr.get("amount") or 0)
                for cr in store.cash_reports.values()
                if cr.get("organization_id") == oid
                and cr.get("movement_type") == "descuento_inicial"
                and cr.get("date") is not None
                and (dt_from <= cr.get("date") <= dt_to)
            ),
            2,
        )

        ingresos_by_bucket = {}
        for cr in store.cash_reports.values():
            if (
                cr.get("organization_id") == oid
                and cr.get("movement_type") == "descuento_inicial"
                and cr.get("date") is not None
                and dt_from <= cr.get("date") <= dt_to
            ):
                ingresos_by_bucket[bkey(cr.get("date"))] = ingresos_by_bucket.get(bkey(cr.get("date")), 0) + float(
                    cr.get("amount") or 0
                )

        gastos_by_bucket = {}
        for e in route_expenses:
            d = e.get("created_at").date()
            gastos_by_bucket[bkey(d)] = gastos_by_bucket.get(bkey(d), 0) + float(e.get("amount") or 0)

        prestamos_by_bucket = {}
        for cr in store.cash_reports.values():
            if (
                cr.get("organization_id") == oid
                and cr.get("movement_type") == "prestamo_entregado"
                and cr.get("date") is not None
                and dt_from <= cr.get("date") <= dt_to
            ):
                prestamos_by_bucket[bkey(cr.get("date"))] = prestamos_by_bucket.get(bkey(cr.get("date")), 0) + float(
                    cr.get("amount") or 0
                )

        cobros_by_bucket = {}
        def loan_org_ok(loan_id):
            L = store.loans.get(loan_id) if loan_id is not None else None
            return bool(L and L.get("organization_id") == oid)

        for p in store.payments.values():
            pd = p.get("date")
            if pd is None:
                continue
            if not (dt_from <= pd <= dt_to):
                continue
            if not loan_org_ok(p.get("loan_id")):
                continue
            if cobrador_id is not None and p.get("created_by") != cobrador_id:
                continue
            cobros_by_bucket[bkey(pd)] = cobros_by_bucket.get(bkey(pd), 0) + float(p.get("amount") or 0)

    # Pagos en rango para reporte por cobrador
    if USE_DATABASE:
        pagos_in_range = pagos_list
    else:
        loan_ids_tenant = {L.get("id") for L in loans_tenant}
        pagos_in_range = []
        for p in store.payments.values():
            pd = p.get("date") or p.get("pago_date") or p.get("fecha")
            if pd is None:
                continue
            d_val = pd if isinstance(pd, date) else datetime.strptime(str(pd)[:10], "%Y-%m-%d").date()
            if not (dt_from <= d_val <= dt_to):
                continue
            if p.get("loan_id") not in loan_ids_tenant:
                continue
            if cobrador_id is not None and p.get("created_by") != cobrador_id:
                continue
            pagos_in_range.append({
                "created_by": p.get("created_by"),
                "amount": float(p.get("amount") or p.get("monto") or 0),
                "interest": float(p.get("interest") or p.get("interes") or 0),
                "capital": float(p.get("capital") or 0),
            })

    # Reporte por cobrador y ranking
    cobrador_sums = {}
    cobrador_names = {u.get("id"): (u.get("username") or u.get("nombre") or u.get("name") or "Usuario") for u in cobradores_list}
    for p in pagos_in_range:
        amt = float(p.get("amount") or p.get("monto") or 0)
        inte = float(p.get("interest") or p.get("interes") or 0)
        cap = float(p.get("capital") or 0)
        uid = p.get("created_by")
        key = uid if uid is not None else 0
        if key not in cobrador_sums:
            cobrador_sums[key] = {"nombre": cobrador_names.get(uid) or "Sin asignar", "total_cobrado": 0.0, "total_interes": 0.0, "total_capital": 0.0}
        cobrador_sums[key]["total_cobrado"] += amt
        cobrador_sums[key]["total_interes"] += inte
        cobrador_sums[key]["total_capital"] += cap
    report_by_collector = [
        {"nombre": v["nombre"], "total_cobrado": round(v["total_cobrado"], 2), "total_interes": round(v["total_interes"], 2), "total_capital": round(v["total_capital"], 2)}
        for v in cobrador_sums.values()
    ]
    ranking = sorted(report_by_collector, key=lambda x: x["total_cobrado"], reverse=True)

    banco_disp = get_bank_available(oid)

    # Serie labels (día o mes) dentro del rango
    labels = []
    if bucket_by == "day":
        d = dt_from
        while d <= dt_to:
            labels.append(bkey(d))
            d += timedelta(days=1)
    else:
        y, m = dt_from.year, dt_from.month
        cur = date(y, m, 1)
        while cur <= dt_to:
            labels.append(bkey(cur))
            # siguiente mes
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)

    if not USE_DATABASE:
        ingresos_by_bucket = {}
        for cr in store.cash_reports.values():
            if (
                cr.get("organization_id") == oid
                and cr.get("movement_type") == "descuento_inicial"
                and cr.get("date") is not None
                and dt_from <= cr.get("date") <= dt_to
            ):
                ingresos_by_bucket[bkey(cr.get("date"))] = ingresos_by_bucket.get(bkey(cr.get("date")), 0) + float(
                    cr.get("amount") or 0
                )

        gastos_by_bucket = {}
        for e in route_expenses:
            d = e.get("created_at")
            if d:
                d = d.date() if hasattr(d, "date") else d
                gastos_by_bucket[bkey(d)] = gastos_by_bucket.get(bkey(d), 0) + float(e.get("amount") or 0)

        prestamos_by_bucket = {}
        for cr in store.cash_reports.values():
            if (
                cr.get("organization_id") == oid
                and cr.get("movement_type") == "prestamo_entregado"
                and cr.get("date") is not None
                and dt_from <= cr.get("date") <= dt_to
            ):
                prestamos_by_bucket[bkey(cr.get("date"))] = prestamos_by_bucket.get(bkey(cr.get("date")), 0) + float(
                    cr.get("amount") or 0
                )

        cobros_by_bucket = {}
        def _loan_org_ok(loan_id):
            L = store.loans.get(loan_id) if loan_id is not None else None
            return bool(L and L.get("organization_id") == oid)

        for p in store.payments.values():
            pd = p.get("date")
            if pd is None:
                continue
            if not (dt_from <= pd <= dt_to):
                continue
            if not _loan_org_ok(p.get("loan_id")):
                continue
            if cobrador_id is not None and p.get("created_by") != cobrador_id:
                continue
            cobros_by_bucket[bkey(pd)] = cobros_by_bucket.get(bkey(pd), 0) + float(p.get("amount") or 0)

    ingresos_series = build_series(ingresos_by_bucket, labels)
    gastos_series = build_series(gastos_by_bucket, labels)
    prestamos_series = build_series(prestamos_by_bucket, labels)
    cobros_series = build_series(cobros_by_bucket, labels)

    charts_js = """
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
function money(v){
  const n = Number(v || 0);
  return 'RD$ ' + n.toLocaleString('es-DO', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
const labels = LABELS;
const ingresos = INGRESOS;
const gastos = GASTOS;
const prestamos = PRESTAMOS;
const cobros = COBROS;

function build(){
  const incomeCtx = document.getElementById('chartIngresosGastos');
  const loansCtx = document.getElementById('chartPrestamosCobros');
  const dailyCtx = document.getElementById('chartIngresoDiario');
  if(incomeCtx){
    new Chart(incomeCtx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          label: 'Ingresos (descuento inicial)',
          data: ingresos,
          borderColor: 'rgba(37,99,235,1)',
          backgroundColor: 'rgba(37,99,235,.15)',
          tension: 0.3,
          fill: true,
          pointRadius: 2
        },{
          label: 'Gastos de ruta',
          data: gastos,
          borderColor: 'rgba(239,68,68,1)',
          backgroundColor: 'rgba(239,68,68,.12)',
          tension: 0.3,
          fill: true,
          pointRadius: 2
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: true } },
        scales: { y: { beginAtZero: true, ticks: { callback: (v)=>money(v) } } }
      }
    });
  }
  if(loansCtx){
    new Chart(loansCtx, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          label: 'Préstamos (entregados)',
          data: prestamos,
          backgroundColor: 'rgba(99,102,241,.35)',
          borderColor: 'rgba(99,102,241,1)',
          borderWidth: 1
        },{
          label: 'Cobros (pagos)',
          data: cobros,
          backgroundColor: 'rgba(16,185,129,.25)',
          borderColor: 'rgba(16,185,129,1)',
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: true } },
        scales: { y: { beginAtZero: true, ticks: { callback: (v)=>money(v) } } }
      }
    });
  }
  if(dailyCtx){
    new Chart(dailyCtx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          label: 'Dinero cobrado',
          data: cobros,
          borderColor: 'rgba(16,185,129,1)',
          backgroundColor: 'rgba(16,185,129,.2)',
          tension: 0.3,
          fill: true,
          pointRadius: 3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: true } },
        scales: { y: { beginAtZero: true, ticks: { callback: (v)=>money(v) } } }
      }
    });
  }
}
window.addEventListener('load', build);
</script>
"""
    charts_js = (
        charts_js.replace("LABELS", json.dumps(labels))
        .replace("INGRESOS", json.dumps(ingresos_series))
        .replace("GASTOS", json.dumps(gastos_series))
        .replace("PRESTAMOS", json.dumps(prestamos_series))
        .replace("COBROS", json.dumps(cobros_series))
    )

    # Mini-modo dark visual para las tarjetas del dashboard.
    dashboard_css = """
<style>
  .rep-wrap{max-width:1080px;margin:0 auto;padding:12px 0 26px}
  .rep-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap}
  .rep-title{font-size:20px;font-weight:1000;margin:0}
  .rep-sub{opacity:.88;font-weight:800;font-size:13px;margin-top:6px}
  .rep-actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center}

  .rep-grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px;margin-top:12px}
  .rep-card{grid-column: span 12; background:rgba(255,255,255,.92);border:1px solid rgba(148,163,184,.25);border-radius:18px;box-shadow:0 10px 24px rgba(0,0,0,.06);padding:14px;transition: transform .14s ease, box-shadow .14s ease}
  .rep-card:hover{transform: translateY(-2px);box-shadow:0 14px 34px rgba(0,0,0,.09)}
  body.theme-dark .rep-card{background:rgba(15,23,42,.92);border-color:rgba(148,163,184,.18);box-shadow:0 14px 34px rgba(0,0,0,.32)}

  @media(min-width:720px){.rep-card{grid-column: span 6}}
  @media(min-width:980px){.rep-card{grid-column: span 4}}

  .rep-card-top{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
  .rep-ico{width:40px;height:40px;border-radius:14px;display:flex;align-items:center;justify-content:center;background:rgba(148,163,184,.18);border:1px solid rgba(148,163,184,.25);color:#0f172a}
  body.theme-dark .rep-ico{color:#e5e7eb}
  .rep-k{opacity:.8;font-weight:900;font-size:12px;margin-top:4px}
  .rep-v{font-size:28px;font-weight:1000;margin-top:6px;letter-spacing:-.02em}

  .tone-green{background:rgba(16,185,129,.12)!important;border-color:rgba(16,185,129,.25)!important}
  .tone-red{background:rgba(239,68,68,.10)!important;border-color:rgba(239,68,68,.22)!important}
  .tone-blue{background:rgba(37,99,235,.10)!important;border-color:rgba(37,99,235,.22)!important}
  .tone-purple{background:rgba(124,58,237,.10)!important;border-color:rgba(124,58,237,.22)!important}

  .rep-charts{display:grid;grid-template-columns:1fr;gap:12px;margin-top:14px}
  @media(min-width:920px){.rep-charts{grid-template-columns:1fr 1fr}}
  .rep-chart-card{background:rgba(255,255,255,.92);border:1px solid rgba(148,163,184,.25);border-radius:18px;box-shadow:0 10px 24px rgba(0,0,0,.06);padding:14px}
  body.theme-dark .rep-chart-card{background:rgba(15,23,42,.92);border-color:rgba(148,163,184,.18);box-shadow:0 14px 34px rgba(0,0,0,.32)}
  .rep-chart-title{font-weight:1000;margin:0 0 10px 0;font-size:14px}
  .rep-canvas-wrap{height:260px}

  .rep-filter{display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap;justify-content:flex-end}
  .rep-filter label{font-weight:900;font-size:12px;opacity:.9}
  .rep-input{padding:10px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);background:rgba(255,255,255,.8);color:#0f172a;font-weight:800}
  body.theme-dark .rep-input{background:rgba(2,6,23,.55);color:#e5e7eb}

  .rep-badge-soft{display:inline-flex;align-items:center;gap:8px;border-radius:999px;padding:6px 10px;border:1px solid rgba(148,163,184,.25);background:rgba(255,255,255,.55);font-weight:1000;opacity:.95}
  body.theme-dark .rep-badge-soft{background:rgba(2,6,23,.35)}

  [data-rep-anim]{opacity:0;transform: translateY(10px);transition: opacity .35s ease, transform .35s ease}

  .rep-tables{display:grid;grid-template-columns:1fr;gap:12px;margin-top:14px}
  @media(min-width:720px){.rep-tables{grid-template-columns:1fr 1fr}}
  .rep-table-card{background:rgba(255,255,255,.92);border:1px solid rgba(148,163,184,.25);border-radius:18px;box-shadow:0 10px 24px rgba(0,0,0,.06);padding:14px}
  body.theme-dark .rep-table-card{background:rgba(15,23,42,.92);border-color:rgba(148,163,184,.18)}
  .rep-table-title{font-weight:1000;margin:0 0 10px 0;font-size:14px}
  .rep-table{table:border-collapse:collapse;width:100%;font-size:13px}
  .rep-table th,.rep-table td{padding:8px 10px;text-align:left}
  .rep-table th{font-weight:900;opacity:.9;border-bottom:1px solid rgba(148,163,184,.3)}
  .rep-table td{border-bottom:1px solid rgba(148,163,184,.15)}
  .rep-table td.num{text-align:right;font-weight:800}
  .rep-rank-1,.rep-rank-2,.rep-rank-3{font-weight:1000}
  .rep-rank-1{color:#f59e0b}
  .rep-rank-2{color:#94a3b8}
  .rep-rank-3{color:#b45309}
</style>
"""
    _hoy_kw = {"hoy": 1, "cobrador_id": cobrador_id} if cobrador_id else {"hoy": 1}
    rep_hoy_url = url_for("reportes", **_hoy_kw)
    week_start = today - timedelta(days=today.weekday())
    _range_kw = {"desde": week_start.isoformat(), "hasta": today.isoformat(), "cobrador_id": cobrador_id} if cobrador_id else {"desde": week_start.isoformat(), "hasta": today.isoformat()}
    rep_semana_url = url_for("reportes", **_range_kw)
    month_start = date(today.year, today.month, 1)
    _mes_kw = {"desde": month_start.isoformat(), "hasta": today.isoformat(), "cobrador_id": cobrador_id} if cobrador_id else {"desde": month_start.isoformat(), "hasta": today.isoformat()}
    rep_mes_url = url_for("reportes", **_mes_kw)

    cobrador_options = "".join(
        "<option value='%s'%s>%s</option>"
        % (u.get("id"), " selected" if cobrador_id == u.get("id") else "", html.escape(u.get("username") or "Usuario"))
        for u in cobradores_list
    )
    cobrador_select = (
        "<div><label>Cobrador</label>"
        + "<select class='rep-input' name='cobrador_id'>"
        + f"<option value=''>Todos</option>{cobrador_options}"
        + "</select></div>"
    ) if cobradores_list else ""

    html = (
        dashboard_css
        + "<div class='rep-wrap'>"
        + "<div class='rep-head'>"
        + "<div>"
        + "<h2 class='rep-title'>📊 Reportes</h2>"
        + "<div class='rep-sub'>Dashboard financiero con filtros y gráficos</div>"
        + "</div>"
        + "<div class='rep-actions'>"
        + "<div class='rep-filter no-print'>"
        + "<form method='get' style='display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end'>"
        + cobrador_select
        + "<div>"
        + "<label>Desde</label>"
        + f"<input class='rep-input' type='date' name='desde' value='{(dt_from.isoformat() if dt_from else '')}'>"
        + "</div>"
        + "<div>"
        + "<label>Hasta</label>"
        + f"<input class='rep-input' type='date' name='hasta' value='{(dt_to.isoformat() if dt_to else '')}'>"
        + "</div>"
        + "<button class='btn btn-primary btn-action' type='submit'><span class='btn-ic' aria-hidden='true'>"
        + "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='18' height='18' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><path d='M21 10c0 8-6 13-9 13s-9-5-9-13a9 9 0 0 1 18 0z'/><circle cx='12' cy='10' r='2'/></svg>"
        + "</span>Filtrar</button>"
        + "</form>"
        + "</div>"
        + "<div class='rep-quick-btns' style='display:flex;gap:6px;flex-wrap:wrap'>"
        + f"<a class='btn btn-primary btn-action' href='{rep_hoy_url}'>Reporte de hoy</a>"
        + f"<a class='rep-badge-soft' href='{rep_hoy_url}'>Hoy</a>"
        + f"<a class='rep-badge-soft' href='{rep_semana_url}'>Esta semana</a>"
        + f"<a class='rep-badge-soft' href='{rep_mes_url}'>Este mes</a>"
        + "</div>"
        + f"<a class='btn btn-secondary btn-action' href='{url_for('reportes_cobradores')}'>Por cobrador</a>"
        + f"<a class='btn btn-secondary btn-action' href='{url_for('dashboard')}'>Dashboard</a>"
        + "</div>"
        + "</div>"
        + "<div class='rep-grid'>"
        + "<div class='rep-card tone-green' data-rep-anim='1'>"
        + "<div class='rep-card-top'><div><div class='rep-k'>Total cobrado hoy</div><div class='rep-v'>" + fmt_money(total_cobrado_hoy or 0) + "</div></div><div class='rep-ico'>💰</div></div></div>"
        + "<div class='rep-card tone-blue' data-rep-anim='1'>"
        + "<div class='rep-card-top'><div><div class='rep-k'>Interés ganado hoy</div><div class='rep-v'>" + fmt_money(total_interes_hoy or 0) + "</div></div><div class='rep-ico'>📈</div></div></div>"
        + "<div class='rep-card tone-purple' data-rep-anim='1'>"
        + "<div class='rep-card-top'><div><div class='rep-k'>Capital cobrado hoy</div><div class='rep-v'>" + fmt_money(total_capital_hoy or 0) + "</div></div><div class='rep-ico'>📋</div></div></div>"
        + "<div class='rep-card tone-green' data-rep-anim='1'>"
        + "<div class='rep-card-top'>"
        + "<div>"
        + "<div class='rep-k'>Banco disponible</div>"
        + f"<div class='rep-v'>{fmt_money(banco_disp)}</div>"
        + "</div>"
        + "<div class='rep-ico'>💚</div>"
        + "</div>"
        + "</div>"
        + "<div class='rep-card tone-red' data-rep-anim='1'>"
        + "<div class='rep-card-top'>"
        + "<div>"
        + "<div class='rep-k'>Gastos de ruta</div>"
        + f"<div class='rep-v'>{fmt_money(gastos_ruta_total)}</div>"
        + "</div>"
        + "<div class='rep-ico'>🚗</div>"
        + "</div>"
        + "</div>"
        + "<div class='rep-card tone-blue' data-rep-anim='1'>"
        + "<div class='rep-card-top'>"
        + "<div>"
        + "<div class='rep-k'>Ingreso por descuentos</div>"
        + f"<div class='rep-v'>{fmt_money(descuento_income_total)}</div>"
        + "</div>"
        + "<div class='rep-ico'>💳</div>"
        + "</div>"
        + "</div>"
        + "<div class='rep-card tone-purple' data-rep-anim='1'>"
        + "<div class='rep-card-top'>"
        + "<div>"
        + "<div class='rep-k'>Total préstamos</div>"
        + f"<div class='rep-v'>{total_prestamos}</div>"
        + "</div>"
        + "<div class='rep-ico'>📄</div>"
        + "</div>"
        + "</div>"
        + "<div class='rep-card' data-rep-anim='1' style='grid-column:span 12;background:rgba(255,255,255,.92)'>"
        + "<div class='rep-card-top'>"
        + "<div>"
        + "<div class='rep-k'>Total clientes</div>"
        + f"<div class='rep-v'>{total_clientes}</div>"
        + "</div>"
        + "<div class='rep-ico'>👥</div>"
        + "</div>"
        + "</div>"
        + "</div>"
        + "<div class='rep-tables' data-rep-anim='1'>"
        + "<div class='rep-table-card'>"
        + "<div class='rep-table-title'>Reporte por cobrador</div>"
        + "<table class='rep-table'><tr><th>Nombre</th><th class='num'>Total cobrado</th><th class='num'>Interés</th><th class='num'>Capital</th></tr>"
        + "".join(
            f"<tr><td>{html.escape(r['nombre'])}</td><td class='num'>{fmt_money(r['total_cobrado'])}</td>"
            f"<td class='num'>{fmt_money(r['total_interes'])}</td><td class='num'>{fmt_money(r['total_capital'])}</td></tr>"
            for r in report_by_collector
        )
        + (f"<tr><td colspan='4' style='opacity:.7'>Sin cobros en el rango</td></tr>" if not report_by_collector else "")
        + "</table></div>"
        + "<div class='rep-table-card'>"
        + "<div class='rep-table-title'>Ranking de cobradores</div>"
        + "<table class='rep-table'><tr><th>#</th><th>Nombre</th><th class='num'>Total cobrado</th></tr>"
        + "".join(
            f"<tr><td><span class='rep-rank-{i}'>{i}</span></td><td>{html.escape(r['nombre'])}</td><td class='num'>{fmt_money(r['total_cobrado'])}</td></tr>"
            for i, r in enumerate(ranking[:10], 1)
        )
        + (f"<tr><td colspan='3' style='opacity:.7'>Sin cobros en el rango</td></tr>" if not ranking else "")
        + "</table></div>"
        + "</div>"
        + "<div class='rep-charts' style='margin-top:14px'>"
        + "<div class='rep-chart-card' data-rep-anim='1'>"
        + "<div class='rep-chart-title'>Ingresos vs Gastos (rango)</div>"
        + "<div class='rep-canvas-wrap'><canvas id='chartIngresosGastos'></canvas></div>"
        + "</div>"
        + "<div class='rep-chart-card' data-rep-anim='1'>"
        + "<div class='rep-chart-title'>Préstamos vs Cobros (rango)</div>"
        + "<div class='rep-canvas-wrap'><canvas id='chartPrestamosCobros'></canvas></div>"
        + "</div>"
        + "<div class='rep-chart-card' data-rep-anim='1' style='grid-column:1/-1'>"
        + "<div class='rep-chart-title'>Ingreso diario (cobros)</div>"
        + "<div class='rep-canvas-wrap'><canvas id='chartIngresoDiario'></canvas></div>"
        + "</div>"
        + "</div>"
        + charts_js
        + "<script>"
        + "document.addEventListener('DOMContentLoaded',()=>{"
        + "document.querySelectorAll('[data-rep-anim]').forEach(el=>el.classList.add('in'));"
        + "});"
        + "</script>"
        + nav_subfooter()
        + "</div>"
    )
    return page(html)


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
        if count_loan_cuota_payments(L.get("id")) >= int(L.get("term_count") or 1)
    ]
    tr = ""
    for L in rows:
        loan_id = L.get("id")
        client_id = L.get("client_id")
        c = store.clients.get(client_id, {}) if client_id is not None else {}
        client_name = (
            f"{c.get('first_name') or ''} {c.get('last_name') or ''}".strip()
            or f"#{client_id}" if client_id is not None else "—"
        )
        sd = calc_client_score(client_id, org_id) if client_id is not None else {"score": 0, "nivel": "—"}
        nivel = sd.get("nivel") or "—"
        score = sd.get("score") or 0
        is_bueno = int(score) >= 60
        calif_html = (
            f"<span style='color:#16a34a;font-weight:900'>{html.escape(nivel)}</span>"
            if is_bueno
            else f"<span style='color:#d97706;font-weight:900'>{html.escape(nivel)}</span>"
        )
        tr += (
            f"<tr>"
            f"<td>#{loan_id}</td>"
            f"<td>{fmt_money(L.get('amount'))}</td>"
            f"<td>{html.escape(str(client_name))}</td>"
            f"<td>{calif_html} <span style='opacity:.85;font-weight:800'>(Score {score})</span></td>"
            f"<td><a class='btn btn-secondary' href='{url_for('loan_detail', loan_id=loan_id)}'>Ver</a></td>"
            f"</tr>"
        )
    return page(
        f'<div class="card"><h2>Préstamos pagados / cerrados</h2><div class="table-scroll">'
        f'<table><tr><th>ID</th><th>Monto original</th><th>Cliente</th><th>Calificación</th><th></th></tr>{tr or "<tr><td colspan=5>Ninguno</td></tr>"}</table></div>{nav_subfooter()}</div>'
    )


@app.route("/gps/update", methods=["POST"])
@login_required
def gps_update():
    uid = session.get("user_id")
    store.gps_positions[uid] = {
        "lat": request.form.get("lat"), "lng": request.form.get("lng"), "ts": utc_now_for_db(),
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
        return format_dt_rd(ts) if isinstance(ts, datetime) else str(ts)

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
        markers = []
        for c in collectors:
            p = store.gps_positions.get(c.get("id"))
            lat = p.get("lat") if p else None
            lng = p.get("lng") if p else None
            try:
                lat_f = float(lat) if lat is not None else None
                lng_f = float(lng) if lng is not None else None
            except Exception:
                lat_f, lng_f = None, None

            if lat_f is not None and lng_f is not None:
                markers.append(
                    {
                        "name": c.get("name") or c.get("username") or f"#{c.get('id')}",
                        "lat": lat_f,
                        "lng": lng_f,
                        "ts": (p.get("ts").isoformat() if p and p.get("ts") else None),
                    }
                )
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
        extra = (
            f"<p>{('Última posición registrada: ' + str(me_pos.get('ts')) ) if me_pos else 'Sin posición (activa GPS en el navegador).'} </p>"
            f"<p style='margin-top:6px;opacity:.9;font-size:14px'>Abre el mapa para registrar tu ubicación automáticamente.</p>"
        )
        markers = []
        if me_pos:
            try:
                lat_f = float(me_pos.get("lat"))
                lng_f = float(me_pos.get("lng"))
            except Exception:
                lat_f = lng_f = None
            if lat_f is not None and lng_f is not None:
                markers.append(
                    {
                        "name": user.get("name") or user.get("username") or "Yo",
                        "lat": lat_f,
                        "lng": lng_f,
                        "ts": (me_pos.get("ts").isoformat() if me_pos.get("ts") else None),
                    }
                )

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

    markers_json = json.dumps(markers or [])
    map_ui = (
        "<link rel='stylesheet' href='https://unpkg.com/leaflet@1.9.4/dist/leaflet.css' />"
        "<div id='collectorMap' style='height:360px;border-radius:16px;overflow:hidden;border:1px solid rgba(148,163,184,.35);box-shadow:0 10px 24px rgba(0,0,0,.06);'></div>"
    )
    map_js = """
    <script>
      const markers = MARKERS_JSON;
      function fmtTsISO(ts){
        if(!ts) return '—';
        try{
          const d = new Date(ts);
          return d.toLocaleString('es-DO');
        }catch(e){ return ts; }
      }
      function initMap(){
        if(typeof L === 'undefined') return;
        const defaultCenter = [18.4861, -69.9312]; // RD (aprox.)
        const has = Array.isArray(markers) && markers.length > 0;
        const center = has ? [markers[0].lat, markers[0].lng] : defaultCenter;
        const map = L.map('collectorMap').setView(center, has ? 12 : 6);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
          maxZoom: 18,
          attribution: '&copy; OpenStreetMap'
        }).addTo(map);
        if(!has) return;
        markers.forEach(m => {
          if(m && typeof m.lat === 'number' && typeof m.lng === 'number'){
            const popup = `<b>${m.name || '—'}</b><br/>` +
              `Lat: ${m.lat}<br/>Lng: ${m.lng}<br/>` +
              `Actualizado: ${fmtTsISO(m.ts)}`;
            L.marker([m.lat, m.lng]).addTo(map).bindPopup(popup);
          }
        });
      }
      window.addEventListener('load', initMap);
    </script>
    <script src='https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'></script>
    """
    map_js = map_js.replace("MARKERS_JSON", markers_json)

    body = (
        "<div class='card'>"
        "<h2>Mapa cobrador</h2>"
        + "<div style='display:flex;gap:14px;flex-wrap:wrap;align-items:flex-start'>"
        + "<div style='flex:1 1 520px;min-width:320px'>" + map_ui + map_js + "</div>"
        + "<div style='flex:0 1 380px;min-width:300px'>" + extra + "</div>"
        + "</div>"
        + js
        + "</div>"
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


@app.route("/bank/legal/<int:prestamo_id>")
@login_required
def bank_legal_document(prestamo_id):
    """Alias para /bank/legal/view/<id> — compatible con enlaces que usan /bank/legal/<id>."""
    return redirect(url_for("view_legal_document", loan_id=prestamo_id))


@app.route("/bank/legal/list")
@login_required
def bank_legal_list():
    ensure_org()
    oid = session.get("org_id")
    user = current_user()
    try:
        log_action(
            user.get("id"),
            "ver documentos legales",
            module="documento legal",
            detail="lista",
        )
    except Exception:
        pass
    rows = []

    def _client_name(cid):
        if cid is None:
            return "—"
        c = client_dict_by_id(cid, oid)
        if not c:
            return f"Cliente #{cid}"
        return f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or f"Cliente #{cid}"

    def _cobrador_name(uid):
        if uid is None:
            return "—"
        if USE_DATABASE:
            from credimapa_pg import get_user
            u = get_user(uid)
            return (u.get("name") or u.get("username") or "—") if u else "—"
        return (store.users.get(uid, {}).get("name") or store.users.get(uid, {}).get("username") or "—")

    for L in sorted(loans_for_user(oid, user), key=lambda x: -x.get("id", 0))[:200]:
        nm = _client_name(L.get("client_id"))
        cobrador = _cobrador_name(L.get("created_by"))
        firmado = bool(L.get("signature_b64"))
        status_label = "Firmado" if firmado else "Pendiente"
        status_color = "#16a34a" if firmado else "#d97706"
        rows.append(
            "<div class='legal-card' style='background:#ffffffb8;border-radius:18px;padding:12px;box-shadow:0 8px 22px rgba(0,0,0,.06);'>"
            f"<div style='display:flex;justify-content:space-between;gap:10px;align-items:flex-start;'>"
            f"<div style='font-weight:900;line-height:1.2'>{html.escape(nm)}</div>"
            f"<div style='font-weight:900;color:{status_color}'>{status_label}</div>"
            f"</div>"
            f"<div style='opacity:.85;margin-top:6px;font-size:12px;'>Cobrador: {cobrador}</div>"
            f"<div style='opacity:.85;margin-top:3px;font-size:12px;'>Prestamo: #{L.get('id')}</div>"
            f"<div style='margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;'>"
            f"<a class='btn btn-secondary' style='padding:7px 12px' href='{url_for('bank_legal_document', prestamo_id=L.get('id'))}'>Ver</a>"
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
    loan = loan_dict_by_id(loan_id, oid)
    if not loan:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("bank_legal_list"))
    u = current_user()
    if not scope_owns_loan(u, loan):
        flash("Sin acceso.", "danger")
        return redirect(url_for("bank_legal_list"))
    try:
        client = client_dict_by_id(loan.get("client_id"), oid) or {}
        client_nm = f"{client.get('first_name') or ''} {client.get('last_name') or ''}".strip() or "Cliente"
        log_action(
            u.get("id"),
            "ver documento legal",
            module="documento legal",
            detail=f"Préstamo #{loan_id} • {client_nm}",
        )
    except Exception:
        pass

    client = client_dict_by_id(loan.get("client_id"), oid) or {}
    uid_cob = loan.get("created_by")
    if uid_cob and USE_DATABASE:
        from credimapa_pg import get_user
        cob_u = get_user(uid_cob) or {}
    else:
        cob_u = store.users.get(uid_cob, {}) if uid_cob else {}
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
  function legalScrollFirma() {
    const c = document.getElementById('sigCanvas');
    if (c) c.scrollIntoView({behavior:'smooth', block:'center'});
  }
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
      El cliente <b>{cliente_nombre_esc}</b> reconoce haber recibido el capital del préstamo y se compromete de manera expresa, voluntaria e irrevocable a pagar la totalidad de la deuda a <b>CREDIMAPA</b>, incluyendo capital, intereses, cargos y penalidades aplicables, en los plazos establecidos.
    </p>
    <p>
      El incumplimiento de este compromiso autoriza a <b>CREDIMAPA</b> a iniciar las acciones legales correspondientes conforme a la ley vigente.
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
    loan = loan_dict_by_id(loan_id, oid)
    if not loan:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("bank_legal_list"))
    u = current_user()
    if not scope_owns_loan(u, loan):
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

    b64_val = f"data:image/png;base64,{base64.b64encode(raw).decode('ascii')}"
    if USE_DATABASE:
        from credimapa_pg import update_prestamo_legal_docs
        if not update_prestamo_legal_docs(loan_id, oid, id_photo_b64=b64_val):
            flash("No se pudo guardar la foto.", "danger")
            return redirect(url_for("view_legal_document", loan_id=loan_id))
    else:
        loan["id_photo_b64"] = b64_val
    try:
        cl = client_dict_by_id(loan.get("client_id"), oid) or {}
        client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{loan.get('client_id')}"
        log_action(u.get("id"), "subir foto ID (frente)", module="documento legal", detail=f"Préstamo #{loan_id} • {client_nm}")
    except Exception:
        pass
    flash("ID (frente) guardada.", "success")
    return redirect(url_for("view_legal_document", loan_id=loan_id))


@app.route("/bank/legal/upload-id-back/<int:loan_id>", methods=["POST"])
@login_required
def upload_id_back(loan_id):
    ensure_org()
    oid = session.get("org_id")
    loan = loan_dict_by_id(loan_id, oid)
    if not loan:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("bank_legal_list"))
    u = current_user()
    if not scope_owns_loan(u, loan):
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

    b64_val = f"data:image/png;base64,{base64.b64encode(raw).decode('ascii')}"
    if USE_DATABASE:
        from credimapa_pg import update_prestamo_legal_docs
        if not update_prestamo_legal_docs(loan_id, oid, id_photo_back_b64=b64_val):
            flash("No se pudo guardar la foto.", "danger")
            return redirect(url_for("view_legal_document", loan_id=loan_id))
    else:
        loan["id_photo_back_b64"] = b64_val
    try:
        cl = client_dict_by_id(loan.get("client_id"), oid) or {}
        client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{loan.get('client_id')}"
        log_action(u.get("id"), "subir foto ID (atrás)", module="documento legal", detail=f"Préstamo #{loan_id} • {client_nm}")
    except Exception:
        pass
    flash("ID (atrás) guardada.", "success")
    return redirect(url_for("view_legal_document", loan_id=loan_id))


@app.route("/bank/legal/sign/<int:loan_id>", methods=["GET", "POST"])
@login_required
def sign_legal_document(loan_id):
    # Nota: mostramos el formulario completo en `view_legal_document`.
    # Este endpoint solo guarda la firma al hacer POST.
    if request.method == "POST":
        ensure_org()
        oid = session.get("org_id")
        loan = loan_dict_by_id(loan_id, oid)
        if not loan:
            flash("Préstamo no encontrado.", "danger")
            return redirect(url_for("bank_legal_list"))

        u = current_user()
        if not scope_owns_loan(u, loan):
            flash("Sin acceso.", "danger")
            return redirect(url_for("bank_legal_list"))

        sig = request.form.get("signature_b64")
        if not sig or not str(sig).startswith("data:"):
            flash("Firma inválida.", "danger")
            return redirect(url_for("view_legal_document", loan_id=loan_id))

        if USE_DATABASE:
            from credimapa_pg import update_prestamo_legal_docs
            if not update_prestamo_legal_docs(loan_id, oid, signature_b64=sig):
                flash("No se pudo guardar la firma.", "danger")
                return redirect(url_for("view_legal_document", loan_id=loan_id))
        else:
            loan["signature_b64"] = sig
        try:
            cl = client_dict_by_id(loan.get("client_id"), oid) or {}
            client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{loan.get('client_id')}"
            log_action(u.get("id"), "firmar documento legal", module="documento legal", detail=f"Préstamo #{loan_id} • {client_nm}")
        except Exception:
            pass
        flash("Firma guardada.", "success")
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
        if isinstance(d, date) and not isinstance(d, datetime):
            return format_dt_rd(combine_date_at_rd_midnight(d))
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
    u = current_user()
    if not (can_admin_actions(u) or is_cajero_role(u) or user_is_cobrador_limited(u)):
        flash("Acción restringida.", "danger")
        return redirect(url_for("bank_home"))
    try:
        log_action(u.get("id"), "ver gastos de ruta", module="gastos de ruta", detail="")
    except Exception:
        pass
    restrict = user_is_cobrador_limited(u)
    can_manage = can_admin_actions(u)

    kind_options = "".join(
        f'<option value="{k}">{ico} {lbl}</option>' for k, (lbl, ico) in ROUTE_EXPENSE_KINDS.items()
    )

    expense_list = sorted(
        (
            e
            for e in store.route_expenses.values()
            if e.get("organization_id") == oid and (not restrict or e.get("user_id") == u.get("id"))
        ),
        key=lambda x: x.get("created_at") or datetime.min,
        reverse=True,
    )

    tbody_rows = []
    for idx, e in enumerate(expense_list):
        kind = e.get("kind") or "otros"
        lbl, ico = route_expense_kind_info(kind)
        row_bg = "#ecfdf5" if idx % 2 == 0 else "#ffffff"
        edit_btn = (
            f'<a class="route-act route-act-edit" href="{url_for("edit_expense", expense_id=e["id"])}" title="Editar">✏</a>'
            if can_manage
            else ""
        )
        del_form = (
            f'<form method="post" action="{url_for("delete_route_expense", expense_id=e["id"])}" style="display:inline" '
            f'onsubmit="return confirm(\'¿Eliminar este gasto?\');">'
            f'<button type="submit" class="route-act route-act-del" title="Eliminar">🗑</button></form>'
            if can_manage
            else ""
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
    ensure_org()
    oid = session.get("org_id")
    if exp.get("organization_id") != oid:
        flash("Sin acceso.", "danger")
        return redirect(url_for("bank_expenses"))
    u = current_user()
    if not can_admin_actions(u):
        flash("Acción restringida: solo admin puede eliminar gastos de ruta.", "danger")
        return redirect(url_for("bank_expenses"))
    cash_id = exp.get("cash_report_id")
    if cash_id is not None:
        store.cash_reports.pop(cash_id, None)
    store.route_expenses.pop(expense_id, None)
    try:
        exp_nm = f"{exp.get('route') or '—'} • {exp.get('kind') or '—'}"
        log_action(
            u.get("id"),
            "eliminar gasto de ruta",
            module="gastos de ruta",
            detail=f"{exp_nm} — {fmt_money(exp.get('amount'))}",
        )
    except Exception:
        pass
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
    u = current_user()
    if not can_admin_actions(u):
        flash("Acción restringida: solo admin puede editar gastos de ruta.", "danger")
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
        try:
            route_nm = f"{route or '—'}"
            kind_lbl = ROUTE_EXPENSE_KINDS.get(kind, (kind, ""))[0] if "ROUTE_EXPENSE_KINDS" in globals() else (kind, "")
            log_action(
                current_user()["id"],
                "editar gasto de ruta",
                module="gastos de ruta",
                detail=f"{route_nm} • {kind_lbl} — {fmt_money(exp.get('amount'))}",
            )
        except Exception:
            pass
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
    u = current_user()
    if not (can_admin_actions(u) or is_cajero_role(u)):
        flash("Acción restringida.", "danger")
        return redirect(url_for("bank_home"))
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
        "created_at": utc_now_for_db(),
        "organization_id": org_id,
        "cash_report_id": cash_id,
    }
    try:
        u = current_user()
        log_action(
            u.get("id"),
            "agregar gasto de ruta",
            module="gastos de ruta",
            detail=f"{route or '—'} • {lbl} — {fmt_money(exp_amount)}",
        )
    except Exception:
        pass
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
    u = current_user()
    if not can_admin_actions(u):
        flash("Acción restringida: solo admin puede eliminar descuentos.", "danger")
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
    # Total a pagar = capital aprobado + interés (el descuento no lo reduce)
    target["total_to_pay"] = round(amount_total + total_interest, 2)
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

    try:
        user_id = u.get("id")
        loan_id = target.get("id") or "—"
        cl = store.clients.get(client_id, {})
        client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{client_id}"
        log_action(
            user_id,
            "eliminar descuento inicial",
            module="descuentos",
            detail=f"Préstamo #{loan_id} • {fmt_money(old_discount_amount)} • {client_nm}",
        )
    except Exception:
        pass
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
    u = current_user()
    if not can_admin_actions(u):
        flash("Acción restringida: solo admin puede editar descuentos.", "danger")
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
    # Capital que debe = amount_total. Solo corregimos remaining cuando delta > 0 (quitando descuento).
    delta = new_net_initial - old_net_initial
    target["upfront_percent"] = new_upfront_percent
    if delta > 0:
        target["remaining"] = max(0.0, float(target.get("remaining") or 0) + delta)
        if "remaining_capital" in target:
            target["remaining_capital"] = max(0.0, float(target.get("remaining_capital") or 0) + delta)

    total_interest = round(float(target.get("total_interest") or 0), 2)
    term_count = int(target.get("term_count") or 1)
    # Total a pagar = capital aprobado + interés (el descuento no lo reduce)
    target["total_to_pay"] = round(amount_total + total_interest, 2)
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

    try:
        user_id = u.get("id") if "u" in locals() else current_user().get("id")
        loan_id = target.get("id") or "—"
        cl = store.clients.get(client_id, {})
        client_nm = f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip() or f"Cliente #{client_id}"
        log_action(
            user_id,
            "editar descuento inicial",
            module="descuentos",
            detail=f"Préstamo #{loan_id} • {fmt_money(new_discount_amount)} • {client_nm}",
        )
    except Exception:
        pass
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
    puede_registrar_devolucion = user.get("role") in ("admin", "supervisor", "cobrador", "cajero") or is_cartera_admin(user)

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
            if not cob or cob.get("organization_id") != oid or cob.get("role") not in ("cobrador", "cajero"):
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
            try:
                log_action(
                    user.get("id"),
                    "entrega de dinero",
                    module="banco",
                    detail=f"{fmt_money(amt)} para {cob.get('username')}",
                )
            except Exception:
                pass
            flash(f"Entrega registrada: {fmt_money(amt)} para {cob.get('username')}.", "success")
            return redirect(url_for("bank_delivery"))

        if form_type == "devolucion":
            if not puede_registrar_devolucion:
                flash("No tiene permiso para registrar devoluciones.", "danger")
                return redirect(url_for("bank_delivery"))

            amt = request.form.get("amount", type=float)
            note = (request.form.get("note") or "").strip()

            # Un cobrador/cajero solo puede registrar devolución a su nombre (si no es admin/outsider).
            if is_cajero_role(user) and not is_cartera_admin(user):
                collector_id = user["id"]
            else:
                collector_id = request.form.get("collector_id", type=int)

            cob = store.users.get(collector_id) if collector_id else None
            if not cob or cob.get("organization_id") != oid or cob.get("role") not in ("cobrador", "cajero"):
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

            try:
                log_action(
                    user.get("id"),
                    "devolución de capital",
                    module="banco",
                    detail=f"{fmt_money(amt)} para {cob.get('username')}",
                )
            except Exception:
                pass
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
    if is_cajero_role(user) and not is_cartera_admin(user):
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
    if is_cajero_role(user) and not is_cartera_admin(user):
        devoluciones = [d for d in devoluciones if d.get("collector_id") == user["id"]]

    cobradores = sorted(
        [u for u in store.users.values() if u.get("organization_id") == oid and is_cajero_role(u)],
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
        fecha_s = format_dt_rd(fecha) if isinstance(fecha, datetime) else str(fecha or "—")
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
        fecha_s = format_dt_rd(fecha) if isinstance(fecha, datetime) else str(fecha or "—")
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
    u = current_user()
    if not (can_admin_actions(u) or is_cajero_role(u)):
        flash("Acción restringida.", "danger")
        return redirect(url_for("bank_home"))
    try:
        log_action(u.get("id"), "ver acta", module="banco", detail="")
    except Exception:
        pass
    restrict = user_is_cobrador_limited(u)
    can_manage = can_admin_actions(u)

    # Para cajero/cobrador: los "descuentos iniciales" deben verse por préstamos en alcance
    # (loan.created_by == user.id), no solo por cash_report.user_id.
    loan_ids = loan_ids_visible(oid, u) if restrict else None
    discount_cash_ids = None
    if restrict:
        discount_cash_ids = {
            L.get("discount_cash_report_id")
            for L in loans_for_user(oid, u)
            if L.get("discount_cash_report_id") is not None
        }
    # ============================================================
    # Acta global — fuente de verdad: tabla `banco` (admin_id = tenant).
    # - Caja global: SUM(amount) de movimientos (sin saldo inicial; ver "Disponible")
    # - Descuento total: SUM(amount) donde movement_type = descuento_inicial
    # - Gastos realizados: SUM(ABS(amount)) donde movement_type en gasto / gasto_ruta
    # - Disponible: starting_bank + SUM(amount) en banco (= get_bank_available)
    # ============================================================
    GASTO_MOVEMENT_TYPES = ("gasto", "gasto_ruta")

    if USE_DATABASE:
        from credimapa_pg import (
            get_session,
            get_starting_bank,
            get_banco_sum,
            get_clientes_dict,
            get_prestamos_dict,
            get_user,
            list_banco_descuentos_iniciales,
            sum_banco_abs_amount,
            sum_banco_amount,
        )

        sess = get_session()
        if restrict:
            caja_global = round(
                float(get_starting_bank(sess, oid))
                + sum_banco_amount(sess, oid, movement_type="deposito_banco", user_id=u.get("id")),
                2,
            )
            descuento_total = round(
                sum_banco_amount(
                    sess,
                    oid,
                    movement_type="descuento_inicial",
                    banco_ids=discount_cash_ids,
                ),
                2,
            )
            gastos_realizados = round(
                sum_banco_abs_amount(sess, oid, GASTO_MOVEMENT_TYPES, user_id=u.get("id")),
                2,
            )
        else:
            caja_global = round(get_banco_sum(sess, oid), 2)
            descuento_total = round(
                sum_banco_amount(sess, oid, movement_type="descuento_inicial"),
                2,
            )
            gastos_realizados = round(sum_banco_abs_amount(sess, oid, GASTO_MOVEMENT_TYPES), 2)
        disponible = get_bank_available(oid)
        discount_moves = list_banco_descuentos_iniciales(
            sess,
            oid,
            banco_ids=discount_cash_ids if restrict else None,
            limit=400,
        )
        loans_d = get_prestamos_dict(sess, [oid])
        clients_d = get_clientes_dict(sess, [oid])
    else:
        caja_global = float(getattr(store, "starting_banks", {}).get(oid, 0.0) or 0.0) + sum(
            float(cr.get("amount") or 0)
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == oid
            and cr.get("movement_type") == "deposito_banco"
            and (not restrict or cr.get("user_id") == u.get("id"))
        )
        descuento_total = round(
            sum(
                float(cr.get("amount") or 0)
                for cr in store.cash_reports.values()
                if cr.get("organization_id") == oid
                and cr.get("movement_type") == "descuento_inicial"
                and (not restrict or cr.get("id") in discount_cash_ids)
            ),
            2,
        )
        gastos_realizados = round(
            sum(
                abs(float(cr.get("amount") or 0))
                for cr in store.cash_reports.values()
                if cr.get("organization_id") == oid
                and cr.get("movement_type") in GASTO_MOVEMENT_TYPES
                and (not restrict or cr.get("user_id") == u.get("id"))
            ),
            2,
        )
        disponible = get_bank_available(oid)
        discount_moves = sorted(
            (
                cr
                for cr in store.cash_reports.values()
                if cr.get("organization_id") == oid
                and cr.get("movement_type") == "descuento_inicial"
                and (not restrict or cr.get("id") in discount_cash_ids)
            ),
            key=lambda x: x.get("created_at") or datetime.min,
            reverse=True,
        )
        loans_d = store.loans
        clients_d = store.clients

    def fmt_dt(dt):
        if not dt:
            return ""
        return format_dt_rd(dt) if isinstance(dt, datetime) else str(dt)

    rows = ""
    for cr in discount_moves[:200]:
        discount_id = cr.get("id")
        loan = None
        for L in loans_d.values():
            if L.get("organization_id") == oid and L.get("discount_cash_report_id") == discount_id:
                loan = L
                break
        if restrict and loan and loan.get("created_by") != u.get("id"):
            continue
        client = clients_d.get((loan or {}).get("client_id"), {}) if loan else {}
        user_id = cr.get("user_id")
        if USE_DATABASE:
            cobrador = (get_user(user_id) or {}).get("username") or "—"
        else:
            cobrador = store.users.get(user_id, {}).get("username") or "—"
        ruta = client.get("route") or "—"
        monto = float(cr.get("amount") or 0)
        del_form = (
            f"<form method='post' action='{url_for('delete_discount', discount_id=discount_id)}' "
            f"onsubmit=\"return confirm('¿Eliminar este descuento y ajustar el banco?');\" style='margin:0'>"
            f"<button class='btn btn-secondary' type='submit' title='Eliminar'>🗑</button></form>"
            if can_manage
            else "<span style='opacity:.55'>—</span>"
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
    # Filtros (query params)
    ruta_q = (request.args.get("ruta") or "").strip()
    prestamista_q = request.args.get("prestamista", type=int)
    try:
        log_action(
            user.get("id"),
            "ver capital por ruta",
            module="banco",
            detail=f"ruta={ruta_q or '—'} • prestamista={prestamista_q or '—'}",
        )
    except Exception:
        pass

    by_route = {}
    prestamista_ids = set()

    for L in loans_for_user(oid, user):
        if str(L.get("status", "")).upper() != "ACTIVO":
            continue
        c = store.clients.get(L.get("client_id"), {})
        ruta = (c.get("route") or "").strip() or "Sin ruta"
        cb = L.get("created_by")
        if cb:
            prestamista_ids.add(cb)

        if ruta_q and ruta_q != ruta:
            continue
        if prestamista_q is not None and prestamista_q != cb:
            continue

        if ruta not in by_route:
            by_route[ruta] = {"remaining": 0.0, "n": 0, "cids": set()}

        by_route[ruta]["remaining"] += float(L.get("remaining") or 0)
        by_route[ruta]["n"] += 1
        if cb:
            by_route[ruta]["cids"].add(cb)

    total_rest = round(sum(float(x["remaining"]) for x in by_route.values()), 2)
    total_n = sum(int(x["n"]) for x in by_route.values())

    rutas_sorted = sorted(by_route.keys(), key=lambda x: x.lower())

    # Clasificación por tier (alto/medio/bajo) según capital relativo.
    max_cap = max((float(by_route[r]["remaining"]) for r in rutas_sorted), default=0.0)
    def tier(rem):
        if max_cap <= 0:
            return "low"
        ratio = rem / max_cap
        if ratio >= 0.66:
            return "high"
        if ratio >= 0.33:
            return "mid"
        return "low"

    # Opciones filtro
    route_options_html = "".join(
        f"<option value='{html.escape(r)}'{(' selected' if ruta_q == r else '') if ruta_q else (' selected' if False else '')}>{html.escape(r)}</option>"
        for r in rutas_sorted
    )

    prest_options = []
    for uid in sorted(prestamista_ids):
        u = store.users.get(uid, {})
        nm = html.escape(u.get("name") or u.get("username") or str(uid))
        prest_options.append((uid, nm))
    prest_options_html = "".join(
        f"<option value='{uid}'{(' selected' if prestamista_q == uid else '')}>{nm}</option>"
        for uid, nm in prest_options
    )

    style_block = """
<style>
  .routes-wrap{max-width:1080px;margin:0 auto;padding:12px 0 26px}
  .routes-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;margin-bottom:14px}
  .routes-title{font-size:20px;font-weight:1000;margin:0}
  .routes-sub{opacity:.88;font-weight:800;font-size:13px;margin-top:6px}

  .routes-actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
  .btn-ghost{
    display:inline-flex;align-items:center;gap:8px;padding:10px 12px;border-radius:14px;
    border:1px solid rgba(148,163,184,.35);
    background:rgba(255,255,255,.7);
    text-decoration:none;color:#0f172a;font-weight:1000;
    transition: transform .12s ease, box-shadow .12s ease, filter .12s ease;
  }
  body.theme-dark .btn-ghost{background:rgba(2,6,23,.55);border-color:rgba(148,163,184,.22);color:#e5e7eb}
  .btn-ghost:hover{transform: translateY(-1px);box-shadow:0 10px 24px rgba(0,0,0,.08);filter:brightness(1.02)}
  .btn-ghost:active{transform: translateY(0) scale(.99)}

  .routes-grid-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:12px 0 16px}
  .m-card{
    border-radius:18px;border:1px solid rgba(148,163,184,.25);
    box-shadow:0 10px 24px rgba(0,0,0,.06);
    padding:14px;transition: transform .14s ease, box-shadow .14s ease;
    background: rgba(255,255,255,.92);
  }
  body.theme-dark .m-card{background:rgba(15,23,42,.92);border-color:rgba(148,163,184,.18);box-shadow:0 14px 34px rgba(0,0,0,.32)}
  .m-card:hover{transform: translateY(-2px);box-shadow:0 14px 34px rgba(0,0,0,.10)}
  .m-top{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
  .m-k{opacity:.85;font-weight:900;font-size:12px}
  .m-v{font-weight:1000;font-size:28px;margin-top:8px}
  .m-ic{width:40px;height:40px;border-radius:14px;display:flex;align-items:center;justify-content:center}
  .tone-green{background:linear-gradient(135deg, rgba(16,185,129,.20), rgba(16,185,129,.08))}
  .tone-red{background:linear-gradient(135deg, rgba(239,68,68,.20), rgba(239,68,68,.08))}
  .tone-blue{background:linear-gradient(135deg, rgba(37,99,235,.20), rgba(37,99,235,.08))}
  .tone-purple{background:linear-gradient(135deg, rgba(124,58,237,.20), rgba(124,58,237,.08))}

  .routes-filter{
    display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;justify-content:flex-end;
    margin: 6px 0 14px;
  }
  .routes-filter label{font-size:12px;font-weight:1000;opacity:.9}
  .routes-input{
    padding:10px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);
    background:rgba(255,255,255,.85);color:#0f172a;font-weight:800;
  }
  body.theme-dark .routes-input{background:rgba(2,6,23,.55);color:#e5e7eb}
  .routes-form-btn{
    display:inline-flex;align-items:center;gap:8px;padding:10px 12px;border-radius:14px;
    border:none;font-weight:1000;cursor:pointer;background:rgba(16,185,129,.14);color:#047857;
  }
  body.theme-dark .routes-form-btn{background:rgba(16,185,129,.12);color:#34d399}
  .routes-form-btn:hover{filter:brightness(1.04)}

  .routes-cards-grid{
    display:grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 12px;
    margin-top: 14px;
  }

  .route-card{
    border-radius:18px;
    border:1px solid rgba(148,163,184,.25);
    background: rgba(255,255,255,.92);
    box-shadow:0 10px 24px rgba(0,0,0,.06);
    padding:14px;
    transition: transform .14s ease, box-shadow .14s ease, filter .14s ease;
    position:relative;
    overflow:hidden;
  }
  body.theme-dark .route-card{background:rgba(15,23,42,.92);border-color:rgba(148,163,184,.18);box-shadow:0 14px 34px rgba(0,0,0,.32)}
  .route-card:hover{transform: translateY(-2px);box-shadow:0 14px 34px rgba(0,0,0,.10);filter:brightness(1.02)}
  .rc-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;margin-bottom:10px}
  .rc-name{font-weight:1000;font-size:15px}
  .rc-n{opacity:.88;font-weight:900;font-size:12px;margin-top:4px}
  .rc-amt{font-weight:1000;font-size:22px;margin-top:6px}
  .rc-amt.pos{color:#16a34a}
  .rc-amt.mid{color:#2563eb}
  .rc-amt.low{color:#a16207}
  .rc-by{opacity:.9;font-weight:900;font-size:12px;margin-top:10px}

  .chart-card{
    margin-top: 14px;
    border-radius:18px;
    border:1px solid rgba(148,163,184,.25);
    background:rgba(255,255,255,.92);
    box-shadow:0 10px 24px rgba(0,0,0,.06);
    padding:14px;
  }
  body.theme-dark .chart-card{background:rgba(15,23,42,.92);border-color:rgba(148,163,184,.18);box-shadow:0 14px 34px rgba(0,0,0,.32)}

  .chart-title{font-weight:1000;margin:0 0 10px 0;font-size:14px}

  [data-r-anim]{opacity:0;transform: translateY(10px);transition: opacity .35s ease, transform .35s ease}
  [data-r-anim].in{opacity:1;transform: translateY(0)}

</style>
"""

    # Tarjetas por ruta
    cards_html = ""
    bar_labels = []
    bar_values = []
    bar_colors = []

    for r in rutas_sorted:
        info = by_route[r]
        rem = float(info["remaining"])
        cb_names = []
        for uid in info["cids"]:
            u = store.users.get(uid, {})
            cb_names.append(u.get("name") or u.get("username") or str(uid))
        pres = html.escape(", ".join(sorted(cb_names)) if cb_names else "—")
        tier_v = tier(rem)
        if tier_v == "high":
            tone_bg = "rgba(16,185,129,.12)"
            tone_border = "rgba(16,185,129,.35)"
            amt_class = "pos"
            icon_color = "#047857"
            bar_color = "rgba(16,185,129,.55)"
        elif tier_v == "mid":
            tone_bg = "rgba(37,99,235,.10)"
            tone_border = "rgba(37,99,235,.32)"
            amt_class = "mid"
            icon_color = "#1d4ed8"
            bar_color = "rgba(37,99,235,.50)"
        else:
            tone_bg = "rgba(245,158,11,.14)"
            tone_border = "rgba(245,158,11,.35)"
            amt_class = "low"
            icon_color = "#a16207"
            bar_color = "rgba(245,158,11,.50)"

        cards_html += (
            "<div class='route-card' data-r-anim='1' "
            f"style='background:{tone_bg}; border-color:{tone_border}'>"
            "<div class='rc-head'>"
            f"<div>"
            f"<div class='rc-name'>🗺️ {html.escape(r)}</div>"
            f"<div class='rc-n'>📌 {info['n']} préstamos activos</div>"
            "</div>"
            "<div style='width:40px;height:40px;border-radius:14px;display:flex;align-items:center;justify-content:center;"
            "background:rgba(255,255,255,.55); border:1px solid rgba(148,163,184,.25)'>"
            f"<span style='color:{icon_color}'>💵</span></div>"
            "</div>"
            f"<div class='rc-amt {amt_class}'>{html.escape(fmt_money(rem))}</div>"
            f"<div class='rc-by'>👤 {pres}</div>"
            "</div>"
        )

        bar_labels.append(r)
        bar_values.append(round(rem, 2))
        bar_colors.append(bar_color)

    # Totales
    active_routes = len(rutas_sorted)

    # Gráfica por ruta
    chart_block = ""
    if rutas_sorted:
        labels_json = json.dumps(bar_labels)
        values_json = json.dumps(bar_values)
        colors_json = json.dumps(bar_colors)
        chart_block = (
            "<div class='chart-card' data-r-anim='1'>"
            "<div class='chart-title'>📊 Capital por ruta</div>"
            "<div style='height:260px'><canvas id='routesChart'></canvas></div>"
            "<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>"
            "<script>"
            "const labels = LABELS;"
            "const values = VALUES;"
            "const colors = COLORS;"
            "const ctx = document.getElementById('routesChart');"
            "if(ctx){"
            "new Chart(ctx,{type:'bar',data:{labels:labels,datasets:[{label:'Capital activo',data:values,"
            "backgroundColor:colors,borderColor:colors,borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,"
            "plugins:{legend:{display:false}},scales:{y:{beginAtZero:true}}}});"
            "}"
            "</script>"
            "</div>"
        ).replace("LABELS", labels_json).replace("VALUES", values_json).replace("COLORS", colors_json)
    else:
        chart_block = "<div class='chart-card' data-r-anim='1'><div class='chart-title'>📊 Capital por ruta</div><div style='opacity:.85;font-weight:900'>Sin datos</div></div>"

    # HTML filtro
    filtro_html = (
        "<form method='get' class='routes-filter no-print'>"
        "<div>"
        "<label>Ruta</label>"
        "<select name='ruta' class='routes-input'>"
        f"<option value=''>-- Todas --</option>{route_options_html}"
        "</select>"
        "</div>"
        "<div>"
        "<label>Cobrador</label>"
        "<select name='prestamista' class='routes-input'>"
        "<option value=''>-- Todos --</option>" + prest_options_html +
        "</select>"
        "</div>"
        "<button class='routes-form-btn' type='submit'>"
        "<span style='display:inline-flex;align-items:center;justify-content:center'>🔎</span>Filtrar"
        "</button>"
        "</form>"
    )

    # Tarjetas arriba (totales)
    # Colors: capital total -> verde/azul/amarillo según tier sobre el máximo global.
    overall_tier = tier(total_rest)
    if overall_tier == "high":
        tone_class = "tone-green"
    elif overall_tier == "mid":
        tone_class = "tone-blue"
    else:
        tone_class = "tone-red"

    bank_btns = (
        "<div class='routes-actions no-print'>"
        f"<a class='btn-ghost' href='{url_for('dashboard')}'><span aria-hidden='true'>📊</span>Dashboard</a>"
        f"<a class='btn-ghost' href='{url_for('bank_home')}'><span aria-hidden='true'>🏦</span>Banco</a>"
        "</div>"
    )

    # Mensaje si no hay rutas
    empty_cards = (
        "<div class='route-card' data-r-anim='1'>"
        "<div class='rc-name'>Sin préstamos activos con ruta</div>"
        "<div class='rc-n'>Cambia filtros o crea nuevos préstamos.</div>"
        "</div>"
    )

    cards_section = cards_html if cards_html else empty_cards

    body = (
        style_block
        + "<div class='routes-wrap'>"
        + "<div class='routes-head'>"
        + "<div>"
        + "<div class='routes-title'>🏦 Capital por ruta</div>"
        + "<div class='routes-sub'>Saldo vivo agrupado por la ruta del cliente, con métricas y gráfica.</div>"
        + "</div>"
        + bank_btns
        + "</div>"
        + f"{filtro_html}"
        + "<div class='routes-grid-metrics'>"
        + "<div class='m-card tone-blue' data-r-anim='1'>"
        + "<div class='m-top'><div><div class='m-k'>Total préstamos activos</div><div class='m-v'>" + str(total_n) + "</div></div>"
        + "<div class='m-ic'>🧾</div></div></div>"
        + "<div class='m-card tone-green' data-r-anim='1'>"
        + "<div class='m-top'><div><div class='m-k'>Capital total activo</div><div class='m-v'>" + html.escape(fmt_money(total_rest)) + "</div></div>"
        + "<div class='m-ic'>💵</div></div></div>"
        + "<div class='m-card tone-purple' data-r-anim='1'>"
        + "<div class='m-top'><div><div class='m-k'>Rutas activas</div><div class='m-v'>" + str(active_routes) + "</div></div>"
        + "<div class='m-ic'>🗺️</div></div></div>"
        + "</div>"
        + "<div class='routes-cards-grid'>"
        + cards_section
        + "</div>"
        + chart_block
        + "<script>"
        + "document.addEventListener('DOMContentLoaded',function(){"
        + "document.querySelectorAll('[data-r-anim]').forEach(function(el){el.classList.add('in');});"
        + "});"
        + "</script>"
        + nav_subfooter()
        + "</div>"
    )
    return page(body)


@app.route("/bank/late")
@login_required
def bank_late():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    today = today_rd()

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
        c = client_dict_by_id(cid, org_id) or {}
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
    today = today_rd()
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
        c = client_dict_by_id(cid, org_id) or {}
        nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or (f"Cliente #{cid}" if cid else "Sin nombre")
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
    user = current_user()
    org_id = session.get("org_id")
    clientes_list = clients_for_user(org_id, user)
    loans_list = loans_for_user(org_id, user)
    client_ids_from_loans = {L.get("client_id") for L in loans_list if L.get("client_id")}
    seen = set()
    rows = []
    for c in clientes_list:
        cid = c.get("id")
        if cid in seen:
            continue
        seen.add(cid)
        sd = calc_client_score(cid, org_id)
        mc = calc_max_credito(sd["prestamos_pagados"], sd["score"])
        nm = f"{c.get('first_name', '')} {c.get('last_name') or ''}".strip() or f"Cliente #{cid}"
        rows.append(f"<tr><td>{html.escape(nm)}</td><td>{sd['score']}</td><td>{fmt_money(mc)}</td></tr>")
    for cid in client_ids_from_loans:
        if cid in seen:
            continue
        c = client_dict_by_id(cid, org_id)
        if not c:
            continue
        seen.add(cid)
        sd = calc_client_score(cid, org_id)
        mc = calc_max_credito(sd["prestamos_pagados"], sd["score"])
        nm = f"{c.get('first_name', '')} {c.get('last_name') or ''}".strip() or f"Cliente #{cid}"
        rows.append(f"<tr><td>{html.escape(nm)}</td><td>{sd['score']}</td><td>{fmt_money(mc)}</td></tr>")
    t = "".join(rows)
    return page(
        f'<div class="card"><h2>⭐ Score de clientes</h2><p style="opacity:.9;margin:0 0 12px 0;">Clientes con sugerencia de crédito según historial de préstamos.</p>'
        f'<div class="table-scroll"><table><tr><th>Cliente</th><th>Score</th><th>Crédito sug.</th></tr>'
        f"{t or '<tr><td colspan=3 style=\"text-align:center;opacity:.85\">Sin clientes</td></tr>'}</table></div>{nav_subfooter()}</div>"
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

    # Charts + Top clientes
    today = today_rd()
    d0 = today - timedelta(days=90)
    daily_labels, daily_balances = [], []
    ing_labels, ing_vals, gas_vals = [], [], []
    top_clientes = []
    if USE_DATABASE:
        try:
            from credimapa_pg import banco_daily_balance_data, banco_ingresos_gastos_by_date, get_top_clientes_by_loans, session_scope
            with session_scope() as sess:
                daily_data = banco_daily_balance_data(sess, oid, d0, today)
                daily_labels = [str(d[0]) for d in daily_data]
                daily_balances = [d[1] for d in daily_data]
                ig_data = banco_ingresos_gastos_by_date(sess, oid, d0, today)
                ing_labels = [str(x["date"]) for x in ig_data]
                ing_vals = [x["ingresos"] for x in ig_data]
                gas_vals = [x["gastos"] for x in ig_data]
                top_clientes = get_top_clientes_by_loans(sess, oid, 5)
        except Exception:
            pass
    else:
        # Store: aggregate from cash_reports
        by_date = {}
        for cr in store.cash_reports.values():
            if cr.get("organization_id") != oid:
                continue
            d = cr.get("date") or (cr.get("created_at").date() if hasattr(cr.get("created_at"), "date") else None)
            if not d or not (d0 <= d <= today):
                continue
            key = str(d)
            if key not in by_date:
                by_date[key] = {"ing": 0, "gas": 0, "net": 0}
            amt = float(cr.get("amount") or 0)
            by_date[key]["net"] += amt
            if amt > 0:
                by_date[key]["ing"] += amt
            else:
                by_date[key]["gas"] += abs(amt)
        sorted_dates = sorted(by_date.keys())
        base = float(getattr(store, "starting_banks", {}).get(oid, 0) or 0)
        cum = base
        cum_by_date = {}
        for d in sorted_dates:
            cum += by_date[d]["net"]
            cum_by_date[d] = round(cum, 2)
        daily_labels = sorted_dates[-60:]
        daily_balances = [cum_by_date.get(d, base) for d in daily_labels]
        ing_labels = sorted_dates[-30:]
        ing_vals = [by_date.get(d, {}).get("ing", 0) for d in ing_labels]
        gas_vals = [by_date.get(d, {}).get("gas", 0) for d in ing_labels]
        # Top clientes from store
        client_totals = {}
        for L in store.loans.values():
            if L.get("organization_id") != oid:
                continue
            cid = L.get("client_id")
            if cid:
                client_totals[cid] = client_totals.get(cid, 0) + float(L.get("amount") or 0)
        sorted_clients = sorted(client_totals.items(), key=lambda x: -x[1])[:5]
        for cid, tot in sorted_clients:
            c = store.clients.get(cid, {})
            top_clientes.append({
                "client_id": cid,
                "total": round(tot, 2),
                "nombre": f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".strip() or "—",
            })

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

  <div class="res-daily-title" style="margin-top:20px;">Gráficos</div>
  <div style="display:grid;grid-template-columns:1fr;gap:16px;margin:12px 0">
    <div style="background:rgba(255,255,255,.9);border-radius:16px;padding:14px;border:1px solid rgba(148,163,184,.2)">
      <div style="font-weight:900;margin-bottom:8px;font-size:13px">Saldo diario</div>
      <div style="height:220px"><canvas id="chartSaldoDiario"></canvas></div>
    </div>
    <div style="background:rgba(255,255,255,.9);border-radius:16px;padding:14px;border:1px solid rgba(148,163,184,.2)">
      <div style="font-weight:900;margin-bottom:8px;font-size:13px">Ingresos vs Gastos</div>
      <div style="height:220px"><canvas id="chartIngresosGastos"></canvas></div>
    </div>
  </div>

  <div class="res-daily-title" style="margin-top:20px;">Top 5 clientes (por préstamos)</div>
  <div style="margin:12px 0;display:flex;flex-wrap:wrap;gap:10px">
    {"".join(f'<div style="background:linear-gradient(135deg,#0d9488,#14b8a6);color:#fff;border-radius:14px;padding:10px 14px;min-width:140px"><div style="font-size:11px;opacity:.9">#{i}</div><div style="font-weight:900;font-size:14px">{html.escape(tc["nombre"][:20])}</div><div style="font-size:13px;margin-top:4px">{fmt_money(tc["total"])}</div></div>' for i, tc in enumerate(top_clientes, 1))}
    {"" if top_clientes else '<div style="opacity:.7;font-weight:800">Sin datos</div>'}
  </div>

  <div style="margin-top:14px;">{nav_subfooter()}</div>
</div>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
(function(){{
  const fmt = v => 'RD$ ' + Number(v||0).toLocaleString('es-DO',{{minimumFractionDigits:2}});
  const dailyLabels = {json.dumps(daily_labels)};
  const dailyBalances = {json.dumps(daily_balances)};
  const ingLabels = {json.dumps(ing_labels)};
  const ingVals = {json.dumps(ing_vals)};
  const gasVals = {json.dumps(gas_vals)};
  if(dailyLabels.length && document.getElementById('chartSaldoDiario')){{
    new Chart(document.getElementById('chartSaldoDiario'), {{
      type:'line',
      data:{{labels:dailyLabels,datasets:[{{label:'Saldo',data:dailyBalances,borderColor:'#0d9488',backgroundColor:'rgba(13,148,136,.15)',fill:true,tension:0.3,pointRadius:2}}]}},
      options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{beginAtZero:true,ticks:{{callback:v=>fmt(v)}}}}}}}}
    }});
  }}
  if(ingLabels.length && document.getElementById('chartIngresosGastos')){{
    new Chart(document.getElementById('chartIngresosGastos'), {{
      type:'bar',
      data:{{labels:ingLabels,datasets:[{{label:'Ingresos',data:ingVals,backgroundColor:'rgba(22,163,74,.5)'}},{{label:'Gastos',data:gasVals,backgroundColor:'rgba(239,68,68,.5)'}}]}},
      options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{beginAtZero:true,ticks:{{callback:v=>fmt(v)}}}}}}}}
    }});
  }}
}})();
</script>
"""
    )
    return page(body)


@app.route("/bank/cierre-semanal", methods=["GET"])
@login_required
def cierre_semanal():
    ensure_org()
    org_id = session.get("org_id")
    user = current_user()
    restrict = user_is_cobrador_limited(user)
    mode = (request.args.get("mode") or "").strip().lower()
    is_print = mode in ("print", "pdf", "imprimir")
    is_58mm = mode in ("58mm", "58", "t59", "recibo-58mm")

    def _parse_cierre_date(arg, default: date) -> date:
        if arg is None or not str(arg).strip():
            return default
        try:
            return datetime.strptime(str(arg).strip()[:10], "%Y-%m-%d").date()
        except ValueError:
            return default

    today = today_rd()
    default_ws = today - timedelta(days=today.weekday())
    default_we = default_ws + timedelta(days=6)
    week_start = _parse_cierre_date(request.args.get("fecha_inicio"), default_ws)
    week_end = _parse_cierre_date(request.args.get("fecha_fin"), default_we)
    if week_start > week_end:
        week_start, week_end = week_end, week_start

    GASTO_MOV_TYPES = ("gasto", "gasto_ruta")
    _pg_get_user = None
    if USE_DATABASE:
        from credimapa_pg import (
            compute_cierre_period_data,
            ensure_cierres_schema,
            get_session,
            get_clientes_dict,
            get_prestamos_dict,
            get_user as _pg_get_user,
        )

        ensure_cierres_schema()
        sess = get_session()
        cd = compute_cierre_period_data(
            sess,
            org_id,
            week_start,
            week_end,
            restrict=restrict,
            user_id=user.get("id"),
        )
        clients_map = get_clientes_dict(sess, [org_id])
        loans_map = get_prestamos_dict(sess, [org_id])
        payments_week = cd["payments_week"]
        prestamos_periodo = cd["prestamos_periodo"]
        descuentos = cd["descuentos"]
        gastos = cd["gastos"]
        capital_cobrado = cd["capital_cobrado"]
        interes_cobrado = cd["interes_cobrado"]
        descuentos_total = cd["descuentos_total"]
        gastos_total = cd["gastos_total"]
        ganancias_reales = cd["ganancia"]
        prestado_total = cd["prestado_total"]
    else:
        clients_map = store.clients
        loans_map = store.loans

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
            and (not restrict or p.get("created_by") == user.get("id"))
        ]
        prestamos_periodo = [
            L
            for L in store.loans.values()
            if L.get("organization_id") == org_id
            and L.get("start_date") is not None
            and week_start <= L.get("start_date") <= week_end
            and (not restrict or L.get("created_by") == user.get("id"))
        ]
        descuentos = [
            cr
            for cr in store.cash_reports.values()
            if cr.get("organization_id") == org_id
            and cr.get("movement_type") == "descuento_inicial"
            and cr.get("date") is not None
            and week_start <= cr.get("date") <= week_end
            and (not restrict or cr.get("user_id") == user.get("id"))
        ]
        gastos = [
            e
            for e in store.route_expenses.values()
            if e.get("organization_id") == org_id
            and e.get("created_at") is not None
            and week_start <= e.get("created_at").date() <= week_end
            and (not restrict or e.get("user_id") == user.get("id"))
        ]
        capital_cobrado = round(sum(float(p.get("amount") or 0) for p in payments_week), 2)
        interes_cobrado = round(sum(float(p.get("interest") or 0) for p in payments_week), 2)
        descuentos_total = round(sum(float(cr.get("amount") or 0) for cr in descuentos), 2)
        gastos_total = round(sum(abs(float(e.get("amount") or 0)) for e in gastos), 2)
        ganancias_reales = round(interes_cobrado + descuentos_total - gastos_total, 2)
        prestado_total = round(sum(float(L.get("amount") or 0) for L in prestamos_periodo), 2)

    payments_week.sort(key=lambda p: (p.get("created_by") or 0, p.get("id") or 0))

    total_amount_week = round(sum(float(p.get("amount") or 0) for p in payments_week), 2)
    period_qs = urlencode({"fecha_inicio": week_start.isoformat(), "fecha_fin": week_end.isoformat()})

    def _cierre_user_label(uid):
        if not uid:
            return "—"
        if _pg_get_user is not None:
            rw = _pg_get_user(uid) or {}
            return rw.get("name") or rw.get("username") or "—"
        u = store.users.get(uid, {})
        return (u.get("name") or u.get("username") or "—") if u else "—"

    # =========================
    # Pagos agrupados por cliente
    # =========================
    # Importante: NO mostramos el "admin" que registró el pago. Mostramos
    # el cliente real asociado al préstamo: payment.loan_id -> loan.client_id.
    payments_by_client = {}  # cid -> {client_name, total, items[{loan_id, amount, date}]}
    for p in payments_week:
        loan_id = p.get("loan_id")
        L = loans_map.get(loan_id) if loan_id is not None else None
        if not L or L.get("organization_id") != org_id:
            continue
        cid = L.get("client_id")
        cl = clients_map.get(cid, {}) if cid is not None else {}
        client_name = (
            f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip()
            or (f"Cliente #{cid}" if cid is not None else "—")
        )
        group = payments_by_client.get(cid)
        if not group:
            group = {"client_name": client_name, "total": 0.0, "items": []}
            payments_by_client[cid] = group
        amt = float(p.get("amount") or 0)
        group["total"] += amt
        group["items"].append({"loan_id": loan_id, "amount": amt, "date": p.get("date")})

    # Orden estable: por nombre de cliente.
    client_groups = sorted(
        payments_by_client.values(),
        key=lambda g: str(g.get("client_name") or "").lower(),
    )

    if not client_groups:
        client_groups_html = "<div style='opacity:.78'>Sin pagos</div>"
        client_print_html = "<div style='opacity:.78'>Sin pagos</div>"
    else:
        def payment_li(item):
            return (
                f"<li>"
                f"<span style='font-weight:950'>Préstamo #{item.get('loan_id')}</span>"
                f"<span style='float:right; font-weight:950'>{fmt_money(item.get('amount') or 0)}</span>"
                f"<div style='clear:both; opacity:.85; font-size:12px; margin-top:2px'>Fecha: {item.get('date') or '—'}</div>"
                f"</li>"
            )

        # Vista normal (fintech).
        cards = []
        for g in client_groups:
            items = sorted(g["items"], key=lambda x: (x.get("date") or date.min, x.get("loan_id") or 0))
            items_html = "".join(payment_li(it) for it in items)
            cards.append(
                "<div class='pay-client-card fade-soft'>"
                f"<div class='pay-client-head'>"
                f"<div class='pay-client-name'>{html.escape(g['client_name'])}</div>"
                f"<div class='pay-client-total'>Total: <b>{fmt_money(g['total'])}</b></div>"
                f"</div>"
                f"<ul class='pay-client-list'>{items_html}</ul>"
                f"</div>"
            )
        client_groups_html = "".join(cards)

        # Vista 58mm (recibo térmico).
        lines = []
        # Nota: para 58mm priorizamos legibilidad y un layout vertical.
        for g in client_groups:
            items = sorted(g["items"], key=lambda x: (x.get("date") or date.min, x.get("loan_id") or 0))
            lines.append(
                "<div class='receipt58-block'>"
                f"<div class='receipt58-title'>{html.escape(g['client_name'])}</div>"
                f"<div class='receipt58-sub'>Total: <b>{fmt_money(g['total'])}</b></div>"
                "<div class='receipt58-items'>"
                + "".join(
                    "<div class='receipt58-line'>"
                    f"<span style='font-weight:900'>#{it.get('loan_id')}</span>"
                    f"<span style='float:right; font-weight:900'>{fmt_money(it.get('amount') or 0)}</span>"
                    "<div style='clear:both; font-size:12px; opacity:.85; margin-top:2px'>"
                    f"Fecha: {it.get('date') or '—'}"
                    "</div>"
                    "</div>"
                    for it in items
                )
                + "</div>"
                "</div>"
            )
        client_print_html = "".join(lines)

    prestamos_rows = ""
    for L in sorted(
        prestamos_periodo,
        key=lambda x: (x.get("created_by") or 0, x.get("id") or 0),
    ):
        cid = L.get("client_id")
        cl = clients_map.get(cid, {}) if cid is not None else {}
        name = (
            f"{cl.get('first_name') or ''} {cl.get('last_name') or ''}".strip()
            or (f"Cliente #{cid}" if cid is not None else "—")
        )
        prestamos_rows += (
            "<tr>"
            f"<td style='padding-right:12px'>{html.escape(str(name))}</td>"
            f"<td style='text-align:right;font-weight:900'>{fmt_money(L.get('amount') or 0)}</td>"
            "</tr>"
        )
    if not prestamos_rows:
        prestamos_rows = "<tr><td colspan=2 style='opacity:.78'>Sin préstamos</td></tr>"

    descuentos_rows = ""
    for cr in descuentos:
        name = _cierre_user_label(cr.get("user_id"))
        raw_amt = float(cr.get("amount") or 0)
        descuentos_rows += (
            "<tr>"
            f"<td style='padding-right:12px'>{html.escape(str(name))}</td>"
            f"<td style='text-align:right;font-weight:900'>-{fmt_money(abs(raw_amt))}</td>"
            "</tr>"
        )
    if not descuentos_rows:
        descuentos_rows = "<tr><td colspan=2 style='opacity:.78'>Sin descuentos</td></tr>"

    gastos_rows = ""
    for e in sorted(gastos, key=lambda x: x.get("created_at") or datetime.min):
        name = _cierre_user_label(e.get("user_id"))
        amt_g = abs(float(e.get("amount") or 0))
        gastos_rows += (
            "<tr>"
            f"<td style='padding-right:12px'>{html.escape(str(name))}</td>"
            f"<td style='text-align:right;font-weight:900'>{fmt_money(amt_g)}</td>"
            "</tr>"
        )
    if not gastos_rows:
        gastos_rows = "<tr><td colspan=2 style='opacity:.78'>Sin gastos</td></tr>"

    # Ganancia real: (interés cobrado + descuentos en libro) − gastos (descuentos suelen ser negativos).
    ganancias_reales = round(interes_cobrado + descuentos_total - gastos_total, 2)

    report_brand_name = APP_BRAND
    logo_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64' width='34' height='34' "
        "fill='none' stroke='currentColor' stroke-width='3' stroke-linecap='round' stroke-linejoin='round'>"
        "<path d='M12 48V16a8 8 0 0 1 8-8h24a8 8 0 0 1 8 8v32'/>"
        "<path d='M18 44h28'/>"
        "<path d='M20 28l12-12 12 12'/>"
        "</svg>"
    )

    chart_vals = {
        "capital_cobrado": capital_cobrado,
        "interes_cobrado": interes_cobrado,
        "ganancias_reales": ganancias_reales,
    }
    chart_vals_json = json.dumps(chart_vals)
    charts_js = (
        "<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>"
        "<script>"
        "const chartVals = CHART_VALS;"
        "const ctx = document.getElementById('cierreChart');"
        "if(ctx){"
        "const data = {"
        "labels:['Capital cobrado','Interés cobrado','Ganancias'],"
        "datasets:[{label:'RD$',data:[chartVals.capital_cobrado,chartVals.interes_cobrado,chartVals.ganancias_reales],"
        "backgroundColor:['rgba(34,197,94,.35)','rgba(59,130,246,.35)','rgba(239,68,68,.35)'],"
        "borderColor:['rgba(34,197,94,1)','rgba(59,130,246,1)','rgba(239,68,68,1)'],"
        "borderWidth:1}]};"
        "new Chart(ctx,{type:'bar',data:data,options:{responsive:true,maintainAspectRatio:false,"
        "plugins:{legend:{display:false}},scales:{y:{beginAtZero:true}}}});"
        "}"
        "</script>"
    ).replace("CHART_VALS", chart_vals_json)

    # CSS extra del reporte.
    report_css = f"""
<style>
  .cierre-wrap{{max-width:1080px;margin:0 auto;padding:12px 0 26px}}
  .cierre-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap}}
  .cierre-brand{{display:flex;gap:12px;align-items:center}}
  .cierre-brandtxt{{display:flex;flex-direction:column;line-height:1.1}}
  .cierre-brandtxt .t{{font-weight:1000;font-size:18px}}
  .cierre-brandtxt .s{{opacity:.85;font-size:13px;font-weight:800}}
  .cierre-sub{{opacity:.85;font-weight:800;margin:8px 0 10px 0}}

  .cierre-grid{{display:grid;grid-template-columns: 1fr; gap:14px; margin-top:12px}}
  @media(min-width:920px){{.cierre-grid{{grid-template-columns: 1fr .95fr;}}}}

  .cierre-card{{background:rgba(255,255,255,.92);border:1px solid rgba(148,163,184,.35);border-radius:18px;box-shadow:0 10px 24px rgba(0,0,0,.06);padding:14px}}
  body.theme-dark .cierre-card{{background:rgba(15,23,42,.92);border-color:rgba(148,163,184,.25);box-shadow:0 14px 34px rgba(0,0,0,.35)}}
  .cierre-title{{font-weight:1000;margin:0 0 8px 0; font-size:16px}}
  .cierre-sectionhr{{opacity:.18;margin:10px 0}}

  .pay-client-card{{border:1px solid rgba(148,163,184,.22);background:rgba(248,250,252,.85);border-radius:16px;padding:12px;box-shadow:0 8px 20px rgba(0,0,0,.04);margin-bottom:10px}}
  body.theme-dark .pay-client-card{{background:rgba(2,6,23,.45);border-color:rgba(148,163,184,.18)}}
  .pay-client-head{{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:6px}}
  .pay-client-name{{font-weight:1000;font-size:15px}}
  .pay-client-total{{opacity:.9;font-weight:900}}
  .pay-client-list{{list-style:none;padding-left:0;margin:0;display:flex;flex-direction:column;gap:8px}}
  .pay-client-list li{{padding:10px 10px;border-radius:14px;background:rgba(255,255,255,.65);border:1px solid rgba(148,163,184,.18)}}
  body.theme-dark .pay-client-list li{{background:rgba(15,23,42,.55);border-color:rgba(148,163,184,.14)}}

  .no-print{{}}
  @media print{{
    header.topbar, #sideMenu, #menuOverlay, .premium-btn, .menu-overlay, .no-print{{display:none!important}}
    .container{{padding:0!important}}
    .cierre-wrap{{padding:0!important}}
  }}

  /* 58mm recibo */
  .receipt58-wrap{{width:58mm; margin:0 auto; padding:0 0 14px}}
  @page{{ size:58mm auto; margin:0; }}
  .receipt58-block{{border:1px dashed rgba(148,163,184,.35);padding:10px;border-radius:10px;margin:10px 0}}
  .receipt58-title{{font-weight:1000; font-size:14px}}
  .receipt58-sub{{opacity:.9; font-weight:900; font-size:12px; margin-top:4px}}
  .receipt58-line{{margin-top:8px;}}
</style>
"""

    # -------------------------
    # Render por modo
    # -------------------------
    if is_58mm:
        body = (
            report_css
            + "<div class='receipt58-wrap'>"
            + "<div style='display:flex;align-items:center;gap:10px;padding:12px 0'>"
            + "<div style='color:#16a34a;font-weight:1000'>" + logo_svg + "</div>"
            + "<div>"
            + f"<div style='font-weight:1000;font-size:15px'>{report_brand_name}</div>"
            + f"<div style='opacity:.85;font-size:12px;font-weight:900'>Cierre semanal</div>"
            + f"<div style='opacity:.85;font-size:11px'>Semana: {week_start.isoformat()} → {week_end.isoformat()}</div>"
            + "</div>"
            + "</div>"
            + "<div style='font-weight:1000;opacity:.95; padding:0 0 8px 0'>Pagos por cliente</div>"
            + client_print_html
            + "<div style='margin-top:10px;border-top:1px solid rgba(148,163,184,.35);padding-top:10px'>"
            + f"<div style='display:flex;justify-content:space-between'><span style='opacity:.85;font-weight:900'>Capital cobrado</span><b>{fmt_money(capital_cobrado)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.85;font-weight:900'>Interés cobrado</span><b>{fmt_money(interes_cobrado)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.85;font-weight:900'>Descuentos (Σ)</span><b>{fmt_money(descuentos_total)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.85;font-weight:900'>Gastos</span><b>{fmt_money(gastos_total)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.85;font-weight:900'>Ganancia real</span><b>{fmt_money(ganancias_reales)}</b></div>"
            + "</div>"
            + f"<div style='margin-top:10px;opacity:.85;font-size:11px'>Fecha reporte: {today_rd().isoformat()}</div>"
            + "<script>window.print();</script>"
            + "</div>"
        )
        return page(body)

    if is_print:
        # Vista imprimible (bank style). Incluye todas las secciones.
        body = (
            report_css
            + "<div class='cierre-wrap'>"
            + "<div class='cierre-head'>"
            + "<div class='cierre-brand'>"
            + "<div style='color:#16a34a'>" + logo_svg + "</div>"
            + "<div class='cierre-brandtxt'>"
            + f"<div class='t'>{report_brand_name}</div>"
            + "<div class='s'>Reporte financiero - Cierre semanal</div>"
            + "</div>"
            + "</div>"
            + f"<div class='cierre-sub'>Semana: {week_start.isoformat()} → {week_end.isoformat()}<br/>Fecha reporte: {today_rd().isoformat()}</div>"
            + "</div>"
            + "<div class='cierre-grid'>"
            + "<div class='cierre-card'>"
            + "<div class='cierre-title'>💰 Pagos (agrupados por cliente)</div>"
            + client_groups_html
            + "<hr class='cierre-sectionhr'/>"
            + "<div class='cierre-title'>💸 Préstamos entregados</div>"
            + "<div class='table-scroll'><table><tbody>" + prestamos_rows + "</tbody></table></div>"
            + "<hr class='cierre-sectionhr'/>"
            + "<div class='cierre-title'>📉 Descuentos (informativo)</div>"
            + "<div class='table-scroll'><table><tbody>" + descuentos_rows + "</tbody></table></div>"
            + "<hr class='cierre-sectionhr'/>"
            + "<div class='cierre-title'>🚗 Gastos ruta</div>"
            + "<div class='table-scroll'><table><tbody>" + gastos_rows + "</tbody></table></div>"
            + "</div>"
            + "<div class='cierre-card'>"
            + "<div class='cierre-title'>📊 Totales</div>"
            + f"<div style='padding:10px 12px;background:rgba(236,253,245,.55);border:1px solid rgba(148,163,184,.25);border-radius:14px'>"
            + f"<div style='display:flex;justify-content:space-between'><span style='opacity:.88'>Capital cobrado</span><b>{fmt_money(capital_cobrado)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Interés cobrado</span><b>{fmt_money(interes_cobrado)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Descuentos (Σ libro)</span><b>{fmt_money(descuentos_total)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Gastos</span><b>{fmt_money(gastos_total)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Ganancia real</span><b>{fmt_money(ganancias_reales)}</b></div>"
            + "</div>"
            + "<hr class='cierre-sectionhr'/>"
            + "<div class='cierre-title'>Gráficas</div>"
            + "<div style='height:220px' class='no-print'><div style='height:220px'></div></div>"
            + "<script>window.print();</script>"
            + "</div>"
            + "</div>"
            + "</div>"
        )
        return page(body)

    # -------------------------
    # Vista normal (con gráficos + acciones)
    # -------------------------
    print_href = url_for("cierre_semanal") + "?" + period_qs + "&mode=print"
    print58_href = url_for("cierre_semanal") + "?" + period_qs + "&mode=58mm"

    period_form = (
        f'<form method="get" action="{url_for("cierre_semanal")}" class="no-print" '
        f'style="margin:0 0 12px 0;padding:12px;background:rgba(248,250,252,.9);border:1px solid rgba(148,163,184,.25);border-radius:14px;display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end">'
        f'<div><div style="font-size:11px;font-weight:900;opacity:.8;margin-bottom:4px">Desde (RD)</div>'
        f'<input type="date" name="fecha_inicio" value="{week_start.isoformat()}" required style="padding:8px 10px;border-radius:10px;border:1px solid rgba(148,163,184,.35)"/></div>'
        f'<div><div style="font-size:11px;font-weight:900;opacity:.8;margin-bottom:4px">Hasta (RD)</div>'
        f'<input type="date" name="fecha_fin" value="{week_end.isoformat()}" required style="padding:8px 10px;border-radius:10px;border:1px solid rgba(148,163,184,.35)"/></div>'
        f'<button type="submit" class="btn btn-secondary" style="margin-top:18px">Actualizar periodo</button>'
        f"</form>"
    )

    cerrar_block = ""
    if USE_DATABASE and can_admin_actions(user):
        cerrar_block = (
            f'<form method="post" action="{url_for("cerrar_semana")}" style="margin-top:14px" class="no-print">'
            f'<input type="hidden" name="fecha_inicio" value="{week_start.isoformat()}"/>'
            f'<input type="hidden" name="fecha_fin" value="{week_end.isoformat()}"/>'
            f'<p style="margin:0 0 8px 0;font-size:13px;opacity:.88">Cierra el cuadre del periodo mostrado: guarda totales en historial y vacía préstamos, pagos y clientes. El libro <b>banco</b> no se modifica.</p>'
            f'<button class="btn btn-primary" type="submit" style="width:100%;font-size:16px;padding:12px 10px" '
            f'onclick="return confirm(\'¿Cerrar cuadre? Esta acción borra préstamos, pagos y clientes del tenant.\');">'
            f"✅ CERRAR CUADRE</button>"
            f"</form>"
        )
    elif USE_DATABASE:
        cerrar_block = (
            '<p class="no-print" style="margin-top:14px;font-size:13px;opacity:.85">Solo administración puede cerrar cuadre.</p>'
        )
    else:
        if can_admin_actions(user):
            cerrar_block = (
                '<p class="no-print" style="margin-top:14px;font-size:13px;opacity:.85">'
                "Sin PostgreSQL: puedes registrar un snapshot en memoria (no borra datos).</p>"
                f'<form method="post" action="{url_for("cerrar_semana")}" class="no-print" style="margin-top:8px">'
                f'<input type="hidden" name="fecha_inicio" value="{week_start.isoformat()}"/>'
                f'<input type="hidden" name="fecha_fin" value="{week_end.isoformat()}"/>'
                f'<button class="btn btn-secondary" type="submit">Registrar cierre (memoria)</button>'
                f"</form>"
            )
        else:
            cerrar_block = ""

    body = (
        report_css
        + "<div class='cierre-wrap'>"
        + period_form
        + "<div class='cierre-head'>"
        + "<div class='cierre-brand'>"
        + "<div style='color:#16a34a'>" + logo_svg + "</div>"
        + "<div class='cierre-brandtxt'>"
        + f"<div class='t'>{report_brand_name}</div>"
        + "<div class='s'>Reporte financiero - Cierre semanal</div>"
        + "</div>"
        + "</div>"
        + "<div class='cierre-actions no-print' style='display:flex;gap:10px;flex-wrap:wrap;align-items:center'>"
        + "<a class='btn btn-secondary btn-action' href='" + print_href + "'>"
        + "<span class='btn-ic' aria-hidden='true'><svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='18' height='18' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><path d='M6 9V2h12v7'/><path d='M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2'/><path d='M6 14h12v10H6z'/></svg></span>Imprimir</a>"
        + "<a class='btn btn-primary btn-action' href='" + print58_href + "' style='color:#047857'>"
        + "<span class='btn-ic' aria-hidden='true'><svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='18' height='18' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><path d='M7 10V3h10v7'/><path d='M7 21h10v-8H7v8z'/><path d='M9 13h6'/></svg></span>58mm</a>"
        + "</div>"
        + "</div>"
        + f"<div class='cierre-sub'>Semana: {week_start.isoformat()} → {week_end.isoformat()}<br/>Fecha reporte: {today_rd().isoformat()}</div>"
        + "<div class='cierre-grid'>"
        + "<div class='cierre-card'>"
        + "<div class='cierre-title'>💰 Pagos (agrupados por cliente)</div>"
        + client_groups_html
        + "<hr class='cierre-sectionhr'/>"
        + "<div class='cierre-title'>💸 Préstamos entregados</div>"
        + "<div class='table-scroll'><table><tbody>" + prestamos_rows + "</tbody></table></div>"
        + "<hr class='cierre-sectionhr'/>"
        + "<div class='cierre-title'>📉 Descuentos (informativo)</div>"
        + "<div class='table-scroll'><table><tbody>" + descuentos_rows + "</tbody></table></div>"
        + "<hr class='cierre-sectionhr'/>"
        + "<div class='cierre-title'>🚗 Gastos ruta</div>"
        + "<div class='table-scroll'><table><tbody>" + gastos_rows + "</tbody></table></div>"
        + "</div>"
        + "<div class='cierre-card'>"
        + "<div class='cierre-title'>📊 Totales</div>"
        + f"<div style='padding:10px 12px;background:rgba(236,253,245,.55);border:1px solid rgba(148,163,184,.25);border-radius:14px'>"
            + f"<div style='display:flex;justify-content:space-between'><span style='opacity:.88'>Capital cobrado</span><b>{fmt_money(capital_cobrado)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Interés cobrado</span><b>{fmt_money(interes_cobrado)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Descuentos (Σ libro)</span><b>{fmt_money(descuentos_total)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Gastos</span><b>{fmt_money(gastos_total)}</b></div>"
            + f"<div style='display:flex;justify-content:space-between;margin-top:6px'><span style='opacity:.88'>Ganancia real</span><b>{fmt_money(ganancias_reales)}</b></div>"
        + "</div>"
        + "<div style='height:18px'></div>"
        + "<div class='cierre-title'>📈 Gráficas</div>"
        + "<div style='height:260px; margin-top:8px' class='no-print'>"
        + "<div style='height:260px'><canvas id='cierreChart'></canvas></div>"
        + "</div>"
        + charts_js
        + cerrar_block
        + "</div>"
        + "</div>"
        + nav_subfooter()
        + "</div>"
    )

    return page(body)


@app.route("/bank/cerrar-semana", methods=["POST"])
@login_required
def cerrar_semana():
    ensure_org()
    org_id = session.get("org_id")
    user = current_user()
    if not can_admin_actions(user):
        flash("Solo administración puede cerrar cuadre.", "danger")
        return redirect(url_for("cierre_semanal"))

    def _fd(name):
        raw = (request.form.get(name) or "").strip()[:10]
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None

    d0 = _fd("fecha_inicio")
    d1 = _fd("fecha_fin")
    if d0 is None or d1 is None:
        flash("Indica fecha inicio y fin del periodo.", "danger")
        return redirect(url_for("cierre_semanal"))
    if d0 > d1:
        d0, d1 = d1, d0
    notas = (request.form.get("notas") or "").strip()

    if USE_DATABASE:
        try:
            from credimapa_pg import execute_cerrar_cuadre

            execute_cerrar_cuadre(org_id, user["id"], d0, d1, notas)
            try:
                log_action(user["id"], "cerrar_cuadre_pg", module="banco", detail=f"{d0}..{d1}")
            except Exception:
                pass
            flash("Cuadre cerrado correctamente", "success")
            return redirect(url_for("dashboard"))
        except Exception as e:
            flash(f"No se pudo cerrar el cuadre: {e}", "danger")
            qs = urlencode({"fecha_inicio": d0.isoformat(), "fecha_fin": d1.isoformat()})
            return redirect(url_for("cierre_semanal") + "?" + qs)

    k = compute_financial_kpis(org_id, user)
    rec = {
        "id": store.nid("cierre"),
        "organization_id": org_id,
        "closed_at": utc_now_for_db(),
        "user_id": user["id"],
        "notas": notas,
        "fecha_inicio": d0,
        "fecha_fin": d1,
        "cobrado_hoy_snapshot": k["cobrado_hoy"],
        "total_por_cobrar_snapshot": k["total_por_cobrar"],
        "n_activos": k["n_activos"],
    }
    store.closure_history.append(rec)
    try:
        log_action(user["id"], "cierre_semana", str(rec["id"]))
    except Exception:
        pass
    flash("Cierre registrado en memoria (sin PostgreSQL).", "success")
    return redirect(url_for("historial_cierres"))


@app.route("/bank/historial-cierres")
@login_required
def historial_cierres():
    ensure_org()
    oid = session.get("org_id")
    rows = ""
    if USE_DATABASE:
        from credimapa_pg import ensure_cierres_schema, list_cierres_admin

        ensure_cierres_schema()
        recs = list_cierres_admin(oid, limit=50)
        for rec in recs:
            cid = rec.get("id")
            del_form = ""
            if can_admin_actions(current_user()):
                del_form = (
                    f'<form method="post" action="{url_for("borrar_cierre", cierre_id=cid)}" style="display:inline" '
                    f'onsubmit="return confirm(\'¿Eliminar este registro?\');"><button type="submit" class="btn btn-secondary">Borrar</button></form>'
                )
            fi = rec.get("fecha_inicio")
            ff = rec.get("fecha_fin")
            periodo = f"{fi or '—'} → {ff or '—'}"
            cap = rec.get("capital_cobrado")
            intr = rec.get("interes_cobrado")
            gas = rec.get("gastos_cuadre") if rec.get("gastos_cuadre") is not None else rec.get("gastos")
            des = rec.get("descuentos_cuadre") if rec.get("descuentos_cuadre") is not None else rec.get("descuentos")
            gan = rec.get("ganancia_cuadre") if rec.get("ganancia_cuadre") is not None else rec.get("ganancia")
            rows += (
                f"<tr><td>{html.escape(str(rec.get('closed_at') or '—'))}</td>"
                f"<td>{html.escape(periodo)}</td>"
                f"<td>{fmt_money(cap)}</td><td>{fmt_money(intr)}</td>"
                f"<td>{fmt_money(des)}</td><td>{fmt_money(gas)}</td><td>{fmt_money(gan)}</td>"
                f"<td>{html.escape(str(rec.get('notas') or '—'))}</td><td>{del_form}</td></tr>"
            )
        head = (
            "<tr><th>Cerrado</th><th>Periodo</th><th>Capital cobrado</th><th>Interés</th>"
            "<th>Descuentos</th><th>Gastos</th><th>Ganancia</th><th>Notas</th><th></th></tr>"
        )
        sub = "<p>Registros guardados en PostgreSQL al cerrar cuadre.</p>"
        colspan = 9
    else:
        head = (
            "<tr><th>Fecha</th><th>Cobrado hoy (snap)</th><th>Total por cobrar</th><th>Activos</th><th>Notas</th><th></th></tr>"
        )
        sub = "<p>Últimos cierres en memoria (modo sin base de datos).</p>"
        colspan = 6
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
        f'<div class="card"><h2>✔️ Cuadres cerrados</h2>{sub}'
        f'<div class="table-scroll"><table>{head}'
        f"{rows or f'<tr><td colspan={colspan}>Sin cierres aún</td></tr>'}</table></div>{nav_subfooter()}</div>"
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
    u = current_user()
    if not can_admin_actions(u):
        flash("Solo administración puede borrar cierres.", "danger")
        return redirect(url_for("historial_cierres"))
    if USE_DATABASE:
        try:
            from credimapa_pg import delete_cierre_admin

            if delete_cierre_admin(cierre_id, oid):
                flash("Cierre eliminado.", "success")
            else:
                flash("Cierre no encontrado.", "warning")
        except Exception as e:
            flash(f"No se pudo eliminar: {e}", "danger")
        return redirect(url_for("historial_cierres"))
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
    u = current_user()
    if not can_admin_actions(u):
        flash("Acción restringida: solo admin puede agregar dinero al banco.", "danger")
        return redirect(url_for("bank_home"))
    if request.method == "POST":
        u = current_user()
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
            {
                "id": rid,
                "organization_id": org_id,
                "user_id": u.get("id"),
                "amount": amt,
                "note": note,
                "at": utc_now_for_db(),
            }
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
    u = current_user()
    restrict = user_is_cobrador_limited(u)
    tipo_filter = request.args.get("tipo", "").strip()  # "", "ingreso", "egreso"

    def parse_date(raw):
        raw = (raw or "").strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None

    dt_from = parse_date(request.args.get("desde"))
    dt_to = parse_date(request.args.get("hasta"))

    if USE_DATABASE:
        try:
            from credimapa_pg import list_banco_all, session_scope
            with session_scope() as sess:
                raw_list = list_banco_all(
                    sess, oid,
                    d0=dt_from, d1=dt_to,
                    user_id=u.get("id") if restrict else None,
                )
            movimientos_asc = []
            for m in raw_list:
                d = dict(m)
                amt = float(d.get("amount") or 0)
                d["tipo_visual"] = "ingreso" if amt >= 0 else "egreso"
                d["created_at"] = d.get("created_at")
                d["mov_date"] = d.get("mov_date") or d.get("date")
                movimientos_asc.append(d)
        except Exception:
            movimientos_asc = []
    else:
        movimientos_asc = []
        for cr in store.cash_reports.values():
            if cr.get("organization_id") != oid:
                continue
            if restrict and cr.get("user_id") != u.get("id"):
                continue
            d_at = cr.get("created_at") or cr.get("date")
            if dt_from or dt_to:
                d_val = d_at.date() if hasattr(d_at, "date") else (d_at if isinstance(d_at, date) else None)
                if d_val is None:
                    continue
                if dt_from and d_val < dt_from:
                    continue
                if dt_to and d_val > dt_to:
                    continue
            amt = float(cr.get("amount") or 0)
            movimientos_asc.append({
                "id": cr.get("id"),
                "amount": amt,
                "movement_type": cr.get("movement_type") or "movimiento",
                "note": cr.get("note"),
                "created_at": d_at,
                "mov_date": cr.get("date") or (d_at.date() if hasattr(d_at, "date") else None),
                "tipo_visual": "ingreso" if amt >= 0 else "egreso",
            })
        movimientos_asc.sort(key=lambda x: (x.get("created_at") or datetime.min, x.get("id") or 0))

    # Running balance (ASC order)
    saldo = 0.0
    for m in movimientos_asc:
        saldo += float(m.get("amount") or 0)
        m["saldo"] = round(saldo, 2)

    # Reverse for display (newest first)
    movimientos = list(reversed(movimientos_asc))

    # Apply tipo filter
    if tipo_filter == "ingreso":
        movimientos = [m for m in movimientos if m.get("tipo_visual") == "ingreso"]
    elif tipo_filter == "egreso":
        movimientos = [m for m in movimientos if m.get("tipo_visual") == "egreso"]

    def fmt_dt(dt):
        if not dt:
            return "—"
        if hasattr(dt, "date"):
            dt = dt
        return format_dt_rd(dt) if isinstance(dt, datetime) else str(dt)

    def tipo_label(mt):
        labels = {
            "deposito_banco": "Depósito banco",
            "pago_prestamo": "Pago préstamo",
            "descuento_inicial": "Descuento inicial",
            "prestamo_entregado": "Préstamo entregado",
            "reverso_pago_prestamo": "Reverso pago",
            "reverso_adelanto": "Reverso adelanto",
            "entrega_cobrador": "Entrega cobrador",
            "devolucion_capital": "Devolución capital",
            "gasto": "Gasto",
            "gasto_ruta": "Gasto ruta",
        }
        return labels.get(mt, (mt or "").replace("_", " ").title())

    style_block = """
<style>
  .dep-wrap{max-width:1060px;margin:0 auto;padding:12px 0 22px}
  .dep-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap}
  .dep-title{font-weight:1000;font-size:18px;margin:0}
  .dep-sub{opacity:.85;font-size:13px;margin-top:6px;font-weight:800}
  .dep-grid{display:grid;gap:12px;margin-top:12px}
  @media(min-width:860px){.dep-grid{grid-template-columns: 1fr .85fr}}

  .dep-card{
    background: rgba(255,255,255,.92);
    border: 1px solid rgba(148,163,184,.35);
    border-radius: 18px;
    box-shadow: 0 10px 24px rgba(0,0,0,.06);
    padding: 14px;
  }
  body.theme-dark .dep-card{
    background: rgba(15,23,42,.92);
    border-color: rgba(148,163,184,.25);
    box-shadow: 0 14px 34px rgba(0,0,0,.35);
  }

  .dep-mini{
    display:flex;flex-direction:column;gap:8px;
    padding:12px 12px;border-radius:16px;
    border:1px solid rgba(148,163,184,.22);
    background: rgba(248,250,252,.75);
  }
  body.theme-dark .dep-mini{background: rgba(2,6,23,.45)}
  .dep-mini-k{font-weight:900;opacity:.9;font-size:13px;display:flex;gap:8px;align-items:center}
  .dep-mini-v{font-weight:1000;font-size:20px;color:#0f766e}
  body.theme-dark .dep-mini-v{color:#5eead4}
  .dep-mini-small{font-weight:1000;font-size:12px;opacity:.85}

  .dep-filter{
    display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;justify-content:flex-end
  }
  .dep-filter label{font-size:12px;font-weight:900;opacity:.9}
  .dep-input{
    padding:10px 10px;border-radius:12px;border:1px solid rgba(148,163,184,.35);
    background: rgba(255,255,255,.8);
    color:#0f172a;font-weight:800;
  }
  body.theme-dark .dep-input{background: rgba(2,6,23,.55);color:#e5e7eb}
  .dep-btn{display:inline-flex;align-items:center;gap:8px}
  .dep-tipo-btn{padding:8px 12px;border-radius:10px;font-weight:800;font-size:12px;text-decoration:none;border:1px solid rgba(148,163,184,.3)}
  .dep-tipo-btn.active{background:rgba(59,130,246,.2);border-color:rgba(59,130,246,.5);color:#1d4ed8}
  .dep-tipo-btn:not(.active){background:rgba(248,250,252,.8);color:#475569}
  body.theme-dark .dep-tipo-btn:not(.active){background:rgba(2,6,23,.5);color:#94a3b8}

  .dep-table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px}
  .dep-table th{
    text-align:left;padding:12px 10px;
    border-bottom:2px solid rgba(148,163,184,.25);
    color: rgba(15,23,42,.92);
    background: rgba(2,132,199,.06);
    position: sticky; top: 0;
  }
  body.theme-dark .dep-table th{color: rgba(229,231,235,.95); background: rgba(59,130,246,.08); border-bottom-color: rgba(148,163,184,.18)}
  .dep-table td{
    padding:12px 10px;border-bottom:1px solid rgba(148,163,184,.16);
    vertical-align: middle;
  }
  .dep-table td.num{text-align:right}
  .dep-row{transition: transform .12s ease, background .12s ease}
  .dep-row:hover{background: rgba(236,253,245,.5); transform: translateY(-1px)}
  body.theme-dark .dep-row:hover{background: rgba(16,185,129,.10)}

  .dep-amt{font-weight:1000; font-size:15px; white-space:nowrap}
  .dep-amt.pos{color:#047857}
  .dep-amt.neg{color:#dc2626}
  .dep-saldo{font-weight:900;font-size:13px;color:#0f172a}
  body.theme-dark .dep-saldo{color:#e5e7eb}

  .dep-empty{
    padding:18px; border-radius:16px;
    border:1px dashed rgba(148,163,184,.35);
    background: rgba(248,250,252,.65);
    text-align:center;
    font-weight:900;
  }
  body.theme-dark .dep-empty{background: rgba(2,6,23,.35)}

  [data-anim]{opacity:0;transform: translateY(10px);transition: opacity .35s ease, transform .35s ease}
  [data-anim].in{opacity:1;transform: translateY(0)}
</style>
"""

    saldo_final = round(movimientos_asc[-1]["saldo"], 2) if movimientos_asc else 0.0
    n_movs = len(movimientos)

    def _tipo_url(t):
        params = {"desde": dt_from.isoformat() if dt_from else "", "hasta": dt_to.isoformat() if dt_to else ""}
        if t:
            params["tipo"] = t
        return url_for("historial_depositos", **{k: v for k, v in params.items() if v})

    tipo_links = (
        f"<a class='dep-tipo-btn{" active" if not tipo_filter else ""}' href='{_tipo_url("")}'>Todos</a>"
        + f"<a class='dep-tipo-btn{" active" if tipo_filter == "ingreso" else ""}' href='{_tipo_url("ingreso")}'>💰 Solo ingresos</a>"
        + f"<a class='dep-tipo-btn{" active" if tipo_filter == "egreso" else ""}' href='{_tipo_url("egreso")}'>💸 Solo egresos</a>"
    )

    rows = ""
    for m in movimientos:
        amt = float(m.get("amount") or 0)
        tv = m.get("tipo_visual") or ("ingreso" if amt >= 0 else "egreso")
        cls = "pos" if tv == "ingreso" else "neg"
        icon = "💰" if tv == "ingreso" else "💸"
        amt_str = f"+{fmt_money(amt)}" if tv == "ingreso" else fmt_money(amt)
        rows += (
            "<tr class='dep-row'>"
            f"<td>{fmt_dt(m.get('mov_date') or m.get('created_at'))}</td>"
            f"<td>{icon} {html.escape(tipo_label(m.get('movement_type')))}</td>"
            f"<td>{html.escape(str(m.get('note') or '—'))}</td>"
            f"<td class='dep-amt {cls} num'>{amt_str}</td>"
            f"<td class='dep-saldo num'>{fmt_money(m.get('saldo') or 0)}</td>"
            "</tr>"
        )

    rows_html = rows if rows else "<tr><td colspan='5'><div class='dep-empty'>No hay movimientos en el rango</div></td></tr>"

    body = (
        style_block
        + "<div class='dep-wrap'>"
        + "<div class='dep-head'>"
        + "<div>"
        + "<h2 class='dep-title'>🏦 Historial de movimientos</h2>"
        + "<div class='dep-sub'>Libro mayor: todos los movimientos del banco con saldo automático.</div>"
        + "</div>"
        + "<div class='dep-filter no-print'>"
        + "<form method='get' style='display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;justify-content:flex-end'>"
        + "<input type='hidden' name='tipo' value='" + html.escape(tipo_filter) + "'>"
        + "<div><label>Desde</label>"
        + f"<input class='dep-input' type='date' name='desde' value='{(dt_from.isoformat() if dt_from else '')}'></div>"
        + "<div><label>Hasta</label>"
        + f"<input class='dep-input' type='date' name='hasta' value='{(dt_to.isoformat() if dt_to else '')}'></div>"
        + "<button class='btn btn-primary dep-btn' type='submit'>Filtrar</button>"
        + "</form>"
        + "</div>"
        + "</div>"

        + "<div style='display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px' data-anim='1'>" + tipo_links + "</div>"

        + "<div class='dep-grid'>"
        + "<div class='dep-card' data-anim='1'>"
        + "<div style='display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap'>"
        + "<div style='display:flex;gap:10px;align-items:center'>"
        + "<div style='width:34px;height:34px;border-radius:14px;background:rgba(16,185,129,.12);border:1px solid rgba(16,185,129,.30);display:flex;align-items:center;justify-content:center;color:#047857'>💰</div>"
        + "<div><div style='font-weight:1000;font-size:14px'>Resumen</div><div style='opacity:.85;font-weight:800;font-size:12px'>Movimientos y saldo</div></div>"
        + "</div>"
        + "<div style='display:flex;gap:10px;flex-wrap:wrap'>"
        + "<div class='dep-mini'>"
        + "<div class='dep-mini-k'>Saldo final</div>"
        + f"<div class='dep-mini-v'>{fmt_money(saldo_final)}</div>"
        + "<div class='dep-mini-small'>Después del rango</div>"
        + "</div>"
        + "<div class='dep-mini'>"
        + "<div class='dep-mini-k'>Movimientos</div>"
        + f"<div class='dep-mini-v'>{n_movs}</div>"
        + "<div class='dep-mini-small'>En la vista</div>"
        + "</div>"
        + "</div>"
        + "</div>"
        + "<div style='height:10px'></div>"
        + "<div class='table-scroll'>"
        + "<table class='dep-table'>"
        + "<thead><tr><th>Fecha</th><th>Tipo</th><th>Nota</th><th class='num'>Monto</th><th class='num'>Saldo</th></tr></thead>"
        + "<tbody>" + rows_html + "</tbody>"
        + "</table>"
        + "</div>"
        + "</div>"

        + "<div class='dep-card' data-anim='1'>"
        + "<div style='font-weight:1000;margin-bottom:8px'>Accesos</div>"
        + "<div style='display:flex;gap:10px;flex-wrap:wrap'>"
        + f"<a class='btn btn-secondary btn-action' href='{url_for('dashboard')}'>Dashboard</a>"
        + f"<a class='btn btn-primary btn-action' href='{url_for('bank_home')}'>Banco</a>"
        + "</div>"
        + "<div style='margin-top:12px;opacity:.88;font-weight:800;font-size:13px'>Sugerencia</div>"
        + "<div style='margin-top:8px;opacity:.85;font-weight:700;font-size:12px;line-height:1.35'>Usa el filtro por fecha y tipo (ingresos/egresos) para analizar el flujo de caja.</div>"
        + "</div>"
        + "</div>"

        + "<script>"
        + "document.addEventListener('DOMContentLoaded',function(){"
        + "document.querySelectorAll('[data-anim]').forEach(function(el){el.classList.add('in');});"
        + "});"
        + "</script>"
        + nav_subfooter()
        + "</div>"
    )

    return page(body)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"[CREDIMAPA — memoria] http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
