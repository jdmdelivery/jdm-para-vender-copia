# -*- coding: utf-8 -*-
# JDM Cash Now — datos solo en memoria (sin base de datos). Se pierden al reiniciar el servidor.
from __future__ import annotations

import os
import base64
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
)

APP_BRAND = "JDM Cash Now"
ADMIN_PIN = os.getenv("ADMIN_PIN", "5555")
CURRENCY = "RD$"
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "3128565688")
CARTERA_ADMIN_USER_ID = 60
ORG_ID = 1
ROLES = ("admin", "supervisor", "cobrador")


def fmt_money(val):
    try:
        v = float(val or 0)
    except (TypeError, ValueError):
        v = 0.0
    return f"{CURRENCY} {v:,.2f}"


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
        "weekly_closures", "deposit_history", "closure_history", "starting_bank", "_seq",
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
        # Banco inicial del sistema (solo para demo en memoria).
        # El banco real se calcula como: starting_bank + suma de cash_reports.amount.
        self.starting_bank = 50000.0
        self._seq = {
            "users": 1,
            "clients": 0,
            "loans": 0,
            "payments": 0,
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
        self.users[1] = {
            "id": 1, "username": "admin", "password_hash": generate_password_hash("admin"),
            "role": "admin", "phone": "", "organization_id": ORG_ID, "created_at": datetime.utcnow(), "name": None,
        }
        uid = self.nid("users")
        self.users[uid] = {
            "id": uid, "username": "cobrador", "password_hash": generate_password_hash("cobrador"),
            "role": "cobrador", "phone": "", "organization_id": ORG_ID, "created_at": datetime.utcnow(), "name": None,
        }
        cid = self.nid("clients")
        self.clients[cid] = {
            "id": cid, "first_name": "Demo", "last_name": "Cliente", "document_id": "000-0000000-0",
            "phone": "8090000000", "address": "Santo Domingo", "route": "Ruta 1",
            "created_by": uid, "organization_id": ORG_ID, "created_at": datetime.utcnow(),
        }
        lid = self.nid("loans")
        self.loans[lid] = {
            "id": lid, "client_id": cid, "amount": 10000, "rate": 10, "frequency": "semanal",
            "start_date": date.today(), "next_payment_date": date.today() + timedelta(days=7),
            "created_by": uid, "remaining": 8000, "remaining_capital": 8000,
            "total_interest_paid": 0, "status": "ACTIVO", "term_count": 10,
            "organization_id": ORG_ID, "total_interest": 2000, "total_to_pay": 12000,
            "upfront_percent": 0, "installment_amount": 1200,
            "signature_b64": None, "id_photo_b64": None, "id_photo_back_b64": None,
        }

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


def log_action(user_id, action, detail=""):
    store.audit_log.append({
        "id": store.nid("audit"), "user_id": user_id, "action": action, "detail": detail, "created_at": datetime.utcnow(),
    })


def ensure_org():
    session["org_id"] = session.get("org_id") or ORG_ID


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
    total = float(getattr(store, "starting_bank", 0.0) or 0.0)
    for cr in store.cash_reports.values():
        if cr.get("organization_id") == oid:
            total += float(cr.get("amount") or 0)
    # Evitar valores tipo -0.00 por redondeos.
    if abs(total) < 1e-9:
        total = 0.0
    return round(total, 2)


def apply_cash_movement(movement_type, amount, note, user_id=None, org_id=None):
    """
    Aplica un movimiento al banco (memoria) y valida que nunca quede negativo.
    movement_type: string informativo (deposito_banco, prestamo_entregado, descuento_inicial, pago_prestamo, gasto_ruta, etc.)
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
    store.cash_reports[rid] = {
        "id": rid,
        "user_id": user_id,
        "date": date.today(),
        "amount": round(amt, 2),
        "note": note or movement_type,
        "created_at": datetime.utcnow(),
        "organization_id": oid,
        "movement_type": movement_type,
    }
    return rid


def loan_ids_visible(org_id, user):
    return {L["id"] for L in loans_for_user(org_id, user)}


def payments_in_scope(org_id, user):
    lids = loan_ids_visible(org_id, user)
    return [p for p in store.payments.values() if p.get("loan_id") in lids]


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
        session["user_id"] = user["id"]
        session["role"] = user.get("role")
        session["org_id"] = user.get("organization_id") or ORG_ID
        try:
            log_action(user["id"], "login", "login")
        except Exception:
            pass
        flash(f"Bienvenido, {user['username']}", "success")
        return redirect(url_for("index"))
    return render_template_string(
        TPL_LOGIN, flashes=get_flashed_messages(with_categories=True), app_brand=APP_BRAND, admin_whatsapp=ADMIN_WHATSAPP
    )


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "success")
    return redirect(url_for("login"))


@app.route("/")
def index():
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
        "name": "JDM Cash Now", "short_name": "JDM Cash",
        "description": "Sistema en memoria (demo).",
        "start_url": "/dashboard", "scope": "/", "display": "standalone",
        "theme_color": "#16a34a", "background_color": "#ecfdf3",
    })


@app.route("/sw.js")
def service_worker():
    return send_from_directory("static", "sw.js", mimetype="application/javascript")


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
        f'<div class="daily-card r"><span class="dk">⚠️ Atrasados</span><span class="dv">{k["atrasados"]}</span></div>'
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
        + op_tile(url_for("bank_ranking"), "⚠️", "Ranking morosos", "", "")
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
    <p>Panel principal · datos en memoria</p>
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
    rows = "".join(
        f"<tr><td>{u['username']}</td><td>{u['role']}</td><td>{u.get('phone') or ''}</td>"
        f"""<td><form method="post" action="{url_for('delete_user', user_id=u['id'])}" style="display:inline" """
        f"""onsubmit="return confirm('¿Eliminar usuario?');"><button class="btn btn-secondary" type="submit">Borrar</button></form></td></tr>"""
        for u in store.users.values()
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
        if any(u["username"] == username for u in store.users.values()):
            flash("Usuario ya existe.", "danger")
            return redirect(url_for("new_user"))
        uid = store.nid("users")
        store.users[uid] = {
            "id": uid, "username": username, "password_hash": generate_password_hash(password),
            "role": role, "phone": phone, "organization_id": ORG_ID, "created_at": datetime.utcnow(), "name": None,
        }
        flash("Usuario creado.", "success")
        return redirect(url_for("users"))
    body = (
        f'<div class="card"><h2>Nuevo usuario</h2><form method="post">'
        f'<label>Usuario</label><input name="username" required>'
        f'<label>Contraseña</label><input type="password" name="password" required>'
        f'<label>Teléfono</label><input name="phone">'
        f'<label>Rol</label><select name="role"><option value="cobrador">Cobrador</option><option value="supervisor">Supervisor</option><option value="admin">Admin</option></select>'
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
    store.users.pop(user_id, None)
    flash("Usuario eliminado.", "success")
    return redirect(url_for("users"))


@app.route("/reassign", methods=["GET", "POST"])
@login_required
@role_required("admin", "supervisor")
def reassign_clients():
    if request.method == "POST":
        flash("Reasignación guardada en memoria (demo).", "info")
        return redirect(url_for("clients"))
    opts = "".join(f"<option value='{u['id']}'>{u['username']}</option>" for u in store.users.values() if u.get("role") == "cobrador")
    body = f'<div class="card"><h2>Reasignar rutas</h2><select>{opts}</select><p><a class="btn btn-secondary" href="{url_for("clients")}">Volver</a></p></div>'
    return page(body)


@app.route("/clients/<int:client_id>/reassign", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def reassign_single_client(client_id):
    flash("Cliente reasignado (memoria).", "success")
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
            "organization_id": ORG_ID,
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
    if user["role"] == "cobrador" and c.get("created_by") != user["id"] and not is_cartera_admin(user):
        flash("Sin acceso.", "danger")
        return redirect(url_for("clients"))
    org_id = session.get("org_id")
    sd = calc_client_score(client_id, org_id)
    mx = calc_max_credito(sd["prestamos_pagados"], sd["score"])
    score_html = f'<div class="card"><h3>Score</h3><p>{sd["score"]} — {sd["nivel"]}</p><p>Crédito sugerido: {fmt_money(mx)}</p></div>'
    loans = [L for L in store.loans.values() if L.get("client_id") == client_id]
    lr = "".join(
        f"<tr><td>#{L['id']}</td><td>{fmt_money(L.get('amount'))}</td><td>{L.get('status')}</td>"
        f"<td><a class='btn btn-secondary' href='{url_for('loan_detail', loan_id=L['id'])}'>Ver</a></td></tr>"
        for L in loans
    )
    body = (
        score_html
        + f'<div class="card"><h2>{c["first_name"]} {c.get("last_name") or ""}</h2>'
        + f"<p>{c.get('phone') or ''} · {c.get('route') or ''}</p>"
        + f'<div class="table-scroll"><table><tr><th>ID</th><th>Monto</th><th>Estado</th><th></th></tr>{lr}</table></div>'
        + f'<a class="btn btn-primary" href="{url_for("new_loan")}?client_id={client_id}">Nuevo préstamo</a> '
        + f'<a class="btn btn-secondary" href="{url_for("edit_client", client_id=client_id)}">Editar</a>'
        + f'<form method="post" action="{url_for("delete_client", client_id=client_id)}" style="margin-top:8px" onsubmit="return confirm(\'¿Borrar cliente?\');">'
        + f'<button class="btn btn-secondary" type="submit">Eliminar cliente</button></form></div>'
    )
    return page(body)


@app.route("/loans")
@login_required
def loans():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    rows = loans_for_user(org_id, user)
    t = "".join(
        f"<tr><td>#{L['id']}</td><td>{L.get('status')}</td><td>{fmt_money(L.get('remaining'))}</td>"
        f"<td><a class='btn btn-secondary' href='{url_for('loan_detail', loan_id=L['id'])}'>Ver</a></td></tr>"
        for L in sorted(rows, key=lambda x: -x["id"])
    )
    body = (
        f'<div class="card"><h2>Préstamos</h2><div class="table-scroll"><table><tr><th>ID</th><th>Estado</th><th>Saldo</th><th></th></tr>{t}</table></div>'
        f'<a class="btn btn-primary" href="{url_for("new_loan")}">Nuevo préstamo</a></div>'
    )
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
    store.loans.pop(loan_id, None)
    flash("Préstamo eliminado.", "success")
    return redirect(url_for("loans"))


@app.route("/loans/<int:loan_id>/edit", methods=["GET", "POST"])
@login_required
def edit_loan(loan_id):
    L = store.loans.get(loan_id)
    if not L:
        flash("No encontrado.", "danger")
        return redirect(url_for("loans"))
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
    <a class="btn btn-secondary" href="{url_for('edit_loan', loan_id=loan_id)}">✏️ Editar</a>
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

        # Registrar movimiento en el banco (siempre suma).
        try:
            apply_cash_movement(
                movement_type="pago_prestamo",
                amount=amt,
                note=f"Pago préstamo #{loan_id}",
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
            "status": "OK", "weeks_advanced": None,
        }
        rem = float(L.get("remaining") or 0) - amt
        L["remaining"] = max(0, rem)
        if L["remaining"] <= 0:
            L["status"] = "cerrado"
        # Si se registra una cuota, avanzamos la próxima fecha automáticamente.
        if str(typ or "").strip().lower() == "cuota":
            interval_days = freq_interval_days(L.get("frequency"))
            current_due = L.get("next_payment_date") or date.today()
            if hasattr(current_due, "strftime"):
                L["next_payment_date"] = current_due + timedelta(days=interval_days)
        flash("Pago registrado.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))
    body = (
        f'<div class="card"><h2>Pago — préstamo #{loan_id}</h2><form method="post">'
        f'<p style="margin:6px 0 10px 0; opacity:.95;"><b>Cuota recomendada:</b> {fmt_money(scheduled_payment)}</p>'
        f'<label>Monto</label><input name="amount" type="number" step="0.01" value="{scheduled_payment}" required>'
        f'<label>Tipo</label><select name="type"><option value="cuota">Cuota</option><option value="capital">Capital</option><option value="interes">Interés</option></select>'
        f'<button class="btn btn-primary" type="submit">Registrar</button></form></div>'
    )
    return page(body)


@app.route("/payment/<int:payment_id>/print")
@login_required
def print_payment(payment_id):
    p = store.payments.get(payment_id)
    if not p:
        return "Pago no encontrado", 404
    return f"<html><body><h1>Recibo pago #{payment_id}</h1><p>Monto {fmt_money(p.get('amount'))}</p></body></html>"


@app.route("/payment/delete/<int:payment_id>", methods=["POST"])
@login_required
def delete_payment(payment_id):
    store.payments.pop(payment_id, None)
    flash("Pago eliminado.", "info")
    return redirect(url_for("loans"))


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
    tiles = """
    <a href="/bank/daily-list" class="bank-tile blue">Lista diaria</a>
    <a href="/bank/expenses" class="bank-tile red">Gastos</a>
    <a href="/bank/late" class="bank-tile orange">Atrasos</a>
    <a href="/bank/legal" class="bank-tile purple">Legal</a>
    <a href="/bank/advance" class="bank-tile indigo">Adelantos</a>
    """
    adm = ""
    if user.get("role") == "admin":
        adm = f'<form method="post" action="{url_for("admin_clear_all")}" onsubmit="return confirm(\'¿Resetear datos?\');" style="margin-top:12px"><button class="btn btn-secondary" type="submit">Reiniciar datos demo</button></form>'
    body = (
        f'<h2 style="text-align:center">🏦 Banco</h2><style>'
        f'.bank-menu{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;align-items:stretch;padding:8px 4px 14px}}'
        f'.bank-tile{{display:flex;align-items:center;justify-content:center;text-align:center;padding:16px 12px;min-height:56px;border-radius:16px;color:#fff;font-weight:900;text-decoration:none;box-shadow:0 10px 24px rgba(0,0,0,.14);transition:transform .12s ease,opacity .12s ease;box-sizing:border-box}}'
        f'.bank-tile:hover{{transform:translateY(-2px);opacity:.98}}'
        f'.bank-tile:focus{{outline:2px solid rgba(255,255,255,.85);outline-offset:2px}}'
        f'.blue{{background:#4f8df7}} .red{{background:#f87171}} .orange{{background:#fb923c}} .purple{{background:#a855f7}} .indigo{{background:#6366f1}}'
        f'</style><div class="bank-menu">{tiles}</div>'
        f'<a class="btn btn-primary" style="display:block;margin-top:12px;text-align:center" href="{url_for("collector_map")}">Mapa cobrador</a>{adm}'
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
    rows = "".join(
        f"<tr><td>{a.get('created_at')}</td><td>{a.get('action')}</td><td>{a.get('detail')}</td></tr>"
        for a in store.audit_log[-200:]
    )
    return page(f'<div class="card"><h2>Auditoría</h2><div class="table-scroll"><table><tr><th>Fecha</th><th>Acción</th><th>Detalle</th></tr>{rows}</table></div></div>')


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
    pos = store.gps_positions.get(session.get("user_id"))
    extra = f"<p>Última posición: {pos}</p>" if pos else "<p>Sin posición (active GPS en el navegador).</p>"
    return page('<div class="card"><h2>Mapa cobrador</h2>' + extra + "</div>")


@app.route("/advance/delete/<int:payment_id>", methods=["POST"])
@login_required
def delete_advance(payment_id):
    store.payments.pop(payment_id, None)
    flash("Adelanto eliminado.", "info")
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
    cobrador = store.users.get(loan.get("created_by"), {}).get("username") or "—"

    def fmt_date(d):
        return d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(d)

    capital_aprobado = float(loan.get("amount") or 0)
    interes_total = float(loan.get("total_interest") or 0)
    start_date = loan.get("start_date")
    cedula = client.get("document_id") or "—"
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
</script>
"""

    body = f"""
<div class="card" style="padding:16px;">
  <h2 style="margin:0 0 10px 0;">Contrato de Préstamo</h2>
  <div style="opacity:.9; font-size:13px; margin-bottom:12px;">
    <div><b>Cliente:</b> {cliente_nombre}</div>
    <div><b>Cédula:</b> {cedula}</div>
    <div><b>Capital aprobado:</b> {fmt_money(capital_aprobado)}</div>
    <div><b>Interés total:</b> {fmt_money(interes_total)}</div>
    <div><b>Fecha inicio:</b> {fmt_date(start_date)}</div>
    <div><b>Cobrador:</b> {cobrador}</div>
  </div>

  <div style="margin:10px 0 16px 0;border-top:1px solid rgba(148,163,184,.35);padding-top:12px;">
    <h3 style="margin:0 0 8px 0;">📜 Compromiso de Pago</h3>
    <p style="margin:0 0 8px 0;">
      El cliente <b>{cliente_nombre}</b> reconoce haber recibido el capital del préstamo y se compromete de manera expresa, voluntaria e irrevocable a pagar la totalidad de la deuda a <b>JDM CASH NOW</b>, incluyendo capital, intereses, cargos y penalidades aplicables, en los plazos establecidos.
    </p>
    <p style="margin:0;">
      El incumplimiento de este compromiso autoriza a <b>JDM CASH NOW</b> a iniciar las acciones legales correspondientes conforme a la ley vigente.
    </p>
  </div>

  <div style="border-top:1px solid rgba(148,163,184,.35);padding-top:12px;">
    <h3 style="margin:0 0 10px 0;">Cédula del cliente</h3>
    <div style="display:flex;gap:14px;flex-wrap:wrap;align-items:flex-start;">
      <div style="flex:1;min-width:240px;">
        <div style="font-weight:900;margin-bottom:6px; font-size:13px;">Cédula (Frente)</div>
        <form method="post" enctype="multipart/form-data" action="{url_for('upload_id_front', loan_id=loan_id)}">
          <input name="id_front" type="file" accept="image/*" required>
          <button class="btn btn-primary" style="margin-top:8px;" type="submit">Subir frente</button>
        </form>
        {preview_front}
      </div>
      <div style="flex:1;min-width:240px;">
        <div style="font-weight:900;margin-bottom:6px; font-size:13px;">Cédula (Parte de atrás)</div>
        <form method="post" enctype="multipart/form-data" action="{url_for('upload_id_back', loan_id=loan_id)}">
          <input name="id_back" type="file" accept="image/*" required>
          <button class="btn btn-primary" style="margin-top:8px;" type="submit">Subir atrás</button>
        </form>
        {preview_back}
      </div>
    </div>
  </div>

  <div style="border-top:1px solid rgba(148,163,184,.35);padding-top:12px;margin-top:14px;">
    <h3 style="margin:0 0 10px 0;">Firma del cliente</h3>
    <form method="post" action="{url_for('sign_legal_document', loan_id=loan_id)}">
      <div style="background:#fff;border-radius:14px;border:1px solid rgba(0,0,0,.08);padding:10px;">
        <div style="opacity:.9;font-size:12px;margin-bottom:8px;">Firma (dibuje con el mouse/touch)</div>
        <canvas id="sigCanvas" style="width:100%;max-width:520px;height:120px;border:1px dashed rgba(0,0,0,.25);border-radius:12px;background:#ffffff;"></canvas>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px;">
          <button class="btn btn-secondary" type="button" onclick="clearSig()">Limpiar</button>
          <button class="btn btn-primary" type="submit">Guardar firma</button>
        </div>
      </div>
      <input type="hidden" name="signature_b64" id="signature_b64">
    </form>
    {preview_sig}
  </div>
</div>
{signature_script}
{nav_subfooter()}
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
    return stub_page("Adelantos de pago")


@app.route("/ruta/agregar-capital", methods=["POST"])
@login_required
def agregar_capital_ruta():
    flash("Capital agregado (memoria).", "success")
    return redirect(url_for("bank_home"))


@app.route("/bank/daily-list", methods=["GET", "POST"])
@login_required
def bank_daily_list():
    rows = loans_for_user(session.get("org_id"), current_user())
    t = "".join(f"<tr><td>#{L['id']}</td><td>{L.get('status')}</td><td>{fmt_money(L.get('remaining'))}</td></tr>" for L in rows[:50])
    return page(f'<div class="card"><h2>Lista diaria</h2><table><tr><th>ID</th><th>Estado</th><th>Saldo</th></tr>{t}</table></div>')


@app.route("/bank/expenses", methods=["GET", "POST"])
@login_required
def bank_expenses():
    ensure_org()
    oid = session.get("org_id")
    rows = "".join(
        f"<tr><td>{e.get('route') or '—'}</td><td>{e.get('amount') and fmt_money(e.get('amount')) or ''}</td>"
        f"<td>{e.get('note') or ''}</td><td>{e.get('created_at') or ''}</td>"
        f"<td><form method='post' action='{url_for('delete_route_expense', expense_id=e['id'])}' onsubmit=\"return confirm('¿Eliminar gasto?');\">"
        f"<button class='btn btn-secondary' type='submit'>Borrar</button></form></td></tr>"
        for e in reversed(list(store.route_expenses.values()))
        if e.get("organization_id") == oid
    )
    if not rows:
        rows = "<tr><td colspan=5 style='opacity:.85'>Sin gastos aún</td></tr>"
    body = (
        f'<div class="card"><h2>🧾 Gastos de ruta</h2>'
        f'<form method="post" action="{url_for("add_route_expense")}">'
        f'<label>Ruta</label><input name="route" placeholder="Ej. Ruta 1">'
        f'<label>Monto</label><input name="amount" type="number" step="0.01" required>'
        f'<label>Nota</label><input name="note" placeholder="Ej. Transporte / comida"></form>'
        f'<button class="btn btn-primary" type="submit">Registrar gasto</button>'
        f'<div class="table-scroll" style="margin-top:12px"><table><tr><th>Ruta</th><th>Monto</th><th>Nota</th><th>Fecha</th><th></th></tr>{rows}</table></div>'
        f"{nav_subfooter()}</div>"
    )
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
    return stub_page("Editar gasto")


@app.route("/route/expenses/new", methods=["POST"])
@login_required
def add_route_expense():
    ensure_org()
    org_id = session.get("org_id")
    route = (request.form.get("route") or "").strip()
    note = (request.form.get("note") or "").strip() or "Gasto de ruta"
    exp_amount = request.form.get("amount", type=float)
    if exp_amount is None or exp_amount <= 0:
        flash("Monto inválido.", "danger")
        return redirect(url_for("bank_expenses"))

    # Movimiento negativo del banco (gasto).
    try:
        cash_id = apply_cash_movement(
            movement_type="gasto_ruta",
            amount=-exp_amount,
            note=f"Gasto de ruta ({route or '—'})",
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
        "expense_type": "gasto_ruta",
        "amount": round(float(exp_amount), 2),
        "note": note,
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
    return stub_page("Entrega de efectivo")


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
    caja_global = float(getattr(store, "starting_bank", 0.0) or 0.0) + sum(
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
    return stub_page("Capital por ruta")


@app.route("/bank/late")
@login_required
def bank_late():
    ensure_org()
    user = current_user()
    org_id = session.get("org_id")
    today = date.today()
    late = [
        L for L in loans_for_user(org_id, user)
        if str(L.get("status", "")).upper() == "ACTIVO"
        and L.get("next_payment_date")
        and L["next_payment_date"] < today
    ]
    late.sort(key=lambda x: x.get("next_payment_date") or today)
    rows = ""
    for L in late:
        cid = L.get("client_id")
        c = store.clients.get(cid, {})
        nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip() or f"Cliente #{cid}"
        rows += (
            f"<tr><td>#{L['id']}</td><td>{nm}</td><td>{L.get('next_payment_date')}</td>"
            f"<td>{fmt_money(L.get('remaining'))}</td>"
            f"<td><a class='btn btn-secondary' href='{url_for('loan_detail', loan_id=L['id'])}'>Ver</a></td></tr>"
        )
    body = (
        f'<div class="card"><h2>⚠️ Préstamos atrasados</h2>'
        f'<div class="table-scroll"><table><tr><th>ID</th><th>Cliente</th><th>Vencía</th><th>Saldo</th><th></th></tr>'
        f"{rows or '<tr><td colspan=5>Sin atrasos</td></tr>'}</table></div>{nav_subfooter()}</div>"
    )
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
    today = date.today()
    days_to_sat = (5 - today.weekday()) % 7
    if days_to_sat == 0:
        days_to_sat = 7
    prox_sab = today + timedelta(days=days_to_sat)
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
            nm = f"{c.get('first_name','')} {c.get('last_name') or ''}".strip()
            rows.append((npd, L, nm))
    rows.sort(key=lambda x: x[0] or date.max)
    tr = ""
    for npd, L, nm in rows:
        tr += (
            f"<tr><td>{nm}</td><td>#{L['id']}</td><td>{npd}</td><td>{fmt_money(L.get('installment_amount'))}</td>"
            f"<td><a class='btn btn-secondary' href='{url_for('loan_detail', loan_id=L['id'])}'>Cobrar</a></td></tr>"
        )
    body = (
        f'<div class="card"><h2>💰 Cobro sábado</h2><p>Préstamos <b>semanales</b> con cuota hasta el próximo sábado ({prox_sab}).</p>'
        f'<div class="table-scroll"><table><tr><th>Cliente</th><th>Prést.</th><th>Próx. pago</th><th>Cuota est.</th><th></th></tr>'
        f"{tr or '<tr><td colspan=5>Ninguno programado para este ciclo</td></tr>'}</table></div>{nav_subfooter()}</div>"
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
    k = compute_financial_kpis(org_id, user)
    body = (
        f'<div class="card"><h2>📅 Cierre semanal</h2>'
        f"<p>Resumen actual (memoria) antes de cerrar:</p><ul>"
        f"<li>Cobrado hoy: {fmt_money(k['cobrado_hoy'])}</li>"
        f"<li>Total por cobrar: {fmt_money(k['total_por_cobrar'])}</li>"
        f"<li>Préstamos activos en vista: {k['n_activos']}</li></ul>"
        f'<form method="post" action="{url_for("cerrar_semana")}">'
        f'<label>Notas del cierre</label><textarea name="notas" rows="3" placeholder="Opcional"></textarea>'
        f'<button class="btn btn-primary" type="submit">Registrar cierre de semana</button></form>'
        f"{nav_subfooter()}</div>"
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
    rows = ""
    for rec in reversed(store.closure_history[-50:]):
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
    store.closure_history = [c for c in store.closure_history if c.get("id") != cierre_id]
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
            {"id": rid, "amount": amt, "note": note, "at": datetime.utcnow()}
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
    rows = "".join(
        f"<tr><td>{d.get('at')}</td><td>{fmt_money(d.get('amount'))}</td><td>{d.get('note') or '—'}</td></tr>"
        for d in reversed(store.deposit_history[-100:])
    )
    body = (
        f'<div class="card"><h2>Historial de depósitos</h2>'
        f'<div class="table-scroll"><table><tr><th>Fecha</th><th>Monto</th><th>Nota</th></tr>'
        f"{rows or '<tr><td colspan=3>Sin depósitos</td></tr>'}</table></div>{nav_subfooter()}</div>"
    )
    return page(body)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"[JDM Cash Now — memoria] http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
