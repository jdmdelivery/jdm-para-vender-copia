# -*- coding: utf-8 -*-
# JDM Cash Now — datos solo en memoria (sin base de datos). Se pierden al reiniciar el servidor.
from __future__ import annotations

import os
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
        "weekly_closures", "deposit_history", "_seq",
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
        self._seq = {"users": 1, "clients": 0, "loans": 0, "payments": 0, "arrears": 0, "cash": 0, "re": 0, "disc": 0, "cierre": 0, "audit": 0}
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
    L = loans_for_user(org_id, user)
    cap = sum(float(x.get("amount") or 0) for x in L)
    body = (
        f'<div class="card"><h2>📊 Dashboard</h2><p>Préstamos visibles: <b>{len(L)}</b></p>'
        f"<p>Capital (suma montos originales): <b>{fmt_money(cap)}</b></p>"
        f'<p><a class="btn btn-primary" href="{url_for("clients")}">Clientes</a> '
        f'<a class="btn btn-primary" href="{url_for("loans")}">Préstamos</a> '
        f'<a class="btn btn-secondary" href="{url_for("bank_home")}">Banco</a></p></div>'
    )
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
@role_required("admin", "supervisor")
def employees():
    rows = "".join(f"<tr><td>{u['username']}</td><td>{u['role']}</td></tr>" for u in store.users.values() if u.get("role") == "cobrador")
    return page(f'<div class="card"><h2>Empleados</h2><table><tr><th>Usuario</th><th>Rol</th></tr>{rows}</table></div>')


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
        freq = request.form.get("frequency") or "semanal"
        start_str = (request.form.get("start_date") or "").strip()
        term_count = request.form.get("term_count", type=int) or 1
        if not client_id or amount is None or not start_str:
            flash("Complete cliente, monto y fecha.", "danger")
            return redirect(url_for("new_loan"))
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        next_payment_date = start_date + timedelta(days=7)
        total_interest = round((amount * rate / 100) * term_count, 2)
        total_to_pay = round(amount + total_interest, 2)
        installment_amount = round(total_to_pay / max(term_count, 1), 2)
        lid = store.nid("loans")
        store.loans[lid] = {
            "id": lid, "client_id": client_id, "amount": amount, "rate": rate, "frequency": freq,
            "start_date": start_date, "next_payment_date": next_payment_date, "created_by": user["id"],
            "remaining": amount, "remaining_capital": amount, "total_interest_paid": 0,
            "status": "ACTIVO", "term_count": term_count, "organization_id": org_id,
            "total_interest": total_interest, "total_to_pay": total_to_pay, "upfront_percent": 0,
            "installment_amount": installment_amount,
            "signature_b64": None, "id_photo_b64": None, "id_photo_back_b64": None,
        }
        flash("Préstamo creado.", "success")
        return redirect(url_for("loan_detail", loan_id=lid))
    opts = "".join(f"<option value='{c['id']}'{' selected' if request.args.get('client_id', type=int)==c['id'] else ''}>{c['first_name']}</option>" for c in clist)
    body = (
        f'<div class="card"><h2>Nuevo préstamo</h2><form method="post">'
        f'<label>Cliente</label><select name="client_id" required>{opts}</select>'
        f'<label>Monto</label><input name="amount" type="number" step="0.01" required>'
        f'<label>Tasa %</label><input name="rate" type="number" step="0.01" value="10">'
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
    pays = [p for p in store.payments.values() if p.get("loan_id") == loan_id]
    pr = "".join(f"<tr><td>{p.get('date')}</td><td>{fmt_money(p.get('amount'))}</td><td>{p.get('type')}</td></tr>" for p in pays)
    body = (
        f'<div class="card"><h2>Préstamo #{loan_id}</h2><p>Estado: {L.get("status")} · Restante: {fmt_money(L.get("remaining"))}</p>'
        f'<div class="table-scroll"><table><tr><th>Fecha</th><th>Monto</th><th>Tipo</th></tr>{pr}</table></div>'
        f'<a class="btn btn-primary" href="{url_for("new_payment", loan_id=loan_id)}">Registrar pago</a> '
        f'<a class="btn btn-secondary" href="{url_for("edit_loan", loan_id=loan_id)}">Editar</a>'
        f'<form method="post" action="{url_for("delete_loan", loan_id=loan_id)}" style="display:inline;margin-left:8px" onsubmit="return confirm(\'¿Eliminar préstamo?\');">'
        f'<button class="btn btn-secondary" type="submit">Eliminar</button></form></div>'
    )
    return page(body)


@app.route("/payment/new/<int:loan_id>", methods=["GET", "POST"])
@login_required
def new_payment(loan_id):
    L = store.loans.get(loan_id)
    if not L:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))
    if request.method == "POST":
        amt = request.form.get("amount", type=float)
        typ = (request.form.get("type") or "cuota").strip()
        if amt is None or amt <= 0:
            flash("Monto inválido.", "danger")
            return redirect(url_for("new_payment", loan_id=loan_id))
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
        flash("Pago registrado.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))
    body = (
        f'<div class="card"><h2>Pago — préstamo #{loan_id}</h2><form method="post">'
        f'<label>Monto</label><input name="amount" type="number" step="0.01" required>'
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
        f'.bank-menu{{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}}'
        f'.bank-tile{{display:block;text-align:center;padding:16px;border-radius:16px;color:#fff;font-weight:800;text-decoration:none}}'
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
    body = (
        f'<div class="card"><h2>Reportes</h2>'
        f'<a class="btn btn-secondary" href="{url_for("reportes_cobradores")}">Por cobrador</a> '
        f'<a class="btn btn-secondary" href="{url_for("dashboard")}">Dashboard</a>'
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
    rows = [L for L in store.loans.values() if str(L.get("status", "")).lower() == "cerrado"]
    t = "".join(f"<tr><td>#{L['id']}</td><td>{fmt_money(L.get('amount'))}</td></tr>" for L in rows)
    return page(f'<div class="card"><h2>Préstamos cerrados</h2><table><tr><th>ID</th><th>Monto</th></tr>{t}</table></div>')


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
    return stub_page("Documento legal")


@app.route("/bank/legal/list")
@login_required
def bank_legal_list():
    return stub_page("Lista documentos legales")


@app.route("/bank/legal/view/<int:loan_id>")
@login_required
def view_legal_document(loan_id):
    return stub_page(f"Vista legal préstamo #{loan_id}")


@app.route("/bank/legal/upload-id-front/<int:loan_id>", methods=["POST"])
@login_required
def upload_id_front(loan_id):
    flash("Subida simulada (memoria).", "info")
    return redirect(url_for("view_legal_document", loan_id=loan_id))


@app.route("/bank/legal/upload-id-back/<int:loan_id>", methods=["POST"])
@login_required
def upload_id_back(loan_id):
    flash("Subida simulada (memoria).", "info")
    return redirect(url_for("view_legal_document", loan_id=loan_id))


@app.route("/bank/legal/sign/<int:loan_id>", methods=["GET", "POST"])
@login_required
def sign_legal_document(loan_id):
    if request.method == "POST":
        flash("Firma guardada en memoria.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))
    return stub_page(f"Firmar préstamo #{loan_id}")


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
    return stub_page("Gastos de ruta")


@app.route("/bank/expenses/delete/<int:expense_id>", methods=["POST"])
@login_required
def delete_route_expense(expense_id):
    store.route_expenses.pop(expense_id, None)
    flash("Gasto eliminado.", "info")
    return redirect(url_for("bank_expenses"))


@app.route("/bank/expenses/edit/<int:expense_id>", methods=["GET", "POST"])
@login_required
def edit_expense(expense_id):
    return stub_page("Editar gasto")


@app.route("/route/expenses/new", methods=["POST"])
@login_required
def add_route_expense():
    flash("Gasto registrado (memoria).", "success")
    return redirect(url_for("bank_expenses"))


@app.route("/bank/discount/delete/<int:discount_id>", methods=["POST"])
@login_required
def delete_discount(discount_id):
    flash("Descuento eliminado (memoria).", "info")
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
    return stub_page("Acta / descuento inicial")


@app.route("/bank/routes", methods=["GET", "POST"])
@login_required
def bank_routes_list():
    return stub_page("Capital por ruta")


@app.route("/bank/late")
@login_required
def bank_late():
    return stub_page("Atrasos")


@app.route("/bank/ranking")
@login_required
def bank_ranking():
    return stub_page("Ranking")


@app.route("/bank/credit-history")
@login_required
def credit_history():
    return stub_page("Historial de crédito")


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
    return page(f'<div class="card"><h2>Scores</h2><table><tr><th>Cliente</th><th>Score</th><th>Crédito sug.</th></tr>{t}</table></div>')


@app.route("/bank/check-client", methods=["GET", "POST"])
@login_required
def check_client():
    if request.method == "POST":
        return page('<div class="card"><h2>Consulta</h2><p>Sin base de datos: búsqueda no aplicable.</p></div>')
    return page('<div class="card"><h2>Consultar por cédula</h2><form method="post"><input name="cedula"><button class="btn btn-primary" type="submit">Buscar</button></form></div>')


@app.route("/bank/risk-clients")
@login_required
def risk_clients():
    return stub_page("Clientes riesgo")


@app.route("/bank/cobro-sabado")
@login_required
def cobro_sabado():
    return stub_page("Cobro sábado")


@app.route("/bank/resumen")
@login_required
def bank_resumen():
    return stub_page("Resumen banco")


@app.route("/bank/cierre-semanal", methods=["GET", "POST"])
@login_required
def cierre_semanal():
    return stub_page("Cierre semanal")


@app.route("/bank/cerrar-semana", methods=["POST"])
@login_required
def cerrar_semana():
    flash("Cierre simulado.", "info")
    return redirect(url_for("bank_resumen"))


@app.route("/bank/historial-cierres")
@login_required
def historial_cierres():
    return stub_page("Historial cierres")


@app.route("/bank/pagar/<int:loan_id>", methods=["POST"])
@login_required
def pagar_prestamo(loan_id):
    flash("Use «Registrar pago» en el detalle del préstamo.", "info")
    return redirect(url_for("loan_detail", loan_id=loan_id))


@app.route("/bank/borrar-cierre/<int:cierre_id>", methods=["POST"])
@login_required
def borrar_cierre(cierre_id):
    flash("Cierre eliminado (memoria).", "info")
    return redirect(url_for("historial_cierres"))


@app.route("/bank/agregar-dinero", methods=["GET", "POST"])
@login_required
def agregar_dinero_banco():
    if request.method == "POST":
        flash("Movimiento simulado.", "success")
        return redirect(url_for("bank_home"))
    return stub_page("Agregar dinero al banco")


@app.route("/bank/historial-depositos")
@login_required
def historial_depositos():
    return stub_page("Historial depósitos")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"[JDM Cash Now — memoria] http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
