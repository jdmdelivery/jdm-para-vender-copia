# -*- coding: utf-8 -*-
"""
Flask app for Banca
Deploy on Render with:
gunicorn app:app
"""
from flask import Flask, render_template_string, request, redirect, session, flash, url_for, jsonify
import re
import sqlite3
import os
import time
from urllib.parse import quote
# Defer psycopg2 import so worker starts faster for Render health check
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import traceback

import threading
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler

_db_init_lock = threading.Lock()

# Zona horaria República Dominicana (Render usa UTC)
TZ_RD = ZoneInfo("America/Santo_Domingo")
RD_TZ = TZ_RD  # alias

def ahora_rd():
    """Hora actual en República Dominicana."""
    return datetime.now(RD_TZ)
from werkzeug.security import generate_password_hash, check_password_hash

# =====================================================
# CONFIGURACIÓN — variable "app" requerida por gunicorn
# =====================================================
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))

# Session timeout: inactivity (handled in before_request) and max lifetime
INACTIVITY_TIMEOUT_SECONDS = int(os.environ.get("INACTIVITY_TIMEOUT", "600"))  # 10 min default
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=60)  # max session 1 hour

BANCA = "LA QUE NO FALLA"

# ===============================
# HEALTH CHECK (Render / Gunicorn)
# Responde 200 para que Render detecte el puerto HTTP
# ===============================
@app.route("/health")
@app.route("/healthz")
def health():
    """Return 200 OK for Render health check. No DB dependency."""
    return "OK", 200


@app.route("/test_resultados")
def test_resultados():
    """Alias: redirige a actualizar_resultados."""
    return redirect("/actualizar_resultados")


@app.route("/actualizar_resultados")
def actualizar_resultados():
    """Ejecuta el scraper de resultados y recarga /ganadores."""
    try:
        obtener_resultados_loteria()
        print(">>> Resultados actualizados manualmente")
    except Exception as e:
        print(">>> ERROR actualizando resultados:", e)
    return redirect("/ganadores")


@app.route("/pagar/<int:ticket_id>")
def pagar_ticket_redirect(ticket_id):
    """Redirige al flujo POST de pago (para enlaces antiguos)."""
    if session.get("u") is None and session.get("user") is None:
        return redirect("/")
    return redirect("/ganadores")


@app.route("/pagar_premio/<int:ticket_id>", methods=["POST"])
def pagar_premio_ticket(ticket_id):
    """Registra el pago del premio, guarda en caja e historial, redirige al recibo. Si falla, rollback y redirige a ganadores."""
    if session.get("u") is None and session.get("user") is None:
        return redirect("/")
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if os.environ.get("DATABASE_URL"):
            cur.execute(
                "SELECT id, COALESCE(premio, 0) AS premio, COALESCE(pagado, false) AS pagado FROM tickets WHERE id = %s",
                (ticket_id,),
            )
        else:
            cur.execute(
                "SELECT id, COALESCE(premio, 0) AS premio, COALESCE(pagado, 0) AS pagado FROM tickets WHERE id = ?",
                (ticket_id,),
            )
        row = cur.fetchone()
        if not row:
            c.close()
            return redirect("/ganadores")
        try:
            _p = row.get("premio") if hasattr(row, "get") else (row[1] if len(row) > 1 else None)
            premio = float(_p) if _p is not None else 0.0
        except (TypeError, ValueError):
            premio = 0.0
        try:
            _paid = row.get("pagado") if hasattr(row, "get") else (row[2] if len(row) > 2 else None)
            pagado = _paid
        except (TypeError, IndexError):
            pagado = False
        if pagado in (True, 1, "t", "true", "1"):
            c.close()
            return redirect(url_for("imprimir_pago", ticket_id=ticket_id))
        if premio <= 0:
            c.close()
            return redirect("/ganadores")
        usuario = session.get("u") or session.get("user") or "sistema"
        if os.environ.get("DATABASE_URL"):
            cur.execute(
                "UPDATE tickets SET pagado = TRUE, fecha_pago = NOW() WHERE id = %s AND (pagado IS NOT TRUE)",
                (ticket_id,),
            )
        else:
            cur.execute(
                "UPDATE tickets SET pagado = 1, fecha_pago = datetime('now') WHERE id = ? AND (COALESCE(pagado, 0) = 0)",
                (ticket_id,),
            )
        actualizado = cur.rowcount
        if actualizado and actualizado > 0:
            if os.environ.get("DATABASE_URL"):
                cur.execute(
                    """
                    INSERT INTO caja (tipo, descripcion, monto, fecha, usuario, ticket_id)
                    VALUES (%s, %s, %s, NOW(), %s, %s)
                    """,
                    ("pago_premio", "Pago premio ticket #" + str(ticket_id), premio, usuario, ticket_id),
                )
                try:
                    cur.execute(
                        "INSERT INTO pagos_premios (ticket_id, monto, cajero) VALUES (%s, %s, %s)",
                        (ticket_id, premio, usuario),
                    )
                except Exception:
                    pass
            else:
                cur.execute(
                    """
                    INSERT INTO caja (tipo, descripcion, monto, fecha, usuario, ticket_id)
                    VALUES (?, ?, ?, datetime('now'), ?, ?)
                    """,
                    ("pago_premio", "Pago premio ticket #" + str(ticket_id), premio, usuario, ticket_id),
                )
                try:
                    cur.execute(
                        "INSERT INTO pagos_premios (ticket_id, monto, cajero) VALUES (?, ?, ?)",
                        (ticket_id, premio, usuario),
                    )
                except Exception:
                    pass
        c.commit()
        c.close()
        return redirect(url_for("imprimir_pago", ticket_id=ticket_id))
    except Exception as e:
        try:
            c.rollback()
        except Exception:
            pass
        print("Error en pagar_premio_ticket:", type(e).__name__, repr(e))
        traceback.print_exc()
        try:
            c.close()
        except Exception:
            pass
        return redirect("/ganadores")


@app.route("/imprimir_pago/<int:ticket_id>")
def imprimir_pago(ticket_id):
    """Muestra el recibo de pago de premio (ticket térmico) y permite imprimir."""
    if session.get("u") is None and session.get("user") is None:
        return redirect("/")
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(
            _sql("SELECT id, premio, pagado, cajero, fecha_pago, created_at FROM tickets WHERE id = %s"),
            (ticket_id,),
        )
        ticket_row = cur.fetchone()
    except Exception as e:
        try:
            c.rollback()
        except Exception:
            pass
        try:
            c.close()
        except Exception:
            pass
        print("Error en imprimir_pago (consulta ticket):", e)
        return "Error al cargar el ticket", 500
    if not ticket_row:
        c.close()
        return "Ticket no encontrado", 404
    try:
        # Premio, cajero (vendedor), fecha de pago o de creación
        premio = float(
            (ticket_row.get("premio") if hasattr(ticket_row, "get") else ticket_row[1]) or 0
        )
        cajero_ticket = (ticket_row.get("cajero") if hasattr(ticket_row, "get") else (ticket_row[3] if len(ticket_row) > 3 else "")) or ""
        fecha_pago = ticket_row.get("fecha_pago") if hasattr(ticket_row, "get") else (ticket_row[4] if len(ticket_row) > 4 else None)
        created_at = ticket_row.get("created_at") if hasattr(ticket_row, "get") else (ticket_row[5] if len(ticket_row) > 5 else None)
        fecha_mostrar = fecha_pago or created_at or ahora_rd().strftime("%Y-%m-%d %H:%M")
        if hasattr(fecha_mostrar, "strftime"):
            fecha_mostrar = fecha_mostrar.strftime("%Y-%m-%d %H:%M") if fecha_mostrar else ""
        else:
            fecha_mostrar = str(fecha_mostrar)[:16] if fecha_mostrar else ""
        # Cajero que realizó el pago (último en pagos_premios) o el de sesión
        cajero_pago = session.get("u") or session.get("user") or cajero_ticket
        try:
            cur.execute(
                _sql("SELECT cajero FROM pagos_premios WHERE ticket_id = %s ORDER BY id DESC LIMIT 1"),
                (ticket_id,),
            )
            rp = cur.fetchone()
            if rp:
                cajero_pago = rp.get("cajero") if hasattr(rp, "get") else (rp[0] if rp else cajero_pago)
        except Exception:
            pass
        # Primera línea del ticket para lotería/jugada
        cur.execute(
            _sql("SELECT lottery, draw, play FROM ticket_lines WHERE ticket_id = %s LIMIT 1"),
            (ticket_id,),
        )
        line_row = cur.fetchone()
        lottery_display = "—"
        play_display = "Quiniela"
        if line_row:
            lot = line_row.get("lottery") if hasattr(line_row, "get") else (line_row[0] if len(line_row) > 0 else "")
            draw = line_row.get("draw") if hasattr(line_row, "get") else (line_row[1] if len(line_row) > 1 else "")
            play_display = line_row.get("play") if hasattr(line_row, "get") else (line_row[2] if len(line_row) > 2 else "Quiniela")
            lottery_display = (lot or "") + (" " + (draw or "").strip() if draw else "")
            if not lottery_display.strip():
                lottery_display = "—"
    except Exception as e:
        try:
            c.rollback()
        except Exception:
            pass
        try:
            c.close()
        except Exception:
            pass
        print("Error en imprimir_pago (datos):", e)
        return "Error al cargar datos del recibo", 500
    finally:
        try:
            c.close()
        except Exception:
            pass
    return render_template_string(
        r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width">
<style>
body{margin:0;padding:0;font-family:monospace;font-size:12px;line-height:1.3;text-align:left;background:#fff}
.ticket{width:80mm;margin:0;padding:12px;box-sizing:border-box}
.ticket pre{margin:0;padding:0;white-space:pre-wrap;font-family:monospace;font-size:12px}
.line{border-top:1px dashed #000;margin:8px 0}
.no-print{display:block}
@media print{
  body{margin:0;padding:0}
  .ticket{position:absolute;top:0;left:0;width:80mm;margin:0;padding:12px}
  .no-print{display:none !important}
}
</style>
</head>
<body>
<div class="no-print" style="text-align:center;padding:20px">
  <a href="/ganadores" style="display:inline-block;padding:10px 20px;background:#22c55e;color:#fff;text-decoration:none;border-radius:8px">← Volver a Ganadores</a>
  <br><br>
  <button onclick="window.print()" style="padding:10px 24px;background:#000;color:#fff;border:none;border-radius:8px;cursor:pointer">🖨 Imprimir recibo</button>
</div>
<div class="ticket">
<pre>------------------------------
      PAGO DE PREMIO
      LA QUE NO FALLA
------------------------------
Ticket: {{ ticket_id }}
Lotería: {{ lottery_display }}
Jugada: {{ play_display }}
Premio: RD$ {{ "%.2f"|format(premio) }}

Cajero: {{ cajero_pago }}
Fecha: {{ fecha_mostrar }}

*** PREMIO PAGADO ***
------------------------------
</pre>
</div>
<script>
setTimeout(function(){ window.onload = function(){ } }, 100);
</script>
</body>
</html>
""",
        ticket_id=ticket_id,
        lottery_display=lottery_display,
        play_display=play_display or "Quiniela",
        premio=premio,
        cajero_pago=cajero_pago or "—",
        fecha_mostrar=fecha_mostrar,
    )


# ===============================
# Crear primer SUPER_ADMIN solo si la tabla users está vacía (patrón profesional)
# ===============================
@app.route("/create-admin")
def create_admin():
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    creado = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        row = cur.fetchone()
        total = list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)
        if total == 0:
            cur.execute(
                _sql("INSERT INTO users (username, password_hash, role, created_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)"),
                ("admin", generate_password_hash("admin"), ROLE_SUPER_ADMIN)
            )
            conn.commit()
            creado = True
        cur.close()
    finally:
        conn.close()
    return "Super Admin creado (admin/admin). Inicie sesión y use el panel para crear más usuarios." if creado else "Ya existen usuarios. Use el panel para crear un admin.", 200


# ===============================
# ABREVIACIONES
# ===============================
ABR_PLAY = {
    "Quiniela": "QL",
    "Pale": "PL",
    "Tripleta": "TP",
    "Super Pale": "SP"
}

ABR_LOTERIA = {
    "La Primera": "P",
    "LoteDom": "LD",
    "La Suerte Dominicana": "SD",
    "Lotería Real": "LR",
    "Lotería Nacional": "LN",
    "Loteka": "LT",
    "Leidsa": "LE",
    "Anguila": "AN",
    "King Lottery": "KL",
    "Florida": "FL",
    "New York": "NY"
}

def abr_play(nombre):
    return ABR_PLAY.get(nombre, nombre[:2].upper())

def abr_loteria(nombre):
    return ABR_LOTERIA.get(nombre, nombre[:2].upper())


# ===============================
# RESULTADOS AUTOMÁTICOS (CONECTATE)
# La página usa <span> con clase "score" para los números (círculos).
# Recorremos todos los span en orden; al detectar un nombre de quiniela,
# tomamos los próximos 3 números de dos dígitos.
# ===============================

# Quinielas que buscamos en Conectate → nombre para guardar en resultados (único por sorteo para no pisar)
# Multi-draw: guardamos con nombre completo (ej. "Anguila 10:00 AM") para tener un registro por sorteo.
QUINIELAS_CONECTATE = {
    "Lotería Nacional": "Lotería Nacional",
    "Gana Más": "Nacional Tarde (Gana Más)",
    "Quiniela Leidsa": "Leidsa",
    "Quiniela Real": "Quiniela Real",
    "Quiniela Loteka": "Loteka",
    "New York 3:30": "New York 2:30",
    "New York 11:30": "New York 11:30",
    "Florida Día": "Florida Día",
    "Florida Noche": "Florida Noche",
    "La Primera Día": "La Primera Día",
    "Primera Noche": "Primera Noche",
    "La Suerte MD": "La Suerte Dominicana",
    "La Suerte 6PM": "La Suerte Dominicana",
    "LoteDom": "LoteDom",
    "King Lottery 12:30": "King Lottery 12:30",
    "King Lottery 7:30": "King Lottery 7:30",
    "Anguila 10:00 AM": "Anguila 10:00 AM",
    "Anguila 1:00 PM": "Anguila 1:00 PM",
    "Anguila 6:00 PM": "Anguila 6:00 PM",
    "Anguila 9:00 PM": "Anguila 9:00 PM",
}

# Para calcular_ganadores: resultado en BD → (lottery, draw) para buscar en ticket_lines.
# Si no está, se usa (lottery, None) y se filtra solo por lottery.
RESULT_LOTTERY_TO_TICKET = {
    "Nacional Tarde (Gana Más)": ("Nacional Tarde (Gana Más)", "2:30 PM"),
    "Anguila 10:00 AM": ("Anguila", "10:00 AM"),
    "Anguila 1:00 PM": ("Anguila", "1:00 PM"),
    "Anguila 6:00 PM": ("Anguila", "6:00 PM"),
    "Anguila 9:00 PM": ("Anguila", "9:00 PM"),
    "New York 3:30": ("New York", "2:30 PM"),
    "New York 11:30": ("New York", "10:30 PM"),
    "Florida Día": ("Florida", "1:30 PM"),
    "Florida Noche": ("Florida", "10:30 PM"),
    "La Primera Día": ("La Primera", "12:00 PM"),
    "Primera Noche": ("La Primera", "8:00 PM"),
    "King Lottery 12:30": ("King Lottery", "12:30 PM"),
    "King Lottery 7:30": ("King Lottery", "7:30 PM"),
}


def _parse_conectate_resultados():
    """
    Descarga y parsea los resultados de https://www.conectate.com.do/loterias/
    Busca bloques con nombres de quinielas y los próximos 3 números (span con 2 dígitos).
    Devuelve dict: { "Lotería Nacional": ("79","07","78"), "Leidsa": ("00","59","76"), ... }
    """
    url = "https://www.conectate.com.do/loterias/"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print("Error descargando resultados conectate:", e)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    # Todos los <span> en orden del documento (incluyen nombres y números)
    all_spans = soup.find_all("span")
    resultados = {}
    i = 0
    while i < len(all_spans):
        text = all_spans[i].get_text(strip=True)
        if text in QUINIELAS_CONECTATE:
            lottery_db = QUINIELAS_CONECTATE[text]
            # Leer los próximos 3 números de dos dígitos (regex r'^\d{2}$')
            nums = []
            j = i + 1
            while j < len(all_spans) and len(nums) < 3:
                num_text = all_spans[j].get_text(strip=True)
                if re.match(r"^\d{2}$", num_text):
                    nums.append(num_text.zfill(2))
                    j += 1
                else:
                    j += 1
            if len(nums) == 3:
                resultados[lottery_db] = (nums[0], nums[1], nums[2])
                print("Resultado encontrado:", lottery_db, nums[0], nums[1], nums[2])
            i = j
        else:
            i += 1
    return resultados


def obtener_resultados_loteria():
    """
    - Lee resultados desde Conectate.
    - Inserta en tabla resultados si no existe registro para fecha+lottery.
    """
    print("Buscando resultados en Conectate...")
    data = _parse_conectate_resultados()
    if not data:
        print(">>> Scraper: no se obtuvieron datos de Conectate")
        return

    c = db()
    if not c:
        print(">>> Scraper: no hay conexión a BD")
        return
    try:
        cur = c.cursor()
        # Fecha de hoy en RD (solo día)
        hoy = ahora_rd().date().strftime("%Y-%m-%d")
        draw_empty = ""
        for key, (r1, r2, r3) in data.items():
            lottery_db, draw_db = RESULT_LOTTERY_TO_TICKET.get(key, (key, None))
            draw_val = draw_db if draw_db else draw_empty
            draw_param = draw_val if draw_val else None
            cur.execute(
                _sql("""
                    SELECT primero, segundo, tercero
                    FROM resultados
                    WHERE lottery = %s AND COALESCE(draw, '') = COALESCE(%s, '') AND fecha = %s
                """),
                (lottery_db, draw_val, hoy),
            )
            existe = cur.fetchone()
            if not existe:
                cur.execute(
                    _sql("""
                        INSERT INTO resultados (lottery, draw, primero, segundo, tercero, fecha)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """),
                    (lottery_db, draw_param, r1, r2, r3, hoy),
                )
                print("Resultado insertado:", lottery_db, draw_val or "", r1, r2, r3)
            else:
                p1 = existe.get("primero") if hasattr(existe, "get") else (existe[0] if len(existe) > 0 else None)
                p2 = existe.get("segundo") if hasattr(existe, "get") else (existe[1] if len(existe) > 1 else None)
                p3 = existe.get("tercero") if hasattr(existe, "get") else (existe[2] if len(existe) > 2 else None)
                if (str(p1), str(p2), str(p3)) != (str(r1), str(r2), str(r3)):
                    cur.execute(
                        _sql("""
                            UPDATE resultados
                            SET primero = %s, segundo = %s, tercero = %s
                            WHERE lottery = %s AND COALESCE(draw, '') = COALESCE(%s, '') AND fecha = %s
                        """),
                        (r1, r2, r3, lottery_db, draw_val, hoy),
                    )
                    print("Resultado actualizado:", lottery_db, draw_val or "", r1, r2, r3)

        c.commit()
    except Exception as e:
        print("Error guardando resultados automáticos:", e)
        try:
            c.rollback()
        except Exception:
            pass
    finally:
        c.close()
    # Calcular ganadores después de insertar resultados
    calcular_ganadores()


def calcular_ganadores():
    """
    Cruza ticket_lines con resultados por lottery y draw, detecta líneas ganadoras
    (number = primero|segundo|tercero), calcula premios y marca tickets ganadores.
    """
    c = db()
    if not c:
        return
    # Fecha de hoy en RD: solo tickets y resultados de este día pueden emparejar
    hoy = ahora_rd().date().strftime("%Y-%m-%d")
    try:
        cur = c.cursor()
        # Solo tickets de hoy + solo resultados de hoy (mismo lottery y draw)
        if os.environ.get("DATABASE_URL"):
            # PostgreSQL: usar "hoy en RD" en el SQL para no mezclar con resultados de ayer
            cur.execute(
                """
                SELECT
                    tl.ticket_id,
                    tl.number,
                    tl.play,
                    tl.amount,
                    tl.lottery,
                    tl.draw,
                    r.primero,
                    r.segundo,
                    r.tercero
                FROM ticket_lines tl
                JOIN tickets tk ON tk.id = tl.ticket_id
                JOIN resultados r
                    ON tl.lottery = r.lottery
                    AND COALESCE(r.draw, '') = COALESCE(tl.draw, '')
                    AND (r.fecha::date) = (NOW() AT TIME ZONE 'America/Santo_Domingo')::date
                WHERE (tk.created_at AT TIME ZONE 'America/Santo_Domingo')::date = (NOW() AT TIME ZONE 'America/Santo_Domingo')::date
                AND COALESCE(tl.estado,'activo') != 'cancelado'
                AND (tl.number = r.primero OR tl.number = r.segundo OR tl.number = r.tercero)
                """
            )
        else:
            cur.execute(
                _sql(
                    """
                    SELECT
                        tl.ticket_id,
                        tl.number,
                        tl.play,
                        tl.amount,
                        tl.lottery,
                        tl.draw,
                        r.primero,
                        r.segundo,
                        r.tercero
                    FROM ticket_lines tl
                    JOIN tickets tk ON tk.id = tl.ticket_id
                    JOIN resultados r
                        ON tl.lottery = r.lottery
                        AND COALESCE(r.draw, '') = COALESCE(tl.draw, '')
                        AND r.fecha = %s
                    WHERE DATE(tk.created_at) = %s
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    AND (tl.number = r.primero OR tl.number = r.segundo OR tl.number = r.tercero)
                    """
                ),
                (hoy, hoy),
            )
        filas = cur.fetchall() or []
        ticket_premio = {}
        for row in filas:
            tid = row["ticket_id"] if hasattr(row, "keys") else row[0]
            numero = row["number"] if hasattr(row, "keys") else row[1]
            play = row["play"] if hasattr(row, "keys") else row[2]
            monto = row["amount"] if hasattr(row, "keys") else row[3]
            r1 = row["primero"] if hasattr(row, "keys") else row[6]
            r2 = row["segundo"] if hasattr(row, "keys") else row[7]
            r3 = row["tercero"] if hasattr(row, "keys") else row[8]
            premio = calcular_premio(play, numero, monto, r1, r2, r3)
            if premio > 0:
                ticket_premio[tid] = ticket_premio.get(tid, 0) + premio
                print("Ticket ganador:", tid, premio)
        for tid, total in ticket_premio.items():
            cur.execute(
                _sql("UPDATE tickets SET ganador = true, premio = %s WHERE id = %s"),
                (total, tid),
            )
        c.commit()
    except Exception as e:
        print("Error en calcular_ganadores:", e)
        try:
            c.rollback()
        except Exception:
            pass
    finally:
        c.close()


# =====================================================
# SCHEDULER RESULTADOS (producción: Gunicorn; local: también)
# Solo se inicia una vez al cargar el módulo.
# Con WEB_CONCURRENCY=1 (default en Render) hay un solo worker y un solo scheduler.
# =====================================================
_scheduler_resultados_started = False

def _iniciar_scheduler_resultados():
    global _scheduler_resultados_started
    if _scheduler_resultados_started:
        return
    _scheduler_resultados_started = True
    try:
        sched = BackgroundScheduler()
        sched.add_job(obtener_resultados_loteria, "interval", minutes=5)
        sched.start()
        print(">>> Scheduler de resultados iniciado (cada 5 minutos)")
    except Exception as e:
        print(">>> No se pudo iniciar scheduler de resultados:", e)


# No iniciar scheduler al importar: Render requiere /health en <5s.
# Se inicia en el primer request (excepto /health) desde before_request.


# =====================================================
# SORTEO APLICA HOY (por día de la semana)
# Domingo (6): solo sorteos que contienen "Domingos"
# Lunes-Sábado (0-5): excluir sorteos que contienen "Domingos"
# Leidsa: Lun-Sáb 8:55 PM | Dom "Domingos 3:55 PM" (cierre 3:45 PM)
# Lotería Nacional: Lun-Sáb 2:30 PM, 8:50 PM | Dom "Domingos 6:00 PM" (cierre 5:50 PM)
# New York: 2:30 PM y 10:30 PM (cierre 2:20 PM y 10:20 PM).
# Florida Día: 1:30 PM (cierre 1:20 PM) | Florida Noche: 10:30 PM (cierre 10:20 PM). (No-domingos cierran 10 min antes).
# =====================================================
def sorteo_aplica_hoy(draw):
    """True si el sorteo aplica al día actual (según hora RD)."""
    if not draw:
        return False
    ahora = ahora_rd()
    es_domingo = ahora.weekday() == 6  # 0=Mon, 6=Sunday
    tiene_domingos = "domingos" in (draw or "").lower()
    # En domingo: deben aplicar tanto sorteos normales como los marcados "Domingos ...".
    # Entre lunes-sábado: excluir los sorteos marcados "Domingos ...".
    if es_domingo:
        return True
    return not tiene_domingos


# =====================================================
# APERTURA 7:00 AM / CIERRE ANTES DEL SORTEO (RD)
# Zona horaria: America/Santo_Domingo (ahora_rd()). Estado dinámico según hora actual en Santo Domingo.
# Regla:
# - Domingos: Nacional y Leidsa (sorteos "Domingos ...") cierran a su hora exacta (sin restar minutos).
# - Todo lo demás: cierra 10 minutos antes de su propio sorteo.
# =====================================================
SALES_CLOSED_MESSAGE = "Ventas cerradas. Las jugadas se cierran 10 minutos antes del sorteo (Domingos: Nacional y Leidsa cierran a su hora)."


def _parse_draw_time(s):
    """Extrae hora y minuto de '3:55 PM', 'Domingos 6:00 PM', etc. Devuelve (h24, min) o None."""
    if not s:
        return None
    s = str(s).strip()
    if s.lower().startswith("domingos "):
        s = s[9:].strip()
    m = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)", s, re.I)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    pm = m.group(3).upper() == "PM"
    if pm and h < 12:
        h += 12
    elif not pm and h == 12:
        h = 0
    return (h, mi)


def loteria_cerrada_para_venta(lottery, draw):
    """True si la lotería está cerrada: antes de 7:00 AM RD, o según regla de cierre (Domingos especial)."""
    if not draw:
        return True
    ahora = ahora_rd()
    hoy = ahora.date()

    # 1) Apertura automática: todas abren a 7:00 AM
    apertura = ahora.replace(hour=7, minute=0, second=0, microsecond=0)
    if ahora < apertura:
        return True

    # 2) El sorteo debe aplicar hoy (Lun-Sáb vs Dom)
    if not sorteo_aplica_hoy(draw):
        return True

    parsed = _parse_draw_time(draw)
    if not parsed:
        return False
    hora_draw, min_draw = parsed
    draw_today = datetime(hoy.year, hoy.month, hoy.day, hora_draw, min_draw, 0, tzinfo=TZ_RD)
    # Domingos especial: solo Nacional/Leidsa con sorteo "Domingos ..." cierran a la hora exacta.
    # Todo lo demás cierra 10 minutos antes de su propio sorteo.
    es_sorteo_domingo = "domingos" in (str(draw) or "").lower()
    lot_norm = (str(lottery or "")).strip().lower()
    es_nacional = "nacional" in lot_norm
    es_leidsa = "leidsa" in lot_norm
    minutos_cierre = 0 if (es_sorteo_domingo and (es_nacional or es_leidsa)) else 10
    close_time = draw_today - timedelta(minutes=minutos_cierre)
    return ahora >= close_time


def loteria_disponible_para_venta(lottery, draw):
    """True si la lotería está abierta para venta."""
    return not loteria_cerrada_para_venta(lottery, draw)


def venta_cerrada_respuesta(lottery=None, draw=None):
    """Devuelve el dict de error estándar cuando las ventas están cerradas (para APIs/JSON)."""
    return {"error": "⚠️ " + SALES_CLOSED_MESSAGE}


def estado_loteria(lottery, draw):
    """Estado: 'cerrada' antes de 7:00 AM o cuando aplica la regla de cierre; sino 'abierta'."""
    if not draw:
        return "abierta"
    ahora = ahora_rd()
    apertura = ahora.replace(hour=7, minute=0, second=0, microsecond=0)
    if ahora < apertura:
        return "cerrada"
    if not sorteo_aplica_hoy(draw):
        return "abierta"
    return "cerrada" if loteria_cerrada_para_venta(lottery, draw) else "abierta"


def draw_ya_paso_hoy(lottery, draw):
    """
    True si el sorteo ya ocurrió hoy (hora RD).
    Usado para ocultar jugadas de la vista activa (Ticket en vivo, detalle ticket);
    los registros siguen en BD para historial y reportes.
    """
    if not draw:
        return True
    if not sorteo_aplica_hoy(draw):
        return True
    parsed = _parse_draw_time(draw)
    if not parsed:
        return False
    ahora = ahora_rd()
    hoy = ahora.date()
    h, mi = parsed
    draw_today = datetime(hoy.year, hoy.month, hoy.day, h, mi, 0, tzinfo=TZ_RD)
    return ahora >= draw_today


# =====================================================
# CONEXION DB (Render: DATABASE_URL → PostgreSQL)
# =====================================================
def _ph():
    """Placeholder: %s para PostgreSQL, ? para SQLite."""
    return "%s" if os.environ.get("DATABASE_URL") else "?"

def _sql(q):
    """Convierte %s a ? para SQLite."""
    return q.replace("%s", _ph()) if isinstance(q, str) else q


# =====================================================
# SISTEMA DE ROLES (super_admin, admin, user/cajero)
# =====================================================
ROLE_SUPER_ADMIN = "super_admin"
ROLE_ADMIN = "admin"
ROLE_SUPERVISOR = "supervisor"
ROLE_COLLECTOR = "collector"
ROLE_CAJERO = "cajero"
ROLE_USER = "user"  # Cajero: solo este rol (y cajero legacy) puede acceder a /venta

# Quien puede ver dashboard admin, reportes, crear usuarios (según rol), etc.
ROLES_ADMIN = (ROLE_SUPER_ADMIN, ROLE_ADMIN)
# Solo estos roles pueden vender tickets en /venta. Admin (banca) no vende.
ROLES_CAN_SELL = (ROLE_SUPER_ADMIN, ROLE_USER, ROLE_CAJERO)
# Quien puede usar la app (venta para cajeros; dashboard/reportes para admin). Incluye todos los roles.
ROLES_STAFF = (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_SUPERVISOR, ROLE_COLLECTOR, ROLE_CAJERO, ROLE_USER)
# Roles que pueden tener meta diaria (cajeros/cobradores)
ROLES_CON_META = (ROLE_COLLECTOR, ROLE_CAJERO, ROLE_USER, ROLE_SUPERVISOR)


def _current_role():
    return (session.get("role") or "").strip().lower()


def is_super_admin():
    return _current_role() == ROLE_SUPER_ADMIN


def is_admin_or_super():
    return _current_role() in ROLES_ADMIN


def is_staff():
    return _current_role() in ROLES_STAFF


def can_create_role(role_to_create):
    """Indica si el usuario actual puede crear un usuario con el rol dado."""
    me = _current_role()
    if me == ROLE_SUPER_ADMIN:
        return role_to_create in (ROLE_ADMIN, ROLE_USER, ROLE_CAJERO, ROLE_SUPERVISOR, ROLE_COLLECTOR, ROLE_SUPER_ADMIN)
    if me == ROLE_ADMIN:
        return role_to_create in (ROLE_USER, ROLE_CAJERO, ROLE_SUPERVISOR, ROLE_COLLECTOR)
    return False


def _role_required(allowed_roles, redirect_path="/"):
    """Decorator factory: exige que session['role'] esté en allowed_roles."""
    def decorator(f):
        from functools import wraps
        @wraps(f)
        def wrapped(*args, **kwargs):
            if _current_role() not in allowed_roles:
                if not session.get("u") and not session.get("user"):
                    return redirect("/")
                return redirect(redirect_path)
            return f(*args, **kwargs)
        return wrapped
    return decorator


def super_admin_required(f):
    """Solo SUPER_ADMIN puede acceder."""
    return _role_required((ROLE_SUPER_ADMIN,), redirect_path="/venta")(f)


def admin_required(f):
    """ADMIN o SUPER_ADMIN pueden acceder (panel admin, crear usuarios, etc.)."""
    return _role_required(ROLES_ADMIN, redirect_path="/venta")(f)


def staff_required(f):
    """Cualquier rol de staff (super_admin, admin, supervisor, collector, cajero) puede acceder."""
    return _role_required(ROLES_STAFF, redirect_path="/")(f)


def check_banca_suspension():
    """Si el usuario es admin (banca) y está suspendido o con pago vencido, devuelve redirect a /banca_suspendida. Sino None."""
    if session.get("role") != ROLE_ADMIN:
        return None
    username = session.get("u") or session.get("user")
    if not username:
        return None
    conn = db()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(_sql("""
            SELECT banca_status, banca_due_date FROM users WHERE username = %s AND role = %s
        """), (username, ROLE_ADMIN))
        row = cur.fetchone()
        if not row:
            return None
        status = (row.get("banca_status") if hasattr(row, "get") else (row[0] if row else None)) or "active"
        due_raw = row.get("banca_due_date") if hasattr(row, "get") else (row[1] if len(row) > 1 else None)
        hoy = ahora_rd().date()
        if due_raw:
            try:
                if isinstance(due_raw, date):
                    due_date = due_raw
                elif hasattr(due_raw, "date"):
                    due_date = due_raw.date()
                else:
                    due_date = datetime.strptime(str(due_raw)[:10], "%Y-%m-%d").date()
                if hoy > due_date:
                    cur.execute(_sql("UPDATE users SET banca_status = %s WHERE username = %s AND role = %s"), ("suspended", username, ROLE_ADMIN))
                    conn.commit()
                    status = "suspended"
            except (ValueError, TypeError):
                pass
        if status != "active":
            return redirect("/banca_suspendida")
        return None
    finally:
        conn.close()


def db():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        try:
            DB = os.environ.get("SQLITE_DB", "banca.db")
            conn = sqlite3.connect(DB, timeout=60, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            print("DB ERROR:", e)
            return None
    try:
        import psycopg2
        from psycopg2 import extras
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        # Timeout 25s para que Render no cuelgue si la BD está en cold start
        connect_timeout = int(os.environ.get("DB_CONNECT_TIMEOUT", "25"))
        return psycopg2.connect(
            database_url,
            cursor_factory=extras.RealDictCursor,
            connect_timeout=connect_timeout,
        )
    except Exception as e:
        print("DB ERROR:", e)
        return None

# =====================================================
# DEFAULT ADMIN (si no hay usuarios — permite login admin/admin)
# =====================================================
def ensure_default_admin():
    """Create super_admin (admin/admin) if users table is empty. Uses same hash as login."""
    conn = db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        row = cur.fetchone()
        n = list(row.values())[0] if hasattr(row, "values") else row[0]
        if n == 0:
            cur.execute(_sql("""
                INSERT INTO users (username, password_hash, role, created_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """), ("admin", generate_password_hash("admin"), ROLE_SUPER_ADMIN))
            conn.commit()
        cur.close()
    except Exception as e:
        print("ensure_default_admin:", e)
    finally:
        conn.close()


# =====================================================
# INIT DB SOLO EN PRIMER REQUEST (no bloquea arranque Render)
# Health check must NOT run DB init or Render will timeout.
# =====================================================
@app.before_request
def init_once():
    if request.path in ("/health", "/healthz"):
        return None  # skip DB init for health check so Render gets 200 fast

    # Evitar arrancar scheduler en requests públicos iniciales (Render port-scan / login),
    # para no meter carga extra durante el arranque.
    if request.method in ("GET", "HEAD") and request.path in ("/", "/login"):
        rutas_sin_auth = ("/", "/login", "/create-admin")
        if request.path in rutas_sin_auth:
            # Solo en GET/HEAD a / o /login: responder al instante y calentar BD en segundo plano
            if request.method in ("GET", "HEAD") and request.path in ("/", "/login"):
                def _warm_db():
                    with _db_init_lock:
                        if not getattr(app, "db_ready", False):
                            try:
                                init_db()
                                init_config()
                                app.db_ready = True
                                print("DB INIT OK (background)")
                            except Exception as e:
                                print("DB INIT (background):", e)
                        if not getattr(app, "default_admin_checked", False):
                            try:
                                ensure_default_admin()
                                app.default_admin_checked = True
                            except Exception as e:
                                print("ensure_default_admin (background):", e)
                        # Iniciar scheduler solo cuando BD esté lista
                        try:
                            _iniciar_scheduler_resultados()
                        except Exception as e:
                            print("scheduler (background):", e)
                t = threading.Thread(target=_warm_db, daemon=True)
                t.start()
                return None
        # fallthrough

    # Rutas públicas: no exigir sesión. (GET/HEAD / y /login ya retornaron arriba con warm_db.)
    rutas_sin_auth = ("/", "/login", "/create-admin")
    if request.path in rutas_sin_auth:
        # POST / (login) o /create-admin: sí ejecutar init para que existan tablas
        pass

    # Crear tablas en el primer request (no en GET / ni GET /login para no bloquear la pantalla de login)
    if not hasattr(app, "db_ready"):
        try:
            init_db()
            init_config()
            app.db_ready = True
            print("DB INIT OK")
        except Exception as e:
            print("DB ERROR:", e)
    if not getattr(app, "default_admin_checked", False):
        try:
            ensure_default_admin()
        except Exception as e:
            print("ensure_default_admin:", e)
        app.default_admin_checked = True

    # Iniciar scheduler una sola vez, después de que la BD esté lista.
    try:
        _iniciar_scheduler_resultados()
    except Exception as e:
        print("scheduler:", e)

    if request.path in rutas_sin_auth:
        return None

    # Si no hay sesión, redirigir a login (protección de todas las rutas protegidas)
    if not session.get("u") and not session.get("user"):
        return redirect("/")

    # Control de pago: si es banca (admin) suspendida o con due_date vencido → bloquear (salvo en página de suspendida y logout)
    if request.path not in ("/banca_suspendida", "/logout"):
        r = check_banca_suspension()
        if r is not None:
            return r

    # Solo cajeros (user/cajero) y super_admin pueden acceder a /venta. Admin (banca) no vende tickets.
    if request.path == "/venta" and _current_role() == ROLE_ADMIN:
        flash("Los administradores no pueden vender tickets. Cree un usuario cajero para realizar ventas.")
        return redirect("/admin")

    # Cierre automático por inactividad (INACTIVITY_TIMEOUT_SECONDS)
    last = session.get("last_activity", 0)
    if time.time() - last > INACTIVITY_TIMEOUT_SECONDS:
        session.clear()
        return redirect("/")
    session["last_activity"] = time.time()


# ===============================
# PAGOS BANCA (tabla configurable)
# ===============================
PAGOS = {
    "quiniela_1": 70,
    "quiniela_2": 8,
    "quiniela_3": 4,
    "pale_12": 1200,
    "pale_13": 1200,
    "pale_23": 100,
    "tripleta": 20000,
    "super_pale": 3500
}

# Tabla para calculadora de premios (premio = monto * multiplicador)
PAGOS_CALCULADORA = {
    "Quiniela 1er": 70,
    "Quiniela 2do": 8,
    "Quiniela 3ro": 4,
    "Pale 1y2": 1200,
    "Pale 1y3": 1200,
    "Pale 2y3": 100,
    "Pale": 1200,
    "Tripleta": 20000,
    "Super Pale": 3500,
}

# =====================================================
# ESTILO iOS + ANDROID ULTRA PRO (POS + APP MODE)
# =====================================================
IOS = """

<!-- MOBILE ULTRA PRO iPHONE + ANDROID -->

<meta name="viewport"
content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover">

<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="format-detection" content="telephone=no">

<!-- APP INSTALL -->
<link rel="manifest" href="/manifest.json">
<meta name="theme-color" content="#002D62">
<link rel="apple-touch-icon" href="https://flagcdn.com/w320/do.png">

<style>

/* ===== RESET ULTRA ===== */
*,*::before,*::after{
box-sizing:border-box;
margin:0;
padding:0;
-webkit-tap-highlight-color:transparent;
}

/* ===== PERFORMANCE MOBILE ===== */
html{
scroll-behavior:smooth;
-webkit-text-size-adjust:100%;
overscroll-behavior:none;
}

body{
max-width:100%;
overflow-x:hidden;
font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display",system-ui,sans-serif;
font-size:15px;
color:#000;

/* fondo dominicano animado */
background:linear-gradient(270deg,#002D62,#ffffff,#CE1126);
background-size:400% 400%;
animation:fondoRD 12s ease infinite;

/* iPhone notch safe area */
padding-left:env(safe-area-inset-left);
padding-right:env(safe-area-inset-right);
padding-bottom:80px;
padding-top:60px;
}

/* animación fondo */
@keyframes fondoRD{
0%{background-position:0% 50%}
50%{background-position:100% 50%}
100%{background-position:0% 50%}
}

/* ===== TOUCH PERFECT ===== */
button,input,select{
font-size:16px !important;
min-height:48px;
touch-action:manipulation;
}

/* teclado numerico telefono */
input[type=number]{inputmode:decimal}

/* ===== CARD ===== */
.card{
width:100%;
max-width:430px;
margin:80px auto;
padding:22px;
border-radius:22px;
background:rgba(255,255,255,.85);
backdrop-filter:blur(20px);
border:1px solid rgba(255,255,255,.4);
box-shadow:0 30px 80px rgba(0,0,0,.25);
border-top:6px solid #CE1126;
}

/* ===== INPUTS ===== */
input,select{
width:100%;
padding:14px;
margin-top:8px;
border-radius:14px;
border:1px solid #ddd;
background:#f5f8ff;
}

input:focus,select:focus{
outline:none;
border-color:#CE1126;
box-shadow:0 0 0 3px rgba(206,17,38,.25);
}

/* ===== BOTONES ===== */
button{
width:100%;
margin-top:12px;
background:linear-gradient(135deg,#002D62,#003DA5);
color:white;
border:none;
border-radius:16px;
font-weight:800;
cursor:pointer;
box-shadow:0 8px 0 #001a40,0 20px 40px rgba(0,45,98,.3);
transition:.2s;
}

button:active{
transform:translateY(4px);
box-shadow:0 2px 0 #001a40;
}

/* ===== TABLAS MOBILE ===== */
table{
width:100%;
border-collapse:collapse;
display:block;
overflow-x:auto;
}

.topbar{
position:fixed;
top:0;
left:0;
width:100%;
height:60px;
background:#0a2e5c;
color:white;
display:flex;
align-items:center;
justify-content:center; /* CENTRA TODO */
padding:0 20px;
z-index:999;
box-shadow:0 5px 20px rgba(0,0,0,.25);
}

/* titulo centrado real */
.brand-center{
position:absolute;
left:50%;
transform:translateX(-50%);
font-weight:900;
font-size:18px;
letter-spacing:1px;
}

/* menu izquierda */
.menu-btn{
position:absolute;
left:20px;
cursor:pointer;
font-size:22px;
}

/* boton salir derecha */
.logout-btn{
position:absolute;
right:20px;
top:12px;
}

/* ===== SIDEBAR ===== */
.sidebar{
position:fixed;
top:0;
left:-260px;
width:260px;
height:100%;
background:linear-gradient(180deg,#002D62,#CE1126);
transition:.3s;
z-index:9999;
padding-top:70px;
}

.sidebar.open{left:0;}

.sidebar a{
display:block;
padding:18px;
color:white;
text-decoration:none;
font-weight:900;
}

/* ===== BOTON SALIR PREMIUM ===== */
.logout-btn{
position:fixed;
top:12px;
right:20px;

background:linear-gradient(135deg,#CE1126,#8f0c1b);
color:white !important;

padding:10px 18px;
border-radius:14px;

font-weight:900;
font-size:14px;
text-decoration:none;

box-shadow:0 6px 18px rgba(0,0,0,.3);
transition:.25s;
z-index:99999;
}

/* hover */
.logout-btn:hover{
transform:translateY(-2px) scale(1.05);
box-shadow:0 12px 30px rgba(0,0,0,.4);
}

/* click */
.logout-btn:active{
transform:scale(.95);
}

/* ===== CINTILLA METAS (ticker desplazamiento continuo) ===== */
.ticker-container{
width:100%;
overflow:hidden;
background:#0b1e3d;
color:white;
height:40px;
display:flex;
align-items:center;
margin-top:60px;
}
#tickerText{
white-space:nowrap;
display:inline-block;
padding-left:100%;
animation:tickerMove 18s linear infinite;
font-weight:bold;
font-size:14px;
}
@keyframes tickerMove{
0%{transform:translateX(0)}
100%{transform:translateX(-100%)}
}

/* ===== BOTON VENTA ===== */
.venta-btn{
position:fixed;
bottom:90px;
right:20px;
background:linear-gradient(135deg,#002D62,#CE1126);
color:white;
padding:18px 24px;
border-radius:22px;
font-weight:900;
text-decoration:none;
box-shadow:0 20px 60px rgba(0,0,0,.4);
z-index:9999;
}

.success{color:#34C759;font-weight:bold}
.danger{color:#CE1126;font-weight:bold}

/* ===== JUGADA CANCELADA ===== */
.cancelada{
    opacity:.45;
    text-decoration:line-through;
    background:#e5e5e5 !important;
}

.cancelada td{
    color:#777;
}

/* etiqueta cancelado */
.bloqueado{
    background:#999;
    color:white;
    padding:6px 12px;
    border-radius:8px;
    font-weight:bold;
}

</style>

{% if session.get("u") and request.path != "/" and session.get("role") != "admin" %}
<a href="/venta" class="venta-btn">💰 Venta</a>
{% endif %}

{% if session.get("u") %}

<div class="topbar">
<div class="menu-btn" onclick="toggleMenu()">☰</div>
<div class="brand-center">$ LA QUE NO FALLA $</div>
<a href="/logout" class="logout-btn">🚪 Salir</a>
</div>

<div class="ticker-container">
<div id="tickerText"></div>
</div>

<div id="sidebar" class="sidebar">
{% if session.get("role") != "admin" %}<a href="/venta">💰 Venta</a>{% endif %}
<a href="/reporte">📊 Reporte</a>
<a href="/ganadores">🏆 Ganadores / Riesgo</a>
{% if session.get("role") in ["admin", "super_admin"] %}
<a href="/admin/imprimir_cierre">🖨 Imprimir Cierre General</a>
<a href="/admin">⚙️ Admin</a>
{% else %}
<a href="/imprimir_cierre">🖨 Imprimir Mi Cierre</a>
{% endif %}
<a href="/logout">🚪 Cerrar Sesión</a>
</div>

<script>
function toggleMenu(){
document.getElementById("sidebar").classList.toggle("open")
}

async function cargarTicker(){
var el = document.getElementById("tickerText");
if(!el) return;
try{
var r = await fetch("/api/ventas_cajeros");
var data = await r.json();
var cajeros = data.cajeros || [];
var fmt = function(n){ return Number(n||0).toLocaleString("es-DO", {minimumFractionDigits:0, maximumFractionDigits:0}); };
var texto = cajeros.map(function(x){
return "🎯 " + (x.cajero||"Cajero") + " META RD$" + fmt(x.meta) + " | 💰 VENTAS RD$" + fmt(x.ventas);
}).join(" ⭐ ");
el.innerText = texto || "Cargando metas…";
}catch(e){ el.innerText = "Cargando metas…"; }
}
if(document.getElementById("tickerText")){
cargarTicker();
setInterval(cargarTicker, 5000);
}

/* service worker offline */
if("serviceWorker" in navigator){
navigator.serviceWorker.register("/sw.js");
}

/* Cierre automático de sesión por inactividad (10 minutos) */
(function(){
var tiempoInactivo;
var tiempoLimite = 10 * 60 * 1000;
function reiniciarTemporizador(){
  clearTimeout(tiempoInactivo);
  tiempoInactivo = setTimeout(function(){ window.location.href = "/logout"; }, tiempoLimite);
}
window.onload = reiniciarTemporizador;
document.onmousemove = reiniciarTemporizador;
document.onkeypress = reiniciarTemporizador;
document.onkeydown = reiniciarTemporizador;
document.onclick = reiniciarTemporizador;
document.ontouchstart = reiniciarTemporizador;
reiniciarTemporizador();
})();
</script>

{% endif %}
"""


# =====================================================
# MENÚ ADMIN (SOLO ADMIN)
# =====================================================
ADMIN_MENU = """
<h3 style="text-align:center;color:#003DA5">

{% if session.get('role') in ['admin', 'super_admin'] %}
<span>🇩🇴 Control Admin</span>
{% else %}
<span>💰 Resumen de la Venta</span>
{% endif %}

</h3>

<table id="adminTable" style="width:100%;border-collapse:collapse">

<tr>
<th>Lotería</th>
<th>Sorteo</th>
<th>Número</th>
<th>Jugada</th>
<th>Monto</th>
</tr>

</table>
"""


# =====================================================
# INIT_DB - CREAR TABLAS SOLO DE ESTA APP (Render PostgreSQL)
# Esta app usa ÚNICAMENTE estas tablas. Ignora cualquier otra tabla en la BD.
# =====================================================
def init_db():
    try:
        _init_db_impl()
    except Exception as e:
        print("DB INIT ERROR:", e)


def _init_db_impl():
    c = db()
    if not c:
        return
    cur = c.cursor()
    pk = "SERIAL PRIMARY KEY" if os.environ.get("DATABASE_URL") else "INTEGER PRIMARY KEY AUTOINCREMENT"

    # 1. TICKETS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS tickets (
            id {pk},
            cajero TEXT,
            created_at TEXT
        )
    """)

    # 2. TICKET_LINES
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS ticket_lines (
            id {pk},
            ticket_id INTEGER,
            lottery TEXT,
            lottery2 TEXT,
            draw TEXT,
            number TEXT,
            play TEXT,
            amount REAL,
            estado TEXT DEFAULT 'activo'
        )
    """)

    # 3. LOTTERIES
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS lotteries (
            id {pk},
            lottery TEXT,
            draw TEXT
        )
    """)

    # 4. HISTORIAL_JUGADAS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS historial_jugadas (
            id {pk},
            ticket_id INTEGER,
            lottery TEXT,
            number TEXT,
            play TEXT,
            amount REAL,
            created_at TEXT,
            estado TEXT DEFAULT 'activo'
        )
    """)

    # 5. BALANCE_POR_LOTERIA
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS balance_por_loteria (
            id {pk},
            lottery TEXT UNIQUE,
            balance NUMERIC DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 6. CONFIG
    cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # USERS (requerido para login - creado aquí para app autocontenida)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS users (
            id {pk},
            username TEXT UNIQUE,
            password_hash TEXT,
            role TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Asegurar columna meta por usuario (SQLite/Postgres; ignora error si ya existe)
    try:
        cur.execute("ALTER TABLE users ADD COLUMN meta NUMERIC DEFAULT 0")
    except Exception:
        pass

    # Columna approved: 1 = aprobado (puede hacer login), 0 = pendiente de aprobación por Super Admin
    try:
        if os.environ.get("DATABASE_URL"):
            cur.execute("ALTER TABLE users ADD COLUMN approved BOOLEAN DEFAULT true")
        else:
            cur.execute("ALTER TABLE users ADD COLUMN approved INTEGER DEFAULT 1")
    except Exception:
        pass
    # Usuarios existentes sin columna approved: marcar como aprobados
    try:
        if os.environ.get("DATABASE_URL"):
            cur.execute("UPDATE users SET approved = true WHERE approved IS NULL")
        else:
            cur.execute("UPDATE users SET approved = 1 WHERE approved IS NULL")
    except Exception:
        pass

    # Control de pago Super Admin: status (active/suspended) y due_date para bancas (role=admin)
    for col_sql in [
        "ALTER TABLE users ADD COLUMN banca_status TEXT DEFAULT 'active'",
        "ALTER TABLE users ADD COLUMN banca_due_date TEXT",
    ]:
        try:
            cur.execute(col_sql)
        except Exception:
            pass
    try:
        cur.execute("UPDATE users SET banca_status = 'active' WHERE banca_status IS NULL AND role = 'admin'")
    except Exception:
        pass

    # Tabla de solicitudes de nuevos Admin (Admin solicita; Super Admin aprueba/crea)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS admin_requests (
            id {pk},
            requested_by TEXT NOT NULL,
            requested_username TEXT NOT NULL,
            requested_role TEXT NOT NULL DEFAULT 'admin',
            reason TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP,
            resolved_by TEXT
        )
    """)

    # Columnas en tickets para sistema de ganadores (ganador, premio, pagado, fecha_pago)
    for col_sql in [
        "ALTER TABLE tickets ADD COLUMN ganador BOOLEAN DEFAULT false",
        "ALTER TABLE tickets ADD COLUMN premio NUMERIC DEFAULT 0",
        "ALTER TABLE tickets ADD COLUMN pagado BOOLEAN DEFAULT false",
        "ALTER TABLE tickets ADD COLUMN fecha_pago TIMESTAMP",
    ]:
        try:
            cur.execute(col_sql)
        except Exception:
            pass

    try:
        cur.execute("ALTER TABLE resultados ADD COLUMN draw TEXT")
    except Exception:
        pass

    # Índice único para UPSERT (lottery, draw, fecha). El scraper usa draw = '' cuando no hay sorteo.
    try:
        cur.execute("UPDATE resultados SET draw = '' WHERE draw IS NULL")
    except Exception:
        pass
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS resultados_unique ON resultados (lottery, COALESCE(draw, ''), fecha)")
    except Exception:
        pass

    # Tablas adicionales mínimas para funcionalidad completa
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS cash_closings (
            id {pk},
            date TEXT NOT NULL,
            cajero TEXT NOT NULL,
            total REAL NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS balance_cajeros (
            id {pk},
            cajero TEXT UNIQUE,
            balance NUMERIC DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS pagos (
            id {pk},
            ticket_id INTEGER,
            numero TEXT,
            jugada TEXT,
            monto REAL,
            fecha TEXT,
            pagado_por TEXT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS pagos_cajeros (
            id {pk},
            cajero TEXT,
            monto REAL,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            admin TEXT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS resultados (
            id {pk},
            fecha TEXT,
            lottery TEXT,
            draw TEXT,
            primero TEXT,
            segundo TEXT,
            tercero TEXT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS caja (
            id {pk},
            tipo TEXT NOT NULL,
            descripcion TEXT,
            monto NUMERIC NOT NULL,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            usuario TEXT,
            ticket_id INTEGER
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS pagos_premios (
            id {pk},
            ticket_id INTEGER,
            monto NUMERIC,
            cajero TEXT,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # USUARIOS DEFAULT (si no hay ninguno): primer usuario = super_admin
    cur.execute("SELECT COUNT(*) FROM users")
    row = cur.fetchone()
    n = list(row.values())[0] if hasattr(row, "values") else row[0]
    if n == 0:
        if os.environ.get("DATABASE_URL"):
            cur.executemany("""
                INSERT INTO users (username, password_hash, role, created_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """, [
                ("admin", generate_password_hash("admin"), ROLE_SUPER_ADMIN),
                ("cajero", generate_password_hash("1234"), ROLE_CAJERO)
            ])
        else:
            cur.executemany("""
                INSERT INTO users (username, password_hash, role, created_at)
                VALUES (?, ?, ?, datetime('now'))
            """, [
                ("admin", generate_password_hash("admin"), ROLE_SUPER_ADMIN),
                ("cajero", generate_password_hash("1234"), ROLE_CAJERO)
            ])

    # ===============================
    # LOTERÍAS DEFAULT
    # ===============================
    data = [
        ("La Primera", "12:00 PM"), ("La Primera", "8:00 PM"),
        ("LoteDom", "12:00 PM"),
        ("La Suerte Dominicana", "12:30 PM"), ("La Suerte Dominicana", "6:00 PM"),
        ("Quiniela Real", "12:55 PM"),

        # Lotería Nacional CORREGIDA
        ("Lotería Nacional", "2:30 PM"),
        ("Lotería Nacional", "8:50 PM"),
        ("Lotería Nacional", "Domingos 6:00 PM"),

        ("Loteka", "7:55 PM"),
        ("Leidsa", "8:55 PM"), ("Leidsa", "Domingos 3:55 PM"),
        ("Anguila", "10:00 AM"), ("Anguila", "1:00 PM"),
        ("Anguila", "6:00 PM"), ("Anguila", "9:00 PM"),
        ("King Lottery", "12:30 PM"), ("King Lottery", "7:30 PM"),
        ("Florida", "1:30 PM"), ("Florida", "10:30 PM"),
        ("New York", "2:30 PM"), ("New York", "10:30 PM")
    ]

    cur.execute("SELECT COUNT(*) FROM lotteries")
    row_lot = cur.fetchone()
    n_lot = list(row_lot.values())[0] if hasattr(row_lot, "values") else row_lot[0]
    if n_lot == 0:
        if os.environ.get("DATABASE_URL"):
            cur.executemany(_sql("INSERT INTO lotteries (lottery, draw) VALUES (%s, %s)"), data)
        else:
            cur.executemany("INSERT INTO lotteries (lottery, draw) VALUES (?, ?)", data)
    else:
        # Migración: asegurar que todas las bancas tengan todos sus horarios (insertar los que falten)
        for lot, dr in data:
            cur.execute(_sql("SELECT 1 FROM lotteries WHERE lottery = %s AND draw = %s"), (lot, dr))
            if not cur.fetchone():
                if os.environ.get("DATABASE_URL"):
                    cur.execute(_sql("INSERT INTO lotteries (lottery, draw) VALUES (%s, %s)"), (lot, dr))
                else:
                    cur.execute("INSERT INTO lotteries (lottery, draw) VALUES (?, ?)", (lot, dr))

    # Migración: corregir horarios (New York 11:30→10:30, 3:30→2:30; Florida 10:45→10:30, 2:30→1:30; eliminar Leidsa Domingos 6:00 PM)
    cur.execute(_sql("UPDATE lotteries SET draw = %s WHERE lottery = %s AND draw = %s"),
                ("10:30 PM", "New York", "11:30 PM"))
    cur.execute(_sql("UPDATE lotteries SET draw = %s WHERE lottery = %s AND draw = %s"),
                ("2:30 PM", "New York", "3:30 PM"))
    cur.execute(_sql("UPDATE lotteries SET draw = %s WHERE lottery = %s AND draw = %s"),
                ("10:30 PM", "Florida", "10:45 PM"))
    cur.execute(_sql("UPDATE lotteries SET draw = %s WHERE lottery = %s AND draw = %s"),
                ("1:30 PM", "Florida", "2:30 PM"))
    cur.execute(_sql("DELETE FROM lotteries WHERE lottery = %s AND draw = %s"),
                ("Leidsa", "Domingos 6:00 PM"))

    c.commit()
    c.close()


# =====================================================
# UTILIDAD: ¿CAJA CERRADA HOY%s
# =====================================================
def caja_cerrada_hoy():
    c = db()
    if not c:
        return False
    try:
        cur = c.cursor()
        hoy = ahora_rd().strftime("%Y-%m-%d")
        cur.execute(_sql("""
            SELECT COUNT(*)
            FROM cash_closings
            WHERE date = %s
        """), (hoy,))
        row = cur.fetchone()
        n = list(row.values())[0] if hasattr(row, "values") else row[0]
        return n > 0
    finally:
        c.close()


# =====================================================
# CONFIGURACION LIMITES DESDE ADMIN (AUTO)
# =====================================================

def init_config():
    c = db()
    if not c:
        return
    cur = c.cursor()

    # ===============================
    # TABLA CONFIG
    # ===============================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    # ===============================
    # VALORES DEFAULT
    # ===============================
    defaults = {
        "usar_limites": "0",
        "limite_sorteo": "5000",
        "limite_numero": "100",
        "meta_diaria": "0",
    }

    # ===============================
    # INSERTAR SI NO EXISTE
    # ===============================
    for k, v in defaults.items():

        cur.execute(
            _sql("SELECT 1 FROM config WHERE key=%s"),
            (k,)
        )

        if not cur.fetchone():
            cur.execute(
                _sql("INSERT INTO config (key, value) VALUES (%s, %s)"),
                (k, v)
            )

    c.commit()
    c.close()
    
# =====================================================
# OBTENER CONFIG
# =====================================================
def get_config(key):
    c = db()
    if not c:
        return None
    try:
        cur = c.cursor()
        cur.execute(_sql("SELECT value FROM config WHERE key=%s"), (key,))
        r = cur.fetchone()
        if r is None:
            return None
        return r.get("value") if hasattr(r, "get") else (r[0] if isinstance(r, (list, tuple)) else getattr(r, "value", None))
    finally:
        c.close()

# ===============================
# NORMALIZAR NUMERO
# ===============================
def normalizar_numero(numero):

    if not numero:
        return ""

    numero = str(numero).strip()

    # 1202 -> 12-02
    if len(numero) == 4 and numero.isdigit():
        return numero[:2] + "-" + numero[2:]

    return numero

# ===============================
# NUMEROS PELIGROSOS
# ===============================
def numeros_peligrosos():
    c = db()
    if not c:
        return []
    try:
        cur = c.cursor()
        cur.execute("SELECT COALESCE(SUM(balance),0) AS total FROM balance_por_loteria")
        balance_row = cur.fetchone()
        balance = float((balance_row.get("total") if hasattr(balance_row, "get") and balance_row else (balance_row[0] if balance_row else 0)) or 0)
        if balance <= 0:
            return []
        ph = "%s" if os.environ.get("DATABASE_URL") else "?"
        cur.execute("""
        SELECT number, SUM(amount*70) AS posible
        FROM ticket_lines
        WHERE COALESCE(estado,'activo') != 'cancelado'
        GROUP BY number
        HAVING SUM(amount*70) > """ + ph, (balance,))
        rows = cur.fetchall()
        return [(r.get("number") if hasattr(r, "get") else r[0]) for r in (rows or [])]
    finally:
        c.close()


# Riesgo = mismo pago que premios reales (PAGOS). Así "Números de Alto Riesgo" refleja lo que la banca pagaría.
def _multiplicador_riesgo(play):
    if play == "Quiniela":
        return PAGOS["quiniela_1"]  # 70
    if play == "Pale":
        return max(PAGOS["pale_12"], PAGOS["pale_13"], PAGOS["pale_23"])  # 1200
    if play == "Tripleta":
        return PAGOS["tripleta"]  # 20000
    if play == "Super Pale":
        return PAGOS["super_pale"]  # 3500
    return PAGOS.get("quiniela_1", 70)


def calcular_riesgo_por_numero():
    """
    Riesgo por número + lotería + sorteo (ej: 90 | Anguila | 10:00 AM).
    Usa multiplicadores PAGOS: Quiniela 70x, Pale 1200x, Tripleta 20000x, Super Pale 3500x.
    """
    c = db()
    if not c:
        return []
    try:
        cur = c.cursor()
        cur.execute(
            _sql("""
            SELECT lottery, draw, number, play, SUM(amount) AS total_apostado
            FROM ticket_lines
            WHERE COALESCE(estado,'activo') != 'cancelado'
            GROUP BY lottery, draw, number, play
            ORDER BY lottery, draw, number, play
            """)
        )
        rows = cur.fetchall() or []
        result = []
        for r in rows:
            if hasattr(r, "keys"):
                lottery = r.get("lottery") or ""
                draw = r.get("draw") or ""
                number = r.get("number") or ""
                play = r.get("play") or "Quiniela"
                total = float(r.get("total_apostado") or 0)
            else:
                lottery = r[0] if len(r) > 0 else ""
                draw = r[1] if len(r) > 1 else ""
                number = r[2] if len(r) > 2 else ""
                play = r[3] if len(r) > 3 else "Quiniela"
                total = float(r[4] or 0) if len(r) > 4 else 0.0
            mult = _multiplicador_riesgo(play)
            posible_pago = total * mult
            result.append({
                "lottery": lottery,
                "draw": draw,
                "number": number,
                "play": play,
                "posible_pago": round(posible_pago, 2),
            })
        result.sort(key=lambda x: -x["posible_pago"])
        return result
    finally:
        c.close()

# ===============================
# CALCULAR PREMIO AUTOMATICO (PRO)
# ===============================
def calcular_premio(play, numero, monto, r1, r2, r3):

    # seguridad monto
    try:
        monto = float(monto)
    except:
        return 0

    if monto <= 0 or not numero:
        return 0

    numero = str(numero).strip()
    r1 = str(r1).strip()
    r2 = str(r2).strip()
    r3 = str(r3).strip()

    # ===============================
    # QUINIELA
    # ===============================
    if play == "Quiniela":

        if numero == r1:
            return monto * PAGOS["quiniela_1"]

        if numero == r2:
            return monto * PAGOS["quiniela_2"]

        if numero == r3:
            return monto * PAGOS["quiniela_3"]

    # ===============================
    # PALE (12-34)
    # ===============================
    if play == "Pale":

        if "-" not in numero:
            return 0

        try:
            n1, n2 = numero.split("-")
            nums = {n1.strip(), n2.strip()}
        except:
            return 0

        if nums == {r1, r2}:
            return monto * PAGOS["pale_12"]

        if nums == {r1, r3}:
            return monto * PAGOS["pale_13"]

        if nums == {r2, r3}:
            return monto * PAGOS["pale_23"]

    # ===============================
    # TRIPLETA (12-34-56)
    # ===============================
    if play == "Tripleta":

        if "-" not in numero:
            return 0

        try:
            nums = set(x.strip() for x in numero.split("-"))
        except:
            return 0

        if nums == {r1, r2, r3}:
            return monto * PAGOS["tripleta"]

    # ===============================
    # SUPER PALE (solo si pega 1er)
    # ===============================
    if play == "Super Pale":
        if numero == r1:
            return monto * PAGOS["super_pale"]

    return 0 
    
# ===============================
# RANKING NUMEROS
# ===============================
def ranking_numeros():
    c = db()
    if not c:
        return []
    try:
        cur = c.cursor()
        cur.execute("""
        SELECT number, SUM(amount) total
        FROM ticket_lines
        WHERE estado!='cancelado' OR estado IS NULL
        GROUP BY number
        ORDER BY total DESC
        LIMIT 10
        """)
        rows = cur.fetchall()
        return rows
    finally:
        c.close()

# =====================================================
# PANEL ADMIN CONTROL LIMITES (FIX COMPLETO Y SEGURO)
# =====================================================
@app.route("/admin/limites", methods=["GET","POST"])
def admin_limites():

    if not is_admin_or_super():
        return redirect("/")

    # ===============================
    # GUARDAR LIMITES
    # ===============================
    if request.method == "POST":

        c = db()
        if not c:
            return "Error conectando base de datos", 500

        try:
            cur = c.cursor()

            limite_sorteo = request.form.get("sorteo", "0")
            limite_numero = request.form.get("numero", "0")
            usar_limites = "1" if request.form.get("activar") else "0"
            meta_diaria = request.form.get("meta_diaria", "0")

            valores = {
                "limite_sorteo": limite_sorteo,
                "limite_numero": limite_numero,
                "usar_limites": usar_limites,
                "meta_diaria": meta_diaria
            }

            for k, v in valores.items():

                cur.execute(
                    _sql("UPDATE config SET value=%s WHERE key=%s"),
                    (v, k)
                )

                if cur.rowcount == 0:
                    cur.execute(
                        _sql("INSERT INTO config (key,value) VALUES (%s,%s)"),
                        (k, v)
                    )

            c.commit()

        finally:
            c.close()

        return redirect("/admin")


    # ===============================
    # CARGAR CONFIGURACION
    # ===============================
    c = db()
    if not c:
        return "Error conectando base de datos", 500

    try:
        cur = c.cursor()

        cur.execute(_sql("SELECT key,value FROM config"))
        rows = cur.fetchall()

        config = {r["key"]: r["value"] for r in rows} if rows else {}

        limite_sorteo = config.get("limite_sorteo", "0")
        limite_numero = config.get("limite_numero", "0")
        usar_limites = config.get("usar_limites", "0")
        meta_diaria = config.get("meta_diaria", "0")

    finally:
        c.close()


    return render_template_string(IOS + """

    <div class="card" style="max-width:420px;margin:80px auto">

        <h2>🎛️ Control de Límites</h2>

        <form method="post">

            <label>Límite Sorteo</label>
            <input name="sorteo" value="{{ limite_sorteo }}">

            <label>Límite Número</label>
            <input name="numero" value="{{ limite_numero }}">

            <label style="display:block;margin-top:15px">Meta diaria global (por defecto) RD$</label>
            <input name="meta_diaria" type="number" min="0" step="100" value="{{ meta_diaria }}" placeholder="Ej: 10000">

            <label style="display:block;margin-top:15px">
                <input type="checkbox" name="activar"
                {% if usar_limites == "1" %}checked{% endif %}>
                Activar límites
            </label>

            <button style="margin-top:15px">
                💾 Guardar Cambios
            </button>

        </form>

        <a href="/admin" style="display:block;text-align:center;margin-top:15px">
            ⬅ Volver
        </a>

    </div>

    """,
    limite_sorteo=limite_sorteo,
    limite_numero=limite_numero,
    usar_limites=usar_limites,
    meta_diaria=meta_diaria
    )

# =====================================================
# LOGIN PRO MAX (BANCA DOMINICANA PREMIUM)
# =====================================================
@app.route("/", methods=["GET", "POST"])
def login():

    if request.method == "POST":

        username = request.form["u"]
        password = request.form["p"]

        # ✅ conexión correcta (ESTABLE)
        conn = db()

        if not conn:
            return "Error conectando base de datos", 500

        cur = conn.cursor()

        cur.execute(
            _sql("SELECT * FROM users WHERE username=%s"),
            (username,)
        )

        row = cur.fetchone()

        cur.close()
        conn.close()

        # Convertir a dict para compatibilidad con sqlite3.Row (no tiene .get()) y psycopg2 RealDictCursor
        if row:
            if hasattr(row, "keys"):
                user = dict(zip(row.keys(), row))
            else:
                user = dict(zip(["id", "username", "password_hash", "role", "created_at", "meta", "approved"], row)) if isinstance(row, (list, tuple)) else None
        else:
            user = None

        # ✅ LOGIN OK (solo si está aprobado)
        if user and check_password_hash(user.get("password_hash") or "", password):
            approved = user.get("approved")
            if approved in (False, 0, "0", "f", "false"):
                return render_template_string(IOS + """
                <div class="card" style="max-width:420px;margin:120px auto;text-align:center">
                    <h2 style="color:#b45309;">Cuenta pendiente de aprobación</h2>
                    <p style="margin:15px 0;color:#555;">Su usuario fue creado por un administrador y debe ser aprobado por el Super Admin antes de poder iniciar sesión.</p>
                    <p style="margin:15px 0"><a href="/">Volver al inicio</a></p>
                </div>
                """)
            session.permanent = True
            session["u"] = user["username"]
            session["role"] = user["role"]
            session["last_activity"] = time.time()
            if user.get("role") == ROLE_ADMIN and check_banca_suspension() is not None:
                return redirect("/banca_suspendida")
            # Admin va al dashboard; cajeros y super_admin van a venta
            if user.get("role") == ROLE_ADMIN:
                return redirect("/admin")
            return redirect("/venta")

        # ❌ LOGIN FALLÓ
        return render_template_string(IOS + """
        <div class="card" style="max-width:420px;margin:120px auto;text-align:center">
            <h2 style="color:red;">Usuario o contraseña incorrectos</h2>
            <p style="margin:15px 0"><a href="/">Intentar otra vez</a></p>
            <p style="font-size:14px;color:#666">Primera vez? Crea el usuario admin (admin/admin):</p>
            <p><a href="/create-admin" style="background:#16a34a;color:white;padding:10px 20px;border-radius:8px;text-decoration:none">Crear admin</a></p>
        </div>
        """)

    return render_template_string(IOS + """

<style>

/* ===== FONDO PREMIUM ===== */
body{
margin:0;
font-family:-apple-system,BlinkMacSystemFont,Arial;
background:linear-gradient(135deg,#0f172a,#1e3a8a,#dc2626);
}

/* ===== LOGIN CENTRO FIJO (NO SE MUEVE) ===== */
.login-wrap{
position:fixed;
top:50%;
left:50%;
transform:translate(-50%,-50%);
width:100%;
max-width:600px;
display:flex;
flex-direction:column;
align-items:center;
margin:0;
padding:0;
}

/* ===== BANDERA LOGO ===== */
.logo-dr img{
width:140px;
border-radius:24px;
padding:12px;
background:rgba(255,255,255,.15);
backdrop-filter:blur(20px);

box-shadow:
0 25px 70px rgba(0,0,0,.35),
0 0 35px rgba(206,17,38,.4),
0 0 35px rgba(0,45,98,.4);

animation:float 3s ease-in-out infinite;
margin-bottom:20px;
}

/* ===== TITULO ===== */
.main-title{
color:white;
font-size:clamp(28px,5vw,50px);
font-weight:900;
margin-bottom:35px;
letter-spacing:2px;
}

.main-title span{
color:#ff2d2d;
}

/* ===== LOGIN CARD ===== */
.login-card{
width:100%;
max-width:520px;
padding:60px;
border-radius:30px;

background:rgba(255,255,255,.1);
backdrop-filter:blur(30px);

border:1px solid rgba(255,255,255,.2);
box-shadow:0 40px 120px rgba(0,0,0,.4);
}

/* ===== INPUTS ===== */
.login-card input{
width:100%;
padding:20px;
border-radius:18px;
border:none;
margin-bottom:20px;
font-size:18px;

background:rgba(255,255,255,.2);
color:white;
outline:none;
}

.login-card input::placeholder{
color:#ddd;
}

/* ===== BOTON ULTRA ===== */
.btn-login{
width:100%;
padding:20px;
border-radius:18px;
border:none;

font-size:20px;
font-weight:900;
color:white;
cursor:pointer;

background:linear-gradient(135deg,#002D62,#CE1126);
box-shadow:0 10px 0 #8f0c1b,0 30px 60px rgba(0,0,0,.4);
transition:.3s;
}

.btn-login:hover{
transform:translateY(-4px);
}

/* ===== ANIMACION ===== */
@keyframes float{
0%{transform:translateY(0)}
50%{transform:translateY(-10px)}
100%{transform:translateY(0)}
}

/* ===== MOBILE ===== */
@media (max-width:768px){
.login-card{
padding:40px;
max-width:90%;
}
}

</style>

<div class="login-wrap">

<div class="logo-dr">
<img src="https://flagcdn.com/w320/do.png">
</div>

<div class="main-title">
<span>$</span> LA QUE NO FALLA <span>$</span>
</div>

<div class="login-card">

<form method="post">
<input name="u" placeholder="Usuario" required>
<input name="p" type="password" placeholder="Contraseña" required>
<button class="btn-login">ENTRAR</button>
</form>

</div>

</div>

""")


# ===============================
# LOGOUT REAL
# ===============================
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


@app.route("/banca_suspendida")
def banca_suspendida():
    """Página mostrada cuando una banca (admin) está suspendida por falta de pago."""
    return render_template_string(IOS + """
    <div class="card" style="max-width:520px;margin:80px auto;text-align:center;padding:32px">
        <h2 style="color:#dc2626;margin-bottom:16px">⚠️ Cuenta suspendida</h2>
        <p style="font-size:18px;margin:12px 0;color:#1e293b">Cuenta suspendida. Contacte al administrador para activar su banca.</p>
        <p style="margin:20px 0;color:#64748b">Su banca está suspendida por falta de pago.</p>
        <p style="margin-top:24px"><a href="/logout" style="display:inline-block;padding:12px 24px;background:#002D62;color:white;border-radius:8px;text-decoration:none">Cerrar sesión</a></p>
    </div>
    """)


@app.route("/crear_usuario", methods=["GET", "POST"])
@admin_required
def crear_usuario():
    # Admin puede crear cajeros (user, cajero), supervisor y collector; Super Admin además puede crear admin
    allowed_roles = [ROLE_USER, ROLE_CAJERO, ROLE_SUPERVISOR, ROLE_COLLECTOR]
    if is_super_admin():
        allowed_roles = [ROLE_ADMIN, ROLE_USER, ROLE_CAJERO, ROLE_SUPERVISOR, ROLE_COLLECTOR]

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        role = (request.form.get("role") or "").strip().lower()

        if role not in allowed_roles:
            flash("No tiene permiso para crear ese rol. Solo Super Admin puede crear Admins.")
            return redirect("/crear_usuario")

        if not username or not password:
            flash("Usuario y contraseña son obligatorios.")
            return redirect("/crear_usuario")

        conn = db()
        if not conn:
            return "Error conectando base de datos", 500
        cur = conn.cursor()

        cur.execute(_sql("SELECT id FROM users WHERE username=%s"), (username,))
        existe = cur.fetchone()
        if existe:
            conn.close()
            flash("Ese nombre de usuario ya existe.")
            return redirect("/crear_usuario")

        # Super Admin crea usuarios ya aprobados; Admin crea usuarios pendientes de aprobación
        approved_val = 1 if is_super_admin() else 0
        if os.environ.get("DATABASE_URL"):
            cur.execute(
                _sql("INSERT INTO users (username, password_hash, role, approved, created_at) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)"),
                (username, generate_password_hash(password), role, approved_val)
            )
        else:
            cur.execute(
                _sql("INSERT INTO users (username, password_hash, role, approved, created_at) VALUES (%s, %s, %s, %s, datetime('now'))"),
                (username, generate_password_hash(password), role, approved_val)
            )
        conn.commit()
        conn.close()
        if approved_val:
            flash("Usuario creado correctamente.")
        else:
            flash("Usuario creado. Quedará pendiente hasta que el Super Admin lo apruebe.")
        return redirect("/usuarios")

    # Opciones de rol para el select (etiquetas amigables)
    role_options = [
        (ROLE_ADMIN, "Admin (banca)") if is_super_admin() else None,
        (ROLE_USER, "Cajero (usuario)"),
        (ROLE_CAJERO, "Cajero (legacy)"),
        (ROLE_SUPERVISOR, "Supervisor"),
        (ROLE_COLLECTOR, "Cobrador"),
    ]
    role_options = [x for x in role_options if x is not None and x[0] in allowed_roles]

    return render_template_string(IOS + """
    <a href="/venta" class="venta-btn">💰 Venta</a>
<style>
body{margin:0;font-family:Arial,sans-serif;background:linear-gradient(135deg,#002D62 0%,#ffffff 50%,#CE1126 100%);min-height:100vh}
.contenedor-centrado{display:flex;justify-content:center;align-items:center;min-height:80vh;}
.panel-crear-usuario{width:420px;max-width:95%;padding:35px;border-radius:22px;background:rgba(255,255,255,.9);backdrop-filter:blur(20px);border:1px solid rgba(255,255,255,.4);box-shadow:0 30px 80px rgba(0,0,0,.15);border-top:6px solid #CE1126;}
h2{text-align:center;color:#002D62;margin-bottom:20px;}
input,select{width:100%;padding:14px;border-radius:14px;border:1px solid #002D62;margin-top:6px;margin-bottom:14px;background:#f1f5ff;font-size:15px;}
input:focus,select:focus{outline:none;border-color:#CE1126;box-shadow:0 0 0 3px rgba(206,17,38,.25);}
button{width:100%;padding:15px;border:none;border-radius:16px;font-size:15px;font-weight:900;color:white;cursor:pointer;background:linear-gradient(135deg,#002D62,#CE1126);box-shadow:0 6px 0 #8f0c1b, 0 18px 40px rgba(0,45,98,.35);transition:.2s;}
button:hover{transform:translateY(-2px);}
a{display:block;text-align:center;margin-top:12px;color:#002D62;font-weight:bold;text-decoration:none;}
a:hover{color:#CE1126;}
.msg{background:#fee2e2;color:#991b1b;padding:10px;border-radius:8px;margin-bottom:12px;}
</style>
<div class="contenedor-centrado">
<div class="panel-crear-usuario">
<h2>➕ Crear Usuario</h2>
{% for msg in get_flashed_messages() %}<p class="msg">{{ msg }}</p>{% endfor %}
<form method="post">
<label>Usuario</label>
<input name="username" required>
<label>Contraseña</label>
<input name="password" type="password" required>
<label>Rol</label>
<select name="role">
{% for val, label in role_options %}
<option value="{{ val }}">{{ label }}</option>
{% endfor %}
</select>
<button>✨ Crear Usuario</button>
</form>
<p style="font-size:13px;color:#555;">¿Necesita crear un <strong>Admin</strong>? Solo el Super Admin puede hacerlo: <a href="/crear_admin">Crear Admin</a>. Si es Admin, puede <a href="/solicitar_admin">solicitar uno</a>.</p>
<a href="/usuarios">👥 Ver Usuarios</a>
<a href="/admin">⬅ Volver al Dashboard</a>
</div>
</div>
""", role_options=role_options)


@app.route("/crear_admin", methods=["GET", "POST"])
@super_admin_required
def crear_admin():
    """Solo SUPER_ADMIN puede crear cuentas de tipo Admin."""
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if not username or not password:
            flash("Usuario y contraseña son obligatorios.")
            return redirect("/crear_admin")
        conn = db()
        if not conn:
            return "Error conectando base de datos", 500
        try:
            cur = conn.cursor()
            cur.execute(_sql("SELECT id FROM users WHERE username=%s"), (username,))
            if cur.fetchone():
                flash("Ese nombre de usuario ya existe.")
                return redirect("/crear_admin")
            # Super Admin crea Admin ya aprobado
            if os.environ.get("DATABASE_URL"):
                cur.execute(
                    _sql("INSERT INTO users (username, password_hash, role, approved, created_at) VALUES (%s, %s, %s, true, CURRENT_TIMESTAMP)"),
                    (username, generate_password_hash(password), ROLE_ADMIN)
                )
            else:
                cur.execute(
                    _sql("INSERT INTO users (username, password_hash, role, approved, created_at) VALUES (%s, %s, %s, 1, datetime('now'))"),
                    (username, generate_password_hash(password), ROLE_ADMIN)
                )
            conn.commit()
            flash("Usuario Admin creado correctamente.")
            return redirect("/usuarios")
        finally:
            conn.close()
    return render_template_string(IOS + """
    <a href="/venta" class="venta-btn">💰 Venta</a>
    <div style="max-width:420px;margin:80px auto;padding:30px;background:#fff;border-radius:16px;box-shadow:0 10px 40px rgba(0,0,0,.1);">
        <h2 style="color:#002D62;">👤 Crear Usuario Admin</h2>
        <p style="color:#555;margin-bottom:16px;">Solo el Super Admin puede crear cuentas de Admin.</p>
        {% for msg in get_flashed_messages() %}<p style="background:#fee2e2;color:#991b1b;padding:10px;border-radius:8px;">{{ msg }}</p>{% endfor %}
        <form method="post">
            <label>Usuario</label>
            <input name="username" required style="width:100%;padding:12px;margin:6px 0;">
            <label>Contraseña</label>
            <input name="password" type="password" required style="width:100%;padding:12px;margin:6px 0;">
            <button type="submit" style="width:100%;padding:14px;margin-top:12px;background:#002D62;color:#fff;border:none;border-radius:10px;font-weight:bold;">Crear Admin</button>
        </form>
        <a href="/usuarios" style="display:block;margin-top:16px;color:#002D62;">👥 Ver Usuarios</a>
        <a href="/admin" style="display:block;margin-top:8px;color:#002D62;">⬅ Volver al Dashboard</a>
    </div>
    """)


@app.route("/solicitar_admin", methods=["GET", "POST"])
@admin_required
def solicitar_admin():
    """Admin puede solicitar nuevo Admin; Super Admin ve la lista y aprueba (crea el usuario)."""
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500

    if request.method == "POST":
        action = request.form.get("action")
        if action == "solicitar" and not is_super_admin():
            username = (request.form.get("requested_username") or "").strip()
            reason = (request.form.get("reason") or "").strip()
            if not username:
                flash("Indique el nombre de usuario solicitado.")
                conn.close()
                return redirect("/solicitar_admin")
            try:
                cur = conn.cursor()
                cur.execute(_sql("SELECT id FROM users WHERE username=%s"), (username,))
                if cur.fetchone():
                    flash("Ese usuario ya existe en el sistema.")
                    conn.close()
                    return redirect("/solicitar_admin")
                cur.execute(_sql("""
                    INSERT INTO admin_requests (requested_by, requested_username, requested_role, reason, status)
                    VALUES (%s, %s, %s, %s, 'pending')
                """), (session.get("u") or session.get("user"), username, ROLE_ADMIN, reason))
                conn.commit()
                flash("Solicitud enviada. El Super Admin la revisará.")
            except Exception as e:
                flash("Error al guardar la solicitud.")
            finally:
                conn.close()
            return redirect("/solicitar_admin")

        if action == "aprobar" and is_super_admin():
            req_id = request.form.get("request_id", type=int)
            new_password = (request.form.get("password_%s" % req_id) or "").strip()
            if req_id and new_password:
                cur = conn.cursor()
                cur.execute(_sql("SELECT id, requested_username, status FROM admin_requests WHERE id=%s"), (req_id,))
                row = cur.fetchone()
                if row and (row.get("status") if hasattr(row, "get") else row[2]) == "pending":
                    uname = row.get("requested_username") if hasattr(row, "get") else row[1]
                    cur.execute(_sql("SELECT id FROM users WHERE username=%s"), (uname,))
                    if not cur.fetchone():
                        cur.execute(_sql("""
                            INSERT INTO users (username, password_hash, role, created_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        """), (uname, generate_password_hash(new_password), ROLE_ADMIN))
                        cur.execute(_sql("""
                            UPDATE admin_requests SET status='approved', resolved_at=CURRENT_TIMESTAMP, resolved_by=%s WHERE id=%s
                        """), (session.get("u") or "super_admin", req_id))
                        conn.commit()
                        flash("Usuario Admin creado: %s" % uname)
                cur.close()
            conn.close()
            return redirect("/solicitar_admin")

        if action == "rechazar" and is_super_admin():
            req_id = request.form.get("request_id", type=int)
            if req_id:
                cur = conn.cursor()
                cur.execute(_sql("UPDATE admin_requests SET status='rejected', resolved_at=CURRENT_TIMESTAMP, resolved_by=%s WHERE id=%s"),
                            (session.get("u") or "super_admin", req_id))
                conn.commit()
                cur.close()
                flash("Solicitud rechazada.")
            conn.close()
            return redirect("/solicitar_admin")
        conn.close()
        return redirect("/solicitar_admin")

    try:
        cur = conn.cursor()
        if is_super_admin():
            cur.execute(_sql("""
                SELECT id, requested_by, requested_username, reason, status, created_at
                FROM admin_requests ORDER BY created_at DESC
            """))
            requests_list = cur.fetchall()
        else:
            cur.execute(_sql("""
                SELECT id, requested_username, reason, status, created_at
                FROM admin_requests WHERE requested_by=%s ORDER BY created_at DESC
            """), (session.get("u") or session.get("user"),))
            requests_list = cur.fetchall()
    finally:
        conn.close()

    return render_template_string(IOS + """
    <a href="/venta" class="venta-btn">💰 Venta</a>
    <div style="max-width:720px;margin:40px auto;padding:24px;background:#fff;border-radius:16px;box-shadow:0 10px 40px rgba(0,0,0,.1);">
        <h2 style="color:#002D62;">📋 Solicitudes de nuevo Admin</h2>
        {% for msg in get_flashed_messages() %}<p style="background:#dbeafe;color:#1e40af;padding:10px;border-radius:8px;">{{ msg }}</p>{% endfor %}
        {% if session.get('role') == 'super_admin' %}
        <p style="color:#555;">Como Super Admin puede aprobar o rechazar solicitudes. Al aprobar, asigne una contraseña y se creará el usuario Admin.</p>
        <table width="100%%" style="border-collapse:collapse;margin-top:16px;">
            <tr style="background:#002D62;color:#fff;"><th style="padding:10px;">Solicitado por</th><th>Usuario</th><th>Motivo</th><th>Estado</th><th>Fecha</th><th>Acción</th></tr>
            {% for r in requests_list %}
            <tr style="border-bottom:1px solid #eee;">
                <td style="padding:8px;">{{ r.requested_by }}</td>
                <td>{{ r.requested_username }}</td>
                <td>{{ r.reason or '-' }}</td>
                <td>{{ r.status }}</td>
                <td>{{ r.created_at }}</td>
                <td>
                    {% if r.status == 'pending' %}
                    <form method="post" style="display:inline;">
                        <input type="hidden" name="action" value="aprobar">
                        <input type="hidden" name="request_id" value="{{ r.id }}">
                        <input type="password" name="password_{{ r.id }}" placeholder="Contraseña" required style="width:120px;">
                        <button type="submit">Aprobar</button>
                    </form>
                    <form method="post" style="display:inline;">
                        <input type="hidden" name="action" value="rechazar">
                        <input type="hidden" name="request_id" value="{{ r.id }}">
                        <button type="submit">Rechazar</button>
                    </form>
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </table>
        {% else %}
        <p style="color:#555;">Puede solicitar al Super Admin la creación de un nuevo usuario Admin.</p>
        <form method="post" style="margin:20px 0;">
            <input type="hidden" name="action" value="solicitar">
            <input name="requested_username" placeholder="Nombre de usuario para el nuevo Admin" required style="width:100%%;padding:12px;">
            <input name="reason" placeholder="Motivo (opcional)" style="width:100%%;padding:12px;margin-top:8px;">
            <button type="submit">Enviar solicitud</button>
        </form>
        <h3>Mis solicitudes</h3>
        <table width="100%%" style="border-collapse:collapse;">
            <tr style="background:#002D62;color:#fff;"><th style="padding:10px;">Usuario</th><th>Estado</th><th>Fecha</th></tr>
            {% for r in requests_list %}
            <tr><td>{{ r.requested_username }}</td><td>{{ r.status }}</td><td>{{ r.created_at }}</td></tr>
            {% endfor %}
        </table>
        {% endif %}
        <a href="/admin" style="display:block;margin-top:20px;color:#002D62;">⬅ Volver al Dashboard</a>
    </div>
    """, requests_list=requests_list)


# =====================================================
# USUARIOS PENDIENTES DE APROBACIÓN (solo Super Admin)
# Los usuarios creados por Admin quedan approved=0 hasta que Super Admin los apruebe.
# =====================================================
@app.route("/usuarios_pendientes")
@super_admin_required
def usuarios_pendientes():
    """Lista de usuarios pendientes de aprobación (approved = false). Solo Super Admin."""
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT id, username, role, created_at
                FROM users
                WHERE approved IS NOT TRUE
                ORDER BY created_at DESC
            """)
        else:
            cur.execute("""
                SELECT id, username, role, created_at
                FROM users
                WHERE COALESCE(approved, 0) = 0
                ORDER BY created_at DESC
            """)
        pendientes = cur.fetchall()
    finally:
        conn.close()

    return render_template_string(IOS + """
    <a href="/venta" class="venta-btn">💰 Venta</a>
    <div style="max-width:800px;margin:40px auto;padding:24px;background:#fff;border-radius:16px;box-shadow:0 10px 40px rgba(0,0,0,.1);">
        <h2 style="color:#002D62;">⏳ Usuarios pendientes de aprobación</h2>
        <p style="color:#555;margin-bottom:20px;">Estos usuarios fueron creados por un Admin y no pueden iniciar sesión hasta que los apruebe.</p>
        {% for msg in get_flashed_messages() %}<p style="background:#dbeafe;color:#1e40af;padding:10px;border-radius:8px;">{{ msg }}</p>{% endfor %}
        {% if pendientes %}
        <table width="100%%" style="border-collapse:collapse;">
            <tr style="background:#002D62;color:#fff;">
                <th style="padding:10px;">ID</th>
                <th>Usuario</th>
                <th>Rol</th>
                <th>Creado</th>
                <th>Acciones</th>
            </tr>
            {% for u in pendientes %}
            <tr style="border-bottom:1px solid #eee;">
                <td style="padding:10px;">{{ u.id }}</td>
                <td>{{ u.username }}</td>
                <td>{{ u.role }}</td>
                <td>{{ u.created_at }}</td>
                <td>
                    <form method="post" action="/aprobar_usuario/{{ u.id }}" style="display:inline;">
                        <button type="submit" style="padding:8px 14px;background:#16a34a;color:#fff;border:none;border-radius:8px;cursor:pointer;">✓ Aprobar</button>
                    </form>
                    <form method="post" action="/rechazar_usuario/{{ u.id }}" style="display:inline;" onsubmit="return confirm('¿Rechazar y eliminar este usuario?');">
                        <button type="submit" style="padding:8px 14px;background:#dc2626;color:#fff;border:none;border-radius:8px;cursor:pointer;">✗ Rechazar</button>
                    </form>
                </td>
            </tr>
            {% endfor %}
        </table>
        {% else %}
        <p style="color:#16a34a;font-weight:bold;">No hay usuarios pendientes de aprobación.</p>
        {% endif %}
        <a href="/admin" style="display:block;margin-top:20px;color:#002D62;">⬅ Volver al Dashboard</a>
        <a href="/usuarios" style="display:block;margin-top:8px;color:#002D62;">👥 Ver todos los usuarios</a>
    </div>
    """, pendientes=pendientes)


@app.route("/aprobar_usuario/<int:user_id>", methods=["POST"])
@super_admin_required
def aprobar_usuario(user_id):
    """Super Admin aprueba un usuario: approved = true. Puede iniciar sesión."""
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        if os.environ.get("DATABASE_URL"):
            cur.execute("UPDATE users SET approved = true WHERE id = %s", (user_id,))
        else:
            cur.execute("UPDATE users SET approved = 1 WHERE id = ?", (user_id,))
        conn.commit()
        flash("Usuario aprobado. Ya puede iniciar sesión.")
    finally:
        conn.close()
    return redirect(url_for("usuarios_pendientes"))


@app.route("/rechazar_usuario/<int:user_id>", methods=["POST"])
@super_admin_required
def rechazar_usuario(user_id):
    """Super Admin rechaza un usuario: se elimina el registro (el Admin puede crear otro si lo desea)."""
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute(_sql("DELETE FROM users WHERE id = %s"), (user_id,))
        conn.commit()
        flash("Usuario rechazado y eliminado.")
    finally:
        conn.close()
    return redirect(url_for("usuarios_pendientes"))


# =====================================================
# SUPER ADMIN: CONTROL DE BANCAS (PAGOS)
# Solo super_admin. Activar/suspender bancas por falta de pago.
# =====================================================
@app.route("/superadmin/bancas", methods=["GET", "POST"])
@super_admin_required
def superadmin_bancas():
    """Panel Super Admin: listar bancas (admins), estado y próxima fecha de pago. Solo super_admin."""
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute(_sql("""
            SELECT username, COALESCE(banca_status, 'active') AS banca_status, banca_due_date
            FROM users
            WHERE role = %s
            ORDER BY username
        """), (ROLE_ADMIN,))
        rows = cur.fetchall()
        bancas = []
        for r in rows:
            d = dict(r) if hasattr(r, "keys") else {"username": r[0], "banca_status": r[1] if len(r) > 1 else "active", "banca_due_date": r[2] if len(r) > 2 else None}
            due = d.get("banca_due_date")
            if due:
                try:
                    if hasattr(due, "strftime"):
                        due_str = due.strftime("%d/%m/%Y") if hasattr(due, "strftime") else str(due)[:10]
                    else:
                        due_str = str(due)[:10]
                        if len(due_str) >= 10:
                            parts = due_str.split("-")
                            if len(parts) == 3:
                                due_str = parts[2] + "/" + parts[1] + "/" + parts[0]
                except Exception:
                    due_str = str(due)[:10]
            else:
                due_str = "—"
            due_val = d.get("banca_due_date")
            due_iso = ""
            if due_val:
                if hasattr(due_val, "strftime"):
                    due_iso = due_val.strftime("%Y-%m-%d")
                else:
                    due_iso = str(due_val)[:10]
            bancas.append({
                "username": d.get("username") or "",
                "status": (d.get("banca_status") or "active").lower(),
                "due_date": due_val,
                "due_display": due_str,
                "due_iso": due_iso,
            })
    finally:
        conn.close()

    return render_template_string(IOS + """
    <a href="/admin" class="venta-btn" style="position:fixed;top:12px;right:12px">⬅ Admin</a>
    <div style="max-width:900px;margin:60px auto;padding:24px;background:#fff;border-radius:16px;box-shadow:0 10px 40px rgba(0,0,0,.1);">
        <h2 style="color:#002D62;margin-bottom:8px">🏦 Control de Bancas (Super Admin)</h2>
        <p style="color:#64748b;margin-bottom:24px">Gestione el estado de pago de cada banca. Si no pagan, suspenda el acceso.</p>
        {% for msg in get_flashed_messages() %}<p style="background:#dbeafe;color:#1e40af;padding:10px;border-radius:8px;margin-bottom:16px">{{ msg }}</p>{% endfor %}
        <table width="100%" style="border-collapse:collapse;font-size:14px">
            <thead>
                <tr style="background:#002D62;color:#fff">
                    <th style="padding:10px;text-align:left">Banca</th>
                    <th style="padding:10px;text-align:left">Usuario</th>
                    <th style="padding:10px;text-align:left">Próximo Pago</th>
                    <th style="padding:10px;text-align:left">Estado</th>
                    <th style="padding:10px;text-align:left">Acción</th>
                </tr>
            </thead>
            <tbody>
            {% for b in bancas %}
                <tr style="border-bottom:1px solid #e2e8f0">
                    <td style="padding:10px">{{ b.username }}</td>
                    <td style="padding:10px">{{ b.username }}</td>
                    <td style="padding:10px">
                        <form method="post" action="/superadmin/bancas/fecha/{{ b.username | urlencode }}" style="display:inline-flex;gap:6px;align-items:center;flex-wrap:wrap">
                            <input type="date" name="due_date" value="{{ b.due_iso }}" style="padding:6px;border:1px solid #cbd5e1;border-radius:6px">
                            <button type="submit" style="padding:6px 12px;background:#64748b;color:#fff;border:none;border-radius:6px;cursor:pointer">Guardar</button>
                        </form>
                        <span style="color:#64748b;margin-left:4px">{{ b.due_display }}</span>
                    </td>
                    <td style="padding:10px">
                        {% if b.status == 'active' %}<span style="color:#16a34a;font-weight:bold">Activa</span>{% else %}<span style="color:#dc2626;font-weight:bold">Suspendida</span>{% endif %}
                    </td>
                    <td style="padding:10px">
                        {% if b.status == 'active' %}
                        <form method="post" action="/superadmin/bancas/suspender/{{ b.username | urlencode }}" style="display:inline" onsubmit="return confirm('¿Suspender esta banca?');">
                            <button type="submit" style="padding:6px 12px;background:#dc2626;color:#fff;border:none;border-radius:6px;cursor:pointer">Suspender</button>
                        </form>
                        {% else %}
                        <form method="post" action="/superadmin/bancas/activar/{{ b.username | urlencode }}" style="display:inline">
                            <button type="submit" style="padding:6px 12px;background:#16a34a;color:#fff;border:none;border-radius:6px;cursor:pointer">Activar</button>
                        </form>
                        {% endif %}
                    </td>
                </tr>
            {% else %}
                <tr><td colspan="5" style="padding:20px;text-align:center;color:#64748b">No hay bancas (usuarios admin) registradas.</td></tr>
            {% endfor %}
            </tbody>
        </table>
        <p style="margin-top:24px"><a href="/admin" style="color:#002D62;font-weight:bold">← Volver al panel Admin</a></p>
    </div>
    """, bancas=bancas)


@app.route("/superadmin/bancas/activar/<username>", methods=["POST"])
@super_admin_required
def superadmin_bancas_activar(username):
    """Super Admin activa una banca. Solo super_admin."""
    from urllib.parse import unquote
    username = unquote(username or "").strip()
    if not username:
        flash("Usuario inválido")
        return redirect("/superadmin/bancas")
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute(_sql("UPDATE users SET banca_status = %s WHERE username = %s AND role = %s"), ("active", username, ROLE_ADMIN))
        conn.commit()
        flash("Banca activada: " + username)
    finally:
        conn.close()
    return redirect("/superadmin/bancas")


@app.route("/superadmin/bancas/suspender/<username>", methods=["POST"])
@super_admin_required
def superadmin_bancas_suspender(username):
    """Super Admin suspende una banca. Solo super_admin."""
    from urllib.parse import unquote
    username = unquote(username or "").strip()
    if not username:
        flash("Usuario inválido")
        return redirect("/superadmin/bancas")
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute(_sql("UPDATE users SET banca_status = %s WHERE username = %s AND role = %s"), ("suspended", username, ROLE_ADMIN))
        conn.commit()
        flash("Banca suspendida: " + username)
    finally:
        conn.close()
    return redirect("/superadmin/bancas")


@app.route("/superadmin/bancas/fecha/<username>", methods=["POST"])
@super_admin_required
def superadmin_bancas_fecha(username):
    """Super Admin asigna próxima fecha de pago a una banca. Solo super_admin."""
    from urllib.parse import unquote
    username = unquote(username or "").strip()
    due_str = (request.form.get("due_date") or "").strip()[:10]
    if not username:
        flash("Usuario inválido")
        return redirect("/superadmin/bancas")
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        if due_str:
            cur.execute(_sql("UPDATE users SET banca_due_date = %s WHERE username = %s AND role = %s"), (due_str, username, ROLE_ADMIN))
        else:
            cur.execute(_sql("UPDATE users SET banca_due_date = NULL WHERE username = %s AND role = %s"), (username, ROLE_ADMIN))
        conn.commit()
        flash("Fecha de pago actualizada: " + username)
    finally:
        conn.close()
    return redirect("/superadmin/bancas")


@app.route("/usuarios")
@admin_required
def ver_usuarios():
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, username, role, COALESCE(meta, 0) AS meta, approved FROM users")
        users = cur.fetchall()
    finally:
        conn.close()

    return render_template_string(IOS + """
    <a href="/venta" class="venta-btn">💰 Venta</a>                              
    <div class="card">
        <h2>👥 Usuarios</h2>
        {% for msg in get_flashed_messages() %}
        <p style="background:#fee2e2;color:#991b1b;padding:10px;border-radius:8px;margin-bottom:12px">{{ msg }}</p>
        {% endfor %}

        <table width="100%">
            <tr>
                <th>ID</th>
                <th>Usuario</th>
                <th>Rol</th>
                <th>Estado</th>
                <th>Meta diaria (RD$)</th>
                <th>Acción</th>
            </tr>

            {% for u in users %}
            <tr>
                <td>{{ u.id }}</td>
                <td>{{ u.username }}</td>
                <td>{{ u.role }}</td>
                <td>{% if u.approved %}<span style="color:#16a34a">✓ Aprobado</span>{% else %}<span style="color:#b45309">⏳ Pendiente</span>{% endif %}</td>
                <td>
                    <form method="post" action="/actualizar_meta_usuario/{{ u.id }}">
                        <input name="meta" type="number" min="0" step="100" value="{{ '%.0f'|format(u.meta or 0) }}" style="width:110px">
                        <button style="margin-top:4px;background:#007bff">Guardar</button>
                    </form>
                </td>
                <td>
                    {% if u.username != session.get("u") %}
                    <form method="post" action="/eliminar_usuario/{{ u.id }}">
                        <button style="background:#CE1126">Eliminar</button>
                    </form>
                    {% else %}
                    🔒
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </table>

        <br>
        <a href="/crear_usuario">➕ Crear Usuario</a>
        <br><br>
        <a href="/admin">⬅ Volver</a>
    </div>
    """, users=users)





# =====================================================
# DASHBOARD ADMIN
# =====================================================
@app.route("/admin", methods=["GET","POST"])
def admin():
    try:
        return _admin_impl()
    except Exception as e:
        traceback.print_exc()
        raise


def _admin_impl():
    # ===============================
    # SEGURIDAD
    # ===============================
    if not is_admin_or_super():
        return redirect("/venta")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    cur = c.cursor()
    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    hoy = hoy_rd

    # ===============================
    # VENTAS HOY (total vendido hoy, hora RD)
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
            SELECT COALESCE(SUM(tl.amount), 0) AS ventas_hoy
            FROM ticket_lines tl
            JOIN tickets t ON t.id = tl.ticket_id
            WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(tl.estado, 'activo') != 'cancelado'
        """, (hoy_rd,))
    else:
        cur.execute(_sql("""
            SELECT COALESCE(SUM(tl.amount), 0) AS ventas_hoy
            FROM ticket_lines tl
            JOIN tickets t ON t.id = tl.ticket_id
            WHERE DATE(t.created_at) = %s
            AND COALESCE(tl.estado, 'activo') != 'cancelado'
        """), (hoy_rd,))
    row = cur.fetchone()
    ventas_hoy = (list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)) or 0
    total = ventas_hoy

    # ===============================
    # VENTAS POR LOTERÍA (FIX BANCA REAL)
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
            SELECT 
                ticket_lines.lottery,
                COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            GROUP BY ticket_lines.lottery
            ORDER BY ticket_lines.lottery
        """, (hoy,))
    else:
        cur.execute(_sql("""
            SELECT 
                ticket_lines.lottery,
                COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at)=%s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            GROUP BY ticket_lines.lottery
            ORDER BY ticket_lines.lottery
        """), (hoy,))
    por_loteria = cur.fetchall()

    # ===============================
    # VENTAS POR CAJERO (NO CANCELADOS)
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
            SELECT 
                tickets.cajero,
                COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            GROUP BY tickets.cajero
        """, (hoy,))
    else:
        cur.execute(_sql("""
            SELECT 
                tickets.cajero,
                COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at)=%s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            GROUP BY tickets.cajero
        """), (hoy,))
    por_cajero = cur.fetchall()

    # ===============================
    # GANANCIA REAL BANCA: Profit = Total Sales - Total Prizes Paid
    # ===============================
    cur.execute("""
        SELECT COALESCE(SUM(amount),0)
        FROM ticket_lines
        WHERE estado!='cancelado' OR estado IS NULL
    """)
    row = cur.fetchone()
    total_sales = (list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)) or 0

    total_prizes_paid = 0
    try:
        cur.execute("SELECT COALESCE(SUM(monto),0) FROM pagos")
        row = cur.fetchone()
        total_prizes_paid += float((list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)) or 0)
    except Exception:
        pass
    try:
        cur.execute("SELECT COALESCE(SUM(monto),0) FROM pagos_premios")
        row = cur.fetchone()
        total_prizes_paid += float((list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)) or 0)
    except Exception:
        pass

    profit = total_sales - total_prizes_paid

    # ===============================
    # VERIFICAR CIERRE CAJA
    # ===============================
    try:
        cur.execute(_sql("""
            SELECT COUNT(*) FROM cash_closings WHERE date=%s
        """), (hoy,))
        row = cur.fetchone()
        n = list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)
        cerrado = (n or 0) > 0
    except Exception:
        cerrado = False

    # ===============================
    # POST → CIERRE / APERTURA / RIESGO
    # ===============================
    if request.method == "POST":

        # ===============================
        # GUARDAR RIESGO DESDE SLIDER
        # ===============================
        # ===============================
        # 🔒 CERRAR CAJA
        # ===============================
        if request.form.get("accion") == "cerrar" and not cerrado:

            cur.execute(_sql("""
                INSERT INTO cash_closings (date, cajero, total, created_at)
                VALUES (%s, %s, %s, %s)
            """), (
                hoy,
                session.get("u"),
                total,
                ahora_rd().strftime("%Y-%m-%d %H:%M:%S")
            ))

            c.commit()
            c.close()
            return redirect("/admin")

        # ===============================
        # 🔓 ABRIR CAJA
        # ===============================
        if request.form.get("accion") == "abrir" and cerrado:

            cur.execute(_sql("""
                DELETE FROM cash_closings
                WHERE date = %s
            """), (hoy,))

            c.commit()
            c.close()
            return redirect("/admin")

    # ===============================
    # CALCULOS DASHBOARD (FUERA DEL POST)
    # ===============================
    try:
        peligrosos = numeros_peligrosos()
    except Exception:
        peligrosos = []
    try:
        ranking = ranking_numeros()
    except Exception:
        ranking = []

    # ===============================
    # LOTERÍAS CON ESTADO (ABIERTA/CERRADA)
    # ===============================
    cur.execute("SELECT lottery, draw FROM lotteries ORDER BY lottery, draw")
    lotteries = []
    for r in cur.fetchall():
        row = dict(r)
        row["estado"] = estado_loteria(row.get("lottery"), row.get("draw"))
        lotteries.append(row)

    # ===============================
    # HTML
    # ===============================
    html = IOS + """

<style>

/* ===== DASHBOARD CONTAINER ===== */
.dashboard{
max-width:1100px;
margin:auto;
padding:24px;
}

/* ===== TARJETAS ===== */
.card{
background:white;
border-radius:14px;
padding:16px;
box-shadow:0 10px 25px rgba(0,0,0,0.15);
border:none;
}

/* ===== TARJETAS PEQUEÑAS (Ventas Hoy, Ganancia) ===== */
.metrics .card.metric-card{
width:220px;
min-height:120px;
flex-shrink:0;
}

.metrics .card.metric-card .metric{
font-size:26px;
font-weight:900;
color:#002D62;
}

/* ===== TARJETA ESTADO LOTERÍAS (más grande) ===== */
.metrics .card.loterias-card{
width:320px;
min-width:280px;
height:auto;
flex:1;
}

/* ===== BADGES ESTADO LOTERÍAS (verde/rojo) ===== */
.badge{padding:4px 10px;border-radius:6px;font-weight:bold;font-size:12px;}
.badge.bg-success{background:#16a34a;color:white;}
.badge.bg-danger{background:#dc2626;color:white;}

/* ===== TITULOS ===== */
.card h3{
margin:0 0 8px 0;
color:#002D62;
font-size:1rem;
}

/* ===== VALORES GRANDES ===== */
.metric{
font-size:26px;
font-weight:900;
color:#002D62;
}

/* ===== FILA SUPERIOR: 3 TARJETAS ===== */
.metrics{
display:flex;
flex-wrap:wrap;
gap:20px;
margin-bottom:25px;
align-items:stretch;
}
@media (max-width:768px){
.metrics{flex-direction:column;}
.metrics .card.metric-card{width:100%;}
.metrics .card.loterias-card{width:100%;}
}

/* ===== GRID BOTONES 2 COLUMNAS ===== */
.panel{
display:grid;
grid-template-columns:repeat(2,1fr);
gap:16px;
margin-top:20px;
}
@media (max-width:600px){
.panel{grid-template-columns:1fr;}
}

/* ===== BOTONES ADMIN ===== */
.admin-btn{
background:#002D62;
color:white;
padding:18px;
border-radius:16px;
text-align:center;
font-weight:800;
text-decoration:none;
transition:.2s;
display:block;
}

.admin-btn:hover{
background:#CE1126;
transform:translateY(-3px);
}

</style>


<div class="dashboard">

<h2 style="text-align:center;margin-bottom:30px">
📊 Panel Administrativo
</h2>
{% for msg in get_flashed_messages() %}
<div style="background:#fee2e2;color:#991b1b;padding:12px;border-radius:8px;margin-bottom:12px;font-weight:bold;text-align:center">{{ msg }}</div>
{% endfor %}

<!-- ===== FILA SUPERIOR: 3 TARJETAS ===== -->
<div class="metrics">

<div class="card metric-card"
style="background:{% if ventas_hoy > 0 %}#dcfce7{% else %}white{% endif %};">
<h3>💰 Ventas Hoy</h3>
<div class="metric"
style="color:{% if ventas_hoy > 0 %}#16a34a{% else %}#1e293b{% endif %};font-weight:900;">
RD$ {{ "%.2f"|format(ventas_hoy) }}
</div>
</div>

<div class="card metric-card"
style="background:{% if ganancia >= 0 %}#dcfce7{% else %}#fee2e2{% endif %};">
<h3>🏦 Ganancia</h3>
<div class="metric"
style="color:{% if ganancia >= 0 %}#16a34a{% else %}#dc2626{% endif %};font-weight:900;">
RD$ {{ "%.2f"|format(ganancia) }}
</div>
</div>

<div class="card loterias-card">
<h3>🎰 Estado de loterías <span style="font-size:11px;color:#64748b;font-weight:normal">(hora Santo Domingo, actualiza cada 30s)</span></h3>
<table style="width:100%;border-collapse:collapse;margin-top:10px;font-size:14px">
<thead>
<tr style="background:#f1f5f9">
<th style="padding:8px;text-align:left;border:1px solid #e2e8f0">Lotería</th>
<th style="padding:8px;text-align:left;border:1px solid #e2e8f0">Sorteo</th>
<th style="padding:8px;text-align:left;border:1px solid #e2e8f0">Estado</th>
</tr>
</thead>
<tbody id="estado-loterias-tbody">
{% for l in lotteries %}
<tr>
<td style="padding:8px;border:1px solid #e2e8f0">{{ l.lottery }}</td>
<td style="padding:8px;border:1px solid #e2e8f0">{{ l.draw }}</td>
<td style="padding:8px;border:1px solid #e2e8f0">
{% if l.estado == "abierta" %}
<span class="badge bg-success">ABIERTA</span>
{% else %}
<span class="badge bg-danger">CERRADA</span>
{% endif %}
</td>
</tr>
{% endfor %}
</tbody>
</table>
</div>
<script>
(function(){
var tbody = document.getElementById("estado-loterias-tbody");
if(!tbody) return;
function esc(s){ var d=document.createElement("div"); d.textContent=s==null?"":s; return d.innerHTML; }
function actualizarEstado(){
fetch("/api/estado_loterias").then(function(r){ return r.json(); }).then(function(data){
var list = data.lotteries || [];
var html = "";
for(var i = 0; i < list.length; i++){
var l = list[i];
var badge = l.estado === "abierta" ? '<span class="badge bg-success">ABIERTA</span>' : '<span class="badge bg-danger">CERRADA</span>';
html += "<tr><td style=\"padding:8px;border:1px solid #e2e8f0\">" + esc(l.lottery) + "</td><td style=\"padding:8px;border:1px solid #e2e8f0\">" + esc(l.draw) + "</td><td style=\"padding:8px;border:1px solid #e2e8f0\">" + badge + "</td></tr>";
}
tbody.innerHTML = html;
}).catch(function(){});
}
setInterval(actualizarEstado, 30000);
})();
</script>

</div>

<!-- ===== GRID BOTONES 2 COLUMNAS ===== -->
<div class="panel">
{% if session.get("role") in ["admin", "super_admin"] %}
<a href="/admin/auditoria" class="admin-btn">🔍 Auditoría Sistema</a>
{% endif %}
<a href="/admin/limites" class="admin-btn">🎛️ Control Límites</a>
<a href="/crear_usuario" class="admin-btn">👤 Crear Usuario</a>
{% if session.get("role") == "super_admin" %}<a href="/crear_admin" class="admin-btn">👤 Crear Admin (solo Super Admin)</a>{% endif %}
{% if session.get("role") == "super_admin" %}<a href="/superadmin/bancas" class="admin-btn">🏦 Control de Bancas (pagos)</a>{% endif %}
{% if session.get("role") == "super_admin" %}<a href="/usuarios_pendientes" class="admin-btn">⏳ Aprobar usuarios pendientes</a>{% endif %}
{% if session.get("role") == "admin" %}<a href="/solicitar_admin" class="admin-btn">📋 Solicitar nuevo Admin</a>{% endif %}
<a href="/admin/metas" class="admin-btn">🎯 Asignar Metas Cajeros</a>
<form method="post" action="/reset_sistema" style="grid-column:1/-1">
<button class="admin-btn" style="width:100%">🗑 Reset Sistema</button>
</form>
<a href="/admin/imprimir_cierre" class="admin-btn">🧾 Imprimir Cierre</a>
<a href="/reporte_hoy" class="admin-btn">📆 Ventas del d&iacute;a</a>
<a href="/reporte_semanal" class="admin-btn">📅 Ventas semanales</a>
<a href="/reporte_mensual" class="admin-btn">📊 Ventas mensuales</a>
<a href="/numeros_populares" class="admin-btn">🔢 N&uacute;meros m&aacute;s jugados</a>
<a href="/admin/pagos" class="admin-btn">💵 Historial Pagos</a>
<a href="/admin/banco_cajeros" class="admin-btn">🏦 Banco Cajeros</a>
<a href="/ventas_cajeros" class="admin-btn">📊 Ventas por Cajero</a>
<button type="button" class="admin-btn" onclick="document.getElementById('modalPanelJugadas').style.display='flex'" style="width:100%;border:none;cursor:pointer;font-size:1em">📊 Panel de Jugadas</button>
{% if cerrado %}
<form method="post">
<input type="hidden" name="accion" value="abrir">
<button class="admin-btn" style="width:100%">🔓 Abrir Caja</button>
</form>
{% else %}
<form method="post">
<input type="hidden" name="accion" value="cerrar">
<button class="admin-btn" style="width:100%">🔒 Cerrar Caja</button>
</form>
{% endif %}
</div>

<!-- Modal clave Panel de Jugadas -->
<div id="modalPanelJugadas" style="display:none;position:fixed;z-index:9999;left:0;top:0;width:100%;height:100%;background:rgba(0,0,0,0.5);align-items:center;justify-content:center;flex-direction:column;flex-wrap:wrap">
<div style="background:white;padding:28px;border-radius:16px;max-width:360px;width:90%;box-shadow:0 20px 60px rgba(0,0,0,0.3)">
<h3 style="margin:0 0 16px 0;text-align:center">Panel de Jugadas</h3>
<p style="color:#64748b;margin:0 0 16px 0;text-align:center">Ingrese clave de administrador</p>
<form method="post" action="/panel-jugadas/verificar-clave">
<input type="password" name="clave" placeholder="Clave" required autofocus style="width:100%;padding:12px;border:1px solid #e2e8f0;border-radius:8px;font-size:16px;box-sizing:border-box;margin-bottom:12px">
<button type="submit" style="width:100%;padding:12px;background:#002D62;color:white;border:none;border-radius:8px;font-weight:bold;cursor:pointer">Acceder</button>
</form>
<button type="button" onclick="document.getElementById('modalPanelJugadas').style.display='none'" style="width:100%;margin-top:10px;padding:8px;background:#94a3b8;color:white;border:none;border-radius:8px;cursor:pointer">Cancelar</button>
</div>
</div>
<script>
document.getElementById('modalPanelJugadas').onclick=function(e){if(e.target===this)this.style.display='none'};
</script>

</div>
"""

    c.close()

    return render_template_string(
        html,
        ventas_hoy=total,
        total=total,
        por_loteria=por_loteria,
        por_cajero=por_cajero,
        cerrado=cerrado,
        ventas=total_sales,
        pagado=total_prizes_paid,
        ganancia=profit,
        peligrosos=peligrosos,
        ranking=ranking,
        lotteries=lotteries
    )


# ===============================
# PANEL DE JUGADAS (clave 0219)
# ===============================
PANEL_JUGADAS_CLAVE = "0219"

@app.route("/panel-jugadas/verificar-clave", methods=["POST"])
def panel_jugadas_verificar_clave():
    clave = (request.form.get("clave") or "").strip()
    if clave == PANEL_JUGADAS_CLAVE:
        session["panel_jugadas_ok"] = True
        return redirect("/panel-jugadas")
    flash("Clave incorrecta")
    return redirect("/admin")

@app.route("/panel-jugadas")
def panel_jugadas():
    if not session.get("panel_jugadas_ok"):
        return redirect("/admin")
    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    periodo = request.args.get("periodo", "hoy")  # hoy, semana, mes
    cajero_filtro = request.args.get("cajero", "").strip()
    loteria_filtro = request.args.get("loteria", "").strip()
    buscar = request.args.get("buscar", "").strip()
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    cur = conn.cursor()
    # Resumen general (siempre de hoy)
    if os.environ.get("DATABASE_URL"):
        cur.execute(_sql("""
            SELECT COUNT(DISTINCT tickets.id) AS tickets_hoy,
                   COALESCE(SUM(ticket_lines.amount),0) AS total_hoy
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        """), (hoy_rd,))
    else:
        cur.execute(_sql("""
            SELECT COUNT(DISTINCT tickets.id) AS tickets_hoy,
                   COALESCE(SUM(ticket_lines.amount),0) AS total_hoy
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at) = %s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        """), (hoy_rd,))
    row = cur.fetchone()
    tickets_hoy = int(row["tickets_hoy"] if hasattr(row, "keys") else (row[0] or 0))
    total_hoy = float(row["total_hoy"] if hasattr(row, "keys") else (row[1] or 0))
    # Total jugadas (líneas) hoy
    if os.environ.get("DATABASE_URL"):
        cur.execute(_sql("""
            SELECT COUNT(*) AS n FROM ticket_lines tl
            JOIN tickets t ON t.id = tl.ticket_id
            WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(tl.estado,'activo') != 'cancelado'
        """), (hoy_rd,))
    else:
        cur.execute(_sql("""
            SELECT COUNT(*) AS n FROM ticket_lines tl
            JOIN tickets t ON t.id = tl.ticket_id
            WHERE DATE(t.created_at) = %s AND COALESCE(tl.estado,'activo') != 'cancelado'
        """), (hoy_rd,))
    rn = cur.fetchone()
    total_jugadas_hoy = int(rn["n"] if hasattr(rn, "keys") else (rn[0] or 0))
    # Listas para filtros: cajeros de tickets + usuarios con role cajero/admin para que aparezcan todos
    cur.execute("SELECT DISTINCT cajero FROM tickets WHERE cajero IS NOT NULL AND cajero != '' ORDER BY cajero")
    cajeros_list = [r["cajero"] if hasattr(r, "keys") else r[0] for r in (cur.fetchall() or [])]
    cur.execute(_sql("SELECT username FROM users WHERE role IN (%s, %s, %s) ORDER BY username"), ("cajero", "user", "admin"))
    for r in (cur.fetchall() or []):
        u = r["username"] if hasattr(r, "keys") else r[0]
        if u and u not in cajeros_list:
            cajeros_list.append(u)
    cajeros_list.sort()
    cur.execute("SELECT DISTINCT lottery FROM ticket_lines WHERE lottery IS NOT NULL AND lottery != '' ORDER BY lottery")
    loterias_list = [r["lottery"] if hasattr(r, "keys") else r[0] for r in (cur.fetchall() or [])]
    # Query base jugadas
    ph = _ph()
    where_parts = ["COALESCE(ticket_lines.estado,'activo') != 'cancelado'"]
    params = []
    if os.environ.get("DATABASE_URL"):
        if periodo == "hoy":
            where_parts.append("(tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s")
            params.append(hoy_rd)
        elif periodo == "semana":
            where_parts.append("tickets.created_at >= CURRENT_DATE - INTERVAL '7 days'")
        elif periodo == "mes":
            where_parts.append("tickets.created_at >= CURRENT_DATE - INTERVAL '30 days'")
    else:
        if periodo == "hoy":
            where_parts.append("DATE(tickets.created_at) = %s")
            params.append(hoy_rd)
        elif periodo == "semana":
            where_parts.append("DATE(tickets.created_at) >= DATE('now','-7 days')")
        elif periodo == "mes":
            where_parts.append("DATE(tickets.created_at) >= DATE('now','-30 days')")
    if cajero_filtro:
        where_parts.append("tickets.cajero = %s")
        params.append(cajero_filtro)
    if loteria_filtro:
        where_parts.append("ticket_lines.lottery = %s")
        params.append(loteria_filtro)
    if buscar:
        where_parts.append("(CAST(tickets.id AS TEXT) LIKE %s OR ticket_lines.number LIKE %s OR tickets.cajero LIKE %s)")
        params.extend(["%" + buscar + "%", "%" + buscar + "%", "%" + buscar + "%"])
    where_sql = " AND ".join(where_parts)
    order_sql = "ORDER BY tickets.created_at DESC LIMIT 500"
    q = """
    SELECT tickets.id AS ticket, tickets.created_at,
           COALESCE(users.username, tickets.cajero) AS cajero,
           ticket_lines.lottery, ticket_lines.number AS numero, ticket_lines.amount AS monto
    FROM tickets
    JOIN ticket_lines ON tickets.id = ticket_lines.ticket_id
    LEFT JOIN users ON users.username = tickets.cajero
    WHERE """ + where_sql + " " + order_sql
    cur.execute(_sql(q), params)
    rows = cur.fetchall()
    jugadas = []
    for r in rows:
        d = dict(r) if hasattr(r, "keys") else {}
        if not d:
            d = {"ticket": r[0], "created_at": r[1], "cajero": r[2], "lottery": r[3], "numero": r[4], "monto": r[5]}
        created = d.get("created_at")
        if created and hasattr(created, "strftime"):
            hora_str = created.strftime("%I:%M %p") if hasattr(created, "strftime") else str(created)
        else:
            try:
                hora_str = datetime.strptime(str(created)[:19], "%Y-%m-%d %H:%M:%S").strftime("%I:%M %p") if created else ""
            except Exception:
                hora_str = str(created) if created else ""
        jugadas.append({
            "hora": hora_str,
            "ticket": d.get("ticket"),
            "cajero": d.get("cajero") or "",
            "lottery": d.get("lottery") or "",
            "numero": d.get("numero") or "",
            "monto": float(d.get("monto") or 0),
        })
    # Resumen por cajero (si hay filtro cajero)
    resumen_cajero = None
    numeros_top = []
    if cajero_filtro:
        cur.execute(_sql("""
            SELECT COUNT(*) AS jugadas, COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE tickets.cajero = %s AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        """), (cajero_filtro,))
        rc = cur.fetchone()
        if rc:
            rc = dict(rc) if hasattr(rc, "keys") else {"jugadas": rc[0], "total": rc[1]}
            resumen_cajero = {"cajero": cajero_filtro, "jugadas": int(rc.get("jugadas") or 0), "total": float(rc.get("total") or 0)}
        cur.execute(_sql("""
            SELECT ticket_lines.number AS numero, SUM(ticket_lines.amount) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE tickets.cajero = %s AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            GROUP BY ticket_lines.number
            ORDER BY total DESC
            LIMIT 20
        """), (cajero_filtro,))
        for r in cur.fetchall() or []:
            ro = dict(r) if hasattr(r, "keys") else {"numero": r[0], "total": r[1]}
            numeros_top.append({"numero": ro.get("numero") or "", "total": float(ro.get("total") or 0)})
    conn.close()
    from urllib.parse import urlencode
    def build_url(p):
        d = {"periodo": p}
        if cajero_filtro:
            d["cajero"] = cajero_filtro
        if loteria_filtro:
            d["loteria"] = loteria_filtro
        if buscar:
            d["buscar"] = buscar
        return "/panel-jugadas?" + urlencode(d)
    url_hoy = build_url("hoy")
    url_semana = build_url("semana")
    url_mes = build_url("mes")
    html_panel = _panel_jugadas_html(
        tickets_hoy=tickets_hoy, total_hoy=total_hoy, total_jugadas_hoy=total_jugadas_hoy,
        jugadas=jugadas, cajeros_list=cajeros_list, loterias_list=loterias_list,
        periodo=periodo, cajero_filtro=cajero_filtro, loteria_filtro=loteria_filtro, buscar=buscar,
        resumen_cajero=resumen_cajero, numeros_top=numeros_top,
        url_hoy=url_hoy, url_semana=url_semana, url_mes=url_mes,
    )
    return render_template_string(html_panel)


def _panel_jugadas_html(tickets_hoy, total_hoy, total_jugadas_hoy, jugadas, cajeros_list, loterias_list,
                        periodo, cajero_filtro, loteria_filtro, buscar, resumen_cajero, numeros_top,
                        url_hoy, url_semana, url_mes):
    return """
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Panel de Jugadas</title>
<style>
*{box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#f1f5f9;margin:0;padding:16px;color:#1e293b}
.wrap{max-width:1200px;margin:0 auto}
h1{font-size:1.5rem;margin:0 0 20px 0;color:#0f172a}
.summary-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:24px}
.summary-cards .card{background:linear-gradient(135deg,#0f172a,#1e293b);color:#fff;padding:16px;border-radius:12px;box-shadow:0 4px 12px rgba(0,0,0,.1)}
.summary-cards .card .label{font-size:0.8rem;opacity:.9}
.summary-cards .card .value{font-size:1.4rem;font-weight:800}
.filters{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:16px;background:#fff;padding:14px;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.filters a{color:#0f172a;text-decoration:none;padding:8px 14px;border-radius:8px;font-weight:600;background:#e2e8f0}
.filters a:hover,.filters a.active{background:#002D62;color:#fff}
.filters select{padding:8px 12px;border:1px solid #e2e8f0;border-radius:8px;font-size:14px}
.filters input[type=text]{padding:8px 12px;border:1px solid #e2e8f0;border-radius:8px;min-width:180px}
.resumen-cajero{background:#fff;padding:16px;border-radius:12px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.08);display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px}
.resumen-cajero .item .label{font-size:0.85rem;color:#64748b}
.resumen-cajero .item .value{font-size:1.2rem;font-weight:700;color:#0f172a}
.table-wrap{overflow-x:auto;background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:20px;max-height:70vh;overflow-y:auto}
.tbl{width:100%;border-collapse:collapse;min-width:600px}
.tbl th{background:#0f172a;color:#fff;padding:12px 14px;text-align:left;font-weight:600;font-size:0.9rem;position:sticky;top:0;z-index:1}
.tbl td{padding:10px 14px;border-bottom:1px solid #e2e8f0}
.tbl tbody tr:nth-child(even){background:#f8fafc}
.tbl tbody tr:hover{background:#e0f2fe}
.tbl td.num{text-align:right}
.numeros-top{background:#fff;padding:16px;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.numeros-top h3{margin:0 0 12px 0;font-size:1.1rem}
.numeros-top table{width:100%;max-width:320px;border-collapse:collapse}
.numeros-top th,.numeros-top td{padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0}
.numeros-top tr:hover{background:#f8fafc}
.back{display:inline-block;margin-bottom:16px;color:#002D62;font-weight:700;text-decoration:none}
.back:hover{text-decoration:underline}
</style>
</head>
<body>
<div class="wrap">
<a href="/admin" class="back">&larr; Volver al panel</a>
<h1>Panel de Jugadas</h1>
<div class="summary-cards">
<div class="card"><span class="label">Tickets vendidos hoy</span><div class="value">""" + str(tickets_hoy) + """</div></div>
<div class="card"><span class="label">Total vendido hoy</span><div class="value">RD$ """ + ("%.2f" % total_hoy) + """</div></div>
<div class="card"><span class="label">Total jugadas hoy</span><div class="value">""" + str(total_jugadas_hoy) + """</div></div>
</div>
<form method="get" action="/panel-jugadas" class="filters" id="filtersForm">
<input type="hidden" name="periodo" value=\"""" + (periodo or "hoy") + """\">
<a href=\"""" + url_hoy + """\" class=\"""" + ("active" if periodo == "hoy" else "") + """\">Hoy</a>
<a href=\"""" + url_semana + """\" class=\"""" + ("active" if periodo == "semana" else "") + """\">Semana</a>
<a href=\"""" + url_mes + """\" class=\"""" + ("active" if periodo == "mes" else "") + """\">Mes</a>
<select name="cajero" onchange="this.form.submit()">
<option value="">Todos los cajeros</option>
""" + "".join('<option value="' + quote(c) + '"' + (" selected" if c == cajero_filtro else "") + ">" + (c or "Sin nombre") + "</option>" for c in cajeros_list) + """
</select>
<select name="loteria" onchange="this.form.submit()">
<option value="">Todas las loterías</option>
""" + "".join('<option value="' + quote(l) + '"' + (" selected" if l == loteria_filtro else "") + ">" + (l or "") + "</option>" for l in loterias_list) + """
</select>
<input type="text" name="buscar" placeholder="Buscar ticket, número o cajero" value=\"""" + (buscar.replace('"', '&quot;') if buscar else "") + """\">
<button type="submit">Buscar</button>
</form>
""" + ("""
<div class="resumen-cajero">
<div class="item"><span class="label">Cajero</span><span class="value">""" + (resumen_cajero["cajero"] or "") + """</span></div>
<div class="item"><span class="label">Total jugadas</span><span class="value">""" + str(resumen_cajero["jugadas"]) + """</span></div>
<div class="item"><span class="label">Total vendido</span><span class="value">RD$ """ + ("%.2f" % resumen_cajero["total"]) + """</span></div>
</div>
""" if resumen_cajero else "") + """
<div class="table-wrap">
<table class="tbl" id="tblJugadas">
<thead><tr>
<th data-col="hora">Hora</th>
<th data-col="ticket">Ticket</th>
<th data-col="cajero">Cajero</th>
<th data-col="lottery">Lotería</th>
<th data-col="numero">Número</th>
<th data-col="monto" class="num">Monto</th>
</tr></thead>
<tbody>
""" + "".join(
    "<tr><td>" + (j.get("hora") or "") + "</td><td>" + str(j.get("ticket") or "") + "</td><td>" + (j.get("cajero") or "") + "</td><td>" + (j.get("lottery") or "") + "</td><td>" + (j.get("numero") or "") + "</td><td class=\"num\">RD$ " + ("%.2f" % (j.get("monto") or 0)) + "</td></tr>"
    for j in jugadas
) + """
</tbody>
</table>
</div>
""" + ("""
<div class="numeros-top">
<h3>Números más jugados por ese cajero</h3>
<table><thead><tr><th>Número</th><th>Total jugado</th></tr></thead>
<tbody>
""" + "".join("<tr><td>" + (n.get("numero") or "") + "</td><td>RD$ " + ("%.2f" % n.get("total", 0)) + "</td></tr>" for n in numeros_top) + """
</tbody></table>
</div>
""" if numeros_top else "") + """
</div>
<script>
(function(){
var tbl = document.getElementById('tblJugadas');
if(!tbl) return;
var thead = tbl.querySelector('thead th');
var ths = tbl ? [].slice.call(tbl.querySelectorAll('thead th')) : [];
var body = tbl.querySelector('tbody');
var rows = body ? [].slice.call(body.querySelectorAll('tr')) : [];
var dir = 1;
function getCell(row, i){ return row.cells[i]; }
function sort(colIndex){
 dir = -dir;
 rows.sort(function(a,b){
  var va = (getCell(a,colIndex).textContent || '').trim();
  var vb = (getCell(b,colIndex).textContent || '').trim();
  var na = parseFloat(va.replace(/[^0-9.-]/g,'')) || 0;
  var nb = parseFloat(vb.replace(/[^0-9.-]/g,'')) || 0;
  if(colIndex===5) return (na - nb)*dir;
  return (va.localeCompare(vb))*dir;
 });
 rows.forEach(function(r){ body.appendChild(r); });
}
ths.forEach(function(th, i){ th.style.cursor='pointer'; th.addEventListener('click', function(){ sort(i); }); });
})();
</script>
</body>
</html>
"""


# ===============================
# HISTORIAL SEMANAL BONITO
# ===============================
@app.route("/historial_semanal")
def historial_semanal():
    if not is_staff():
        return redirect("/venta")
    usuario = session.get("u") or session.get("user") or ""
    es_admin = is_admin_or_super()
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if es_admin:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        t.id,
                        t.created_at,
                        COALESCE(SUM(tl.amount), 0) AS total
                    FROM tickets t
                    LEFT JOIN ticket_lines tl ON tl.ticket_id = t.id
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    WHERE t.created_at >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY t.id, t.created_at
                    ORDER BY t.created_at DESC
                """)
            else:
                cur.execute("""
                    SELECT
                        t.id,
                        t.created_at,
                        COALESCE(SUM(tl.amount), 0) AS total
                    FROM tickets t
                    LEFT JOIN ticket_lines tl ON tl.ticket_id = t.id
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    WHERE DATE(t.created_at) >= DATE('now','-7 days')
                    GROUP BY t.id, t.created_at
                    ORDER BY t.created_at DESC
                """)
        else:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        t.id,
                        t.created_at,
                        COALESCE(SUM(tl.amount), 0) AS total
                    FROM tickets t
                    LEFT JOIN ticket_lines tl ON tl.ticket_id = t.id
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    WHERE t.created_at >= CURRENT_DATE - INTERVAL '7 days'
                    AND t.cajero = %s
                    GROUP BY t.id, t.created_at
                    ORDER BY t.created_at DESC
                """, (usuario,))
            else:
                cur.execute(_sql("""
                    SELECT
                        t.id,
                        t.created_at,
                        COALESCE(SUM(tl.amount), 0) AS total
                    FROM tickets t
                    LEFT JOIN ticket_lines tl ON tl.ticket_id = t.id
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    WHERE DATE(t.created_at) >= DATE('now','-7 days')
                    AND t.cajero = %s
                    GROUP BY t.id, t.created_at
                    ORDER BY t.created_at DESC
                """), (usuario,))
        raw_tickets = cur.fetchall()
        tickets = []
        for r in (raw_tickets or []):
            if hasattr(r, "keys"):
                tickets.append({str(k): r[k] for k in r.keys()})
            else:
                try:
                    tickets.append({
                        "id": r[0],
                        "created_at": r[1],
                        "total": float(r[2]) if r[2] is not None else 0
                    })
                except (IndexError, TypeError):
                    pass
        # para el template: fecha = created_at formateado
        ventas = []
        for t in tickets:
            ca = t.get("created_at")
            if ca and hasattr(ca, "strftime"):
                fecha_str = ca.strftime("%Y-%m-%d %H:%M")
            else:
                fecha_str = str(ca)[:16] if ca else ""
            ventas.append({"fecha": fecha_str, "total": t.get("total", 0)})
    except Exception as e:
        if c:
            try:
                c.close()
            except Exception:
                pass
        print("ERROR HISTORIAL SEMANAL:", e)
        traceback.print_exc()
        return render_template_string(IOS + """
        <div class="card text-center">
            <h2 class="danger">Error en historial semanal</h2>
            <p>""" + str(e) + """</p>
            <a href="/admin">Volver al panel</a>
        </div>
        """), 500
    finally:
        try:
            c.close()
        except Exception:
            pass

    html = IOS + """
    <style>
    body{
        margin:0;
        font-family:Arial;
        background: linear-gradient(135deg,#002D62 0%,#ffffff 50%,#CE1126 100%);
    }

    .container{
        max-width:600px;
        margin:120px auto;
    }

    /* ===== BOTON PREMIUM PRINCIPAL (AZUL PRO) ===== */
.btn-main{
width:100%;
padding:16px;
border:none;
border-radius:18px;
font-size:16px;
font-weight:900;
color:white;
cursor:pointer;

background:linear-gradient(135deg,#002D62,#003DA5);
box-shadow:0 6px 0 #001a40, 0 18px 40px rgba(0,0,0,.25);

position:relative;
overflow:hidden;
transition:.25s;
}

/* brillo animado */
.btn-main::after{
content:"";
position:absolute;
top:0;
left:-100%;
width:100%;
height:100%;
background:linear-gradient(120deg,transparent,rgba(255,255,255,.5),transparent);
transition:.6s;
}

.btn-main:hover::after{
left:100%;
}

.btn-main:hover{
transform:translateY(-3px);
}

.btn-main:active{
transform:translateY(4px);
box-shadow:0 2px 0 #001a40;
}


/* ===== BOTON ROJO (ELIMINAR / RESET / BORRAR) ===== */
.btn-danger{
width:100%;
padding:16px;
border:none;
border-radius:18px;
font-size:16px;
font-weight:900;
color:white;
cursor:pointer;

background:linear-gradient(135deg,#CE1126,#8f0c1b);
box-shadow:0 6px 0 #5c0812, 0 18px 40px rgba(0,0,0,.25);
transition:.25s;
}

.btn-danger:hover{
transform:translateY(-3px);
}

.btn-danger:active{
transform:translateY(4px);
box-shadow:0 2px 0 #5c0812;
}


/* ===== BOTON SUAVE (LINKS / VOLVER) ===== */
.btn-soft{
display:block;
text-align:center;
padding:14px;
margin-top:12px;
border-radius:14px;
text-decoration:none;
font-weight:800;
color:#002D62;
background:#eef2ff;
transition:.25s;
}

.btn-soft:hover{
background:#dbe4ff;
}

    .card{
        background:white;
        padding:25px;
        border-radius:20px;
        border-top:6px solid #CE1126;
        box-shadow:0 20px 50px rgba(0,0,0,.15);
    }

    h2{text-align:center;color:#002D62;}

    table{width:100%;border-collapse:collapse;margin-top:20px;}
    th{background:#002D62;color:white;padding:12px;}
    td{padding:12px;border-bottom:1px solid #eee;text-align:center;}

    .btn{
        display:block;
        padding:12px;
        margin-top:15px;
        text-align:center;
        background:#002D62;
        color:white;
        border-radius:12px;
        text-decoration:none;
        font-weight:bold;
    }
    </style>

    <a href="/venta" class="venta-btn">💰 Venta</a>

    <div class="container">
        <div class="card">
            <h2>📅 Historial Semanal</h2>

            <table>
                <tr><th>Fecha</th><th>Total</th></tr>

                {% for v in ventas %}
                <tr>
                    <td>{{ v.fecha }}</td>
                    <td>${{ "%.2f"|format(v.total) }}</td>
                </tr>
                {% endfor %}
            </table>

            <a href="/admin" class="btn">← Volver al Dashboard</a>
        </div>
    </div>
    """

    return render_template_string(html, ventas=ventas)

# ===============================
# HISTORIAL MENSUAL BONITO
# ===============================
@app.route("/historial_mensual")
def historial_mensual():
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        # tablas: tickets, ticket_lines; columnas: tickets.id, tickets.created_at, ticket_lines.ticket_id, ticket_lines.amount
        if _reporte_sql_postgres():
            cur.execute("""
                SELECT
                    to_char((tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date, 'YYYY-MM') AS mes,
                    COALESCE(SUM(ticket_lines.amount), 0) AS total
                FROM ticket_lines
                JOIN tickets ON ticket_lines.ticket_id = tickets.id
                WHERE COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                GROUP BY to_char((tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date, 'YYYY-MM')
                ORDER BY mes DESC
            """)
        else:
            cur.execute("""
                SELECT
                    strftime('%Y-%m', tickets.created_at) AS mes,
                    COALESCE(SUM(ticket_lines.amount), 0) AS total
                FROM ticket_lines
                JOIN tickets ON ticket_lines.ticket_id = tickets.id
                WHERE COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                GROUP BY strftime('%Y-%m', tickets.created_at)
                ORDER BY mes DESC
            """)
        raw_ventas = cur.fetchall()
        ventas = []
        for r in (raw_ventas or []):
            if hasattr(r, "keys"):
                ventas.append({str(k): r[k] for k in r.keys()})
            else:
                try:
                    ventas.append({"mes": r[0], "total": float(r[1]) if r[1] is not None else 0})
                except (IndexError, TypeError):
                    pass
    except Exception as e:
        if c:
            try:
                c.close()
            except Exception:
                pass
        print("ERROR HISTORIAL MENSUAL:", e)
        traceback.print_exc()
        return render_template_string(IOS + """
        <div class="card text-center">
            <h2 class="danger">Error en historial mensual</h2>
            <p>""" + str(e) + """</p>
            <a href="/admin">Volver al panel</a>
        </div>
        """), 500
    finally:
        try:
            c.close()
        except Exception:
            pass

    html = IOS + """
    <style>
    body{
        margin:0;
        font-family:Arial;
        background: linear-gradient(135deg,#002D62 0%,#ffffff 50%,#CE1126 100%);
    }

    .container{
        max-width:600px;
        margin:120px auto;
    }

    .card{
        background:white;
        padding:25px;
        border-radius:20px;
        border-top:6px solid #CE1126;
        box-shadow:0 20px 50px rgba(0,0,0,.15);
    }

    h2{text-align:center;color:#002D62;}

    table{width:100%;border-collapse:collapse;margin-top:20px;}
    th{background:#002D62;color:white;padding:12px;}
    td{padding:12px;border-bottom:1px solid #eee;text-align:center;}

    .btn{
        display:block;
        padding:12px;
        margin-top:15px;
        text-align:center;
        background:#002D62;
        color:white;
        border-radius:12px;
        text-decoration:none;
        font-weight:bold;
    }
    </style>

    <div class="container">
        <div class="card">
            <h2>📊 Historial Mensual</h2>

            <table>
                <tr><th>Mes</th><th>Total</th></tr>

                {% for v in ventas %}
                <tr>
                    <td>{{ v.mes }}</td>
                    <td>${{ "%.2f"|format(v.total) }}</td>
                </tr>
                {% endfor %}
            </table>

            <a href="/admin" class="btn">← Volver al Dashboard</a>
        </div>
    </div>
    """

    return render_template_string(html, ventas=ventas)


# ===============================
# REPORTES ADMIN (PostgreSQL)
# ===============================
def _reporte_sql_postgres():
    """True si usamos PostgreSQL (Render)."""
    return bool(os.environ.get("DATABASE_URL"))


@app.route("/reporte_hoy")
def reporte_hoy():
    if not is_staff():
        return redirect("/venta")
    usuario = session.get("u") or session.get("user") or ""
    es_admin = is_admin_or_super()
    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if es_admin:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        COUNT(DISTINCT ticket_lines.ticket_id) AS tickets,
                        COALESCE(SUM(ticket_lines.amount),0) AS total_ventas
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                """, (hoy_rd,))
            else:
                cur.execute(_sql("""
                    SELECT
                        COUNT(DISTINCT ticket_lines.ticket_id) AS tickets,
                        COALESCE(SUM(ticket_lines.amount),0) AS total_ventas
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at) = %s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                """), (hoy_rd,))
        else:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        COUNT(DISTINCT ticket_lines.ticket_id) AS tickets,
                        COALESCE(SUM(ticket_lines.amount),0) AS total_ventas
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                """, (hoy_rd, usuario))
            else:
                cur.execute(_sql("""
                    SELECT
                        COUNT(DISTINCT ticket_lines.ticket_id) AS tickets,
                        COALESCE(SUM(ticket_lines.amount),0) AS total_ventas
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at) = %s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                """), (hoy_rd, usuario))
        row = cur.fetchone()
        if row:
            data = dict(row) if hasattr(row, "keys") else {"tickets": row[0] if len(row) > 0 else 0, "total_ventas": row[1] if len(row) > 1 else 0}
        else:
            data = {"tickets": 0, "total_ventas": 0}
        data.setdefault("tickets", 0)
        data.setdefault("total_ventas", 0)
    finally:
        c.close()
    html = IOS + """
    <div style="max-width:600px;margin:80px auto;padding:20px">
    <h2>Ventas del d&iacute;a</h2>
    <table style="width:100%;border-collapse:collapse;margin-top:16px">
    <tr style="background:#002D62;color:white"><th style="padding:10px;text-align:left">Concepto</th><th style="padding:10px;text-align:right">Valor</th></tr>
    <tr><td style="padding:10px;border:1px solid #e2e8f0">Tickets</td><td style="padding:10px;border:1px solid #e2e8f0;text-align:right">{{ data.tickets }}</td></tr>
    <tr><td style="padding:10px;border:1px solid #e2e8f0">Total ventas</td><td style="padding:10px;border:1px solid #e2e8f0;text-align:right">RD$ {{ "%.2f"|format(data.total_ventas|float) }}</td></tr>
    </table>
    <p style="margin-top:20px"><a href="/admin" style="color:#002D62;font-weight:bold">← Volver al panel</a></p>
    </div>
    """
    return render_template_string(html, data=data)


# ===============================
# VENTAS POR CAJERO (solo admin/super_admin)
# ===============================
@app.route("/ventas_cajeros")
@admin_required
def ventas_cajeros():
    """Ventas agrupadas por cajero con filtro por rango de fechas y cajero. Solo admin/super_admin."""
    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    ayer_rd = (ahora_rd().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    primer_dia_mes = ahora_rd().date().replace(day=1).strftime("%Y-%m-%d")
    desde_param = (request.args.get("desde") or "").strip()
    hasta_param = (request.args.get("hasta") or "").strip()
    cajero_sel = (request.args.get("cajero") or "todos").strip()
    if not cajero_sel:
        cajero_sel = "todos"
    cajero_sel_norm = (cajero_sel or "todos").strip().lower()
    if cajero_sel_norm == "todos":
        cajero_sel = "todos"
    try:
        desde_consulta = datetime.strptime(desde_param, "%Y-%m-%d").strftime("%Y-%m-%d") if desde_param else primer_dia_mes
        hasta_consulta = datetime.strptime(hasta_param, "%Y-%m-%d").strftime("%Y-%m-%d") if hasta_param else hoy_rd
    except ValueError:
        desde_consulta = primer_dia_mes
        hasta_consulta = hoy_rd
    if desde_consulta > hasta_consulta:
        desde_consulta, hasta_consulta = hasta_consulta, desde_consulta

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    cur = c.cursor()

    # Cajeros (solo role='user') para el dropdown
    cur.execute(_sql("SELECT username FROM users WHERE role = %s ORDER BY username"), (ROLE_USER,))
    cajeros_rows = cur.fetchall() or []
    cajeros_list = []
    for r in cajeros_rows:
        if hasattr(r, "keys"):
            d = dict(r) if hasattr(r, "get") else dict(zip(r.keys(), r))
            cajeros_list.append((d.get("username") or "") or "")
        else:
            cajeros_list.append((r[0] if len(r) > 0 else "") or "")
    cajeros_list = [u for u in cajeros_list if u]

    # Ventas por cajero en el rango [desde_consulta, hasta_consulta], solo cajeros role='user'
    por_rango = []
    if os.environ.get("DATABASE_URL"):
        if cajero_sel_norm != "todos":
            cur.execute("""
                SELECT
                    t.cajero,
                    COUNT(DISTINCT t.id) AS cantidad_tickets,
                    COALESCE(SUM(tl.amount), 0) AS total_vendido
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                JOIN users u ON u.username = t.cajero AND u.role = 'user'
                WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                AND t.cajero = %s
                GROUP BY t.cajero
                ORDER BY total_vendido DESC
            """, (desde_consulta, hasta_consulta, cajero_sel))
        else:
            cur.execute("""
                SELECT
                    t.cajero,
                    COUNT(DISTINCT t.id) AS cantidad_tickets,
                    COALESCE(SUM(tl.amount), 0) AS total_vendido
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                JOIN users u ON u.username = t.cajero AND u.role = 'user'
                WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                GROUP BY t.cajero
                ORDER BY total_vendido DESC
            """, (desde_consulta, hasta_consulta))
        por_rango = cur.fetchall()
    else:
        if cajero_sel_norm != "todos":
            cur.execute(_sql("""
                SELECT
                    t.cajero,
                    COUNT(DISTINCT t.id) AS cantidad_tickets,
                    COALESCE(SUM(tl.amount), 0) AS total_vendido
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                JOIN users u ON u.username = t.cajero AND u.role = 'user'
                WHERE DATE(t.created_at) BETWEEN %s AND %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                AND t.cajero = %s
                GROUP BY t.cajero
                ORDER BY total_vendido DESC
            """), (desde_consulta, hasta_consulta, cajero_sel))
        else:
            cur.execute(_sql("""
                SELECT
                    t.cajero,
                    COUNT(DISTINCT t.id) AS cantidad_tickets,
                    COALESCE(SUM(tl.amount), 0) AS total_vendido
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                JOIN users u ON u.username = t.cajero AND u.role = 'user'
                WHERE DATE(t.created_at) BETWEEN %s AND %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                GROUP BY t.cajero
                ORDER BY total_vendido DESC
            """), (desde_consulta, hasta_consulta))
        por_rango = cur.fetchall()
    c.close()

    def row_to_dict(r, keys):
        if hasattr(r, "keys"):
            return dict(r)
        return dict(zip(keys, r))

    keys_rango = ("cajero", "cantidad_tickets", "total_vendido")
    lista = [row_to_dict(r, keys_rango) for r in por_rango]
    for item in lista:
        item["cajero_display"] = (item.get("cajero") or "") or "(sin cajero)"

    html_ventas = IOS + """
<style>
.ventas-cajeros-container{ max-width:640px; margin:auto; padding:20px; box-sizing:border-box; }
.ventas-cajeros-container h2{ margin:0 0 16px 0; font-size:1.35rem; color:#1e293b; }
.ventas-cajeros-filtros{ margin-bottom:12px; font-size:15px; }
.ventas-cajeros-filtros a{ color:#002D62; text-decoration:none; font-weight:600; }
.ventas-cajeros-form{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:16px; }
.ventas-cajeros-form label{ font-size:14px; color:#475569; }
.ventas-cajeros-form select{ padding:8px 10px; border:1px solid #cbd5e1; border-radius:6px; font-size:15px; background:white; }
.ventas-cajeros-form input[type=date]{ padding:8px 10px; border:1px solid #cbd5e1; border-radius:6px; font-size:15px; }
.ventas-cajeros-form button{ padding:8px 20px; background:#002D62; color:white; border:none; border-radius:6px; font-weight:600; cursor:pointer; font-size:15px; }
.ventas-cajeros-form button:hover{ background:#1e40af; }
.ventas-cajeros-table{ width:100%; border-collapse:collapse; margin-top:12px; font-size:14px; }
.ventas-cajeros-table th,.ventas-cajeros-table td{ padding:10px; text-align:center; border-bottom:1px solid #e2e8f0; }
.ventas-cajeros-table th{ background:#002D62; color:white; }
.ventas-cajeros-table td:first-child{ text-align:left; }
.ventas-cajeros-table a{ color:#002D62; font-weight:600; text-decoration:none; }
.ventas-cajeros-back{ display:inline-block; margin-top:16px; color:#002D62; font-weight:600; text-decoration:none; font-size:14px; }
@media (max-width:600px){ .ventas-cajeros-container{ padding:12px; } .ventas-cajeros-form{ flex-direction:column; align-items:stretch; } }
</style>
<div class="ventas-cajeros-container">
  <h2>📊 Ventas por Cajero</h2>
  <form method="get" action="/ventas_cajeros" class="ventas-cajeros-form">
    <label>Desde:</label>
    <input type="date" name="desde" value="{{ desde_consulta }}" required>
    <label>Hasta:</label>
    <input type="date" name="hasta" value="{{ hasta_consulta }}" required>
    <label>Cajero:</label>
    <select name="cajero">
      <option value="todos" {% if cajero_sel == 'todos' %}selected{% endif %}>Todos</option>
      {% for u in cajeros_list %}
      <option value="{{ u }}" {% if cajero_sel == u %}selected{% endif %}>{{ u }}</option>
      {% endfor %}
    </select>
    <button type="submit">Buscar</button>
  </form>
  <p style="margin:0 0 12px 0;color:#64748b;font-size:14px;">Rango: <strong>{{ desde_consulta }}</strong> a <strong>{{ hasta_consulta }}</strong>{% if cajero_sel != 'todos' %} &nbsp;|&nbsp; Cajero: <strong>{{ cajero_sel }}</strong>{% endif %}</p>
  <table class="ventas-cajeros-table">
    <thead>
      <tr>
        <th>Cajero</th>
        <th>Tickets</th>
        <th>Total Vendido</th>
      </tr>
    </thead>
    <tbody>
    {% for r in lista %}
      <tr>
        <td><a href="/ventas_cajeros/detalle/{{ (r.cajero or '') | urlencode }}?desde={{ desde_consulta }}&hasta={{ hasta_consulta }}&cajero={{ cajero_sel }}">{{ r.cajero_display }}</a></td>
        <td>{{ r.cantidad_tickets }}</td>
        <td>RD$ {{ "%.2f"|format(r.total_vendido|float) }}</td>
      </tr>
    {% else %}
      <tr><td colspan="3">No hay ventas para este filtro.</td></tr>
    {% endfor %}
    </tbody>
  </table>
  <a href="/admin" class="ventas-cajeros-back">← Volver al panel Admin</a>
</div>
    """
    return render_template_string(
        html_ventas,
        lista=lista,
        desde_consulta=desde_consulta,
        hasta_consulta=hasta_consulta,
        cajero_sel=cajero_sel,
        cajeros_list=cajeros_list,
        hoy_rd=hoy_rd,
        ayer_rd=ayer_rd,
    )


@app.route("/ventas_cajeros/detalle/<path:cajero>")
@admin_required
def ventas_cajeros_detalle(cajero):
    """Historial de ventas y tickets de un cajero en la fecha o rango seleccionado. Solo admin/super_admin."""
    from urllib.parse import unquote
    cajero = unquote(cajero).strip() if cajero else ""
    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    fecha_consulta = request.args.get("fecha") or hoy_rd
    desde_consulta = request.args.get("desde") or fecha_consulta
    hasta_consulta = request.args.get("hasta") or fecha_consulta
    try:
        datetime.strptime(desde_consulta, "%Y-%m-%d")
        datetime.strptime(hasta_consulta, "%Y-%m-%d")
    except ValueError:
        desde_consulta = hasta_consulta = hoy_rd
    if desde_consulta > hasta_consulta:
        desde_consulta, hasta_consulta = hasta_consulta, desde_consulta

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    cur = c.cursor()

    # Tickets del cajero en el rango [desde_consulta, hasta_consulta]
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
            SELECT t.id, t.created_at,
                   COALESCE(SUM(tl.amount), 0) AS total_ticket
            FROM tickets t
            LEFT JOIN ticket_lines tl ON tl.ticket_id = t.id AND COALESCE(tl.estado, 'activo') != 'cancelado'
            WHERE t.cajero = %s
            AND (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
            GROUP BY t.id, t.created_at
            ORDER BY t.created_at DESC
        """, (cajero, desde_consulta, hasta_consulta))
    else:
        cur.execute(_sql("""
            SELECT t.id, t.created_at,
                   COALESCE(SUM(tl.amount), 0) AS total_ticket
            FROM tickets t
            LEFT JOIN ticket_lines tl ON tl.ticket_id = t.id AND COALESCE(tl.estado, 'activo') != 'cancelado'
            WHERE t.cajero = %s
            AND DATE(t.created_at) BETWEEN %s AND %s
            GROUP BY t.id, t.created_at
            ORDER BY t.created_at DESC
        """), (cajero, desde_consulta, hasta_consulta))
    tickets_rows = cur.fetchall()

    tickets_list = []
    for r in tickets_rows:
        d = dict(r) if hasattr(r, "keys") else {"id": r[0], "created_at": r[1], "total_ticket": r[2]}
        created = d.get("created_at")
        if created and hasattr(created, "strftime"):
            d["created_at_str"] = created.strftime("%Y-%m-%d %H:%M")
        else:
            d["created_at_str"] = str(created)[:16] if created else ""
        tickets_list.append(d)

    # Historial de jugadas (ticket_lines) de esos tickets
    ticket_ids = [t.get("id") for t in tickets_list if t.get("id") is not None]
    jugadas = []
    if ticket_ids:
        ph = ",".join(["%s"] * len(ticket_ids)) if os.environ.get("DATABASE_URL") else ",".join(["?"] * len(ticket_ids))
        q = """
            SELECT tl.ticket_id, tl.lottery, tl.draw, tl.number, tl.play, tl.amount, tl.estado
            FROM ticket_lines tl
            WHERE tl.ticket_id IN (""" + ph + """)
            ORDER BY tl.ticket_id, tl.id
        """
        if not os.environ.get("DATABASE_URL"):
            q = _sql(q)
        cur.execute(q, tuple(ticket_ids))
        for r in cur.fetchall():
            row = dict(r) if hasattr(r, "keys") else {"ticket_id": r[0], "lottery": r[1], "draw": r[2], "number": r[3], "play": r[4], "amount": r[5], "estado": r[6] if len(r) > 6 else "activo"}
            jugadas.append(row)
    c.close()

    html_detalle = IOS + """
    <div style="max-width:950px;margin:40px auto;padding:20px;box-sizing:border-box">
    <h2 style="color:#002D62">Historial ventas — {{ cajero }}</h2>
    <p style="color:#555">Rango: <strong>{{ desde_consulta }}</strong> a <strong>{{ hasta_consulta }}</strong> &nbsp;|&nbsp; <a href="/ventas_cajeros?desde={{ desde_consulta }}&hasta={{ hasta_consulta }}">← Volver a Ventas por Cajero</a></p>
    <h3 style="margin-top:24px">Tickets vendidos ({{ tickets_list | length }})</h3>
    <table style="width:100%;border-collapse:collapse;margin-top:12px;background:white;box-shadow:0 2px 8px rgba(0,0,0,.08);border-radius:8px;overflow:hidden">
    <thead>
    <tr style="background:#002D62;color:white">
        <th style="padding:10px;text-align:left">Ticket</th>
        <th style="padding:10px;text-align:left">Hora</th>
        <th style="padding:10px;text-align:right">Total</th>
    </tr>
    </thead>
    <tbody>
    {% for t in tickets_list %}
    <tr style="border-bottom:1px solid #e2e8f0">
        <td style="padding:10px">#{{ t.id }}</td>
        <td style="padding:10px">{{ t.created_at_str }}</td>
        <td style="padding:10px;text-align:right">RD$ {{ "%.2f"|format(t.total_ticket|float) }}</td>
    </tr>
    {% else %}
    <tr><td colspan="3" style="padding:16px;text-align:center;color:#64748b">No hay tickets.</td></tr>
    {% endfor %}
    </tbody>
    </table>
    <h3 style="margin-top:24px">Jugadas (detalle)</h3>
    <table style="width:100%;border-collapse:collapse;margin-top:12px;background:white;box-shadow:0 2px 8px rgba(0,0,0,.08);border-radius:8px;overflow:hidden">
    <thead>
    <tr style="background:#002D62;color:white">
        <th style="padding:10px">Ticket</th>
        <th style="padding:10px">Lotería</th>
        <th style="padding:10px">Sorteo</th>
        <th style="padding:10px">Número</th>
        <th style="padding:10px">Jugada</th>
        <th style="padding:10px;text-align:right">Monto</th>
        <th style="padding:10px">Estado</th>
    </tr>
    </thead>
    <tbody>
    {% for j in jugadas %}
    <tr style="border-bottom:1px solid #e2e8f0" class="{{ 'cancelada' if j.estado == 'cancelado' else '' }}">
        <td style="padding:10px">#{{ j.ticket_id }}</td>
        <td style="padding:10px">{{ j.lottery or '-' }}</td>
        <td style="padding:10px">{{ j.draw or '-' }}</td>
        <td style="padding:10px">{{ j.number or '-' }}</td>
        <td style="padding:10px">{{ j.play or '-' }}</td>
        <td style="padding:10px;text-align:right">RD$ {{ "%.2f"|format(j.amount|float) }}</td>
        <td style="padding:10px">{{ j.estado or 'activo' }}</td>
    </tr>
    {% else %}
    <tr><td colspan="7" style="padding:16px;text-align:center;color:#64748b">No hay jugadas.</td></tr>
    {% endfor %}
    </tbody>
    </table>
    <p style="margin-top:20px"><a href="/admin" style="color:#002D62;font-weight:bold">← Volver al panel</a></p>
    </div>
    """
    return render_template_string(
        html_detalle,
        cajero=cajero or "(sin cajero)",
        fecha_consulta=fecha_consulta,
        desde_consulta=desde_consulta,
        hasta_consulta=hasta_consulta,
        tickets_list=tickets_list,
        jugadas=jugadas,
    )


@app.route("/reporte_semanal")
def reporte_semanal():
    if not is_staff():
        return redirect("/venta")
    usuario = session.get("u") or session.get("user") or ""
    es_admin = is_admin_or_super()
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if es_admin:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE tickets.created_at >= CURRENT_DATE - INTERVAL '7 days'
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    GROUP BY (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date
                    ORDER BY fecha
                """)
            else:
                cur.execute("""
                    SELECT
                        DATE(tickets.created_at) AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE DATE(tickets.created_at) >= DATE('now','-7 days')
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    GROUP BY DATE(tickets.created_at)
                    ORDER BY fecha
                """)
        else:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE tickets.created_at >= CURRENT_DATE - INTERVAL '7 days'
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                    GROUP BY (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date
                    ORDER BY fecha
                """, (usuario,))
            else:
                cur.execute(_sql("""
                    SELECT
                        DATE(tickets.created_at) AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE DATE(tickets.created_at) >= DATE('now','-7 days')
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                    GROUP BY DATE(tickets.created_at)
                    ORDER BY fecha
                """), (usuario,))
        raw_rows = cur.fetchall()
        rows = []
        for r in (raw_rows or []):
            if hasattr(r, "keys"):
                rows.append({str(k): r[k] for k in r.keys()})
            else:
                try:
                    rows.append({"fecha": r[0], "total": float(r[1]) if r[1] is not None else 0})
                except (IndexError, TypeError):
                    pass
    except Exception as e:
        if c:
            try:
                c.close()
            except Exception:
                pass
        print("ERROR REPORTE SEMANAL:", e)
        traceback.print_exc()
        return render_template_string(IOS + """
        <div class="card text-center">
            <h2 class="danger">Error en reporte semanal</h2>
            <p>""" + str(e) + """</p>
            <a href="/admin">Volver al panel</a>
        </div>
        """), 500
    finally:
        try:
            c.close()
        except Exception:
            pass
    html = IOS + """
    <div style="max-width:600px;margin:80px auto;padding:20px">
    <h2>Ventas semanales</h2>
    <table style="width:100%;border-collapse:collapse;margin-top:16px">
    <tr style="background:#002D62;color:white"><th style="padding:10px;text-align:left">Fecha</th><th style="padding:10px;text-align:right">Total</th></tr>
    {% for r in rows %}
    <tr><td style="padding:10px;border:1px solid #e2e8f0">{{ r.fecha }}</td><td style="padding:10px;border:1px solid #e2e8f0;text-align:right">RD$ {{ "%.2f"|format(r.total|float) }}</td></tr>
    {% endfor %}
    </table>
    <p style="margin-top:20px"><a href="/admin" style="color:#002D62;font-weight:bold">← Volver al panel</a></p>
    </div>
    """
    return render_template_string(html, rows=rows)


@app.route("/reporte_mensual")
def reporte_mensual():
    if not is_staff():
        return redirect("/venta")
    usuario = session.get("u") or session.get("user") or ""
    es_admin = is_admin_or_super()
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if es_admin:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE tickets.created_at >= CURRENT_DATE - INTERVAL '30 days'
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    GROUP BY (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date
                    ORDER BY fecha
                """)
            else:
                cur.execute("""
                    SELECT
                        DATE(tickets.created_at) AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE DATE(tickets.created_at) >= DATE('now','-30 days')
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    GROUP BY DATE(tickets.created_at)
                    ORDER BY fecha
                """)
        else:
            if _reporte_sql_postgres():
                cur.execute("""
                    SELECT
                        (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE tickets.created_at >= CURRENT_DATE - INTERVAL '30 days'
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                    GROUP BY (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date
                    ORDER BY fecha
                """, (usuario,))
            else:
                cur.execute(_sql("""
                    SELECT
                        DATE(tickets.created_at) AS fecha,
                        COALESCE(SUM(ticket_lines.amount),0) AS total
                    FROM ticket_lines
                    JOIN tickets ON ticket_lines.ticket_id = tickets.id
                    WHERE DATE(tickets.created_at) >= DATE('now','-30 days')
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                    GROUP BY DATE(tickets.created_at)
                    ORDER BY fecha
                """), (usuario,))
        raw_rows = cur.fetchall()
        rows = []
        for r in (raw_rows or []):
            if hasattr(r, "keys"):
                rows.append({str(k): r[k] for k in r.keys()})
            else:
                try:
                    rows.append({"fecha": r[0], "total": float(r[1]) if r[1] is not None else 0})
                except (IndexError, TypeError):
                    pass
    except Exception as e:
        if c:
            try:
                c.close()
            except Exception:
                pass
        print("ERROR REPORTE MENSUAL:", e)
        traceback.print_exc()
        return render_template_string(IOS + """
        <div class="card text-center">
            <h2 class="danger">Error en reporte mensual</h2>
            <p>""" + str(e) + """</p>
            <a href="/admin">Volver al panel</a>
        </div>
        """), 500
    finally:
        try:
            c.close()
        except Exception:
            pass
    html = IOS + """
    <div style="max-width:600px;margin:80px auto;padding:20px">
    <h2>Ventas mensuales (&uacute;ltimos 30 d&iacute;as)</h2>
    <table style="width:100%;border-collapse:collapse;margin-top:16px">
    <tr style="background:#002D62;color:white"><th style="padding:10px;text-align:left">Fecha</th><th style="padding:10px;text-align:right">Total</th></tr>
    {% for r in rows %}
    <tr><td style="padding:10px;border:1px solid #e2e8f0">{{ r.fecha }}</td><td style="padding:10px;border:1px solid #e2e8f0;text-align:right">RD$ {{ "%.2f"|format(r.total|float) }}</td></tr>
    {% endfor %}
    </table>
    <p style="margin-top:20px"><a href="/admin" style="color:#002D62;font-weight:bold">← Volver al panel</a></p>
    </div>
    """
    return render_template_string(html, rows=rows)


# ===============================
# BALANCE POR CAJERO
# ===============================
@app.route("/balance_cajeros")
def balance_cajeros():
    if not is_admin_or_super():
        return redirect("/")
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute("""
            SELECT cajero, COALESCE(balance, 0) AS balance
            FROM balance_cajeros
            ORDER BY cajero
        """)
        raw = cur.fetchall()
        balances = []
        for r in (raw or []):
            if hasattr(r, "keys"):
                balances.append({"cajero": r.get("cajero", ""), "balance": float(r.get("balance", 0) or 0)})
            else:
                try:
                    balances.append({"cajero": r[0], "balance": float(r[1]) if r[1] is not None else 0})
                except (IndexError, TypeError):
                    pass
    except Exception as e:
        if c:
            try:
                c.close()
            except Exception:
                pass
        print("ERROR BALANCE CAJEROS:", e)
        traceback.print_exc()
        return render_template_string(IOS + """
        <div class="card text-center">
            <h2 class="danger">Error en balance por cajero</h2>
            <p>""" + str(e) + """</p>
            <a href="/admin">Volver al panel</a>
        </div>
        """), 500
    finally:
        try:
            c.close()
        except Exception:
            pass
    html = IOS + """
    <div style="max-width:600px;margin:80px auto;padding:20px">
    <h2>📊 Balance por Cajero</h2>
    <table style="width:100%;border-collapse:collapse;margin-top:16px">
    <tr style="background:#002D62;color:white"><th style="padding:10px;text-align:left">Cajero</th><th style="padding:10px;text-align:right">Balance RD$</th></tr>
    {% for b in balances %}
    <tr><td style="padding:10px;border:1px solid #e2e8f0">{{ b.cajero }}</td><td style="padding:10px;border:1px solid #e2e8f0;text-align:right">{{ "%.2f"|format(b.balance) }}</td></tr>
    {% endfor %}
    </table>
    {% if not balances %}
    <p style="margin-top:16px;color:#666">No hay datos aún.</p>
    {% else %}
    <h3 style="margin-top:30px">Agregar dinero a cajero</h3>
    <form method="POST" action="/admin/agregar_balance_cajero" style="margin-top:12px;display:flex;flex-wrap:wrap;gap:10px;align-items:center">
    <select name="cajero" required style="padding:8px;min-width:140px">
    {% for b in balances %}
    <option value="{{ b.cajero }}">{{ b.cajero }}</option>
    {% endfor %}
    </select>
    <input type="number" name="monto" step="0.01" min="0.01" placeholder="Monto RD$" required style="padding:8px;width:120px">
    <button type="submit" style="background:#002D62;color:white;padding:8px 16px;border:none;border-radius:8px;cursor:pointer">Agregar dinero</button>
    </form>
    {% endif %}

    <p style="margin-top:20px"><a href="/admin" style="color:#002D62;font-weight:bold">← Volver al panel</a></p>
    </div>
    """
    return render_template_string(html, balances=balances)


@app.route("/admin/agregar_balance_cajero", methods=["POST"])
def agregar_balance_cajero():
    if not is_admin_or_super():
        return redirect("/")
    try:
        conn = db()
        if not conn:
            return "Error conectando base de datos", 500
        cur = conn.cursor()
        cajero = request.form.get("cajero", "").strip()
        monto = float(request.form.get("monto", 0))
        if not cajero:
            conn.close()
            return "Cajero requerido", 400
        cur.execute(_sql("""
            UPDATE balance_cajeros
            SET balance = COALESCE(balance, 0) + %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE cajero = %s
        """), (monto, cajero))
        if cur.rowcount == 0:
            cur.execute(_sql("""
                INSERT INTO balance_cajeros (cajero, balance, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
            """), (cajero, monto))
        conn.commit()
        conn.close()
        return redirect("/balance_cajeros")
    except Exception as e:
        print("ERROR AGREGAR BALANCE:", e)
        traceback.print_exc()
        return str(e), 500


# ===============================
# BANCO POR CAJERO (ventas - premios_pagados = dinero_caja, entregar al admin)
# ===============================
def _banco_cajeros_desde_hasta():
    """Parse desde/hasta from request. Default: first day of current month to today."""
    hoy = ahora_rd().date()
    primer_dia = hoy.replace(day=1)
    desde_param = (request.args.get("desde") or request.form.get("desde") or primer_dia.strftime("%Y-%m-%d")).strip()
    hasta_param = (request.args.get("hasta") or request.form.get("hasta") or hoy.strftime("%Y-%m-%d")).strip()
    try:
        desde = datetime.strptime(desde_param, "%Y-%m-%d").strftime("%Y-%m-%d")
        hasta = datetime.strptime(hasta_param, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        desde = primer_dia.strftime("%Y-%m-%d")
        hasta = hoy.strftime("%Y-%m-%d")
    if desde > hasta:
        desde, hasta = hasta, desde
    return desde, hasta


@app.route("/admin/banco_cajeros", methods=["GET", "POST"])
def admin_banco_cajeros():
    if not is_admin_or_super():
        return redirect("/")

    desde, hasta = _banco_cajeros_desde_hasta()

    if request.method == "POST":
        cajero = (request.form.get("cajero") or "").strip()
        if cajero:
            conn = db()
            if conn:
                try:
                    cur = conn.cursor()
                    admin_user = session.get("u") or "admin"
                    if os.environ.get("DATABASE_URL"):
                        cur.execute("""
                            SELECT COALESCE(SUM(tl.amount), 0) AS ventas
                            FROM tickets t
                            JOIN ticket_lines tl ON tl.ticket_id = t.id AND COALESCE(tl.estado, 'activo') != 'cancelado'
                            WHERE t.cajero = %s
                            AND (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                        """, (cajero, desde, hasta))
                    else:
                        cur.execute(_sql("""
                            SELECT COALESCE(SUM(tl.amount), 0) AS ventas
                            FROM tickets t
                            JOIN ticket_lines tl ON tl.ticket_id = t.id AND COALESCE(tl.estado, 'activo') != 'cancelado'
                            WHERE t.cajero = %s AND DATE(t.created_at) BETWEEN %s AND %s
                        """), (cajero, desde, hasta))
                    row = cur.fetchone()
                    ventas = float((row[0] if row else 0) or 0)
                    if os.environ.get("DATABASE_URL"):
                        cur.execute("""
                            SELECT COALESCE(SUM(pp.monto), 0) AS premios
                            FROM pagos_premios pp
                            JOIN tickets t ON t.id = pp.ticket_id AND t.cajero = %s
                            WHERE (pp.fecha AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                        """, (cajero, desde, hasta))
                    else:
                        cur.execute(_sql("""
                            SELECT COALESCE(SUM(pp.monto), 0) AS premios
                            FROM pagos_premios pp
                            JOIN tickets t ON t.id = pp.ticket_id AND t.cajero = %s
                            WHERE DATE(pp.fecha) BETWEEN %s AND %s
                        """), (cajero, desde, hasta))
                    rp = cur.fetchone()
                    premios = float((rp[0] if rp else 0) or 0)
                    dinero_caja = ventas - premios
                    if os.environ.get("DATABASE_URL"):
                        cur.execute("""
                            SELECT COALESCE(SUM(monto), 0) AS entregado FROM pagos_cajeros
                            WHERE cajero = %s
                            AND (fecha AT TIME ZONE 'UTC' AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                        """, (cajero, desde, hasta))
                    else:
                        cur.execute(_sql("""
                            SELECT COALESCE(SUM(monto), 0) AS entregado FROM pagos_cajeros
                            WHERE cajero = %s AND DATE(fecha) BETWEEN %s AND %s
                        """), (cajero, desde, hasta))
                    r2 = cur.fetchone()
                    entregado = float((r2[0] if r2 else 0) or 0)
                    monto_entregar = max(0, dinero_caja - entregado)
                    if monto_entregar > 0:
                        cur.execute(_sql("""
                            INSERT INTO pagos_cajeros (cajero, monto, admin)
                            VALUES (%s, %s, %s)
                        """), (cajero, monto_entregar, admin_user))
                        cur.execute(_sql("""
                            INSERT INTO caja (tipo, descripcion, monto, usuario)
                            VALUES (%s, %s, %s, %s)
                        """), ("entrada", "Entrega cajero " + cajero, monto_entregar, cajero))
                        conn.commit()
                        fecha_entrega = ahora_rd().strftime("%Y-%m-%d %H:%M")
                        return redirect(url_for("recibo_entrega_58mm", cajero=cajero, ventas=ventas, premios=premios, monto=monto_entregar, admin=admin_user, fecha=fecha_entrega, autoprint=1))
                    conn.rollback()
                finally:
                    conn.close()
        return redirect("/admin/banco_cajeros?desde=" + quote(desde) + "&hasta=" + quote(hasta))

    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    filas = []
    try:
        cur = conn.cursor()
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT t.cajero,
                    COALESCE(SUM(tl.amount), 0) AS ventas
                FROM tickets t
                JOIN ticket_lines tl ON tl.ticket_id = t.id
                    AND COALESCE(tl.estado, 'activo') != 'cancelado'
                WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                GROUP BY t.cajero
            """, (desde, hasta))
        else:
            cur.execute(_sql("""
                SELECT t.cajero,
                    COALESCE(SUM(tl.amount), 0) AS ventas
                FROM tickets t
                JOIN ticket_lines tl ON tl.ticket_id = t.id
                    AND COALESCE(tl.estado, 'activo') != 'cancelado'
                WHERE DATE(t.created_at) BETWEEN %s AND %s
                GROUP BY t.cajero
            """), (desde, hasta))
        ventas_rows = cur.fetchall()
        ventas_map = {}
        for r in ventas_rows:
            d = dict(r) if hasattr(r, "keys") else {}
            c = d.get("cajero") or (r[0] if r else "")
            v = float(d.get("ventas") or 0)
            if c:
                ventas_map[c] = v

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT t.cajero,
                    COALESCE(SUM(pp.monto), 0) AS premios_pagados
                FROM tickets t
                JOIN pagos_premios pp ON pp.ticket_id = t.id
                WHERE (pp.fecha AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                GROUP BY t.cajero
            """, (desde, hasta))
        else:
            cur.execute(_sql("""
                SELECT t.cajero,
                    COALESCE(SUM(pp.monto), 0) AS premios_pagados
                FROM tickets t
                JOIN pagos_premios pp ON pp.ticket_id = t.id
                WHERE DATE(pp.fecha) BETWEEN %s AND %s
                GROUP BY t.cajero
            """), (desde, hasta))
        premios_rows = cur.fetchall()
        premios_map = {}
        for r in premios_rows:
            d = dict(r) if hasattr(r, "keys") else {}
            c = d.get("cajero") or (r[0] if r else "")
            p = float(d.get("premios_pagados") or 0)
            if c:
                premios_map[c] = p

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT cajero,
                    COALESCE(SUM(monto), 0) AS entregado
                FROM pagos_cajeros
                WHERE (fecha AT TIME ZONE 'UTC' AT TIME ZONE 'America/Santo_Domingo')::date BETWEEN %s AND %s
                GROUP BY cajero
            """, (desde, hasta))
        else:
            cur.execute(_sql("""
                SELECT cajero,
                    COALESCE(SUM(monto), 0) AS entregado
                FROM pagos_cajeros
                WHERE DATE(fecha) BETWEEN %s AND %s
                GROUP BY cajero
            """), (desde, hasta))
        entregado_rows = cur.fetchall()
        entregado_map = {}
        for r in entregado_rows:
            d = dict(r) if hasattr(r, "keys") else {}
            c = d.get("cajero") or (r[0] if r else "")
            e = float(d.get("entregado") or 0)
            if c:
                entregado_map[c] = e

        # Incluir todos los cajeros (usuarios con role cajero o user)
        cur.execute(_sql("SELECT username FROM users WHERE role IN (%s, %s) ORDER BY username"), (ROLE_CAJERO, ROLE_USER))
        users_rows = cur.fetchall()
        all_cajeros = set()
        for r in users_rows:
            u = (dict(r).get("username") or r[0]) if hasattr(r, "keys") else r[0]
            if u:
                all_cajeros.add(u)
        for c in ventas_map:
            all_cajeros.add(c)
        for c in premios_map:
            all_cajeros.add(c)
        for c in entregado_map:
            all_cajeros.add(c)

        base_url = "/admin/banco_cajeros?desde=" + quote(desde) + "&hasta=" + quote(hasta)
        filas = []
        for cajero in sorted(all_cajeros):
            ventas = ventas_map.get(cajero, 0)
            premios_pagados = premios_map.get(cajero, 0)
            dinero_caja = ventas - premios_pagados
            entregado = entregado_map.get(cajero, 0)
            balance = dinero_caja - entregado
            filas.append({
                "cajero": cajero,
                "ventas": round(ventas, 2),
                "premios_pagados": round(premios_pagados, 2),
                "dinero_caja": round(dinero_caja, 2),
                "entregado": round(entregado, 2),
                "balance": round(balance, 2),
                "entregar_url": base_url + "&entregar=" + quote(cajero),
                "historial_url": "/admin/banco_cajeros/historial/" + quote(cajero),
            })
    finally:
        conn.close()

    entregar_cajero = request.args.get("entregar", "").strip()

    return render_template_string(IOS + ADMIN_MENU + """
<div class="card" style="max-width:980px;margin:20px auto">
    <h2>🏦 Banco por Cajero</h2>
    <p style="margin-bottom:8px;color:#555">Ventas - Premios = Dinero en Caja. Dinero en Caja - Entregado = Balance.</p>
    <p style="margin-bottom:16px;color:#64748b;font-size:14px">Registrar entregas cuando el cajero entregue dinero al admin.</p>

    <form method="get" action="/admin/banco_cajeros" style="display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:20px">
        <label style="font-size:14px;color:#475569">Desde:</label>
        <input type="date" name="desde" value="{{ desde }}" required style="padding:8px;border:1px solid #cbd5e1;border-radius:6px">
        <label style="font-size:14px;color:#475569">Hasta:</label>
        <input type="date" name="hasta" value="{{ hasta }}" required style="padding:8px;border:1px solid #cbd5e1;border-radius:6px">
        <button type="submit" class="admin-btn" style="padding:8px 20px;border:none;cursor:pointer">Buscar</button>
    </form>

    {% if entregar_cajero %}
    <div style="background:#f0f7ff;border:1px solid #002D62;border-radius:12px;padding:16px;margin-bottom:20px">
        <h3 style="margin-top:0;color:#002D62">Confirmar pago (Pagar)</h3>
        <p><b>Cajero:</b> {{ entregar_cajero }}</p>
        <p>El monto a entregar es el <b>Balance</b> (Dinero en Caja - Entregado). Se registrará en Entregado al Banco y el balance quedará en 0.</p>
        <form method="post" style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-top:12px">
            <input type="hidden" name="cajero" value="{{ entregar_cajero }}">
            <input type="hidden" name="desde" value="{{ desde }}">
            <input type="hidden" name="hasta" value="{{ hasta }}">
            <button type="submit" style="background:linear-gradient(135deg,#002D62,#CE1126);color:white;padding:10px 20px;border:none;border-radius:8px;font-weight:bold;cursor:pointer">✓ Pagar</button>
            <a href="/admin/banco_cajeros?desde={{ desde }}&hasta={{ hasta }}" style="margin-left:8px;color:#002D62">Cancelar</a>
        </form>
    </div>
    {% endif %}

    <table width="100%" style="border-collapse:collapse">
        <tr style="background:#002D62;color:white">
            <th style="padding:10px;text-align:left">Cajero</th>
            <th style="padding:10px;text-align:right">Ventas</th>
            <th style="padding:10px;text-align:right">Premios Pagados</th>
            <th style="padding:10px;text-align:right">Dinero en Caja</th>
            <th style="padding:10px;text-align:right">Entregado al Banco</th>
            <th style="padding:10px;text-align:right">Balance</th>
            <th style="padding:10px">Acciones</th>
        </tr>
        {% for f in filas %}
        <tr style="border-bottom:1px solid #e2e8f0">
            <td style="padding:10px">{{ f.cajero }}</td>
            <td style="padding:10px;text-align:right">RD$ {{ "{:,.2f}".format(f.ventas) }}</td>
            <td style="padding:10px;text-align:right">RD$ {{ "{:,.2f}".format(f.premios_pagados) }}</td>
            <td style="padding:10px;text-align:right">RD$ {{ "{:,.2f}".format(f.dinero_caja) }}</td>
            <td style="padding:10px;text-align:right">RD$ {{ "{:,.2f}".format(f.entregado) }}</td>
            <td style="padding:10px;text-align:right;font-weight:bold">RD$ {{ "{:,.2f}".format(f.balance) }}</td>
            <td style="padding:10px">
                {% if f.balance > 0 %}
                <a href="{{ f.entregar_url }}" class="admin-btn" style="display:inline-block;padding:6px 12px;text-decoration:none;margin-right:4px">Pagar</a>
                {% else %}
                <span class="admin-btn" style="display:inline-block;padding:6px 12px;margin-right:4px;background:#9ca3af;color:white;border:none;cursor:not-allowed;opacity:0.9" title="Balance en 0">Pagar</span>
                {% endif %}
                <a href="{{ f.historial_url }}" style="display:inline-block;padding:6px 12px;background:#6b7280;color:white;border-radius:6px;text-decoration:none;font-size:13px">VER HISTORIAL</a>
            </td>
        </tr>
        {% endfor %}
    </table>
    {% if not filas %}
    <p style="color:#666;margin-top:16px">No hay cajeros con actividad en este rango de fechas.</p>
    {% endif %}
    <br>
    <a href="/admin" style="color:#002D62;font-weight:bold">← Volver al Dashboard</a>
</div>
""", filas=filas, entregar_cajero=entregar_cajero, desde=desde, hasta=hasta)


@app.route("/admin/banco_cajeros/recibo_entrega")
def recibo_entrega_admin():
    """Recibo de entrega al admin (para imprimir)."""
    if not is_admin_or_super():
        return redirect("/")
    cajero = request.args.get("cajero", "")
    try:
        ventas = float(request.args.get("ventas", 0))
        premios = float(request.args.get("premios", 0))
        monto = float(request.args.get("monto", 0))
    except ValueError:
        ventas = premios = monto = 0
    fecha = ahora_rd().strftime("%Y-%m-%d %H:%M")
    return render_template_string(IOS + """
<div style="max-width:320px;margin:40px auto;font-family:monospace;font-size:14px;text-align:center" id="recibo-entrega">
    <pre style="background:#f8f9fa;padding:20px;border-radius:8px;border:1px solid #ddd">
========================
ENTREGA AL ADMIN
========================

Cajero: {{ cajero }}
Ventas: RD$ {{ "{:,.2f}".format(ventas) }}
Premios pagados: RD$ {{ "{:,.2f}".format(premios) }}
Dinero entregado: RD$ {{ "{:,.2f}".format(monto) }}
Fecha: {{ fecha }}

========================
</pre>
    <button onclick="window.print()" style="background:#002D62;color:white;padding:12px 24px;border:none;border-radius:8px;cursor:pointer;margin-top:12px">🖨 Imprimir</button>
    <br><br>
    <a href="/admin/banco_cajeros" style="color:#002D62">← Volver a Banco Cajeros</a>
</div>
""", cajero=cajero, ventas=ventas, premios=premios, monto=monto, fecha=fecha)


RECIBO_ENTREGA_58MM_HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Entrega de Caja - 58mm</title>
    <style>
        body {
            width: 58mm;
            font-family: monospace;
            font-size: 12px;
            margin: 0;
            padding: 6px;
            line-height: 1.35;
        }
        .ticket {
            width: 58mm;
            max-width: 58mm;
        }
        .center {
            text-align: center;
        }
        .line {
            border-top: 1px dashed black;
            margin: 5px 0;
        }
        .mb { margin-bottom: 4px; }
        @media print {
            @page { size: 58mm auto; margin: 0; }
            body { width: 58mm; margin: 0; padding: 4px; }
            .no-print { display: none !important; }
        }
    </style>
</head>
<body>
    <div class="ticket">
        <div class="center mb">LA QUE NO FALLA</div>
        <div class="center mb">ENTREGA DE CAJA</div>
        <div class="line"></div>

        <div class="mb">Cajero: {{ cajero }}</div>
        <div class="mb">Fecha: {{ fecha }}</div>

        <div class="mb">Ventas: RD$ {{ ventas }}</div>
        <div class="mb">Premios: RD$ {{ premios }}</div>

        <div class="line"></div>
        <div class="center mb">ENTREGADO</div>
        <div class="center mb"><strong>RD$ {{ ganancia }}</strong></div>
        <div class="line"></div>

        <div class="mb">Administrador: {{ admin }}</div>
        <div class="mb">Firma Cajero: ______</div>
        <div class="mb">Firma Admin: ______</div>

        <div class="line"></div>
        <div class="center mb">Gracias</div>
    </div>
    <div class="no-print" style="margin-top:16px;text-align:center">
        <button onclick="window.print()" style="padding:10px 20px;background:#002D62;color:white;border:none;border-radius:8px;cursor:pointer;font-weight:bold">🖨 Imprimir</button>
        <br><br>
        <a href="/admin/banco_cajeros" style="color:#002D62;font-weight:bold">← Volver a Banco Cajeros</a>
    </div>
    <script>
        window.onload = function() { window.print(); };
    </script>
</body>
</html>
"""


@app.route("/admin/banco_cajeros/recibo_entrega_58mm")
def recibo_entrega_58mm():
    """Recibo 58mm para entrega de caja (térmica). Auto-print cuando autoprint=1."""
    if not is_admin_or_super():
        return redirect("/")
    cajero = (request.args.get("cajero") or "").strip()
    vs = request.args.get("ventas")
    ps = request.args.get("premios")
    try:
        ventas = float(vs) if vs not in (None, "") else None
    except (ValueError, TypeError):
        ventas = None
    try:
        premios = float(ps) if ps not in (None, "") else None
    except (ValueError, TypeError):
        premios = None
    try:
        monto = float(request.args.get("monto") or 0)
    except (ValueError, TypeError):
        monto = 0
    admin = (request.args.get("admin") or "").strip() or "—"
    fecha_raw = request.args.get("fecha") or ""
    if fecha_raw and hasattr(fecha_raw, "strftime"):
        fecha = fecha_raw.strftime("%Y-%m-%d %H:%M")
    else:
        fecha = str(fecha_raw) if fecha_raw else ahora_rd().strftime("%Y-%m-%d %H:%M")
    ventas = "{:,.2f}".format(ventas) if ventas is not None else "0.00"
    premios = "{:,.2f}".format(premios) if premios is not None else "0.00"
    ganancia = "{:,.2f}".format(monto) if monto is not None else "0.00"
    return render_template_string(
        RECIBO_ENTREGA_58MM_HTML,
        cajero=cajero,
        fecha=fecha,
        ventas=ventas,
        premios=premios,
        ganancia=ganancia,
        admin=admin,
    )


@app.route("/admin/banco_cajeros/historial/<path:cajero_enc>")
def historial_entregas_cajero(cajero_enc):
    """Historial de entregas de un cajero."""
    if not is_admin_or_super():
        return redirect("/")
    from urllib.parse import unquote
    cajero = unquote(cajero_enc)
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    entregas = []
    try:
        cur = conn.cursor()
        cur.execute(_sql("""
            SELECT id, cajero, monto, fecha, admin
            FROM pagos_cajeros
            WHERE cajero = %s
            ORDER BY fecha DESC
            LIMIT 100
        """), (cajero,))
        for r in cur.fetchall():
            d = dict(r) if hasattr(r, "keys") else r
            if hasattr(d, "get"):
                f = d.get("fecha") or ""
                monto = float(d.get("monto") or 0)
                admin = d.get("admin") or "—"
            else:
                f = d[3] if len(d) > 3 else ""
                monto = float(d[2] if len(d) > 2 else 0)
                admin = d[4] if len(d) > 4 else "—"
            if hasattr(f, "strftime"):
                f = f.strftime("%Y-%m-%d %H:%M") if f else ""
            entregas.append({"monto": monto, "fecha": str(f) if f else "", "admin": admin})
    finally:
        conn.close()
    return render_template_string(IOS + ADMIN_MENU + """
<div class="card" style="max-width:640px;margin:20px auto">
    <h2>📋 Historial de pagos: {{ cajero }}</h2>
    <p style="color:#64748b;font-size:14px;margin-bottom:16px">Fecha | Cajero | Monto Pagado</p>
    <table width="100%" style="border-collapse:collapse">
        <tr style="background:#002D62;color:white">
            <th style="padding:10px;text-align:left">Fecha</th>
            <th style="padding:10px;text-align:left">Cajero</th>
            <th style="padding:10px;text-align:right">Monto Pagado</th>
            <th style="padding:10px">Acciones</th>
        </tr>
        {% for e in entregas %}
        <tr style="border-bottom:1px solid #e2e8f0">
            <td style="padding:10px">{{ e.fecha }}</td>
            <td style="padding:10px">{{ cajero }}</td>
            <td style="padding:10px;text-align:right;font-weight:600">RD$ {{ "{:,.2f}".format(e.monto) }}</td>
            <td style="padding:10px"><a href="{{ url_for('recibo_entrega_58mm', cajero=cajero, monto=e.monto, fecha=e.fecha, admin=e.admin) }}" style="color:#002D62;font-size:13px">🖨 Imprimir</a></td>
        </tr>
        {% endfor %}
    </table>
    {% if not entregas %}
    <p style="color:#666">No hay pagos registrados.</p>
    {% endif %}
    <br>
    <a href="/admin/banco_cajeros" style="color:#002D62;font-weight:bold">← Volver a Banco Cajeros</a>
</div>
""", cajero=cajero, entregas=entregas)


@app.route("/numeros_populares")
def numeros_populares():
    if not is_admin_or_super():
        return redirect("/venta")
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute("""
            SELECT
                number AS number,
                COUNT(*) AS veces,
                COALESCE(SUM(amount),0) AS total
            FROM ticket_lines
            WHERE COALESCE(estado,'activo') != 'cancelado'
            GROUP BY number
            ORDER BY veces DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        rows = [dict(r) for r in rows] if rows else []
    finally:
        c.close()
    html = IOS + """
    <div style="max-width:600px;margin:80px auto;padding:20px">
    <h2>N&uacute;meros m&aacute;s jugados (Top 20)</h2>
    <table style="width:100%;border-collapse:collapse;margin-top:16px">
    <tr style="background:#002D62;color:white"><th style="padding:10px;text-align:left">N&uacute;mero</th><th style="padding:10px;text-align:center">Veces</th><th style="padding:10px;text-align:right">Total RD$</th></tr>
    {% for r in rows %}
    <tr><td style="padding:10px;border:1px solid #e2e8f0">{{ r.number }}</td><td style="padding:10px;border:1px solid #e2e8f0;text-align:center">{{ r.veces }}</td><td style="padding:10px;border:1px solid #e2e8f0;text-align:right">{{ "%.2f"|format(r.total|float) }}</td></tr>
    {% endfor %}
    </table>
    <p style="margin-top:20px"><a href="/admin" style="color:#002D62;font-weight:bold">← Volver al panel</a></p>
    </div>
    """
    return render_template_string(html, rows=rows)


# ===============================
# RESET SISTEMA (BORRAR TODO)
# ===============================
@app.route("/reset_sistema", methods=["POST"])
def reset_sistema():

    # 🔒 solo admin
    if not is_admin_or_super():
        return redirect("/venta")

    conn = db()
    if not conn:
        return "Error de conexión", 500

    cur = conn.cursor()

    try:
        # ===============================
        # BORRAR DATOS
        # ===============================
        cur.execute("DELETE FROM ticket_lines")
        cur.execute("DELETE FROM tickets")
        cur.execute("DELETE FROM cash_closings")

        # ===============================
        # REINICIAR IDS
        # ===============================

        database_url = os.environ.get("DATABASE_URL")

        if database_url:
            # 🔥 POSTGRESQL (Render)
            cur.execute("ALTER SEQUENCE ticket_lines_id_seq RESTART WITH 1")
            cur.execute("ALTER SEQUENCE tickets_id_seq RESTART WITH 1")
            cur.execute("ALTER SEQUENCE cash_closings_id_seq RESTART WITH 1")
        else:
            # 🔥 SQLITE (local)
            cur.execute("DELETE FROM sqlite_sequence WHERE name='ticket_lines'")
            cur.execute("DELETE FROM sqlite_sequence WHERE name='tickets'")
            cur.execute("DELETE FROM sqlite_sequence WHERE name='cash_closings'")

        conn.commit()

    except Exception as e:
        conn.rollback()
        return f"Error al resetear: {e}"

    finally:
        cur.close()
        conn.close()

    return redirect("/admin")


# ===============================
# 🖨 IMPRIMIR CIERRE POR CAJERO (solo jugadas del cajero logueado)
# ===============================
@app.route("/imprimir_cierre")
def imprimir_cierre_cajero():
    cajero = session.get("u") or session.get("user")
    if not cajero:
        return redirect("/")
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        hoy = ahora_rd().strftime("%Y-%m-%d")
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play,
                       SUM(ticket_lines.amount) AS total
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                GROUP BY ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play
                ORDER BY ticket_lines.lottery, ticket_lines.draw
            """, (hoy, cajero))
        else:
            cur.execute(_sql("""
                SELECT ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play,
                       SUM(ticket_lines.amount) AS total
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at) = %s
                AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                GROUP BY ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play
                ORDER BY ticket_lines.lottery, ticket_lines.draw
            """), (hoy, cajero))
        numeros_vendidos = cur.fetchall()
        numeros_vendidos_list = []
        for r in numeros_vendidos:
            if hasattr(r, "keys"):
                numeros_vendidos_list.append({
                    "lottery": r.get("lottery") or "",
                    "draw": r.get("draw") or "",
                    "number": r.get("number") or "",
                    "play": r.get("play") or "Quiniela",
                    "total": float(r.get("total") or 0),
                })
            else:
                numeros_vendidos_list.append({
                    "lottery": (r[0] if len(r) > 0 else "") or "",
                    "draw": (r[1] if len(r) > 1 else "") or "",
                    "number": (r[2] if len(r) > 2 else "") or "",
                    "play": (r[3] if len(r) > 3 else "Quiniela") or "Quiniela",
                    "total": float((r[4] if len(r) > 4 else 0) or 0),
                })
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            """, (hoy, cajero))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at) = %s AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            """), (hoy, cajero))
        row = cur.fetchone()
        total_ventas = float((list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)) or 0)
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COUNT(DISTINCT tickets.id)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            """, (hoy, cajero))
        else:
            cur.execute(_sql("""
                SELECT COUNT(DISTINCT tickets.id)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at) = %s AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            """), (hoy, cajero))
        row_t = cur.fetchone()
        total_tickets = int((list(row_t.values())[0] if row_t and hasattr(row_t, "values") else (row_t[0] if row_t else 0)) or 0)

        # Premios pagados por este cajero (desde pagos_premios)
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(monto),0) FROM pagos_premios
                WHERE cajero = %s AND (fecha AT TIME ZONE 'America/Santo_Domingo')::date = %s
            """, (cajero, hoy))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(monto),0) FROM pagos_premios
                WHERE cajero = %s AND DATE(fecha) = %s
            """), (cajero, hoy))
        rp = cur.fetchone()
        total_pagado = float((list(rp.values())[0] if rp and hasattr(rp, "values") else (rp[0] if rp else 0)) or 0)
        ganancia = total_ventas - total_pagado

        # Total por tipo de jugada (Pale, Quiniela, etc.) - solo de este cajero
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT ticket_lines.play, COALESCE(SUM(ticket_lines.amount),0) AS total
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                GROUP BY ticket_lines.play
                ORDER BY ticket_lines.play
            """, (hoy, cajero))
        else:
            cur.execute(_sql("""
                SELECT ticket_lines.play, COALESCE(SUM(ticket_lines.amount),0) AS total
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at) = %s AND tickets.cajero = %s
                AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                GROUP BY ticket_lines.play
                ORDER BY ticket_lines.play
            """), (hoy, cajero))
        por_tipo_play = cur.fetchall()
        por_tipo_play_list = []
        for r in por_tipo_play:
            if hasattr(r, "keys"):
                por_tipo_play_list.append({"play": r.get("play") or "Sin tipo", "total": float(r.get("total") or 0)})
            else:
                por_tipo_play_list.append({"play": (r[0] if r else "Sin tipo") or "Sin tipo", "total": float((r[1] if len(r) > 1 else 0) or 0)})
    finally:
        c.close()
    html = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width">
<style>
body{font-family:Arial;background:#f5f5f5;margin:0;padding:20px}
.container{max-width:420px;margin:auto;background:white;padding:20px;border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.1)}
.line{border-top:1px solid #ddd;margin:12px 0}
.ticket-print{display:none}
.ticket-print,.ticket pre{margin:0;padding:0;white-space:pre-wrap;font-family:monospace;font-size:11px;line-height:1.2;text-align:left}
.ticket{width:58mm;font-family:monospace;font-size:11px;line-height:1.2;margin:0;padding:5px;text-align:left}
@media print{
  @page{size:58mm auto;margin:0}
  body{margin:0;padding:0}
  body *{visibility:hidden}
  .ticket-print,.ticket-print *{visibility:visible}
  .ticket-print{display:block !important;position:absolute;top:0;left:0;width:58mm;font-family:monospace;font-size:11px;line-height:1.2;margin:0;padding:5px;text-align:left}
  .container{display:none !important}
  pre{white-space:pre-wrap;margin:0}
  .ticket-print hr{border:none;border-top:1px dashed #000;margin:4px 0}
}
</style>
</head>
<body>
<div class="container">
<h2 style="text-align:center">🖨 Cierre por Cajero</h2>
<div class="line"></div>
Cajero: <b>{{ cajero }}</b><br>
Fecha: {{ fecha }}<br>
Tickets: {{ total_tickets }} | Ventas: RD$ {{ "%.2f"|format(total_ventas) }}<br>
Premios: RD$ {{ "%.2f"|format(total_pagado) }} | Balance: RD$ {{ "%.2f"|format(ganancia) }}
<div class="line"></div>
<button onclick="window.print()" style="background:#000;color:#fff;padding:12px;width:100%;border:none;border-radius:8px">🖨️ IMPRIMIR MI CIERRE</button>
<br><br><a href="/venta">⬅ Volver a Venta</a>
</div>
<div class="ticket-print ticket">
<pre>========================
LA QUE NO FALLA
CIERRE BANCA
========================

Cajero: {{ cajero }}
Fecha: {{ fecha }}

Tickets vendidos: {{ total_tickets }}
Ventas: RD$ {{ "%.2f"|format(total_ventas) }}
Premios pagados: RD$ {{ "%.2f"|format(total_pagado) }}
Balance final: RD$ {{ "%.2f"|format(ganancia) }}

---------------------
DETALLE DE JUGADAS
---------------------
{% for p in por_tipo_play_list %}{{ p.play }}: RD$ {{ "%.2f"|format(p.total) }}
{% endfor %}

---------------------
NUMEROS VENDIDOS
---------------------
{% for j in numeros_vendidos_list %}{{ j.number }} | {{ j.play }} | {{ j.lottery }}{% if j.draw %} {{ j.draw }}{% endif %} | RD${{ "%.2f"|format(j.total) }}
{% endfor %}

LA QUE NO FALLA
</pre>
</div>
</body>
</html>
"""
    return render_template_string(
        html,
        cajero=cajero,
        fecha=hoy,
        numeros_vendidos_list=numeros_vendidos_list,
        por_tipo_play_list=por_tipo_play_list,
        total_ventas=total_ventas,
        total_pagado=total_pagado,
        ganancia=ganancia,
        total_tickets=total_tickets,
    )


# ===============================
# 🏦 IMPRIMIR CIERRE ULTRA PRO 58MM (BANCA REAL) - ADMIN
# ===============================
@app.route("/admin/imprimir_cierre")
def imprimir_cierre():
    if not is_admin_or_super():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    cur = c.cursor()
    hoy = ahora_rd().strftime("%Y-%m-%d")

    # ===============================
    # TOTAL VENDIDO (SOLO ACTIVOS)
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
        SELECT COALESCE(SUM(ticket_lines.amount),0)
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
        AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        """, (hoy,))
    else:
        cur.execute(_sql("""
        SELECT COALESCE(SUM(ticket_lines.amount),0)
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE DATE(tickets.created_at)=%s
        AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        """), (hoy,))
    row = cur.fetchone()
    total_ventas = (list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)) or 0

    # ===============================
    # TOTAL PAGADO PREMIOS (desde pagos_premios)
    # ===============================
    try:
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(monto),0)
                FROM pagos_premios
                WHERE (fecha AT TIME ZONE 'America/Santo_Domingo')::date = %s
            """, (hoy,))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(monto),0)
                FROM pagos_premios
                WHERE DATE(fecha) = %s
            """), (hoy,))
        rp = cur.fetchone()
        total_pagado = float((list(rp.values())[0] if rp and hasattr(rp, "values") else (rp[0] if rp else 0)) or 0)
    except Exception:
        total_pagado = 0.0

    # ===============================
    # BALANCE BANCA
    # ===============================
    ganancia = total_ventas - total_pagado
    perdida = ganancia < 0

    # ===============================
    # JUGADAS POR CAJERO (orden: cajero -> ticket -> jugada)
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
        SELECT
        tickets.id AS ticket_id,
        COALESCE(users.username, tickets.cajero) AS cajero,
        ticket_lines.lottery,
        ticket_lines.play,
        ticket_lines.number,
        ticket_lines.amount
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        LEFT JOIN users ON tickets.cajero = users.username
        WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
        AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        ORDER BY COALESCE(users.username, tickets.cajero), tickets.id, ticket_lines.id
        """, (hoy,))
    else:
        cur.execute(_sql("""
        SELECT
        tickets.id AS ticket_id,
        COALESCE(users.username, tickets.cajero) AS cajero,
        ticket_lines.lottery,
        ticket_lines.play,
        ticket_lines.number,
        ticket_lines.amount
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        LEFT JOIN users ON tickets.cajero = users.username
        WHERE DATE(tickets.created_at)=%s
        AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        ORDER BY COALESCE(users.username, tickets.cajero), tickets.id, ticket_lines.id
        """), (hoy,))
    jugadas = cur.fetchall()

    # ===============================
    # TOTAL POR LOTERIA (SOLO ACTIVOS)
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
        SELECT 
            ticket_lines.lottery,
            COALESCE(SUM(ticket_lines.amount),0) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
        AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        GROUP BY ticket_lines.lottery
        """, (hoy,))
    else:
        cur.execute(_sql("""
        SELECT 
            ticket_lines.lottery,
            COALESCE(SUM(ticket_lines.amount),0) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE DATE(tickets.created_at)=%s
        AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        GROUP BY ticket_lines.lottery
        """), (hoy,))
    por_loteria = cur.fetchall()

    # ===============================
    # NUMEROS VENDIDOS (agrupado: lottery, draw, number, play, total)
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
        SELECT
            ticket_lines.lottery,
            ticket_lines.draw,
            ticket_lines.number,
            ticket_lines.play,
            SUM(ticket_lines.amount) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
        AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        GROUP BY ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play
        ORDER BY ticket_lines.lottery, ticket_lines.draw
        """, (hoy,))
    else:
        cur.execute(_sql("""
        SELECT
            ticket_lines.lottery,
            ticket_lines.draw,
            ticket_lines.number,
            ticket_lines.play,
            SUM(ticket_lines.amount) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE DATE(tickets.created_at) = %s
        AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
        GROUP BY ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play
        ORDER BY ticket_lines.lottery, ticket_lines.draw
        """), (hoy,))
    numeros_vendidos = cur.fetchall()

    # ===============================
    # TOTAL POR CAJERO
    # ===============================
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
        SELECT tickets.cajero, COALESCE(SUM(ticket_lines.amount),0) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
        AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        GROUP BY tickets.cajero
        """, (hoy,))
    else:
        cur.execute(_sql("""
        SELECT tickets.cajero, COALESCE(SUM(ticket_lines.amount),0) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE DATE(tickets.created_at)=%s
        AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        GROUP BY tickets.cajero
        """), (hoy,))
    por_cajero = cur.fetchall()

    # TOTAL POR TIPO DE JUGADA (Quiniela, Pale, Tripleta, etc.)
    if os.environ.get("DATABASE_URL"):
        cur.execute("""
        SELECT ticket_lines.play, COALESCE(SUM(ticket_lines.amount),0) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
        AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        GROUP BY ticket_lines.play
        ORDER BY ticket_lines.play
        """, (hoy,))
    else:
        cur.execute(_sql("""
        SELECT ticket_lines.play, COALESCE(SUM(ticket_lines.amount),0) AS total
        FROM ticket_lines
        JOIN tickets ON tickets.id = ticket_lines.ticket_id
        WHERE DATE(tickets.created_at)=%s
        AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        GROUP BY ticket_lines.play
        ORDER BY ticket_lines.play
        """), (hoy,))
    por_tipo_play_raw = cur.fetchall()
    por_tipo_play = []
    for r in (por_tipo_play_raw or []):
        if hasattr(r, "keys"):
            d = dict(zip(r.keys(), r)) if not hasattr(r, "get") else r
            por_tipo_play.append({"play": d.get("play") or "Quiniela", "total": float(d.get("total") or 0)})
        else:
            por_tipo_play.append({"play": (r[0] if len(r) > 0 else "Quiniela") or "Quiniela", "total": float((r[1] if len(r) > 1 else 0) or 0)})

    total_tickets = len(set([(j.get("ticket_id") if hasattr(j, "get") else j[0]) for j in jugadas]))
    # Lista para NUMEROS VENDIDOS: lottery, draw, number, play, total (agrupado)
    numeros_vendidos_list = []
    for r in numeros_vendidos:
        if hasattr(r, "keys"):
            d = dict(zip(r.keys(), r)) if not hasattr(r, "get") else r
            numeros_vendidos_list.append({
                "lottery": d.get("lottery") or "",
                "draw": d.get("draw") or "",
                "number": d.get("number") or "",
                "play": d.get("play") or "Quiniela",
                "total": float(d.get("total") or 0),
            })
        else:
            numeros_vendidos_list.append({
                "lottery": (r[0] if len(r) > 0 else "") or "",
                "draw": (r[1] if len(r) > 1 else "") or "",
                "number": (r[2] if len(r) > 2 else "") or "",
                "play": (r[3] if len(r) > 3 else "Quiniela") or "Quiniela",
                "total": float((r[4] if len(r) > 4 else 0) or 0),
            })

    c.close()

    html = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width">
<style>
body{font-family:Arial;background:#f5f5f5;margin:0;padding:20px}
.container{max-width:420px;margin:auto;background:white;padding:20px;border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.1)}
.line{border-top:1px solid #ddd;margin:12px 0}
.ticket-print{display:none}
.ticket-print,.ticket pre{margin:0;padding:0;white-space:pre-wrap;font-family:monospace;font-size:12px;line-height:1.2;text-align:left}
.ticket{width:80mm;font-family:monospace;font-size:12px;line-height:1.2;margin:0;padding:0;text-align:left}
@media print{
  body{margin:0;padding:0}
  body *{visibility:hidden}
  .ticket-print,.ticket-print *{visibility:visible}
  .ticket-print{display:block !important;position:absolute;top:0;left:0;width:80mm;margin:0;padding:8px;text-align:left}
  .container{display:none !important}
  @page{size:80mm auto;margin:0}
  .ticket-print hr{border:none;border-top:1px dashed #000;margin:4px 0}
}
</style>
</head>
<body>

<div class="container">
<h2 style="text-align:center">🏦 Cierre Banca - Ticket 80mm</h2>
<div class="line"></div>
Fecha: {{ hoy }}<br>
Tickets: {{ total_tickets }} | Ventas: RD$ {{ "%.2f"|format(total_ventas) }}<br>
Premios: RD$ {{ "%.2f"|format(total_pagado) }} | Balance: RD$ {{ "%.2f"|format(ganancia) }}
<div class="line"></div>
<button onclick="window.print()" style="background:#000;color:#fff;padding:12px;width:100%;border:none;border-radius:8px">🖨️ IMPRIMIR TICKET 80MM</button>
<br><br><a href="/admin">⬅ Volver</a>
</div>

<div class="ticket-print ticket">
<pre>========================
LA QUE NO FALLA
CIERRE BANCA
========================

Fecha: {{ hoy }}

Tickets vendidos: {{ total_tickets }}
Ventas: RD$ {{ "%.2f"|format(total_ventas) }}
Premios pagados: RD$ {{ "%.2f"|format(total_pagado) }}
Balance final: RD$ {{ "%.2f"|format(ganancia) }}

---------------------
DETALLE DE JUGADAS
---------------------
{% for p in por_tipo_play %}{{ p.play or 'Sin tipo' }}: RD$ {{ "%.2f"|format(p.total) }}
{% endfor %}

---------------------
NUMEROS VENDIDOS
---------------------
{% for j in numeros_vendidos_list %}{{ j.number }} | {{ j.play }} | {{ j.lottery }}{% if j.draw %} {{ j.draw }}{% endif %} | RD${{ "%.2f"|format(j.total) }}
{% endfor %}

LA QUE NO FALLA
</pre>
</div>

</body>
</html>
"""

    return render_template_string(
        html,
        hoy=hoy,
        numeros_vendidos_list=numeros_vendidos_list,
        por_tipo_play=por_tipo_play,
        por_loteria=por_loteria,
        por_cajero=por_cajero,
        total_ventas=total_ventas,
        total_pagado=total_pagado,
        ganancia=ganancia,
        perdida=perdida,
        total_tickets=total_tickets,
    )


# ===============================
# HISTORIAL DE CIERRES
# ===============================
@app.route("/admin/historial_cierres")
def historial_cierres():
    if not is_admin_or_super():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute("SELECT DISTINCT date FROM cash_closings ORDER BY date DESC")
        fechas_cierre = cur.fetchall()

        def _v(row):
            if not row: return 0
            return list(row.values())[0] if hasattr(row, "keys") else row[0]

        cierres = []
        for row in fechas_cierre:
            fecha = row.get("date", row[0]) if hasattr(row, "get") else row[0]

            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT COUNT(DISTINCT tickets.id)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
                """, (fecha,))
            else:
                cur.execute(_sql("""
                    SELECT COUNT(DISTINCT tickets.id)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at)=%s
                    AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
                """), (fecha,))
            r1 = cur.fetchone()
            total_tickets = int(_v(r1) or 0)

            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT COALESCE(SUM(ticket_lines.amount),0)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
                """, (fecha,))
            else:
                cur.execute(_sql("""
                    SELECT COALESCE(SUM(ticket_lines.amount),0)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at)=%s
                    AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
                """), (fecha,))
            r2 = cur.fetchone()
            ventas = float(_v(r2) or 0)

            try:
                if os.environ.get("DATABASE_URL"):
                    cur.execute("""
                        SELECT COALESCE(SUM(monto),0)
                        FROM pagos_premios
                        WHERE (fecha AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    """, (fecha,))
                else:
                    cur.execute(_sql("SELECT COALESCE(SUM(monto),0) FROM pagos_premios WHERE DATE(fecha)=%s"), (fecha,))
                r3 = cur.fetchone()
                pagado = float(_v(r3) or 0)
            except Exception:
                pagado = 0

            balance = ventas - pagado

            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT COUNT(DISTINCT tickets.cajero)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
                """, (fecha,))
            else:
                cur.execute(_sql("""
                    SELECT COUNT(DISTINCT tickets.cajero)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at)=%s
                    AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
                """), (fecha,))
            r4 = cur.fetchone()
            num_cajeros = int(_v(r4) or 0)

            cierres.append({
                "fecha": fecha,
                "total_tickets": total_tickets,
                "ventas": ventas,
                "pagado": pagado,
                "balance": balance,
                "num_cajeros": num_cajeros
            })
    finally:
        c.close()

    return render_template_string(IOS + """
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<div class="container py-4">
<div class="d-flex justify-content-between align-items-center mb-4">
<h2>📋 Historial de Cierres</h2>
<a href="/admin" class="btn btn-outline-secondary">⬅ Volver</a>
</div>
<div class="card shadow">
<div class="card-body p-0">
<table class="table table-striped table-hover mb-0">
<thead class="table-dark">
<tr>
<th>Fecha</th>
<th>Tickets</th>
<th>Ventas</th>
<th>Pagado</th>
<th>Balance</th>
<th>Cajeros</th>
<th>Acción</th>
</tr>
</thead>
<tbody>
{% for c in cierres %}
<tr>
<td>{{ c.fecha }}</td>
<td>{{ c.total_tickets }}</td>
<td>RD$ {{ "{:,.2f}".format(c.ventas) }}</td>
<td>RD$ {{ "{:,.2f}".format(c.pagado) }}</td>
<td>RD$ {{ "{:,.2f}".format(c.balance) }}</td>
<td>{{ c.num_cajeros }}</td>
<td>
<a href="/admin/cierre_detalle/{{ c.fecha }}" class="btn btn-sm btn-primary">Ver Detalle</a>
</td>
</tr>
{% else %}
<tr><td colspan="7" class="text-center text-muted">No hay cierres registrados</td></tr>
{% endfor %}
</tbody>
</table>
</div>
</div>
</div>
""", cierres=cierres)


# ===============================
# DETALLE DE CIERRE
# ===============================
@app.route("/admin/cierre_detalle/<fecha>")
def cierre_detalle(fecha):
    if not is_admin_or_super():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            """, (fecha,))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at)=%s
                AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            """), (fecha,))
        row = cur.fetchone()
        total_ventas = (list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)) or 0

        try:
            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT COALESCE(SUM(monto),0)
                    FROM pagos_premios
                    WHERE (fecha AT TIME ZONE 'America/Santo_Domingo')::date = %s
                """, (fecha,))
            else:
                cur.execute(_sql("""
                    SELECT COALESCE(SUM(monto),0)
                    FROM pagos_premios
                    WHERE DATE(fecha) = %s
                """), (fecha,))
            rp = cur.fetchone()
            total_pagado = float((list(rp.values())[0] if rp and hasattr(rp, "values") else (rp[0] if rp else 0)) or 0)
        except Exception:
            total_pagado = 0.0

        ganancia = total_ventas - total_pagado

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
            SELECT tickets.id AS ticket_id,
            COALESCE(users.username, tickets.cajero) AS cajero,
            ticket_lines.lottery, ticket_lines.play, ticket_lines.number, ticket_lines.amount
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            LEFT JOIN users ON tickets.cajero = users.username
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            ORDER BY COALESCE(users.username, tickets.cajero), tickets.id, ticket_lines.id
            """, (fecha,))
        else:
            cur.execute(_sql("""
            SELECT tickets.id AS ticket_id,
            COALESCE(users.username, tickets.cajero) AS cajero,
            ticket_lines.lottery, ticket_lines.play, ticket_lines.number, ticket_lines.amount
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            LEFT JOIN users ON tickets.cajero = users.username
            WHERE DATE(tickets.created_at)=%s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            ORDER BY COALESCE(users.username, tickets.cajero), tickets.id, ticket_lines.id
            """), (fecha,))
        jugadas = cur.fetchall()

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
            SELECT ticket_lines.lottery, COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            GROUP BY ticket_lines.lottery ORDER BY ticket_lines.lottery
            """, (fecha,))
        else:
            cur.execute(_sql("""
            SELECT ticket_lines.lottery, COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at)=%s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            GROUP BY ticket_lines.lottery ORDER BY ticket_lines.lottery
            """), (fecha,))
        por_loteria = cur.fetchall()

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
            SELECT tickets.cajero, COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            GROUP BY tickets.cajero ORDER BY tickets.cajero
            """, (fecha,))
        else:
            cur.execute(_sql("""
            SELECT tickets.cajero, COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at)=%s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            GROUP BY tickets.cajero ORDER BY tickets.cajero
            """), (fecha,))
        por_cajero = cur.fetchall()

        total_tickets = len(set(j["ticket_id"] for j in jugadas))
    finally:
        c.close()

    return render_template_string(IOS + """
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
.ticket-print{display:none}
@media print{
body *{visibility:hidden}
.ticket-print,.ticket-print *{visibility:visible}
.ticket-print{display:block!important;position:absolute;left:0;top:0;width:58mm;font-family:monospace;font-size:11px;padding:4px;margin:0}
body{width:58mm;margin:0;padding:0;font-family:monospace;font-size:11px}
.detalle-screen{display:none!important}
@page{size:58mm auto;margin:2mm}
}
.line{border-top:1px solid #ddd;margin:8px 0}
</style>
<div class="detalle-screen container py-4">
<div class="d-flex justify-content-between align-items-center mb-4">
<h2>📊 Detalle Cierre {{ fecha }}</h2>
<div>
<a href="/admin/imprimir_cierre_fecha/{{ fecha }}" target="_blank" class="btn btn-success">🖨️ Imprimir Cierre</a>
<a href="/admin/historial_cierres" class="btn btn-outline-secondary">⬅ Volver</a>
</div>
</div>

<div class="card shadow mb-4">
<div class="card-header bg-primary text-white"><b>RESUMEN DEL CIERRE</b></div>
<div class="card-body">
<p><b>Fecha:</b> {{ fecha }}</p>
<p><b>Tickets vendidos:</b> {{ total_tickets }}</p>
<p><b>Ventas totales:</b> RD$ {{ "{:,.2f}".format(total_ventas) }}</p>
<p><b>Premios pagados:</b> RD$ {{ "{:,.2f}".format(total_pagado) }}</p>
<p><b>Balance final:</b> RD$ {{ "{:,.2f}".format(ganancia) }}</p>
</div>
</div>

<div class="row">
<div class="col-md-6">
<div class="card shadow mb-4">
<div class="card-header">TOTAL POR LOTERÍA</div>
<div class="card-body">
<ul class="list-group list-group-flush">
{% for l in por_loteria %}
<li class="list-group-item d-flex justify-content-between">{{ l.lottery }} <span>RD$ {{ "{:,.2f}".format(l.total) }}</span></li>
{% endfor %}
</ul>
</div>
</div>
</div>
<div class="col-md-6">
<div class="card shadow mb-4">
<div class="card-header">TOTAL POR CAJERO</div>
<div class="card-body">
<ul class="list-group list-group-flush">
{% for c in por_cajero %}
<li class="list-group-item d-flex justify-content-between">{{ c.cajero }} <span>RD$ {{ "{:,.2f}".format(c.total) }}</span></li>
{% endfor %}
</ul>
</div>
</div>
</div>
</div>

<div class="card shadow">
<div class="card-header">DETALLE DE JUGADAS</div>
<div class="card-body" style="font-family:monospace;font-size:13px">
{% for cajero, jugadas_cajero in jugadas|groupby('cajero') %}
<div class="mb-4">
<div class="bg-light p-2 mb-2"><b>====================</b><br><b>CAJERO: {{ cajero }}</b><br><b>====================</b></div>
{% for j in jugadas_cajero %}
<div class="mb-1">
Ticket {{ j.ticket_id }}<br>
{{ j.lottery }}<br>
{{ j.play }} {{ j.number }}  RD${{ "%.2f"|format(j.amount) }}
</div>
{% endfor %}
</div>
{% endfor %}
</div>
</div>
</div>
""",
        fecha=fecha, total_ventas=total_ventas, total_pagado=total_pagado, ganancia=ganancia,
        jugadas=jugadas, por_loteria=por_loteria, por_cajero=por_cajero, total_tickets=total_tickets)


# ===============================
# IMPRIMIR CIERRE POR FECHA (ticket 58mm)
# ===============================
@app.route("/admin/imprimir_cierre_fecha/<fecha>")
def imprimir_cierre_fecha(fecha):
    if not is_admin_or_super():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            """, (fecha,))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at)=%s AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            """), (fecha,))
        rv = cur.fetchone()
        total_ventas = float((list(rv.values())[0] if rv and hasattr(rv, "keys") else (rv[0] if rv else 0)) or 0)

        try:
            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT COALESCE(SUM(monto),0)
                    FROM pagos_premios
                    WHERE (fecha AT TIME ZONE 'America/Santo_Domingo')::date = %s
                """, (fecha,))
            else:
                cur.execute(_sql("""
                    SELECT COALESCE(SUM(monto),0)
                    FROM pagos_premios
                    WHERE DATE(fecha) = %s
                """), (fecha,))
            rp = cur.fetchone()
            total_pagado = float((list(rp.values())[0] if rp and hasattr(rp, "keys") else (rp[0] if rp else 0)) or 0)
        except Exception:
            total_pagado = 0.0

        ganancia = total_ventas - total_pagado

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
            SELECT tickets.id AS ticket_id, COALESCE(users.username, tickets.cajero) AS cajero,
            ticket_lines.lottery, ticket_lines.play, ticket_lines.number, ticket_lines.amount
            FROM ticket_lines JOIN tickets ON tickets.id = ticket_lines.ticket_id
            LEFT JOIN users ON tickets.cajero = users.username
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            ORDER BY COALESCE(users.username, tickets.cajero), tickets.id, ticket_lines.id
            """, (fecha,))
        else:
            cur.execute(_sql("""
            SELECT tickets.id AS ticket_id, COALESCE(users.username, tickets.cajero) AS cajero,
            ticket_lines.lottery, ticket_lines.play, ticket_lines.number, ticket_lines.amount
            FROM ticket_lines JOIN tickets ON tickets.id = ticket_lines.ticket_id
            LEFT JOIN users ON tickets.cajero = users.username
            WHERE DATE(tickets.created_at)=%s AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            ORDER BY COALESCE(users.username, tickets.cajero), tickets.id, ticket_lines.id
            """), (fecha,))
        jugadas = cur.fetchall()

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
            SELECT ticket_lines.play, COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            GROUP BY ticket_lines.play
            ORDER BY ticket_lines.play
            """, (fecha,))
        else:
            cur.execute(_sql("""
            SELECT ticket_lines.play, COALESCE(SUM(ticket_lines.amount),0) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at)=%s AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
            GROUP BY ticket_lines.play
            ORDER BY ticket_lines.play
        """), (fecha,))
        por_tipo_play = cur.fetchall()

        # NUMEROS VENDIDOS: lottery, draw, number, play, SUM(amount)
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
            SELECT ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play,
                   SUM(ticket_lines.amount) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            GROUP BY ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play
            ORDER BY ticket_lines.lottery, ticket_lines.draw
            """, (fecha,))
        else:
            cur.execute(_sql("""
            SELECT ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play,
                   SUM(ticket_lines.amount) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at) = %s
            AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
            GROUP BY ticket_lines.lottery, ticket_lines.draw, ticket_lines.number, ticket_lines.play
            ORDER BY ticket_lines.lottery, ticket_lines.draw
            """), (fecha,))
        numeros_vendidos = cur.fetchall()

        total_tickets = len(set((j.get("ticket_id") if hasattr(j, "get") else j[0]) for j in jugadas))
        numeros_vendidos_list = []
        for r in numeros_vendidos:
            if hasattr(r, "keys"):
                numeros_vendidos_list.append({
                    "lottery": r.get("lottery") or "",
                    "draw": r.get("draw") or "",
                    "number": r.get("number") or "",
                    "play": r.get("play") or "Quiniela",
                    "total": float(r.get("total") or 0),
                })
            else:
                numeros_vendidos_list.append({
                    "lottery": (r[0] if len(r) > 0 else "") or "",
                    "draw": (r[1] if len(r) > 1 else "") or "",
                    "number": (r[2] if len(r) > 2 else "") or "",
                    "play": (r[3] if len(r) > 3 else "Quiniela") or "Quiniela",
                    "total": float((r[4] if len(r) > 4 else 0) or 0),
                })
    finally:
        c.close()

    html = r"""
<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
body{font-family:Arial;background:#f5f5f5;margin:0;padding:20px}
.ticket-print{display:none}
.no-print{margin-bottom:10px}
@media print{
  body *{visibility:hidden}
  .ticket-print,.ticket-print *{visibility:visible}
  .ticket-print{display:block!important;width:80mm;margin:0 auto;font-family:monospace;font-size:12px;text-align:center;padding:8px}
  body{width:80mm;margin:0 auto;font-family:monospace;font-size:12px;text-align:center}
  .no-print{display:none!important}
  @page{size:80mm auto;margin:4mm}
  .ticket-print hr{border:none;border-top:1px dashed black;margin:5px 0}
}
</style>
</head><body>
<div class="no-print">
<button onclick="window.print()" style="padding:8px 16px;background:#000;color:#fff;border:none;cursor:pointer">🖨️ Imprimir</button>
<a href="/admin/cierre_detalle/{{ fecha }}" style="margin-left:10px">Volver</a>
</div>
<div class="ticket-print">
========================
<b>LA QUE NO FALLA</b><br>
CIERRE BANCA
========================

Fecha: {{ fecha }}

Tickets vendidos: {{ total_tickets }}
Ventas: RD$ {{ "%.2f"|format(total_ventas) }}
Premios pagados: RD$ {{ "%.2f"|format(total_pagado) }}
Balance final: RD$ {{ "%.2f"|format(ganancia) }}

<hr>
## DETALLE DE JUGADAS
<hr>
{% for p in por_tipo_play %}
{{ p.play or 'Sin tipo' }}: RD$ {{ "%.2f"|format(p.total) }}
{% endfor %}

<hr>
## NUMEROS VENDIDOS
<hr>
{% for j in numeros_vendidos_list %}
{{ j.number }} | {{ j.play }} | {{ j.lottery }}{% if j.draw %} {{ j.draw }}{% endif %} | RD${{ "%.2f"|format(j.total) }}
{% endfor %}
LA QUE NO FALLA
</div>
</body></html>"""
    return render_template_string(
        html,
        fecha=fecha,
        total_tickets=total_tickets,
        total_ventas=total_ventas,
        total_pagado=total_pagado,
        ganancia=ganancia,
        numeros_vendidos_list=numeros_vendidos_list,
        por_tipo_play=por_tipo_play
    )


# =====================================================
# VENTA FULL PRO POS RD 🇩🇴
# =====================================================
@app.route("/venta", methods=["GET","POST"])
def venta():

    # 🔒 BLOQUEO SI CAJA CERRADA
    if not is_admin_or_super() and caja_cerrada_hoy():
        return render_template_string(IOS + """
        <div class="card text-center">
        <h2 class="danger">🔒 Caja Cerrada</h2>
        <p>No se pueden realizar ventas.</p>
        <a href="/">Salir</a>
        </div>
        """)

    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    cur = conn.cursor()

    # ===============================
    # CARGAR LOTERÍAS: mostrar catálogo completo del día
    # (no ocultar por hora pasada, para que siempre aparezcan todas las loterías)
    # ===============================
    cur.execute("SELECT * FROM lotteries ORDER BY lottery, draw")
    rows = cur.fetchall()
    all_lotteries = [dict(r) for r in rows]
    lotteries = [x for x in all_lotteries if sorteo_aplica_hoy(x.get("draw", ""))]

    # ===============================
    # POST → GUARDAR VENTA
    # ===============================
    if request.method == "POST":

        # 1. Crear ticket y obtener id real de PostgreSQL (RETURNING id)
        cajero = session.get("u", session.get("user", "admin"))
        if os.environ.get("DATABASE_URL"):
            cur.execute(_sql("""
                INSERT INTO tickets (cajero, created_at)
                VALUES (%s, (CURRENT_TIMESTAMP AT TIME ZONE 'America/Santo_Domingo'))
                RETURNING id
            """), (cajero,))
            ticket_id = cur.fetchone()["id"]
        else:
            created_rd = ahora_rd().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                "INSERT INTO tickets (cajero, created_at) VALUES (?, ?)",
                (cajero, created_rd)
            )
            cur.execute("SELECT last_insert_rowid()")
            ticket_id = cur.fetchone()[0]
        if not ticket_id:
            conn.rollback()
            conn.close()
            return "Error al crear ticket", 500

        # 2. Construir jugadas válidas desde el formulario
        loteria_list = request.form.getlist("loteria[]")
        loteria2_list = request.form.getlist("loteria2[]")
        sorteo_list = request.form.getlist("sorteo[]")
        numero_list = request.form.getlist("numero[]")
        jugada_list = request.form.getlist("jugada[]")
        monto_list = request.form.getlist("monto[]")

        jugadas = []
        rechazo_cerrada = False
        for i in range(len(numero_list)):
            n = normalizar_numero(numero_list[i] if i < len(numero_list) else "")
            a = monto_list[i] if i < len(monto_list) else ""
            l = loteria_list[i] if i < len(loteria_list) else ""
            l2 = loteria2_list[i] if i < len(loteria2_list) else None
            d = sorteo_list[i] if i < len(sorteo_list) else ""
            p = jugada_list[i] if i < len(jugada_list) else "Quiniela"
            if not n or not a or not l or not d:
                continue
            if not sorteo_aplica_hoy(d) or loteria_cerrada_para_venta(l, d):
                rechazo_cerrada = True
                continue
            if p == "Pale" and "-" not in n:
                continue
            if p == "Super Pale" and (not l2 or l == l2):
                continue
            try:
                a_f = float(a)
                if a_f <= 0:
                    continue
            except (ValueError, TypeError):
                continue
            jugadas.append({
                "lottery": l,
                "lottery2": l2 if p == "Super Pale" else None,
                "draw": d,
                "play": p,
                "number": n,
                "amount": a_f
            })

        if not jugadas:
            conn.rollback()
            conn.close()
            if rechazo_cerrada:
                msg = "⚠️ " + SALES_CLOSED_MESSAGE
            else:
                msg = "No se guardó ninguna jugada válida."
            return render_template_string(IOS + """
            <div class="card">
            <h2 class="danger">""" + msg + """</h2>
            <p style="margin-top:12px;color:#666">Las loterías abren a las 7:00 AM. Cierran 10 minutos antes de cada sorteo (Domingos: Nacional y Leidsa cierran a su hora).</p>
            <a href="/venta" style="display:inline-block;margin-top:16px;padding:10px 20px;background:#002D62;color:white;border-radius:8px;text-decoration:none">Volver a venta</a>
            </div>
            """)

        # 3. Insertar jugadas en ticket_lines
        for j in jugadas:
            cur.execute(_sql("""
                INSERT INTO ticket_lines
                (ticket_id, lottery, lottery2, draw, play, number, amount)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """), (
                ticket_id,
                j["lottery"],
                j["lottery2"],
                j["draw"],
                j["play"],
                j["number"],
                j["amount"]
            ))
            cur.execute(_sql("""
                INSERT INTO historial_jugadas
                (ticket_id, lottery, number, play, amount, created_at)
                VALUES (%s,%s,%s,%s,%s,%s)
            """), (
                ticket_id,
                j["lottery"],
                j["number"],
                j["play"],
                j["amount"],
                ahora_rd().strftime("%Y-%m-%d %H:%M:%S")
            ))
            try:
                if os.environ.get("DATABASE_URL"):
                    cur.execute(_sql("""
                        INSERT INTO balance_por_loteria (lottery, balance)
                        VALUES (%s, %s)
                        ON CONFLICT (lottery)
                        DO UPDATE SET balance = balance_por_loteria.balance + EXCLUDED.balance,
                        updated_at = CURRENT_TIMESTAMP
                    """), (j["lottery"], j["amount"]))
                else:
                    cur.execute("""
                        INSERT INTO balance_por_loteria (lottery, balance)
                        VALUES (?, ?)
                        ON CONFLICT (lottery)
                        DO UPDATE SET balance = balance_por_loteria.balance + excluded.balance,
                        updated_at = CURRENT_TIMESTAMP
                    """, (j["lottery"], j["amount"]))
            except Exception:
                pass

        conn.commit()
        conn.close()
        return redirect(f"/ticket/{ticket_id}")

    # ===============================
    # GET → mostrar formulario de venta (POS)
    # ===============================
    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    usuario = session.get("u") or session.get("user") or ""
    es_admin = is_admin_or_super()
    if es_admin:
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(tl.amount), 0) AS ventas_hoy
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
            """, (hoy_rd,))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(tl.amount), 0) AS ventas_hoy
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                WHERE DATE(t.created_at) = %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
            """), (hoy_rd,))
    else:
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(tl.amount), 0) AS ventas_hoy
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                AND t.cajero = %s
            """, (hoy_rd, usuario))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(tl.amount), 0) AS ventas_hoy
                FROM ticket_lines tl
                JOIN tickets t ON t.id = tl.ticket_id
                WHERE DATE(t.created_at) = %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                AND t.cajero = %s
            """), (hoy_rd, usuario))
    row_v = cur.fetchone()
    ventas_hoy = float((list(row_v.values())[0] if row_v and hasattr(row_v, "values") else (row_v[0] if row_v else 0)) or 0)

    repetir_data = session.pop("repetir_data", None) or []
    conn.close()

    return render_template_string(IOS + """

<style>

/* ===== OCULTAR BANDERA IOS SOLO EN VENTA ===== */
.sidebar img,
.topbar img,
img[src*="flagcdn"]{
display:none !important;
}

/* =====================================================
   LAYOUT VENTA CENTRADO (COMO LOGIN)
===================================================== */

/* contenedor principal */
.venta-wrap{
width:100%;
min-height:100vh;
display:flex;
justify-content:center;
align-items:center;
flex-direction:column;
padding-top:100px; /* espacio para navbar + sintilla metas */
}

/* =====================================================
   CARD PRINCIPAL
===================================================== */

.card{
width:100%;
max-width:520px;
margin:0 auto;
padding:25px;
border-radius:18px;
}

.venta-header{
display:flex;
justify-content:space-between;
align-items:center;
flex-wrap:wrap;
gap:10px;
margin-bottom:8px;
}

.ventas-hoy{
font-weight:bold;
color:#16a34a;
}

/* =====================================================
   TABLA RESPONSIVE
===================================================== */

table{
width:100%;
border-collapse:collapse;
display:block;
overflow-x:auto;
}

/* =====================================================
   COLUMNAS TABLA (POS MAS GRANDE)
===================================================== */

#tabla td:nth-child(1){min-width:140px;} /* Loteria */
#tabla td:nth-child(2){min-width:120px;} /* Sorteo */
#tabla td:nth-child(3){min-width:150px;} /* Numero */
#tabla td:nth-child(4){min-width:130px;} /* Jugada */
#tabla td:nth-child(5){min-width:120px;} /* Monto */
#tabla td:nth-child(6){min-width:60px;}  /* X */

/* inputs grandes tipo POS */
#tabla select,
#tabla input{
width:100%;
padding:14px;
font-size:16px;
border-radius:8px;
}

/* =====================================================
   BOTON ELIMINAR
===================================================== */

.btn-del{
background:#CE1126;
color:white;
border:none;
border-radius:8px;
padding:10px 12px;
cursor:pointer;
}

/* =====================================================
   NAVBAR SUPERIOR
===================================================== */

.navbar{
background:#002D62;
color:white;
padding:12px;
text-align:center;
font-weight:900;
}

/* =====================================================
   MOBILE
===================================================== */

@media (max-width:768px){

.card{
max-width:95%;
padding:18px;
}

#tabla select,
#tabla input{
font-size:15px;
padding:12px;
}

}

</style>


<div class="escudo-3d">
<img src="https://flagcdn.com/w320/do.png" style="width:110px">
</div>

<div class="card">
<div class="venta-header">
<h2>💰 Venta</h2>
<span class="ventas-hoy">Total vendido hoy: RD$ {{ "{:,.2f}".format(ventas_hoy) }}</span>
</div>
{% for cat, msg in get_flashed_messages(with_categories=true) %}
<div class="flash flash-{{ cat }}" style="background:#d1fae5;color:#065f46;padding:12px;border-radius:8px;margin-bottom:12px;font-weight:bold">
{{ msg }}
</div>
{% endfor %}

{% if repetir_data %}
<div style="background:#d1fae5;color:#065f46;padding:12px;border-radius:8px;margin-bottom:12px;font-weight:bold">
🔄 Jugada repetida - Puede cambiar la lotería antes de vender
</div>
{% endif %}
<form method="post">
<table id="tabla">
<tr>
<th>Lotería</th><th>Sorteo</th><th>Número</th><th>Jugada</th><th>Monto</th><th>X</th>
</tr>
<tbody></tbody>
</table>

<div style="margin-top:15px;font-size:20px;font-weight:900;text-align:right">
TOTAL: $ <span id="total">0.00</span>
</div>

<button type="button" onclick="agregar()">➕ Agregar</button>
<button type="submit">🎟️ Vender</button>

<a href="/calculadora_premios">
<button type="button">💰 Calcular Premio</button>
</a>

<button type="button" onclick="limpiarTodo()" style="
background:linear-gradient(135deg,#CE1126,#8f0c1b);
margin-top:8px;">
🧹 Limpiar Jugadas
</button>

<!-- =============================== -->
<!-- BOTON HISTORIAL JUGADAS -->
<!-- =============================== -->

<a href="/admin/jugadas" style="
display:block;
margin-top:10px;
background:#7c3aed;
color:white;
padding:14px;
border-radius:12px;
font-weight:bold;
text-decoration:none;
text-align:center;
box-shadow:0 6px 15px rgba(0,0,0,.2);
">
📜 Historial Jugadas (Editar / Cancelar Ticket)
</a>

<div style="background:#f0f9ff;padding:12px;border-radius:8px;margin-top:12px;margin-bottom:12px;border:1px solid #bae6fd">
<form method="get" action="/admin/buscar_ticket" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
<label style="font-weight:bold">Buscar Ticket</label>
<input type="number" name="ticket_id" placeholder="Nº ticket" min="1" style="width:100px;padding:8px;border-radius:6px;border:1px solid #94a3b8">
<button type="submit" style="background:#002D62;color:white;padding:8px 16px;border:none;border-radius:8px;font-weight:bold;cursor:pointer">🔎 Buscar Ticket</button>
</form>
</div>

<hr style="margin-top:20px">

<div id="preview" style="
background:white;padding:14px;border-radius:14px;margin-top:14px;
box-shadow:0 6px 20px rgba(0,0,0,.08);border-left:5px solid #CE1126;">
<div style="font-weight:900;color:#002D62;margin-bottom:10px;font-size:15px">
🧾 Ticket en vivo
</div>
<div id="lista">No hay jugadas</div>
</div>
</form>
</div>

<script>
const data={{lotteries|tojson}};
const tabla=document.querySelector("#tabla tbody");

function actualizar(){
let total = 0;
let html = "";
document.querySelectorAll("#tabla tbody tr").forEach(r=>{
var selLottery = r.querySelector("[name='loteria[]']");
var selDraw = r.querySelector("[name='sorteo[]']");
var inpNumber = r.querySelector("[name='numero[]']");
var selPlay = r.querySelector("[name='jugada[]']");
var inpAmount = r.querySelector("[name='monto[]']");
var l = selLottery ? selLottery.value : "";
var d = selDraw ? selDraw.value : "";
var n = inpNumber ? (inpNumber.value || "").trim() : "";
var p = selPlay ? selPlay.value : "";
var a = parseFloat(inpAmount ? inpAmount.value : "") || 0;
if(!isNaN(a) && a > 0){
    total += a;
    html += (l + " " + d + " • " + p + " • " + n + " → $" + a.toFixed(2) + "<br>");
}
});
var totalEl = document.getElementById("total");
if(totalEl) totalEl.innerText = total.toFixed(2);
var listaEl = document.getElementById("lista");
if(listaEl) listaEl.innerHTML = html || "No hay jugadas";
}


window.agregar=function(){
let r=tabla.insertRow();
let lotes=[...new Set(data.map(x=>x.lottery))];

r.innerHTML=`
<td>
<select name="loteria[]" onchange="filtrar(this)">
${lotes.map(l=>`<option>${l}</option>`).join("")}
</select>

<select name="loteria2[]" style="display:none;margin-top:4px">
<option value="">---</option>
${lotes.map(l=>`<option>${l}</option>`).join("")}
</select>
</td>

<td><select name="sorteo[]"></select></td>
<td>
<input name="numero[]"
inputmode="numeric"
maxlength="6"
placeholder="12 / 1256 / 12-34">
</td>

<td>
<select name="jugada[]" onchange="validarTipo(this)">
<option>Quiniela</option>
<option>Pale</option>
<option>Tripleta</option>
<option>Super Pale</option>
</select>
</td>

<td><input name="monto[]" type="number" step="0.01"></td>

<td>
<button type="button" class="btn-del"
onclick="this.closest('tr').remove();actualizar()">✕</button>
</td>
`;

filtrar(r.querySelector("[name='loteria[]']"));
r.querySelectorAll("input,select").forEach(e=>{
e.addEventListener("input",actualizar);
e.addEventListener("change",actualizar);
});
actualizar();
}

window.filtrar=function(sel){
let fila=sel.closest("tr");
let draw=fila.querySelector("[name='sorteo[]']");
let horas=[...new Set(data.filter(x=>x.lottery==sel.value).map(x=>x.draw))];
draw.innerHTML=horas.map(h=>`<option>${h}</option>`).join("");
}

window.validarTipo=function(sel){
let fila=sel.closest("tr");
let lot2=fila.querySelector("[name='loteria2[]']");
let input=fila.querySelector("[name='numero[]']");
if(sel.value=="Super Pale"){lot2.style.display="block";input.placeholder="12-34";}
else{lot2.style.display="none";lot2.value="";}
}

// LIMPIAR TODAS LAS JUGADAS
window.limpiarTodo=function(){

if(!confirm("¿Borrar todas las jugadas?")) return;

document.querySelector("#tabla tbody").innerHTML="";
document.getElementById("total").innerText="0.00";
document.getElementById("lista").innerHTML="No hay jugadas";

}

// CARGAR REPETIR TICKET (si hay datos prellenados)
const repetirData = {{ repetir_data|tojson }};
if(repetirData && repetirData.length > 0){
document.addEventListener("DOMContentLoaded", function(){
 repetirData.forEach(function(item){
  agregar();
  var filas = document.querySelectorAll("#tabla tbody tr");
  var last = filas[filas.length - 1];
  if(!last) return;
  var lotSel = last.querySelector("[name='loteria[]']");
  var drawSel = last.querySelector("[name='sorteo[]']");
  var numInp = last.querySelector("[name='numero[]']");
  var playSel = last.querySelector("[name='jugada[]']");
  var amtInp = last.querySelector("[name='monto[]']");
  var lot2Sel = last.querySelector("[name='loteria2[]']");
  if(lotSel){
   lotSel.value = item.lottery;
   filtrar(lotSel);
  }
  setTimeout(function(){
   if(drawSel && item.draw) drawSel.value = item.draw;
   if(numInp) numInp.value = item.number || "";
   if(playSel){ playSel.value = item.play || "Quiniela"; validarTipo(playSel); }
   if(amtInp) amtInp.value = item.amount || "";
   if(lot2Sel && item.lottery2) lot2Sel.value = item.lottery2;
   actualizar();
  }, 80);
 });
});
}
</script>

""", lotteries=lotteries, repetir_data=repetir_data, ventas_hoy=ventas_hoy)


# ===============================
# ELIMINAR USUARIO (SOLO ADMIN)
# No se puede eliminar el único admin. Si hay 2 o más admins, sí se puede.
# ===============================
@app.route("/eliminar_usuario/<int:user_id>", methods=["POST"])
def eliminar_usuario(user_id):
    if not is_admin_or_super():
        return redirect("/")

    usuario_actual = session.get("u")
    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute(
            _sql("SELECT id, username, role FROM users WHERE id=%s"),
            (user_id,)
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return redirect("/usuarios")

        user = dict(row) if hasattr(row, "keys") else {"id": row[0], "username": row[1], "role": row[2] if len(row) > 2 else ""}
        username = user.get("username", "")
        role = user.get("role", "")

        if username == usuario_actual:
            flash("No puede eliminarse a sí mismo.")
            return redirect("/usuarios")

        if role in (ROLE_ADMIN, ROLE_SUPER_ADMIN):
            cur.execute(_sql("SELECT COUNT(*) FROM users WHERE role IN (%s, %s)"), (ROLE_ADMIN, ROLE_SUPER_ADMIN))
            r = cur.fetchone()
            num_admins = list(r.values())[0] if r and hasattr(r, "values") else (r[0] if r else 0)
            if num_admins <= 1:
                flash("No se puede eliminar el único administrador. Cree otro admin antes.")
                return redirect("/usuarios")

        cur.execute(_sql("DELETE FROM users WHERE id=%s"), (user_id,))
        conn.commit()
    finally:
        conn.close()

    return redirect("/usuarios")


@app.route("/actualizar_meta_usuario/<int:user_id>", methods=["POST"])
def actualizar_meta_usuario(user_id):
    if not is_admin_or_super():
        return redirect("/")

    meta_val = request.form.get("meta", "").strip()
    try:
        meta = float(meta_val) if meta_val else 0.0
    except ValueError:
        meta = 0.0

    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute(
            _sql("UPDATE users SET meta=%s WHERE id=%s"),
            (meta, user_id)
        )
        conn.commit()
    finally:
        conn.close()

    return redirect("/usuarios")


# =====================================================
# ASIGNAR METAS CAJEROS (solo admin)
# =====================================================
@app.route("/admin/metas", methods=["GET", "POST"])
def admin_metas():
    if not is_admin_or_super():
        return redirect("/")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        meta_val = (request.form.get("meta") or "").strip()
        try:
            meta = float(meta_val) if meta_val else 0.0
        except ValueError:
            meta = 0.0
        if username:
            conn = db()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute(_sql("UPDATE users SET meta=%s WHERE username=%s"), (meta, username))
                    conn.commit()
                finally:
                    conn.close()
        return redirect("/admin/metas")

    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    try:
        cur = conn.cursor()
        cur.execute(_sql("""
            SELECT username, COALESCE(meta, 0) AS meta
            FROM users
            WHERE role IN (%s, %s, %s)
            ORDER BY username
        """), (ROLE_CAJERO, ROLE_COLLECTOR, ROLE_SUPERVISOR))
        rows = cur.fetchall()
        cajeros = []
        for r in rows:
            d = dict(r) if hasattr(r, "keys") else {}
            name = d.get("username") or (r[0] if r else "")
            meta = float(d.get("meta") or 0)
            cajeros.append({"username": name, "meta": meta})
    finally:
        conn.close()

    return render_template_string(IOS + ADMIN_MENU + """
<div class="card" style="max-width:720px;margin:20px auto">
    <h2>🎯 Asignar Metas Cajeros</h2>
    <p style="margin-bottom:16px;color:#555">Asigna la meta diaria (RD$) por cajero. La cintilla superior mostrará ventas y meta de cada uno.</p>
    <table width="100%" style="border-collapse:collapse">
        <tr style="background:#002D62;color:white">
            <th style="padding:10px;text-align:left">Cajero</th>
            <th style="padding:10px;text-align:right">Meta actual</th>
            <th style="padding:10px">Nueva meta</th>
            <th style="padding:10px">Guardar</th>
        </tr>
        {% for c in cajeros %}
        <tr style="border-bottom:1px solid #e2e8f0">
            <td style="padding:10px">{{ c.username }}</td>
            <td style="padding:10px;text-align:right">RD$ {{ "%.0f"|format(c.meta) }}</td>
            <td style="padding:10px"><input form="form_meta_{{ loop.index }}" name="meta" type="number" min="0" step="100" value="{{ "%.0f"|format(c.meta) }}" style="width:120px;padding:8px"></td>
            <td style="padding:10px">
                <form id="form_meta_{{ loop.index }}" method="post" style="display:inline">
                    <input type="hidden" name="username" value="{{ c.username }}">
                    <button type="submit" style="width:auto;padding:8px 16px">Guardar</button>
                </form>
            </td>
        </tr>
        {% endfor %}
    </table>
    {% if not cajeros %}
    <p style="color:#666">No hay cajeros. Crea usuarios con rol &quot;Cajero&quot; en Crear Usuario.</p>
    {% endif %}
    <br>
    <a href="/admin">⬅ Volver al Dashboard</a>
</div>
""", cajeros=cajeros)


# =====================================================
# ANULAR TICKET ACTUAL (ADMIN)
# =====================================================
@app.route("/anular_ticket_actual")
def anular_ticket_actual():
    return redirect("/venta")



# ===============================
# CERRAR VENTA / VER TICKET
# ===============================
@app.route("/vender")
def vender():
    ticket_id = session.pop("ticket_id", None)

    if not ticket_id:
        return redirect("/venta")

    return redirect(f"/ticket/{ticket_id}")


# ===============================
# BUSCAR TICKET (cargar jugadas en venta)
# ===============================
@app.route("/admin/buscar_ticket", methods=["GET"])
def buscar_ticket():

    if not is_staff():
        return redirect("/")

    ticket_id = request.args.get("ticket_id", "").strip()
    if not ticket_id:
        session["repetir_data"] = []
        return redirect("/venta")

    try:
        ticket_id = int(ticket_id)
    except ValueError:
        session["repetir_data"] = []
        return redirect("/venta")

    c = db()
    if not c:
        session["repetir_data"] = []
        return redirect("/venta")
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            SELECT lottery, draw, lottery2, number, play, amount
            FROM ticket_lines
            WHERE ticket_id=%s AND COALESCE(estado,'activo')!='cancelado'
            ORDER BY id
        """), (ticket_id,))
        rows = cur.fetchall()
    finally:
        c.close()

    repetir_data = []
    for r in rows:
        try:
            d = dict(r)
        except Exception:
            continue
        repetir_data.append({
            "lottery": d.get("lottery", ""),
            "draw": d.get("draw", ""),
            "lottery2": d.get("lottery2") or "",
            "number": str(d.get("number", "")),
            "play": d.get("play", "Quiniela"),
            "amount": float(d.get("amount", 0) or 0)
        })

    session["repetir_data"] = repetir_data
    return redirect("/venta")


# ===============================
# REPETIR TICKET (misma jugada)
# ===============================
@app.route("/admin/repetir_ticket/<int:ticket_id>")
def repetir_ticket(ticket_id):
    if not is_staff():
        return redirect("/")

    c = db()
    if not c:
        return redirect("/venta")
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            SELECT lottery, draw, lottery2, number, play, amount
            FROM ticket_lines
            WHERE ticket_id=%s AND COALESCE(estado,'activo')!='cancelado'
            ORDER BY id
        """), (ticket_id,))
        rows = cur.fetchall()
    finally:
        c.close()

    if not rows:
        return redirect("/venta")

    repetir_data = []
    for r in rows:
        try:
            d = dict(r)
        except:
            continue
        if d:
            repetir_data.append({
                "lottery": d.get("lottery", ""),
                "draw": d.get("draw", ""),
                "lottery2": d.get("lottery2") or "",
                "number": str(d.get("number", "")),
                "play": d.get("play", "Quiniela"),
                "amount": float(d.get("amount", 0) or 0)
            })

    session["repetir_data"] = repetir_data
    return redirect("/venta")

# ===============================
# TURNO POR HORA (Día / Tarde / Noche)
# ===============================
def turno_loteria(draw):

    if not draw:
        return ""

    try:
        hora = int(draw[:2])
    except:
        return ""

    if hora < 12:
        return "Día"
    elif hora < 18:
        return "Tarde"
    else:
        return "Noche"


# ===============================
# TICKET BANCA REAL RD (CON HORA SORTEO)
# ===============================
@app.route("/ticket/<int:id>")
def ticket_detail(id):

    conn = db()
    if not conn:
        return "Error conectando base de datos", 500

    try:
        cur = conn.cursor()

        cur.execute(_sql("SELECT * FROM tickets WHERE id = %s"), (id,))
        ticket = cur.fetchone()

        if not ticket:
            return "Ticket no encontrado", 404

        ticket = dict(ticket) if hasattr(ticket, "keys") else {
            "id": ticket[0],
            "cajero": ticket[1],
            "created_at": ticket[2]
        }
        if session.get("role") in (ROLE_CAJERO, ROLE_USER) and (ticket.get("cajero") or "") != (session.get("u") or ""):
            return "No autorizado para ver este ticket", 403

        created = ticket.get("created_at")
        if created is None:
            hora_rd = ""
            fecha_display = ""
        else:
            cs = str(created)
            fecha_display = cs[:10] if len(cs) >= 10 else ""
            try:
                dt = datetime.fromisoformat(cs.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=TZ_RD)
                else:
                    dt = dt.astimezone(TZ_RD)
                hora_rd = dt.strftime("%I:%M %p")
            except Exception:
                hora_rd = cs[11:16] if len(cs) >= 16 else ""

        # Todas las jugadas del ticket (sin filtrar por sorteo ni fecha)
        cur.execute(_sql("""
            SELECT number, play, amount
            FROM ticket_lines
            WHERE ticket_id = %s
            ORDER BY id ASC
        """), (id,))
        rows = cur.fetchall()

        lines = []
        for r in rows:
            if hasattr(r, "keys"):
                d = dict(zip(r.keys(), r)) if not hasattr(r, "get") else r
                number = (d.get("number") or "") or ""
                play = (d.get("play") or "") or ""
                amount = float(d.get("amount") or 0)
            else:
                number = r[0] if len(r) > 0 else ""
                play = r[1] if len(r) > 1 else ""
                amount = float(r[2] if len(r) > 2 else 0)
            lines.append({"number": number, "bet_type": play, "amount": amount})

        # Total desde la base de datos
        cur.execute(_sql("""
            SELECT COALESCE(SUM(amount), 0)
            FROM ticket_lines
            WHERE ticket_id = %s
        """), (id,))
        row_total = cur.fetchone()
        total = float((list(row_total.values())[0] if row_total and hasattr(row_total, "values") else (row_total[0] if row_total else 0)) or 0)

    finally:
        conn.close()

    html = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width">

<style>
*{margin:0;padding:0;box-sizing:border-box}

body{
background:white;
font-family:monospace;
}

.ticket{
width:58mm;
padding:4px;
font-size:12px;
line-height:1.2;
text-align:center;
}

.titulo-banca{
font-weight:900;
margin:6px 0 4px 0;
position:relative;
}

.titulo-banca:before,
.titulo-banca:after{
content:"";
position:absolute;
top:50%;
width:25%;
border-top:1px dashed black;
}

.titulo-banca:before{left:0}
.titulo-banca:after{right:0}

.jugada{margin:4px 0 8px 0}

.lot{font-size:12px;font-weight:700}
.num{font-size:16px;font-weight:900}
.val{font-size:12px}

.line{
border-top:1px dashed #000;
margin:5px 0;
}

.total{
font-size:16px;
font-weight:bold;
margin:6px 0;
}

@media print{

body *{visibility:hidden}

.ticket,.ticket *{visibility:visible}

.ticket{
position:absolute;
left:0;
top:0;
width:58mm;
margin:0;
}

html,body{
width:58mm;
margin:0;
padding:0;
}

@page{size:58mm auto;margin:0}

button{display:none}

}
</style>
</head>

<body>

<div class="ticket">

<div><b>LA QUE NUNCA FALLA</b></div>
<div>NO PAGAMOS SIN TICKET</div>

<div class="line"></div>

Fecha: {{ fecha_display }} &nbsp;&nbsp; Hora Venta: {{ hora_rd }}<br>
Ticket: {{ ticket.get("id","") }} &nbsp;&nbsp; Serial: {{ ticket.get("id","") }}<br>
Cajero: {{ ticket.get("cajero","") }}

<div class="line"></div>

{% for line in lines %}
<div class="jugada">
<span class="num">{{ line.number }}</span>
<span class="val">{{ line.bet_type }}</span>
<span class="val">RD$ {{ "%.2f"|format(line.amount) }}</span>
</div>
{% endfor %}

<div class="line"></div>
<div class="total">
MONTO TOTAL: RD$ {{ "%.2f"|format(total) }}
</div>

<div class="line"></div>

<div>
REVISE SU TICKET AL RECIBIRLO<br>
BUENA SUERTE
</div>

<div style="margin-top:8px">
<img src="https://api.qrserver.com/v1/create-qr-code/?size=120x120&data={{ ticket["id"] }}">
</div>

<div style="font-size:9px;margin-top:4px">
ESCANEA PARA VERIFICAR
</div>

<button onclick="window.print()">IMPRIMIR</button>

</div>

</body>
</html>
"""

    return render_template_string(
        html,
        ticket=ticket,
        lines=lines,
        total=total,
        hora_rd=hora_rd,
        fecha_display=fecha_display,
        abr_loteria=abr_loteria,
        turno_loteria=turno_loteria
    )
# =====================================================
# REPORTE (FIX BANCA REAL)
# =====================================================
@app.route("/reporte")
def reporte():
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    usuario = session.get("u") or session.get("user") or ""
    es_admin = is_admin_or_super()
    try:
        cur = c.cursor()
        hoy = ahora_rd().strftime("%Y-%m-%d")

        if es_admin:
            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT lottery, draw, SUM(amount) AS total
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
                    GROUP BY lottery, draw
                """, (hoy,))
            else:
                cur.execute(_sql("""
                    SELECT lottery, draw, SUM(amount) AS total
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at)=%s
                    AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
                    GROUP BY lottery, draw
                """), (hoy,))
        else:
            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT lottery, draw, SUM(amount) AS total
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
                    AND tickets.cajero = %s
                    GROUP BY lottery, draw
                """, (hoy, usuario))
            else:
                cur.execute(_sql("""
                    SELECT lottery, draw, SUM(amount) AS total
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at)=%s
                    AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
                    AND tickets.cajero = %s
                    GROUP BY lottery, draw
                """), (hoy, usuario))
        data = cur.fetchall()

        if es_admin:
            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT COALESCE(SUM(ticket_lines.amount),0)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                """, (hoy,))
            else:
                cur.execute(_sql("""
                    SELECT COALESCE(SUM(ticket_lines.amount),0)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at)=%s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                """), (hoy,))
        else:
            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT COALESCE(SUM(ticket_lines.amount),0)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                """, (hoy, usuario))
            else:
                cur.execute(_sql("""
                    SELECT COALESCE(SUM(ticket_lines.amount),0)
                    FROM ticket_lines
                    JOIN tickets ON tickets.id = ticket_lines.ticket_id
                    WHERE DATE(tickets.created_at)=%s
                    AND COALESCE(ticket_lines.estado,'activo') != 'cancelado'
                    AND tickets.cajero = %s
                """), (hoy, usuario))
        row = cur.fetchone()
        total = list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)

        # Ventas por Cajero: solo para admin/super_admin (solo cajeros con role = 'user'), hoy
        ventas_cajero_list = []
        if es_admin:
            if os.environ.get("DATABASE_URL"):
                cur.execute("""
                    SELECT t.cajero,
                           COUNT(DISTINCT t.id) AS tickets,
                           COALESCE(SUM(tl.amount), 0) AS total_vendido
                    FROM tickets t
                    JOIN ticket_lines tl ON tl.ticket_id = t.id
                    JOIN users u ON u.username = t.cajero AND u.role = 'user'
                    WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(tl.estado, 'activo') != 'cancelado'
                    GROUP BY t.cajero
                    ORDER BY total_vendido DESC
                """, (hoy,))
            else:
                cur.execute(_sql("""
                    SELECT t.cajero,
                           COUNT(DISTINCT t.id) AS tickets,
                           COALESCE(SUM(tl.amount), 0) AS total_vendido
                    FROM tickets t
                    JOIN ticket_lines tl ON tl.ticket_id = t.id
                    JOIN users u ON u.username = t.cajero AND u.role = 'user'
                    WHERE DATE(t.created_at) = %s
                    AND COALESCE(tl.estado, 'activo') != 'cancelado'
                    GROUP BY t.cajero
                    ORDER BY total_vendido DESC
                """), (hoy,))
            ventas_cajero_rows = cur.fetchall() or []
            for r in ventas_cajero_rows:
                if hasattr(r, "keys"):
                    d = dict(zip(r.keys(), r)) if not hasattr(r, "get") else r
                    ventas_cajero_list.append({
                        "cajero": (d.get("cajero") or "") or "",
                        "tickets": int(d.get("tickets") or 0),
                        "total_vendido": float(d.get("total_vendido") or 0),
                    })
                else:
                    ventas_cajero_list.append({
                        "cajero": (r[0] if len(r) > 0 else "") or "",
                        "tickets": int(r[1] if len(r) > 1 else 0),
                        "total_vendido": float((r[2] if len(r) > 2 else 0) or 0),
                    })
    finally:
        c.close()

    return render_template_string(
        IOS + ADMIN_MENU + """
<style>
.reporte-grid { display:flex; flex-wrap:wrap; gap:20px; align-items:flex-start; margin-bottom:20px; }
.reporte-grid .card { flex:1; min-width:280px; max-width:100%; }
</style>
<div class="reporte-grid">
<div class="card">
    <h2>📈 Reporte Diario</h2>

    <div class="success" style="text-align:center;font-size:20px;margin-bottom:15px">
        Total del día: ${{ "%.2f"|format(total) }}
    </div>

    <table width="100%">
        <tr>
            <th>Lotería</th>
            <th>Sorteo</th>
            <th>Total</th>
        </tr>

        {% for r in data %}
        <tr>
            <td>{{ r.lottery }}</td>
            <td>{{ r.draw }}</td>
            <td class="success">${{ "%.2f"|format(r.total) }}</td>
        </tr>
        {% endfor %}

    </table>
</div>

{% if es_admin %}
<div class="card">
    <h2>📊 Ventas por Cajero</h2>
    <p style="margin:0 0 12px 0;color:#64748b;font-size:14px;">Hoy (solo cajeros)</p>
    <table width="100%" style="border-collapse:collapse">
        <thead>
            <tr style="background:#f1f5f9">
                <th style="padding:8px;text-align:left;border:1px solid #e2e8f0">Cajero</th>
                <th style="padding:8px;text-align:center;border:1px solid #e2e8f0">Tickets</th>
                <th style="padding:8px;text-align:right;border:1px solid #e2e8f0">Total Vendido</th>
            </tr>
        </thead>
        <tbody>
        {% for c in ventas_cajero_list %}
        <tr>
            <td style="padding:8px;border:1px solid #e2e8f0">{{ c.cajero or '—' }}</td>
            <td style="padding:8px;text-align:center;border:1px solid #e2e8f0">{{ c.tickets }}</td>
            <td style="padding:8px;text-align:right;border:1px solid #e2e8f0;font-weight:600">${{ "%.2f"|format(c.total_vendido) }}</td>
        </tr>
        {% endfor %}
        {% if not ventas_cajero_list %}
        <tr><td colspan="3" style="padding:12px;color:#64748b;text-align:center">No hay ventas por cajero hoy.</td></tr>
        {% endif %}
        </tbody>
    </table>
</div>
{% endif %}
</div>
""",
        data=data,
        total=total,
        ventas_cajero_list=ventas_cajero_list,
        es_admin=es_admin
    )


# ===============================
# API: resultados de hoy (para alerta sonora en /ganadores)
# ===============================
@app.route("/api/resultados_hoy")
def api_resultados_hoy():
    """Devuelve JSON con resultados del día actual (RD) para detectar cambios y reproducir sonido."""
    hoy = ahora_rd().date().strftime("%Y-%m-%d")
    c = db()
    if not c:
        return jsonify([])
    try:
        cur = c.cursor()
        if os.environ.get("DATABASE_URL"):
            cur.execute(
                """
                SELECT lottery, draw, primero, segundo, tercero
                FROM resultados
                WHERE (fecha::date) = (NOW() AT TIME ZONE 'America/Santo_Domingo')::date
                ORDER BY lottery, draw
                """
            )
        else:
            cur.execute(
                _sql("SELECT lottery, draw, primero, segundo, tercero FROM resultados WHERE fecha = %s ORDER BY lottery, draw"),
                (hoy,),
            )
        rows = cur.fetchall() or []
        out = []
        for r in rows:
            if hasattr(r, "keys"):
                lot, dr, p1, p2, p3 = r.get("lottery"), r.get("draw"), r.get("primero"), r.get("segundo"), r.get("tercero")
            else:
                lot, dr, p1, p2, p3 = (r[0] if len(r) > 0 else None), (r[1] if len(r) > 1 else None), (r[2] if len(r) > 2 else None), (r[3] if len(r) > 3 else None), (r[4] if len(r) > 4 else None)
            out.append({
                "lottery": lot,
                "draw": dr,
                "primero": p1,
                "segundo": p2,
                "tercero": p3,
                "n1": p1,
                "n2": p2,
                "n3": p3,
            })
        return jsonify(out)
    finally:
        c.close()


# ===============================
# GANADORES / RIESGO (VISIBLE PARA TODOS)
# ===============================
@app.route("/ganadores")
def ganadores_riesgo():

    if session.get("u") is None and session.get("user") is None:
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500

    hoy = ahora_rd().date().strftime("%Y-%m-%d")
    total_ganadores = 0
    total_pagar = 0.0
    lista_ganadores = []
    numeros_riesgo = []
    ventas_del_dia = 0.0
    premios_pagados_del_dia = 0.0
    ganancia_dia = 0.0
    resultados_hoy = []
    resultados_fecha = hoy

    try:
        cur = c.cursor()

        # 0) Resultados de hoy (o última fecha con datos si hoy está vacío)
        cur.execute(
            _sql(
                """
                SELECT lottery, draw, primero, segundo, tercero, fecha
                FROM resultados
                WHERE fecha = %s
                ORDER BY lottery, draw
                """
            ),
            (hoy,),
        )
        res_rows = cur.fetchall() or []
        resultados_hoy = []
        resultados_fecha = hoy
        for r in res_rows:
            if hasattr(r, "keys"):
                d = dict(r)
                d["fecha"] = d.get("fecha") or hoy
                d["lottery_display"] = (d.get("lottery") or "") + ((" " + (d.get("draw") or "").strip()) if d.get("draw") else "")
            else:
                lot = r[0] if len(r) > 0 else ""
                draw = r[1] if len(r) > 1 else ""
                d = {
                    "lottery": lot,
                    "draw": draw,
                    "primero": r[2] if len(r) > 2 else "",
                    "segundo": r[3] if len(r) > 3 else "",
                    "tercero": r[4] if len(r) > 4 else "",
                    "fecha": r[5] if len(r) > 5 else hoy,
                }
                d["lottery_display"] = lot + (" " + str(draw).strip() if draw else "")
            resultados_hoy.append(d)
        # Si no hay resultados de hoy, mostrar los de la última fecha disponible
        if not resultados_hoy:
            cur.execute(_sql("SELECT MAX(fecha) AS ultima FROM resultados"))
            row = cur.fetchone()
            ultima = row["ultima"] if hasattr(row, "keys") and row else (row[0] if row else None)
            if ultima:
                cur.execute(
                    _sql(
                        """
                        SELECT lottery, draw, primero, segundo, tercero, fecha
                        FROM resultados
                        WHERE fecha = %s
                        ORDER BY lottery, draw
                        """
                    ),
                    (ultima,),
                )
                res_rows = cur.fetchall() or []
                resultados_hoy = []
                for r in res_rows:
                    if hasattr(r, "keys"):
                        d = dict(r)
                        d["fecha"] = d.get("fecha") or ultima
                        d["lottery_display"] = (d.get("lottery") or "") + ((" " + (d.get("draw") or "").strip()) if d.get("draw") else "")
                    else:
                        lot = r[0] if len(r) > 0 else ""
                        draw = r[1] if len(r) > 1 else ""
                        d = {
                            "lottery": lot,
                            "draw": draw,
                            "primero": r[2] if len(r) > 2 else "",
                            "segundo": r[3] if len(r) > 3 else "",
                            "tercero": r[4] if len(r) > 4 else "",
                            "fecha": r[5] if len(r) > 5 else ultima,
                        }
                        d["lottery_display"] = lot + (" " + str(draw).strip() if draw else "")
                    resultados_hoy.append(d)
                resultados_fecha = ultima

        # 1) Ganadores: solo tickets de hoy + solo resultados de hoy (lottery, draw y fecha)
        if os.environ.get("DATABASE_URL"):
            cur.execute(
                """
                SELECT
                    tl.ticket_id,
                    tk.cajero,
                    tl.number,
                    tl.lottery,
                    tl.draw,
                    tl.play,
                    tl.amount,
                    r.primero,
                    r.segundo,
                    r.tercero
                FROM ticket_lines tl
                JOIN tickets tk ON tk.id = tl.ticket_id
                JOIN resultados r
                    ON tl.lottery = r.lottery
                    AND COALESCE(r.draw, '') = COALESCE(tl.draw, '')
                    AND (r.fecha::date) = (NOW() AT TIME ZONE 'America/Santo_Domingo')::date
                WHERE (tk.created_at AT TIME ZONE 'America/Santo_Domingo')::date = (NOW() AT TIME ZONE 'America/Santo_Domingo')::date
                AND COALESCE(tl.estado,'activo') != 'cancelado'
                AND (tl.number = r.primero OR tl.number = r.segundo OR tl.number = r.tercero)
                ORDER BY tl.ticket_id, tl.lottery, tl.draw
                """
            )
        else:
            cur.execute(
                _sql(
                    """
                    SELECT
                        tl.ticket_id,
                        tk.cajero,
                        tl.number,
                        tl.lottery,
                        tl.draw,
                        tl.play,
                        tl.amount,
                        r.primero,
                        r.segundo,
                        r.tercero
                    FROM ticket_lines tl
                    JOIN tickets tk ON tk.id = tl.ticket_id
                    JOIN resultados r
                        ON tl.lottery = r.lottery
                        AND COALESCE(r.draw, '') = COALESCE(tl.draw, '')
                        AND r.fecha = %s
                    WHERE DATE(tk.created_at) = %s
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    AND (tl.number = r.primero OR tl.number = r.segundo OR tl.number = r.tercero)
                    ORDER BY tl.ticket_id, tl.lottery, tl.draw
                    """
                ),
                (hoy, hoy),
            )
        filas_ganadoras = cur.fetchall() or []
        tickets_ganadores = set()
        for row in filas_ganadoras:
            tid = row["ticket_id"] if hasattr(row, "keys") else row[0]
            cajero = row["cajero"] if hasattr(row, "keys") else row[1]
            numero = row["number"] if hasattr(row, "keys") else row[2]
            lottery = row["lottery"] if hasattr(row, "keys") else row[3]
            draw = row["draw"] if hasattr(row, "keys") else row[4]
            play = row["play"] if hasattr(row, "keys") else row[5]
            monto = row["amount"] if hasattr(row, "keys") else row[6]
            r1 = row["primero"] if hasattr(row, "keys") else row[7]
            r2 = row["segundo"] if hasattr(row, "keys") else row[8]
            r3 = row["tercero"] if hasattr(row, "keys") else row[9]
            premio = calcular_premio(play, numero, monto, r1, r2, r3)
            if premio > 0:
                tickets_ganadores.add(tid)
                total_pagar += premio
                lista_ganadores.append({
                    "ticket_id": tid,
                    "number": numero,
                    "lottery": lottery,
                    "draw": draw or "",
                    "play": play,
                    "premio": premio,
                    "cajero": cajero or "",
                })
        total_ganadores = len(tickets_ganadores)

        # 3) Riesgo por número + lotería + sorteo (toda la banca)
        numeros_riesgo = calcular_riesgo_por_numero()

        # 4) Estado de pago por ticket (para botón Pagar / ✔ Pagado)
        try:
            ticket_ids = list({g["ticket_id"] for g in lista_ganadores})
            ticket_pagado = {}
            if ticket_ids:
                ph = ",".join([_ph()] * len(ticket_ids))
                cur.execute(
                    _sql("SELECT id, COALESCE(pagado, false) AS pagado FROM tickets WHERE id IN (" + ph + ")"),
                    tuple(ticket_ids),
                )
                for row in cur.fetchall() or []:
                    tid = row["id"] if hasattr(row, "keys") else row[0]
                    ticket_pagado[tid] = row["pagado"] if hasattr(row, "keys") else row[1]
            for g in lista_ganadores:
                g["pagado"] = ticket_pagado.get(g["ticket_id"], False)
        except Exception:
            for g in lista_ganadores:
                g["pagado"] = False

        # 5) Ventas del día y premios pagados del día → ganancia del día
        try:
            if os.environ.get("DATABASE_URL"):
                cur.execute(
                    """
                    SELECT COALESCE(SUM(tl.amount), 0) AS ventas
                    FROM ticket_lines tl
                    JOIN tickets t ON t.id = tl.ticket_id
                    WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    """,
                    (hoy,),
                )
            else:
                cur.execute(
                    _sql("""
                    SELECT COALESCE(SUM(tl.amount), 0) AS ventas
                    FROM ticket_lines tl
                    JOIN tickets t ON t.id = tl.ticket_id
                    WHERE DATE(t.created_at) = %s
                    AND COALESCE(tl.estado,'activo') != 'cancelado'
                    """),
                    (hoy,),
                )
            row_v = cur.fetchone()
            ventas_del_dia = float(row_v["ventas"] if hasattr(row_v, "keys") and row_v else (row_v[0] if row_v else 0))
            if os.environ.get("DATABASE_URL"):
                cur.execute(
                    """
                    SELECT COALESCE(SUM(monto), 0) AS premios
                    FROM pagos_premios
                    WHERE (fecha AT TIME ZONE 'America/Santo_Domingo')::date = %s
                    """,
                    (hoy,),
                )
            else:
                cur.execute(
                    _sql("""
                    SELECT COALESCE(SUM(monto), 0) AS premios
                    FROM pagos_premios
                    WHERE DATE(fecha) = %s
                    """),
                    (hoy,),
                )
            row_p = cur.fetchone()
            premios_pagados_del_dia = float(row_p["premios"] if hasattr(row_p, "keys") and row_p else (row_p[0] if row_p else 0))
            ganancia_dia = ventas_del_dia - premios_pagados_del_dia
        except Exception:
            ventas_del_dia = 0.0
            premios_pagados_del_dia = 0.0
            ganancia_dia = 0.0

    finally:
        c.close()

    ganancia_dia_fmt = "{:,.2f}".format(ganancia_dia)

    return render_template_string(
        IOS
        + """
    <style>
    .numero{ padding:6px 10px; border-radius:50%%; font-weight:bold; margin:3px; display:inline-block; min-width:40px; text-align:center; white-space:nowrap; }
    .numero.verde{ background:#22c55e; color:white; }
    .numero.gris{ background:#d1d5db; color:#333; }
    .tabla-resultados{ width:100%%; overflow-x:auto; -webkit-overflow-scrolling:touch; }
    .tabla-resultados table{ min-width:420px; width:100%%; border-collapse:collapse; }
    .tabla-resultados.tabla-ganadores-wrap table{ min-width:580px; }
    @media (max-width:600px){
    .numero{ min-width:36px; font-size:14px; }
    }
    </style>
    <audio id="alertaResultado" preload="auto">
        <source src="/static/sounds/resultado.mp3" type="audio/mpeg">
    </audio>
    <div class="card" style="margin-top:70px">
        <h2>🎰 Resultados de Hoy</h2>
        {% if resultados_hoy %}
        <p style="color:#666;margin-bottom:10px">
            {% if resultados_fecha == hoy %}
            Resultados del día ({{ resultados_fecha }})
            {% else %}
            Últimos resultados disponibles ({{ resultados_fecha }})
            {% endif %}
        </p>
        <div class="tabla-resultados">
        <table width="100%%">
            <tr>
                <th>Lotería</th>
                <th colspan="3">1er / 2do / 3er Número</th>
            </tr>
            {% for r in resultados_hoy %}
            <tr>
                <td>{{ r.lottery_display }}</td>
                <td colspan="3" id="{{ r.lottery }}_{{ r.draw or '' }}" data-num="{{ (r.primero or '') }}-{{ (r.segundo or '') }}-{{ (r.tercero or '') }}">
                {% if (r.fecha|string)[:10] == hoy and (r.primero or r.segundo or r.tercero) %}
                <span class="numero verde">{{ r.primero or '--' }}</span>
                <span class="numero verde">{{ r.segundo or '--' }}</span>
                <span class="numero verde">{{ r.tercero or '--' }}</span>
                {% elif r.primero or r.segundo or r.tercero %}
                <span class="numero gris">{{ r.primero or '--' }}</span>
                <span class="numero gris">{{ r.segundo or '--' }}</span>
                <span class="numero gris">{{ r.tercero or '--' }}</span>
                {% else %}
                <span class="numero gris">--</span>
                <span class="numero gris">--</span>
                <span class="numero gris">--</span>
                {% endif %}
                </td>
            </tr>
            {% endfor %}
        </table>
        </div>
        {% else %}
            <p>No hay resultados cargados aún.</p>
            <p style="margin-top:10px;color:#666">El sistema actualiza los resultados cada 5 minutos desde Conectate. Puedes forzar una actualización ahora:</p>
            <a href="/actualizar_resultados" style="display:inline-block;margin-top:8px;padding:10px 16px;border-radius:8px;background:#002D62;color:white;text-decoration:none;font-weight:bold">🔄 Actualizar resultados ahora</a>
        {% endif %}
        {% if resultados_hoy %}
        <p style="margin-top:12px">
            <a href="/actualizar_resultados" style="color:#002D62;text-decoration:none;font-size:14px">🔄 Actualizar resultados ahora</a>
        </p>
        {% endif %}
    </div>

    <div class="card">
        <h2>🏆 Ganadores Hoy</h2>
        <p>Total ganadores: <b>{{ total_ganadores }}</b></p>
        <p>Total dinero a pagar: <b>RD$ {{ "%.2f"|format(total_pagar) }}</b></p>
        <p style="margin-top:12px;font-size:18px;">
            💰 <b>Ganancia del día:</b>
            <span style="color:#16a34a;font-weight:bold;">RD$ {{ ganancia_dia_fmt }}</span>
        </p>
        <a href="#lista" style="
            display:inline-block;
            margin-top:10px;
            padding:10px 16px;
            border-radius:999px;
            background:#002D62;
            color:white;
            text-decoration:none;
            font-weight:bold;
        ">VER GANADORES</a>
    </div>

    <div class="card" id="lista">
        <h2>📋 Lista de Ganadores</h2>
        {% if lista_ganadores and lista_ganadores|length > 0 %}
        <div class="tabla-resultados tabla-ganadores-wrap">
        <table width="100%%" style="border-collapse:collapse" class="tabla-ganadores">
            <thead>
            <tr>
                <th>Ticket</th>
                <th>Número</th>
                <th>Lotería</th>
                <th>Sorteo</th>
                <th>Jugada</th>
                <th>Premio</th>
                <th>Cajero</th>
                <th>Acción</th>
            </tr>
            </thead>
            <tbody>
            {% for g in lista_ganadores %}
            <tr>
                <td>#{{ g.ticket_id }}</td>
                <td>{{ g.number }}</td>
                <td>{{ g.lottery }}</td>
                <td>{{ g.draw }}</td>
                <td>{{ g.play }}</td>
                <td>RD$ {{ "%.2f"|format(g.premio) }}</td>
                <td>{{ g.cajero }}</td>
                <td>
                    {% if g.pagado %}
                    <span style="color:green;font-weight:bold;">✔ Pagado</span>
                    <a href="/imprimir_pago/{{ g.ticket_id }}" target="_blank" style="margin-left:6px;font-size:12px;color:#666">🖨 Recibo</a>
                    {% else %}
                    <form method="POST" action="/pagar_premio/{{ g.ticket_id }}" style="display:inline">
                    <button type="submit" class="btn btn-success" style="padding:6px 12px;border-radius:6px;background:#34C759;color:white;border:none;font-weight:bold;cursor:pointer">💰 Pagar</button>
                    </form>
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
        </div>
        {% else %}
        <div style="text-align:center;padding:30px;font-size:18px;color:#666;">
            🎉 <b>Hoy la banca ganó.</b><br>
            No hay tickets ganadores registrados.
        </div>
        <div style="text-align:center;font-size:18px;margin-top:15px;">
            💰 <b>Ganancia del día:</b>
            <span style="color:green;font-weight:bold;">
                RD$ {{ ganancia_dia_fmt }}
            </span>
        </div>
        {% endif %}
    </div>

    <div class="card">
        <h2>⚠️ Números de Alto Riesgo</h2>
        <p style="color:#666;font-size:14px;margin-bottom:10px">Riesgo = total apostado × pago real (Quiniela 70×, Pale 1200×, Tripleta 20000×, Super Pale 3500×). Por número + lotería + sorteo.</p>
        {% if numeros_riesgo %}
            <ul>
            {% for n in numeros_riesgo[:20] %}
                <li>⚠️ Si sale {{ n.number }} | {{ n.lottery }}{% if n.draw %} {{ n.draw }}{% endif %} ({{ n.play }}) → pierdes RD$ {{ "%.2f"|format(n.posible_pago) }}</li>
            {% endfor %}
            </ul>
        {% else %}
            <p>No hay números de alto riesgo actualmente.</p>
        {% endif %}
    </div>
    <script>
    (function(){
        var audioResultado = document.getElementById("alertaResultado") || document.getElementById("sonidoResultado");
        function sonarResultado(){
            if(audioResultado) audioResultado.play().catch(function(){});
        }
        async function actualizarResultados(){
            try {
                var res = await fetch("/api/resultados_hoy");
                var data = await res.json();
                for (var i = 0; i < data.length; i++) {
                    var r = data[i];
                    var id = r.lottery + "_" + (r.draw || "");
                    var el = document.getElementById(id);
                    if (!el) continue;
                    var n1 = r.n1 || r.primero || "--";
                    var n2 = r.n2 || r.segundo || "--";
                    var n3 = r.n3 || r.tercero || "--";
                    var nuevo = n1 + "-" + n2 + "-" + n3;
                    if (el.getAttribute("data-num") !== nuevo) {
                        el.setAttribute("data-num", nuevo);
                        el.innerHTML = "<span class=\"numero verde\">" + n1 + "</span> " +
                            "<span class=\"numero verde\">" + n2 + "</span> " +
                            "<span class=\"numero verde\">" + n3 + "</span>";
                        sonarResultado();
                    }
                }
            } catch (e) {
                console.log("Error actualizando resultados", e);
            }
        }
        setInterval(actualizarResultados, 30000);
        setTimeout(actualizarResultados, 2000);
    })();
    </script>
    """,
        hoy=hoy,
        resultados_fecha=resultados_fecha,
        resultados_hoy=resultados_hoy,
        total_ganadores=total_ganadores,
        total_pagar=total_pagar,
        lista_ganadores=lista_ganadores,
        numeros_riesgo=numeros_riesgo,
        ganancia_dia=ganancia_dia,
        ganancia_dia_fmt=ganancia_dia_fmt,
    )


# ===============================
# INGRESAR RESULTADOS
# ===============================
@app.route("/admin/resultados", methods=["GET","POST"])
def resultados():

    # 🔒 seguridad
    if not is_admin_or_super():
        return redirect("/")

    # ===============================
    # GUARDAR RESULTADOS
    # ===============================
    if request.method == "POST":
        c = db()
        if not c:
            return "Error conectando base de datos", 500
        try:
            cur = c.cursor()
            loteria = request.form["lottery"]
            draw = request.form.get("draw") or None
            cur.execute(_sql("""
            INSERT INTO resultados (fecha, lottery, draw, primero, segundo, tercero)
            VALUES (%s,%s,%s,%s,%s,%s)
            """), (
                ahora_rd().strftime("%Y-%m-%d"),
                loteria,
                draw,
                request.form["r1"],
                request.form["r2"],
                request.form["r3"]
            ))
            c.commit()
        finally:
            c.close()

        # ✅ REDIRECT CON LOTERIA (FIX IMPORTANTE)
        return redirect(
            f"/admin/premios?fecha={ahora_rd().strftime('%Y-%m-%d')}&loteria={loteria}"
        )

    # ===============================
    # FORMULARIO RESULTADOS
    # ===============================
    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute("SELECT DISTINCT lottery FROM lotteries")
        lotteries = cur.fetchall()
    finally:
        c.close()

    return render_template_string(IOS + """
    <div style="max-width:400px;margin:60px auto;font-family:Arial">

    <h2>🎯 Resultados</h2>

    <form method="post">

    Lotería
    <select name="lottery" required>
        <option value="">Seleccionar</option>
        {% for l in lotteries %}
        <option value="{{ l.lottery }}">{{ l.lottery }}</option>
        {% endfor %}
    </select>

    <br><br>
    1er Premio <input name="r1" required>
    <br><br>
    2do Premio <input name="r2" required>
    <br><br>
    3er Premio <input name="r3" required>

    <br><br>
    <button style="
        background:#002D62;
        color:white;
        padding:12px;
        width:100%;
        border:none;
        border-radius:8px;
        font-weight:bold;
    ">
    Guardar
    </button>

    </form>
    </div>
    """, lotteries=lotteries)

# ===============================
# CIERRE DE CAJA (CORREGIDO PRO)
# ===============================
@app.route("/cierre", methods=["GET", "POST"])
def cierre():
    if not is_admin_or_super():
        return redirect("/venta")

    conn = db()
    if not conn:
        return "Error conectando base de datos", 500
    cur = conn.cursor()
    hoy = ahora_rd().strftime("%Y-%m-%d")

    if os.environ.get("DATABASE_URL"):
        cur.execute("""
            SELECT COALESCE(SUM(ticket_lines.amount),0)
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        """, (hoy,))
    else:
        cur.execute(_sql("""
            SELECT COALESCE(SUM(ticket_lines.amount),0)
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at)=%s
            AND COALESCE(ticket_lines.estado,'activo')!='cancelado'
        """), (hoy,))
    row = cur.fetchone()
    total = list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)

    if os.environ.get("DATABASE_URL"):
        cur.execute("""
            SELECT number, lottery, SUM(amount) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
            AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
            GROUP BY number, lottery
            ORDER BY lottery, number
        """, (hoy,))
    else:
        cur.execute(_sql("""
            SELECT number, lottery, SUM(amount) AS total
            FROM ticket_lines
            JOIN tickets ON tickets.id = ticket_lines.ticket_id
            WHERE DATE(tickets.created_at)=%s
            AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
            GROUP BY number, lottery
            ORDER BY lottery, number
        """), (hoy,))
    numeros = cur.fetchall()

    # ===============================
    # POST → CONFIRMAR CIERRE
    # ===============================
    if request.method == "POST":

        cur.execute(_sql("""
            INSERT INTO cash_closings (date, cajero, total, created_at)
            VALUES (%s, %s, %s, %s)
        """), (
            hoy,
            session.get("u"),
            total,
            ahora_rd().strftime("%Y-%m-%d %H:%M:%S")
        ))

        conn.commit()
        conn.close()
        return redirect("/admin")

    conn.close()

    # ===============================
    # VISTA
    # ===============================
    return render_template_string(
        IOS + ADMIN_MENU + """
<div class="card" id="print-area" style="max-width:600px;margin:40px auto">

<h2>📊 Cierre de Caja</h2>

<p><b>Fecha:</b> {{hoy}}</p>
<p><b>Cajero:</b> {{user}}</p>

<div class="success" style="
background:#eef2ff;
padding:18px;
border-radius:12px;
font-size:22px;
font-weight:bold;
text-align:center;
margin:20px 0;
">
Total del día: ${{ "%.2f"|format(total) }}
</div>

<h3>📋 Números Vendidos</h3>

<table width="100%">
<tr>
<th>Número</th>
<th>Lotería</th>
<th>Total Vendido</th>
</tr>

{% for n,l,t in numeros %}
<tr>
<td>{{n}}</td>
<td>{{l}}</td>
<td>${{ "%.2f"|format(t) }}</td>
</tr>
{% endfor %}
</table>

<form method="POST">
<button style="
margin-top:20px;
background:#CE1126;
font-size:18px;
padding:16px;
">
🔒 Cerrar Caja
</button>
</form>

<button onclick="window.print()" style="margin-top:10px">
🖨️ Imprimir
</button>

<br><br>
<a href="/admin">⬅ Volver al Dashboard</a>

</div>

<style>
@media print{
body *{visibility:hidden}
#print-area,#print-area *{visibility:visible}
#print-area{
position:absolute;
left:0;
top:0;
width:100%;
}
}
</style>
""",
        total=total,
        numeros=numeros,
        hoy=hoy,
        user=session.get("u")
    )

@app.route("/admin/premios")
def premios():

    if not is_admin_or_super():
        return redirect("/")

    fecha = request.args.get("fecha", ahora_rd().strftime("%Y-%m-%d"))
    loteria = request.args.get("loteria")

    # 🔴 OBLIGAR LOTERIA
    if not loteria:
        return "<h2>Debes seleccionar una loteria</h2>"

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            SELECT primero,segundo,tercero
            FROM resultados
            WHERE lottery=%s
            ORDER BY id DESC
            LIMIT 1
        """), (loteria,))
        r = cur.fetchone()

        if not r:
            return f"<h2>No hay resultados cargados para {loteria}</h2>"

        r1, r2, r3 = r["primero"], r["segundo"], r["tercero"]

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT ticket_lines.*, tickets.created_at
                FROM ticket_lines
                JOIN tickets ON tickets.id=ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND ticket_lines.lottery=%s
            """, (fecha, loteria))
        else:
            cur.execute(_sql("""
                SELECT ticket_lines.*, tickets.created_at
                FROM ticket_lines
                JOIN tickets ON tickets.id=ticket_lines.ticket_id
                WHERE DATE(tickets.created_at)=%s
                AND ticket_lines.lottery=%s
            """), (fecha, loteria))
        jugadas = cur.fetchall()

        ganadores = []
        for j in jugadas:
            premio = calcular_premio(
                j["play"],
                j["number"],
                j["amount"],
                r1, r2, r3
            )
            if premio > 0:
                cur.execute(_sql("""
                    SELECT 1 FROM pagos
                    WHERE ticket_id=%s AND numero=%s AND jugada=%s
                """), (j["ticket_id"], j["number"], j["play"]))
                ya_pagado = cur.fetchone()
                ganadores.append({
                    "ticket_id": j["ticket_id"],
                    "lottery": j["lottery"],
                    "number": j["number"],
                    "play": j["play"],
                    "premio": premio,
                    "pagado": True if ya_pagado else False
                })
    finally:
        c.close()
    # ===============================
    # UI PROFESIONAL
    return render_template_string(IOS + """

<style>

/* ===== ANIMACION ENTRADA ===== */
@keyframes aparecer{
0%{opacity:0;transform:translateY(30px) scale(.95)}
100%{opacity:1;transform:translateY(0) scale(1)}
}

/* ===== BRILLO PREMIO PENDIENTE ===== */
@keyframes glow{
0%{box-shadow:0 0 0 rgba(34,197,94,0)}
50%{box-shadow:0 0 25px rgba(34,197,94,.6)}
100%{box-shadow:0 0 0 rgba(34,197,94,0)}
}

/* ===== TARJETA ===== */
.card-ganador{
background:white;
padding:20px;
margin-bottom:20px;
border-radius:18px;
box-shadow:0 12px 35px rgba(0,0,0,.12);
animation:aparecer .4s ease forwards;
transition:.2s;
}

.card-ganador:hover{
transform:translateY(-4px) scale(1.01);
}

/* ===== PAGADO ===== */
.pagado{
border-left:6px solid #16a34a;
}

/* ===== PENDIENTE ===== */
.pendiente{
border-left:6px solid #CE1126;
animation:glow 2s infinite;
}

/* ===== BOTON PAGAR ===== */
.btn-pagar{
width:100%;
background:#22c55e;
color:white;
border:none;
padding:14px;
border-radius:12px;
font-weight:900;
font-size:16px;
cursor:pointer;
transition:.2s;
}

.btn-pagar:hover{
transform:scale(1.05);
background:#16a34a;
}

/* ===== MONTO GRANDE ===== */
.monto{
font-size:24px;
font-weight:900;
color:#002D62;
margin-top:10px;
}

</style>

<div style="max-width:520px;margin:60px auto">

<h2 style="text-align:center">💰 Ganadores</h2>

{% if not ganadores %}

<div style="
background:white;
padding:30px;
border-radius:14px;
box-shadow:0 10px 30px rgba(0,0,0,.1);
text-align:center;
font-weight:bold;
">
❌ No hay ganadores para esta lotería
</div>

{% else %}

{% for g in ganadores %}
<div class="card-ganador {% if g.pagado %}pagado{% else %}pendiente{% endif %}">

<div><b>🎫 Ticket:</b> {{ g.ticket_id }}</div>
<div><b>🎯 Lotería:</b> {{ g.lottery }}</div>
<div><b>🔢 Número:</b> {{ g.number }}</div>
<div><b>🎮 Jugada:</b> {{ g.play }}</div>

<div class="monto">
💵 RD$ {{ "%.2f"|format(g.premio) }}
</div>

{% if g.pagado %}
<div style="
margin-top:10px;
background:#dcfce7;
color:#166534;
padding:10px;
border-radius:10px;
text-align:center;
font-weight:bold;
">
✔ PAGADO
</div>

{% else %}

<form method="post" action="/admin/pagar_premio" onsubmit="return confirmarPago(this)" style="margin-top:15px">

<input type="hidden" name="ticket_id" value="{{ g.ticket_id }}">
<input type="hidden" name="numero" value="{{ g.number }}">
<input type="hidden" name="jugada" value="{{ g.play }}">
<input type="hidden" name="monto" value="{{ g.premio }}">
<input type="hidden" name="lottery" value="{{ g.lottery }}">

<button class="btn-pagar">
💵 PAGAR PREMIO
</button>

</form>

{% endif %}

</div>
{% endfor %}

{% endif %}

</div>

<script>
function confirmarPago(form){
if(confirm("¿Confirmar pago de este premio%s")){
const btn=form.querySelector("button")
btn.disabled=true
btn.innerText="Pagando..."
return true
}
return false
}
</script>

""", ganadores=ganadores)

@app.route("/marcar_pagado", methods=["POST"])
def marcar_pagado():

    # 🔒 seguridad admin
    if not is_admin_or_super():
        return redirect("/")

    ticket_id = request.form.get("ticket_id")
    numero = request.form.get("numero")
    jugada = request.form.get("jugada")
    monto = request.form.get("monto")

    # validar datos
    if not ticket_id or not numero or not jugada or not monto:
        return "Datos inválidos"

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            SELECT 1 FROM pagos
            WHERE ticket_id=%s AND numero=%s AND jugada=%s
        """), (ticket_id, numero, jugada))
        ya_pagado = cur.fetchone()

        if ya_pagado:
            return """
            <h2 style="color:red;text-align:center">
            ESTE PREMIO YA FUE PAGADO
            </h2>
            <a href="/admin/premios">Volver</a>
            """

        cur.execute(_sql("""
            INSERT INTO pagos(ticket_id,numero,jugada,monto,fecha,pagado_por)
            VALUES(%s,%s,%s,%s,%s,%s)
        """), (
            ticket_id,
            numero,
            jugada,
            monto,
            ahora_rd().strftime("%Y-%m-%d"),
            session.get("u")
        ))
        c.commit()
    finally:
        c.close()

    return redirect("/admin/premios")

@app.route("/api/estado_loterias")
def api_estado_loterias():
    """Devuelve el estado actual de cada lotería (ABIERTA/CERRADA) según hora Santo Domingo. Actualización cada 30s en el panel."""
    if not session.get("u") and not session.get("user"):
        return jsonify({"lotteries": []}), 200
    c = db()
    if not c:
        return jsonify({"lotteries": []}), 200
    try:
        cur = c.cursor()
        cur.execute("SELECT lottery, draw FROM lotteries ORDER BY lottery, draw")
        lotteries = []
        for r in cur.fetchall():
            row = dict(r) if hasattr(r, "keys") else {"lottery": r[0], "draw": r[1] if len(r) > 1 else ""}
            lot = row.get("lottery", "") or ""
            draw = row.get("draw", "") or ""
            lotteries.append({"lottery": lot, "draw": draw, "estado": estado_loteria(lot, draw)})
        return jsonify({"lotteries": lotteries})
    finally:
        c.close()


@app.route("/api/ventas_cajeros")
def api_ventas_cajeros():
    """Cintilla global: todos los cajeros con ventas del día y meta desde DB (todos ven lo mismo)."""
    from flask import jsonify
    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    meta_global = float(get_config("meta_diaria") or 0)

    c = db()
    if not c:
        return jsonify({"cajeros": []})
    try:
        cur = c.cursor()

        # 1. Todos los cajeros y su meta (users; role = 'cajero' o 'user')
        cur.execute(_sql("""
            SELECT username, COALESCE(meta, 0) AS meta
            FROM users
            WHERE role IN (%s, %s)
            ORDER BY username
        """), (ROLE_CAJERO, ROLE_USER))
        users_rows = cur.fetchall()
        cajeros_map = {}
        for r in users_rows:
            row = dict(r) if hasattr(r, "keys") else {"username": r[0], "meta": float(r[1] or 0)}
            name = (row.get("username") or "") if hasattr(row, "get") else (r[0] or "")
            meta_val = float(row.get("meta", 0) or 0) if hasattr(row, "get") else float(r[1] or 0)
            if not name:
                continue
            cajeros_map[name] = {"cajero": name, "ventas": 0.0, "meta": meta_val if meta_val else meta_global}

        # 2. Ventas del día por cajero
        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT
                    t.cajero,
                    COALESCE(SUM(tl.amount), 0) AS ventas
                FROM tickets t
                JOIN ticket_lines tl ON tl.ticket_id = t.id
                WHERE (t.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                GROUP BY t.cajero
            """, (hoy_rd,))
        else:
            cur.execute(_sql("""
                SELECT
                    t.cajero,
                    COALESCE(SUM(tl.amount), 0) AS ventas
                FROM tickets t
                JOIN ticket_lines tl ON tl.ticket_id = t.id
                WHERE DATE(t.created_at) = %s
                AND COALESCE(tl.estado, 'activo') != 'cancelado'
                GROUP BY t.cajero
            """), (hoy_rd,))
        ventas_rows = cur.fetchall()
        for r in ventas_rows:
            row = dict(r) if hasattr(r, "keys") else {"cajero": r[0], "ventas": float(r[1] or 0)}
            cajero = row.get("cajero") or (r[0] if r else "")
            ventas = round(float(row.get("ventas") or 0), 2)
            if cajero in cajeros_map:
                cajeros_map[cajero]["ventas"] = ventas
            else:
                cajeros_map[cajero] = {"cajero": cajero, "ventas": ventas, "meta": meta_global}

        cajeros = list(cajeros_map.values())
        return jsonify({"cajeros": cajeros})
    finally:
        c.close()


@app.route("/api/ganancia")
def api_ganancia():
    c = db()
    if not c:
        return {"ventas": 0, "pagado": 0, "ganancia": 0}
    try:
        cur = c.cursor()
        cur.execute("""
            SELECT COALESCE(SUM(amount),0)
            FROM ticket_lines
            WHERE estado!='cancelado' OR estado IS NULL
        """)
        row = cur.fetchone()
        ventas = list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)
        cur.execute("""
            SELECT COALESCE(SUM(monto),0)
            FROM pagos
        """)
        row2 = cur.fetchone()
        pagado = list(row2.values())[0] if row2 and hasattr(row2, "values") else (row2[0] if row2 else 0)
        return {
            "ventas": ventas,
            "pagado": pagado,
            "ganancia": ventas - pagado
        }
    finally:
        c.close()

# ===============================
# ANULAR TICKET (ADMIN)
# ===============================
@app.route("/anular_ticket/<int:ticket_id>", methods=["POST"])
def anular_ticket(ticket_id):

    # 🔒 SEGURIDAD → solo admin
    if not is_admin_or_super():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("SELECT id FROM tickets WHERE id=%s"), (ticket_id,))
        ticket = cur.fetchone()

        if not ticket:
            return "Ticket no encontrado"

        cur.execute(
            _sql("DELETE FROM ticket_lines WHERE ticket_id=%s"),
            (ticket_id,)
        )
        cur.execute(
            _sql("DELETE FROM tickets WHERE id=%s"),
            (ticket_id,)
        )
        c.commit()
    finally:
        c.close()

    session.pop("ticket_id", None)
    return redirect("/venta")

@app.route("/manifest.json")
def manifest():
    return {
        "name":"LA QUE NO FALLA",
        "short_name":"Banca",
        "start_url":"/",
        "display":"standalone",
        "background_color":"#002D62",
        "theme_color":"#002D62",
        "icons":[{"src":"https://flagcdn.com/w320/do.png","sizes":"192x192","type":"image/png"}]
    }

@app.route("/sw.js")
def sw():
    return "self.addEventListener('install',e=>self.skipWaiting())",200,{"Content-Type":"application/javascript"}

# ===============================
# VER HISTORIAL DE PAGOS
# ===============================
@app.route("/admin/pagos")
@app.route("/pagos")
def ver_pagos():
    if not is_admin_or_super():
        return redirect("/")

    hoy_rd = ahora_rd().strftime("%Y-%m-%d")
    primer_dia_mes = ahora_rd().date().replace(day=1).strftime("%Y-%m-%d")
    desde_param = (request.args.get("desde") or "").strip()
    hasta_param = (request.args.get("hasta") or "").strip()
    cajero_sel = (request.args.get("cajero") or "todos").strip() or "todos"
    try:
        desde = datetime.strptime(desde_param, "%Y-%m-%d").strftime("%Y-%m-%d") if desde_param else primer_dia_mes
        hasta = datetime.strptime(hasta_param, "%Y-%m-%d").strftime("%Y-%m-%d") if hasta_param else hoy_rd
    except ValueError:
        desde, hasta = primer_dia_mes, hoy_rd
    if desde > hasta:
        desde, hasta = hasta, desde

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        # Lista de cajeros para dropdown
        cur.execute(_sql("SELECT username FROM users WHERE role IN (%s, %s) ORDER BY username"), (ROLE_USER, ROLE_CAJERO))
        cajeros_rows = cur.fetchall() or []
        cajeros_list = []
        for r in cajeros_rows:
            if hasattr(r, "keys"):
                d = dict(r) if hasattr(r, "get") else dict(zip(r.keys(), r))
                u = (d.get("username") or "").strip()
            else:
                u = (r[0] if len(r) > 0 else "") or ""
            if u:
                cajeros_list.append(u)
        cajeros_set = set(cajeros_list)

        # Historial unificado: pagos (legacy) + pagos_premios (flujo actual), con filtro por fecha/cajero
        if os.environ.get("DATABASE_URL"):
            if cajero_sel.lower() != "todos":
                cur.execute("""
                    SELECT id, monto, fecha_txt AS fecha, usuario, origen
                    FROM (
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha::text, '') AS fecha_txt,
                               COALESCE(NULLIF(pagado_por, ''), '—') AS usuario,
                               'pagos' AS origen,
                               LEFT(COALESCE(fecha::text, ''), 10)::date AS fecha_date
                        FROM pagos
                        UNION ALL
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha::text, '') AS fecha_txt,
                               COALESCE(NULLIF(cajero, ''), '—') AS usuario,
                               'pagos_premios' AS origen,
                               LEFT(COALESCE(fecha::text, ''), 10)::date AS fecha_date
                        FROM pagos_premios
                    ) q
                    WHERE fecha_date BETWEEN %s AND %s
                      AND usuario = %s
                    ORDER BY fecha_date DESC, id DESC
                    LIMIT 300
                """, (desde, hasta, cajero_sel))
            else:
                cur.execute("""
                    SELECT id, monto, fecha_txt AS fecha, usuario, origen
                    FROM (
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha::text, '') AS fecha_txt,
                               COALESCE(NULLIF(pagado_por, ''), '—') AS usuario,
                               'pagos' AS origen,
                               LEFT(COALESCE(fecha::text, ''), 10)::date AS fecha_date
                        FROM pagos
                        UNION ALL
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha::text, '') AS fecha_txt,
                               COALESCE(NULLIF(cajero, ''), '—') AS usuario,
                               'pagos_premios' AS origen,
                               LEFT(COALESCE(fecha::text, ''), 10)::date AS fecha_date
                        FROM pagos_premios
                    ) q
                    WHERE fecha_date BETWEEN %s AND %s
                    ORDER BY fecha_date DESC, id DESC
                    LIMIT 300
                """, (desde, hasta))
        else:
            if cajero_sel.lower() != "todos":
                cur.execute(_sql("""
                    SELECT id, monto, fecha_txt AS fecha, usuario, origen
                    FROM (
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha, '') AS fecha_txt,
                               COALESCE(NULLIF(pagado_por, ''), '—') AS usuario,
                               'pagos' AS origen,
                               DATE(fecha) AS fecha_date
                        FROM pagos
                        UNION ALL
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha, '') AS fecha_txt,
                               COALESCE(NULLIF(cajero, ''), '—') AS usuario,
                               'pagos_premios' AS origen,
                               DATE(fecha) AS fecha_date
                        FROM pagos_premios
                    ) q
                    WHERE fecha_date BETWEEN %s AND %s
                      AND usuario = %s
                    ORDER BY fecha_date DESC, id DESC
                    LIMIT 300
                """), (desde, hasta, cajero_sel))
            else:
                cur.execute(_sql("""
                    SELECT id, monto, fecha_txt AS fecha, usuario, origen
                    FROM (
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha, '') AS fecha_txt,
                               COALESCE(NULLIF(pagado_por, ''), '—') AS usuario,
                               'pagos' AS origen,
                               DATE(fecha) AS fecha_date
                        FROM pagos
                        UNION ALL
                        SELECT id,
                               COALESCE(monto,0) AS monto,
                               COALESCE(fecha, '') AS fecha_txt,
                               COALESCE(NULLIF(cajero, ''), '—') AS usuario,
                               'pagos_premios' AS origen,
                               DATE(fecha) AS fecha_date
                        FROM pagos_premios
                    ) q
                    WHERE fecha_date BETWEEN %s AND %s
                    ORDER BY fecha_date DESC, id DESC
                    LIMIT 300
                """), (desde, hasta))
        rows = cur.fetchall() or []
        pagos = []
        for r in rows:
            if hasattr(r, "keys"):
                d = dict(r) if hasattr(r, "get") else dict(zip(r.keys(), r))
            else:
                d = {
                    "id": r[0] if len(r) > 0 else "",
                    "monto": r[1] if len(r) > 1 else 0,
                    "fecha": r[2] if len(r) > 2 else "",
                    "usuario": r[3] if len(r) > 3 else "—",
                    "origen": r[4] if len(r) > 4 else "",
                }
            d["usuario"] = d.get("usuario") or "—"
            pagos.append(d)
            if d["usuario"] not in ("", "—"):
                cajeros_set.add(d["usuario"])
        total = sum(float(p.get("monto") or 0) for p in pagos)
        cajeros_list = sorted(cajeros_set)
    finally:
        c.close()

    return render_template_string(IOS + """
    <div class="card">
        <h2>💰 Historial de Pagos</h2>
        <form method="get" action="/admin/pagos" style="display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:10px 0 14px 0">
            <label>Desde:</label>
            <input type="date" name="desde" value="{{ desde }}" required style="padding:8px;border:1px solid #cbd5e1;border-radius:6px">
            <label>Hasta:</label>
            <input type="date" name="hasta" value="{{ hasta }}" required style="padding:8px;border:1px solid #cbd5e1;border-radius:6px">
            <label>Cajero:</label>
            <select name="cajero" style="padding:8px;border:1px solid #cbd5e1;border-radius:6px">
                <option value="todos" {% if cajero_sel == "todos" %}selected{% endif %}>Todos</option>
                {% for u in cajeros_list %}
                <option value="{{ u }}" {% if cajero_sel == u %}selected{% endif %}>{{ u }}</option>
                {% endfor %}
            </select>
            <button type="submit" class="admin-btn" style="padding:8px 18px;border:none;cursor:pointer">Buscar</button>
        </form>
        <h3 class="danger">Total pagado: RD$ {{ "%.2f"|format(total) }}</h3>
    </div>

    <div class="card">
        <table width="100%">
            <tr>
                <th>ID</th>
                <th>Monto</th>
                <th>Usuario</th>
                <th>Fecha</th>
                <th>Origen</th>
            </tr>

            {% for p in pagos %}
            <tr>
                <td>#{{ p.id }}</td>
                <td class="success">RD$ {{ "%.2f"|format(p.monto) }}</td>
                <td>{{ p.usuario }}</td>
                <td>{{ p.fecha }}</td>
                <td>{{ p.origen }}</td>
            </tr>
            {% endfor %}
        </table>
        {% if not pagos %}
        <p style="margin-top:12px;color:#64748b">No hay pagos para este filtro.</p>
        {% endif %}
    </div>
    """, pagos=pagos, total=total, desde=desde, hasta=hasta, cajero_sel=cajero_sel, cajeros_list=cajeros_list)

# ===============================
# PROBAR CALCULO DE PREMIOS
# ===============================
@app.route("/probar_premio")
def probar_premio():

    ejemplo1 = calcular_premio("Quiniela", 10, 1)
    ejemplo2 = calcular_premio("Pale", 5, "12")
    ejemplo3 = calcular_premio("Tripleta", 2)
    ejemplo4 = calcular_premio("Super Pale", 3)

    return f"""
    <h2>Prueba de Premios</h2>

    Quiniela $10 (1er premio) → {ejemplo1}<br>
    Pale $5 (1ro+2do) → {ejemplo2}<br>
    Tripleta $2 → {ejemplo3}<br>
    Super Pale $3 → {ejemplo4}
    """

@app.route("/calculadora_premios", methods=["GET","POST"])
def calculadora_premios():

    if session.get("u") is None:
        return redirect("/")

    premio = None

    if request.method == "POST":

        try:
            play = request.form.get("play")
            monto = float(request.form.get("monto", 0))
        except:
            monto = 0

        if monto > 0:
            premio = monto * PAGOS_CALCULADORA.get(play or "", 0)


    return render_template_string(IOS + """
    <div class="card">
        <h2>💰 Calculadora de Premios</h2>

        <form method="post">

            <label>Tipo de jugada</label>
            <select name="play">
                <option>Quiniela 1er</option>
                <option>Quiniela 2do</option>
                <option>Quiniela 3ro</option>
                <option>Pale 1y2</option>
                <option>Pale 1y3</option>
                <option>Pale 2y3</option>
                <option>Pale</option>
                <option>Tripleta</option>
                <option>Super Pale</option>
            </select>

            <label>Monto apostado</label>
            <input name="monto" type="number" step="0.01" required>

            <button>Calcular</button>

        </form>

        {% if premio %}

        <div class="success" style="
            margin-top:20px;
            font-size:22px;
            text-align:center;
            color:#34C759;
            font-weight:900;
        ">
            Ganas: RD$ {{ "%.2f"|format(premio) }}
        </div>

        <form method="post" action="/marcar_pagado">
            <input type="hidden" name="monto" value="{{ premio }}">

            <button style="
                margin-top:12px;
                background:#34C759;
                font-size:18px;
                padding:16px;
                width:100%;
                border:none;
                border-radius:14px;
                font-weight:900;
                color:white;
                cursor:pointer;
            ">
                💵 PAGAR PREMIO
            </button>
        </form>

        {% endif %}

    </div>
    """, premio=premio)

# ===============================
# PAGAR PREMIO
# ===============================
@app.route("/admin/pagar_premio", methods=["POST"])
def pagar_premio():

    if not is_admin_or_super():
        return redirect("/")

    ticket_id = request.form.get("ticket_id")
    numero = request.form.get("numero")
    jugada = request.form.get("jugada")
    monto = request.form.get("monto")
    lottery = request.form.get("lottery")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            SELECT 1 FROM pagos
            WHERE ticket_id=%s AND numero=%s AND jugada=%s
        """), (ticket_id, numero, jugada))
        existe = cur.fetchone()

        if not existe:
            cur.execute(_sql("""
                INSERT INTO pagos (ticket_id, numero, jugada, monto, fecha, pagado_por)
                VALUES (%s,%s,%s,%s,%s,%s)
            """), (
                ticket_id,
                numero,
                jugada,
                monto,
                ahora_rd().strftime("%Y-%m-%d %H:%M:%S"),
                (session.get("u") or session.get("user") or "admin")
            ))
            # Restar premio de la caja de la lotería (balance baja con premios pagados)
            if lottery:
                try:
                    monto_f = float(monto)
                    cur.execute(_sql("""
                        UPDATE balance_por_loteria
                        SET balance = balance - %s,
                        updated_at = CURRENT_TIMESTAMP
                        WHERE lottery = %s
                    """), (monto_f, lottery))
                except (ValueError, TypeError):
                    pass
            c.commit()
    finally:
        c.close()

    return redirect(f"/admin/recibo_pago/{ticket_id}/{numero}/{jugada}")

# ===============================
# RECIBO PAGO PREMIO
# ===============================
@app.route("/admin/recibo_pago/<ticket_id>/<numero>/<jugada>")
def recibo_pago(ticket_id, numero, jugada):
    if not is_admin_or_super():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            SELECT monto, fecha
            FROM pagos
            WHERE ticket_id=%s AND numero=%s AND jugada=%s
            ORDER BY id DESC LIMIT 1
        """), (ticket_id, numero, jugada))
        pago = cur.fetchone()
    finally:
        c.close()

    if not pago:
        return "Pago no encontrado"

    return render_template_string("""

<style>
body{font-family:monospace;text-align:center}

.ticket{
width:58mm;
margin:auto;
padding:10px;
}

.line{border-top:1px dashed black;margin:10px 0}

button{
margin-top:10px;
padding:10px;
border:none;
background:black;
color:white;
}
</style>

<div class="ticket">

<b>$ LA QUE NO FALLA $</b><br>
RECIBO PAGO PREMIO

<div class="line"></div>

Ticket: {{ticket_id}}<br>
Número: {{numero}}<br>
Jugada: {{jugada}}<br>

<div class="line"></div>

<b>PAGADO</b><br>
RD$ {{monto}}

<div class="line"></div>

Fecha: {{fecha}}

<div class="line"></div>

GRACIAS POR JUGAR

<br>
<button onclick="window.print()">IMPRIMIR</button>

</div>

<script>
window.onload=function(){
setTimeout(()=>window.print(),500)
}
</script>

""",
ticket_id=ticket_id,
numero=numero,
jugada=jugada,
monto=pago["monto"],
fecha=pago["fecha"]
)

# ===============================
# GUARDAR PREMIO PAGADO
# ===============================
@app.route("/marcar_pagado_manual", methods=["POST"])
def marcar_pagado_manual():

    if session.get("u") is None:
        return redirect("/")

    monto = float(request.form.get("monto", 0))

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            INSERT INTO pagos (ticket_id, numero, jugada, monto, fecha, usuario)
            VALUES (%s,%s,%s,%s,%s,%s)
        """), (
            0,
            "MANUAL",
            "PREMIO",
            monto,
            ahora_rd().strftime("%Y-%m-%d"),
            session.get("u")
        ))
        c.commit()
    finally:
        c.close()

    return render_template_string(IOS + """
    <div class="card">
    <h2 style="color:#16a34a">✅ Premio Pagado</h2>
    <h3>RD$ {{monto}}</h3>
    <a href="/venta">Volver</a>
    </div>
    """, monto=monto)

# ===============================
# VER HISTORIAL JUGADAS (FIX PRO)
# ===============================
@app.route("/admin/jugadas")
def ver_jugadas():
    if not is_staff():
        return redirect("/")

    usuario = session.get("u") or session.get("user") or ""
    es_admin = is_admin_or_super()

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        if es_admin:
            cur.execute(_sql("""
                SELECT hj.id, hj.ticket_id, hj.lottery, hj.number, hj.play, hj.amount, hj.created_at,
                COALESCE(hj.estado,'activo') AS estado_real, t.created_at AS ticket_created_at
                FROM historial_jugadas hj
                JOIN tickets t ON t.id = hj.ticket_id
                ORDER BY hj.id DESC
            """))
        else:
            cur.execute(_sql("""
                SELECT hj.id, hj.ticket_id, hj.lottery, hj.number, hj.play, hj.amount, hj.created_at,
                COALESCE(hj.estado,'activo') AS estado_real, t.created_at AS ticket_created_at
                FROM historial_jugadas hj
                JOIN tickets t ON t.id = hj.ticket_id
                WHERE t.cajero = %s
                ORDER BY hj.id DESC
            """), (usuario,))
        rows = cur.fetchall()
    finally:
        c.close()

    ahora = ahora_rd()
    TIEMPO_LIMITE_SEG = 120  # 2 minutos: no editar/cancelar después (salvo admin)
    jugadas = []
    for r in rows:
        row = dict(r) if hasattr(r, "keys") else {
            "id": r[0], "ticket_id": r[1], "lottery": r[2], "number": r[3],
            "play": r[4], "amount": r[5], "created_at": r[6] if len(r) > 6 else None,
            "estado_real": r[7] if len(r) > 7 else "activo",
            "ticket_created_at": r[8] if len(r) > 8 else None
        }
        ca = row.get("created_at")
        ticket_created = row.get("ticket_created_at")
        # Editable: dentro de 2 min desde CREACIÓN DEL TICKET, o siempre si admin
        dentro_limite_ticket = False
        if ticket_created:
            try:
                if hasattr(ticket_created, "timestamp"):
                    tc = ticket_created
                    if getattr(tc, "tzinfo", None) is None:
                        tc = tc.replace(tzinfo=TZ_RD) if hasattr(tc, "replace") else None
                    else:
                        tc = tc.astimezone(TZ_RD)
                else:
                    tc = datetime.strptime(str(ticket_created)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_RD)
                if tc:
                    dentro_limite_ticket = (ahora - tc).total_seconds() <= TIEMPO_LIMITE_SEG
            except (ValueError, TypeError):
                pass
        editable = es_admin or (dentro_limite_ticket and row.get("estado_real") != "cancelado")
        jugadas.append({
            "id": row.get("id"),
            "ticket_id": row.get("ticket_id"),
            "lottery": row.get("lottery"),
            "number": row.get("number"),
            "play": row.get("play"),
            "amount": row.get("amount"),
            "created_at": ca if not hasattr(ca, "strftime") else (ca.strftime("%Y-%m-%d %H:%M:%S") if ca else ""),
            "editable": editable,
            "estado": row.get("estado_real", "activo")
        })

    return render_template_string(IOS + """

<style>

.historial{
max-width:950px;
margin:40px auto;
padding:0 10px;
box-sizing:border-box;
}

/* ===== BOTON ELIMINAR PREMIUM ===== */
.btn-eliminar{
background:linear-gradient(135deg,#ef4444,#b91c1c);
color:white;
border:none;
padding:10px 18px;
border-radius:10px;
font-weight:900;
cursor:pointer;
margin-right:8px;
box-shadow:0 4px 12px rgba(0,0,0,.25);
transition:.2s;
}

.btn-eliminar:hover{
transform:scale(1.05);
}

/* ===== BOTON EDITAR PREMIUM ===== */
.btn-editar{
background:linear-gradient(135deg,#3b82f6,#1d4ed8);
color:white;
padding:10px 18px;
border-radius:10px;
font-weight:900;
text-decoration:none;
box-shadow:0 4px 12px rgba(0,0,0,.25);
transition:.2s;
}

.btn-editar:hover{
transform:scale(1.05);
}

.historial table{
width:100%;
border-collapse:collapse;
background:white;
border-radius:12px;
box-shadow:0 10px 30px rgba(0,0,0,.1);
}

.historial th{
background:#002D62;
color:white;
padding:14px;
text-align:left;
}

.historial td{
padding:12px;
border-bottom:1px solid #eee;
}

.historial tr:hover{
background:#f5f8ff;
}

/* botones */
.btn{
padding:6px 12px;
border-radius:8px;
text-decoration:none;
color:white;
font-weight:bold;
margin-right:6px;
}

.btn-del{background:#ef4444;}
.btn-edit{background:#2563eb;}

.bloqueado{
color:#999;
font-weight:bold;
}

/* Tabla responsive móvil - scroll horizontal */
.tabla-resultados{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;margin:0 -10px;padding:0 10px;}
.tabla-resultados table{min-width:640px;width:100%;table-layout:auto;}
@media (max-width:640px){
.historial{margin:20px 0;padding:0 8px;}
.tabla-resultados{margin:0 -8px;padding:0 8px;}
}

</style>

<div class="historial">

<h2 style="margin-bottom:20px">🎯 Historial Jugadas</h2>

<div class="tabla-resultados">
<table>

<tr>
<th>ID</th>
<th>Ticket</th>
<th>Lotería</th>
<th>Número</th>
<th>Jugada</th>
<th>Monto</th>
<th>Hora</th>
<th>Acciones</th>
</tr>

{% for j in jugadas %}

<tr class="{{ 'cancelada' if j.estado=='cancelado' else '' }}">

<td>{{j.id}}</td>
<td>{{j.ticket_id}}</td>
<td>{{j.lottery}}</td>
<td>{{j.number}}</td>
<td>{{j.play}}</td>
<td>RD$ {{j.amount}}</td>
<td>{{j.created_at}}</td>

<td>

{% if j.estado == "cancelado" %}

<span class="bloqueado">❌ CANCELADO</span>

{% elif j.editable %}

<form action="/admin/eliminar_jugada/{{ j.id }}"
      method="POST"
      style="display:inline"
      onsubmit="return confirm('¿Eliminar esta jugada%s')">

<button type="submit" class="btn-eliminar">
❌ Eliminar
</button>

</form>

<a href="/admin/editar_jugada/{{j.id}}" class="btn-editar">
✏ Editar
</a>

<a href="/admin/repetir_ticket/{{j.ticket_id}}" class="btn-repetir" style="
background:linear-gradient(135deg,#10b981,#059669);
color:white;
padding:8px 12px;
border-radius:8px;
font-weight:bold;
text-decoration:none;
margin-left:4px;
">
🔄 Repetir
</a>

{% else %}

<span class="bloqueado">⛔ Bloqueado</span>
<a href="/admin/repetir_ticket/{{j.ticket_id}}" class="btn-repetir" style="
background:linear-gradient(135deg,#10b981,#059669);
color:white;
padding:8px 12px;
border-radius:8px;
font-weight:bold;
text-decoration:none;
margin-left:8px;
display:inline-block;
">
🔄 Repetir
</a>

{% endif %}

</td>

</tr>

{% endfor %}

</table>
</div>

</div>

""", jugadas=jugadas)
    
# ===============================
# ANULAR JUGADA (REAL PRO FINAL FIX)
# ===============================
@app.route("/admin/eliminar_jugada/<int:id>", methods=["POST"])
def eliminar_jugada(id):

    if not is_staff():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("""
            SELECT id, ticket_id, lottery, play, number, amount, created_at,
            COALESCE(estado,'activo') as estado
            FROM historial_jugadas
            WHERE id=%s
        """), (id,))
        jugada = cur.fetchone()

        if not jugada:
            return "Jugada no existe"
        jugada = dict(jugada) if hasattr(jugada, "keys") else {"id": jugada[0], "ticket_id": jugada[1], "lottery": jugada[2], "play": jugada[3], "number": jugada[4], "amount": jugada[5], "created_at": jugada[6], "estado": jugada[7] if len(jugada) > 7 else "activo"}
        if session.get("role") in (ROLE_CAJERO, ROLE_USER):
            cur.execute(_sql("SELECT cajero FROM tickets WHERE id=%s"), (jugada["ticket_id"],))
            trow = cur.fetchone()
            tcajero = (trow.get("cajero") if hasattr(trow, "get") and trow else (trow[0] if trow else None)) or ""
            if tcajero != (session.get("u") or ""):
                return "No autorizado", 403
        if jugada["estado"] == "cancelado":
            return redirect("/admin/jugadas")
        # Bloqueo anti-trampa: ticket no editable/cancelable después de 2 min (solo ADMIN puede después)
        cur.execute(_sql("SELECT created_at FROM tickets WHERE id=%s"), (jugada["ticket_id"],))
        trow = cur.fetchone()
        ticket_created = (trow.get("created_at") if hasattr(trow, "get") and trow else (trow[0] if trow else None)) if trow else None
        try:
            if ticket_created:
                if hasattr(ticket_created, "timestamp"):
                    tc = ticket_created
                    if getattr(tc, "tzinfo", None) is None:
                        tc = tc.replace(tzinfo=TZ_RD) if hasattr(tc, "replace") else datetime.fromisoformat(str(tc)[:19].replace("Z", "+00:00")).astimezone(TZ_RD)
                else:
                    tc = datetime.strptime(str(ticket_created)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_RD)
                if hasattr(tc, "astimezone") and getattr(tc, "tzinfo", None) is not None and tc.tzinfo != TZ_RD:
                    tc = tc.astimezone(TZ_RD)
                tiempo_limite = tc + timedelta(minutes=2)
                if ahora_rd() > tiempo_limite and not is_admin_or_super():
                    return "Tiempo expirado. Solo el administrador puede cancelar después de 2 minutos.", 403
        except (ValueError, TypeError):
            pass
        try:
            ca = jugada.get("created_at")
            fecha = datetime.strptime(str(ca)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_RD) if ca else None
        except (ValueError, TypeError):
            return "Error formato fecha"
        if not fecha:
            return "Error formato fecha"
        if (ahora_rd() - fecha).total_seconds() > 600:
            return "Tiempo expirado"

        cur.execute(_sql("""
            UPDATE historial_jugadas
            SET estado='cancelado'
            WHERE id=%s
        """), (id,))
        cur.execute(_sql("""
            UPDATE ticket_lines
            SET estado='cancelado'
            WHERE id = (
                SELECT id FROM ticket_lines
                WHERE ticket_id=%s
                AND lottery=%s
                AND play=%s
                AND number=%s
                AND amount=%s
                LIMIT 1
            )
        """), (
            jugada["ticket_id"],
            jugada["lottery"],
            jugada["play"],
            jugada["number"],
            jugada["amount"]
        ))
        c.commit()
    except Exception as e:
        if c:
            try:
                c.rollback()
            except Exception:
                pass
        print("ERROR CANCELAR:", e)
        return "Error cancelando jugada", 500
    finally:
        if c:
            c.close()

    return redirect("/admin/jugadas")
    
# ===============================
# EDITAR JUGADA (PRO REAL FIX)
# ===============================
@app.route("/admin/editar_jugada/<int:id>", methods=["GET","POST"])
def editar_jugada(id):

    if not is_staff():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute(_sql("SELECT * FROM historial_jugadas WHERE id=%s"), (id,))
        j = cur.fetchone()
    except Exception:
        if c:
            c.close()
        return "Error leyendo jugada", 500
    if not j:
        c.close()
        return "Jugada no existe"
    j = dict(j) if hasattr(j, "keys") else j
    if session.get("role") in (ROLE_CAJERO, ROLE_USER):
        cur.execute(_sql("SELECT cajero FROM tickets WHERE id=%s"), (j.get("ticket_id"),))
        trow = cur.fetchone()
        tcajero = (trow.get("cajero") if hasattr(trow, "get") and trow else (trow[0] if trow else None)) or ""
        if tcajero != (session.get("u") or ""):
            c.close()
            return "No autorizado", 403
    # Bloqueo anti-trampa: ticket no editable después de 2 min (solo ADMIN puede)
    cur.execute(_sql("SELECT created_at FROM tickets WHERE id=%s"), (j.get("ticket_id"),))
    trow = cur.fetchone()
    ticket_created = (trow.get("created_at") if hasattr(trow, "get") and trow else (trow[0] if trow else None)) if trow else None
    if ticket_created and not is_admin_or_super():
        try:
            if hasattr(ticket_created, "timestamp"):
                tc = ticket_created
                if getattr(tc, "tzinfo", None) is None:
                    tc = tc.replace(tzinfo=TZ_RD) if hasattr(tc, "replace") else datetime.fromisoformat(str(tc)[:19]).replace(tzinfo=TZ_RD)
                elif getattr(tc, "tzinfo", None) is not None:
                    tc = tc.astimezone(TZ_RD)
            else:
                tc = datetime.strptime(str(ticket_created)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_RD)
            if ahora_rd() > tc + timedelta(minutes=2):
                c.close()
                return "Tiempo expirado para editar (2 min). Solo el administrador puede editar después.", 403
        except (ValueError, TypeError):
            pass
    try:
        ca = j.get("created_at")
        fecha = datetime.strptime(str(ca)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_RD) if ca else None
    except (ValueError, TypeError):
        c.close()
        return "Error formato fecha", 500
    if not fecha or (ahora_rd() - fecha).total_seconds() > 600:
        c.close()
        return "Tiempo expirado para editar", 400

    if request.method == "POST":
        number = request.form.get("number", "").strip()
        amount = request.form.get("amount", "").strip()

        if not number or not amount:
            c.close()
            return "Todos los campos obligatorios", 400

        number = normalizar_numero(number)
        import re
        if not re.match(r'^\d+(-\d+){0,2}$', number):
            c.close()
            return "Numero invalido", 400

        try:
            amount = float(amount)
            if amount <= 0:
                raise ValueError()
        except (ValueError, TypeError):
            c.close()
            return "Monto invalido", 400

        old_number = j["number"]
        old_amount = j["amount"]
        if old_number == number and old_amount == amount:
            c.close()
            return redirect("/admin/jugadas")

        try:
            cur.execute(_sql("""
                UPDATE historial_jugadas
                SET number=%s, amount=%s
                WHERE id=%s
            """), (number, amount, id))
            try:
                cur.execute(_sql("""
                    UPDATE ticket_lines
                    SET number=%s, amount=%s
                    WHERE ticket_id=%s
                """), (number, amount, j["ticket_id"]))
            except Exception:
                pass
            c.commit()
        finally:
            c.close()
        return redirect("/admin/jugadas")

    c.close()

    return render_template_string(IOS + """
    <div class="card">
        <h2>✏ Editar Jugada</h2>

        <form method="post">
            Número
            <input name="number" value="{{j.number}}" required>

            Monto
            <input name="amount" value="{{j.amount}}" required>

            <button>Guardar</button>
        </form>
    </div>
    """, j=j)

# =====================================================
# CUADRE PREVIO DEL DÍA (ANTES DEL SORTEO) — FIX BANCA REAL
# =====================================================
@app.route("/cuadre_previo")
def cuadre_previo():
    if not is_admin_or_super():
        return redirect("/venta")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        hoy = ahora_rd().strftime("%Y-%m-%d")
        ahora = ahora_rd().strftime("%Y-%m-%d %H:%M:%S")

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
            """, (hoy,))
        else:
            cur.execute(_sql("""
                SELECT COALESCE(SUM(ticket_lines.amount),0)
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at)=%s
                AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
            """), (hoy,))
        row = cur.fetchone()
        total = list(row.values())[0] if row and hasattr(row, "values") else (row[0] if row else 0)

        if os.environ.get("DATABASE_URL"):
            cur.execute("""
                SELECT lottery, draw, SUM(amount) AS total
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE (tickets.created_at AT TIME ZONE 'America/Santo_Domingo')::date = %s
                AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
                GROUP BY lottery, draw
                ORDER BY lottery, draw
            """, (hoy,))
        else:
            cur.execute(_sql("""
                SELECT lottery, draw, SUM(amount) AS total
                FROM ticket_lines
                JOIN tickets ON tickets.id = ticket_lines.ticket_id
                WHERE DATE(tickets.created_at)=%s
                AND (ticket_lines.estado!='cancelado' OR ticket_lines.estado IS NULL)
                GROUP BY lottery, draw
                ORDER BY lottery, draw
            """), (hoy,))
        detalle = cur.fetchall()
    finally:
        c.close()
    
    return render_template_string(IOS + """
<pre style="font-family:monospace;font-size:12px">
LA QUE NO FALLA
CUADRE PREVIO DEL DIA
------------------------------
Fecha: {{hoy}}
Hora : {{ahora}}
Cajero: {{user}}
------------------------------
Loteria        Sorteo   Total
------------------------------
{% for d in detalle %}
{{ d.lottery[:13].ljust(13) }} {{ d.draw[:8].ljust(8) }} ${{ "%.2f"|format(d.total) }}
{% endfor %}
------------------------------
TOTAL GENERAL: ${{ "%.2f"|format(total) }}
------------------------------
*** CUADRE PREVIO ***
ANTES DE SALIR LOS NUMEROS
NO ES CIERRE FINAL
------------------------------

</pre>

<div style="text-align:center">
  <button onclick="window.print()">IMPRIMIR</button>
  <br><br>
  <a href="/venta">VOLVER</a>
</div>
""",
        hoy=hoy,
        ahora=ahora,
        user=session["u"],
        detalle=detalle,
        total=total
    )

# ===============================
# VER AUDITORIA SIMPLE
# ===============================
@app.route("/admin/auditoria")
def auditoria():
    if not is_admin_or_super():
        return redirect("/")

    c = db()
    if not c:
        return "Error conectando base de datos", 500
    try:
        cur = c.cursor()
        cur.execute("""
            SELECT id, ticket_id, lottery, number, play, amount, created_at,
                   COALESCE(estado,'activo') as estado
            FROM historial_jugadas
            ORDER BY id DESC
            LIMIT 100
        """)
        raw_logs = cur.fetchall()
        logs = []
        for r in (raw_logs or []):
            if hasattr(r, "keys"):
                d = dict(r)
            else:
                try:
                    d = {"id": r[0], "ticket_id": r[1], "lottery": r[2], "number": r[3],
                         "play": r[4], "amount": r[5], "created_at": r[6] if len(r) > 6 else None,
                         "estado": r[7] if len(r) > 7 else "activo"}
                except (IndexError, TypeError):
                    continue
            ca = d.get("created_at")
            if ca and hasattr(ca, "strftime"):
                d["fecha"] = ca.strftime("%Y-%m-%d %H:%M:%S")
            else:
                d["fecha"] = str(ca) if ca else ""
            d["jugada_id"] = d.get("id", "")
            d["accion"] = d.get("estado", "-")
            d["usuario"] = "-"
            logs.append(d)
    except Exception as e:
        if c:
            try:
                c.close()
            except Exception:
                pass
        print("ERROR AUDITORIA:", e)
        traceback.print_exc()
        return render_template_string(IOS + """
        <div class="card text-center">
            <h2 class="danger">Error en auditoría</h2>
            <p>""" + str(e) + """</p>
            <a href="/admin">Volver al panel</a>
        </div>
        """), 500
    finally:
        try:
            c.close()
        except Exception:
            pass

    return render_template_string("""
    <h2>Auditoría</h2>
    <table border=1 cellpadding=10>
    <tr>
        <th>ID</th>
        <th>Jugada</th>
        <th>Acción</th>
        <th>Numero</th>
        <th>Monto</th>
        <th>Usuario</th>
        <th>Fecha</th>
    </tr>

    {% for l in logs %}
    <tr>
        <td>{{l.id}}</td>
        <td>{{l.jugada_id}}</td>
        <td>{{l.accion}}</td>
        <td>{{l.number}}</td>
        <td>{{l.amount}}</td>
        <td>{{l.usuario}}</td>
        <td>{{l.fecha}}</td>
    </tr>
    {% endfor %}
    </table>
    """, logs=logs)

# =====================================================
# RUN (local only; Render uses gunicorn with 0.0.0.0:$PORT)
# =====================================================
if __name__ == "__main__":
    # El scheduler de resultados ya se inició al importar el módulo (cada 5 min).
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)




