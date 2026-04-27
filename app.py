import io
import re
import html
import hashlib
import json
import os
import sqlite3
import threading
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "Control FULL Aurora"
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "aurora_full_v3.db"
MAESTRO_PATH = DATA_DIR / "maestro_sku_ean.xlsx"
DEFAULT_SHEETS_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbzwfCk7ov8fCdX3WoTon-25Q8W-iLZUfWqUTvRSLjOGrkid6J2fNgGSmnSbB7lqUiw/exec"

st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")

# ============================================================
# Utilidades
# ============================================================

def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)


def db():
    ensure_data_dir()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def clean_text(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).replace("\u00a0", " ").strip()
    if s.lower() in {"nan", "none", "null", "nat"}:
        return ""
    return re.sub(r"\s+", " ", s)


def normalize_header(v) -> str:
    s = clean_text(v).lower()
    trans = str.maketrans("áéíóúüñ°º", "aeiouunoo")
    s = s.translate(trans)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def norm_code(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return ("%.0f" % v).strip()
    s = str(v).strip().replace("\u00a0", "")
    if s.lower() in {"nan", "none", "null"}:
        return ""
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\s+", "", s)
    return s.upper()


def to_int(v) -> int:
    s = clean_text(v)
    if not s:
        return 0
    s = s.replace(".", "").replace(",", ".")
    try:
        return int(float(s))
    except Exception:
        return 0


def esc(v) -> str:
    return html.escape(clean_text(v), quote=True)


def fmt_dt(v) -> str:
    s = clean_text(v)
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s).strftime("%d-%m-%Y %H:%M:%S")
    except Exception:
        return s


def col_exact(columns, aliases):
    cmap = {normalize_header(c): c for c in columns}
    for a in aliases:
        key = normalize_header(a)
        if key in cmap:
            return cmap[key]
    return None


def col_required(columns, field_name, aliases):
    c = col_exact(columns, aliases)
    if not c:
        raise ValueError(f"No encontré columna obligatoria para {field_name}. Encabezados leídos: {list(columns)}")
    return c


def split_codes(v):
    text = clean_text(v)
    if not text:
        return []
    parts = re.split(r"[,;/|\n\t ]+", text)
    out = []
    for p in parts:
        c = norm_code(p)
        if c:
            out.append(c)
    return list(dict.fromkeys(out))


def is_supermercado(v) -> bool:
    return "SUPERMERCADO" in clean_text(v).upper()


# ============================================================
# Base de datos nueva v3
# ============================================================

def init_db():
    with db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                archivo TEXT,
                hoja TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                area TEXT,
                nro TEXT,
                codigo_ml TEXT,
                codigo_universal TEXT,
                sku TEXT,
                descripcion TEXT,
                unidades INTEGER NOT NULL DEFAULT 0,
                acopiadas INTEGER NOT NULL DEFAULT 0,
                identificacion TEXT,
                vence TEXT,
                dia TEXT,
                hora TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                scan_primario TEXT,
                scan_secundario TEXT,
                cantidad INTEGER NOT NULL,
                modo TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS maestro (
                code TEXT PRIMARY KEY,
                sku TEXT NOT NULL,
                descripcion TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS backup_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                sent_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS label_prints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                codigo_ml TEXT,
                sku TEXT,
                descripcion TEXT,
                cantidad INTEGER NOT NULL DEFAULT 0,
                print_scope TEXT NOT NULL,
                print_kind TEXT NOT NULL DEFAULT 'NORMAL',
                block_index INTEGER,
                block_key TEXT,
                is_reprint INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS label_blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                block_index INTEGER NOT NULL,
                block_key TEXT NOT NULL,
                products_count INTEGER NOT NULL DEFAULT 0,
                normal_qty INTEGER NOT NULL DEFAULT 0,
                separator_qty INTEGER NOT NULL DEFAULT 0,
                total_qty INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'IMPRESO',
                download_count INTEGER NOT NULL DEFAULT 1,
                last_printed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_label_blocks_unique ON label_blocks (lote_id, block_index, block_key)")
        c.commit()


# ============================================================
# Respaldo externo Google Sheets por webhook
# ============================================================

def get_backup_webhook_url() -> str:
    """URL de respaldo externo.
    Prioridad: Streamlit Secrets, variable de entorno y URL integrada en este app.py.
    """
    try:
        url = st.secrets.get("SHEETS_WEBHOOK_URL", "")
    except Exception:
        url = ""
    if not url:
        url = os.environ.get("SHEETS_WEBHOOK_URL", "")
    if not url:
        url = DEFAULT_SHEETS_WEBHOOK_URL
    return clean_text(url)


def enqueue_backup_event(event_type: str, payload: dict):
    """Guarda el evento en cola local y dispara envío en segundo plano.
    La operación principal nunca queda bloqueada por Google Sheets.
    """
    now = datetime.now().isoformat(timespec="seconds")
    safe_payload = json.dumps(payload, ensure_ascii=False, default=str)
    with db() as c:
        c.execute(
            "INSERT INTO backup_queue (event_type, payload_json, status, attempts, created_at) VALUES (?, ?, 'pending', 0, ?)",
            (event_type, safe_payload, now),
        )
        c.commit()

    webhook_url = get_backup_webhook_url()
    if webhook_url:
        threading.Thread(target=flush_backup_queue, args=(webhook_url,), daemon=True).start()


def send_webhook_event(url: str, event: dict) -> tuple[bool, str]:
    """Envía un evento a Apps Script y valida que la respuesta sea JSON con ok=true.
    Esto evita marcar como enviado cuando Google responde una página HTML de error/autorización.
    """
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=12) as resp:
        status = getattr(resp, "status", None) or resp.getcode()
        response_text = resp.read().decode("utf-8", errors="replace")

    if status < 200 or status >= 300:
        return False, f"HTTP {status}: {response_text[:300]}"

    try:
        parsed = json.loads(response_text)
    except Exception:
        return False, f"Respuesta no JSON desde Apps Script: {response_text[:300]}"

    if parsed.get("ok") is True:
        return True, response_text[:300]

    return False, f"Apps Script respondió ok=false: {response_text[:500]}"




def enqueue_backup_events_batch(events):
    """Inserta muchos eventos en la cola local y dispara un solo envío."""
    if not events:
        return
    now = datetime.now().isoformat(timespec="seconds")
    rows = [(et, json.dumps(payload, ensure_ascii=False, default=str), now) for et, payload in events]
    with db() as c:
        c.executemany(
            "INSERT INTO backup_queue (event_type, payload_json, status, attempts, created_at) VALUES (?, ?, 'pending', 0, ?)",
            rows,
        )
        c.commit()
    url = get_backup_webhook_url()
    if url:
        threading.Thread(target=flush_backup_queue, args=(url, 1000), daemon=True).start()


def get_backup_events_from_sheets():
    url = get_backup_webhook_url()
    if not url:
        return False, [], "No hay URL de respaldo configurada."
    sep = "&" if "?" in url else "?"
    read_url = f"{url}{sep}{urllib.parse.urlencode({'action': 'events'})}"
    try:
        with urllib.request.urlopen(read_url, timeout=20) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        data = json.loads(text)
        if data.get("ok") is not True:
            return False, [], f"Apps Script respondió error: {text[:500]}"
        return True, data.get("events") or [], f"Eventos leídos: {len(data.get('events') or [])}"
    except Exception as e:
        return False, [], f"No pude leer respaldo externo: {e}"


def local_lotes_count():
    with db() as c:
        row = c.execute("SELECT COUNT(*) AS n FROM lotes").fetchone()
    return int(row["n"] or 0) if row else 0


def restore_from_backup_if_empty():
    if local_lotes_count() > 0:
        return False, "Base local con datos; no se restaura."
    ok, events, msg = get_backup_events_from_sheets()
    if not ok:
        return False, msg
    if not events:
        return False, "No hay eventos en el respaldo externo."

    lotes = {}
    items_by_lote = {}
    deleted_lotes = set()
    movement_by_item = {}
    scan_rows = []

    for ev in events:
        et = clean_text(ev.get("event_type", ""))
        try:
            lote_id = int(ev.get("lote_id"))
        except Exception:
            continue

        if et == "lote_creado":
            lotes[lote_id] = {
                "id": lote_id,
                "nombre": clean_text(ev.get("lote_nombre", "")) or f"Lote {lote_id}",
                "archivo": clean_text(ev.get("archivo", "")),
                "hoja": clean_text(ev.get("hoja", "")),
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or datetime.now().isoformat(timespec="seconds"),
            }
        elif et == "lote_item":
            try:
                item_id = int(ev.get("item_id"))
            except Exception:
                continue
            items_by_lote.setdefault(lote_id, {})[item_id] = {
                "id": item_id,
                "lote_id": lote_id,
                "area": clean_text(ev.get("area", "")),
                "nro": clean_text(ev.get("nro", "")),
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion", "")),
                "unidades": to_int(ev.get("unidades", 0)),
                "acopiadas": 0,
                "identificacion": clean_text(ev.get("identificacion", "")),
                "vence": clean_text(ev.get("vence", "")),
                "dia": clean_text(ev.get("dia", "")),
                "hora": clean_text(ev.get("hora", "")),
                "created_at": clean_text(ev.get("item_created_at", "")) or clean_text(ev.get("created_at", "")) or datetime.now().isoformat(timespec="seconds"),
                "updated_at": clean_text(ev.get("item_updated_at", "")) or clean_text(ev.get("created_at", "")) or datetime.now().isoformat(timespec="seconds"),
            }
        elif et == "lote_snapshot_chunk":
            items = ev.get("items") or []
            for item_ev in items:
                try:
                    item_id = int(item_ev.get("item_id"))
                except Exception:
                    continue
                items_by_lote.setdefault(lote_id, {})[item_id] = {
                    "id": item_id,
                    "lote_id": lote_id,
                    "area": clean_text(item_ev.get("area", "")),
                    "nro": clean_text(item_ev.get("nro", "")),
                    "codigo_ml": norm_code(item_ev.get("codigo_ml", "")),
                    "codigo_universal": norm_code(item_ev.get("codigo_universal", "")),
                    "sku": norm_code(item_ev.get("sku", "")),
                    "descripcion": clean_text(item_ev.get("descripcion", "")),
                    "unidades": to_int(item_ev.get("unidades", 0)),
                    "acopiadas": 0,
                    "identificacion": clean_text(item_ev.get("identificacion", "")),
                    "vence": clean_text(item_ev.get("vence", "")),
                    "dia": clean_text(item_ev.get("dia", "")),
                    "hora": clean_text(item_ev.get("hora", "")),
                    "created_at": clean_text(item_ev.get("item_created_at", "")) or clean_text(ev.get("created_at", "")) or datetime.now().isoformat(timespec="seconds"),
                    "updated_at": clean_text(item_ev.get("item_updated_at", "")) or clean_text(ev.get("created_at", "")) or datetime.now().isoformat(timespec="seconds"),
                }
        elif et == "scan_agregado":
            try:
                item_id = int(ev.get("item_id"))
                qty = int(ev.get("cantidad") or 0)
            except Exception:
                continue
            movement_by_item[item_id] = movement_by_item.get(item_id, 0) + qty
            scan_rows.append((lote_id, item_id, norm_code(ev.get("scan_primario", "")), norm_code(ev.get("scan_secundario", "")), qty, clean_text(ev.get("modo", "")), clean_text(ev.get("created_at", "")) or datetime.now().isoformat(timespec="seconds")))
        elif et == "scan_deshacer":
            try:
                item_id = int(ev.get("item_id"))
                qty = int(ev.get("cantidad") or 0)
            except Exception:
                continue
            movement_by_item[item_id] = movement_by_item.get(item_id, 0) - qty
        elif et == "lote_eliminado":
            deleted_lotes.add(lote_id)

    active_lote_ids = [lid for lid in lotes if lid not in deleted_lotes and items_by_lote.get(lid)]
    if not active_lote_ids:
        return False, "No encontré lotes activos con snapshot completo en Sheets. Crea el lote una vez con esta nueva versión para activar restauración automática."

    now = datetime.now().isoformat(timespec="seconds")
    restored_lotes = 0
    restored_items = 0
    with db() as c:
        for lid in sorted(active_lote_ids):
            lote = lotes[lid]
            c.execute("INSERT OR REPLACE INTO lotes (id, nombre, archivo, hoja, created_at) VALUES (?, ?, ?, ?, ?)", (lote["id"], lote["nombre"], lote["archivo"], lote["hoja"], lote["created_at"]))
            restored_lotes += 1
            for item in items_by_lote[lid].values():
                qty = max(0, min(int(item["unidades"]), int(movement_by_item.get(int(item["id"]), 0))))
                item["acopiadas"] = qty
                item["updated_at"] = now if qty else item["updated_at"]
                c.execute("""
                    INSERT OR REPLACE INTO items
                    (id, lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades, acopiadas,
                     identificacion, vence, dia, hora, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (item["id"], item["lote_id"], item["area"], item["nro"], item["codigo_ml"], item["codigo_universal"], item["sku"], item["descripcion"], item["unidades"], item["acopiadas"], item["identificacion"], item["vence"], item["dia"], item["hora"], item["created_at"], item["updated_at"]))
                restored_items += 1
        for lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at in scan_rows:
            if lote_id in active_lote_ids and cantidad > 0:
                c.execute("INSERT INTO scans (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at))
        c.commit()
    return True, f"Restauración automática completa: {restored_lotes} lote(s), {restored_items} producto(s)."


def flush_backup_queue(webhook_url: str | None = None, limit: int = 25):
    url = clean_text(webhook_url or get_backup_webhook_url())
    if not url:
        return

    with db() as c:
        rows = c.execute(
            """
            SELECT id, event_type, payload_json, attempts, created_at
            FROM backup_queue
            WHERE status='pending'
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    for row in rows:
        event = {
            "event_type": row["event_type"],
            "queue_id": int(row["id"]),
            "queued_at": row["created_at"],
            **json.loads(row["payload_json"]),
        }
        try:
            ok, detail = send_webhook_event(url, event)
            if not ok:
                raise RuntimeError(detail)

            sent_at = datetime.now().isoformat(timespec="seconds")
            with db() as c:
                c.execute(
                    "UPDATE backup_queue SET status='sent', sent_at=?, last_error=NULL WHERE id=?",
                    (sent_at, int(row["id"])),
                )
                c.commit()

        except Exception as e:
            with db() as c:
                c.execute(
                    "UPDATE backup_queue SET attempts=attempts+1, last_error=? WHERE id=?",
                    (str(e)[:500], int(row["id"])),
                )
                c.commit()


def backup_status():
    with db() as c:
        row = c.execute(
            """
            SELECT
                SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent,
                MAX(sent_at) AS last_sent,
                MAX(last_error) AS last_error
            FROM backup_queue
            """
        ).fetchone()
    return dict(row) if row else {"pending": 0, "sent": 0, "last_sent": "", "last_error": ""}


def test_backup_webhook() -> tuple[bool, str]:
    url = get_backup_webhook_url()
    if not url:
        return False, "No hay SHEETS_WEBHOOK_URL configurada."
    event = {
        "event_type": "test_webhook",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "lote_id": "TEST",
        "lote_nombre": "Prueba manual desde Streamlit",
        "archivo": "test",
        "hoja": "test",
        "item_id": "",
        "sku": "TEST-SKU",
        "codigo_ml": "TEST-ML",
        "codigo_universal": "TEST-EAN",
        "descripcion": "Evento de prueba de respaldo externo",
        "cantidad": 1,
        "modo": "TEST",
        "scan_primario": "TEST",
        "scan_secundario": "TEST",
        "operador": "",
        "dispositivo": "",
    }
    return send_webhook_event(url, event)


def build_lote_payload(lote_id: int) -> dict:
    lote = get_lote(lote_id)
    return {
        "lote_id": lote_id,
        "lote_nombre": clean_text(lote.get("nombre", "")),
        "archivo": clean_text(lote.get("archivo", "")),
        "hoja": clean_text(lote.get("hoja", "")),
    }



def list_lotes():
    with db() as c:
        return pd.read_sql_query("""
            SELECT l.id, l.nombre, l.archivo, l.hoja, l.created_at,
                   COALESCE(SUM(i.unidades), 0) unidades,
                   COALESCE(SUM(i.acopiadas), 0) acopiadas,
                   COUNT(i.id) lineas
            FROM lotes l
            LEFT JOIN items i ON i.lote_id = l.id
            GROUP BY l.id
            ORDER BY l.id DESC
        """, c)


def get_lote(lote_id):
    with db() as c:
        row = c.execute("SELECT * FROM lotes WHERE id=?", (lote_id,)).fetchone()
    return dict(row) if row else {}


def get_items(lote_id):
    with db() as c:
        return pd.read_sql_query(
            "SELECT * FROM items WHERE lote_id=? ORDER BY area, CAST(nro AS INTEGER), id",
            c,
            params=(lote_id,),
        )


def get_last_scans(lote_id):
    with db() as c:
        return pd.read_sql_query("""
            SELECT item_id, MAX(created_at) procesado_at, SUM(cantidad) escaneado_total
            FROM scans
            WHERE lote_id=?
            GROUP BY item_id
        """, c, params=(lote_id,))


def create_lote(nombre, archivo, hoja, df):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        cur = c.execute(
            "INSERT INTO lotes (nombre, archivo, hoja, created_at) VALUES (?, ?, ?, ?)",
            (nombre, archivo, hoja, now),
        )
        lote_id = cur.lastrowid
        rows = []
        for r in df.itertuples(index=False):
            rows.append((
                lote_id,
                clean_text(r.area),
                clean_text(r.nro),
                norm_code(r.codigo_ml),
                norm_code(r.codigo_universal),
                norm_code(r.sku),
                clean_text(r.descripcion),
                int(r.unidades),
                0,
                clean_text(r.identificacion),
                clean_text(r.vence),
                clean_text(r.dia),
                clean_text(r.hora),
                now,
                now,
            ))
        c.executemany("""
            INSERT INTO items
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades, acopiadas,
             identificacion, vence, dia, hora, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        c.commit()

    lote_payload = build_lote_payload(lote_id)
    inserted = get_items(lote_id)

    snapshot_items = []
    for r in inserted.itertuples(index=False):
        snapshot_items.append({
            "item_id": int(r.id),
            "area": clean_text(r.area),
            "nro": clean_text(r.nro),
            "codigo_ml": norm_code(r.codigo_ml),
            "codigo_universal": norm_code(r.codigo_universal),
            "sku": norm_code(r.sku),
            "descripcion": clean_text(r.descripcion),
            "unidades": int(r.unidades),
            "identificacion": clean_text(r.identificacion),
            "vence": clean_text(r.vence),
            "dia": clean_text(r.dia),
            "hora": clean_text(r.hora),
            "item_created_at": clean_text(r.created_at),
            "item_updated_at": clean_text(r.updated_at),
        })

    events = [("lote_creado", {
        **lote_payload,
        "created_at": now,
        "total_lineas": int(len(df)),
        "total_unidades": int(df["unidades"].sum()) if "unidades" in df.columns else 0,
        "snapshot_mode": "lote_item",
    })]

    # Respaldo de snapshot producto a producto.
    # Esto es más largo en Sheets, pero es mucho más seguro y fácil de auditar/restaurar.
    for item in snapshot_items:
        events.append(("lote_item", {
            **lote_payload,
            "created_at": now,
            **item,
        }))

    enqueue_backup_events_batch(events)
    flush_backup_queue(limit=max(1000, len(events) + 10))
    return lote_id

def delete_lote(lote_id):
    lote_payload = build_lote_payload(lote_id)
    items_count = len(get_items(lote_id))
    with db() as c:
        c.execute("DELETE FROM scans WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM items WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM lotes WHERE id=?", (lote_id,))
        c.commit()

    enqueue_backup_event("lote_eliminado", {
        **lote_payload,
        "items_eliminados": int(items_count),
        "deleted_at": datetime.now().isoformat(timespec="seconds"),
    })


def add_acopio(lote_id, item_id, cantidad, scan_primario, scan_secundario, modo):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        item = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (item_id, lote_id)).fetchone()
        if not item:
            return False, "Producto no encontrado."
        pendiente = int(item["unidades"]) - int(item["acopiadas"])
        if pendiente <= 0:
            return False, "Este producto ya está completo."
        if cantidad <= 0:
            return False, "La cantidad debe ser mayor a cero."
        if cantidad > pendiente:
            return False, f"No puedes agregar {cantidad}. Solo quedan {pendiente} pendientes."
        c.execute("UPDATE items SET acopiadas=acopiadas+?, updated_at=? WHERE id=?", (cantidad, now, item_id))
        c.execute("""
            INSERT INTO scans (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (lote_id, item_id, norm_code(scan_primario), norm_code(scan_secundario), cantidad, modo, now))
        c.commit()

    enqueue_backup_event("scan_agregado", {
        **build_lote_payload(lote_id),
        "item_id": int(item_id),
        "sku": clean_text(item["sku"]),
        "codigo_ml": clean_text(item["codigo_ml"]),
        "codigo_universal": clean_text(item["codigo_universal"]),
        "descripcion": clean_text(item["descripcion"]),
        "cantidad": int(cantidad),
        "modo": clean_text(modo),
        "scan_primario": norm_code(scan_primario),
        "scan_secundario": norm_code(scan_secundario),
        "created_at": now,
    })
    return True, "Cantidad agregada."


def undo_last_scan(lote_id):
    with db() as c:
        row = c.execute("SELECT * FROM scans WHERE lote_id=? ORDER BY id DESC LIMIT 1", (lote_id,)).fetchone()
        if not row:
            return False, "No hay escaneos para deshacer."
        now = datetime.now().isoformat(timespec="seconds")
        item = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (int(row["item_id"]), lote_id)).fetchone()
        c.execute("UPDATE items SET acopiadas=MAX(acopiadas-?,0), updated_at=? WHERE id=?", (int(row["cantidad"]), now, int(row["item_id"])))
        c.execute("DELETE FROM scans WHERE id=?", (int(row["id"]),))
        c.commit()

    item_payload = dict(item) if item else {}
    enqueue_backup_event("scan_deshacer", {
        **build_lote_payload(lote_id),
        "item_id": int(row["item_id"]),
        "sku": clean_text(item_payload.get("sku", "")),
        "codigo_ml": clean_text(item_payload.get("codigo_ml", "")),
        "codigo_universal": clean_text(item_payload.get("codigo_universal", "")),
        "descripcion": clean_text(item_payload.get("descripcion", "")),
        "cantidad": int(row["cantidad"]),
        "modo": clean_text(row["modo"]),
        "scan_primario": norm_code(row["scan_primario"]),
        "scan_secundario": norm_code(row["scan_secundario"]),
        "created_at": now,
    })
    return True, "Último escaneo deshecho."


# ============================================================
# Lectura Excel: UNA hoja por lote, sin mezclar formatos históricos
# ============================================================

def sheet_names(uploaded_file):
    xls = pd.ExcelFile(uploaded_file)
    return xls.sheet_names


def read_full_excel_sheet(uploaded_file, sheet_name):
    raw = pd.read_excel(uploaded_file, sheet_name=sheet_name, dtype=object)
    raw = raw.dropna(how="all")
    if raw.empty:
        return pd.DataFrame(), ["La hoja seleccionada está vacía."]

    raw.columns = [clean_text(c) for c in raw.columns]
    cols = list(raw.columns)

    warnings = []

    area_col = col_exact(cols, ["Area.", "Area", "AREA"])
    nro_col = col_exact(cols, ["Nº", "N°", "n°", "NRO", "Numero", "Número"])
    codigo_ml_col = col_required(cols, "Código ML", ["Código ML", "Codigo ML", "CODIGO ML", "COD ML", "Cod ML"])
    codigo_universal_col = col_exact(cols, ["Código Universal", "Codigo Universal", "COD UNIVERSAL", "Codigo de barras", "EAN"])
    sku_col = col_required(cols, "SKU", ["SKU", "SKU ML"])
    descripcion_col = col_required(cols, "Descripción", ["Descripción", "Descripcion", "DESCRIPCION", "Producto", "Título", "Titulo"])
    unidades_col = col_required(cols, "Unidades", ["Unidades", "CANT", "Cant", "Cantidad"])

    # Separación estricta: Identificación y Vence son columnas independientes.
    identificacion_col = col_exact(cols, ["Identificación", "Identificacion", "ETIQUETA", "ETIQ"])
    vence_col = col_exact(cols, ["Vence", "VCTO", "Vencimiento", "Fecha vencimiento", "Fecha de vencimiento"])
    dia_col = col_exact(cols, ["Dia", "Día"])
    hora_col = col_exact(cols, ["Hora"])

    if not identificacion_col:
        warnings.append("No encontré columna de Identificación/ETIQUETA/ETIQ en esta hoja. Se cargará vacía.")
    if not vence_col:
        warnings.append("No encontré columna Vence/VCTO en esta hoja. Se cargará vacía.")

    df = pd.DataFrame({
        "area": raw[area_col] if area_col else "",
        "nro": raw[nro_col] if nro_col else "",
        "codigo_ml": raw[codigo_ml_col],
        "codigo_universal": raw[codigo_universal_col] if codigo_universal_col else "",
        "sku": raw[sku_col],
        "descripcion": raw[descripcion_col],
        "unidades": raw[unidades_col],
        "identificacion": raw[identificacion_col] if identificacion_col else "",
        "vence": raw[vence_col] if vence_col else "",
        "dia": raw[dia_col] if dia_col else "",
        "hora": raw[hora_col] if hora_col else "",
    })

    for k in ["area", "nro", "descripcion", "identificacion", "vence", "dia", "hora"]:
        df[k] = df[k].map(clean_text)
    for k in ["codigo_ml", "codigo_universal", "sku"]:
        df[k] = df[k].map(norm_code)
    df["unidades"] = df["unidades"].map(to_int)

    df = df[(df["unidades"] > 0) & ((df["sku"] != "") | (df["codigo_ml"] != "") | (df["codigo_universal"] != ""))]
    return df.reset_index(drop=True), warnings


# ============================================================
# Maestro SKU/EAN desde repo
# ============================================================

def parse_maestro(file_or_path):
    if not Path(file_or_path).exists():
        return pd.DataFrame(columns=["code", "sku", "descripcion"])
    xls = pd.ExcelFile(file_or_path)
    frames = []
    for sh in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sh, dtype=object).dropna(how="all")
        if raw.empty:
            continue
        raw.columns = [clean_text(c) for c in raw.columns]
        cols = list(raw.columns)
        sku_col = col_exact(cols, ["SKU", "SKU ML", "sku_ml"])
        desc_col = col_exact(cols, ["Descripción", "Descripcion", "Producto", "Title", "Titulo"])
        if not sku_col:
            continue
        barcode_cols = []
        for c in cols:
            h = normalize_header(c)
            if any(x in h for x in ["ean", "barra", "barcode", "codigo universal", "cod universal", "codigo de barras"]):
                barcode_cols.append(c)
        if sku_col not in barcode_cols:
            barcode_cols.append(sku_col)
        rows = []
        for _, r in raw.iterrows():
            sku = norm_code(r.get(sku_col, ""))
            if not sku:
                continue
            desc = clean_text(r.get(desc_col, "")) if desc_col else ""
            codes = {sku}
            for bc in barcode_cols:
                for code in split_codes(r.get(bc, "")):
                    codes.add(code)
            for code in codes:
                rows.append({"code": code, "sku": sku, "descripcion": desc})
        if rows:
            frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame(columns=["code", "sku", "descripcion"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["code"])


def load_maestro_from_repo():
    df = parse_maestro(MAESTRO_PATH)
    if df.empty:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        c.execute("DELETE FROM maestro")
        c.executemany("INSERT OR REPLACE INTO maestro (code, sku, descripcion, updated_at) VALUES (?, ?, ?, ?)",
                      [(norm_code(r.code), norm_code(r.sku), clean_text(r.descripcion), now) for r in df.itertuples(index=False)])
        c.commit()
    return len(df)


def maestro_lookup(code):
    cn = norm_code(code)
    if not cn:
        return ""
    with db() as c:
        row = c.execute("SELECT sku FROM maestro WHERE code=?", (cn,)).fetchone()
    return clean_text(row["sku"]) if row else ""


# ============================================================
# Matching
# ============================================================

def pending_items(items):
    if items.empty:
        return items
    p = items.copy()
    p["pendiente"] = (p["unidades"].astype(int) - p["acopiadas"].astype(int)).clip(lower=0)
    return p[p["pendiente"] > 0]


def match_ml(items, code):
    cn = norm_code(code)
    p = pending_items(items)
    return p[p["codigo_ml"].map(norm_code) == cn] if cn else p.iloc[0:0]


def match_secondary(items, code, only_super=None):
    cn = norm_code(code)
    if not cn:
        return items.iloc[0:0]
    sku_master = norm_code(maestro_lookup(cn))
    p = pending_items(items)
    if only_super is True:
        p = p[p["identificacion"].map(is_supermercado)]
    elif only_super is False:
        p = p[~p["identificacion"].map(is_supermercado)]
    mask = (p["sku"].map(norm_code) == cn) | (p["codigo_universal"].map(norm_code) == cn)
    if sku_master:
        mask = mask | (p["sku"].map(norm_code) == sku_master)
    return p[mask]


def best_match(df):
    if df.empty:
        return None
    m = df.copy()
    m["pendiente"] = (m["unidades"].astype(int) - m["acopiadas"].astype(int)).clip(lower=0)
    return m.sort_values(["pendiente", "id"], ascending=[False, True]).iloc[0]


def reset_scan_state():
    """Limpia el flujo de escaneo sin modificar directamente widgets ya creados."""
    st.session_state["primary_validated"] = False
    st.session_state["primary_code"] = ""
    st.session_state["candidate_id"] = None
    st.session_state["candidate_mode"] = ""
    st.session_state["_clear_scan_inputs_next_run"] = True


def clear_scan_inputs_if_needed():
    """Se ejecuta antes de crear los inputs de escaneo/cantidad."""
    if st.session_state.get("_clear_scan_inputs_next_run", False):
        st.session_state["scan_primary"] = ""
        st.session_state["scan_secondary"] = ""
        st.session_state["scan_qty_input"] = ""
        st.session_state["_clear_scan_inputs_next_run"] = False


def get_item_row(items, item_id):
    try:
        iid = int(item_id)
    except Exception:
        return None
    m = items[items["id"].astype(int) == iid]
    return None if m.empty else m.iloc[0]


# ============================================================
# Etiquetas Zebra ZPL 50x30 mm (módulo independiente)
# ============================================================

ROLL_CAPACITY_DEFAULT = 2500
LABEL_SEPARATOR_PER_PRODUCT = 2  # INICIO + FIN


def zpl_safe(v) -> str:
    """Limpia texto para ZPL evitando caracteres que suelen romper impresión."""
    s = clean_text(v)
    repl = {
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ü": "U", "Ñ": "N",
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n",
        "^": "", "~": "", "\n": " ", "\r": " ",
    }
    for a, b in repl.items():
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s).strip()


def split_desc_2_lines(desc: str, max_len: int = 34) -> tuple[str, str]:
    text = zpl_safe(desc)
    if len(text) <= max_len:
        return text, ""
    cut = text.rfind(" ", 0, max_len + 1)
    if cut < 12:
        cut = max_len
    line1 = text[:cut].strip()
    rest = text[cut:].strip()
    if len(rest) <= max_len:
        return line1, rest
    cut2 = rest.rfind(" ", 0, max_len + 1)
    if cut2 < 12:
        cut2 = max_len
    return line1, rest[:cut2].strip()


def zpl_ml_label_50x30(codigo_ml, sku, descripcion, copies=1) -> str:
    codigo = zpl_safe(codigo_ml)
    sku = zpl_safe(sku)
    line1, line2 = split_desc_2_lines(descripcion, 34)
    copies = max(1, int(copies or 1))
    return f"""^XA
^PW400
^LL240
^LH0,0
^PQ{copies}

^FO15,12^BY2,2,55
^BCN,55,N,N,N
^FD{codigo}^FS

^FO120,78^A0N,28,28
^FD{codigo}^FS

^FO15,118^A0N,21,21
^FD{line1}^FS

^FO15,145^A0N,21,21
^FD{line2}^FS

^FO15,195^A0N,25,25
^FDSKU: {sku}^FS

^XZ
"""


def zpl_separator_50x30(tipo: str, codigo_ml, sku, descripcion) -> str:
    tipo = "INICIO" if clean_text(tipo).upper() == "INICIO" else "FIN"
    codigo = zpl_safe(codigo_ml)
    sku = zpl_safe(sku)
    line1, line2 = split_desc_2_lines(descripcion, 28)
    return f"""^XA
^PW400
^LL240
^LH0,0

^FO25,20^A0N,44,44
^FD{tipo} PRODUCTO^FS

^FO25,78^A0N,32,32
^FD{codigo}^FS

^FO25,118^A0N,22,22
^FD{line1}^FS
^FO25,145^A0N,22,22
^FD{line2}^FS

^FO25,190^A0N,26,26
^FDSKU: {sku}^FS

^XZ
"""


def zpl_for_item_with_separators(row, copies=None) -> str:
    qty = int(copies if copies is not None else row.get("unidades", 0))
    qty = max(1, qty)
    return (
        zpl_separator_50x30("INICIO", row.get("codigo_ml", ""), row.get("sku", ""), row.get("descripcion", ""))
        + zpl_ml_label_50x30(row.get("codigo_ml", ""), row.get("sku", ""), row.get("descripcion", ""), qty)
        + zpl_separator_50x30("FIN", row.get("codigo_ml", ""), row.get("sku", ""), row.get("descripcion", ""))
    )


def get_label_print_summary(lote_id: int) -> pd.DataFrame:
    with db() as c:
        df = pd.read_sql_query(
            """
            SELECT item_id,
                   SUM(CASE WHEN print_kind='NORMAL' THEN cantidad ELSE 0 END) AS printed_normal,
                   SUM(CASE WHEN print_kind!='NORMAL' THEN cantidad ELSE 0 END) AS printed_separators,
                   SUM(CASE WHEN is_reprint=1 THEN cantidad ELSE 0 END) AS reprinted_qty,
                   MAX(created_at) AS last_label_printed_at
            FROM label_prints
            WHERE lote_id=?
            GROUP BY item_id
            """,
            c,
            params=(lote_id,),
        )
    if df.empty:
        return pd.DataFrame(columns=["item_id", "printed_normal", "printed_separators", "reprinted_qty", "last_label_printed_at"])
    for col in ["printed_normal", "printed_separators", "reprinted_qty"]:
        df[col] = df[col].fillna(0).astype(int)
    return df


def label_control_view(lote_id: int) -> pd.DataFrame:
    items = get_items(lote_id)
    if items.empty:
        return items
    summary = get_label_print_summary(lote_id)
    view = items.merge(summary, left_on="id", right_on="item_id", how="left")
    for col in ["printed_normal", "printed_separators", "reprinted_qty"]:
        view[col] = view[col].fillna(0).astype(int)
    view["label_pending"] = (view["unidades"].astype(int) - view["printed_normal"].astype(int)).clip(lower=0)

    def status_row(r):
        req = int(r["unidades"])
        printed = int(r["printed_normal"])
        if printed == 0:
            return "SIN IMPRIMIR"
        if printed < req:
            return "PARCIAL"
        if printed == req:
            return "COMPLETO"
        return "SOBREIMPRESO"

    view["label_status"] = view.apply(status_row, axis=1)
    return view


def item_label_total(row) -> int:
    return int(row.get("unidades", 0)) + LABEL_SEPARATOR_PER_PRODUCT


def build_label_blocks(items: pd.DataFrame, capacity: int = ROLL_CAPACITY_DEFAULT) -> list[dict]:
    blocks = []
    current = []
    current_total = 0
    capacity = max(1, int(capacity or ROLL_CAPACITY_DEFAULT))

    for _, row in items.iterrows():
        qty = item_label_total(row)
        # Si un solo producto excede el rollo, se deja solo en un bloque y se advierte en UI.
        if current and current_total + qty > capacity:
            blocks.append({"items": current, "total_qty": current_total})
            current = []
            current_total = 0
        current.append(row.to_dict())
        current_total += qty

    if current:
        blocks.append({"items": current, "total_qty": current_total})

    out = []
    for idx, b in enumerate(blocks, start=1):
        normal = sum(int(x.get("unidades", 0)) for x in b["items"])
        separators = len(b["items"]) * LABEL_SEPARATOR_PER_PRODUCT
        key_raw = "|".join(f"{int(x.get('id'))}:{int(x.get('unidades',0))}" for x in b["items"])
        block_key = hashlib.sha1(key_raw.encode("utf-8")).hexdigest()[:16]
        out.append({
            "block_index": idx,
            "block_key": block_key,
            "items": b["items"],
            "products_count": len(b["items"]),
            "normal_qty": normal,
            "separator_qty": separators,
            "total_qty": normal + separators,
            "over_capacity": (normal + separators) > capacity,
        })
    return out


def zpl_for_block(block: dict) -> str:
    chunks = []
    for item in block["items"]:
        chunks.append(zpl_for_item_with_separators(item, int(item.get("unidades", 0))))
    return "".join(chunks)


def get_label_block_record(lote_id: int, block_index: int, block_key: str) -> dict:
    with db() as c:
        row = c.execute(
            "SELECT * FROM label_blocks WHERE lote_id=? AND block_index=? AND block_key=?",
            (int(lote_id), int(block_index), clean_text(block_key)),
        ).fetchone()
    return dict(row) if row else {}


def register_block_download(lote_id: int, block: dict):
    now = datetime.now().isoformat(timespec="seconds")
    existing = get_label_block_record(lote_id, block["block_index"], block["block_key"])
    is_reprint = 1 if existing else 0
    status = "REIMPRESO" if is_reprint else "IMPRESO"

    with db() as c:
        if existing:
            c.execute(
                """
                UPDATE label_blocks
                SET status=?, download_count=download_count+1, last_printed_at=?, updated_at=?
                WHERE lote_id=? AND block_index=? AND block_key=?
                """,
                (status, now, now, int(lote_id), int(block["block_index"]), clean_text(block["block_key"])),
            )
        else:
            c.execute(
                """
                INSERT INTO label_blocks
                (lote_id, block_index, block_key, products_count, normal_qty, separator_qty, total_qty,
                 status, download_count, last_printed_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'IMPRESO', 1, ?, ?, ?)
                """,
                (
                    int(lote_id), int(block["block_index"]), clean_text(block["block_key"]), int(block["products_count"]),
                    int(block["normal_qty"]), int(block["separator_qty"]), int(block["total_qty"]), now, now, now,
                ),
            )
        rows = []
        for item in block["items"]:
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                clean_text(item.get("descripcion", "")), int(item.get("unidades", 0)), "BLOQUE", "NORMAL",
                int(block["block_index"]), clean_text(block["block_key"]), is_reprint, now,
            ))
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                clean_text(item.get("descripcion", "")), LABEL_SEPARATOR_PER_PRODUCT, "BLOQUE", "SEPARADOR",
                int(block["block_index"]), clean_text(block["block_key"]), is_reprint, now,
            ))
        c.executemany(
            """
            INSERT INTO label_prints
            (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
             block_index, block_key, is_reprint, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()


def register_individual_download(lote_id: int, item: dict, qty: int):
    now = datetime.now().isoformat(timespec="seconds")
    qty = max(1, int(qty or 1))
    summary = get_label_print_summary(lote_id)
    already = 0
    if not summary.empty:
        m = summary[summary["item_id"].astype(int) == int(item.get("id"))]
        if not m.empty:
            already = int(m.iloc[0].get("printed_normal", 0))
    is_reprint = 1 if already >= int(item.get("unidades", 0)) else 0
    with db() as c:
        rows = [
            (int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
             clean_text(item.get("descripcion", "")), qty, "INDIVIDUAL", "NORMAL", None, None, is_reprint, now),
            (int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
             clean_text(item.get("descripcion", "")), LABEL_SEPARATOR_PER_PRODUCT, "INDIVIDUAL", "SEPARADOR", None, None, is_reprint, now),
        ]
        c.executemany(
            """
            INSERT INTO label_prints
            (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
             block_index, block_key, is_reprint, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()

# ============================================================
# Exportación
# ============================================================

def export_lote(lote_id):
    items = get_items(lote_id)
    if not items.empty:
        items["pendiente"] = (items["unidades"].astype(int) - items["acopiadas"].astype(int)).clip(lower=0)
        items["estado"] = items["pendiente"].apply(lambda x: "COMPLETO" if int(x) == 0 else "PENDIENTE")
    scans = pd.DataFrame()
    with db() as c:
        scans = pd.read_sql_query("SELECT created_at, item_id, scan_primario, scan_secundario, cantidad, modo FROM scans WHERE lote_id=? ORDER BY id DESC", c, params=(lote_id,))
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        items.to_excel(writer, sheet_name="control_full", index=False)
        scans.to_excel(writer, sheet_name="escaneos", index=False)
    return out.getvalue()


# ============================================================
# UI
# ============================================================

init_db()
load_maestro_from_repo()

if "_auto_restore_checked" not in st.session_state:
    st.session_state["_auto_restore_checked"] = True
    restored, restore_msg = restore_from_backup_if_empty()
    st.session_state["_auto_restore_msg"] = restore_msg
    st.session_state["_auto_restore_ok"] = restored

st.markdown("""
<style>
/* Estilo general: control y carga mantienen tamaño normal para no desproporcionar la UI */
.stButton > button {font-weight:800!important;}
div[data-testid="stMetricValue"] {font-size:1.8rem!important;}
.product-title {font-size:1.3rem;font-weight:850;line-height:1.25;margin:8px 0;}
.control-card {border:1px solid #E5E7EB;border-radius:16px;padding:15px 17px;margin:12px 0;background:#FFF;}
.control-title {font-size:1.05rem;font-weight:850;line-height:1.35;margin-bottom:8px;}
.control-meta {font-size:.92rem;color:#374151;margin-bottom:8px;}
.badge {display:inline-block;padding:6px 10px;border-radius:999px;background:#F3F4F6;margin:3px 4px 3px 0;font-size:.92rem;font-weight:750;}
.badge-alert {background:#FFF7ED;}
.label-card {border:1px solid #D1D5DB;border-radius:16px;padding:16px;margin:12px 0;background:#FFFFFF;}
.label-card-printed {border-color:#86EFAC;background:#F0FDF4;}
.label-card-warn {border-color:#FDBA74;background:#FFF7ED;}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("Menú")
    page = st.radio("Vista", ["Escaneo", "Cargar lote FULL", "Control", "Etiquetas"], label_visibility="collapsed")
    st.divider()
    lotes = list_lotes()
    if lotes.empty:
        active_lote = None
        st.info("Sin lotes creados.")
    else:
        options = {f"{r.nombre} · {int(r.acopiadas)}/{int(r.unidades)}": int(r.id) for r in lotes.itertuples(index=False)}
        active_lote = options[st.selectbox("Lote activo", list(options.keys()))]

    st.divider()
    bs = backup_status()
    pending_backup = int(bs.get("pending") or 0)
    sent_backup = int(bs.get("sent") or 0)
    if pending_backup:
        st.warning(f"Respaldo externo: {pending_backup} eventos pendientes")
        if bs.get("last_error"):
            st.caption(f"Último error: {clean_text(bs.get('last_error'))[:180]}")
        if st.button("Reintentar respaldo"):
            flush_backup_queue(limit=100)
            st.rerun()
    else:
        st.success(f"Respaldo externo activo · enviados: {sent_backup}")
    if bs.get("last_sent"):
        st.caption(f"Último respaldo: {fmt_dt(bs.get('last_sent'))}")
    if st.session_state.get("_auto_restore_msg"):
        if st.session_state.get("_auto_restore_ok"):
            st.success(st.session_state.get("_auto_restore_msg"))
        else:
            st.caption(f"Restauración: {st.session_state.get('_auto_restore_msg')}")
    if st.button("Restaurar desde Sheets"):
        if local_lotes_count() > 0:
            st.warning("Ya hay lotes en la base local.")
        else:
            ok_restore, msg_restore = restore_from_backup_if_empty()
            st.session_state["_auto_restore_ok"] = ok_restore
            st.session_state["_auto_restore_msg"] = msg_restore
            if ok_restore:
                st.success(msg_restore)
                st.rerun()
            else:
                st.error(msg_restore)
    if st.button("Probar respaldo Sheets"):
        ok_test, detail_test = test_backup_webhook()
        if ok_test:
            st.success("Prueba enviada a Google Sheets.")
        else:
            st.error(f"Falló prueba Sheets: {detail_test[:250]}")

if page == "Cargar lote FULL":
    st.subheader("Cargar lote FULL")
    full_file = st.file_uploader("Excel FULL", type=["xlsx"])
    if full_file:
        names = sheet_names(full_file)
        default_idx = len(names) - 1 if names else 0
        selected_sheet = st.selectbox("Hoja a cargar", names, index=default_idx)
        try:
            df, warns = read_full_excel_sheet(full_file, selected_sheet)
            for w in warns:
                st.warning(w)
            if df.empty:
                st.error("No se encontraron productos válidos en la hoja seleccionada.")
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Hoja", selected_sheet)
                c2.metric("Líneas", len(df))
                c3.metric("Unidades", int(df["unidades"].sum()))
                c4.metric("SKUs únicos", int(df["sku"].nunique()))
                with st.expander("Revisión rápida de columnas leídas", expanded=True):
                    st.dataframe(df[["codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence"]].head(20), use_container_width=True, hide_index=True)
                nombre = st.text_input("Nombre del lote", value=f"{selected_sheet} {datetime.now().strftime('%d-%m-%Y %H:%M')}")
                if st.button("Crear lote", type="primary"):
                    create_lote(nombre, full_file.name, selected_sheet, df)
                    reset_scan_state()
                    st.success("Lote creado correctamente.")
                    st.rerun()
        except Exception as e:
            st.error(f"No pude leer la hoja seleccionada: {e}")

elif page == "Escaneo":
    st.markdown("""
    <style>
    /* Escaneo PDA: visión grande para operación en piso */
    div[data-testid="stTextInput"] label,
    div[data-testid="stNumberInput"] label {
        font-size:1.85rem!important;
        font-weight:900!important;
        margin-bottom:.35rem!important;
    }
    div[data-testid="stTextInput"] input,
    div[data-testid="stNumberInput"] input {
        font-size:2.35rem!important;
        min-height:4.8rem!important;
        font-weight:800!important;
    }
    .stButton > button {
        font-size:1.75rem!important;
        min-height:4.5rem!important;
        width:100%;
        font-weight:900!important;
        border-radius:14px!important;
    }
    div[data-testid="stMetricLabel"] {font-size:1.35rem!important;font-weight:800!important;}
    div[data-testid="stMetricValue"] {font-size:2.35rem!important;font-weight:900!important;}
    .product-title {font-size:1.8rem!important;font-weight:900!important;line-height:1.25;margin:12px 0;}
    div[data-testid="stAlert"] {font-size:1.35rem!important;font-weight:800!important;}
    </style>
    """, unsafe_allow_html=True)
    if not active_lote:
        st.warning("Primero crea un lote FULL.")
    else:
        items = get_items(active_lote)
        total = int(items["unidades"].sum()) if not items.empty else 0
        done = int(items["acopiadas"].sum()) if not items.empty else 0
        st.progress(done / total if total else 0)
        a, b, c = st.columns(3)
        a.metric("Solicitado", total)
        b.metric("Acopiado", done)
        c.metric("Pendiente", max(total - done, 0))
        st.divider()

        for k, v in {"primary_validated": False, "primary_code": "", "candidate_id": None, "candidate_mode": "", "_clear_scan_inputs_next_run": False}.items():
            if k not in st.session_state:
                st.session_state[k] = v

        clear_scan_inputs_if_needed()

        st.text_input("Código ML o EAN supermercado", key="scan_primary")
        cv, cl = st.columns([2, 1])
        with cv:
            validar_primario = st.button("Validar código", type="primary")
        with cl:
            limpiar = st.button("Limpiar")
        if limpiar:
            reset_scan_state(); st.rerun()

        if validar_primario:
            st.session_state["candidate_id"] = None
            st.session_state["candidate_mode"] = ""
            st.session_state["primary_validated"] = False
            st.session_state["primary_code"] = norm_code(st.session_state.get("scan_primary", ""))
            st.session_state["scan_secondary"] = ""
            code = st.session_state["primary_code"]
            if not code:
                st.error("Escanea o ingresa un código.")
            else:
                sm = match_secondary(items, code, only_super=True)
                if not sm.empty:
                    cand = best_match(sm)
                    st.session_state["candidate_id"] = int(cand["id"])
                    st.session_state["candidate_mode"] = "SUPERMERCADO"
                    st.session_state["primary_validated"] = True
                else:
                    m1 = match_ml(items, code)
                    if m1.empty:
                        st.error("Código no encontrado en productos pendientes.")
                    elif m1["identificacion"].map(is_supermercado).all():
                        st.error("Este producto es SUPERMERCADO. Debe confirmarse escaneando SKU/EAN/Código Universal, no Código ML.")
                    else:
                        st.session_state["primary_validated"] = True

        candidate = None
        modo = st.session_state.get("candidate_mode", "")
        candidate_from_preview_this_run = False

        if st.session_state.get("candidate_id"):
            candidate = get_item_row(items, st.session_state["candidate_id"])
        elif st.session_state.get("primary_validated") and st.session_state.get("primary_code"):
            m1 = match_ml(items, st.session_state["primary_code"])
            m1 = m1[~m1["identificacion"].map(is_supermercado)]
            preview = best_match(m1)
            if preview is not None:
                pendiente_preview = int(preview["unidades"]) - int(preview["acopiadas"])
                st.markdown(f"<div class='product-title'>{esc(preview['descripcion'])}</div>", unsafe_allow_html=True)
                q1, q2, q3 = st.columns(3)
                q1.metric("Solicitadas", int(preview["unidades"]))
                q2.metric("Acopiadas", int(preview["acopiadas"]))
                q3.metric("Pendientes", max(pendiente_preview, 0))
                st.text_input("SKU / EAN / Código Universal", key="scan_secondary")
                b1, b2 = st.columns(2)
                with b1:
                    validar_sec = st.button("Validar SKU/EAN", type="primary")
                with b2:
                    sin_ean = st.button("Sin EAN")

                if sin_ean:
                    m_no_super = m1[~m1["identificacion"].map(is_supermercado)]
                    if m_no_super.empty:
                        st.error("No encontré ese Código ML pendiente para usar Sin EAN.")
                    else:
                        cand = best_match(m_no_super)
                        st.session_state["candidate_id"] = int(cand["id"])
                        st.session_state["candidate_mode"] = "SIN_EAN"
                        candidate = cand
                        modo = "SIN_EAN"
                        candidate_from_preview_this_run = True

                if validar_sec and candidate is None:
                    sec = st.session_state.get("scan_secondary", "")
                    if not norm_code(sec):
                        st.error("Escanea o ingresa el SKU/EAN.")
                    else:
                        m2 = match_secondary(m1, sec, only_super=False)
                        if m2.empty:
                            st.error("El SKU/EAN/Código Universal no corresponde a este producto.")
                        else:
                            cand = best_match(m2)
                            st.session_state["candidate_id"] = int(cand["id"])
                            st.session_state["candidate_mode"] = "ML+SECUNDARIO"
                            candidate = cand
                            modo = "ML+SECUNDARIO"
                            candidate_from_preview_this_run = True

        if candidate is not None:
            pendiente = int(candidate["unidades"]) - int(candidate["acopiadas"])
            st.success("Producto validado")

            # Si el producto se acaba de validar en esta misma corrida, ya mostramos arriba
            # nombre y cantidades. No los duplicamos para evitar parpadeos y confusión en PDA.
            if not candidate_from_preview_this_run:
                st.markdown(f"<div class='product-title'>{esc(candidate['descripcion'])}</div>", unsafe_allow_html=True)
                x1, x2, x3, x4 = st.columns(4)
                x1.metric("SKU", candidate["sku"])
                x2.metric("Solicitadas", int(candidate["unidades"]))
                x3.metric("Acopiadas", int(candidate["acopiadas"]))
                x4.metric("Pendientes", max(pendiente, 0))

            with st.form("form_agregar_cantidad", clear_on_submit=False):
                qty_txt = st.text_input(
                    "Cantidad a agregar",
                    value="",
                    key="scan_qty_input",
                    placeholder="Ingresa cantidad",
                )
                agregar = st.form_submit_button("Agregar cantidad", type="primary")

            if agregar:
                qty = to_int(qty_txt)
                if qty <= 0:
                    st.error("Ingresa una cantidad válida mayor a cero.")
                elif qty > pendiente:
                    st.error(f"No puedes agregar {qty}. Solo quedan {pendiente} pendientes.")
                else:
                    ok, msg = add_acopio(active_lote, int(candidate["id"]), int(qty), st.session_state.get("scan_primary", ""), st.session_state.get("scan_secondary", ""), modo)
                    if ok:
                        reset_scan_state()
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        st.divider()
        if st.button("Deshacer último escaneo"):
            ok, msg = undo_last_scan(active_lote)
            st.success(msg) if ok else st.warning(msg)
            if ok: st.rerun()

elif page == "Etiquetas":
    st.subheader("Etiquetas Zebra 50x30")
    st.caption("Módulo independiente: solo genera/descarga ZPL y registra etiquetas. No modifica el escaneo ni las unidades acopiadas.")

    if not active_lote:
        st.warning("Primero crea o selecciona un lote FULL.")
    else:
        lote = get_lote(active_lote)
        view = label_control_view(active_lote)
        if view.empty:
            st.warning("El lote activo no tiene productos.")
        else:
            capacity = st.number_input("Capacidad de rollo dedicado", min_value=100, max_value=10000, value=ROLL_CAPACITY_DEFAULT, step=100)
            blocks = build_label_blocks(view, int(capacity))
            total_products = int(len(view))
            total_normal = int(view["unidades"].sum())
            total_separators = int(total_products * LABEL_SEPARATOR_PER_PRODUCT)
            total_labels = int(total_normal + total_separators)
            printed_normal = int(view["printed_normal"].sum())
            pending_normal = max(total_normal - printed_normal, 0)

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Productos", total_products)
            c2.metric("Etiquetas producto", total_normal)
            c3.metric("Inicio/Fin", total_separators)
            c4.metric("Total ZPL", total_labels)
            c5.metric("Bloques", len(blocks))
            st.caption(f"Lote: {lote.get('nombre','')} · Archivo: {lote.get('archivo','')} · Hoja: {lote.get('hoja','')}")

            if any(b.get("over_capacity") for b in blocks):
                st.warning("Hay al menos un producto que por sí solo supera la capacidad del rollo. Ese producto quedará en un bloque propio.")

            tab_blocks, tab_individual, tab_control = st.tabs(["Bloques por rollo", "Individual", "Control etiquetas"])

            with tab_blocks:
                st.info("Regla activa: 1 bloque = 1 rollo nuevo dedicado. Cada producto imprime: INICIO + etiquetas normales + FIN. Al descargar un ZPL, queda registrado automáticamente como impreso.")
                for block in blocks:
                    rec = get_label_block_record(active_lote, block["block_index"], block["block_key"])
                    printed = bool(rec)
                    status = rec.get("status", "PENDIENTE") if rec else "PENDIENTE"
                    card_class = "label-card-printed" if printed else "label-card"
                    first_item = block["items"][0]
                    last_item = block["items"][-1]
                    st.markdown(f"""
                        <div class='label-card {card_class}'>
                            <b>Bloque {int(block['block_index'])}</b><br>
                            Estado: <b>{esc(status)}</b><br>
                            Productos: <b>{int(block['products_count'])}</b> · Etiquetas normales: <b>{int(block['normal_qty'])}</b> · Inicio/Fin: <b>{int(block['separator_qty'])}</b> · Total rollo: <b>{int(block['total_qty'])}</b><br>
                            Desde: <b>{esc(first_item.get('codigo_ml',''))}</b> / SKU {esc(first_item.get('sku',''))}<br>
                            Hasta: <b>{esc(last_item.get('codigo_ml',''))}</b> / SKU {esc(last_item.get('sku',''))}
                        </div>
                        """, unsafe_allow_html=True)
                    zpl_data = zpl_for_block(block).encode("utf-8")
                    fname = f"etiquetas_lote_{active_lote}_bloque_{int(block['block_index'])}.zpl"
                    if printed:
                        st.warning(f"Bloque {int(block['block_index'])} ya fue marcado como impreso. Si descargas nuevamente, quedará registrado como REIMPRESIÓN.")
                        label = f"Reimprimir bloque {int(block['block_index'])} y registrar reimpresión"
                    else:
                        label = f"Descargar ZPL bloque {int(block['block_index'])} y marcar como impreso"
                    st.download_button(label, data=zpl_data, file_name=fname, mime="text/plain", key=f"download_block_{active_lote}_{block['block_index']}_{block['block_key']}", on_click=register_block_download, args=(active_lote, block))
                    with st.expander(f"Ver productos del bloque {int(block['block_index'])}"):
                        bdf = pd.DataFrame(block["items"])
                        show_cols = ["codigo_ml", "sku", "descripcion", "unidades", "printed_normal", "label_pending", "label_status"]
                        existing_cols = [c for c in show_cols if c in bdf.columns]
                        st.dataframe(bdf[existing_cols], use_container_width=True, hide_index=True)

            with tab_individual:
                st.info("Para excepciones: imprimir 1 o varias etiquetas de un producto específico. También queda registrado automáticamente al descargar.")
                options = []
                option_map = {}
                for _, r in view.iterrows():
                    label = f"{clean_text(r.get('descripcion',''))[:70]} | ML {clean_text(r.get('codigo_ml',''))} | SKU {clean_text(r.get('sku',''))} | Estado {clean_text(r.get('label_status',''))}"
                    options.append(label)
                    option_map[label] = int(r["id"])
                selected = st.selectbox("Buscar producto", options, index=0 if options else None, placeholder="Escribe nombre, Código ML o SKU")
                selected_id = option_map.get(selected) if selected else None
                if selected_id:
                    row = view[view["id"].astype(int) == int(selected_id)].iloc[0].to_dict()
                    req = int(row.get("unidades", 0))
                    printed = int(row.get("printed_normal", 0))
                    pending = max(req - printed, 0)
                    status = clean_text(row.get("label_status", ""))
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Unidades", req)
                    m2.metric("Impresas", printed)
                    m3.metric("Pendientes", pending)
                    m4.metric("Estado", status)
                    st.markdown(f"**{clean_text(row.get('descripcion',''))}**")
                    st.caption(f"Código ML: {clean_text(row.get('codigo_ml',''))} · SKU: {clean_text(row.get('sku',''))}")
                    qty_ind = st.number_input("Cantidad de etiquetas normales a descargar", min_value=1, max_value=9999, value=1, step=1)
                    if printed >= req:
                        st.warning("Este producto ya tiene todas sus etiquetas normales impresas. La descarga se registrará como REIMPRESIÓN.")
                    elif int(qty_ind) > pending:
                        st.warning(f"La cantidad supera lo pendiente ({pending}). Puede dejar el producto SOBREIMPRESO.")
                    zpl_ind = zpl_for_item_with_separators(row, int(qty_ind)).encode("utf-8")
                    fname_ind = f"etiqueta_{norm_code(row.get('codigo_ml','')) or 'producto'}_{norm_code(row.get('sku',''))}.zpl"
                    st.download_button("Descargar ZPL individual y marcar como impreso", data=zpl_ind, file_name=fname_ind, mime="text/plain", key=f"download_individual_{active_lote}_{selected_id}_{qty_ind}", on_click=register_individual_download, args=(active_lote, row, int(qty_ind)))

            with tab_control:
                st.caption(f"Etiquetas normales impresas: {printed_normal}/{total_normal} · Pendientes normales: {pending_normal}")
                filtro_label = st.selectbox("Filtro estado etiquetas", ["Todos", "SIN IMPRIMIR", "PARCIAL", "COMPLETO", "SOBREIMPRESO"])
                show = view.copy()
                if filtro_label != "Todos":
                    show = show[show["label_status"] == filtro_label]
                out = show.rename(columns={
                    "codigo_ml": "Código ML",
                    "sku": "SKU",
                    "descripcion": "Producto",
                    "unidades": "Unidades requeridas",
                    "printed_normal": "Etiquetas impresas",
                    "label_pending": "Pendientes",
                    "label_status": "Estado etiquetas",
                    "printed_separators": "Inicio/Fin impresos",
                    "last_label_printed_at": "Última impresión",
                })
                cols = ["Código ML", "SKU", "Producto", "Unidades requeridas", "Etiquetas impresas", "Pendientes", "Estado etiquetas", "Inicio/Fin impresos", "Última impresión"]
                st.dataframe(out[[c for c in cols if c in out.columns]], use_container_width=True, hide_index=True, height=620)

elif page == "Control":
    st.subheader("Control de lote")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        lote = get_lote(active_lote)
        items = get_items(active_lote)
        if items.empty:
            st.warning("El lote no tiene productos.")
        else:
            view = items.copy()
            view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
            view["estado"] = view["pendiente"].apply(lambda x: "COMPLETO" if int(x) == 0 else "PENDIENTE")
            scans = get_last_scans(active_lote)
            if not scans.empty:
                view = view.merge(scans, left_on="id", right_on="item_id", how="left")
            else:
                view["procesado_at"] = ""
            c1, c2, c3, c4 = st.columns(4)
            total = int(view["unidades"].sum()); done = int(view["acopiadas"].sum())
            c1.metric("Unidades", total)
            c2.metric("Acopiadas", done)
            c3.metric("Pendientes", max(total-done, 0))
            c4.metric("Avance", f"{(done/total*100) if total else 0:.1f}%")
            st.caption(f"Archivo: {lote.get('archivo','')} · Hoja: {lote.get('hoja','')} · Cargado: {fmt_dt(lote.get('created_at',''))}")

            filtro = st.selectbox("Filtro", ["Todos", "Pendientes", "Completos", "Supermercado"])

            show = view
            if filtro == "Pendientes":
                show = view[view["pendiente"] > 0]
            elif filtro == "Completos":
                show = view[view["pendiente"] == 0]
            elif filtro == "Supermercado":
                show = view[view["identificacion"].map(is_supermercado)]

            # Buscador dinámico nativo: el selectbox permite escribir y muestra coincidencias al instante.
            option_rows = []
            option_map = {"": None}
            for _, sr in show.iterrows():
                desc = clean_text(sr.get("descripcion", ""))
                sku = clean_text(sr.get("sku", ""))
                ml = clean_text(sr.get("codigo_ml", ""))
                ean = clean_text(sr.get("codigo_universal", ""))
                ident = clean_text(sr.get("identificacion", ""))
                label = f"{desc} | SKU {sku} | ML {ml} | EAN {ean} | {ident}"
                # Limita el largo visual, pero mantiene códigos suficientes para buscar.
                label = label[:180]
                option_rows.append(label)
                option_map[label] = int(sr["id"])

            selected_search = st.selectbox(
                "Buscar tarjeta",
                [""] + option_rows,
                index=0,
                placeholder="Escribe nombre, SKU, Código ML, EAN o supermercado",
                key="control_search_select",
            )

            selected_id = option_map.get(selected_search)
            if selected_id:
                show = show[show["id"].astype(int) == int(selected_id)]

            st.caption(f"Mostrando {len(show)} de {len(view)} líneas del lote.")

            modo_vista = st.radio("Vista", ["Tarjetas operativas", "Tabla"], horizontal=True)
            if modo_vista == "Tarjetas operativas":
                for _, r in show.iterrows():
                    ident = clean_text(r.get("identificacion", ""))
                    vence = clean_text(r.get("vence", ""))
                    proc = fmt_dt(r.get("procesado_at", "")) or "Sin procesar"
                    badges_parts = [
                        f"<span class='badge'>Unidades: {int(r['unidades'])}</span>",
                        f"<span class='badge'>Acopiadas: {int(r['acopiadas'])}</span>",
                        f"<span class='badge'>Pendiente: {int(r['pendiente'])}</span>",
                    ]
                    if ident:
                        badges_parts.append(f"<span class='badge badge-alert'>Identificación: {esc(ident)}</span>")
                    if vence:
                        badges_parts.append(f"<span class='badge badge-alert'>Vence: {esc(vence)}</span>")
                    badges_parts.append(f"<span class='badge'>Procesado: {esc(proc)}</span>")
                    badges = "".join(badges_parts)
                    st.markdown(
                        f"""
                        <div class='control-card'>
                            <div class='control-title'>{esc(r['descripcion'])}</div>
                            <div class='control-meta'><b>SKU:</b> {esc(r['sku'])} &nbsp; | &nbsp; <b>Código ML:</b> {esc(r['codigo_ml'])}</div>
                            <div>{badges}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
            else:
                out = show.copy()
                out["Procesado"] = out["procesado_at"].map(fmt_dt)
                out = out.rename(columns={
                    "sku":"SKU", "codigo_ml":"Código ML", "codigo_universal":"EAN / Código universal",
                    "descripcion":"Producto", "unidades":"Unidades", "acopiadas":"Acopiadas", "pendiente":"Pendiente",
                    "identificacion":"Identificación", "vence":"Vence", "estado":"Estado"
                })
                cols = ["SKU", "Código ML", "EAN / Código universal", "Producto", "Unidades", "Acopiadas", "Pendiente", "Identificación", "Vence", "Procesado", "Estado"]
                st.dataframe(out[cols], use_container_width=True, hide_index=True, height=620)

            st.download_button("Exportar control Excel", data=export_lote(active_lote), file_name="control_full_aurora.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.divider()
            if st.button("Eliminar lote activo"):
                delete_lote(active_lote); st.success("Lote eliminado."); st.rerun()
