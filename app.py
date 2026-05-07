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
from zoneinfo import ZoneInfo
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

APP_TITLE = "Control FULL Aurora"
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "aurora_full_v3.db"
MAESTRO_PATH = DATA_DIR / "maestro_sku_ean.xlsx"
DEFAULT_SHEETS_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbzwfCk7ov8fCdX3WoTon-25Q8W-iLZUfWqUTvRSLjOGrkid6J2fNgGSmnSbB7lqUiw/exec"
MAX_BACKUP_ATTEMPTS = 5
SCAN_OPERATORS = ["ERICK", "JUAN CARLOS"]

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


CHILE_TZ = ZoneInfo("America/Santiago")


def now_cl() -> datetime:
    """Hora oficial de Chile para guardar eventos operativos."""
    return datetime.now(CHILE_TZ)


def fmt_dt(v) -> str:
    s = clean_text(v)
    if not s:
        return ""
    try:
        raw = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            # Registros antiguos sin zona horaria: se asumen ya en hora Chile.
            dt = dt.replace(tzinfo=CHILE_TZ)
        else:
            dt = dt.astimezone(CHILE_TZ)
        return dt.strftime("%d-%m-%Y %H:%M:%S")
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

def ensure_column(conn, table: str, column: str, definition: str):
    """Agrega una columna si no existe.

    Migración defensiva para Streamlit Cloud:
    - si la columna ya existe, no hace nada;
    - si SQLite igual responde "duplicate column name" por una base parcial/antigua, lo ignora;
    - si el error es otro, lo vuelve a levantar para no esconder problemas reales.
    """
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        cols = set()
        for r in rows:
            try:
                cols.add(str(r["name"]))
            except Exception:
                cols.add(str(r[1]))

        if column in cols:
            return

        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if "duplicate column name" in msg or "already exists" in msg:
            return
        raise


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
                instrucciones TEXT,
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
        c.execute("""
            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER,
                item_id INTEGER,
                event_type TEXT NOT NULL,
                detail TEXT,
                qty INTEGER,
                codigo_ml TEXT,
                sku TEXT,
                mode TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS incidencias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                item_id INTEGER,
                tipo TEXT NOT NULL,
                cantidad INTEGER NOT NULL DEFAULT 0,
                comentario TEXT,
                usuario TEXT,
                status TEXT NOT NULL DEFAULT 'ABIERTA',
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                resolved_by TEXT,
                resolution_comment TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS reimpresiones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                item_id INTEGER,
                block_index INTEGER,
                block_key TEXT,
                scope TEXT NOT NULL,
                cantidad INTEGER NOT NULL DEFAULT 0,
                motivo TEXT NOT NULL,
                usuario TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS avisos_operacionales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                codigo_ml TEXT,
                codigo_universal TEXT,
                sku TEXT,
                descripcion TEXT,
                tipo_aviso TEXT NOT NULL,
                mensaje_operador TEXT,
                cantidad_original INTEGER,
                cantidad_nueva INTEGER,
                requiere_ajuste_ml INTEGER NOT NULL DEFAULT 0,
                requiere_ajuste_inventario INTEGER NOT NULL DEFAULT 0,
                confirmado_ml INTEGER NOT NULL DEFAULT 0,
                confirmado_inventario INTEGER NOT NULL DEFAULT 0,
                visible_operador INTEGER NOT NULL DEFAULT 1,
                estado TEXT NOT NULL DEFAULT 'ACTIVO',
                comentario_interno TEXT,
                created_by TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                resolved_by TEXT,
                resolution_comment TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS picking_lists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                codigo_lista TEXT NOT NULL,
                asignado_a TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'CREADA',
                created_by TEXT,
                comentario TEXT,
                created_at TEXT NOT NULL,
                printed_at TEXT,
                completed_at TEXT,
                anulada_at TEXT,
                anulada_by TEXT,
                anulada_motivo TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS picking_list_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                picking_list_id INTEGER NOT NULL,
                lote_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                codigo_ml TEXT,
                codigo_universal TEXT,
                sku TEXT,
                descripcion TEXT,
                cantidad INTEGER NOT NULL DEFAULT 0,
                area TEXT,
                nro TEXT,
                estado TEXT NOT NULL DEFAULT 'PENDIENTE',
                created_at TEXT NOT NULL
            )
        """)
        ensure_column(c, "lotes", "status", "TEXT NOT NULL DEFAULT 'ACTIVO'")
        ensure_column(c, "lotes", "closed_at", "TEXT")
        ensure_column(c, "lotes", "closed_by", "TEXT")
        ensure_column(c, "lotes", "close_note", "TEXT")
        ensure_column(c, "items", "instrucciones", "TEXT")
        # Incidencias por código: se conserva lote_id para control/cierre, pero el operador registra por ML/EAN/SKU.
        ensure_column(c, "incidencias", "codigo_ml", "TEXT")
        ensure_column(c, "incidencias", "codigo_universal", "TEXT")
        ensure_column(c, "incidencias", "sku", "TEXT")
        ensure_column(c, "incidencias", "descripcion", "TEXT")
        ensure_column(c, "label_blocks", "last_reprint_reason", "TEXT")
        ensure_column(c, "label_blocks", "last_reprint_user", "TEXT")
        ensure_column(c, "scans", "operador_validador", "TEXT")
        ensure_column(c, "scans", "picking_list_id", "INTEGER")
        ensure_column(c, "scans", "picking_code", "TEXT")
        ensure_column(c, "scans", "picker_asignado", "TEXT")

        # Confirmaciones externas de avisos operacionales: ML y Kame se pueden marcar después de crear el aviso.
        ensure_column(c, "avisos_operacionales", "confirmado_ml_at", "TEXT")
        ensure_column(c, "avisos_operacionales", "confirmado_ml_by", "TEXT")
        ensure_column(c, "avisos_operacionales", "confirmado_inventario_at", "TEXT")
        ensure_column(c, "avisos_operacionales", "confirmado_inventario_by", "TEXT")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_label_blocks_unique ON label_blocks (lote_id, block_index, block_key)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_lote ON items (lote_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_codigo_ml ON items (lote_id, codigo_ml)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_sku ON items (lote_id, sku)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_scans_lote ON scans (lote_id, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_audit_lote ON audit_events (lote_id, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_incidencias_lote ON incidencias (lote_id, status, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_reimpresiones_lote ON reimpresiones (lote_id, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_avisos_lote ON avisos_operacionales (lote_id, estado, item_id, visible_operador, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_avisos_item ON avisos_operacionales (lote_id, item_id, estado)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_picking_lists_lote ON picking_lists (lote_id, estado, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_picking_items_list ON picking_list_items (picking_list_id, item_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_picking_items_lote ON picking_list_items (lote_id, item_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_scans_picking ON scans (picking_list_id, item_id, created_at)")

        c.commit()



# ============================================================
# Respaldo externo Google Sheets por webhook
# ============================================================

def get_backup_webhook_url() -> str:
    """URL definitiva de respaldo externo.

    Se usa solo la URL fija definida en DEFAULT_SHEETS_WEBHOOK_URL.
    No se toman URLs desde Streamlit Secrets ni variables de entorno para evitar
    que la app envíe eventos a un Apps Script antiguo por error.
    """
    return clean_text(DEFAULT_SHEETS_WEBHOOK_URL)


def get_backup_webhook_source() -> str:
    if clean_text(DEFAULT_SHEETS_WEBHOOK_URL):
        return "URL fija dentro de app.py"
    return "SIN URL CONFIGURADA"


def mask_url(url: str) -> str:
    url = clean_text(url)
    if not url:
        return ""
    if len(url) <= 32:
        return url
    return url[:28] + "..." + url[-12:]


def enqueue_backup_event(event_type: str, payload: dict):
    """Guarda el evento en cola local y dispara envío en segundo plano.
    La operación principal nunca queda bloqueada por Google Sheets.
    """
    now = now_cl().isoformat(timespec="seconds")
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
    now = now_cl().isoformat(timespec="seconds")
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
    """Restaura base local desde Sheets cuando SQLite está vacío.

    Refuerzos producción:
    - deduplica eventos por queue_id;
    - soporta eventos que vengan con raw_json plano desde Apps Script;
    - restaura incidencias;
    - restaura reimpresiones controladas;
    - restaura estado de lote cerrado/reabierto.
    """
    if local_lotes_count() > 0:
        return False, "Base local con datos; no se restaura."
    ok, events, msg = get_backup_events_from_sheets()
    if not ok:
        return False, msg
    if not events:
        return False, "No hay eventos en el respaldo externo."

    def normalize_event(ev: dict) -> dict:
        base = dict(ev or {})
        raw = base.get("raw_json")
        if raw:
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(parsed, dict):
                    base.update(parsed)
            except Exception:
                pass
        return base

    normalized_events = []
    seen_queue_ids = set()
    for raw_ev in events:
        ev = normalize_event(raw_ev)
        qid = clean_text(ev.get("queue_id", ""))
        if qid:
            if qid in seen_queue_ids:
                continue
            seen_queue_ids.add(qid)
        normalized_events.append(ev)

    def event_order_key(ev):
        qid = clean_text(ev.get("queue_id", ""))
        try:
            return (0, int(qid))
        except Exception:
            return (1, clean_text(ev.get("queued_at", "")) or clean_text(ev.get("created_at", "")) or clean_text(ev.get("received_at", "")))

    normalized_events.sort(key=event_order_key)

    lotes = {}
    items_by_lote = {}
    deleted_lotes = set()
    movement_by_item = {}
    scan_rows = []
    incidencias_rows = []
    reimpresiones_rows = []
    avisos_rows = {}
    avisos_status_updates = {}
    picking_rows = {}
    picking_status_updates = {}
    lote_status_updates = {}

    for ev in normalized_events:
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
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "status": clean_text(ev.get("status", "ACTIVO")) or "ACTIVO",
                "closed_at": clean_text(ev.get("closed_at", "")),
                "closed_by": clean_text(ev.get("closed_by", "")),
                "close_note": clean_text(ev.get("close_note", "")),
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
                "instrucciones": clean_text(ev.get("instrucciones", "")),
                "dia": clean_text(ev.get("dia", "")),
                "hora": clean_text(ev.get("hora", "")),
                "created_at": clean_text(ev.get("item_created_at", "")) or clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                "updated_at": clean_text(ev.get("item_updated_at", "")) or clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
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
                    "instrucciones": clean_text(item_ev.get("instrucciones", "")),
                    "dia": clean_text(item_ev.get("dia", "")),
                    "hora": clean_text(item_ev.get("hora", "")),
                    "created_at": clean_text(item_ev.get("item_created_at", "")) or clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "updated_at": clean_text(item_ev.get("item_updated_at", "")) or clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                }
        elif et == "scan_agregado":
            try:
                item_id = int(ev.get("item_id"))
                qty = int(ev.get("cantidad") or 0)
            except Exception:
                continue
            movement_by_item[item_id] = movement_by_item.get(item_id, 0) + qty
            scan_rows.append((
                lote_id, item_id, norm_code(ev.get("scan_primario", "")), norm_code(ev.get("scan_secundario", "")),
                qty, clean_text(ev.get("modo", "")), clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                clean_text(ev.get("operador_validador", "")) or "SIN_USUARIO",
                to_int(ev.get("picking_list_id", 0)) or None,
                clean_text(ev.get("picking_code", "")),
                clean_text(ev.get("picker_asignado", "")),
            ))
        elif et == "scan_deshacer":
            try:
                item_id = int(ev.get("item_id"))
                qty = int(ev.get("cantidad") or 0)
            except Exception:
                continue
            movement_by_item[item_id] = movement_by_item.get(item_id, 0) - qty
        elif et == "incidencia_creada" or et == "INCIDENCIA_ABIERTA":
            try:
                item_id_raw = ev.get("item_id", "")
                item_id = int(item_id_raw) if clean_text(item_id_raw) else None
            except Exception:
                item_id = None
            incidencias_rows.append({
                "lote_id": lote_id,
                "item_id": item_id,
                "tipo": clean_text(ev.get("tipo", "")) or "Otro",
                "cantidad": max(0, to_int(ev.get("cantidad", 0))),
                "comentario": clean_text(ev.get("comentario", "")),
                "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "status": clean_text(ev.get("status", "ABIERTA")) or "ABIERTA",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion", "")),
            })
        elif et == "incidencia_resuelta" or et == "INCIDENCIA_RESUELTA":
            # Se deja listo para futuros eventos de resolución; si no existe id estable, se resuelve por producto/tipo/comentario.
            pass
        elif et == "reimpresion_controlada" or et == "REIMPRESION_CONTROLADA":
            try:
                item_id_raw = ev.get("item_id", "")
                item_id = int(item_id_raw) if clean_text(item_id_raw) else None
            except Exception:
                item_id = None
            reimpresiones_rows.append({
                "lote_id": lote_id,
                "item_id": item_id,
                "block_index": to_int(ev.get("block_index", 0)) or None,
                "block_key": clean_text(ev.get("block_key", "")),
                "scope": clean_text(ev.get("scope", "")) or ("BLOQUE" if clean_text(ev.get("block_key", "")) else "PRODUCTO"),
                "cantidad": max(1, to_int(ev.get("cantidad", 1))),
                "motivo": clean_text(ev.get("motivo", "")) or clean_text(ev.get("comentario", "")) or "Restaurado desde respaldo",
                "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
            })
        elif et == "aviso_operacional_creado" or et == "AVISO_OPERACIONAL_CREADO":
            try:
                aviso_id_raw = ev.get("aviso_id", "")
                aviso_id = int(aviso_id_raw) if clean_text(aviso_id_raw) else None
            except Exception:
                aviso_id = None
            try:
                item_id_raw = ev.get("item_id", "")
                item_id = int(item_id_raw) if clean_text(item_id_raw) else None
            except Exception:
                item_id = None
            if item_id:
                key = aviso_id or f"{lote_id}:{item_id}:{clean_text(ev.get('tipo_aviso',''))}:{clean_text(ev.get('created_at','')) or clean_text(ev.get('queued_at',''))}"
                avisos_rows[key] = {
                    "id": aviso_id,
                    "lote_id": lote_id,
                    "item_id": item_id,
                    "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                    "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                    "sku": norm_code(ev.get("sku", "")),
                    "descripcion": clean_text(ev.get("descripcion", "")),
                    "tipo_aviso": clean_text(ev.get("tipo_aviso", "")) or "Preparar con observación",
                    "mensaje_operador": clean_text(ev.get("mensaje_operador", "")),
                    "cantidad_original": to_int(ev.get("cantidad_original", 0)),
                    "cantidad_nueva": to_int(ev.get("cantidad_nueva", 0)) if clean_text(ev.get("cantidad_nueva", "")) else None,
                    "requiere_ajuste_ml": 1 if ev.get("requiere_ajuste_ml") in [1, "1", True, "true", "TRUE", "Sí", "SI"] else 0,
                    "requiere_ajuste_inventario": 1 if ev.get("requiere_ajuste_inventario") in [1, "1", True, "true", "TRUE", "Sí", "SI"] else 0,
                    "confirmado_ml": 1 if ev.get("confirmado_ml") in [1, "1", True, "true", "TRUE", "Sí", "SI"] else 0,
                    "confirmado_inventario": 1 if (ev.get("confirmado_inventario") in [1, "1", True, "true", "TRUE", "Sí", "SI"] or ev.get("confirmado_kame") in [1, "1", True, "true", "TRUE", "Sí", "SI"]) else 0,
                    "confirmado_ml_at": clean_text(ev.get("confirmado_ml_at", "")),
                    "confirmado_ml_by": clean_text(ev.get("confirmado_ml_by", "")),
                    "confirmado_inventario_at": clean_text(ev.get("confirmado_inventario_at", "")) or clean_text(ev.get("confirmado_kame_at", "")),
                    "confirmado_inventario_by": clean_text(ev.get("confirmado_inventario_by", "")) or clean_text(ev.get("confirmado_kame_by", "")),
                    "visible_operador": 0 if ev.get("visible_operador") in [0, "0", False, "false", "FALSE", "No", "NO"] else 1,
                    "estado": clean_text(ev.get("estado", "ACTIVO")) or "ACTIVO",
                    "comentario_interno": clean_text(ev.get("comentario_interno", "")),
                    "created_by": clean_text(ev.get("created_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                    "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "resolved_at": clean_text(ev.get("resolved_at", "")),
                    "resolved_by": clean_text(ev.get("resolved_by", "")),
                    "resolution_comment": clean_text(ev.get("resolution_comment", "")),
                }
        elif et == "aviso_operacional_ml_confirmado" or et == "AVISO_OPERACIONAL_ML_CONFIRMADO":
            try:
                aviso_id_raw = ev.get("aviso_id", "")
                aviso_id = int(aviso_id_raw) if clean_text(aviso_id_raw) else None
            except Exception:
                aviso_id = None
            if aviso_id:
                upd = avisos_status_updates.setdefault(aviso_id, {})
                upd["confirmado_ml"] = 1
                upd["confirmado_ml_at"] = clean_text(ev.get("confirmado_at", "")) or clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds")
                upd["confirmado_ml_by"] = clean_text(ev.get("confirmado_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO"
        elif et == "aviso_operacional_kame_confirmado" or et == "AVISO_OPERACIONAL_KAME_CONFIRMADO":
            try:
                aviso_id_raw = ev.get("aviso_id", "")
                aviso_id = int(aviso_id_raw) if clean_text(aviso_id_raw) else None
            except Exception:
                aviso_id = None
            if aviso_id:
                upd = avisos_status_updates.setdefault(aviso_id, {})
                upd["confirmado_inventario"] = 1
                upd["confirmado_inventario_at"] = clean_text(ev.get("confirmado_at", "")) or clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds")
                upd["confirmado_inventario_by"] = clean_text(ev.get("confirmado_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO"
        elif et == "aviso_operacional_resuelto" or et == "AVISO_OPERACIONAL_RESUELTO":
            try:
                aviso_id_raw = ev.get("aviso_id", "")
                aviso_id = int(aviso_id_raw) if clean_text(aviso_id_raw) else None
            except Exception:
                aviso_id = None
            if aviso_id:
                upd = avisos_status_updates.setdefault(aviso_id, {})
                upd.update({
                    "estado": "RESUELTO",
                    "visible_operador": 0,
                    "resolved_at": clean_text(ev.get("resolved_at", "")) or clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "resolved_by": clean_text(ev.get("resolved_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                    "resolution_comment": clean_text(ev.get("resolution_comment", "")) or clean_text(ev.get("comentario", "")),
                })
        elif et == "picking_lista_creada" or et == "PICKING_LISTA_CREADA":
            try:
                plid_raw = ev.get("picking_list_id", "")
                plid = int(plid_raw) if clean_text(plid_raw) else None
            except Exception:
                plid = None
            key = plid or clean_text(ev.get("picking_code", "")) or clean_text(ev.get("codigo_lista", ""))
            if key:
                picking_rows[key] = {
                    "id": plid,
                    "lote_id": lote_id,
                    "codigo_lista": clean_text(ev.get("picking_code", "")) or clean_text(ev.get("codigo_lista", "")),
                    "asignado_a": clean_text(ev.get("asignado_a", "")) or "SIN_ASIGNAR",
                    "estado": clean_text(ev.get("estado", "CREADA")) or "CREADA",
                    "created_by": clean_text(ev.get("created_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                    "comentario": clean_text(ev.get("comentario", "")),
                    "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "items": ev.get("items") or [],
                }
        elif et in {"picking_lista_impresa", "PICKING_LISTA_IMPRESA", "picking_lista_completada", "PICKING_LISTA_COMPLETADA", "picking_lista_anulada", "PICKING_LISTA_ANULADA"}:
            try:
                plid_raw = ev.get("picking_list_id", "")
                plid = int(plid_raw) if clean_text(plid_raw) else None
            except Exception:
                plid = None
            key = plid or clean_text(ev.get("picking_code", "")) or clean_text(ev.get("codigo_lista", ""))
            if key:
                upd = picking_status_updates.setdefault(key, {})
                if "impresa" in et.lower():
                    upd["estado"] = "IMPRESA"
                    upd["printed_at"] = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds")
                elif "completada" in et.lower():
                    upd["estado"] = "COMPLETADA"
                    upd["completed_at"] = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds")
                elif "anulada" in et.lower():
                    upd["estado"] = "ANULADA"
                    upd["anulada_at"] = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds")
                    upd["anulada_by"] = clean_text(ev.get("usuario", "")) or "SIN_USUARIO"
                    upd["anulada_motivo"] = clean_text(ev.get("comentario", ""))
        elif et == "lote_cerrado":
            lote_status_updates[lote_id] = {
                "status": "CERRADO",
                "closed_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "closed_by": clean_text(ev.get("usuario", "")) or clean_text(ev.get("closed_by", "")) or "SIN_USUARIO",
                "close_note": clean_text(ev.get("comentario", "")) or clean_text(ev.get("close_note", "")),
            }
        elif et == "lote_reabierto":
            lote_status_updates[lote_id] = {
                "status": "ACTIVO",
                "closed_at": "",
                "closed_by": "",
                "close_note": "",
            }
        elif et == "lote_eliminado":
            deleted_lotes.add(lote_id)

    active_lote_ids = [lid for lid in lotes if lid not in deleted_lotes and items_by_lote.get(lid)]
    if not active_lote_ids:
        return False, "No encontré lotes activos con snapshot completo en Sheets. Crea el lote una vez con esta nueva versión para activar restauración automática."

    now = now_cl().isoformat(timespec="seconds")
    restored_lotes = 0
    restored_items = 0
    restored_scans = 0
    restored_incidencias = 0
    restored_reimpresiones = 0
    restored_avisos = 0
    restored_picking = 0

    with db() as c:
        for lid in sorted(active_lote_ids):
            lote = lotes[lid]
            status_update = lote_status_updates.get(lid, {})
            status = clean_text(status_update.get("status", lote.get("status", "ACTIVO"))) or "ACTIVO"
            closed_at = clean_text(status_update.get("closed_at", lote.get("closed_at", "")))
            closed_by = clean_text(status_update.get("closed_by", lote.get("closed_by", "")))
            close_note = clean_text(status_update.get("close_note", lote.get("close_note", "")))
            c.execute(
                """
                INSERT OR REPLACE INTO lotes
                (id, nombre, archivo, hoja, created_at, status, closed_at, closed_by, close_note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (lote["id"], lote["nombre"], lote["archivo"], lote["hoja"], lote["created_at"], status, closed_at, closed_by, close_note),
            )
            restored_lotes += 1
            for item in items_by_lote[lid].values():
                qty = max(0, min(int(item["unidades"]), int(movement_by_item.get(int(item["id"]), 0))))
                item["acopiadas"] = qty
                item["updated_at"] = now if qty else item["updated_at"]
                c.execute(
                    """
                    INSERT OR REPLACE INTO items
                    (id, lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades, acopiadas,
                     identificacion, vence, instrucciones, dia, hora, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (item["id"], item["lote_id"], item["area"], item["nro"], item["codigo_ml"], item["codigo_universal"], item["sku"], item["descripcion"], item["unidades"], item["acopiadas"], item["identificacion"], item["vence"], item.get("instrucciones", ""), item["dia"], item["hora"], item["created_at"], item["updated_at"]),
                )
                restored_items += 1
        for lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at, operador_validador, picking_list_id, picking_code, picker_asignado in scan_rows:
            if lote_id in active_lote_ids and cantidad > 0:
                c.execute(
                    """
                    INSERT INTO scans
                    (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at,
                     operador_validador, picking_list_id, picking_code, picker_asignado)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at,
                     operador_validador, picking_list_id, picking_code, picker_asignado),
                )
                restored_scans += 1
        for inc in incidencias_rows:
            if inc["lote_id"] in active_lote_ids:
                c.execute(
                    """
                    INSERT INTO incidencias
                    (lote_id, item_id, tipo, cantidad, comentario, usuario, status, created_at,
                     codigo_ml, codigo_universal, sku, descripcion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (inc["lote_id"], inc["item_id"], inc["tipo"], inc["cantidad"], inc["comentario"], inc["usuario"], inc["status"], inc["created_at"], inc["codigo_ml"], inc["codigo_universal"], inc["sku"], inc["descripcion"]),
                )
                restored_incidencias += 1
        for rep in reimpresiones_rows:
            if rep["lote_id"] in active_lote_ids:
                c.execute(
                    """
                    INSERT INTO reimpresiones
                    (lote_id, item_id, block_index, block_key, scope, cantidad, motivo, usuario, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (rep["lote_id"], rep["item_id"], rep["block_index"], rep["block_key"], rep["scope"], rep["cantidad"], rep["motivo"], rep["usuario"], rep["created_at"]),
                )
                restored_reimpresiones += 1
        for aviso in avisos_rows.values():
            if aviso["lote_id"] in active_lote_ids and aviso.get("item_id"):
                upd = avisos_status_updates.get(aviso.get("id"), {}) if aviso.get("id") else {}
                aviso_estado = clean_text(upd.get("estado", aviso.get("estado", "ACTIVO"))) or "ACTIVO"
                aviso_confirmado_ml = int(upd.get("confirmado_ml", aviso.get("confirmado_ml", 0)))
                aviso_confirmado_inv = int(upd.get("confirmado_inventario", aviso.get("confirmado_inventario", 0)))
                aviso_confirmado_ml_at = clean_text(upd.get("confirmado_ml_at", aviso.get("confirmado_ml_at", "")))
                aviso_confirmado_ml_by = clean_text(upd.get("confirmado_ml_by", aviso.get("confirmado_ml_by", "")))
                aviso_confirmado_inv_at = clean_text(upd.get("confirmado_inventario_at", aviso.get("confirmado_inventario_at", "")))
                aviso_confirmado_inv_by = clean_text(upd.get("confirmado_inventario_by", aviso.get("confirmado_inventario_by", "")))
                aviso_visible = int(upd.get("visible_operador", aviso.get("visible_operador", 1)))
                aviso_resolved_at = clean_text(upd.get("resolved_at", aviso.get("resolved_at", "")))
                aviso_resolved_by = clean_text(upd.get("resolved_by", aviso.get("resolved_by", "")))
                aviso_resolution_comment = clean_text(upd.get("resolution_comment", aviso.get("resolution_comment", "")))
                if aviso.get("id"):
                    c.execute(
                        """
                        INSERT OR REPLACE INTO avisos_operacionales
                        (id, lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion,
                         tipo_aviso, mensaje_operador, cantidad_original, cantidad_nueva,
                         requiere_ajuste_ml, requiere_ajuste_inventario, confirmado_ml, confirmado_inventario,
                         confirmado_ml_at, confirmado_ml_by, confirmado_inventario_at, confirmado_inventario_by,
                         visible_operador, estado, comentario_interno, created_by, created_at,
                         resolved_at, resolved_by, resolution_comment)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (aviso["id"], aviso["lote_id"], aviso["item_id"], aviso["codigo_ml"], aviso["codigo_universal"], aviso["sku"], aviso["descripcion"],
                         aviso["tipo_aviso"], aviso["mensaje_operador"], aviso["cantidad_original"], aviso["cantidad_nueva"],
                         aviso["requiere_ajuste_ml"], aviso["requiere_ajuste_inventario"], aviso_confirmado_ml, aviso_confirmado_inv,
                         aviso_confirmado_ml_at, aviso_confirmado_ml_by, aviso_confirmado_inv_at, aviso_confirmado_inv_by,
                         aviso_visible, aviso_estado, aviso["comentario_interno"], aviso["created_by"], aviso["created_at"],
                         aviso_resolved_at, aviso_resolved_by, aviso_resolution_comment),
                    )
                else:
                    c.execute(
                        """
                        INSERT INTO avisos_operacionales
                        (lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion,
                         tipo_aviso, mensaje_operador, cantidad_original, cantidad_nueva,
                         requiere_ajuste_ml, requiere_ajuste_inventario, confirmado_ml, confirmado_inventario,
                         confirmado_ml_at, confirmado_ml_by, confirmado_inventario_at, confirmado_inventario_by,
                         visible_operador, estado, comentario_interno, created_by, created_at,
                         resolved_at, resolved_by, resolution_comment)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (aviso["lote_id"], aviso["item_id"], aviso["codigo_ml"], aviso["codigo_universal"], aviso["sku"], aviso["descripcion"],
                         aviso["tipo_aviso"], aviso["mensaje_operador"], aviso["cantidad_original"], aviso["cantidad_nueva"],
                         aviso["requiere_ajuste_ml"], aviso["requiere_ajuste_inventario"], aviso_confirmado_ml, aviso_confirmado_inv,
                         aviso_confirmado_ml_at, aviso_confirmado_ml_by, aviso_confirmado_inv_at, aviso_confirmado_inv_by,
                         aviso_visible, aviso_estado, aviso["comentario_interno"], aviso["created_by"], aviso["created_at"],
                         aviso_resolved_at, aviso_resolved_by, aviso_resolution_comment),
                    )
                restored_avisos += 1
        for key, plist in picking_rows.items():
            if plist["lote_id"] in active_lote_ids:
                upd = picking_status_updates.get(plist.get("id"), {}) or picking_status_updates.get(plist.get("codigo_lista"), {}) or {}
                estado = clean_text(upd.get("estado", plist.get("estado", "CREADA"))) or "CREADA"
                printed_at = clean_text(upd.get("printed_at", ""))
                completed_at = clean_text(upd.get("completed_at", ""))
                anulada_at = clean_text(upd.get("anulada_at", ""))
                anulada_by = clean_text(upd.get("anulada_by", ""))
                anulada_motivo = clean_text(upd.get("anulada_motivo", ""))
                if plist.get("id"):
                    c.execute(
                        """
                        INSERT OR REPLACE INTO picking_lists
                        (id, lote_id, codigo_lista, asignado_a, estado, created_by, comentario, created_at,
                         printed_at, completed_at, anulada_at, anulada_by, anulada_motivo)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (plist["id"], plist["lote_id"], plist["codigo_lista"], plist["asignado_a"], estado, plist["created_by"], plist["comentario"], plist["created_at"], printed_at, completed_at, anulada_at, anulada_by, anulada_motivo),
                    )
                    list_id_db = int(plist["id"])
                else:
                    cur = c.execute(
                        """
                        INSERT INTO picking_lists
                        (lote_id, codigo_lista, asignado_a, estado, created_by, comentario, created_at,
                         printed_at, completed_at, anulada_at, anulada_by, anulada_motivo)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (plist["lote_id"], plist["codigo_lista"], plist["asignado_a"], estado, plist["created_by"], plist["comentario"], plist["created_at"], printed_at, completed_at, anulada_at, anulada_by, anulada_motivo),
                    )
                    list_id_db = int(cur.lastrowid)
                c.execute("DELETE FROM picking_list_items WHERE picking_list_id=?", (list_id_db,))
                for pit in plist.get("items", []):
                    try:
                        item_id = int(pit.get("item_id"))
                    except Exception:
                        continue
                    c.execute(
                        """
                        INSERT INTO picking_list_items
                        (picking_list_id, lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion,
                         cantidad, area, nro, estado, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDIENTE', ?)
                        """,
                        (list_id_db, plist["lote_id"], item_id, norm_code(pit.get("codigo_ml", "")), norm_code(pit.get("codigo_universal", "")), norm_code(pit.get("sku", "")), clean_text(pit.get("descripcion", "")), to_int(pit.get("cantidad", 0)), clean_text(pit.get("area", "")), clean_text(pit.get("nro", "")), plist["created_at"]),
                    )
                restored_picking += 1
        c.commit()

    return True, f"Restauración completa: {restored_lotes} lote(s), {restored_items} producto(s), {restored_scans} escaneo(s), {restored_incidencias} incidencia(s), {restored_reimpresiones} reimpresión(es), {restored_avisos} aviso(s) operacional(es), {restored_picking} lista(s) picking."

def flush_backup_queue(webhook_url: str | None = None, limit: int = 25, include_failed: bool = False):
    """Envía eventos pendientes a Google Sheets.

    Producción:
    - no borra eventos si falla;
    - después de MAX_BACKUP_ATTEMPTS deja el evento como failed;
    - include_failed permite reintentar fallidos manualmente desde la UI.
    """
    url = clean_text(webhook_url or get_backup_webhook_url())
    if not url:
        return

    statuses = ("'pending','failed'" if include_failed else "'pending'")
    with db() as c:
        rows = c.execute(
            f"""
            SELECT id, event_type, payload_json, attempts, created_at
            FROM backup_queue
            WHERE status IN ({statuses})
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

            sent_at = now_cl().isoformat(timespec="seconds")
            with db() as c:
                c.execute(
                    "UPDATE backup_queue SET status='sent', sent_at=?, last_error=NULL WHERE id=?",
                    (sent_at, int(row["id"])),
                )
                c.commit()

        except Exception as e:
            attempts_next = int(row["attempts"] or 0) + 1
            new_status = "failed" if attempts_next >= MAX_BACKUP_ATTEMPTS else "pending"
            with db() as c:
                c.execute(
                    """
                    UPDATE backup_queue
                    SET attempts=?, status=?, last_error=?
                    WHERE id=?
                    """,
                    (attempts_next, new_status, str(e)[:500], int(row["id"])),
                )
                c.commit()


def retry_failed_backups(limit: int = 1000):
    """Reintenta eventos fallidos sin perder su queue_id original."""
    with db() as c:
        c.execute("UPDATE backup_queue SET status='pending' WHERE status='failed'")
        c.commit()
    flush_backup_queue(limit=limit, include_failed=True)


def get_backup_error_rows(limit: int = 20) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT id, event_type, status, attempts, last_error, created_at, sent_at
            FROM backup_queue
            WHERE COALESCE(last_error,'') <> '' OR status='failed'
            ORDER BY id DESC
            LIMIT ?
            """,
            c,
            params=(int(limit),),
        )

def backup_status():
    with db() as c:
        row = c.execute(
            """
            SELECT
                SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                MAX(sent_at) AS last_sent,
                MAX(last_error) AS last_error
            FROM backup_queue
            """
        ).fetchone()
    return dict(row) if row else {"pending": 0, "sent": 0, "failed": 0, "last_sent": "", "last_error": ""}

def test_backup_webhook() -> tuple[bool, str]:
    url = get_backup_webhook_url()
    if not url:
        return False, "No hay SHEETS_WEBHOOK_URL configurada."
    event = {
        "event_type": "test_webhook",
        "created_at": now_cl().isoformat(timespec="seconds"),
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
        "tipo": "TEST",
        "comentario": "Prueba manual desde botón Probar respaldo Sheets",
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
            SELECT l.id, l.nombre, l.archivo, l.hoja, l.created_at, l.status, l.closed_at, l.closed_by,
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
    now = now_cl().isoformat(timespec="seconds")
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
                clean_text(getattr(r, "vence", "")),
                clean_text(getattr(r, "instrucciones", "")),
                clean_text(getattr(r, "dia", "")),
                clean_text(getattr(r, "hora", "")),
                now,
                now,
            ))
        c.executemany("""
            INSERT INTO items
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades, acopiadas,
             identificacion, vence, instrucciones, dia, hora, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            "vence": clean_text(getattr(r, "vence", "")),
            "instrucciones": clean_text(getattr(r, "instrucciones", "")),
            "dia": clean_text(getattr(r, "dia", "")),
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
    log_audit_event(lote_id, event_type="LOTE_CREADO", detail=f"Lote creado desde {archivo} / {hoja}", qty=int(df["unidades"].sum()) if "unidades" in df.columns else 0)
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
        "deleted_at": now_cl().isoformat(timespec="seconds"),
    })
    log_audit_event(lote_id, event_type="LOTE_ELIMINADO", detail="Lote eliminado", qty=int(items_count))


def add_acopio(lote_id, item_id, cantidad, scan_primario, scan_secundario, modo, operador_validador='', picking_list_id=None):
    if is_lote_closed(lote_id):
        return False, "Este lote está cerrado. Reabre el lote desde Supervisor antes de escanear."
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        item = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (item_id, lote_id)).fetchone()
        if not item:
            return False, "Producto no encontrado."
        pendiente = int(item["unidades"]) - int(item["acopiadas"])
        # Picking obligatorio:
        # todo escaneo debe estar asociado a una lista activa. No se permite validar
        # productos sin lista, porque se pierde trazabilidad de picker/lista.
        try:
            pick_id = int(picking_list_id or 0)
        except Exception:
            pick_id = 0
        if not pick_id:
            return False, "Debes seleccionar una lista de picking activa antes de validar. No se permite escanear sin lista."

        picking_meta = get_picking_list_meta(pick_id)

        # Regla estricta de picking:
        # solo se puede validar productos que pertenecen a la lista activa.
        # Esto evita que el validador PDA cargue por error productos de otro picker
        # o productos todavía sin asignar.
        if not item_in_picking_list(pick_id, int(item_id)):
            return False, f"Este producto no pertenece a la lista activa {clean_text(picking_meta.get('codigo_lista',''))}. Cambia la lista de picking antes de validar."
        pp = picking_pending_for_item(pick_id, int(item_id))
        pendiente_lista = int(pp.get("pendiente") or 0)
        if pendiente_lista <= 0:
            return False, f"Este producto ya está completo en la lista activa {clean_text(picking_meta.get('codigo_lista',''))}."
        if cantidad > pendiente_lista:
            return False, f"No puedes agregar {cantidad} en la lista activa. Solo quedan {pendiente_lista} pendientes para esta lista."

        if pendiente <= 0:
            return False, "Este producto ya está completo."
        if cantidad <= 0:
            return False, "La cantidad debe ser mayor a cero."
        if cantidad > pendiente:
            return False, f"No puedes agregar {cantidad}. Solo quedan {pendiente} pendientes."
        c.execute("UPDATE items SET acopiadas=acopiadas+?, updated_at=? WHERE id=?", (cantidad, now, item_id))
        c.execute("""
            INSERT INTO scans
            (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at,
             operador_validador, picking_list_id, picking_code, picker_asignado)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lote_id, item_id, norm_code(scan_primario), norm_code(scan_secundario), cantidad, modo, now,
            clean_text(operador_validador) or "SIN_USUARIO",
            int(pick_id),
            clean_text(picking_meta.get("codigo_lista", "")),
            clean_text(picking_meta.get("asignado_a", "")),
        ))
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
        "operador_validador": clean_text(operador_validador) or "SIN_USUARIO",
        "picking_list_id": int(pick_id),
        "picking_code": clean_text(picking_meta.get("codigo_lista", "")),
        "picker_asignado": clean_text(picking_meta.get("asignado_a", "")),
    })
    log_audit_event(lote_id, item_id, "SKU_ESCANEADO", clean_text(item["descripcion"]), int(cantidad), item["codigo_ml"], item["sku"], modo)
    return True, "Cantidad agregada."


def undo_last_scan(lote_id):
    with db() as c:
        row = c.execute("SELECT * FROM scans WHERE lote_id=? ORDER BY id DESC LIMIT 1", (lote_id,)).fetchone()
        if not row:
            return False, "No hay escaneos para deshacer."
        now = now_cl().isoformat(timespec="seconds")
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
        "operador_validador": clean_text(row["operador_validador"] if "operador_validador" in row.keys() else ""),
        "picking_list_id": clean_text(row["picking_list_id"] if "picking_list_id" in row.keys() else ""),
        "picking_code": clean_text(row["picking_code"] if "picking_code" in row.keys() else ""),
        "picker_asignado": clean_text(row["picker_asignado"] if "picker_asignado" in row.keys() else ""),
    })
    log_audit_event(lote_id, int(row["item_id"]), "SCAN_DESHECHO", clean_text(item_payload.get("descripcion", "")), int(row["cantidad"]), item_payload.get("codigo_ml", ""), item_payload.get("sku", ""), row["modo"])
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
    instrucciones_col = col_exact(cols, ["Instrucciones", "Instrucciones de preparación", "Instrucciones Preparación", "Preparación", "Preparacion"])
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
        "instrucciones": raw[instrucciones_col] if instrucciones_col else "",
        "dia": raw[dia_col] if dia_col else "",
        "hora": raw[hora_col] if hora_col else "",
    })

    for k in ["area", "nro", "descripcion", "identificacion", "vence", "instrucciones", "dia", "hora"]:
        df[k] = df[k].map(clean_text)
    for k in ["codigo_ml", "codigo_universal", "sku"]:
        df[k] = df[k].map(norm_code)
    df["unidades"] = df["unidades"].map(to_int)

    df = df[(df["unidades"] > 0) & ((df["sku"] != "") | (df["codigo_ml"] != "") | (df["codigo_universal"] != ""))]
    return df.reset_index(drop=True), warnings


# ============================================================
# Carga directa desde PDF Mercado Libre + maestro Kame
# ============================================================

def valid_barcode_code(v) -> bool:
    c = norm_code(v)
    if not c or c in {"N/A", "NA", "SIN", "SINCODIGO"}:
        return False
    if not re.fullmatch(r"\d+", c):
        return False
    # Evita valores basura del maestro, por ejemplo "5".
    return 8 <= len(c) <= 18 and len(set(c)) > 1


def normalize_universal_code(v) -> str:
    c = norm_code(v)
    if valid_barcode_code(c):
        return c
    return "N/A"


def words_to_lines_text(words, y_tol: float = 3.0) -> str:
    """Agrupa palabras extraídas desde PDF respetando líneas visuales."""
    lines = []
    for w in words:
        added = False
        for line in lines:
            if abs(float(line[0]) - float(w.get("top", 0))) <= y_tol:
                line[1].append(w)
                added = True
                break
        if not added:
            lines.append([float(w.get("top", 0)), [w]])
    lines.sort(key=lambda x: x[0])
    out = []
    for _, ws in lines:
        ws.sort(key=lambda w: float(w.get("x0", 0)))
        out.append(" ".join(clean_text(w.get("text", "")) for w in ws if clean_text(w.get("text", ""))))
    return "\n".join([x for x in out if x])


def parse_ml_full_pdf(uploaded_pdf) -> tuple[pd.DataFrame, dict]:
    """Extrae productos desde el PDF de instrucciones de preparación de Mercado Libre.

    El PDF no es una tabla limpia: visualmente tiene columnas. Por eso se parsea con
    posiciones X/Y, no solo con texto plano. Devuelve una tabla base ML y totales
    esperados declarados por el PDF.
    """
    try:
        import pdfplumber
    except Exception as e:
        raise RuntimeError("Falta instalar la dependencia pdfplumber para leer PDFs. Agrega pdfplumber a requirements.txt.") from e

    pdf_bytes = uploaded_pdf.getvalue() if hasattr(uploaded_pdf, "getvalue") else uploaded_pdf.read()
    rows = []
    totals = {"expected_products": None, "expected_units": None, "shipment": ""}

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if pdf.pages:
            first_text = pdf.pages[0].extract_text() or ""
            m_ship = re.search(r"Envío\s*#\s*(\d+)", first_text, re.I)
            if m_ship:
                totals["shipment"] = m_ship.group(1)
            m = re.search(
                r"Productos\s+del\s+env[ií]o:\s*(\d+)\s*\|\s*Total\s+de\s+unidades:\s*(\d+)",
                first_text,
                re.I,
            )
            if m:
                totals["expected_products"] = int(m.group(1))
                totals["expected_units"] = int(m.group(2))

        for page_num, page in enumerate(pdf.pages, start=1):
            words = page.extract_words(x_tolerance=1, y_tolerance=3, keep_blank_chars=False) or []
            starts = []
            for i, w in enumerate(words):
                if (
                    float(w.get("x0", 999)) < 85
                    and clean_text(w.get("text", "")) == "Código"
                    and i + 1 < len(words)
                    and clean_text(words[i + 1].get("text", "")) == "ML:"
                ):
                    starts.append(float(w.get("top", 0)))

            for idx, start in enumerate(starts):
                end = starts[idx + 1] if idx + 1 < len(starts) else float(page.height) + 1
                prod_words = [w for w in words if float(w.get("x0", 0)) < 230 and start - 1 <= float(w.get("top", 0)) < end - 1]
                qty_words = [w for w in words if 225 <= float(w.get("x0", 0)) < 282 and start - 5 <= float(w.get("top", 0)) < end - 1]
                ident_words = [w for w in words if 282 <= float(w.get("x0", 0)) < 370 and start - 5 <= float(w.get("top", 0)) < end - 1]
                instr_words = [w for w in words if float(w.get("x0", 0)) >= 370 and start - 5 <= float(w.get("top", 0)) < end - 1]

                prod_text = words_to_lines_text(prod_words)
                flat = " ".join(prod_text.split())
                ml_m = re.search(r"Código\s+ML:\s*([A-Z0-9]+)", flat, re.I)
                sku_m = re.search(r"SKU:\s*([A-Z0-9]+)", flat, re.I)
                univ_m = re.search(r"Código\s+universal:\s*(.*?)\s+SKU:", flat, re.I)
                if not ml_m or not sku_m:
                    continue

                qty = 0
                for w in sorted(qty_words, key=lambda z: (float(z.get("top", 0)), float(z.get("x0", 0)))):
                    t = clean_text(w.get("text", ""))
                    if re.fullmatch(r"\d+", t):
                        qty = int(t)
                        break

                sku = norm_code(sku_m.group(1))
                desc_pdf = flat[sku_m.end():].strip()
                desc_pdf = re.sub(r"\bSUPERMERCADO\b.*$", "", desc_pdf, flags=re.I).strip()
                desc_pdf = re.sub(r"\bEtiquetado\s+obligatorio\b.*$", "", desc_pdf, flags=re.I).strip()

                ident = " ".join(words_to_lines_text(ident_words).split())
                if "SUPERMERCADO" in flat.upper():
                    ident = "SUPERMERCADO"
                elif re.search(r"c[oó]digo\s+universal", ident, re.I):
                    ident = "Código universal"
                elif re.search(r"etiquetado", ident, re.I):
                    ident = "Etiquetado obligatorio"

                instr = " ".join(words_to_lines_text(instr_words).replace("•", " ").split())

                rows.append({
                    "page": page_num,
                    "codigo_ml": norm_code(ml_m.group(1)),
                    "codigo_universal_pdf": normalize_universal_code(univ_m.group(1) if univ_m else ""),
                    "sku": sku,
                    "descripcion_ml": clean_text(desc_pdf),
                    "unidades": int(qty),
                    "identificacion": clean_text(ident),
                    "instrucciones": clean_text(instr),
                })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["unidades"].astype(int) > 0].reset_index(drop=True)
    return df, totals


def load_kame_master_maps(source=None) -> tuple[dict, dict, dict, int]:
    """Carga maestro Kame por SKU: descripción, familia y código de barras."""
    if source is None:
        if not MAESTRO_PATH.exists():
            return {}, {}, {}, 0
        source = MAESTRO_PATH

    raw = pd.read_excel(source, dtype=object).dropna(how="all")
    if raw.empty:
        return {}, {}, {}, 0
    raw.columns = [clean_text(c) for c in raw.columns]
    cols = list(raw.columns)
    sku_col = col_exact(cols, ["SKU", "SKU ML", "sku_ml"])
    desc_col = col_exact(cols, ["Descripción", "Descripcion", "Producto", "Title", "Titulo"])
    barcode_col = col_exact(cols, ["codigo de barras", "Código de barras", "Codigo Universal", "Código Universal", "EAN", "Barcode"])
    family_col = col_exact(cols, ["Familia", "Family", "Categoría", "Categoria"])
    if not sku_col:
        return {}, {}, {}, 0

    desc_map = {}
    family_map = {}
    barcode_map = {}
    for _, r in raw.iterrows():
        sku = norm_code(r.get(sku_col, ""))
        if not sku:
            continue
        if desc_col:
            desc = clean_text(r.get(desc_col, ""))
            if desc:
                desc_map[sku] = desc
        if family_col:
            fam = clean_text(r.get(family_col, ""))
            if fam:
                family_map[sku] = fam
        if barcode_col:
            bc = norm_code(r.get(barcode_col, ""))
            if valid_barcode_code(bc):
                barcode_map[sku] = bc
    return desc_map, family_map, barcode_map, len(raw)


def build_full_input_from_pdf(uploaded_pdf, master_source=None) -> tuple[pd.DataFrame, dict]:
    pdf_df, totals = parse_ml_full_pdf(uploaded_pdf)
    # En producción usamos el maestro fijo del repo: data/maestro_sku_ean.xlsx.
    # master_source se conserva solo por compatibilidad interna, pero la UI ya no lo solicita.
    desc_map, family_map, barcode_map, master_count = load_kame_master_maps(master_source)

    rows = []
    for idx, r in pdf_df.iterrows():
        sku = norm_code(r.get("sku", ""))
        alerts = []
        desc_kame = clean_text(desc_map.get(sku, ""))
        if not desc_kame:
            alerts.append("SKU no encontrado en maestro Kame; se usa descripción ML")
        desc_final = desc_kame or clean_text(r.get("descripcion_ml", ""))

        pdf_univ = normalize_universal_code(r.get("codigo_universal_pdf", ""))
        master_barcode = normalize_universal_code(barcode_map.get(sku, ""))
        if pdf_univ != "N/A":
            codigo_universal = pdf_univ
        elif master_barcode != "N/A":
            codigo_universal = master_barcode
            alerts.append("Código universal tomado desde maestro Kame")
        else:
            codigo_universal = "N/A"
            alerts.append("Código universal N/A")

        instrucciones = clean_text(r.get("instrucciones", ""))
        # Regla PDF Mercado Libre:
        # el PDF no trae una columna Vence como el Excel depurado, pero sí puede traer
        # una instrucción explícita sobre fecha de vencimiento. Para operación, basta
        # marcar Vence=SI cuando esa instrucción aparece en el texto de preparación.
        instr_norm = normalize_header(instrucciones)
        vence = "SI" if ("fecha de vencimiento" in instr_norm or "vencimiento" in instr_norm) else ""

        rows.append({
            "area": "",
            "nro": str(idx + 1),
            "codigo_ml": norm_code(r.get("codigo_ml", "")),
            "codigo_universal": codigo_universal,
            "sku": sku,
            "descripcion": desc_final,
            "unidades": int(r.get("unidades", 0) or 0),
            "identificacion": clean_text(r.get("identificacion", "")),
            "vence": vence,
            "dia": "",
            "hora": "",
            "instrucciones": instrucciones,
            "descripcion_ml": clean_text(r.get("descripcion_ml", "")),
            "familia_kame": clean_text(family_map.get(sku, "")),
            "alertas": " | ".join(alerts),
        })

    df = pd.DataFrame(rows)
    checks = {
        **totals,
        "detected_products": int(len(df)),
        "detected_units": int(df["unidades"].sum()) if not df.empty else 0,
        "master_rows": int(master_count),
        "sku_not_found": int(df["alertas"].str.contains("SKU no encontrado", na=False).sum()) if not df.empty else 0,
        "codigo_universal_na": int((df["codigo_universal"] == "N/A").sum()) if not df.empty else 0,
        "products_match": (totals.get("expected_products") in [None, 0]) or int(totals.get("expected_products")) == int(len(df)),
        "units_match": (totals.get("expected_units") in [None, 0]) or int(totals.get("expected_units")) == (int(df["unidades"].sum()) if not df.empty else 0),
    }
    return df, checks


def full_input_excel_bytes(df: pd.DataFrame) -> bytes:
    export_cols = [
        ("nro", "Nº"),
        ("codigo_ml", "Código ML"),
        ("codigo_universal", "Código Universal"),
        ("sku", "SKU"),
        ("descripcion", "Descripción"),
        ("unidades", "Unidades"),
        ("identificacion", "Identificación"),
        ("vence", "Vence"),
        ("instrucciones", "Instrucciones de preparación"),
        ("descripcion_ml", "Descripción ML"),
        ("familia_kame", "Familia Kame"),
        ("alertas", "Alertas"),
    ]
    out_df = pd.DataFrame()
    for src, dst in export_cols:
        out_df[dst] = df[src] if src in df.columns else ""
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="full_input", index=False)
        ws = writer.sheets["full_input"]
        for col_idx, col_name in enumerate(out_df.columns, start=1):
            width = 18
            if col_name in {"Descripción", "Instrucciones", "Instrucciones de preparación", "Descripción ML", "Alertas"}:
                width = 48
            ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = width
            if col_name in {"Código ML", "Código Universal", "SKU"}:
                for cell in ws.iter_cols(min_col=col_idx, max_col=col_idx, min_row=2):
                    for c in cell:
                        c.number_format = "@"
    bio.seek(0)
    return bio.getvalue()


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
    now = now_cl().isoformat(timespec="seconds")
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
    """Limpia solo el flujo activo de escaneo.

Mantiene métricas/tablas intactas y deja preparado el foco para el próximo código.
"""
    st.session_state["primary_validated"] = False
    st.session_state["primary_code"] = ""
    st.session_state["candidate_id"] = None
    st.session_state["candidate_mode"] = ""
    st.session_state["_last_scan_submit_sig"] = ""
    st.session_state["_clear_scan_inputs_next_run"] = True
    st.session_state["_focus_scan_primary_next_run"] = True


def focus_scan_primary_once():
    """Best-effort: intenta devolver el foco al primer input del escaneo PDA.

Streamlit no expone autofocus nativo para text_input; este script es defensivo
para PDA/navegador y no rompe si el navegador bloquea el foco.
"""
    if not st.session_state.get("_focus_scan_primary_next_run", True):
        return
    st.session_state["_focus_scan_primary_next_run"] = False
    components.html(
        """
        <script>
        const tryFocus = () => {
          try {
            const parentDoc = window.parent.document;
            const inputs = parentDoc.querySelectorAll('input');
            for (const input of inputs) {
              const aria = (input.getAttribute('aria-label') || '').toLowerCase();
              const ph = (input.getAttribute('placeholder') || '').toLowerCase();
              if (aria.includes('código ml') || aria.includes('codigo ml') || ph.includes('código') || ph.includes('codigo')) {
                input.focus();
                input.select();
                break;
              }
            }
          } catch(e) {}
        };
        setTimeout(tryFocus, 250);
        setTimeout(tryFocus, 750);
        </script>
        """,
        height=0,
    )


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
    if is_lote_closed(lote_id):
        st.error("Este lote está cerrado. Reabre el lote desde Supervisor antes de imprimir etiquetas.")
        return
    now = now_cl().isoformat(timespec="seconds")
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
    log_audit_event(lote_id, event_type="ZPL_REIMPRESO" if is_reprint else "ZPL_DESCARGADO", detail=f"Bloque {int(block['block_index'])}", qty=int(block.get("total_qty", 0)), mode="BLOQUE")


def register_individual_download(lote_id: int, item: dict, qty: int):
    if is_lote_closed(lote_id):
        st.error("Este lote está cerrado. Reabre el lote desde Supervisor antes de imprimir etiquetas.")
        return
    now = now_cl().isoformat(timespec="seconds")
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
    log_audit_event(lote_id, int(item.get("id")), "ZPL_INDIVIDUAL", clean_text(item.get("descripcion", "")), int(qty), item.get("codigo_ml", ""), item.get("sku", ""), "INDIVIDUAL")

# ============================================================
# Auditoría operacional Fase 1
# ============================================================

def log_audit_event(lote_id=None, item_id=None, event_type="", detail="", qty=None, codigo_ml="", sku="", mode=""):
    """Registra una acción operacional local. No bloquea la operación si falla."""
    try:
        now = now_cl().isoformat(timespec="seconds")
        with db() as c:
            c.execute(
                """
                INSERT INTO audit_events
                (lote_id, item_id, event_type, detail, qty, codigo_ml, sku, mode, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(lote_id) if lote_id is not None else None,
                    int(item_id) if item_id is not None else None,
                    clean_text(event_type), clean_text(detail),
                    int(qty) if qty is not None else None,
                    norm_code(codigo_ml), norm_code(sku), clean_text(mode), now,
                ),
            )
            c.commit()
    except Exception:
        pass


def get_audit_events(lote_id=None, limit=300) -> pd.DataFrame:
    with db() as c:
        if lote_id:
            return pd.read_sql_query(
                """
                SELECT created_at, event_type, detail, qty, codigo_ml, sku, mode, item_id
                FROM audit_events
                WHERE lote_id=?
                ORDER BY id DESC
                LIMIT ?
                """,
                c,
                params=(int(lote_id), int(limit)),
            )
        return pd.read_sql_query(
            """
            SELECT created_at, lote_id, event_type, detail, qty, codigo_ml, sku, mode, item_id
            FROM audit_events
            ORDER BY id DESC
            LIMIT ?
            """,
            c,
            params=(int(limit),),
        )


def get_recent_scans(lote_id: int, limit: int = 8) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT s.created_at, i.descripcion, i.codigo_ml, i.sku, s.cantidad, s.modo,
                   s.operador_validador, s.picking_code, s.picker_asignado
            FROM scans s
            LEFT JOIN items i ON i.id=s.item_id
            WHERE s.lote_id=?
            ORDER BY s.id DESC
            LIMIT ?
            """,
            c,
            params=(int(lote_id), int(limit)),
        )


def render_scan_incident_button(lote_id: int, items: pd.DataFrame, current_item=None):
    """Incidencias creadas desde Escaneo por código real del producto.
    No se crean incidencias generales por lote: el operador debe indicar Etiqueta ML, Código Universal o SKU.
    """
    default_code = ""
    if current_item is not None:
        try:
            default_code = norm_code(current_item.get("codigo_ml", "")) or norm_code(current_item.get("codigo_universal", "")) or norm_code(current_item.get("sku", ""))
        except Exception:
            default_code = ""

    with st.expander("Reportar incidencia por código", expanded=False):
        st.caption("Escanea o ingresa Etiqueta ML, Código Universal/EAN o SKU. La incidencia quedará asociada al producto encontrado en el lote activo.")
        with st.form("scan_incident_form", clear_on_submit=True):
            codigo_inc = st.text_input(
                "Etiqueta ML / Código Universal / SKU",
                value=default_code,
                key="scan_inc_codigo",
                placeholder="Escanea o escribe el código afectado",
            )
            c1, c2 = st.columns([2, 1])
            with c1:
                tipo_inc = st.selectbox("Tipo de incidencia", INCIDENCIA_TIPOS, key="scan_inc_tipo")
            with c2:
                qty_inc = st.number_input("Cantidad afectada", min_value=0, max_value=9999, value=0, step=1, key="scan_inc_qty")
            comentario_inc = st.text_area("Comentario", key="scan_inc_comentario", placeholder="Describe qué ocurrió: falta, daño, diferencia, etiqueta, mal embalaje, etc.")
            submit_inc = st.form_submit_button("Guardar incidencia", type="primary")

        if submit_inc:
            ok_inc, msg_inc = create_incidencia_por_codigo(
                lote_id,
                codigo_inc,
                tipo_inc,
                int(qty_inc),
                comentario_inc,
                "SIN_USUARIO",
            )
            if ok_inc:
                st.success(msg_inc)
                st.rerun()
            else:
                st.error(msg_inc)


# ============================================================
# Fase 2: Supervisor, incidencias, reimpresión controlada y cierre
# ============================================================

INCIDENCIA_TIPOS = [
    "Falta producto",
    "Producto dañado",
    "Producto mal embalado",
    "Código no coincide",
    "Cantidad menor",
    "Cantidad mayor",
    "Etiqueta dañada",
    "Otro",
]

AVISO_OPERACIONAL_TIPOS = [
    "Ajuste de cantidad",
    "Producto retirado del lote",
    "Preparar con observación",
    "No escanear / esperar instrucción",
    "Cambio autorizado por administración",
]

AVISO_OPERACIONAL_BLOQUEA = {
    "Producto retirado del lote",
    "No escanear / esperar instrucción",
}

AVISO_OPERACIONAL_REQUIERE_CONFIRMACION = {
    "Ajuste de cantidad",
    "Producto retirado del lote",
    "Cambio autorizado por administración",
}


def get_operator_name() -> str:
    return clean_text(st.session_state.get("operator_name", "")) or "SIN_USUARIO"


def get_lote_status(lote_id: int) -> str:
    lote = get_lote(lote_id)
    return clean_text(lote.get("status", "ACTIVO")) or "ACTIVO"


def is_lote_closed(lote_id: int) -> bool:
    return get_lote_status(lote_id).upper() == "CERRADO"


def item_tiene_incidencia_abierta(lote_id: int, item_id) -> bool:
    try:
        iid = int(item_id)
    except Exception:
        return False
    with db() as c:
        row = c.execute(
            """
            SELECT COUNT(*) AS n
            FROM incidencias
            WHERE lote_id=? AND item_id=? AND status='ABIERTA'
            """,
            (int(lote_id), iid),
        ).fetchone()
    return int(row["n"] or 0) > 0 if row else False


def get_incidencias(lote_id=None, status=None) -> pd.DataFrame:
    with db() as c:
        where = []
        params = []
        if lote_id:
            where.append("inc.lote_id=?")
            params.append(int(lote_id))
        if status and clean_text(status) != "Todas":
            where.append("inc.status=?")
            params.append(clean_text(status))
        sql_where = ("WHERE " + " AND ".join(where)) if where else ""
        return pd.read_sql_query(
            f"""
            SELECT inc.id, inc.created_at, inc.lote_id, inc.item_id, inc.tipo, inc.cantidad,
                   inc.comentario, inc.usuario, inc.status, inc.resolved_at, inc.resolved_by,
                   inc.resolution_comment,
                   COALESCE(i.codigo_ml, inc.codigo_ml, '') AS codigo_ml,
                   COALESCE(i.codigo_universal, inc.codigo_universal, '') AS codigo_universal,
                   COALESCE(i.sku, inc.sku, '') AS sku,
                   COALESCE(i.descripcion, inc.descripcion, '') AS descripcion
            FROM incidencias inc
            LEFT JOIN items i ON i.id=inc.item_id
            {sql_where}
            ORDER BY inc.id DESC
            """,
            c,
            params=params,
        )


def find_item_for_incidencia(lote_id: int, codigo: str) -> dict:
    """Busca el producto afectado por Etiqueta ML, Código Universal/EAN o SKU."""
    cn = norm_code(codigo)
    if not cn:
        return {}
    sku_master = norm_code(maestro_lookup(cn))
    with db() as c:
        row = c.execute(
            """
            SELECT *
            FROM items
            WHERE lote_id=?
              AND (
                    UPPER(COALESCE(codigo_ml,''))=?
                 OR UPPER(COALESCE(codigo_universal,''))=?
                 OR UPPER(COALESCE(sku,''))=?
                 OR (?<>'' AND UPPER(COALESCE(sku,''))=?)
              )
            ORDER BY id ASC
            LIMIT 1
            """,
            (int(lote_id), cn, cn, cn, sku_master, sku_master),
        ).fetchone()
    return dict(row) if row else {}


def create_incidencia(lote_id: int, item_id, tipo: str, cantidad: int, comentario: str, usuario: str):
    now = now_cl().isoformat(timespec="seconds")
    item = {}
    if item_id:
        with db() as c:
            row = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (int(item_id), int(lote_id))).fetchone()
            item = dict(row) if row else {}
    with db() as c:
        c.execute(
            """
            INSERT INTO incidencias
            (lote_id, item_id, tipo, cantidad, comentario, usuario, status, created_at,
             codigo_ml, codigo_universal, sku, descripcion)
            VALUES (?, ?, ?, ?, ?, ?, 'ABIERTA', ?, ?, ?, ?, ?)
            """,
            (
                int(lote_id),
                int(item_id) if item_id else None,
                clean_text(tipo),
                max(0, int(cantidad or 0)),
                clean_text(comentario),
                clean_text(usuario) or "SIN_USUARIO",
                now,
                norm_code(item.get("codigo_ml", "")),
                norm_code(item.get("codigo_universal", "")),
                norm_code(item.get("sku", "")),
                clean_text(item.get("descripcion", "")),
            ),
        )
        c.commit()

    # Respaldo externo: antes las incidencias solo quedaban en SQLite/auditoría local.
    # Este evento es el que el Apps Script usa para escribir en la hoja "incidencias".
    enqueue_backup_event("incidencia_creada", {
        **build_lote_payload(lote_id),
        "item_id": int(item_id) if item_id else "",
        "codigo_ml": norm_code(item.get("codigo_ml", "")),
        "codigo_universal": norm_code(item.get("codigo_universal", "")),
        "sku": norm_code(item.get("sku", "")),
        "descripcion": clean_text(item.get("descripcion", "")),
        "tipo": clean_text(tipo),
        "cantidad": max(0, int(cantidad or 0)),
        "comentario": clean_text(comentario),
        "usuario": clean_text(usuario) or "SIN_USUARIO",
        "status": "ABIERTA",
        "created_at": now,
    })

    log_audit_event(
        lote_id,
        int(item_id) if item_id else None,
        "INCIDENCIA_ABIERTA",
        f"{clean_text(tipo)} · {clean_text(comentario)}",
        max(0, int(cantidad or 0)),
        item.get("codigo_ml", ""),
        item.get("sku", ""),
        clean_text(usuario) or "SIN_USUARIO",
    )


def create_incidencia_por_codigo(lote_id: int, codigo: str, tipo: str, cantidad: int, comentario: str, usuario: str = "SIN_USUARIO"):
    """Crea incidencia desde Escaneo usando Etiqueta ML / Código Universal / SKU."""
    if is_lote_closed(lote_id):
        return False, "Este lote está cerrado. Reabre el lote desde Supervisor antes de registrar incidencias."
    codigo_norm = norm_code(codigo)
    if not codigo_norm:
        return False, "Ingresa una Etiqueta ML, Código Universal o SKU."
    item = find_item_for_incidencia(lote_id, codigo_norm)
    if not item:
        return False, "No encontré ese código en el lote activo. Revisa Etiqueta ML, Código Universal o SKU."
    if len(clean_text(comentario)) < 3:
        return False, "Agrega un comentario mínimo para que la incidencia sea útil."
    create_incidencia(lote_id, int(item["id"]), tipo, int(cantidad or 0), comentario, usuario or "SIN_USUARIO")
    return True, f"Incidencia registrada para SKU {clean_text(item.get('sku',''))}."


def resolve_incidencia(incidencia_id: int, usuario: str, comentario: str):
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        inc = c.execute("SELECT * FROM incidencias WHERE id=?", (int(incidencia_id),)).fetchone()
        if not inc:
            return False, "Incidencia no encontrada."
        if clean_text(inc["status"]) == "RESUELTA":
            return False, "La incidencia ya estaba resuelta."
        c.execute(
            """
            UPDATE incidencias
            SET status='RESUELTA', resolved_at=?, resolved_by=?, resolution_comment=?
            WHERE id=?
            """,
            (now, clean_text(usuario) or "SIN_USUARIO", clean_text(comentario), int(incidencia_id)),
        )
        c.commit()
    log_audit_event(int(inc["lote_id"]), inc["item_id"], "INCIDENCIA_RESUELTA", clean_text(comentario), inc["cantidad"], mode=clean_text(usuario) or "SIN_USUARIO")
    return True, "Incidencia resuelta."


def get_reimpresiones(lote_id=None) -> pd.DataFrame:
    with db() as c:
        if lote_id:
            return pd.read_sql_query(
                """
                SELECT r.created_at, r.scope, r.block_index, r.item_id, r.cantidad, r.motivo, r.usuario,
                       i.codigo_ml, i.sku, i.descripcion
                FROM reimpresiones r
                LEFT JOIN items i ON i.id=r.item_id
                WHERE r.lote_id=?
                ORDER BY r.id DESC
                """,
                c,
                params=(int(lote_id),),
            )
        return pd.read_sql_query("SELECT * FROM reimpresiones ORDER BY id DESC", c)


def get_label_blocks_df(lote_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT *
            FROM label_blocks
            WHERE lote_id=?
            ORDER BY block_index ASC
            """,
            c,
            params=(int(lote_id),),
        )


def register_controlled_block_reprint(lote_id: int, block: dict, motivo: str, usuario: str):
    if is_lote_closed(lote_id):
        return False, "Este lote está cerrado. Reabre el lote antes de reimprimir."
    motivo = clean_text(motivo)
    usuario = clean_text(usuario) or "SIN_USUARIO"
    if len(motivo) < 5:
        return False, "Debes ingresar un motivo claro de reimpresión."

    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        rec = c.execute(
            "SELECT * FROM label_blocks WHERE lote_id=? AND block_index=? AND block_key=?",
            (int(lote_id), int(block["block_index"]), clean_text(block["block_key"])),
        ).fetchone()
        if not rec:
            return False, "Este bloque aún no está impreso. Debe descargarse primero como impresión normal."
        c.execute(
            """
            UPDATE label_blocks
            SET status='REIMPRESO', download_count=download_count+1, last_printed_at=?,
                updated_at=?, last_reprint_reason=?, last_reprint_user=?
            WHERE lote_id=? AND block_index=? AND block_key=?
            """,
            (now, now, motivo, usuario, int(lote_id), int(block["block_index"]), clean_text(block["block_key"])),
        )
        c.execute(
            """
            INSERT INTO reimpresiones
            (lote_id, item_id, block_index, block_key, scope, cantidad, motivo, usuario, created_at)
            VALUES (?, NULL, ?, ?, 'BLOQUE', ?, ?, ?, ?)
            """,
            (int(lote_id), int(block["block_index"]), clean_text(block["block_key"]), int(block["total_qty"]), motivo, usuario, now),
        )

        rows = []
        for item in block["items"]:
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                clean_text(item.get("descripcion", "")), int(item.get("unidades", 0)), "BLOQUE", "NORMAL",
                int(block["block_index"]), clean_text(block["block_key"]), 1, now,
            ))
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                clean_text(item.get("descripcion", "")), LABEL_SEPARATOR_PER_PRODUCT, "BLOQUE", "SEPARADOR",
                int(block["block_index"]), clean_text(block["block_key"]), 1, now,
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

    enqueue_backup_event("reimpresion_controlada", {
        **build_lote_payload(lote_id),
        "item_id": "",
        "block_index": int(block["block_index"]),
        "block_key": clean_text(block["block_key"]),
        "scope": "BLOQUE",
        "cantidad": int(block["total_qty"]),
        "motivo": motivo,
        "usuario": usuario,
        "created_at": now,
    })
    log_audit_event(lote_id, event_type="REIMPRESION_CONTROLADA", detail=f"Bloque {int(block['block_index'])} · {motivo}", qty=int(block["total_qty"]), mode=usuario)
    return True, "Reimpresión registrada."


def register_controlled_item_reprint(lote_id: int, item: dict, qty: int, motivo: str, usuario: str):
    if is_lote_closed(lote_id):
        return False, "Este lote está cerrado. Reabre el lote antes de reimprimir."
    motivo = clean_text(motivo)
    usuario = clean_text(usuario) or "SIN_USUARIO"
    qty = max(1, int(qty or 1))
    if len(motivo) < 5:
        return False, "Debes ingresar un motivo claro de reimpresión."

    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        c.execute(
            """
            INSERT INTO reimpresiones
            (lote_id, item_id, block_index, block_key, scope, cantidad, motivo, usuario, created_at)
            VALUES (?, ?, NULL, NULL, 'PRODUCTO', ?, ?, ?, ?)
            """,
            (int(lote_id), int(item.get("id")), int(qty), motivo, usuario, now),
        )
        rows = [
            (int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
             clean_text(item.get("descripcion", "")), int(qty), "INDIVIDUAL", "NORMAL", None, None, 1, now),
            (int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
             clean_text(item.get("descripcion", "")), LABEL_SEPARATOR_PER_PRODUCT, "INDIVIDUAL", "SEPARADOR", None, None, 1, now),
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
    enqueue_backup_event("reimpresion_controlada", {
        **build_lote_payload(lote_id),
        "item_id": int(item.get("id")),
        "codigo_ml": norm_code(item.get("codigo_ml", "")),
        "codigo_universal": norm_code(item.get("codigo_universal", "")),
        "sku": norm_code(item.get("sku", "")),
        "descripcion": clean_text(item.get("descripcion", "")),
        "block_index": "",
        "block_key": "",
        "scope": "PRODUCTO",
        "cantidad": int(qty),
        "motivo": motivo,
        "usuario": usuario,
        "created_at": now,
    })
    log_audit_event(lote_id, int(item.get("id")), "REIMPRESION_CONTROLADA", f"Producto · {motivo}", qty, item.get("codigo_ml", ""), item.get("sku", ""), usuario)
    return True, "Reimpresión individual registrada."



def get_avisos_operacionales(lote_id=None, estado=None, item_id=None, visible_only: bool = False) -> pd.DataFrame:
    with db() as c:
        where = []
        params = []
        if lote_id:
            where.append("av.lote_id=?")
            params.append(int(lote_id))
        if estado and clean_text(estado) != "Todos":
            where.append("av.estado=?")
            params.append(clean_text(estado))
        if item_id:
            where.append("av.item_id=?")
            params.append(int(item_id))
        if visible_only:
            where.append("av.visible_operador=1")
        sql_where = ("WHERE " + " AND ".join(where)) if where else ""
        return pd.read_sql_query(
            f"""
            SELECT av.*, i.unidades AS unidades_actuales, i.acopiadas AS acopiadas_actuales
            FROM avisos_operacionales av
            LEFT JOIN items i ON i.id=av.item_id
            {sql_where}
            ORDER BY av.id DESC
            """,
            c,
            params=params,
        )


def get_avisos_activos_item(lote_id: int, item_id: int, visible_only: bool = True) -> pd.DataFrame:
    return get_avisos_operacionales(lote_id=lote_id, estado="ACTIVO", item_id=item_id, visible_only=visible_only)


def aviso_bloquea_operacion(avisos_df: pd.DataFrame) -> bool:
    if avisos_df is None or avisos_df.empty:
        return False
    return any(clean_text(x) in AVISO_OPERACIONAL_BLOQUEA for x in avisos_df["tipo_aviso"].fillna("").tolist())


def render_avisos_operacionales_scan(lote_id: int, item_id: int) -> bool:
    avisos = get_avisos_activos_item(lote_id, item_id, visible_only=True)
    if avisos.empty:
        return False
    bloquea = aviso_bloquea_operacion(avisos)
    for _, av in avisos.iterrows():
        tipo = clean_text(av.get("tipo_aviso", ""))
        msg = clean_text(av.get("mensaje_operador", ""))
        cantidad_nueva = av.get("cantidad_nueva")
        cantidad_txt = ""
        try:
            if cantidad_nueva is not None and clean_text(cantidad_nueva) != "" and int(cantidad_nueva) > 0:
                cantidad_txt = f"<br><b>Nueva cantidad objetivo:</b> {int(cantidad_nueva)}"
        except Exception:
            cantidad_txt = ""
        color = "#FEE2E2" if tipo in AVISO_OPERACIONAL_BLOQUEA else "#FEF3C7"
        border = "#EF4444" if tipo in AVISO_OPERACIONAL_BLOQUEA else "#F59E0B"
        titulo = "⛔ PRODUCTO CON BLOQUEO OPERACIONAL" if tipo in AVISO_OPERACIONAL_BLOQUEA else "⚠️ AVISO OPERACIONAL"
        st.markdown(f"""
        <div style="border:3px solid {border}; background:{color}; border-radius:18px; padding:18px; margin:14px 0;">
            <div style="font-size:1.65rem;font-weight:950;line-height:1.2;">{titulo}</div>
            <div style="font-size:1.25rem;font-weight:850;margin-top:8px;">{esc(tipo)}</div>
            <div style="font-size:1.15rem;margin-top:8px;">{esc(msg)}{cantidad_txt}</div>
        </div>
        """, unsafe_allow_html=True)
    return bloquea


def create_aviso_operacional(lote_id: int, item_id: int, tipo_aviso: str, mensaje_operador: str,
                             cantidad_nueva, confirmado_ml: bool, confirmado_inventario: bool,
                             visible_operador: bool, comentario_interno: str, created_by: str):
    if is_lote_closed(lote_id):
        return False, "Este lote está cerrado. Reabre el lote antes de crear avisos operacionales."
    tipo_aviso = clean_text(tipo_aviso)
    mensaje_operador = clean_text(mensaje_operador)
    created_by = clean_text(created_by) or "SIN_USUARIO"
    comentario_interno = clean_text(comentario_interno)
    if not item_id:
        return False, "Selecciona un producto."
    # El aviso puede crearse aunque las confirmaciones externas estén pendientes.
    # Esas confirmaciones se controlan después desde Supervisor y bloquean solo la resolución/cierre del aviso.
    if len(mensaje_operador) < 4:
        return False, "Ingresa un mensaje claro para el operador."
    if len(comentario_interno) < 4:
        return False, "Ingresa comentario interno para trazabilidad."

    with db() as c:
        row = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (int(item_id), int(lote_id))).fetchone()
        if not row:
            return False, "Producto no encontrado en el lote activo."
        item = dict(row)

    now = now_cl().isoformat(timespec="seconds")
    cantidad_original = int(item.get("unidades") or 0)
    try:
        cantidad_nueva_int = int(cantidad_nueva) if clean_text(cantidad_nueva) != "" else None
    except Exception:
        cantidad_nueva_int = None

    requiere_conf = tipo_aviso in AVISO_OPERACIONAL_REQUIERE_CONFIRMACION
    with db() as c:
        cur = c.execute(
            """
            INSERT INTO avisos_operacionales
            (lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion,
             tipo_aviso, mensaje_operador, cantidad_original, cantidad_nueva,
             requiere_ajuste_ml, requiere_ajuste_inventario, confirmado_ml, confirmado_inventario,
             visible_operador, estado, comentario_interno, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVO', ?, ?, ?)
            """,
            (
                int(lote_id), int(item_id), norm_code(item.get("codigo_ml", "")), norm_code(item.get("codigo_universal", "")),
                norm_code(item.get("sku", "")), clean_text(item.get("descripcion", "")), tipo_aviso, mensaje_operador,
                cantidad_original, cantidad_nueva_int, 1 if requiere_conf else 0, 1 if requiere_conf else 0,
                1 if confirmado_ml else 0, 1 if confirmado_inventario else 0, 1 if visible_operador else 0,
                comentario_interno, created_by, now,
            ),
        )
        aviso_id = int(cur.lastrowid)
        c.commit()

    enqueue_backup_event("aviso_operacional_creado", {
        **build_lote_payload(lote_id),
        "aviso_id": aviso_id,
        "item_id": int(item_id),
        "codigo_ml": norm_code(item.get("codigo_ml", "")),
        "codigo_universal": norm_code(item.get("codigo_universal", "")),
        "sku": norm_code(item.get("sku", "")),
        "descripcion": clean_text(item.get("descripcion", "")),
        "tipo_aviso": tipo_aviso,
        "mensaje_operador": mensaje_operador,
        "cantidad_original": cantidad_original,
        "cantidad_nueva": cantidad_nueva_int if cantidad_nueva_int is not None else "",
        "requiere_ajuste_ml": 1 if requiere_conf else 0,
        "requiere_ajuste_inventario": 1 if requiere_conf else 0,
        "confirmado_ml": 1 if confirmado_ml else 0,
        "confirmado_inventario": 1 if confirmado_inventario else 0,
        "confirmado_kame": 1 if confirmado_inventario else 0,
        "visible_operador": 1 if visible_operador else 0,
        "estado": "ACTIVO",
        "comentario_interno": comentario_interno,
        "created_by": created_by,
        "created_at": now,
        "tipo": tipo_aviso,
        "comentario": comentario_interno,
        "modo": "AVISO_OPERACIONAL",
    })
    log_audit_event(lote_id, int(item_id), "AVISO_OPERACIONAL_CREADO", f"{tipo_aviso} · {mensaje_operador}", cantidad_nueva_int, item.get("codigo_ml", ""), item.get("sku", ""), created_by)
    return True, "Aviso operacional creado."


def resolve_aviso_operacional(aviso_id: int, resolved_by: str, resolution_comment: str):
    resolved_by = clean_text(resolved_by) or "SIN_USUARIO"
    resolution_comment = clean_text(resolution_comment)
    if len(resolution_comment) < 3:
        return False, "Ingresa comentario de resolución."
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        row = c.execute("SELECT * FROM avisos_operacionales WHERE id=?", (int(aviso_id),)).fetchone()
        if not row:
            return False, "Aviso operacional no encontrado."
        aviso = dict(row)
        if clean_text(aviso.get("estado")) == "RESUELTO":
            return False, "Este aviso ya estaba resuelto."
        if int(aviso.get("requiere_ajuste_ml") or 0) == 1 and int(aviso.get("confirmado_ml") or 0) != 1:
            return False, "No puedes resolver este aviso: falta confirmar ajuste/rebaja en Mercado Libre."
        if int(aviso.get("requiere_ajuste_inventario") or 0) == 1 and int(aviso.get("confirmado_inventario") or 0) != 1:
            return False, "No puedes resolver este aviso: falta confirmar ajuste en inventario Kame."
        c.execute(
            """
            UPDATE avisos_operacionales
            SET estado='RESUELTO', visible_operador=0, resolved_at=?, resolved_by=?, resolution_comment=?
            WHERE id=?
            """,
            (now, resolved_by, resolution_comment, int(aviso_id)),
        )
        c.commit()

    enqueue_backup_event("aviso_operacional_resuelto", {
        **build_lote_payload(int(aviso["lote_id"])),
        "aviso_id": int(aviso_id),
        "item_id": int(aviso["item_id"]),
        "codigo_ml": norm_code(aviso.get("codigo_ml", "")),
        "codigo_universal": norm_code(aviso.get("codigo_universal", "")),
        "sku": norm_code(aviso.get("sku", "")),
        "descripcion": clean_text(aviso.get("descripcion", "")),
        "tipo_aviso": clean_text(aviso.get("tipo_aviso", "")),
        "estado": "RESUELTO",
        "visible_operador": 0,
        "resolved_at": now,
        "resolved_by": resolved_by,
        "resolution_comment": resolution_comment,
        "created_at": now,
        "tipo": clean_text(aviso.get("tipo_aviso", "")),
        "comentario": resolution_comment,
        "modo": "AVISO_OPERACIONAL",
    })
    log_audit_event(int(aviso["lote_id"]), int(aviso["item_id"]), "AVISO_OPERACIONAL_RESUELTO", resolution_comment, None, aviso.get("codigo_ml", ""), aviso.get("sku", ""), resolved_by)
    return True, "Aviso operacional resuelto y oculto al operador."


def confirmar_tarea_externa_aviso(aviso_id: int, tarea: str, usuario: str):
    """Marca una tarea externa pendiente del aviso operacional.

    tarea='ml' confirma rebaja/ajuste en Mercado Libre.
    tarea='kame' confirma ajuste de inventario Kame.
    """
    tarea = clean_text(tarea).lower()
    usuario = clean_text(usuario) or "SIN_USUARIO"
    now = now_cl().isoformat(timespec="seconds")
    if tarea not in {"ml", "kame"}:
        return False, "Tarea externa inválida."

    with db() as c:
        row = c.execute("SELECT * FROM avisos_operacionales WHERE id=?", (int(aviso_id),)).fetchone()
        if not row:
            return False, "Aviso operacional no encontrado."
        aviso = dict(row)
        if clean_text(aviso.get("estado")) == "RESUELTO":
            return False, "Este aviso ya está resuelto."

        if tarea == "ml":
            if int(aviso.get("confirmado_ml") or 0) == 1:
                return False, "Mercado Libre ya estaba confirmado."
            c.execute(
                """
                UPDATE avisos_operacionales
                SET confirmado_ml=1, confirmado_ml_at=?, confirmado_ml_by=?
                WHERE id=?
                """,
                (now, usuario, int(aviso_id)),
            )
            event_type = "aviso_operacional_ml_confirmado"
            audit_type = "AVISO_OPERACIONAL_ML_CONFIRMADO"
            detail = "Ajuste/rebaja confirmado en Mercado Libre"
            msg = "Mercado Libre confirmado."
        else:
            if int(aviso.get("confirmado_inventario") or 0) == 1:
                return False, "Inventario Kame ya estaba confirmado."
            c.execute(
                """
                UPDATE avisos_operacionales
                SET confirmado_inventario=1, confirmado_inventario_at=?, confirmado_inventario_by=?
                WHERE id=?
                """,
                (now, usuario, int(aviso_id)),
            )
            event_type = "aviso_operacional_kame_confirmado"
            audit_type = "AVISO_OPERACIONAL_KAME_CONFIRMADO"
            detail = "Ajuste confirmado en inventario Kame"
            msg = "Inventario Kame confirmado."
        c.commit()

    enqueue_backup_event(event_type, {
        **build_lote_payload(int(aviso["lote_id"])),
        "aviso_id": int(aviso_id),
        "item_id": int(aviso["item_id"]),
        "codigo_ml": norm_code(aviso.get("codigo_ml", "")),
        "codigo_universal": norm_code(aviso.get("codigo_universal", "")),
        "sku": norm_code(aviso.get("sku", "")),
        "descripcion": clean_text(aviso.get("descripcion", "")),
        "tipo_aviso": clean_text(aviso.get("tipo_aviso", "")),
        "mensaje_operador": clean_text(aviso.get("mensaje_operador", "")),
        "confirmado_ml": 1 if tarea == "ml" else int(aviso.get("confirmado_ml") or 0),
        "confirmado_inventario": 1 if tarea == "kame" else int(aviso.get("confirmado_inventario") or 0),
        "confirmado_kame": 1 if tarea == "kame" else int(aviso.get("confirmado_inventario") or 0),
        "confirmado_at": now,
        "confirmado_by": usuario,
        "created_at": now,
        "tipo": clean_text(aviso.get("tipo_aviso", "")),
        "comentario": detail,
        "modo": "AVISO_OPERACIONAL",
    })
    log_audit_event(int(aviso["lote_id"]), int(aviso["item_id"]), audit_type, detail, None, aviso.get("codigo_ml", ""), aviso.get("sku", ""), usuario)
    return True, msg


def supervisor_metrics(lote_id: int) -> dict:
    items = get_items(lote_id)
    if items.empty:
        return {"total": 0, "done": 0, "pending": 0, "incidencias_abiertas": 0, "avisos_activos": 0, "label_pending": 0}
    view = items.copy()
    view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
    labels = label_control_view(lote_id)
    incid = get_incidencias(lote_id, status="ABIERTA")
    avisos = get_avisos_operacionales(lote_id, estado="ACTIVO")
    return {
        "total": int(view["unidades"].sum()),
        "done": int(view["acopiadas"].sum()),
        "pending": int(view["pendiente"].sum()),
        "incidencias_abiertas": int(len(incid)),
        "avisos_activos": int(len(avisos)),
        "label_pending": int(labels["label_pending"].sum()) if not labels.empty else 0,
    }


def cierre_validaciones(lote_id: int, capacity: int = ROLL_CAPACITY_DEFAULT) -> tuple[bool, list[str], dict]:
    items = get_items(lote_id)
    issues = []
    if items.empty:
        issues.append("El lote no tiene productos.")
        return False, issues, {}
    view = items.copy()
    view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
    pending_units = int(view["pendiente"].sum())
    if pending_units > 0:
        issues.append(f"Quedan {pending_units} unidades pendientes de acopio/escaneo.")

    inc_abiertas = get_incidencias(lote_id, status="ABIERTA")
    if not inc_abiertas.empty:
        issues.append(f"Hay {len(inc_abiertas)} incidencia(s) abiertas.")

    avisos_activos = get_avisos_operacionales(lote_id, estado="ACTIVO")
    if not avisos_activos.empty:
        issues.append(f"Hay {len(avisos_activos)} aviso(s) operacional(es) activo(s).")

    label_view = label_control_view(lote_id)
    label_pending = int(label_view["label_pending"].sum()) if not label_view.empty else 0
    if label_pending > 0:
        issues.append(f"Quedan {label_pending} etiquetas normales pendientes de impresión.")

    blocks_expected = build_label_blocks(label_view, int(capacity)) if not label_view.empty else []
    blocks_db = get_label_blocks_df(lote_id)
    printed_keys = set(blocks_db["block_key"].astype(str).tolist()) if not blocks_db.empty else set()
    missing_blocks = [b for b in blocks_expected if str(b["block_key"]) not in printed_keys]
    if missing_blocks:
        issues.append(f"Faltan {len(missing_blocks)} bloque(s) ZPL por descargar/imprimir.")

    return len(issues) == 0, issues, {
        "pending_units": pending_units,
        "open_incidents": int(len(inc_abiertas)),
        "active_notices": int(len(avisos_activos)),
        "label_pending": label_pending,
        "expected_blocks": int(len(blocks_expected)),
        "printed_blocks": int(len(blocks_db)),
    }


def close_lote(lote_id: int, usuario: str, nota: str):
    if is_lote_closed(lote_id):
        return False, "Este lote ya está cerrado."
    ok, issues, _ = cierre_validaciones(lote_id)
    if not ok:
        return False, "No se puede cerrar: " + " ".join(issues)
    now = now_cl().isoformat(timespec="seconds")
    usuario = clean_text(usuario) or "SIN_USUARIO"
    with db() as c:
        c.execute(
            "UPDATE lotes SET status='CERRADO', closed_at=?, closed_by=?, close_note=? WHERE id=?",
            (now, usuario, clean_text(nota), int(lote_id)),
        )
        c.commit()
    enqueue_backup_event("lote_cerrado", {
        **build_lote_payload(lote_id),
        "created_at": now,
        "usuario": usuario,
        "comentario": clean_text(nota),
        "status": "CERRADO",
    })
    log_audit_event(lote_id, event_type="LOTE_CERRADO", detail=clean_text(nota), mode=usuario)
    return True, "Lote cerrado correctamente."


def reopen_lote(lote_id: int, usuario: str, motivo: str):
    usuario = clean_text(usuario) or "SIN_USUARIO"
    with db() as c:
        c.execute("UPDATE lotes SET status='ACTIVO', closed_at=NULL, closed_by=NULL, close_note=NULL WHERE id=?", (int(lote_id),))
        c.commit()
    enqueue_backup_event("lote_reabierto", {
        **build_lote_payload(lote_id),
        "created_at": now_cl().isoformat(timespec="seconds"),
        "usuario": usuario,
        "comentario": clean_text(motivo),
        "status": "ACTIVO",
    })
    log_audit_event(lote_id, event_type="LOTE_REABIERTO", detail=clean_text(motivo), mode=usuario)
    return True, "Lote reabierto."


# ============================================================
# Picking: listas imprimibles y trazabilidad de preparación
# ============================================================

PICKING_ACTIVE_STATES = ("CREADA", "IMPRESA", "EN PREPARACIÓN", "PARCIAL")


def next_picking_code(lote_id: int) -> str:
    with db() as c:
        row = c.execute("SELECT COUNT(*) AS n FROM picking_lists WHERE lote_id=?", (int(lote_id),)).fetchone()
    n = int(row["n"] or 0) + 1 if row else 1
    return f"PCK-{int(lote_id):03d}-{n:03d}"


def get_picking_list_meta(picking_list_id) -> dict:
    if not picking_list_id:
        return {}
    with db() as c:
        row = c.execute("SELECT * FROM picking_lists WHERE id=?", (int(picking_list_id),)).fetchone()
    return dict(row) if row else {}


def get_picking_lists(lote_id: int | None = None) -> pd.DataFrame:
    with db() as c:
        if lote_id:
            return pd.read_sql_query(
                """
                SELECT *
                FROM picking_lists
                WHERE lote_id=?
                ORDER BY id DESC
                """,
                c,
                params=(int(lote_id),),
            )
        return pd.read_sql_query("SELECT * FROM picking_lists ORDER BY id DESC", c)


def get_picking_items(picking_list_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT *
            FROM picking_list_items
            WHERE picking_list_id=?
            ORDER BY area, CAST(nro AS INTEGER), id
            """,
            c,
            params=(int(picking_list_id),),
        )


def get_picking_assigned_qty(lote_id: int) -> pd.DataFrame:
    with db() as c:
        df = pd.read_sql_query(
            """
            SELECT pli.item_id, SUM(pli.cantidad) AS asignado
            FROM picking_list_items pli
            JOIN picking_lists pl ON pl.id=pli.picking_list_id
            WHERE pli.lote_id=? AND pl.estado <> 'ANULADA'
            GROUP BY pli.item_id
            """,
            c,
            params=(int(lote_id),),
        )
    if df.empty:
        return pd.DataFrame(columns=["item_id", "asignado"])
    df["asignado"] = df["asignado"].fillna(0).astype(int)
    return df


def get_picking_available_items(lote_id: int) -> pd.DataFrame:
    """Productos disponibles para listas de picking.

    Regla operacional: un producto/SKU se asigna completo a una sola lista activa.
    No se permite dividir cantidades del mismo producto entre listas, porque eso
    desordena el papel y la trazabilidad. Si ya tiene cualquier cantidad asignada
    en una lista no anulada, queda bloqueado para nuevas listas.
    """
    items = get_items(lote_id)
    if items.empty:
        return items
    assigned = get_picking_assigned_qty(lote_id)
    view = items.merge(assigned, left_on="id", right_on="item_id", how="left")
    view["asignado"] = view["asignado"].fillna(0).astype(int)
    view["ya_asignado"] = view["asignado"].astype(int) > 0
    view["disponible_asignar"] = view.apply(
        lambda r: int(r["unidades"]) if not bool(r["ya_asignado"]) else 0,
        axis=1,
    )
    view["estado_asignacion"] = view["ya_asignado"].map(lambda x: "YA ASIGNADO" if x else "DISPONIBLE")
    return view


def get_picking_validation_summary(picking_list_id: int) -> pd.DataFrame:
    items = get_picking_items(picking_list_id)
    if items.empty:
        return items
    with db() as c:
        scans = pd.read_sql_query(
            """
            SELECT item_id, SUM(cantidad) AS validado_pda, MAX(created_at) AS ultimo_validado
            FROM scans
            WHERE picking_list_id=?
            GROUP BY item_id
            """,
            c,
            params=(int(picking_list_id),),
        )
    if scans.empty:
        items["validado_pda"] = 0
        items["ultimo_validado"] = ""
    else:
        items = items.merge(scans, on="item_id", how="left")
        items["validado_pda"] = items["validado_pda"].fillna(0).astype(int)
        items["ultimo_validado"] = items["ultimo_validado"].fillna("")
    items["pendiente_picking"] = (items["cantidad"].astype(int) - items["validado_pda"].astype(int)).clip(lower=0)
    def estado_row(r):
        req = int(r["cantidad"])
        val = int(r["validado_pda"])
        if val == 0:
            return "SIN VALIDAR"
        if val < req:
            return "PARCIAL"
        if val == req:
            return "COMPLETO"
        return "SOBREVALIDADO"
    items["estado_validacion"] = items.apply(estado_row, axis=1)
    return items


def item_in_picking_list(picking_list_id, item_id) -> bool:
    if not picking_list_id:
        return True
    with db() as c:
        row = c.execute(
            "SELECT COUNT(*) AS n FROM picking_list_items WHERE picking_list_id=? AND item_id=?",
            (int(picking_list_id), int(item_id)),
        ).fetchone()
    return int(row["n"] or 0) > 0 if row else False


def picking_pending_for_item(picking_list_id, item_id) -> dict:
    if not picking_list_id:
        return {"cantidad": None, "validado_pda": 0, "pendiente": None}
    with db() as c:
        item = c.execute(
            "SELECT cantidad FROM picking_list_items WHERE picking_list_id=? AND item_id=?",
            (int(picking_list_id), int(item_id)),
        ).fetchone()
        if not item:
            return {"cantidad": 0, "validado_pda": 0, "pendiente": 0}
        val = c.execute(
            "SELECT COALESCE(SUM(cantidad),0) AS n FROM scans WHERE picking_list_id=? AND item_id=?",
            (int(picking_list_id), int(item_id)),
        ).fetchone()
    cantidad = int(item["cantidad"] or 0)
    validado = int(val["n"] or 0) if val else 0
    return {"cantidad": cantidad, "validado_pda": validado, "pendiente": max(cantidad - validado, 0)}


def create_picking_list(lote_id: int, asignado_a: str, created_by: str, comentario: str, selected_rows: list[dict]):
    asignado_a = clean_text(asignado_a)
    created_by = clean_text(created_by) or "SIN_USUARIO"
    comentario = clean_text(comentario)
    if not asignado_a:
        return False, "Debes indicar a quién se asigna la lista."
    rows_clean = []
    seen_items = set()
    for r in selected_rows:
        item_id = int(r.get("id") or r.get("item_id") or 0)
        cantidad = int(r.get("unidades") or r.get("cantidad") or 0)
        ya_asignado = bool(r.get("ya_asignado")) or int(r.get("asignado") or 0) > 0
        disponible = int(r.get("disponible_asignar") or 0)
        if item_id and cantidad > 0:
            if item_id in seen_items:
                continue
            if ya_asignado or disponible <= 0:
                return False, f"El producto item {item_id} ya está asignado en otra lista activa. Anula esa lista si necesitas reasignarlo."
            # Regla: se asigna el producto completo, nunca una cantidad parcial.
            rows_clean.append((item_id, cantidad))
            seen_items.add(item_id)
    if not rows_clean:
        return False, "Selecciona al menos un producto disponible."

    # Validación defensiva contra datos desactualizados en pantalla: ningún item
    # seleccionado puede estar ya asignado a otra lista activa/no anulada.
    with db() as c:
        for item_id, _cantidad in rows_clean:
            row = c.execute(
                """
                SELECT COALESCE(SUM(pli.cantidad),0) AS n
                FROM picking_list_items pli
                JOIN picking_lists pl ON pl.id=pli.picking_list_id
                WHERE pli.lote_id=? AND pli.item_id=? AND pl.estado <> 'ANULADA'
                """,
                (int(lote_id), int(item_id)),
            ).fetchone()
            if int(row["n"] or 0) > 0:
                return False, f"El producto item {item_id} ya fue asignado a otra lista activa."

    now = now_cl().isoformat(timespec="seconds")
    codigo = next_picking_code(lote_id)
    with db() as c:
        cur = c.execute(
            """
            INSERT INTO picking_lists
            (lote_id, codigo_lista, asignado_a, estado, created_by, comentario, created_at)
            VALUES (?, ?, ?, 'CREADA', ?, ?, ?)
            """,
            (int(lote_id), codigo, asignado_a, created_by, comentario, now),
        )
        list_id = int(cur.lastrowid)
        inserted_items = []
        for item_id, cantidad in rows_clean:
            item = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (int(item_id), int(lote_id))).fetchone()
            if not item:
                continue
            c.execute(
                """
                INSERT INTO picking_list_items
                (picking_list_id, lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion,
                 cantidad, area, nro, estado, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDIENTE', ?)
                """,
                (
                    list_id, int(lote_id), int(item_id), norm_code(item["codigo_ml"]), norm_code(item["codigo_universal"]),
                    norm_code(item["sku"]), clean_text(item["descripcion"]), int(cantidad), clean_text(item["area"]),
                    clean_text(item["nro"]), now,
                ),
            )
            inserted_items.append({
                "item_id": int(item_id),
                "codigo_ml": norm_code(item["codigo_ml"]),
                "codigo_universal": norm_code(item["codigo_universal"]),
                "sku": norm_code(item["sku"]),
                "descripcion": clean_text(item["descripcion"]),
                "cantidad": int(cantidad),
                "area": clean_text(item["area"]),
                "nro": clean_text(item["nro"]),
            })
        c.commit()

    total_units = sum(int(x["cantidad"]) for x in inserted_items)
    enqueue_backup_event("picking_lista_creada", {
        **build_lote_payload(lote_id),
        "picking_list_id": list_id,
        "picking_code": codigo,
        "codigo_lista": codigo,
        "asignado_a": asignado_a,
        "estado": "CREADA",
        "created_by": created_by,
        "comentario": comentario,
        "created_at": now,
        "productos": len(inserted_items),
        "cantidad": total_units,
        "items": inserted_items,
        "tipo": "PICKING",
        "modo": "PICKING",
    })
    log_audit_event(lote_id, event_type="PICKING_LISTA_CREADA", detail=f"{codigo} asignada a {asignado_a}", qty=total_units, mode=created_by)
    return True, f"Lista {codigo} creada para {asignado_a}."


def mark_picking_printed(picking_list_id: int, usuario: str = ""):
    usuario = clean_text(usuario) or "SIN_USUARIO"
    now = now_cl().isoformat(timespec="seconds")
    meta = get_picking_list_meta(picking_list_id)
    if not meta:
        return
    with db() as c:
        c.execute(
            "UPDATE picking_lists SET estado=CASE WHEN estado='CREADA' THEN 'IMPRESA' ELSE estado END, printed_at=COALESCE(printed_at, ?) WHERE id=?",
            (now, int(picking_list_id)),
        )
        c.commit()
    enqueue_backup_event("picking_lista_impresa", {
        **build_lote_payload(int(meta["lote_id"])),
        "picking_list_id": int(picking_list_id),
        "picking_code": clean_text(meta.get("codigo_lista", "")),
        "codigo_lista": clean_text(meta.get("codigo_lista", "")),
        "asignado_a": clean_text(meta.get("asignado_a", "")),
        "created_at": now,
        "usuario": usuario,
        "estado": "IMPRESA",
        "tipo": "PICKING",
        "modo": "PICKING",
    })
    log_audit_event(int(meta["lote_id"]), event_type="PICKING_LISTA_IMPRESA", detail=clean_text(meta.get("codigo_lista", "")), mode=usuario)


def complete_picking_list(picking_list_id: int, usuario: str, comentario: str = ""):
    usuario = clean_text(usuario) or "SIN_USUARIO"
    comentario = clean_text(comentario)
    meta = get_picking_list_meta(picking_list_id)
    if not meta:
        return False, "Lista no encontrada."
    if clean_text(meta.get("estado")) == "ANULADA":
        return False, "La lista está anulada."
    summary = get_picking_validation_summary(picking_list_id)
    pending = int(summary["pendiente_picking"].sum()) if not summary.empty else 0
    if pending > 0:
        return False, f"No puedes completar la lista: quedan {pending} unidades pendientes por validar en PDA."
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        c.execute("UPDATE picking_lists SET estado='COMPLETADA', completed_at=? WHERE id=?", (now, int(picking_list_id)))
        c.commit()
    enqueue_backup_event("picking_lista_completada", {
        **build_lote_payload(int(meta["lote_id"])),
        "picking_list_id": int(picking_list_id),
        "picking_code": clean_text(meta.get("codigo_lista", "")),
        "codigo_lista": clean_text(meta.get("codigo_lista", "")),
        "asignado_a": clean_text(meta.get("asignado_a", "")),
        "created_at": now,
        "usuario": usuario,
        "comentario": comentario,
        "estado": "COMPLETADA",
        "tipo": "PICKING",
        "modo": "PICKING",
    })
    log_audit_event(int(meta["lote_id"]), event_type="PICKING_LISTA_COMPLETADA", detail=f"{meta.get('codigo_lista','')} · {comentario}", mode=usuario)
    return True, "Lista de picking completada."


def cancel_picking_list(picking_list_id: int, usuario: str, motivo: str):
    usuario = clean_text(usuario) or "SIN_USUARIO"
    motivo = clean_text(motivo)
    if len(motivo) < 3:
        return False, "Ingresa motivo de anulación."
    meta = get_picking_list_meta(picking_list_id)
    if not meta:
        return False, "Lista no encontrada."
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        c.execute(
            "UPDATE picking_lists SET estado='ANULADA', anulada_at=?, anulada_by=?, anulada_motivo=? WHERE id=?",
            (now, usuario, motivo, int(picking_list_id)),
        )
        c.commit()
    enqueue_backup_event("picking_lista_anulada", {
        **build_lote_payload(int(meta["lote_id"])),
        "picking_list_id": int(picking_list_id),
        "picking_code": clean_text(meta.get("codigo_lista", "")),
        "codigo_lista": clean_text(meta.get("codigo_lista", "")),
        "asignado_a": clean_text(meta.get("asignado_a", "")),
        "created_at": now,
        "usuario": usuario,
        "comentario": motivo,
        "estado": "ANULADA",
        "tipo": "PICKING",
        "modo": "PICKING",
    })
    log_audit_event(int(meta["lote_id"]), event_type="PICKING_LISTA_ANULADA", detail=f"{meta.get('codigo_lista','')} · {motivo}", mode=usuario)
    return True, "Lista de picking anulada."


def picking_lists_with_progress(lote_id: int) -> pd.DataFrame:
    lists = get_picking_lists(lote_id)
    if lists.empty:
        return lists
    rows = []
    for r in lists.itertuples(index=False):
        summary = get_picking_validation_summary(int(r.id))
        productos = len(summary) if not summary.empty else 0
        unidades = int(summary["cantidad"].sum()) if not summary.empty else 0
        validado = int(summary["validado_pda"].sum()) if not summary.empty else 0
        pendiente = max(unidades - validado, 0)
        estado_calc = clean_text(r.estado)
        if estado_calc not in {"ANULADA", "COMPLETADA"}:
            if validado > 0 and pendiente > 0:
                estado_calc = "PARCIAL"
            elif unidades > 0 and pendiente == 0:
                estado_calc = "COMPLETADA"
        rows.append({
            "id": int(r.id),
            "Lista": clean_text(r.codigo_lista),
            "Asignado a": clean_text(r.asignado_a),
            "Productos": productos,
            "Unidades": unidades,
            "Validado PDA": validado,
            "Pendiente": pendiente,
            "Estado": estado_calc,
            "Creada": fmt_dt(r.created_at),
            "Impresa": fmt_dt(getattr(r, "printed_at", "")),
        })
    return pd.DataFrame(rows)


def build_picking_print_html(picking_list_id: int) -> str:
    meta = get_picking_list_meta(picking_list_id)
    items = get_picking_items(picking_list_id)
    lote = get_lote(int(meta.get("lote_id", 0))) if meta else {}
    rows_html = []
    for r in items.itertuples(index=False):
        rows_html.append(f"""
        <tr>
          <td class="check">☐</td>
          <td class="codecell"><strong>{esc(r.codigo_ml)}</strong><br><span>{esc(r.codigo_universal)}</span></td>
          <td class="skucell">{esc(r.sku)}</td>
          <td class="desccell">{esc(r.descripcion)}</td>
          <td class="qty">{int(r.cantidad)}</td>
          <td class="obs"></td>
        </tr>
        """)
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{esc(meta.get('codigo_lista','PICKING'))}</title>
<style>
  @page {{ size: A4 landscape; margin: 10mm; }}
  body {{ font-family: Arial, sans-serif; margin: 14px; color: #111; }}
  .top {{ display:flex; justify-content:space-between; align-items:flex-start; border-bottom:2px solid #111; padding-bottom:10px; margin-bottom:14px; }}
  .code {{ font-size:34px; font-weight:900; letter-spacing:1px; }}
  .meta {{ font-size:13px; line-height:1.45; }}
  h1 {{ font-size:20px; margin:0 0 6px 0; }}
  table {{ width:100%; border-collapse:collapse; font-size:12px; table-layout:fixed; }}
  th, td {{ border:1px solid #333; padding:4px; vertical-align:top; }}
  th {{ background:#eee; }}
  col.col-ok {{ width:34px; }}
  col.col-code {{ width:128px; }}
  col.col-sku {{ width:98px; }}
  col.col-qty {{ width:44px; }}
  col.col-obs {{ width:310px; }}
  .check {{ font-size:18px; text-align:center; padding:2px; }}
  .qty {{ font-size:16px; font-weight:900; text-align:center; padding:3px 2px; }}
  .codecell {{ font-size:11px; line-height:1.25; word-break:break-word; }}
  .skucell {{ font-size:11px; line-height:1.25; word-break:break-word; }}
  .desccell {{ font-size:12px; line-height:1.25; }}
  .obs {{ min-width:0; }}
  .firma-wrap {{ margin-top:28px; padding-top:10px; }}
  .firma-linea {{ margin-top:30px; width:320px; border-top:1.5px solid #111; padding-top:6px; font-size:13px; }}
  @media print {{ body {{ margin: 0; }} .no-print {{ display:none; }} }}
</style>
</head>
<body>
<div class="top">
  <div>
    <h1>FERRETERÍA AURORA - LISTA DE PICKING FULL</h1>
    <div class="meta">
      <strong>Lote:</strong> {esc(lote.get('nombre',''))}<br>
      <strong>Asignado a:</strong> {esc(meta.get('asignado_a',''))}<br>
      <strong>Fecha impresión:</strong> {fmt_dt(now_cl().isoformat(timespec='seconds'))}<br>
      <strong>Comentario:</strong> {esc(meta.get('comentario',''))}
    </div>
  </div>
  <div class="code">{esc(meta.get('codigo_lista',''))}</div>
</div>
<table>
<colgroup>
  <col class="col-ok">
  <col class="col-code">
  <col class="col-sku">
  <col class="col-desc">
  <col class="col-qty">
  <col class="col-obs">
</colgroup>
<thead>
<tr>
  <th>OK</th><th>Código ML / Universal</th><th>SKU</th><th>Descripción</th><th>Cant.</th><th>Obs.</th>
</tr>
</thead>
<tbody>
{''.join(rows_html)}
</tbody>
</table>
<div class="firma-wrap">
  <div class="firma-linea">Firma pickeador</div>
</div>
<script>window.onload = function(){{ setTimeout(function(){{ window.print(); }}, 300); }};</script>
</body>
</html>"""


def render_picking_module(active_lote: int):
    st.subheader("Listas de Picking")
    if not active_lote:
        st.warning("Primero selecciona o crea un lote FULL.")
        return
    lote = get_lote(active_lote)
    items_av = get_picking_available_items(active_lote)
    lists_progress = picking_lists_with_progress(active_lote)
    total_units = int(items_av["unidades"].sum()) if not items_av.empty else 0
    assigned_units = int(items_av["asignado"].sum()) if not items_av.empty and "asignado" in items_av.columns else 0
    available_units = int(items_av["disponible_asignar"].sum()) if not items_av.empty and "disponible_asignar" in items_av.columns else 0
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Unidades lote", total_units)
    m2.metric("Unidades asignadas", assigned_units)
    m3.metric("Unidades sin asignar", available_units)
    m4.metric("Listas", 0 if lists_progress.empty else len(lists_progress))
    st.caption(f"Lote: {clean_text(lote.get('nombre',''))}")

    tab_resumen, tab_crear, tab_detalle = st.tabs(["Resumen", "Crear lista", "Detalle / impresión"])

    with tab_resumen:
        if lists_progress.empty:
            st.info("Aún no hay listas de picking para este lote.")
        else:
            st.dataframe(lists_progress.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True, height=330)
        if not items_av.empty:
            sin_asignar = items_av[items_av["disponible_asignar"].astype(int) > 0]
            with st.expander("Productos sin asignar", expanded=False):
                show = sin_asignar[["area", "nro", "codigo_ml", "sku", "descripcion", "unidades", "estado_asignacion"]].copy()
                st.dataframe(show, use_container_width=True, hide_index=True, height=320)

    with tab_crear:
        if items_av.empty:
            st.warning("El lote no tiene productos.")
        else:
            asignado_a = st.text_input("Asignado a", key="pick_asignado_a", placeholder="Nombre del picker")
            created_by = "ADMIN"
            comentario = st.text_input("Comentario", key="pick_comentario", placeholder="Opcional")
            q = st.text_input("Buscar producto", key="pick_search", placeholder="SKU, Código ML o descripción")
            st.info("Regla operativa: cada producto seleccionado se asigna completo a esta lista. No se dividen cantidades del mismo SKU entre listas activas.")
            solo_disp = st.checkbox("Mostrar solo productos sin asignar", value=True, key="pick_solo_disp")
            base = items_av.copy()
            if solo_disp:
                base = base[base["disponible_asignar"].astype(int) > 0]
            qn = normalize_header(q)
            if qn:
                mask = (
                    base["sku"].astype(str).map(normalize_header).str.contains(qn, na=False) |
                    base["codigo_ml"].astype(str).map(normalize_header).str.contains(qn, na=False) |
                    base["descripcion"].astype(str).map(normalize_header).str.contains(qn, na=False)
                )
                base = base[mask]
            base = base[["id", "area", "nro", "codigo_ml", "sku", "descripcion", "unidades", "asignado", "disponible_asignar", "estado_asignacion", "ya_asignado"]].copy()
            base.insert(0, "seleccionar", False)
            edited = st.data_editor(
                base,
                use_container_width=True,
                hide_index=True,
                height=430,
                column_config={
                    "seleccionar": st.column_config.CheckboxColumn("Seleccionar"),
                    "id": None,
                    "ya_asignado": None,
                    "disponible_asignar": None,
                },
                disabled=["area", "nro", "codigo_ml", "sku", "descripcion", "unidades", "asignado", "disponible_asignar", "estado_asignacion", "ya_asignado"],
                key="pick_editor",
            )
            selected = edited[(edited["seleccionar"] == True) & (edited["disponible_asignar"].astype(int) > 0)] if not edited.empty else pd.DataFrame()
            st.caption(f"Seleccionados: {len(selected)} productos · {int(selected['unidades'].sum()) if not selected.empty else 0} unidades completas")
            if st.button("Crear lista de picking", type="primary", disabled=selected.empty):
                ok, msg = create_picking_list(active_lote, asignado_a, created_by, comentario, selected.to_dict("records"))
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

    with tab_detalle:
        lists = get_picking_lists(active_lote)
        if lists.empty:
            st.info("No hay listas para revisar.")
        else:
            options = {f"{r.codigo_lista} · {r.asignado_a} · {r.estado}": int(r.id) for r in lists.itertuples(index=False)}
            selected_label = st.selectbox("Lista", list(options.keys()), key="pick_detail_select")
            list_id = options[selected_label]
            meta = get_picking_list_meta(list_id)
            summary = get_picking_validation_summary(list_id)
            d1, d2, d3, d4 = st.columns(4)
            unidades = int(summary["cantidad"].sum()) if not summary.empty else 0
            validado = int(summary["validado_pda"].sum()) if not summary.empty else 0
            d1.metric("Lista", clean_text(meta.get("codigo_lista", "")))
            d2.metric("Asignado a", clean_text(meta.get("asignado_a", "")))
            d3.metric("Validado PDA", f"{validado}/{unidades}")
            d4.metric("Estado", clean_text(meta.get("estado", "")))
            if not summary.empty:
                show = summary[["area", "nro", "codigo_ml", "sku", "descripcion", "cantidad", "validado_pda", "pendiente_picking", "estado_validacion"]].copy()
                st.dataframe(show, use_container_width=True, hide_index=True, height=360)
            html_print = build_picking_print_html(list_id)
            fname = f"{clean_text(meta.get('codigo_lista','picking'))}.html"
            st.download_button(
                "Imprimir / descargar hoja HTML",
                data=html_print,
                file_name=fname,
                mime="text/html",
                key=f"print_picking_{list_id}_{clean_text(meta.get('estado',''))}",
                on_click=mark_picking_printed,
                args=(list_id, get_operator_name()),
            )
            col_a, col_b = st.columns(2)
            with col_a:
                comp_user = st.text_input("Usuario cierre lista", key=f"pick_complete_user_{list_id}", value=get_operator_name())
                comp_comment = st.text_input("Comentario cierre", key=f"pick_complete_comment_{list_id}")
                if st.button("Marcar lista como completada", key=f"complete_pick_{list_id}"):
                    ok, msg = complete_picking_list(list_id, comp_user, comp_comment)
                    if ok:
                        st.success(msg); st.rerun()
                    else:
                        st.error(msg)
            with col_b:
                cancel_user = st.text_input("Usuario anulación", key=f"pick_cancel_user_{list_id}", value=get_operator_name())
                cancel_reason = st.text_input("Motivo anulación", key=f"pick_cancel_reason_{list_id}")
                if st.button("Anular lista", key=f"cancel_pick_{list_id}"):
                    ok, msg = cancel_picking_list(list_id, cancel_user, cancel_reason)
                    if ok:
                        st.success(msg); st.rerun()
                    else:
                        st.error(msg)

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
        scans = pd.read_sql_query("SELECT created_at, item_id, scan_primario, scan_secundario, cantidad, modo, operador_validador, picking_list_id, picking_code, picker_asignado FROM scans WHERE lote_id=? ORDER BY id DESC", c, params=(lote_id,))
    audit = get_audit_events(lote_id, limit=5000)
    incidencias = get_incidencias(lote_id)
    reimpresiones = get_reimpresiones(lote_id)
    avisos = get_avisos_operacionales(lote_id)
    picking_lists = get_picking_lists(lote_id)
    with db() as c:
        picking_items = pd.read_sql_query("SELECT * FROM picking_list_items WHERE lote_id=? ORDER BY picking_list_id, id", c, params=(lote_id,))
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        items.to_excel(writer, sheet_name="control_full", index=False)
        scans.to_excel(writer, sheet_name="escaneos", index=False)
        audit.to_excel(writer, sheet_name="auditoria", index=False)
        incidencias.to_excel(writer, sheet_name="incidencias", index=False)
        reimpresiones.to_excel(writer, sheet_name="reimpresiones", index=False)
        avisos.to_excel(writer, sheet_name="avisos_operacionales", index=False)
        picking_lists.to_excel(writer, sheet_name="picking_listas", index=False)
        picking_items.to_excel(writer, sheet_name="picking_items", index=False)
    return out.getvalue()



# ============================================================
# Vistas integradas de Supervisor
# ============================================================

def render_control_integrado(active_lote: int):
    """Control operativo integrado al panel Supervisor."""
    lote = get_lote(active_lote)
    items = get_items(active_lote)
    if items.empty:
        st.warning("El lote no tiene productos.")
        return

    view = items.copy()
    view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
    view["estado"] = view["pendiente"].apply(lambda x: "COMPLETO" if int(x) == 0 else "PENDIENTE")
    scans = get_last_scans(active_lote)
    if not scans.empty:
        view = view.merge(scans, left_on="id", right_on="item_id", how="left")
    else:
        view["procesado_at"] = ""

    c1, c2, c3, c4 = st.columns(4)
    total = int(view["unidades"].sum())
    done = int(view["acopiadas"].sum())
    c1.metric("Unidades", total)
    c2.metric("Acopiadas", done)
    c3.metric("Pendientes", max(total - done, 0))
    c4.metric("Avance", f"{(done / total * 100) if total else 0:.1f}%")
    st.caption(f"Archivo: {lote.get('archivo','')} · Hoja: {lote.get('hoja','')} · Cargado: {fmt_dt(lote.get('created_at',''))}")

    filtro = st.selectbox("Filtro", ["Todos", "Pendientes", "Completos", "Supermercado"], key="sup_control_filtro")
    show = view
    if filtro == "Pendientes":
        show = view[view["pendiente"] > 0]
    elif filtro == "Completos":
        show = view[view["pendiente"] == 0]
    elif filtro == "Supermercado":
        show = view[view["identificacion"].map(is_supermercado)]

    option_rows = []
    option_map = {"": None}
    for _, sr in show.iterrows():
        desc = clean_text(sr.get("descripcion", ""))
        sku = clean_text(sr.get("sku", ""))
        ml = clean_text(sr.get("codigo_ml", ""))
        ean = clean_text(sr.get("codigo_universal", ""))
        ident = clean_text(sr.get("identificacion", ""))
        label = f"{desc} | SKU {sku} | ML {ml} | EAN {ean} | {ident}"[:180]
        option_rows.append(label)
        option_map[label] = int(sr["id"])

    selected_search = st.selectbox(
        "Buscar producto",
        [""] + option_rows,
        index=0,
        placeholder="Escribe nombre, SKU, Código ML, EAN o supermercado",
        key="sup_control_search_select",
    )
    selected_id = option_map.get(selected_search)
    if selected_id:
        show = show[show["id"].astype(int) == int(selected_id)]

    st.caption(f"Mostrando {len(show)} de {len(view)} líneas del lote.")
    modo_vista = st.radio("Vista", ["Tarjetas operativas", "Tabla"], horizontal=True, key="sup_control_modo_vista")

    if modo_vista == "Tabla":
        out = show.rename(columns={
            "codigo_ml": "Código ML",
            "codigo_universal": "Código Universal",
            "sku": "SKU",
            "descripcion": "Producto",
            "unidades": "Solicitadas",
            "acopiadas": "Acopiadas",
            "pendiente": "Pendiente",
            "estado": "Estado",
            "identificacion": "Identificación",
            "vence": "Vence",
            "procesado_at": "Último escaneo",
        })
        cols = ["Estado", "Código ML", "Código Universal", "SKU", "Producto", "Solicitadas", "Acopiadas", "Pendiente", "Identificación", "Vence", "Último escaneo"]
        st.dataframe(out[[c for c in cols if c in out.columns]], use_container_width=True, hide_index=True, height=620)
        return

    for _, r in show.iterrows():
        ident = clean_text(r.get("identificacion", ""))
        vence = clean_text(r.get("vence", ""))
        proc = fmt_dt(r.get("procesado_at", "")) or "Sin procesar"
        badges_parts = [
            f"<span class='badge'>Unidades: {int(r['unidades'])}</span>",
            f"<span class='badge'>Acopiadas: {int(r['acopiadas'])}</span>",
            f"<span class='badge'>Pendiente: {int(r['pendiente'])}</span>",
            f"<span class='badge'>{esc(r['estado'])}</span>",
        ]
        if is_supermercado(ident):
            badges_parts.append("<span class='badge badge-alert'>SUPERMERCADO</span>")
        if ident:
            badges_parts.append(f"<span class='badge'>Identificación: {esc(ident)}</span>")
        if vence:
            badges_parts.append(f"<span class='badge'>Vence: {esc(vence)}</span>")
        badges_html = "".join(badges_parts)
        st.markdown(f"""
        <div class='control-card'>
            <div class='control-title'>{esc(r.get('descripcion',''))}</div>
            <div class='control-meta'><b>ML:</b> {esc(r.get('codigo_ml',''))} · <b>EAN:</b> {esc(r.get('codigo_universal',''))} · <b>SKU:</b> {esc(r.get('sku',''))}</div>
            <div>{badges_html}</div>
            <div class='control-meta' style='margin-top:8px;'><b>Último escaneo:</b> {esc(proc)}</div>
        </div>
        """, unsafe_allow_html=True)


def render_auditoria_integrada(active_lote: int):
    """Auditoría integrada al panel Supervisor."""
    eventos = get_audit_events(active_lote, limit=500)
    if eventos.empty:
        st.info("Aún no hay eventos de auditoría para este lote.")
        return

    f_eventos = ["Todos"] + sorted([x for x in eventos["event_type"].dropna().unique().tolist()])
    filtro_evento = st.selectbox("Filtrar evento", f_eventos, key="sup_audit_filtro_evento")
    show = eventos.copy()
    if filtro_evento != "Todos":
        show = show[show["event_type"] == filtro_evento]
    show = show.rename(columns={
        "created_at": "Fecha",
        "event_type": "Evento",
        "detail": "Detalle",
        "qty": "Cantidad",
        "codigo_ml": "Código ML",
        "sku": "SKU",
        "mode": "Modo",
        "item_id": "Item ID",
    })
    st.dataframe(show, use_container_width=True, hide_index=True, height=650)
    st.caption("La auditoría queda guardada en SQLite y también se incluye en el Excel de control exportado.")

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
    page = st.radio("Vista", ["Escaneo", "Cargar lote FULL", "Picking", "Supervisor", "Etiquetas"], label_visibility="collapsed")
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
    failed_backup = int(bs.get("failed") or 0)
    sent_backup = int(bs.get("sent") or 0)
    if failed_backup:
        st.error(f"Respaldo externo: {failed_backup} eventos fallidos")
        if st.button("Reintentar fallidos"):
            retry_failed_backups(limit=1000)
            st.rerun()
    if pending_backup:
        st.warning(f"Respaldo externo: {pending_backup} eventos pendientes")
        if bs.get("last_error"):
            st.caption(f"Último error: {clean_text(bs.get('last_error'))[:180]}")
        with st.expander("Últimos errores respaldo", expanded=False):
            err_df = get_backup_error_rows(limit=20)
            if err_df.empty:
                st.caption("Sin errores registrados.")
            else:
                st.dataframe(err_df, use_container_width=True, hide_index=True, height=220)
        if st.button("Reintentar respaldo"):
            flush_backup_queue(limit=1000)
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
    modo_carga = st.radio(
        "Origen del lote",
        ["Excel depurado", "PDF Mercado Libre"],
        horizontal=True,
        help="Puedes mantener el flujo actual con Excel o crear el lote directo desde el PDF de preparación de Mercado Libre.",
    )

    if modo_carga == "Excel depurado":
        full_file = st.file_uploader("Excel FULL", type=["xlsx"], key="excel_full_upload")
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
                        preview_cols_excel = ["codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence"]
                        if "instrucciones" in df.columns and df["instrucciones"].astype(str).str.strip().ne("").any():
                            preview_cols_excel.append("instrucciones")
                        st.dataframe(df[preview_cols_excel].head(20), use_container_width=True, hide_index=True)
                    nombre = st.text_input("Nombre del lote", value=f"{selected_sheet} {now_cl().strftime('%d-%m-%Y %H:%M')}")
                    if st.button("Crear lote", type="primary"):
                        create_lote(nombre, full_file.name, selected_sheet, df)
                        reset_scan_state()
                        st.success("Lote creado correctamente.")
                        st.rerun()
            except Exception as e:
                st.error(f"No pude leer la hoja seleccionada: {e}")

    else:
        st.caption("Carga el PDF de instrucciones de preparación de Mercado Libre. La app cruza por SKU contra el maestro Kame del repositorio y genera el mismo formato operativo del Excel depurado.")
        pdf_file = st.file_uploader("PDF Mercado Libre", type=["pdf"], key="pdf_ml_upload")

        if MAESTRO_PATH.exists():
            st.success(f"Maestro SKU/EAN Kame detectado en repo: {MAESTRO_PATH}")
        else:
            st.error("No encontré data/maestro_sku_ean.xlsx en el repositorio. Sube ese archivo al repo para usar la carga desde PDF.")

        if pdf_file and MAESTRO_PATH.exists():
            try:
                df_pdf, checks = build_full_input_from_pdf(pdf_file)
                if df_pdf.empty:
                    st.error("No pude detectar productos válidos en el PDF.")
                else:
                    c1, c2, c3, c4 = st.columns(4)
                    expected_products = checks.get("expected_products") or "N/D"
                    expected_units = checks.get("expected_units") or "N/D"
                    c1.metric("Productos", f"{checks['detected_products']} / {expected_products}")
                    c2.metric("Unidades", f"{checks['detected_units']} / {expected_units}")
                    c3.metric("SKU sin maestro", checks.get("sku_not_found", 0))
                    c4.metric("Cód. universal N/A", checks.get("codigo_universal_na", 0))

                    if checks.get("products_match") and checks.get("units_match"):
                        st.success("Validación OK: productos y unidades cuadran con el PDF.")
                    else:
                        st.error("La validación no cuadra con los totales del PDF. Revisa antes de crear el lote.")

                    if checks.get("sku_not_found", 0):
                        st.warning("Hay SKUs no encontrados en maestro Kame. Se usará la descripción de Mercado Libre para esos productos.")

                    preview_cols = ["nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "alertas"]
                    with st.expander("Vista previa del lote generado desde PDF", expanded=True):
                        st.dataframe(df_pdf[preview_cols], use_container_width=True, hide_index=True, height=380)

                    excel_name_base = f"full_input_pdf_{checks.get('shipment') or now_cl().strftime('%Y%m%d_%H%M')}"
                    st.download_button(
                        "Descargar Excel depurado generado",
                        data=full_input_excel_bytes(df_pdf),
                        file_name=f"{excel_name_base}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                    nombre_default = f"PDF ML {checks.get('shipment') or ''} {now_cl().strftime('%d-%m-%Y %H:%M')}".strip()
                    nombre = st.text_input("Nombre del lote", value=nombre_default, key="pdf_lote_nombre")
                    can_create = bool(checks.get("products_match") and checks.get("units_match"))
                    if not can_create:
                        st.caption("Por seguridad, la creación se bloquea hasta que productos y unidades detectadas cuadren con el PDF.")
                    if st.button("Crear lote desde PDF", type="primary", disabled=not can_create):
                        create_lote(nombre, pdf_file.name, "PDF Mercado Libre", df_pdf)
                        reset_scan_state()
                        st.success("Lote creado correctamente desde PDF.")
                        st.rerun()
            except Exception as e:
                st.error(f"No pude procesar el PDF: {e}")

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

    /* El formulario de incidencia NO debe heredar el tamaño gigante del PDA. */
    div[data-testid="stExpander"] div[data-testid="stTextInput"] label,
    div[data-testid="stExpander"] div[data-testid="stNumberInput"] label,
    div[data-testid="stExpander"] div[data-testid="stSelectbox"] label,
    div[data-testid="stExpander"] div[data-testid="stTextArea"] label {
        font-size:0.95rem!important;
        font-weight:650!important;
        margin-bottom:0.25rem!important;
    }
    div[data-testid="stExpander"] div[data-testid="stTextInput"] input,
    div[data-testid="stExpander"] div[data-testid="stNumberInput"] input {
        font-size:1.05rem!important;
        min-height:2.6rem!important;
        font-weight:500!important;
    }
    div[data-testid="stExpander"] textarea {
        font-size:1.05rem!important;
        min-height:5.5rem!important;
        font-weight:500!important;
    }
    div[data-testid="stExpander"] .stButton > button,
    div[data-testid="stExpander"] div[data-testid="stFormSubmitButton"] button {
        font-size:1rem!important;
        min-height:2.6rem!important;
        width:auto!important;
        font-weight:800!important;
        border-radius:10px!important;
    }
    </style>
    """, unsafe_allow_html=True)
    if not active_lote:
        st.warning("Primero crea un lote FULL.")
    else:
        lote_scan = get_lote(active_lote)
        lote_cerrado = clean_text(lote_scan.get("status", "ACTIVO")).upper() == "CERRADO"
        items = get_items(active_lote)
        total = int(items["unidades"].sum()) if not items.empty else 0
        done = int(items["acopiadas"].sum()) if not items.empty else 0
        st.progress(done / total if total else 0)
        a, b, c = st.columns(3)
        a.metric("Solicitado", total)
        b.metric("Acopiado", done)
        c.metric("Pendiente", max(total - done, 0))
        st.divider()

        # Sesión de trazabilidad: se define una vez, no por cada escaneo.
        # Solo hay dos validadores PDA, por eso se usa selector fijo para evitar errores de tipeo.
        if "scan_operator" not in st.session_state or clean_text(st.session_state.get("scan_operator", "")).upper() not in SCAN_OPERATORS:
            st.session_state["scan_operator"] = SCAN_OPERATORS[0]
        if "scan_picking_list_id" not in st.session_state:
            st.session_state["scan_picking_list_id"] = 0

        picking_options = {}
        pl_active = get_picking_lists(active_lote)
        if not pl_active.empty:
            pl_active = pl_active[~pl_active["estado"].astype(str).str.upper().isin(["ANULADA", "COMPLETADA"])]
            for r in pl_active.itertuples(index=False):
                picking_options[f"{r.codigo_lista} · {r.asignado_a} · {r.estado}"] = int(r.id)

        current_pick_id = int(st.session_state.get("scan_picking_list_id") or 0)
        valid_pick_ids = set(picking_options.values())
        if current_pick_id not in valid_pick_ids:
            st.session_state["scan_picking_list_id"] = next(iter(valid_pick_ids), 0)
            current_pick_id = int(st.session_state.get("scan_picking_list_id") or 0)

        labels_pick = list(picking_options.keys())
        default_idx_pick = 0
        for idx, label in enumerate(labels_pick):
            if picking_options[label] == current_pick_id:
                default_idx_pick = idx
                break

        with st.container(border=True):
            st.markdown("**Sesión PDA**")
            sx1, sx2 = st.columns([1, 2])
            with sx1:
                current_op = clean_text(st.session_state.get("scan_operator", "")).upper()
                op_idx = SCAN_OPERATORS.index(current_op) if current_op in SCAN_OPERATORS else 0
                op_selected = st.radio(
                    "Validador",
                    SCAN_OPERATORS,
                    index=op_idx,
                    horizontal=True,
                    key="scan_operator_radio",
                )
                st.session_state["scan_operator"] = op_selected
            with sx2:
                if labels_pick:
                    chosen_pick = st.selectbox("Lista picking activa", labels_pick, index=default_idx_pick, key="scan_picking_select")
                    st.session_state["scan_picking_list_id"] = int(picking_options[chosen_pick])
                else:
                    st.session_state["scan_picking_list_id"] = 0
                    st.warning("No hay listas de picking activas para este lote. Crea una lista en el módulo Picking antes de escanear.")


        if lote_cerrado:
            st.error(f"Lote cerrado por {clean_text(lote_scan.get('closed_by',''))} el {fmt_dt(lote_scan.get('closed_at',''))}. No se permiten escaneos ni incidencias nuevas.")
            recientes = get_recent_scans(active_lote, limit=8)
            if not recientes.empty:
                st.subheader("Últimos escaneos")
                st.dataframe(recientes, use_container_width=True, hide_index=True, height=260)
            st.stop()

        if not int(st.session_state.get("scan_picking_list_id") or 0):
            st.error("Para validar productos debes tener una lista de picking activa. No se permite escanear con 'Sin lista de picking'.")
            st.info("Ve al módulo Picking, crea/asigna una lista y vuelve a Escaneo para validar.")
            recientes = get_recent_scans(active_lote, limit=8)
            if not recientes.empty:
                st.subheader("Últimos escaneos")
                st.dataframe(recientes, use_container_width=True, hide_index=True, height=260)
            st.stop()

        for k, v in {"primary_validated": False, "primary_code": "", "candidate_id": None, "candidate_mode": "", "_clear_scan_inputs_next_run": False}.items():
            if k not in st.session_state:
                st.session_state[k] = v

        clear_scan_inputs_if_needed()

        st.text_input("Código ML o EAN supermercado", key="scan_primary", placeholder="Escanea código")
        focus_scan_primary_once()
        cv, cl = st.columns([3, 1])
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
        aviso_prevalidacion_item_id = None
        aviso_bloqueante_prevalidacion = False

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

                # Regla estricta de picking en el primer paso:
                # si hay lista activa, el Código ML debe pertenecer a esa lista.
                # Si no pertenece, se informa de inmediato y no se permite seguir a SKU/EAN.
                picking_bloqueo_prevalidacion = False
                active_pick_id_preview = int(st.session_state.get("scan_picking_list_id") or 0)
                if active_pick_id_preview:
                    pm_preview = get_picking_list_meta(active_pick_id_preview)
                    if not item_in_picking_list(active_pick_id_preview, int(preview["id"])):
                        picking_bloqueo_prevalidacion = True
                        st.error(
                            f"Este producto NO pertenece a la lista activa {clean_text(pm_preview.get('codigo_lista',''))}. "
                            "Cambia la lista de picking activa antes de validar este producto."
                        )
                    else:
                        pp_preview = picking_pending_for_item(active_pick_id_preview, int(preview["id"]))
                        st.info(
                            f"Producto correcto para lista {clean_text(pm_preview.get('codigo_lista',''))} · "
                            f"Picker {clean_text(pm_preview.get('asignado_a',''))} · "
                            f"Validado {int(pp_preview['validado_pda'])}/{int(pp_preview['cantidad'])} · "
                            f"Pendiente lista {int(pp_preview['pendiente'])}"
                        )

                # Aviso operacional temprano: se muestra apenas se valida el Código ML,
                # antes de pedir/validar SKU, EAN o Código Universal.
                aviso_prevalidacion_item_id = int(preview["id"])
                aviso_bloqueante_prevalidacion = render_avisos_operacionales_scan(active_lote, aviso_prevalidacion_item_id)
                if aviso_bloqueante_prevalidacion:
                    st.error("Este producto tiene un aviso operacional bloqueante. No continúes con SKU/EAN ni agregues cantidad hasta que Supervisor lo resuelva.")

                bloqueo_prevalidacion = aviso_bloqueante_prevalidacion or picking_bloqueo_prevalidacion
                st.text_input("SKU / EAN / Código Universal", key="scan_secondary", disabled=bloqueo_prevalidacion)
                b1, b2 = st.columns(2)
                with b1:
                    validar_sec = st.button("Validar SKU/EAN", type="primary", disabled=bloqueo_prevalidacion)
                with b2:
                    sin_ean = st.button("Sin EAN", disabled=bloqueo_prevalidacion)

                if sin_ean and not bloqueo_prevalidacion:
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

                if validar_sec and candidate is None and not bloqueo_prevalidacion:
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
            if item_tiene_incidencia_abierta(active_lote, int(candidate["id"])):
                st.warning("⚠️ ESTE PRODUCTO TIENE INCIDENCIAS ABIERTAS. Revisa Supervisor antes de cerrar el lote.")

            # Si ya mostramos el aviso al validar Código ML, no lo duplicamos después
            # de validar SKU/EAN. Si el candidato viene de otro flujo, lo mostramos aquí.
            if aviso_prevalidacion_item_id == int(candidate["id"]):
                aviso_bloqueante = aviso_bloqueante_prevalidacion
            else:
                aviso_bloqueante = render_avisos_operacionales_scan(active_lote, int(candidate["id"]))

            picking_bloqueo = False
            active_pick_id = int(st.session_state.get("scan_picking_list_id") or 0)
            if active_pick_id:
                pm = get_picking_list_meta(active_pick_id)
                if not item_in_picking_list(active_pick_id, int(candidate["id"])):
                    picking_bloqueo = True
                    st.error(f"Este producto no pertenece a la lista activa {clean_text(pm.get('codigo_lista',''))}. Cambia la lista de picking activa.")
                else:
                    pp = picking_pending_for_item(active_pick_id, int(candidate["id"]))
                    st.info(f"Picking {clean_text(pm.get('codigo_lista',''))} · Asignado a {clean_text(pm.get('asignado_a',''))} · Validado {int(pp['validado_pda'])}/{int(pp['cantidad'])} · Pendiente lista {int(pp['pendiente'])}")

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
                agregar = st.form_submit_button("Agregar cantidad", type="primary", disabled=(aviso_bloqueante or picking_bloqueo))

            if aviso_bloqueante:
                st.error("Este producto tiene un aviso operacional bloqueante. No se permite agregar cantidad hasta que Supervisor lo resuelva.")
            if picking_bloqueo:
                st.error("No se puede registrar este escaneo contra la lista picking activa.")

            if agregar:
                qty = to_int(qty_txt)
                if qty <= 0:
                    st.error("Ingresa una cantidad válida mayor a cero.")
                elif qty > pendiente:
                    st.error(f"No puedes agregar {qty}. Solo quedan {pendiente} pendientes.")
                elif active_pick_id and qty > int(picking_pending_for_item(active_pick_id, int(candidate["id"])).get("pendiente") or 0):
                    pp_now = picking_pending_for_item(active_pick_id, int(candidate["id"]))
                    st.error(f"No puedes agregar {qty} en la lista activa. Solo quedan {int(pp_now.get('pendiente') or 0)} pendientes para esta lista.")
                else:
                    submit_sig = f"{active_lote}:{int(candidate['id'])}:{qty}:{norm_code(st.session_state.get('scan_primary', ''))}:{norm_code(st.session_state.get('scan_secondary', ''))}:{modo}:{active_pick_id}"
                    if st.session_state.get("_last_scan_submit_sig") == submit_sig:
                        st.warning("Este escaneo ya fue procesado. Limpia o escanea el siguiente producto.")
                    else:
                        st.session_state["_last_scan_submit_sig"] = submit_sig
                        ok, msg = add_acopio(
                            active_lote,
                            int(candidate["id"]),
                            int(qty),
                            st.session_state.get("scan_primary", ""),
                            st.session_state.get("scan_secondary", ""),
                            modo,
                            clean_text(st.session_state.get("scan_operator", "")) or "SIN_USUARIO",
                            active_pick_id if active_pick_id else None,
                        )
                        if ok:
                            reset_scan_state()
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

        render_scan_incident_button(active_lote, items, candidate)

        st.divider()
        if st.button("Deshacer último escaneo"):
            ok, msg = undo_last_scan(active_lote)
            st.success(msg) if ok else st.warning(msg)
            if ok: st.rerun()

        recientes = get_recent_scans(active_lote, limit=8)
        if not recientes.empty:
            st.subheader("Últimos escaneos")
            recientes = recientes.rename(columns={
                "created_at": "Fecha",
                "descripcion": "Producto",
                "codigo_ml": "Código ML",
                "sku": "SKU",
                "cantidad": "Cantidad",
                "modo": "Modo",
                "operador_validador": "Operador",
                "picking_code": "Lista picking",
                "picker_asignado": "Picker",
            })
            st.dataframe(recientes, use_container_width=True, hide_index=True, height=260)

elif page == "Picking":
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        render_picking_module(active_lote)

elif page == "Supervisor":
    st.subheader("Panel supervisor")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        lote = get_lote(active_lote)
        items = get_items(active_lote)
        capacity_sup = st.number_input("Capacidad de rollo para validar bloques", min_value=100, max_value=10000, value=ROLL_CAPACITY_DEFAULT, step=100, key="supervisor_capacity")
        ok_cierre, issues, cierre_data = cierre_validaciones(active_lote, int(capacity_sup))
        metrics = supervisor_metrics(active_lote)
        total = metrics["total"]
        done = metrics["done"]
        avance = (done / total * 100) if total else 0

        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("Estado lote", clean_text(lote.get("status", "ACTIVO")))
        s2.metric("Avance", f"{avance:.1f}%")
        s3.metric("Pendientes", metrics["pending"])
        s4.metric("Incidencias abiertas", metrics["incidencias_abiertas"])
        s5.metric("Avisos activos", metrics.get("avisos_activos", 0))
        s6.metric("Etiquetas pendientes", metrics["label_pending"])

        st.progress(done / total if total else 0)
        st.caption(f"Archivo: {lote.get('archivo','')} · Hoja: {lote.get('hoja','')} · Creado: {fmt_dt(lote.get('created_at',''))}")

        if ok_cierre:
            st.success("El lote está apto para cierre formal.")
        else:
            st.warning("El lote aún no está apto para cierre.")
            for issue in issues:
                st.write(f"• {issue}")

        tab_resumen, tab_control, tab_pendientes, tab_incid, tab_avisos, tab_bloques, tab_reimp, tab_cierre, tab_auditoria = st.tabs(["Resumen", "Control operativo", "Pendientes", "Incidencias", "Avisos operacionales", "Bloques", "Reimpresión", "Cierre", "Auditoría"])

        with tab_resumen:
            view = items.copy()
            if not view.empty:
                view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
                resumen = pd.DataFrame([{
                    "Unidades solicitadas": int(view["unidades"].sum()),
                    "Unidades acopiadas": int(view["acopiadas"].sum()),
                    "Unidades pendientes": int(view["pendiente"].sum()),
                    "Líneas totales": int(len(view)),
                    "Líneas pendientes": int((view["pendiente"] > 0).sum()),
                    "Bloques impresos": cierre_data.get("printed_blocks", 0),
                    "Bloques esperados": cierre_data.get("expected_blocks", 0),
                    "Incidencias abiertas": cierre_data.get("open_incidents", 0),
                    "Avisos operacionales activos": cierre_data.get("active_notices", 0),
                }])
                st.dataframe(resumen, use_container_width=True, hide_index=True)

        with tab_control:
            render_control_integrado(active_lote)

        with tab_pendientes:
            view = items.copy()
            if not view.empty:
                view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
                pend = view[view["pendiente"] > 0].copy()
                if pend.empty:
                    st.success("No hay productos pendientes.")
                else:
                    out = pend.rename(columns={"codigo_ml": "Código ML", "sku": "SKU", "descripcion": "Producto", "unidades": "Solicitadas", "acopiadas": "Acopiadas", "pendiente": "Pendiente", "identificacion": "Identificación", "vence": "Vence"})
                    cols = ["Código ML", "SKU", "Producto", "Solicitadas", "Acopiadas", "Pendiente", "Identificación", "Vence"]
                    st.dataframe(out[[c for c in cols if c in out.columns]], use_container_width=True, hide_index=True, height=520)

        with tab_incid:
            inc = get_incidencias(active_lote)
            if inc.empty:
                st.success("Sin incidencias registradas.")
            else:
                out = inc.rename(columns={"created_at": "Fecha", "tipo": "Tipo", "cantidad": "Cantidad", "comentario": "Comentario", "usuario": "Usuario", "status": "Estado", "codigo_ml": "Código ML", "sku": "SKU", "descripcion": "Producto"})
                cols = ["Fecha", "Estado", "Tipo", "Cantidad", "Código ML", "SKU", "Producto", "Comentario", "Usuario"]
                st.dataframe(out[[c for c in cols if c in out.columns]], use_container_width=True, hide_index=True, height=520)

        with tab_avisos:
            st.info("Los avisos operacionales los crea Supervisor/Admin. El operador solo los ve al escanear el producto.")
            sub_crear, sub_activos, sub_historial = st.tabs(["Crear aviso", "Activos", "Historial"])
            with sub_crear:
                if is_lote_closed(active_lote):
                    st.warning("El lote está cerrado. Reabre el lote para crear avisos operacionales.")
                else:
                    items_av = get_items(active_lote)
                    opciones_av = []
                    mapa_av = {}
                    for _, r in items_av.iterrows():
                        label = f"{clean_text(r.get('descripcion',''))[:85]} | ML {clean_text(r.get('codigo_ml',''))} | EAN {clean_text(r.get('codigo_universal',''))} | SKU {clean_text(r.get('sku',''))}"
                        opciones_av.append(label)
                        mapa_av[label] = int(r["id"])
                    if not opciones_av:
                        st.warning("No hay productos para avisar.")
                    else:
                        producto_label = st.selectbox("Producto", opciones_av, key="aviso_producto_select")
                        aviso_item_id = mapa_av[producto_label]
                        item_av = items_av[items_av["id"].astype(int) == int(aviso_item_id)].iloc[0].to_dict()
                        c1, c2 = st.columns([2, 1])
                        with c1:
                            tipo_av = st.selectbox("Tipo de aviso", AVISO_OPERACIONAL_TIPOS, key="aviso_tipo")
                        with c2:
                            cantidad_nueva = st.text_input("Cantidad nueva objetivo opcional", key="aviso_cantidad_nueva", placeholder="Ej: 10")
                        mensaje_def = ""
                        if tipo_av == "Ajuste de cantidad" and clean_text(cantidad_nueva):
                            mensaje_def = f"Producto con ajuste administrativo. Nueva cantidad objetivo: {clean_text(cantidad_nueva)}."
                        elif tipo_av == "Producto retirado del lote":
                            mensaje_def = "Producto retirado del lote. No continuar preparación."
                        elif tipo_av == "No escanear / esperar instrucción":
                            mensaje_def = "No escanear este producto. Esperar instrucción de Supervisor."
                        mensaje_operador = st.text_area("Mensaje visible para operador", value=mensaje_def, key="aviso_msg_operador")
                        requiere_conf = tipo_av in AVISO_OPERACIONAL_REQUIERE_CONFIRMACION
                        if requiere_conf:
                            st.info("Este aviso puede crearse aunque Mercado Libre o Kame queden pendientes. No podrá resolverse hasta confirmar ambas tareas externas.")
                        cc1, cc2, cc3 = st.columns(3)
                        with cc1:
                            confirmado_ml = st.checkbox("Mercado Libre ya rebajado/ajustado", value=False, key="aviso_conf_ml", disabled=not requiere_conf)
                        with cc2:
                            confirmado_inv = st.checkbox("Inventario Kame ya ajustado", value=False, key="aviso_conf_inv", disabled=not requiere_conf)
                        with cc3:
                            visible_op = st.checkbox("Visible para operador", value=True, key="aviso_visible")
                        created_by = st.text_input("Creado por", key="aviso_created_by", placeholder="Ej: administrador / supervisor")
                        comentario_interno = st.text_area("Comentario interno / respaldo administrativo", key="aviso_comentario_interno", placeholder="Indica quién autorizó, qué se ajustó en ML/inventario y por qué.")
                        if st.button("Guardar aviso operacional", type="primary", key="aviso_guardar"):
                            ok_av, msg_av = create_aviso_operacional(
                                active_lote,
                                aviso_item_id,
                                tipo_av,
                                mensaje_operador,
                                cantidad_nueva,
                                bool(confirmado_ml) if requiere_conf else False,
                                bool(confirmado_inv) if requiere_conf else False,
                                bool(visible_op),
                                comentario_interno,
                                created_by,
                            )
                            st.success(msg_av) if ok_av else st.error(msg_av)
                            if ok_av:
                                st.rerun()
            with sub_activos:
                avisos_act = get_avisos_operacionales(active_lote, estado="ACTIVO")
                if avisos_act.empty:
                    st.success("No hay avisos operacionales activos.")
                else:
                    for _, av in avisos_act.iterrows():
                        tipo = clean_text(av.get("tipo_aviso", ""))
                        color = "#FEE2E2" if tipo in AVISO_OPERACIONAL_BLOQUEA else "#FEF3C7"
                        requiere_ml = int(av.get('requiere_ajuste_ml') or 0) == 1
                        requiere_kame = int(av.get('requiere_ajuste_inventario') or 0) == 1
                        ml_ok = int(av.get('confirmado_ml') or 0) == 1
                        kame_ok = int(av.get('confirmado_inventario') or 0) == 1
                        estado_ml = '✅ Mercado Libre confirmado' if (not requiere_ml or ml_ok) else '⏳ Mercado Libre pendiente'
                        estado_kame = '✅ Kame confirmado' if (not requiere_kame or kame_ok) else '⏳ Kame pendiente'
                        st.markdown(f"""
                        <div class='control-card' style='background:{color};'>
                            <div class='control-title'>{esc(tipo)} · {esc(av.get('descripcion',''))}</div>
                            <div class='control-meta'><b>ML:</b> {esc(av.get('codigo_ml',''))} · <b>EAN:</b> {esc(av.get('codigo_universal',''))} · <b>SKU:</b> {esc(av.get('sku',''))}</div>
                            <div><b>Mensaje operador:</b> {esc(av.get('mensaje_operador',''))}</div>
                            <div class='control-meta' style='margin-top:8px;'><b>Estado externo:</b> {estado_ml} · {estado_kame}</div>
                            <div class='control-meta' style='margin-top:8px;'><b>Creado por:</b> {esc(av.get('created_by',''))} · <b>Fecha:</b> {esc(fmt_dt(av.get('created_at','')))} · <b>Visible:</b> {'Sí' if int(av.get('visible_operador') or 0) == 1 else 'No'}</div>
                        </div>
                        """, unsafe_allow_html=True)
                        if requiere_ml or requiere_kame:
                            with st.expander(f"Tareas externas del aviso #{int(av['id'])}"):
                                conf_by = st.text_input("Confirmado por", key=f"aviso_conf_by_{int(av['id'])}", placeholder="Ej: administrador")
                                ccml, cckame = st.columns(2)
                                with ccml:
                                    st.caption(estado_ml)
                                    if requiere_ml and not ml_ok:
                                        if st.button("Marcar Mercado Libre ajustado", key=f"aviso_conf_ml_btn_{int(av['id'])}"):
                                            ok_conf, msg_conf = confirmar_tarea_externa_aviso(int(av['id']), 'ml', conf_by)
                                            st.success(msg_conf) if ok_conf else st.error(msg_conf)
                                            if ok_conf:
                                                st.rerun()
                                with cckame:
                                    st.caption(estado_kame)
                                    if requiere_kame and not kame_ok:
                                        if st.button("Marcar inventario Kame ajustado", key=f"aviso_conf_kame_btn_{int(av['id'])}"):
                                            ok_conf, msg_conf = confirmar_tarea_externa_aviso(int(av['id']), 'kame', conf_by)
                                            st.success(msg_conf) if ok_conf else st.error(msg_conf)
                                            if ok_conf:
                                                st.rerun()
                        with st.expander(f"Resolver aviso #{int(av['id'])}"):
                            if (requiere_ml and not ml_ok) or (requiere_kame and not kame_ok):
                                st.warning("Este aviso tiene tareas externas pendientes. Puedes mantenerlo activo, pero no resolverlo hasta confirmar Mercado Libre y Kame.")
                            res_by = st.text_input("Resuelto por", key=f"aviso_res_by_{int(av['id'])}", placeholder="Ej: supervisor")
                            res_comment = st.text_area("Comentario de resolución", key=f"aviso_res_comment_{int(av['id'])}")
                            if st.button("Marcar aviso como resuelto", key=f"aviso_resolve_{int(av['id'])}", type="primary"):
                                ok_res, msg_res = resolve_aviso_operacional(int(av["id"]), res_by, res_comment)
                                st.success(msg_res) if ok_res else st.error(msg_res)
                                if ok_res:
                                    st.rerun()
            with sub_historial:
                avisos_all = get_avisos_operacionales(active_lote)
                if avisos_all.empty:
                    st.info("Sin avisos operacionales registrados.")
                else:
                    out_av = avisos_all.rename(columns={
                        "created_at": "Fecha", "estado": "Estado", "tipo_aviso": "Tipo", "mensaje_operador": "Mensaje operador",
                        "cantidad_original": "Cantidad original", "cantidad_nueva": "Cantidad nueva", "confirmado_ml": "Conf. ML",
                        "confirmado_inventario": "Conf. Kame", "visible_operador": "Visible operador", "created_by": "Creado por",
                        "resolved_at": "Fecha resolución", "resolved_by": "Resuelto por", "codigo_ml": "Código ML",
                        "codigo_universal": "Código Universal", "sku": "SKU", "descripcion": "Producto",
                    })
                    cols_av = ["Fecha", "Estado", "Tipo", "Código ML", "Código Universal", "SKU", "Producto", "Mensaje operador", "Cantidad original", "Cantidad nueva", "Conf. ML", "Conf. Kame", "Visible operador", "Creado por", "Fecha resolución", "Resuelto por"]
                    st.dataframe(out_av[[c for c in cols_av if c in out_av.columns]], use_container_width=True, hide_index=True, height=520)

        with tab_bloques:
            labels = label_control_view(active_lote)
            expected = build_label_blocks(labels, int(capacity_sup)) if not labels.empty else []
            blocks_db = get_label_blocks_df(active_lote)
            printed_keys = set(blocks_db["block_key"].astype(str).tolist()) if not blocks_db.empty else set()
            rows = []
            for b in expected:
                rows.append({"Bloque": int(b["block_index"]), "Estado": "IMPRESO" if str(b["block_key"]) in printed_keys else "PENDIENTE", "Productos": int(b["products_count"]), "Etiquetas normales": int(b["normal_qty"]), "Inicio/Fin": int(b["separator_qty"]), "Total": int(b["total_qty"]), "Key": b["block_key"]})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, height=520)

        with tab_reimp:
            st.info("Toda reimpresión requiere motivo. Esto evita duplicaciones no controladas.")
            mode_rep = st.radio("Tipo de reimpresión", ["Bloque completo", "Producto individual"], horizontal=True, key="sup_rep_mode")
            usuario_rep = st.text_input("Usuario que reimprime", key="sup_rep_usuario", placeholder="Ej: p1, p2, supervisor")
            motivo_rep = st.text_area("Motivo obligatorio", key="sup_rep_motivo", placeholder="Ej: rollo se cortó a mitad de bloque, etiqueta dañada, impresora pausada, etc.")
            if mode_rep == "Bloque completo":
                view_rep = label_control_view(active_lote)
                expected_rep = build_label_blocks(view_rep, int(capacity_sup)) if not view_rep.empty else []
                blocks_db_rep = get_label_blocks_df(active_lote)
                printed_keys_rep = set(blocks_db_rep["block_key"].astype(str).tolist()) if not blocks_db_rep.empty else set()
                printed_blocks = [b for b in expected_rep if str(b["block_key"]) in printed_keys_rep]
                if not printed_blocks:
                    st.warning("Aún no hay bloques impresos para reimprimir.")
                else:
                    labels_rep = [f"Bloque {int(b['block_index'])} · {int(b['products_count'])} productos · {int(b['total_qty'])} etiquetas" for b in printed_blocks]
                    map_blocks = {labels_rep[i]: printed_blocks[i] for i in range(len(labels_rep))}
                    selected_block_label = st.selectbox("Bloque a reimprimir", labels_rep, key="sup_rep_block")
                    block = map_blocks[selected_block_label]
                    zpl_data = zpl_for_block(block).encode("utf-8")
                    fname = f"reimpresion_lote_{active_lote}_bloque_{int(block['block_index'])}.zpl"
                    if clean_text(motivo_rep) and clean_text(usuario_rep):
                        st.download_button("Descargar ZPL y registrar reimpresión", data=zpl_data, file_name=fname, mime="text/plain", key=f"sup_reprint_block_{active_lote}_{block['block_index']}_{block['block_key']}_{hashlib.sha1((clean_text(motivo_rep)+clean_text(usuario_rep)).encode()).hexdigest()[:8]}", on_click=register_controlled_block_reprint, args=(active_lote, block, motivo_rep, usuario_rep))
                    else:
                        st.warning("Ingresa usuario y motivo para habilitar descarga.")
            else:
                view_rep = label_control_view(active_lote)
                options_rep = []
                option_map_rep = {}
                for _, r in view_rep.iterrows():
                    label = f"{clean_text(r.get('descripcion',''))[:80]} | ML {clean_text(r.get('codigo_ml',''))} | SKU {clean_text(r.get('sku',''))}"
                    options_rep.append(label)
                    option_map_rep[label] = int(r["id"])
                if not options_rep:
                    st.warning("No hay productos.")
                else:
                    selected_item_label = st.selectbox("Producto a reimprimir", options_rep, key="sup_rep_item")
                    item_id = option_map_rep[selected_item_label]
                    row = view_rep[view_rep["id"].astype(int) == int(item_id)].iloc[0].to_dict()
                    qty_rep = st.number_input("Cantidad de etiquetas normales", min_value=1, max_value=9999, value=1, step=1, key="sup_rep_qty")
                    zpl_ind = zpl_for_item_with_separators(row, int(qty_rep)).encode("utf-8")
                    fname_ind = f"reimpresion_{norm_code(row.get('codigo_ml','')) or 'producto'}_{norm_code(row.get('sku',''))}.zpl"
                    if clean_text(motivo_rep) and clean_text(usuario_rep):
                        st.download_button("Descargar ZPL individual y registrar reimpresión", data=zpl_ind, file_name=fname_ind, mime="text/plain", key=f"sup_reprint_item_{active_lote}_{item_id}_{qty_rep}_{hashlib.sha1((clean_text(motivo_rep)+clean_text(usuario_rep)).encode()).hexdigest()[:8]}", on_click=register_controlled_item_reprint, args=(active_lote, row, int(qty_rep), motivo_rep, usuario_rep))
                    else:
                        st.warning("Ingresa usuario y motivo para habilitar descarga.")
            hist_rep = get_reimpresiones(active_lote)
            if not hist_rep.empty:
                st.divider()
                st.subheader("Historial de reimpresiones")
                out_rep = hist_rep.rename(columns={"created_at": "Fecha", "scope": "Alcance", "block_index": "Bloque", "cantidad": "Cantidad", "motivo": "Motivo", "usuario": "Usuario", "codigo_ml": "Código ML", "sku": "SKU", "descripcion": "Producto"})
                st.dataframe(out_rep, use_container_width=True, hide_index=True, height=320)

        with tab_cierre:
            lote_close = get_lote(active_lote)
            ok_close2, issues2, data_close2 = cierre_validaciones(active_lote, int(capacity_sup))
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Estado actual", clean_text(lote_close.get("status", "ACTIVO")))
            c2.metric("Unidades pendientes", data_close2.get("pending_units", 0))
            c3.metric("Incidencias abiertas", data_close2.get("open_incidents", 0))
            c4.metric("Avisos activos", data_close2.get("active_notices", 0))
            c5.metric("Bloques", f"{data_close2.get('printed_blocks',0)}/{data_close2.get('expected_blocks',0)}")
            if clean_text(lote_close.get("status")) == "CERRADO":
                st.success(f"Lote cerrado por {clean_text(lote_close.get('closed_by',''))} el {fmt_dt(lote_close.get('closed_at',''))}.")
                st.caption(clean_text(lote_close.get("close_note", "")))
                with st.expander("Reabrir lote"):
                    reopen_user = st.text_input("Usuario", key="sup_reopen_user", placeholder="Ej: supervisor")
                    reopen_reason = st.text_area("Motivo de reapertura", key="sup_reopen_reason")
                    if st.button("Reabrir lote", type="primary", key="sup_reopen_btn"):
                        if not clean_text(reopen_user):
                            st.error("Ingresa el usuario.")
                        else:
                            ok_reopen, msg_reopen = reopen_lote(active_lote, reopen_user, reopen_reason)
                            st.success(msg_reopen) if ok_reopen else st.error(msg_reopen)
                            if ok_reopen:
                                st.rerun()
            else:
                if ok_close2:
                    st.success("Validación correcta. El lote puede cerrarse.")
                else:
                    st.error("El lote no se puede cerrar todavía.")
                    for issue in issues2:
                        st.write(f"• {issue}")
                close_user = st.text_input("Cerrado por", key="sup_close_user", placeholder="Ej: supervisor")
                close_note = st.text_area("Nota de cierre", placeholder="Ej: lote revisado completo, sin diferencias abiertas.", key="sup_close_note")
                if st.button("Cerrar lote", type="primary", disabled=not ok_close2, key="sup_close_btn"):
                    if not clean_text(close_user):
                        st.error("Ingresa quién cierra el lote.")
                    else:
                        ok_final, msg_final = close_lote(active_lote, close_user, close_note)
                        st.success(msg_final) if ok_final else st.error(msg_final)
                        if ok_final:
                            st.rerun()



        with tab_auditoria:
            render_auditoria_integrada(active_lote)

elif page == "Incidencias":
    st.subheader("Incidencias operativas")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        items = get_items(active_lote)
        tab_new, tab_open, tab_all = st.tabs(["Nueva incidencia", "Abiertas", "Historial"])
        with tab_new:
            st.info("Registra la incidencia por Etiqueta ML, Código Universal/EAN o SKU. No se crean incidencias generales por lote.")
            codigo_inc = st.text_input("Etiqueta ML / Código Universal / SKU", key="inc_codigo_manual")
            tipo_inc = st.selectbox("Tipo de incidencia", INCIDENCIA_TIPOS)
            qty_inc = st.number_input("Cantidad afectada", min_value=0, max_value=99999, value=1, step=1)
            comentario_inc = st.text_area("Comentario", placeholder="Describe qué ocurrió y qué evidencia existe.")
            if st.button("Registrar incidencia", type="primary"):
                ok_inc, msg_inc = create_incidencia_por_codigo(active_lote, codigo_inc, tipo_inc, int(qty_inc), comentario_inc, "SIN_USUARIO")
                if ok_inc:
                    st.success(msg_inc)
                    st.rerun()
                else:
                    st.error(msg_inc)
        with tab_open:
            inc = get_incidencias(active_lote, status="ABIERTA")
            if inc.empty:
                st.success("No hay incidencias abiertas.")
            else:
                for _, r in inc.iterrows():
                    st.markdown(f"""
                    <div class='control-card'>
                        <div class='control-title'>{esc(r.get('tipo',''))} · {esc(r.get('descripcion','') or 'General del lote')}</div>
                        <div class='control-meta'><b>Estado:</b> {esc(r.get('status',''))} · <b>Cantidad:</b> {int(r.get('cantidad') or 0)} · <b>Usuario:</b> {esc(r.get('usuario',''))} · <b>Fecha:</b> {esc(fmt_dt(r.get('created_at','')))}</div>
                        <div>{esc(r.get('comentario',''))}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    with st.expander(f"Resolver incidencia #{int(r['id'])}"):
                        res_user = st.text_input("Resuelto por", value=get_operator_name(), key=f"res_user_{int(r['id'])}")
                        res_comment = st.text_area("Comentario de resolución", key=f"res_comment_{int(r['id'])}")
                        if st.button("Marcar como resuelta", key=f"resolve_{int(r['id'])}", type="primary"):
                            ok_res, msg_res = resolve_incidencia(int(r["id"]), res_user, res_comment)
                            st.success(msg_res) if ok_res else st.error(msg_res)
                            if ok_res:
                                st.rerun()
        with tab_all:
            inc = get_incidencias(active_lote)
            if inc.empty:
                st.info("Sin incidencias.")
            else:
                out = inc.rename(columns={"created_at": "Fecha", "tipo": "Tipo", "cantidad": "Cantidad", "comentario": "Comentario", "usuario": "Usuario", "status": "Estado", "resolved_at": "Fecha resolución", "resolved_by": "Resuelto por", "resolution_comment": "Comentario resolución", "codigo_ml": "Código ML", "sku": "SKU", "descripcion": "Producto"})
                st.dataframe(out, use_container_width=True, hide_index=True, height=620)


elif page == "Reimpresión":
    st.subheader("Reimpresión controlada")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        st.info("Toda reimpresión requiere motivo. Esto evita duplicaciones no controladas.")
        mode_rep = st.radio("Tipo de reimpresión", ["Bloque completo", "Producto individual"], horizontal=True)
        usuario_rep = st.text_input("Usuario que reimprime", value=get_operator_name(), key="rep_usuario")
        motivo_rep = st.text_area("Motivo obligatorio", placeholder="Ej: rollo se cortó a mitad de bloque, etiqueta dañada, impresora pausada, etc.")
        if mode_rep == "Bloque completo":
            view = label_control_view(active_lote)
            capacity_rep = st.number_input("Capacidad de rollo usada para reconstruir bloques", min_value=100, max_value=10000, value=ROLL_CAPACITY_DEFAULT, step=100, key="rep_capacity")
            expected = build_label_blocks(view, int(capacity_rep)) if not view.empty else []
            blocks_db = get_label_blocks_df(active_lote)
            printed_keys = set(blocks_db["block_key"].astype(str).tolist()) if not blocks_db.empty else set()
            printed_blocks = [b for b in expected if str(b["block_key"]) in printed_keys]
            if not printed_blocks:
                st.warning("Aún no hay bloques impresos para reimprimir.")
            else:
                labels = [f"Bloque {int(b['block_index'])} · {int(b['products_count'])} productos · {int(b['total_qty'])} etiquetas" for b in printed_blocks]
                map_blocks = {labels[i]: printed_blocks[i] for i in range(len(labels))}
                selected_block_label = st.selectbox("Bloque a reimprimir", labels)
                block = map_blocks[selected_block_label]
                zpl_data = zpl_for_block(block).encode("utf-8")
                fname = f"reimpresion_lote_{active_lote}_bloque_{int(block['block_index'])}.zpl"
                if clean_text(motivo_rep):
                    st.download_button("Descargar ZPL y registrar reimpresión", data=zpl_data, file_name=fname, mime="text/plain", key=f"reprint_block_{active_lote}_{block['block_index']}_{block['block_key']}_{hashlib.sha1(clean_text(motivo_rep).encode()).hexdigest()[:8]}", on_click=register_controlled_block_reprint, args=(active_lote, block, motivo_rep, usuario_rep))
                else:
                    st.warning("Ingresa motivo para habilitar descarga.")
                with st.expander("Productos del bloque"):
                    bdf = pd.DataFrame(block["items"])
                    st.dataframe(bdf[[c for c in ["codigo_ml", "sku", "descripcion", "unidades"] if c in bdf.columns]], use_container_width=True, hide_index=True)
        else:
            view = label_control_view(active_lote)
            options = []
            option_map = {}
            for _, r in view.iterrows():
                label = f"{clean_text(r.get('descripcion',''))[:80]} | ML {clean_text(r.get('codigo_ml',''))} | SKU {clean_text(r.get('sku',''))}"
                options.append(label)
                option_map[label] = int(r["id"])
            if not options:
                st.warning("No hay productos.")
            else:
                selected_item_label = st.selectbox("Producto a reimprimir", options)
                item_id = option_map[selected_item_label]
                row = view[view["id"].astype(int) == int(item_id)].iloc[0].to_dict()
                qty_rep = st.number_input("Cantidad de etiquetas normales", min_value=1, max_value=9999, value=1, step=1)
                zpl_ind = zpl_for_item_with_separators(row, int(qty_rep)).encode("utf-8")
                fname_ind = f"reimpresion_{norm_code(row.get('codigo_ml','')) or 'producto'}_{norm_code(row.get('sku',''))}.zpl"
                if clean_text(motivo_rep):
                    st.download_button("Descargar ZPL individual y registrar reimpresión", data=zpl_ind, file_name=fname_ind, mime="text/plain", key=f"reprint_item_{active_lote}_{item_id}_{qty_rep}_{hashlib.sha1(clean_text(motivo_rep).encode()).hexdigest()[:8]}", on_click=register_controlled_item_reprint, args=(active_lote, row, int(qty_rep), motivo_rep, usuario_rep))
                else:
                    st.warning("Ingresa motivo para habilitar descarga.")
        hist = get_reimpresiones(active_lote)
        if not hist.empty:
            st.divider()
            st.subheader("Historial de reimpresiones")
            out = hist.rename(columns={"created_at": "Fecha", "scope": "Alcance", "block_index": "Bloque", "cantidad": "Cantidad", "motivo": "Motivo", "usuario": "Usuario", "codigo_ml": "Código ML", "sku": "SKU", "descripcion": "Producto"})
            st.dataframe(out, use_container_width=True, hide_index=True, height=360)


elif page == "Cierre de lote":
    st.subheader("Cierre formal de lote")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        lote = get_lote(active_lote)
        capacity_close = st.number_input("Capacidad de rollo para validar bloques", min_value=100, max_value=10000, value=ROLL_CAPACITY_DEFAULT, step=100, key="close_capacity")
        ok_close, issues, data_close = cierre_validaciones(active_lote, int(capacity_close))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Estado actual", clean_text(lote.get("status", "ACTIVO")))
        c2.metric("Unidades pendientes", data_close.get("pending_units", 0))
        c3.metric("Incidencias abiertas", data_close.get("open_incidents", 0))
        c4.metric("Bloques", f"{data_close.get('printed_blocks',0)}/{data_close.get('expected_blocks',0)}")
        if clean_text(lote.get("status")) == "CERRADO":
            st.success(f"Lote cerrado por {clean_text(lote.get('closed_by',''))} el {fmt_dt(lote.get('closed_at',''))}.")
            st.caption(clean_text(lote.get("close_note", "")))
            with st.expander("Reabrir lote"):
                reopen_user = st.text_input("Usuario", value=get_operator_name(), key="reopen_user")
                reopen_reason = st.text_area("Motivo de reapertura", key="reopen_reason")
                if st.button("Reabrir lote", type="primary"):
                    ok_reopen, msg_reopen = reopen_lote(active_lote, reopen_user, reopen_reason)
                    st.success(msg_reopen) if ok_reopen else st.error(msg_reopen)
                    if ok_reopen:
                        st.rerun()
        else:
            if ok_close:
                st.success("Validación correcta. El lote puede cerrarse.")
            else:
                st.error("El lote no se puede cerrar todavía.")
                for issue in issues:
                    st.write(f"• {issue}")
            close_user = st.text_input("Cerrado por", value=get_operator_name(), key="close_user")
            close_note = st.text_area("Nota de cierre", placeholder="Ej: lote revisado completo, sin diferencias abiertas.", key="close_note")
            if st.button("Cerrar lote", type="primary", disabled=not ok_close):
                ok_final, msg_final = close_lote(active_lote, close_user, close_note)
                st.success(msg_final) if ok_final else st.error(msg_final)
                if ok_final:
                    st.rerun()


elif page == "Etiquetas":
    st.subheader("Etiquetas Zebra 50x30")
    st.caption("Módulo independiente: solo genera/descarga ZPL y registra etiquetas. No modifica el escaneo ni las unidades acopiadas.")

    if not active_lote:
        st.warning("Primero crea o selecciona un lote FULL.")
    else:
        lote = get_lote(active_lote)
        if clean_text(lote.get("status", "ACTIVO")).upper() == "CERRADO":
            st.error(f"Lote cerrado por {clean_text(lote.get('closed_by',''))} el {fmt_dt(lote.get('closed_at',''))}. No se permite impresión normal ni reimpresión sin reapertura.")
            st.stop()
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
                        st.warning(f"Bloque {int(block['block_index'])} ya fue marcado como impreso. Para volver a imprimirlo usa la vista Reimpresión y registra motivo obligatorio.")
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

elif page == "Auditoría":
    st.subheader("Auditoría operacional")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        eventos = get_audit_events(active_lote, limit=500)
        if eventos.empty:
            st.info("Aún no hay eventos de auditoría para este lote.")
        else:
            f_eventos = ["Todos"] + sorted([x for x in eventos["event_type"].dropna().unique().tolist()])
            filtro_evento = st.selectbox("Filtrar evento", f_eventos)
            show = eventos.copy()
            if filtro_evento != "Todos":
                show = show[show["event_type"] == filtro_evento]
            show = show.rename(columns={
                "created_at": "Fecha",
                "event_type": "Evento",
                "detail": "Detalle",
                "qty": "Cantidad",
                "codigo_ml": "Código ML",
                "sku": "SKU",
                "mode": "Modo",
                "item_id": "Item ID",
            })
            st.dataframe(show, use_container_width=True, hide_index=True, height=650)
            st.caption("La auditoría queda guardada en SQLite y también se incluye en el Excel de control exportado.")

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
