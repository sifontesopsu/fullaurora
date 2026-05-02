import io
import re
import html
import hashlib
import json
import hashlib
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

def ensure_column(conn, table: str, column: str, definition: str):
    """Agrega una columna si no existe. Evita romper bases SQLite antiguas."""
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


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
        ensure_column(c, "backup_queue", "event_key", "TEXT")
        ensure_column(c, "audit_events", "event_key", "TEXT")
        ensure_column(c, "audit_events", "usuario", "TEXT")
        ensure_column(c, "audit_events", "source_module", "TEXT")
        ensure_column(c, "audit_events", "before_json", "TEXT")
        ensure_column(c, "audit_events", "after_json", "TEXT")
        ensure_column(c, "audit_events", "synced_to_sheets", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "lotes", "status", "TEXT NOT NULL DEFAULT 'ACTIVO'")
        ensure_column(c, "lotes", "closed_at", "TEXT")
        ensure_column(c, "lotes", "closed_by", "TEXT")
        ensure_column(c, "lotes", "close_note", "TEXT")
        ensure_column(c, "label_blocks", "last_reprint_reason", "TEXT")
        ensure_column(c, "label_blocks", "last_reprint_user", "TEXT")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_label_blocks_unique ON label_blocks (lote_id, block_index, block_key)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_lote ON items (lote_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_codigo_ml ON items (lote_id, codigo_ml)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_sku ON items (lote_id, sku)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_scans_lote ON scans (lote_id, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_audit_lote ON audit_events (lote_id, created_at)")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_event_key ON audit_events (event_key) WHERE event_key IS NOT NULL AND event_key != ''")
        c.execute("CREATE INDEX IF NOT EXISTS idx_incidencias_lote ON incidencias (lote_id, status, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_reimpresiones_lote ON reimpresiones (lote_id, created_at)")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_backup_queue_event_key ON backup_queue (event_key) WHERE event_key IS NOT NULL AND event_key != ''")
        c.execute("CREATE INDEX IF NOT EXISTS idx_backup_queue_status ON backup_queue (status, id)")
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


def backup_event_key(event_type: str, payload: dict) -> str:
    """Clave idempotente para que un mismo evento operativo no pueda entrar dos veces a la cola.
    Esto protege contra doble click, rerun de Streamlit y reintentos de sincronización.
    """
    if event_type == "lote_creado":
        return f"lote_creado:{payload.get('lote_id')}"
    if event_type == "lote_item":
        return f"lote_item:{payload.get('lote_id')}:{payload.get('item_id')}"
    if event_type == "lote_eliminado":
        return f"lote_eliminado:{payload.get('lote_id')}:{payload.get('deleted_at')}"
    if event_type in {"scan_agregado", "scan_deshacer"}:
        # Estos eventos sí pueden repetirse operativamente, por eso llevan timestamp + datos del movimiento.
        return f"{event_type}:{payload.get('lote_id')}:{payload.get('item_id')}:{payload.get('created_at')}:{payload.get('cantidad')}:{payload.get('scan_primario')}:{payload.get('scan_secundario')}"
    if event_type == "audit_event":
        # La auditoría tiene su propia clave única; si llega duplicada por rerun, no entra dos veces.
        key = clean_text(payload.get("event_key", ""))
        if key:
            return key
    base = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return f"{event_type}:{hashlib.sha1(base.encode('utf-8')).hexdigest()}"


def enqueue_backup_event(event_type: str, payload: dict):
    """Guarda un evento en cola local de forma idempotente.
    Importante: NO dispara envío en segundo plano; el envío se hace mediante flush_backup_queue()
    para evitar carreras donde dos hilos mandan los mismos pendientes a Sheets.
    """
    now = datetime.now().isoformat(timespec="seconds")
    safe_payload = json.dumps(payload, ensure_ascii=False, default=str)
    event_key = backup_event_key(event_type, payload)
    with db() as c:
        c.execute(
            """
            INSERT OR IGNORE INTO backup_queue
            (event_type, payload_json, status, attempts, created_at, event_key)
            VALUES (?, ?, 'pending', 0, ?, ?)
            """,
            (event_type, safe_payload, now, event_key),
        )
        c.commit()


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
    """Inserta muchos eventos en cola local, sin duplicar claves operativas.
    No dispara threads: la sincronización se ejecuta de forma controlada con flush_backup_queue().
    """
    if not events:
        return
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for et, payload in events:
        rows.append((et, json.dumps(payload, ensure_ascii=False, default=str), now, backup_event_key(et, payload)))
    with db() as c:
        c.executemany(
            """
            INSERT OR IGNORE INTO backup_queue
            (event_type, payload_json, status, attempts, created_at, event_key)
            VALUES (?, ?, 'pending', 0, ?, ?)
            """,
            rows,
        )
        c.commit()


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
    """Reconstruye SQLite desde Sheets cuando Streamlit despierta sin base local.

    Sheets se trata como memoria permanente/event log. La reconstrucción es idempotente:
    ignora eventos repetidos por event_key o por una firma estable del movimiento.
    """
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
    audit_rows = []
    incidencias_rows = []
    lote_status_events = []
    seen = set()
    seen_scans = set()
    seen_audit = set()
    seen_incid = set()

    def event_signature(ev: dict) -> str:
        ek = clean_text(ev.get("event_key", ""))
        if ek:
            return ek
        # Firma estable para proteger restauración contra duplicados históricos en Sheets.
        base = json.dumps(ev, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def split_incident_detail(detail: str) -> tuple[str, str]:
        txt = clean_text(detail)
        if "·" in txt:
            a, b = txt.split("·", 1)
            return clean_text(a), clean_text(b)
        return txt or "Incidencia", ""

    for ev in events:
        if not isinstance(ev, dict):
            continue
        sig = event_signature(ev)
        if sig in seen:
            continue
        seen.add(sig)

        et = clean_text(ev.get("event_type", ""))
        try:
            lote_id = int(ev.get("lote_id"))
        except Exception:
            lote_id = None

        if lote_id is None and et not in {"test_webhook"}:
            continue

        if et == "lote_creado":
            lotes[lote_id] = {
                "id": lote_id,
                "nombre": clean_text(ev.get("lote_nombre", "")) or f"Lote {lote_id}",
                "archivo": clean_text(ev.get("archivo", "")),
                "hoja": clean_text(ev.get("hoja", "")),
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or datetime.now().isoformat(timespec="seconds"),
                "status": "ACTIVO",
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
            # Compatibilidad con respaldos viejos.
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
            scan_sig = clean_text(ev.get("event_key", "")) or f"scan_agregado:{lote_id}:{item_id}:{clean_text(ev.get('created_at',''))}:{qty}:{norm_code(ev.get('scan_primario',''))}:{norm_code(ev.get('scan_secundario',''))}"
            if scan_sig in seen_scans:
                continue
            seen_scans.add(scan_sig)
            movement_by_item[item_id] = movement_by_item.get(item_id, 0) + qty
            scan_rows.append((lote_id, item_id, norm_code(ev.get("scan_primario", "")), norm_code(ev.get("scan_secundario", "")), qty, clean_text(ev.get("modo", "")), clean_text(ev.get("created_at", "")) or datetime.now().isoformat(timespec="seconds")))
        elif et == "scan_deshacer":
            try:
                item_id = int(ev.get("item_id"))
                qty = int(ev.get("cantidad") or 0)
            except Exception:
                continue
            undo_sig = clean_text(ev.get("event_key", "")) or f"scan_deshacer:{lote_id}:{item_id}:{clean_text(ev.get('created_at',''))}:{qty}:{norm_code(ev.get('scan_primario',''))}:{norm_code(ev.get('scan_secundario',''))}"
            if undo_sig in seen_scans:
                continue
            seen_scans.add(undo_sig)
            movement_by_item[item_id] = movement_by_item.get(item_id, 0) - qty
        elif et == "lote_eliminado":
            deleted_lotes.add(lote_id)
        elif et == "audit_event":
            audit_key = clean_text(ev.get("event_key", "")) or sig
            if audit_key in seen_audit:
                continue
            seen_audit.add(audit_key)
            audit_type = clean_text(ev.get("audit_type", "")) or clean_text(ev.get("audit_event_type", "")) or "AUDIT_EVENT"
            created = clean_text(ev.get("created_at", "")) or clean_text(ev.get("audit_created_at", "")) or clean_text(ev.get("queued_at", "")) or datetime.now().isoformat(timespec="seconds")
            item_id_val = None
            try:
                if clean_text(ev.get("item_id", "")):
                    item_id_val = int(ev.get("item_id"))
            except Exception:
                item_id_val = None
            qty_val = None
            try:
                if clean_text(ev.get("qty", "")):
                    qty_val = int(float(ev.get("qty")))
            except Exception:
                qty_val = None
            audit_rows.append((
                lote_id, item_id_val, audit_type, clean_text(ev.get("detail", "")), qty_val,
                norm_code(ev.get("codigo_ml", "")), norm_code(ev.get("sku", "")), clean_text(ev.get("mode", "")), created,
                audit_key, clean_text(ev.get("usuario", "")), clean_text(ev.get("source_module", "")),
                clean_text(ev.get("before_json", "")), clean_text(ev.get("after_json", "")), 1,
            ))
            if audit_type == "INCIDENCIA_ABIERTA":
                inc_sig = audit_key
                if inc_sig not in seen_incid:
                    seen_incid.add(inc_sig)
                    tipo, comentario = split_incident_detail(clean_text(ev.get("detail", "")))
                    incidencias_rows.append((lote_id, item_id_val, tipo, qty_val or 0, comentario, clean_text(ev.get("usuario", "")) or clean_text(ev.get("mode", "")) or "SIN_USUARIO", "ABIERTA", created, None, None, None))
            elif audit_type == "INCIDENCIA_RESUELTA":
                # Se refleja de forma conservadora: deja un registro de resolución en auditoría.
                pass
            elif audit_type in {"LOTE_CERRADO", "LOTE_REABIERTO"}:
                lote_status_events.append((created, lote_id, audit_type, clean_text(ev.get("usuario", "")) or clean_text(ev.get("mode", "")), clean_text(ev.get("detail", ""))))

    active_lote_ids = [lid for lid in lotes if lid not in deleted_lotes and items_by_lote.get(lid)]
    if not active_lote_ids:
        return False, "No encontré lotes activos con snapshot completo en Sheets. Crea el lote una vez con esta nueva versión para activar restauración automática."

    now = datetime.now().isoformat(timespec="seconds")
    restored_lotes = 0
    restored_items = 0
    with db() as c:
        for lid in sorted(active_lote_ids):
            lote = lotes[lid]
            c.execute("""
                INSERT OR REPLACE INTO lotes
                (id, nombre, archivo, hoja, created_at, status, closed_at, closed_by, close_note)
                VALUES (?, ?, ?, ?, ?, COALESCE((SELECT status FROM lotes WHERE id=?), 'ACTIVO'), NULL, NULL, NULL)
            """, (lote["id"], lote["nombre"], lote["archivo"], lote["hoja"], lote["created_at"], lote["id"]))
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
        for row in audit_rows:
            if row[0] in active_lote_ids:
                c.execute("""
                    INSERT OR IGNORE INTO audit_events
                    (lote_id, item_id, event_type, detail, qty, codigo_ml, sku, mode, created_at,
                     event_key, usuario, source_module, before_json, after_json, synced_to_sheets)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
        for row in incidencias_rows:
            if row[0] in active_lote_ids:
                c.execute("""
                    INSERT INTO incidencias
                    (lote_id, item_id, tipo, cantidad, comentario, usuario, status, created_at, resolved_at, resolved_by, resolution_comment)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
        for created, lid, typ, user, note in sorted(lote_status_events):
            if lid not in active_lote_ids:
                continue
            if typ == "LOTE_CERRADO":
                c.execute("UPDATE lotes SET status='CERRADO', closed_at=?, closed_by=?, close_note=? WHERE id=?", (created, user or "SIN_USUARIO", note, lid))
            elif typ == "LOTE_REABIERTO":
                c.execute("UPDATE lotes SET status='ACTIVO', closed_at=NULL, closed_by=NULL, close_note=NULL WHERE id=?", (lid,))
        c.commit()
    return True, f"Restauración automática completa: {restored_lotes} lote(s), {restored_items} producto(s), {len(audit_rows)} evento(s) de auditoría."


def flush_backup_queue(webhook_url: str | None = None, limit: int = 25):
    """Envía pendientes a Sheets con bloqueo lógico.

    Antes de llamar al webhook, los eventos pasan de pending -> sending dentro de una
    transacción IMMEDIATE. Así, aunque Streamlit rerunée o el usuario presione otra vez,
    otro flush no puede tomar las mismas filas. Si falla el envío, vuelven a pending.
    """
    url = clean_text(webhook_url or get_backup_webhook_url())
    if not url:
        return

    # Reclamo atómico de trabajo pendiente.
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        ids = [int(r["id"]) for r in c.execute(
            """
            SELECT id
            FROM backup_queue
            WHERE status='pending'
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()]
        if not ids:
            c.commit()
            return
        placeholders = ",".join(["?"] * len(ids))
        c.execute(f"UPDATE backup_queue SET status='sending' WHERE id IN ({placeholders}) AND status='pending'", ids)
        rows = c.execute(
            f"""
            SELECT id, event_type, payload_json, attempts, created_at
            FROM backup_queue
            WHERE id IN ({placeholders}) AND status='sending'
            ORDER BY id ASC
            """,
            ids,
        ).fetchall()
        c.commit()

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
                    "UPDATE backup_queue SET status='sent', sent_at=?, last_error=NULL WHERE id=? AND status='sending'",
                    (sent_at, int(row["id"])),
                )
                c.commit()

        except Exception as e:
            with db() as c:
                c.execute(
                    "UPDATE backup_queue SET status='pending', attempts=attempts+1, last_error=? WHERE id=? AND status='sending'",
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
        "deleted_at": datetime.now().isoformat(timespec="seconds"),
    })
    flush_backup_queue(limit=50)
    log_audit_event(lote_id, event_type="LOTE_ELIMINADO", detail="Lote eliminado", qty=int(items_count))


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
    flush_backup_queue(limit=50)
    log_audit_event(lote_id, item_id, "SKU_ESCANEADO", clean_text(item["descripcion"]), int(cantidad), item["codigo_ml"], item["sku"], modo)
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
    flush_backup_queue(limit=50)
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
    log_audit_event(lote_id, event_type="ZPL_REIMPRESO" if is_reprint else "ZPL_DESCARGADO", detail=f"Bloque {int(block['block_index'])}", qty=int(block.get("total_qty", 0)), mode="BLOQUE")


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
    log_audit_event(lote_id, int(item.get("id")), "ZPL_INDIVIDUAL", clean_text(item.get("descripcion", "")), int(qty), item.get("codigo_ml", ""), item.get("sku", ""), "INDIVIDUAL")

# ============================================================
# Auditoría operacional Fase 1
# ============================================================

def log_audit_event(lote_id=None, item_id=None, event_type="", detail="", qty=None, codigo_ml="", sku="", mode="", usuario="", source_module="", before=None, after=None, sync=True):
    """Registra una acción operacional en SQLite y la deja en cola para Sheets.

    Regla de arquitectura:
    - SQLite = memoria rápida temporal.
    - Sheets = memoria permanente oficial.
    Por eso cada auditoría local genera también un evento audit_event idempotente para Sheets.
    """
    try:
        now = datetime.now().isoformat(timespec="seconds")
        usuario_final = clean_text(usuario) or clean_text(mode) or "SIN_USUARIO"
        before_json = json.dumps(before, ensure_ascii=False, default=str) if before is not None else ""
        after_json = json.dumps(after, ensure_ascii=False, default=str) if after is not None else ""
        base_key = {
            "lote_id": int(lote_id) if lote_id is not None else None,
            "item_id": int(item_id) if item_id is not None else None,
            "event_type": clean_text(event_type),
            "detail": clean_text(detail),
            "qty": int(qty) if qty is not None else None,
            "codigo_ml": norm_code(codigo_ml),
            "sku": norm_code(sku),
            "mode": clean_text(mode),
            "usuario": usuario_final,
            "created_at": now,
        }
        event_key = "audit:" + hashlib.sha1(json.dumps(base_key, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        with db() as c:
            c.execute(
                """
                INSERT OR IGNORE INTO audit_events
                (lote_id, item_id, event_type, detail, qty, codigo_ml, sku, mode, created_at,
                 event_key, usuario, source_module, before_json, after_json, synced_to_sheets)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    int(lote_id) if lote_id is not None else None,
                    int(item_id) if item_id is not None else None,
                    clean_text(event_type), clean_text(detail),
                    int(qty) if qty is not None else None,
                    norm_code(codigo_ml), norm_code(sku), clean_text(mode), now,
                    event_key, usuario_final, clean_text(source_module), before_json, after_json,
                ),
            )
            c.commit()

        if sync:
            enqueue_backup_event("audit_event", {
                "event_key": event_key,
                "audit_type": clean_text(event_type),
                "lote_id": int(lote_id) if lote_id is not None else None,
                "item_id": int(item_id) if item_id is not None else None,
                "detail": clean_text(detail),
                "qty": int(qty) if qty is not None else None,
                "codigo_ml": norm_code(codigo_ml),
                "sku": norm_code(sku),
                "mode": clean_text(mode),
                "usuario": usuario_final,
                "source_module": clean_text(source_module),
                "before_json": before_json,
                "after_json": after_json,
                "created_at": now,
            })
            # Flush pequeño y controlado: evita que la auditoría quede solo local hasta el próximo click.
            flush_backup_queue(limit=20)
            try:
                with db() as c:
                    c.execute("UPDATE audit_events SET synced_to_sheets=1 WHERE event_key=?", (event_key,))
                    c.commit()
            except Exception:
                pass
    except Exception:
        # La auditoría nunca debe botar el flujo operativo.
        pass


def get_audit_events(lote_id=None, limit=300) -> pd.DataFrame:
    with db() as c:
        if lote_id:
            return pd.read_sql_query(
                """
                SELECT created_at, event_type, detail, qty, codigo_ml, sku, mode, usuario, source_module, item_id
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
            SELECT created_at, lote_id, event_type, detail, qty, codigo_ml, sku, mode, usuario, source_module, item_id
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
            SELECT s.created_at, i.descripcion, i.codigo_ml, i.sku, s.cantidad, s.modo
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
    """Incidencias creadas desde terreno.
    El operador reporta el problema en el mismo flujo de escaneo; supervisor solo gestiona/resuelve.
    """
    current_item_id = None
    current_label = ""
    if current_item is not None:
        try:
            current_item_id = int(current_item.get("id"))
            current_label = f"Producto actual: {clean_text(current_item.get('descripcion',''))[:80]} | ML {clean_text(current_item.get('codigo_ml',''))} | SKU {clean_text(current_item.get('sku',''))}"
        except Exception:
            current_item_id = None
            current_label = ""

    with st.expander("Reportar incidencia", expanded=False):
        st.caption("Usa esto en el momento exacto del problema. Queda registrado para que Supervisor lo resuelva antes del cierre.")
        source_options = []
        option_map = {}
        if current_item_id:
            source_options.append(current_label)
            option_map[current_label] = current_item_id
        source_options.append("Incidencia general del lote")
        option_map["Incidencia general del lote"] = None

        if items is not None and not items.empty:
            source_options.append("Seleccionar otro producto")

        with st.form("scan_incident_form", clear_on_submit=True):
            selected_source = st.radio("Asociar incidencia a", source_options, horizontal=False, key="scan_inc_source")
            selected_item_id = option_map.get(selected_source)

            if selected_source == "Seleccionar otro producto":
                choices = []
                choice_map = {}
                work = items.copy()
                work["pendiente"] = (work["unidades"].astype(int) - work["acopiadas"].astype(int)).clip(lower=0)
                work = work.sort_values(["pendiente", "descripcion"], ascending=[False, True])
                for _, r in work.iterrows():
                    label = f"{clean_text(r.get('descripcion',''))[:75]} | ML {clean_text(r.get('codigo_ml',''))} | SKU {clean_text(r.get('sku',''))} | Pend: {int(r.get('pendiente') or 0)}"
                    choices.append(label)
                    choice_map[label] = int(r["id"])
                picked = st.selectbox("Producto", choices, key="scan_inc_item_select") if choices else None
                selected_item_id = choice_map.get(picked) if picked else None

            c1, c2 = st.columns([2, 1])
            with c1:
                tipo_inc = st.selectbox("Tipo de incidencia", INCIDENCIA_TIPOS, key="scan_inc_tipo")
            with c2:
                qty_inc = st.number_input("Cantidad afectada", min_value=0, max_value=9999, value=0, step=1, key="scan_inc_qty")
            usuario_inc = st.text_input("Usuario que reporta", key="scan_inc_usuario", placeholder="Ej: p1, p2, supervisor")
            comentario_inc = st.text_area("Comentario", key="scan_inc_comentario", placeholder="Describe qué ocurrió: falta, daño, diferencia, etiqueta, etc.")
            submit_inc = st.form_submit_button("Guardar incidencia", type="primary")

        if submit_inc:
            if not clean_text(usuario_inc):
                st.error("Ingresa el usuario que reporta la incidencia.")
            elif len(clean_text(comentario_inc)) < 3:
                st.error("Agrega un comentario mínimo para que la incidencia sea útil.")
            else:
                create_incidencia(lote_id, selected_item_id, tipo_inc, int(qty_inc), comentario_inc, usuario_inc)
                st.success("Incidencia registrada para revisión de Supervisor.")
                st.rerun()


# ============================================================
# Fase 2: Supervisor, incidencias, reimpresión controlada y cierre
# ============================================================

INCIDENCIA_TIPOS = [
    "Falta producto",
    "Producto dañado",
    "Código no coincide",
    "Cantidad menor",
    "Cantidad mayor",
    "Etiqueta dañada",
    "Problema de impresión",
    "Otro",
]


def get_operator_name() -> str:
    return clean_text(st.session_state.get("operator_name", "")) or "SIN_USUARIO"


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
                   inc.resolution_comment, i.codigo_ml, i.sku, i.descripcion
            FROM incidencias inc
            LEFT JOIN items i ON i.id=inc.item_id
            {sql_where}
            ORDER BY inc.id DESC
            """,
            c,
            params=params,
        )


def create_incidencia(lote_id: int, item_id, tipo: str, cantidad: int, comentario: str, usuario: str):
    now = datetime.now().isoformat(timespec="seconds")
    item = {}
    if item_id:
        with db() as c:
            row = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (int(item_id), int(lote_id))).fetchone()
            item = dict(row) if row else {}
    with db() as c:
        c.execute(
            """
            INSERT INTO incidencias
            (lote_id, item_id, tipo, cantidad, comentario, usuario, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'ABIERTA', ?)
            """,
            (
                int(lote_id),
                int(item_id) if item_id else None,
                clean_text(tipo),
                max(0, int(cantidad or 0)),
                clean_text(comentario),
                clean_text(usuario) or "SIN_USUARIO",
                now,
            ),
        )
        c.commit()

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


def resolve_incidencia(incidencia_id: int, usuario: str, comentario: str):
    now = datetime.now().isoformat(timespec="seconds")
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
    motivo = clean_text(motivo)
    usuario = clean_text(usuario) or "SIN_USUARIO"
    if len(motivo) < 5:
        return False, "Debes ingresar un motivo claro de reimpresión."

    now = datetime.now().isoformat(timespec="seconds")
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

    log_audit_event(lote_id, event_type="REIMPRESION_CONTROLADA", detail=f"Bloque {int(block['block_index'])} · {motivo}", qty=int(block["total_qty"]), mode=usuario)
    return True, "Reimpresión registrada."


def register_controlled_item_reprint(lote_id: int, item: dict, qty: int, motivo: str, usuario: str):
    motivo = clean_text(motivo)
    usuario = clean_text(usuario) or "SIN_USUARIO"
    qty = max(1, int(qty or 1))
    if len(motivo) < 5:
        return False, "Debes ingresar un motivo claro de reimpresión."

    now = datetime.now().isoformat(timespec="seconds")
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
    log_audit_event(lote_id, int(item.get("id")), "REIMPRESION_CONTROLADA", f"Producto · {motivo}", qty, item.get("codigo_ml", ""), item.get("sku", ""), usuario)
    return True, "Reimpresión individual registrada."


def supervisor_metrics(lote_id: int) -> dict:
    items = get_items(lote_id)
    if items.empty:
        return {"total": 0, "done": 0, "pending": 0, "incidencias_abiertas": 0, "label_pending": 0}
    view = items.copy()
    view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
    labels = label_control_view(lote_id)
    incid = get_incidencias(lote_id, status="ABIERTA")
    return {
        "total": int(view["unidades"].sum()),
        "done": int(view["acopiadas"].sum()),
        "pending": int(view["pendiente"].sum()),
        "incidencias_abiertas": int(len(incid)),
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
        "label_pending": label_pending,
        "expected_blocks": int(len(blocks_expected)),
        "printed_blocks": int(len(blocks_db)),
    }


def close_lote(lote_id: int, usuario: str, nota: str):
    ok, issues, _ = cierre_validaciones(lote_id)
    if not ok:
        return False, "No se puede cerrar: " + " ".join(issues)
    now = datetime.now().isoformat(timespec="seconds")
    usuario = clean_text(usuario) or "SIN_USUARIO"
    with db() as c:
        c.execute(
            "UPDATE lotes SET status='CERRADO', closed_at=?, closed_by=?, close_note=? WHERE id=?",
            (now, usuario, clean_text(nota), int(lote_id)),
        )
        c.commit()
    log_audit_event(lote_id, event_type="LOTE_CERRADO", detail=clean_text(nota), mode=usuario)
    return True, "Lote cerrado correctamente."


def reopen_lote(lote_id: int, usuario: str, motivo: str):
    usuario = clean_text(usuario) or "SIN_USUARIO"
    with db() as c:
        c.execute("UPDATE lotes SET status='ACTIVO', closed_at=NULL, closed_by=NULL, close_note=NULL WHERE id=?", (int(lote_id),))
        c.commit()
    log_audit_event(lote_id, event_type="LOTE_REABIERTO", detail=clean_text(motivo), mode=usuario)
    return True, "Lote reabierto."

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
    audit = get_audit_events(lote_id, limit=5000)
    incidencias = get_incidencias(lote_id)
    reimpresiones = get_reimpresiones(lote_id)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        items.to_excel(writer, sheet_name="control_full", index=False)
        scans.to_excel(writer, sheet_name="escaneos", index=False)
        audit.to_excel(writer, sheet_name="auditoria", index=False)
        incidencias.to_excel(writer, sheet_name="incidencias", index=False)
        reimpresiones.to_excel(writer, sheet_name="reimpresiones", index=False)
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
    # Usuario se solicita solo donde realmente corresponde: incidencia, reimpresión o cierre.
    page = st.radio("Vista", ["Escaneo", "Cargar lote FULL", "Supervisor", "Etiquetas"], label_visibility="collapsed")
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
                    submit_sig = f"{active_lote}:{int(candidate['id'])}:{qty}:{norm_code(st.session_state.get('scan_primary', ''))}:{norm_code(st.session_state.get('scan_secondary', ''))}:{modo}"
                    if st.session_state.get("_last_scan_submit_sig") == submit_sig:
                        st.warning("Este escaneo ya fue procesado. Limpia o escanea el siguiente producto.")
                    else:
                        st.session_state["_last_scan_submit_sig"] = submit_sig
                        ok, msg = add_acopio(active_lote, int(candidate["id"]), int(qty), st.session_state.get("scan_primary", ""), st.session_state.get("scan_secondary", ""), modo)
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
            })
            st.dataframe(recientes, use_container_width=True, hide_index=True, height=260)

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

        s1, s2, s3, s4, s5 = st.columns(5)
        s1.metric("Estado lote", clean_text(lote.get("status", "ACTIVO")))
        s2.metric("Avance", f"{avance:.1f}%")
        s3.metric("Pendientes", metrics["pending"])
        s4.metric("Incidencias abiertas", metrics["incidencias_abiertas"])
        s5.metric("Etiquetas pendientes", metrics["label_pending"])

        st.progress(done / total if total else 0)
        st.caption(f"Archivo: {lote.get('archivo','')} · Hoja: {lote.get('hoja','')} · Creado: {fmt_dt(lote.get('created_at',''))}")

        if ok_cierre:
            st.success("El lote está apto para cierre formal.")
        else:
            st.warning("El lote aún no está apto para cierre.")
            for issue in issues:
                st.write(f"• {issue}")

        tab_resumen, tab_pendientes, tab_incid, tab_control, tab_auditoria, tab_bloques, tab_reimp, tab_cierre = st.tabs(["Resumen", "Pendientes", "Incidencias", "Control", "Auditoría", "Bloques", "Reimpresión", "Cierre"])

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
                }])
                st.dataframe(resumen, use_container_width=True, hide_index=True)

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

        with tab_control:
            st.subheader("Control de lote")
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
                total_control = int(view["unidades"].sum())
                done_control = int(view["acopiadas"].sum())
                c1.metric("Unidades", total_control)
                c2.metric("Acopiadas", done_control)
                c3.metric("Pendientes", max(total_control - done_control, 0))
                c4.metric("Avance", f"{(done_control / total_control * 100) if total_control else 0:.1f}%")

                filtro = st.selectbox("Filtro", ["Todos", "Pendientes", "Completos", "Supermercado"], key="sup_control_filter")
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
                modo_vista = st.radio("Vista control", ["Tarjetas operativas", "Tabla"], horizontal=True, key="sup_control_view_mode")
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
                        st.markdown(
                            f"""
                            <div class='control-card'>
                                <div class='control-title'>{esc(r['descripcion'])}</div>
                                <div class='control-meta'><b>SKU:</b> {esc(r['sku'])} &nbsp; | &nbsp; <b>Código ML:</b> {esc(r['codigo_ml'])}</div>
                                <div>{''.join(badges_parts)}</div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
                else:
                    out = show.copy()
                    out["Procesado"] = out["procesado_at"].map(fmt_dt)
                    out = out.rename(columns={
                        "sku": "SKU", "codigo_ml": "Código ML", "codigo_universal": "EAN / Código universal",
                        "descripcion": "Producto", "unidades": "Unidades", "acopiadas": "Acopiadas", "pendiente": "Pendiente",
                        "identificacion": "Identificación", "vence": "Vence", "estado": "Estado"
                    })
                    cols = ["SKU", "Código ML", "EAN / Código universal", "Producto", "Unidades", "Acopiadas", "Pendiente", "Identificación", "Vence", "Procesado", "Estado"]
                    st.dataframe(out[[c for c in cols if c in out.columns]], use_container_width=True, hide_index=True, height=620)

                st.download_button("Exportar control Excel", data=export_lote(active_lote), file_name="control_full_aurora.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="sup_export_control_excel")
                st.divider()
                if st.button("Eliminar lote activo", key="sup_delete_lote"):
                    delete_lote(active_lote)
                    st.success("Lote eliminado.")
                    st.rerun()

        with tab_auditoria:
            st.subheader("Auditoría operacional")
            eventos = get_audit_events(active_lote, limit=500)
            if eventos.empty:
                st.info("Aún no hay eventos de auditoría para este lote.")
            else:
                f_eventos = ["Todos"] + sorted([x for x in eventos["event_type"].dropna().unique().tolist()])
                filtro_evento = st.selectbox("Filtrar evento", f_eventos, key="sup_audit_filter")
                show_audit = eventos.copy()
                if filtro_evento != "Todos":
                    show_audit = show_audit[show_audit["event_type"] == filtro_evento]
                show_audit = show_audit.rename(columns={
                    "created_at": "Fecha",
                    "event_type": "Evento",
                    "detail": "Detalle",
                    "qty": "Cantidad",
                    "codigo_ml": "Código ML",
                    "sku": "SKU",
                    "mode": "Modo",
                    "item_id": "Item ID",
                })
                st.dataframe(show_audit, use_container_width=True, hide_index=True, height=650)
                st.caption("La auditoría queda guardada en SQLite y también se incluye en el Excel de control exportado.")

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
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Estado actual", clean_text(lote_close.get("status", "ACTIVO")))
            c2.metric("Unidades pendientes", data_close2.get("pending_units", 0))
            c3.metric("Incidencias abiertas", data_close2.get("open_incidents", 0))
            c4.metric("Bloques", f"{data_close2.get('printed_blocks',0)}/{data_close2.get('expected_blocks',0)}")
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


elif page == "Incidencias":
    st.subheader("Incidencias operativas")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        items = get_items(active_lote)
        tab_new, tab_open, tab_all = st.tabs(["Nueva incidencia", "Abiertas", "Historial"])
        with tab_new:
            st.info("Registra problemas reales del lote: faltantes, daños, códigos que no coinciden o problemas de impresión.")
            options = ["General del lote"]
            option_map = {"General del lote": None}
            for _, r in items.iterrows():
                label = f"{clean_text(r.get('descripcion',''))[:80]} | ML {clean_text(r.get('codigo_ml',''))} | SKU {clean_text(r.get('sku',''))}"
                options.append(label)
                option_map[label] = int(r["id"])
            selected_inc = st.selectbox("Producto afectado", options, index=0)
            tipo_inc = st.selectbox("Tipo de incidencia", INCIDENCIA_TIPOS)
            qty_inc = st.number_input("Cantidad afectada", min_value=0, max_value=99999, value=1, step=1)
            comentario_inc = st.text_area("Comentario", placeholder="Describe qué ocurrió y qué evidencia existe.")
            usuario_inc = st.text_input("Usuario responsable del registro", value=get_operator_name(), key="inc_usuario")
            if st.button("Registrar incidencia", type="primary"):
                if len(clean_text(comentario_inc)) < 3:
                    st.error("Agrega un comentario mínimo para que la incidencia sea útil.")
                else:
                    create_incidencia(active_lote, option_map.get(selected_inc), tipo_inc, int(qty_inc), comentario_inc, usuario_inc)
                    st.success("Incidencia registrada.")
                    st.rerun()
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
