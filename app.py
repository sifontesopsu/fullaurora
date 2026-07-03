import io
import re
import html
import hashlib
import json
import os
import time
import uuid
import socket
import sqlite3
import threading
import urllib.request
import urllib.parse
import urllib.error
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
PACKS_PATH = DATA_DIR / "packs.xlsx"
DEFAULT_SHEETS_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbzwfCk7ov8fCdX3WoTon-25Q8W-iLZUfWqUTvRSLjOGrkid6J2fNgGSmnSbB7lqUiw/exec"
MAX_BACKUP_ATTEMPTS = 5
SHEETS_STRICT_MODE = False  # Modo SQLite-first: Sheets es espejo/respaldo, no bloquea la operación.
SCAN_OPERATORS = ["ERICK"]
_BACKUP_SYNC_RUNNING = False
_BACKUP_SYNC_LOCK = threading.Lock()

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
        c.execute("""
            CREATE TABLE IF NOT EXISTS reservas_kame (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                folio TEXT NOT NULL,
                folio_auto TEXT,
                ficha TEXT,
                fecha TEXT,
                glosa TEXT,
                bodega_salida TEXT,
                unidad_negocio TEXT,
                sku_count INTEGER NOT NULL DEFAULT 0,
                unidades_total REAL NOT NULL DEFAULT 0,
                productos_full INTEGER NOT NULL DEFAULT 0,
                packs_expandidos INTEGER NOT NULL DEFAULT 0,
                lineas_csv INTEGER NOT NULL DEFAULT 0,
                archivo_nombre TEXT,
                csv_hash TEXT,
                usuario TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS postventa_full_errores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                item_id INTEGER,
                codigo_ml TEXT,
                codigo_universal TEXT,
                sku TEXT,
                descripcion TEXT,
                tipo_error TEXT NOT NULL,
                cantidad_solicitada INTEGER NOT NULL DEFAULT 0,
                cantidad_preparada INTEGER NOT NULL DEFAULT 0,
                cantidad_reportada_full INTEGER,
                cantidad_diferencia INTEGER NOT NULL DEFAULT 0,
                cantidad_afectada INTEGER NOT NULL DEFAULT 0,
                comentario TEXT,
                usuario TEXT,
                estado TEXT NOT NULL DEFAULT 'ACTIVO',
                created_at TEXT NOT NULL,
                anulado_at TEXT,
                anulado_by TEXT,
                anulado_motivo TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS postventa_full_cierres (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                lote_nombre TEXT,
                total_errores INTEGER NOT NULL DEFAULT 0,
                errores_activos INTEGER NOT NULL DEFAULT 0,
                unidades_afectadas INTEGER NOT NULL DEFAULT 0,
                cerrado_por TEXT,
                comentario TEXT,
                created_at TEXT NOT NULL
            )
        """)
        ensure_column(c, "backup_queue", "last_attempt_at", "TEXT")
        ensure_column(c, "lotes", "status", "TEXT NOT NULL DEFAULT 'ACTIVO'")
        ensure_column(c, "lotes", "closed_at", "TEXT")
        ensure_column(c, "lotes", "closed_by", "TEXT")
        ensure_column(c, "lotes", "close_note", "TEXT")
        # Identidad estable de respaldo: evita cruzar FULL distintos cuando SQLite reinicia sus IDs.
        ensure_column(c, "lotes", "backup_lote_key", "TEXT")
        ensure_column(c, "items", "instrucciones", "TEXT")
        ensure_column(c, "items", "fuente_rescate", "TEXT")
        ensure_column(c, "items", "rescue_note", "TEXT")
        # Descripciones separadas: Kame para operación; ML para etiquetas.
        ensure_column(c, "items", "descripcion_kame", "TEXT")
        ensure_column(c, "items", "descripcion_ml", "TEXT")
        ensure_column(c, "items", "descripcion_fuente", "TEXT")
        ensure_column(c, "items", "familia_kame", "TEXT")
        ensure_column(c, "items", "maestro_match_status", "TEXT")
        # Anexos manuales al FULL: productos agregados después del PDF/Excel original.
        ensure_column(c, "items", "origen_item", "TEXT NOT NULL DEFAULT 'PDF_FULL'")
        ensure_column(c, "items", "motivo_anexo", "TEXT")
        ensure_column(c, "items", "usuario_anexo", "TEXT")
        ensure_column(c, "items", "fecha_anexo", "TEXT")
        ensure_column(c, "items", "anexo_ml_confirmado", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "items", "anexo_ml_confirmado_at", "TEXT")
        ensure_column(c, "items", "anexo_ml_confirmado_by", "TEXT")
        ensure_column(c, "items", "anexo_ml_confirmado_comment", "TEXT")
        ensure_column(c, "items", "anexo_kame_confirmado", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "items", "anexo_kame_confirmado_at", "TEXT")
        ensure_column(c, "items", "anexo_kame_confirmado_by", "TEXT")
        ensure_column(c, "items", "anexo_kame_confirmado_comment", "TEXT")
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
        # Snapshot del producto al momento del scan.
        # Esto es crítico para restaurar desde Sheets cuando el item_id histórico no
        # coincide con el id local reconstruido, o cuando el snapshot del lote llegó incompleto.
        ensure_column(c, "scans", "original_item_id", "INTEGER")
        ensure_column(c, "scans", "codigo_ml", "TEXT")
        ensure_column(c, "scans", "codigo_universal", "TEXT")
        ensure_column(c, "scans", "sku", "TEXT")
        ensure_column(c, "scans", "descripcion", "TEXT")
        ensure_column(c, "scans", "descripcion_kame", "TEXT")
        ensure_column(c, "scans", "descripcion_ml", "TEXT")
        ensure_column(c, "scans", "familia_kame", "TEXT")
        ensure_column(c, "scans", "maestro_match_status", "TEXT")
        ensure_column(c, "scans", "restore_match_status", "TEXT")

        # Confirmaciones externas de avisos operacionales: ML y Kame se pueden marcar después de crear el aviso.
        ensure_column(c, "avisos_operacionales", "confirmado_ml_at", "TEXT")
        ensure_column(c, "avisos_operacionales", "confirmado_ml_by", "TEXT")
        ensure_column(c, "avisos_operacionales", "confirmado_inventario_at", "TEXT")
        ensure_column(c, "avisos_operacionales", "confirmado_inventario_by", "TEXT")
        # La reserva Kame se registra como archivo generado/resumen, no producto por producto.
        ensure_column(c, "reservas_kame", "csv_hash", "TEXT")
        ensure_column(c, "postventa_full_errores", "codigo_ml", "TEXT")
        ensure_column(c, "postventa_full_errores", "codigo_universal", "TEXT")
        ensure_column(c, "postventa_full_errores", "sku", "TEXT")
        ensure_column(c, "postventa_full_errores", "descripcion", "TEXT")
        ensure_column(c, "postventa_full_errores", "cantidad_solicitada", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "postventa_full_errores", "cantidad_preparada", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "postventa_full_errores", "cantidad_reportada_full", "INTEGER")
        ensure_column(c, "postventa_full_errores", "cantidad_diferencia", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "postventa_full_errores", "cantidad_afectada", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "postventa_full_errores", "estado", "TEXT NOT NULL DEFAULT 'ACTIVO'")
        ensure_column(c, "postventa_full_errores", "anulado_at", "TEXT")
        ensure_column(c, "postventa_full_errores", "anulado_by", "TEXT")
        ensure_column(c, "postventa_full_errores", "anulado_motivo", "TEXT")
        ensure_column(c, "picking_list_items", "descripcion_kame", "TEXT")
        ensure_column(c, "picking_list_items", "descripcion_ml", "TEXT")
        ensure_column(c, "picking_list_items", "familia_kame", "TEXT")
        ensure_column(c, "picking_list_items", "maestro_match_status", "TEXT")
        ensure_column(c, "postventa_full_errores", "descripcion_kame", "TEXT")
        ensure_column(c, "postventa_full_errores", "descripcion_ml", "TEXT")
        ensure_column(c, "postventa_full_errores", "familia_kame", "TEXT")
        ensure_column(c, "label_prints", "descripcion_kame", "TEXT")
        ensure_column(c, "label_prints", "descripcion_ml", "TEXT")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_label_blocks_unique ON label_blocks (lote_id, block_index, block_key)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_lotes_backup_key ON lotes (backup_lote_key)")
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
        c.execute("CREATE INDEX IF NOT EXISTS idx_reservas_kame_lote ON reservas_kame (lote_id, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_postventa_full_errores_lote ON postventa_full_errores (lote_id, estado, created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_postventa_full_errores_tipo ON postventa_full_errores (tipo_error, estado)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_postventa_full_errores_sku ON postventa_full_errores (sku, estado)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_postventa_full_cierres_lote ON postventa_full_cierres (lote_id, created_at)")

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






def make_event_uid(event_type: str, queued_at: str) -> str:
    """Identificador global del evento.

    No depende del id local de SQLite, porque ese id puede reiniciarse
    cuando Streamlit hace reboot o cuando se reconstruye la base.
    """
    return f"EVT:{clean_text(event_type)}:{clean_text(queued_at)}:{uuid.uuid4().hex}"


def attach_event_identity(event_type: str, payload: dict, queued_at: str) -> dict:
    out = dict(payload or {})
    if not clean_text(out.get("event_uid", "")):
        out["event_uid"] = make_event_uid(event_type, queued_at)
    out["event_source"] = "streamlit_fullaurora"
    return out


def sheet_event_semantic_identity(ev: dict) -> str:
    """Llave de deduplicación para eventos leídos desde Sheets.

    Regla importante:
    Apps Script devuelve el mismo evento desde la hoja madre `eventos` y también
    desde hojas estructuradas como `picking_validaciones`, `picking_items`, etc.
    La hoja madre suele traer `event_uid`, mientras la estructurada suele traer
    `event_key`. Si priorizamos event_uid, el mismo scan puede quedar como dos
    eventos distintos al rescatar.

    Por eso, para rescate, `event_key` manda primero. Luego viene `event_uid` y,
    si no existe ninguno, una llave semántica estable sin usar queue_id solo.
    """
    if not isinstance(ev, dict):
        return ""
    event_key = clean_text(ev.get("event_key", ""))
    if event_key:
        return f"KEY:{event_key}"
    event_uid = clean_text(ev.get("event_uid", ""))
    if event_uid:
        return f"UID:{event_uid}"
    return "SEM:" + "|".join([
        clean_text(ev.get("event_type", "")),
        clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", "")),
        clean_text(ev.get("lote_id", "")),
        clean_text(ev.get("picking_code", "")) or clean_text(ev.get("codigo_lista", "")),
        clean_text(ev.get("picking_list_id", "")),
        clean_text(ev.get("item_id", "")),
        norm_code(ev.get("codigo_ml", "")),
        norm_code(ev.get("codigo_universal", "")),
        norm_code(ev.get("sku", "")),
        clean_text(ev.get("cantidad", "")),
        clean_text(ev.get("scan_primario", "")),
        clean_text(ev.get("scan_secundario", "")),
        clean_text(ev.get("tipo", "")) or clean_text(ev.get("tipo_aviso", "")),
        clean_text(ev.get("comentario", "")) or clean_text(ev.get("detail", "")),
        # Los snapshots vienen en varios chunks con mismo timestamp/lote. Sin
        # estos campos la deduplicación los colapsa y la restauración queda con
        # un fragmento parcial o mezcla de snapshots.
        clean_text(ev.get("snapshot_hash", "")),
        clean_text(ev.get("chunk_index", "")),
        clean_text(ev.get("chunk_total", "")),
    ])


def sheet_event_all_dedupe_ids(ev: dict) -> list[str]:
    """Identidades equivalentes del mismo evento para deduplicar hoja madre + hojas estructuradas."""
    if not isinstance(ev, dict):
        return []
    ids = []
    event_key = clean_text(ev.get("event_key", ""))
    event_uid = clean_text(ev.get("event_uid", ""))
    if event_key:
        ids.append(f"KEY:{event_key}")
        # Muchos event_key antiguos guardan literalmente UID:EVT:...
        if event_key.startswith("UID:"):
            ids.append(event_key)
    if event_uid:
        ids.append(f"UID:{event_uid}")
    sem = sheet_event_semantic_identity({k: v for k, v in ev.items() if k not in {"event_key", "event_uid"}})
    if sem:
        ids.append(sem)
    # Orden estable sin duplicados internos.
    return list(dict.fromkeys([x for x in ids if x]))


def sheet_event_seen_or_mark(ev: dict, seen_ids: set[str]) -> bool:
    """Retorna True si el evento ya fue visto por cualquiera de sus identidades."""
    ids = sheet_event_all_dedupe_ids(ev)
    if ids and any(x in seen_ids for x in ids):
        return True
    for x in ids:
        seen_ids.add(x)
    return False

def stop_for_backup_failure(message: str):
    """Detiene la operación de forma controlada cuando Sheets no confirma respaldo.

    No usamos RuntimeError para no mostrar pantalla roja de Streamlit. Si Sheets es
    la fuente única, la operación debe detenerse, pero con una instrucción clara.
    """
    msg = clean_text(message)
    try:
        st.warning("⚠️ Respaldo Sheets no confirmó.")
        st.info("La operación local en SQLite queda registrada. Revisa la cola de respaldo y sincroniza cuando sea posible.")
        if msg:
            st.code(msg[:1200])
        st.info("Revisa: Apps Script implementado como Nueva versión, URL webhook definitiva y hoja de errores del Apps Script.")
        st.stop()
    except Exception:
        raise RuntimeError(msg or "Respaldo obligatorio en Sheets falló")

def enqueue_backup_event(event_type: str, payload: dict):
    """Registra un evento en la cola local de respaldo sin bloquear la operación.

    Arquitectura SQLite-first:
    - SQLite es la base operativa inmediata.
    - Sheets es espejo/auditoría y se sincroniza desde backup_queue.
    - Ningún operador debe quedar detenido porque Apps Script/Sheets responda lento.
    """
    now = now_cl().isoformat(timespec="seconds")
    payload = attach_event_identity(event_type, payload, now)
    safe_payload = json.dumps(payload, ensure_ascii=False, default=str)
    with db() as c:
        cur = c.execute(
            "INSERT INTO backup_queue (event_type, payload_json, status, attempts, created_at) VALUES (?, ?, 'pending', 0, ?)",
            (event_type, safe_payload, now),
        )
        event_id = int(cur.lastrowid)
        c.commit()

    # Sincronización best-effort, no bloqueante. Si falla, el evento queda pendiente.
    trigger_backup_sync_async(limit=10)
    return event_id


def send_webhook_event(url: str, event: dict, timeout: int = 25) -> tuple[bool, str]:
    """Envía un evento a Apps Script y valida que la respuesta sea JSON con ok=true.

    Importante:
    - Un timeout de respuesta no siempre significa que Sheets no escribió el evento.
      Apps Script puede haber alcanzado a guardar y demorarse en responder.
    - Por eso esta función nunca deja que Streamlit reviente con pantalla roja: devuelve
      (False, detalle) para que la cola/reintentos decidan qué hacer.
    """
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=int(timeout)) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            response_text = resp.read().decode("utf-8", errors="replace")
    except (TimeoutError, socket.timeout) as e:
        return False, (
            "Timeout esperando respuesta de Apps Script. "
            "Si el evento aparece en Sheets, el respaldo sí se escribió; "
            "solo se demoró la confirmación HTTP. Reintenta la prueba o revisa la hoja eventos. "
            f"Detalle: {type(e).__name__}: {e}"
        )
    except urllib.error.URLError as e:
        return False, f"Error de conexión con Apps Script: {e}"
    except Exception as e:
        return False, f"Error inesperado enviando webhook: {type(e).__name__}: {e}"

    if status < 200 or status >= 300:
        return False, f"HTTP {status}: {response_text[:300]}"

    try:
        parsed = json.loads(response_text)
    except Exception:
        return False, f"Respuesta no JSON desde Apps Script: {response_text[:300]}"

    if parsed.get("ok") is True:
        return True, response_text[:300]

    if parsed.get("transient") is True or parsed.get("retry") is True:
        return False, f"TRANSIENT_APPS_SCRIPT: {response_text[:500]}"

    return False, f"Apps Script respondió ok=false: {response_text[:500]}"




def enqueue_backup_events_batch(events):
    """Inserta muchos eventos en cola local sin esperar a Sheets."""
    if not events:
        return []
    now = now_cl().isoformat(timespec="seconds")
    rows = []
    for et, payload in events:
        payload = attach_event_identity(et, payload, now)
        rows.append((et, json.dumps(payload, ensure_ascii=False, default=str), now))
    with db() as c:
        ids = []
        for et, payload_json, created_at in rows:
            cur = c.execute(
                "INSERT INTO backup_queue (event_type, payload_json, status, attempts, created_at) VALUES (?, ?, 'pending', 0, ?)",
                (et, payload_json, created_at),
            )
            ids.append(int(cur.lastrowid))
        c.commit()

    # Best-effort en segundo plano; la operación ya quedó en SQLite.
    trigger_backup_sync_async(limit=max(10, min(100, len(ids))))
    return ids


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


def local_lote_ids() -> set[int]:
    with db() as c:
        rows = c.execute("SELECT id FROM lotes").fetchall()
    return {int(r["id"]) for r in rows}


def restore_from_backup_if_empty(allow_existing: bool = False, only_missing: bool = False, only_lote_id: int | None = None, replace_existing: bool = False):
    """Restaura/sincroniza base local desde Sheets.

    - Modo automático: restaura solo si SQLite está vacío.
    - Modo manual con allow_existing=True y only_missing=True: trae lotes que existen en Sheets
      pero no existen en la base local, sin duplicar movimientos del lote ya presente.
    - Modo rescate con only_lote_id: restaura únicamente el lote elegido por el usuario.
    """

    existing_local_ids = local_lote_ids()
    if existing_local_ids and not allow_existing:
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
    seen_event_ids = set()
    for raw_ev in events:
        ev = normalize_event(raw_ev)
        semantic_id = sheet_event_semantic_identity(ev)
        if semantic_id:
            if semantic_id in seen_event_ids:
                continue
            seen_event_ids.add(semantic_id)
        normalized_events.append(ev)

    def event_order_key(ev):
        ts = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", ""))
        qid = clean_text(ev.get("queue_id", ""))
        try:
            qorder = int(qid)
        except Exception:
            qorder = 0
        return (ts, qorder, sheet_event_semantic_identity(ev))

    normalized_events.sort(key=event_order_key)

    def _snapshot_item_from_event(item_ev: dict, parent_ev: dict) -> dict | None:
        try:
            item_id = int(item_ev.get("item_id"))
        except Exception:
            return None
        return {
            "id": item_id,
            "lote_id": int(parent_ev.get("lote_id")),
            "area": clean_text(item_ev.get("area", "")),
            "nro": clean_text(item_ev.get("nro", "")),
            "codigo_ml": norm_code(item_ev.get("codigo_ml", "")),
            "codigo_universal": norm_code(item_ev.get("codigo_universal", "")),
            "sku": norm_code(item_ev.get("sku", "")),
            "descripcion": clean_text(item_ev.get("descripcion_kame", "")) or clean_text(item_ev.get("descripcion", "")),
            "descripcion_kame": clean_text(item_ev.get("descripcion_kame", "")) or clean_text(item_ev.get("descripcion", "")),
            "descripcion_ml": clean_text(item_ev.get("descripcion_ml", "")) or clean_text(item_ev.get("descripcion", "")),
            "descripcion_fuente": clean_text(item_ev.get("descripcion_fuente", "")),
            "familia_kame": clean_text(item_ev.get("familia_kame", "")),
            "maestro_match_status": clean_text(item_ev.get("maestro_match_status", "")),
            "origen_item": clean_text(item_ev.get("origen_item", "PDF_FULL")) or "PDF_FULL",
            "motivo_anexo": clean_text(item_ev.get("motivo_anexo", "")),
            "usuario_anexo": clean_text(item_ev.get("usuario_anexo", "")),
            "fecha_anexo": clean_text(item_ev.get("fecha_anexo", "")),
            "anexo_ml_confirmado": to_int(item_ev.get("anexo_ml_confirmado", 0)),
            "anexo_ml_confirmado_at": clean_text(item_ev.get("anexo_ml_confirmado_at", "")),
            "anexo_ml_confirmado_by": clean_text(item_ev.get("anexo_ml_confirmado_by", "")),
            "anexo_ml_confirmado_comment": clean_text(item_ev.get("anexo_ml_confirmado_comment", "")),
            "anexo_kame_confirmado": to_int(item_ev.get("anexo_kame_confirmado", 0)),
            "anexo_kame_confirmado_at": clean_text(item_ev.get("anexo_kame_confirmado_at", "")),
            "anexo_kame_confirmado_by": clean_text(item_ev.get("anexo_kame_confirmado_by", "")),
            "anexo_kame_confirmado_comment": clean_text(item_ev.get("anexo_kame_confirmado_comment", "")),
            "unidades": to_int(item_ev.get("unidades", 0)),
            "acopiadas": 0,
            "identificacion": clean_text(item_ev.get("identificacion", "")),
            "vence": clean_text(item_ev.get("vence", "")),
            "instrucciones": clean_text(item_ev.get("instrucciones", "")),
            "dia": clean_text(item_ev.get("dia", "")),
            "hora": clean_text(item_ev.get("hora", "")),
            "created_at": clean_text(item_ev.get("item_created_at", "")) or clean_text(parent_ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
            "updated_at": clean_text(item_ev.get("item_updated_at", "")) or clean_text(parent_ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
        }

    def _latest_complete_snapshot_maps(events_list: list[dict]) -> tuple[dict[int, dict[int, dict]], dict[int, dict]]:
        """Selecciona un solo snapshot completo por lote para evitar mezclas históricas."""
        groups = {}
        for order, ev in enumerate(events_list):
            if clean_text(ev.get("event_type", "")) != "lote_snapshot_chunk":
                continue
            try:
                lid = int(ev.get("lote_id"))
            except Exception:
                continue
            if not (ev.get("items") or []):
                continue
            total_chunks = to_int(ev.get("chunk_total", 0)) or 1
            chunk_index = to_int(ev.get("chunk_index", 0))
            snap_hash = clean_text(ev.get("snapshot_hash", ""))
            if not snap_hash:
                snap_hash = "SNAP:" + "|".join([
                    clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", "")),
                    clean_text(ev.get("productos_total", "")),
                    clean_text(ev.get("unidades_total", "")),
                    clean_text(ev.get("chunk_total", "")),
                ])
            key = (lid, snap_hash)
            g = groups.setdefault(key, {
                "lote_id": lid, "snapshot_hash": snap_hash, "chunk_total": total_chunks,
                "chunks": {}, "last_order": order,
                "last_ts": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", "")),
                "productos_total": to_int(ev.get("productos_total", 0)),
                "unidades_total": to_int(ev.get("unidades_total", 0)),
            })
            g["chunk_total"] = max(int(g.get("chunk_total") or 1), int(total_chunks))
            g["chunks"][chunk_index] = ev
            g["last_order"] = max(int(g.get("last_order") or 0), int(order))
            ts = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", ""))
            if ts:
                g["last_ts"] = max(clean_text(g.get("last_ts", "")), ts)

        by_lote = {}
        for g in groups.values():
            expected = int(g.get("chunk_total") or 1)
            if expected <= 0 or len(g.get("chunks", {})) < expected:
                continue
            lid = int(g["lote_id"])
            score = (clean_text(g.get("last_ts", "")), int(g.get("last_order") or 0))
            if lid not in by_lote or score > by_lote[lid]["score"]:
                by_lote[lid] = {"group": g, "score": score}

        selected_items = {}
        selected_meta = {}
        for lid, packed in by_lote.items():
            g = packed["group"]
            imap = {}
            for _, ev in sorted(g["chunks"].items(), key=lambda kv: kv[0]):
                for item_ev in (ev.get("items") or []):
                    item = _snapshot_item_from_event(item_ev, ev)
                    if item:
                        imap[int(item["id"])] = item
            if imap:
                selected_items[int(lid)] = imap
                selected_meta[int(lid)] = {
                    "snapshot_hash": clean_text(g.get("snapshot_hash", "")),
                    "productos_total": int(g.get("productos_total") or len(imap)),
                    "unidades_total": int(g.get("unidades_total") or sum(to_int(x.get("unidades", 0)) for x in imap.values())),
                    "chunk_total": int(g.get("chunk_total") or 1),
                    "last_ts": clean_text(g.get("last_ts", "")),
                }
        return selected_items, selected_meta

    selected_snapshot_items, selected_snapshot_meta = _latest_complete_snapshot_maps(normalized_events)

    lotes = {}
    items_by_lote = {}
    deleted_lotes = set()
    movement_by_item = {}
    scan_rows = []
    incidencias_rows = []
    incidencias_status_updates = {}
    reimpresiones_rows = []
    label_print_events = []
    avisos_rows = {}
    avisos_status_updates = {}
    picking_rows = {}
    picking_status_updates = {}
    lote_status_updates = {}
    # Fallback crítico: en algunos respaldos antiguos o fallidos puede faltar el evento
    # lote_creado, pero sí existen los lote_item con lote_nombre/archivo/hoja en raw_json.
    # Sheets es la fuente real, así que no podemos ignorar un lote solo porque falte
    # el encabezado lote_creado. Lo reconstruimos desde sus items.
    lote_fallbacks = {}

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
                "descripcion": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_kame": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_ml": clean_text(ev.get("descripcion_ml", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_fuente": clean_text(ev.get("descripcion_fuente", "")),
                "familia_kame": clean_text(ev.get("familia_kame", "")),
                "maestro_match_status": clean_text(ev.get("maestro_match_status", "")),
                "origen_item": clean_text(ev.get("origen_item", "PDF_FULL")) or "PDF_FULL",
                "motivo_anexo": clean_text(ev.get("motivo_anexo", "")),
                "usuario_anexo": clean_text(ev.get("usuario_anexo", "")),
                "fecha_anexo": clean_text(ev.get("fecha_anexo", "")),
                "anexo_ml_confirmado": to_int(ev.get("anexo_ml_confirmado", 0)),
                "anexo_ml_confirmado_at": clean_text(ev.get("anexo_ml_confirmado_at", "")),
                "anexo_ml_confirmado_by": clean_text(ev.get("anexo_ml_confirmado_by", "")),
                "anexo_ml_confirmado_comment": clean_text(ev.get("anexo_ml_confirmado_comment", "")),
                "anexo_kame_confirmado": to_int(ev.get("anexo_kame_confirmado", 0)),
                "anexo_kame_confirmado_at": clean_text(ev.get("anexo_kame_confirmado_at", "")),
                "anexo_kame_confirmado_by": clean_text(ev.get("anexo_kame_confirmado_by", "")),
                "anexo_kame_confirmado_comment": clean_text(ev.get("anexo_kame_confirmado_comment", "")),
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
            if lote_id not in lotes:
                lote_fallbacks.setdefault(lote_id, {
                    "id": lote_id,
                    "nombre": clean_text(ev.get("lote_nombre", "")) or f"Lote {lote_id}",
                    "archivo": clean_text(ev.get("archivo", "")),
                    "hoja": clean_text(ev.get("hoja", "")),
                    "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "status": clean_text(ev.get("status", "ACTIVO")) or "ACTIVO",
                    "closed_at": clean_text(ev.get("closed_at", "")),
                    "closed_by": clean_text(ev.get("closed_by", "")),
                    "close_note": clean_text(ev.get("close_note", "")),
                })
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
                    "descripcion": clean_text(item_ev.get("descripcion_kame", "")) or clean_text(item_ev.get("descripcion", "")),
                    "descripcion_kame": clean_text(item_ev.get("descripcion_kame", "")) or clean_text(item_ev.get("descripcion", "")),
                    "descripcion_ml": clean_text(item_ev.get("descripcion_ml", "")) or clean_text(item_ev.get("descripcion", "")),
                    "descripcion_fuente": clean_text(item_ev.get("descripcion_fuente", "")),
                    "familia_kame": clean_text(item_ev.get("familia_kame", "")),
                    "maestro_match_status": clean_text(item_ev.get("maestro_match_status", "")),
                    "origen_item": clean_text(item_ev.get("origen_item", "PDF_FULL")) or "PDF_FULL",
                    "motivo_anexo": clean_text(item_ev.get("motivo_anexo", "")),
                    "usuario_anexo": clean_text(item_ev.get("usuario_anexo", "")),
                    "fecha_anexo": clean_text(item_ev.get("fecha_anexo", "")),
                    "anexo_ml_confirmado": to_int(item_ev.get("anexo_ml_confirmado", 0)),
                    "anexo_ml_confirmado_at": clean_text(item_ev.get("anexo_ml_confirmado_at", "")),
                    "anexo_ml_confirmado_by": clean_text(item_ev.get("anexo_ml_confirmado_by", "")),
                    "anexo_ml_confirmado_comment": clean_text(item_ev.get("anexo_ml_confirmado_comment", "")),
                    "anexo_kame_confirmado": to_int(item_ev.get("anexo_kame_confirmado", 0)),
                    "anexo_kame_confirmado_at": clean_text(item_ev.get("anexo_kame_confirmado_at", "")),
                    "anexo_kame_confirmado_by": clean_text(item_ev.get("anexo_kame_confirmado_by", "")),
                    "anexo_kame_confirmado_comment": clean_text(item_ev.get("anexo_kame_confirmado_comment", "")),
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
            if lote_id not in lotes and items_by_lote.get(lote_id):
                lote_fallbacks.setdefault(lote_id, {
                    "id": lote_id,
                    "nombre": clean_text(ev.get("lote_nombre", "")) or f"Lote {lote_id}",
                    "archivo": clean_text(ev.get("archivo", "")),
                    "hoja": clean_text(ev.get("hoja", "")),
                    "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "status": clean_text(ev.get("status", "ACTIVO")) or "ACTIVO",
                    "closed_at": clean_text(ev.get("closed_at", "")),
                    "closed_by": clean_text(ev.get("closed_by", "")),
                    "close_note": clean_text(ev.get("close_note", "")),
                })
        elif et == "producto_anexado_lote":
            try:
                item_id = int(ev.get("item_id"))
            except Exception:
                item_id = 0
            if item_id:
                items_by_lote.setdefault(lote_id, {})[item_id] = {
                    "id": item_id,
                    "lote_id": lote_id,
                    "area": clean_text(ev.get("area", "ANEXO")) or "ANEXO",
                    "nro": clean_text(ev.get("nro", "")),
                    "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                    "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                    "sku": norm_code(ev.get("sku", "")),
                    "descripcion": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                    "descripcion_kame": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                    "descripcion_ml": clean_text(ev.get("descripcion_ml", "")) or clean_text(ev.get("descripcion", "")),
                    "descripcion_fuente": clean_text(ev.get("descripcion_fuente", "")),
                    "familia_kame": clean_text(ev.get("familia_kame", "")),
                    "maestro_match_status": clean_text(ev.get("maestro_match_status", "")),
                    "origen_item": "ANEXO_MANUAL",
                    "motivo_anexo": clean_text(ev.get("motivo_anexo", ev.get("motivo", ev.get("comentario", "")))),
                    "usuario_anexo": clean_text(ev.get("usuario_anexo", ev.get("usuario", ""))) or "SIN_USUARIO",
                    "fecha_anexo": clean_text(ev.get("fecha_anexo", ev.get("created_at", ev.get("queued_at", "")))),
                    "anexo_ml_confirmado": to_int(ev.get("anexo_ml_confirmado", 0)),
                    "anexo_ml_confirmado_at": clean_text(ev.get("anexo_ml_confirmado_at", "")),
                    "anexo_ml_confirmado_by": clean_text(ev.get("anexo_ml_confirmado_by", "")),
                    "anexo_ml_confirmado_comment": clean_text(ev.get("anexo_ml_confirmado_comment", "")),
                    "anexo_kame_confirmado": to_int(ev.get("anexo_kame_confirmado", 0)),
                    "anexo_kame_confirmado_at": clean_text(ev.get("anexo_kame_confirmado_at", "")),
                    "anexo_kame_confirmado_by": clean_text(ev.get("anexo_kame_confirmado_by", "")),
                    "anexo_kame_confirmado_comment": clean_text(ev.get("anexo_kame_confirmado_comment", "")),
                    "unidades": to_int(ev.get("unidades", ev.get("cantidad", 0))),
                    "acopiadas": 0,
                    "identificacion": clean_text(ev.get("identificacion", "")),
                    "vence": clean_text(ev.get("vence", "")),
                    "instrucciones": clean_text(ev.get("instrucciones", "")),
                    "dia": clean_text(ev.get("dia", "")),
                    "hora": clean_text(ev.get("hora", "")),
                    "created_at": clean_text(ev.get("item_created_at", "")) or clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "updated_at": clean_text(ev.get("item_updated_at", "")) or clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                }
                if lote_id not in lotes:
                    lote_fallbacks.setdefault(lote_id, {
                        "id": lote_id,
                        "nombre": clean_text(ev.get("lote_nombre", "")) or f"Lote {lote_id}",
                        "archivo": clean_text(ev.get("archivo", "")),
                        "hoja": clean_text(ev.get("hoja", "")),
                        "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                        "status": clean_text(ev.get("status", "ACTIVO")) or "ACTIVO",
                        "closed_at": "", "closed_by": "", "close_note": "",
                    })
        elif et == "scan_agregado":
            try:
                item_id = int(ev.get("item_id"))
                qty = int(ev.get("cantidad") or 0)
            except Exception:
                continue
            movement_by_item[item_id] = movement_by_item.get(item_id, 0) + qty
            scan_rows.append({
                "lote_id": lote_id,
                "original_item_id": item_id,
                "item_id": item_id,
                "scan_primario": norm_code(ev.get("scan_primario", "")),
                "scan_secundario": norm_code(ev.get("scan_secundario", "")),
                "cantidad": qty,
                "modo": clean_text(ev.get("modo", "")),
                "created_at": clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                "operador_validador": clean_text(ev.get("operador_validador", "")) or "SIN_USUARIO",
                "picking_list_id": to_int(ev.get("picking_list_id", 0)) or None,
                "picking_code": clean_text(ev.get("picking_code", "")),
                "picker_asignado": clean_text(ev.get("picker_asignado", "")),
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_kame": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_ml": clean_text(ev.get("descripcion_ml", "")) or clean_text(ev.get("descripcion", "")),
                "familia_kame": clean_text(ev.get("familia_kame", "")),
                "maestro_match_status": clean_text(ev.get("maestro_match_status", "")),
                "restore_match_status": "PENDING",
            })
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
                "id": to_int(ev.get("incidencia_id", 0)) or None,
                "lote_id": lote_id,
                "item_id": item_id,
                "tipo": clean_text(ev.get("tipo", "")) or "Otro",
                "cantidad": max(0, to_int(ev.get("cantidad", 0))),
                "comentario": clean_text(ev.get("comentario", "")),
                "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "status": clean_text(ev.get("status", "ABIERTA")) or clean_text(ev.get("estado", "ABIERTA")) or "ABIERTA",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion", "")),
            })
        elif et == "incidencia_resuelta" or et == "INCIDENCIA_RESUELTA":
            inc_id = to_int(ev.get("incidencia_id", 0))
            if inc_id:
                incidencias_status_updates[inc_id] = {
                    "status": "RESUELTA",
                    "resolved_at": clean_text(ev.get("resolved_at", "")) or clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "resolved_by": clean_text(ev.get("resolved_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                    "resolution_comment": clean_text(ev.get("resolution_comment", "")) or clean_text(ev.get("comentario", "")),
                }
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
        elif et == "zpl_etiquetas_generado" or et == "ZPL_ETIQUETAS_GENERADO":
            label_print_events.append({
                "lote_id": lote_id,
                "print_scope": clean_text(ev.get("print_scope", "")).upper(),
                "print_kind": clean_text(ev.get("print_kind", "NORMAL")).upper() or "NORMAL",
                "block_index": to_int(ev.get("block_index", 0)) or None,
                "block_key": clean_text(ev.get("block_key", "")),
                "picking_list_id": to_int(ev.get("picking_list_id", 0)) or None,
                "picking_code": clean_text(ev.get("picking_code", "")),
                "asignado_a": clean_text(ev.get("asignado_a", "")),
                "item_id": to_int(ev.get("item_id", 0)) or None,
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion", "")),
                "productos_count": to_int(ev.get("productos_count", 0)),
                "cantidad_normal": to_int(ev.get("cantidad_normal", ev.get("normal_qty", 0))),
                "cantidad_separadores": to_int(ev.get("cantidad_separadores", ev.get("separator_qty", 0))),
                "cantidad_total": to_int(ev.get("cantidad_total", ev.get("total_qty", 0))),
                "archivo_nombre": clean_text(ev.get("archivo_nombre", "")),
                "zpl_hash": clean_text(ev.get("zpl_hash", "")),
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

    # Si existe un snapshot completo, ese snapshot manda como base del lote.
    # Esto evita mezclar snapshots históricos con cantidades diferentes después
    # de reinicios/rescates. Los anexos manuales que no estén dentro del snapshot
    # se conservan por trazabilidad.
    for lid, snap_items in selected_snapshot_items.items():
        if not snap_items:
            continue
        manual_items = {}
        for iid, it in (items_by_lote.get(lid, {}) or {}).items():
            if int(iid) not in snap_items and clean_text(it.get("origen_item", "")).upper() == "ANEXO_MANUAL":
                manual_items[int(iid)] = it
        items_by_lote[lid] = dict(snap_items)
        items_by_lote[lid].update(manual_items)

    # Reconciliaciones oficiales PDF se aplican aun cuando el snapshot seleccionado
    # sea anterior; son idempotentes y preservan la corrección en futuros rescates.
    reconciliation_events_by_lote = {}
    for ev in normalized_events:
        if clean_text(ev.get("event_type", "")).lower() == PDF_RECONCILIATION_EVENT:
            try:
                lid = int(ev.get("lote_id"))
            except Exception:
                continue
            reconciliation_events_by_lote.setdefault(lid, []).append(ev)
    for lid, rec_events in reconciliation_events_by_lote.items():
        if lid in items_by_lote:
            apply_pdf_reconciliation_events_to_item_map(items_by_lote[lid], rec_events, lid, set(items_by_lote[lid].keys()))

    # Si existen items de un lote pero falta lote_creado, recuperamos el lote igual.
    # Esto soluciona respaldos donde el snapshot de productos llegó a Sheets, pero
    # el evento encabezado no quedó registrado o no fue exportado.
    for lid, lote_info in lote_fallbacks.items():
        if lid not in lotes and items_by_lote.get(lid):
            lotes[lid] = lote_info

    active_lote_ids = [lid for lid in lotes if lid not in deleted_lotes and items_by_lote.get(lid)]
    if only_missing:
        active_lote_ids = [lid for lid in active_lote_ids if lid not in existing_local_ids]
    if only_lote_id is not None:
        try:
            target_lote_id = int(only_lote_id)
            active_lote_ids = [lid for lid in active_lote_ids if int(lid) == target_lote_id]
        except Exception:
            active_lote_ids = []
    if not active_lote_ids:
        if only_lote_id is not None:
            return False, f"No encontré el lote seleccionado {only_lote_id} con snapshot completo en Sheets. Revisa que tenga lote_item o lote_snapshot_chunk."
        if only_missing:
            return False, "No encontré lotes nuevos en Sheets para sincronizar. Si el lote existe en eventos pero no aparece, revisa que tenga lote_item o lote_snapshot_chunk."
        return False, "No encontré lotes activos con snapshot completo en Sheets. Crea el lote una vez con esta nueva versión para activar restauración automática."

    now = now_cl().isoformat(timespec="seconds")
    restored_lotes = 0
    restored_items = 0
    restored_scans = 0
    restored_incidencias = 0
    restored_reimpresiones = 0
    restored_label_prints = 0
    restored_avisos = 0
    restored_picking = 0

    # Reconciliación segura de scans.
    # Regla: nunca buscar en otros FULL. Solo se resuelve contra items del lote seleccionado.
    # Si no hay match seguro, el scan igual se conserva con snapshot propio para auditoría/visor,
    # pero no se fuerza una asociación falsa a un producto.
    def _build_item_lookups(items_map):
        lookups = {"id": {}, "codigo_ml": {}, "codigo_universal": {}, "sku": {}}
        for _iid, _it in (items_map or {}).items():
            try:
                lookups["id"][int(_iid)] = int(_iid)
            except Exception:
                pass
            for _field in ["codigo_ml", "codigo_universal", "sku"]:
                _code = norm_code(_it.get(_field, ""))
                if _code:
                    lookups[_field].setdefault(_code, set()).add(int(_iid))
        return lookups

    item_lookups_by_lote = {int(lid): _build_item_lookups(items_by_lote.get(lid, {})) for lid in active_lote_ids}
    movement_by_item = {}
    resolved_scan_rows = []
    unmatched_scan_count = 0
    ambiguous_scan_count = 0
    for sr in scan_rows:
        try:
            lid = int(sr.get("lote_id"))
        except Exception:
            continue
        if lid not in active_lote_ids:
            continue
        lookups = item_lookups_by_lote.get(lid, {})
        original_item_id = to_int(sr.get("original_item_id", 0))
        resolved_id = None
        status = "ACOPIO_RECUPERADO_SHEETS"
        resolved_id, status = resolve_restore_item_identity(items_by_lote.get(lid, {}), sr, original_item_id)
        if status == "AMBIGUOUS_SAME_LOTE":
            ambiguous_scan_count += 1
        if resolved_id is None:
            # Conservamos el id histórico para traza, pero el visor usará el snapshot guardado en scans.
            resolved_id = original_item_id or 0
            unmatched_scan_count += 1
        else:
            movement_by_item[resolved_id] = movement_by_item.get(resolved_id, 0) + int(sr.get("cantidad") or 0)
        sr["item_id"] = resolved_id
        sr["restore_match_status"] = status
        resolved_scan_rows.append(sr)

    with db() as c:
        if replace_existing:
            for lid in sorted(active_lote_ids):
                c.execute("DELETE FROM scans WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM incidencias WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM reimpresiones WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM avisos_operacionales WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM picking_list_items WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM picking_lists WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM label_prints WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM label_blocks WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM reservas_kame WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM audit_events WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM items WHERE lote_id=?", (lid,))
                c.execute("DELETE FROM lotes WHERE id=?", (lid,))
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
                    (id, lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml,
                     descripcion_fuente, familia_kame, maestro_match_status, origen_item, motivo_anexo, usuario_anexo, fecha_anexo,
                     anexo_ml_confirmado, anexo_ml_confirmado_at, anexo_ml_confirmado_by, anexo_ml_confirmado_comment,
                     anexo_kame_confirmado, anexo_kame_confirmado_at, anexo_kame_confirmado_by, anexo_kame_confirmado_comment,
                     unidades, acopiadas, identificacion, vence, instrucciones, dia, hora, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (item["id"], item["lote_id"], item["area"], item["nro"], item["codigo_ml"], item["codigo_universal"], item["sku"],
                     item["descripcion"], item.get("descripcion_kame", item["descripcion"]), item.get("descripcion_ml", item["descripcion"]),
                     item.get("descripcion_fuente", ""), item.get("familia_kame", ""), item.get("maestro_match_status", ""),
                     item.get("origen_item", "PDF_FULL"), item.get("motivo_anexo", ""), item.get("usuario_anexo", ""), item.get("fecha_anexo", ""),
                     to_int(item.get("anexo_ml_confirmado", 0)), item.get("anexo_ml_confirmado_at", ""), item.get("anexo_ml_confirmado_by", ""), item.get("anexo_ml_confirmado_comment", ""),
                     to_int(item.get("anexo_kame_confirmado", 0)), item.get("anexo_kame_confirmado_at", ""), item.get("anexo_kame_confirmado_by", ""), item.get("anexo_kame_confirmado_comment", ""),
                     item["unidades"], item["acopiadas"], item["identificacion"], item["vence"], item.get("instrucciones", ""), item["dia"], item["hora"], item["created_at"], item["updated_at"]),
                )
                restored_items += 1
        for sr in resolved_scan_rows:
            lote_id = int(sr.get("lote_id") or 0)
            cantidad = int(sr.get("cantidad") or 0)
            if lote_id in active_lote_ids and cantidad > 0:
                c.execute(
                    """
                    INSERT INTO scans
                    (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at,
                     operador_validador, picking_list_id, picking_code, picker_asignado,
                     original_item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, maestro_match_status, restore_match_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        lote_id, int(sr.get("item_id") or 0), norm_code(sr.get("scan_primario", "")), norm_code(sr.get("scan_secundario", "")),
                        cantidad, clean_text(sr.get("modo", "")), clean_text(sr.get("created_at", "")),
                        clean_text(sr.get("operador_validador", "")) or "SIN_USUARIO",
                        sr.get("picking_list_id"), clean_text(sr.get("picking_code", "")), clean_text(sr.get("picker_asignado", "")),
                        to_int(sr.get("original_item_id", 0)) or None,
                        norm_code(sr.get("codigo_ml", "")), norm_code(sr.get("codigo_universal", "")), norm_code(sr.get("sku", "")),
                        clean_text(sr.get("descripcion", "")), clean_text(sr.get("descripcion_kame", sr.get("descripcion", ""))),
                        clean_text(sr.get("descripcion_ml", sr.get("descripcion", ""))), clean_text(sr.get("familia_kame", "")),
                        clean_text(sr.get("maestro_match_status", "")), clean_text(sr.get("restore_match_status", "")),
                    ),
                )
                restored_scans += 1
        for inc in incidencias_rows:
            if inc["lote_id"] in active_lote_ids:
                if inc.get("id"):
                    c.execute(
                        """
                        INSERT OR REPLACE INTO incidencias
                        (id, lote_id, item_id, tipo, cantidad, comentario, usuario, status, created_at,
                         codigo_ml, codigo_universal, sku, descripcion)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (inc["id"], inc["lote_id"], inc["item_id"], inc["tipo"], inc["cantidad"], inc["comentario"], inc["usuario"], inc["status"], inc["created_at"], inc["codigo_ml"], inc["codigo_universal"], inc["sku"], inc["descripcion"]),
                    )
                else:
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
        for inc_id, upd in incidencias_status_updates.items():
            c.execute(
                """
                UPDATE incidencias
                SET status=?, resolved_at=?, resolved_by=?, resolution_comment=?
                WHERE id=?
                """,
                (upd["status"], upd["resolved_at"], upd["resolved_by"], upd["resolution_comment"], int(inc_id)),
            )
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
                    item_id, _ = resolve_restore_item_identity(items_by_lote.get(int(plist["lote_id"]), {}), pit, to_int(pit.get("item_id", 0)))
                    if not item_id:
                        continue
                    c.execute(
                        """
                        INSERT INTO picking_list_items
                        (picking_list_id, lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, maestro_match_status,
                         cantidad, area, nro, estado, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDIENTE', ?)
                        """,
                        (list_id_db, plist["lote_id"], item_id, norm_code(pit.get("codigo_ml", "")), norm_code(pit.get("codigo_universal", "")), norm_code(pit.get("sku", "")),
                         clean_text(pit.get("descripcion_kame", "")) or clean_text(pit.get("descripcion", "")),
                         clean_text(pit.get("descripcion_kame", "")) or clean_text(pit.get("descripcion", "")),
                         clean_text(pit.get("descripcion_ml", "")) or clean_text(pit.get("descripcion", "")),
                         clean_text(pit.get("familia_kame", "")), clean_text(pit.get("maestro_match_status", "")),
                         to_int(pit.get("cantidad", 0)), clean_text(pit.get("area", "")), clean_text(pit.get("nro", "")), plist["created_at"]),
                    )
                restored_picking += 1

        # Después de reconstruir scans y listas, ajusta solo excesos de cantidad
        # contra el objetivo oficial restaurado/reconciliado.
        for _lid in sorted(active_lote_ids):
            _notes = reconcile_all_active_picking_quantities(c, int(_lid))
            for _note in _notes:
                c.execute(
                    "INSERT INTO audit_events (lote_id, event_type, detail, created_at) VALUES (?, ?, ?, ?)",
                    (int(_lid), "RECONCILIACION_PICKING_RESTAURACION", _note, now),
                )

        # Restaura estado local de etiquetas desde eventos resumen zpl_etiquetas_generado.
        # No guarda producto por producto en Sheets, pero reconstruye label_prints/label_blocks
        # usando los productos/listas ya restaurados del mismo lote.
        for evp in label_print_events:
            lid = int(evp.get("lote_id") or 0)
            if lid not in active_lote_ids:
                continue
            scope = clean_text(evp.get("print_scope", "")).upper()
            kind = clean_text(evp.get("print_kind", "NORMAL")).upper() or "NORMAL"
            is_reprint = 1 if kind == "REIMPRESION" else 0
            created_at = clean_text(evp.get("created_at", "")) or now
            try:
                if scope == "BLOQUE":
                    block_index = to_int(evp.get("block_index", 0))
                    block_key = clean_text(evp.get("block_key", ""))
                    if not block_index or not block_key:
                        continue
                    # Reconstrucción tolerante: primero por block_key, luego por block_index.
                    # Esto evita falsos pendientes visuales si el block_key cambia después de restaurar.
                    labels_view_restore = label_control_view(lid)
                    blocks_restore = build_label_blocks(labels_view_restore, ROLL_CAPACITY_DEFAULT) if not labels_view_restore.empty else []
                    block = find_restored_label_block(blocks_restore, block_index, block_key)
                    restored_block_key = clean_text(block.get("block_key", "")) if block else block_key
                    if not block:
                        normal_qty = to_int(evp.get("cantidad_normal", evp.get("normal_qty", 0)))
                        sep_qty = to_int(evp.get("cantidad_separadores", evp.get("separator_qty", 0)))
                        total_qty = to_int(evp.get("cantidad_total", evp.get("total_qty", normal_qty + sep_qty)))
                        products_count = to_int(evp.get("productos_count", 0))
                        if normal_qty <= 0 and total_qty <= 0:
                            continue
                        c.execute(
                            """
                            INSERT OR REPLACE INTO label_blocks
                            (lote_id, block_index, block_key, products_count, normal_qty, separator_qty, total_qty,
                             status, download_count, last_printed_at, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                            """,
                            (lid, block_index, restored_block_key, products_count, normal_qty, sep_qty, total_qty,
                             "REIMPRESO" if is_reprint else "IMPRESO", created_at, created_at, created_at),
                        )
                        restored_label_prints += 1
                        continue
                    c.execute(
                        """
                        INSERT OR REPLACE INTO label_blocks
                        (lote_id, block_index, block_key, products_count, normal_qty, separator_qty, total_qty,
                         status, download_count, last_printed_at, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                        """,
                        (lid, block_index, restored_block_key, int(block.get("products_count", 0)), int(block.get("normal_qty", 0)),
                         int(block.get("separator_qty", 0)), int(block.get("total_qty", 0)), "REIMPRESO" if is_reprint else "IMPRESO",
                         created_at, created_at, created_at),
                    )
                    for item in block.get("items", []):
                        c.execute(
                            """
                            INSERT INTO label_prints
                            (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
                             block_index, block_key, is_reprint, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, 'BLOQUE', 'NORMAL', ?, ?, ?, ?)
                            """,
                            (lid, int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                             descripcion_etiqueta_value(item), int(item.get("unidades", 0)), block_index, block_key, is_reprint, created_at),
                        )
                        c.execute(
                            """
                            INSERT INTO label_prints
                            (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
                             block_index, block_key, is_reprint, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, 'BLOQUE', 'SEPARADOR', ?, ?, ?, ?)
                            """,
                            (lid, int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                             descripcion_etiqueta_value(item), LABEL_SEPARATOR_PER_PRODUCT, block_index, block_key, is_reprint, created_at),
                        )
                    restored_label_prints += 1
                elif scope == "PICKING":
                    picking_id = to_int(evp.get("picking_list_id", 0)) or to_int(evp.get("block_index", 0))
                    block_key = clean_text(evp.get("block_key", ""))
                    if not picking_id:
                        continue
                    items_df = pd.read_sql_query(
                        "SELECT * FROM picking_list_items WHERE picking_list_id=? ORDER BY id", c, params=(int(picking_id),)
                    )
                    if items_df.empty:
                        continue
                    for _, item in items_df.iterrows():
                        c.execute(
                            """
                            INSERT INTO label_prints
                            (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
                             block_index, block_key, is_reprint, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, 'PICKING', 'NORMAL', ?, ?, ?, ?)
                            """,
                            (lid, int(item.get("item_id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                             descripcion_etiqueta_value(item), to_int(item.get("cantidad", 0)), picking_id, block_key, is_reprint, created_at),
                        )
                        c.execute(
                            """
                            INSERT INTO label_prints
                            (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
                             block_index, block_key, is_reprint, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, 'PICKING', 'SEPARADOR', ?, ?, ?, ?)
                            """,
                            (lid, int(item.get("item_id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                             descripcion_etiqueta_value(item), LABEL_SEPARATOR_PER_PRODUCT, picking_id, block_key, is_reprint, created_at),
                        )
                    restored_label_prints += 1
                elif scope == "INDIVIDUAL":
                    item_id = to_int(evp.get("item_id", 0))
                    qty = max(1, to_int(evp.get("cantidad_normal", 1)))
                    if not item_id:
                        continue
                    row_item = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (item_id, lid)).fetchone()
                    item = dict(row_item) if row_item else {}
                    c.execute(
                        """
                        INSERT INTO label_prints
                        (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
                         block_index, block_key, is_reprint, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, 'INDIVIDUAL', 'NORMAL', NULL, NULL, ?, ?)
                        """,
                        (lid, item_id, norm_code(item.get("codigo_ml", evp.get("codigo_ml", ""))), norm_code(item.get("sku", evp.get("sku", ""))),
                         descripcion_etiqueta_value(item) or clean_text(evp.get("descripcion", "")), qty, is_reprint, created_at),
                    )
                    c.execute(
                        """
                        INSERT INTO label_prints
                        (lote_id, item_id, codigo_ml, sku, descripcion, cantidad, print_scope, print_kind,
                         block_index, block_key, is_reprint, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, 'INDIVIDUAL', 'SEPARADOR', NULL, NULL, ?, ?)
                        """,
                        (lid, item_id, norm_code(item.get("codigo_ml", evp.get("codigo_ml", ""))), norm_code(item.get("sku", evp.get("sku", ""))),
                         descripcion_etiqueta_value(item) or clean_text(evp.get("descripcion", "")), LABEL_SEPARATOR_PER_PRODUCT, is_reprint, created_at),
                    )
                    restored_label_prints += 1
            except Exception:
                # El evento queda en Sheets aunque no se pueda reconstruir localmente por cambios de snapshot.
                # No bloqueamos el rescate completo del lote.
                continue
        c.commit()

    extra = ""
    if unmatched_scan_count or ambiguous_scan_count:
        extra = f" Atención: {unmatched_scan_count} acopio(s) recuperado(s) desde Sheets y {ambiguous_scan_count} ambiguo(s). Se conservan como scans válidos del mismo lote para trazabilidad."
    if selected_snapshot_meta:
        snap_notes = []
        for lid in sorted(set(active_lote_ids).intersection(selected_snapshot_meta.keys())):
            m = selected_snapshot_meta.get(lid, {})
            snap_notes.append(f"lote {lid}: {m.get('productos_total', 0)} producto(s) / {m.get('unidades_total', 0)} unidad(es) / {m.get('chunk_total', 0)} chunk(s)")
        if snap_notes:
            extra += " Snapshot base usado: " + "; ".join(snap_notes) + "."
    return True, f"Restauración completa: {restored_lotes} lote(s), {restored_items} producto(s), {restored_scans} escaneo(s), {restored_incidencias} incidencia(s), {restored_reimpresiones} reimpresión(es), {restored_label_prints} evento(s) de etiquetas, {restored_avisos} aviso(s) operacional(es), {restored_picking} lista(s) picking.{extra}"


def is_ambiguous_timeout_error(message: str) -> bool:
    """Timeout de Apps Script: puede haber escrito en Sheets aunque no respondió.

    No debe contarse como falla definitiva de respaldo, porque eso infla eventos
    fallidos aun cuando el evento ya llegó a Sheets.
    """
    msg = clean_text(message).lower()
    return (
        ("timeout esperando respuesta" in msg)
        or ("timed out" in msg)
        or ("timeouterror" in msg)
        or ("lock timeout" in msg)
        or ("holding the lock" in msg)
        or ("another process was holding the lock" in msg)
        or ("lock_busy" in msg)
        or ("apps script ocupado" in msg)
        or ("script ocupado" in msg)
        or ("service invoked too many times" in msg)
    )




def is_transient_backup_error(message: str) -> bool:
    """Errores temporales de Sheets/App Script que no deben inflar fallidos."""
    return is_ambiguous_timeout_error(message)


def parse_iso_dt_safe(v):
    s = clean_text(v)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def should_skip_recent_timeout(row, cooldown_seconds: int = 90) -> bool:
    """Evita reintentar el mismo timeout en cada rerun de Streamlit."""
    last_error = clean_text(row["last_error"] if "last_error" in row.keys() else "")
    if not is_ambiguous_timeout_error(last_error):
        return False
    last_attempt = parse_iso_dt_safe(row["last_attempt_at"] if "last_attempt_at" in row.keys() else "")
    if not last_attempt:
        return False
    return (now_cl() - last_attempt).total_seconds() < int(cooldown_seconds)


def flush_backup_queue(webhook_url: str | None = None, limit: int = 25, include_failed: bool = False):
    """Envía eventos pendientes a Google Sheets sin castigar timeouts ambiguos.

    Si Apps Script demora más que el timeout, puede que Sheets sí haya escrito el
    evento. En ese caso no lo mandamos directo a failed; queda pendiente para
    reconciliar/reintentar con calma, evitando que el contador de fallidos suba
    artificialmente durante la operación.
    """
    url = clean_text(webhook_url or get_backup_webhook_url())
    if not url:
        return

    statuses = ("'pending','failed'" if include_failed else "'pending'")
    fetch_limit = max(int(limit) * 3, int(limit))
    with db() as c:
        rows = c.execute(
            f"""
            SELECT id, event_type, payload_json, attempts, created_at, status, last_error, last_attempt_at
            FROM backup_queue
            WHERE status IN ({statuses})
            ORDER BY id ASC
            LIMIT ?
            """,
            (fetch_limit,),
        ).fetchall()

    processed = 0
    for row in rows:
        if processed >= int(limit):
            break
        if not include_failed and should_skip_recent_timeout(row, cooldown_seconds=90):
            continue

        event = {
            "event_type": row["event_type"],
            "queue_id": int(row["id"]),
            "queued_at": row["created_at"],
            **json.loads(row["payload_json"]),
        }

        attempt_at = now_cl().isoformat(timespec="seconds")
        with db() as c:
            c.execute("UPDATE backup_queue SET last_attempt_at=? WHERE id=?", (attempt_at, int(row["id"])))
            c.commit()

        processed += 1
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
            detail = str(e)[:500]
            if is_transient_backup_error(detail):
                # Apps Script está ocupado o respondió tarde. No cuenta como falla definitiva.
                # Cortamos este ciclo para no golpear el candado de Apps Script en cascada.
                with db() as c:
                    c.execute(
                        """
                        UPDATE backup_queue
                        SET status='pending', last_error=?
                        WHERE id=?
                        """,
                        (detail, int(row["id"])),
                    )
                    c.commit()
                break

            attempts_next = int(row["attempts"] or 0) + 1
            new_status = "failed" if attempts_next >= MAX_BACKUP_ATTEMPTS else "pending"
            with db() as c:
                c.execute(
                    """
                    UPDATE backup_queue
                    SET attempts=?, status=?, last_error=?
                    WHERE id=?
                    """,
                    (attempts_next, new_status, detail, int(row["id"])),
                )
                c.commit()


def flush_backup_queue_ids(ids, webhook_url: str | None = None, include_failed: bool = True):
    """Envía solo los eventos indicados a Google Sheets.

    Mantiene la misma regla: timeout de Apps Script no cuenta como falla definitiva,
    porque puede haber escrito en Sheets y no alcanzó a responder.
    """
    ids = [int(x) for x in (ids or []) if str(x).strip()]
    if not ids:
        return
    url = clean_text(webhook_url or get_backup_webhook_url())
    if not url:
        return
    statuses = ("'pending','failed'" if include_failed else "'pending'")
    qmarks = ",".join("?" for _ in ids)
    with db() as c:
        rows = c.execute(
            f"""
            SELECT id, event_type, payload_json, attempts, created_at, status, last_error, last_attempt_at
            FROM backup_queue
            WHERE id IN ({qmarks}) AND status IN ({statuses})
            ORDER BY id ASC
            """,
            ids,
        ).fetchall()

    for row in rows:
        event = {
            "event_type": row["event_type"],
            "queue_id": int(row["id"]),
            "queued_at": row["created_at"],
            **json.loads(row["payload_json"]),
        }
        attempt_at = now_cl().isoformat(timespec="seconds")
        with db() as c:
            c.execute("UPDATE backup_queue SET last_attempt_at=? WHERE id=?", (attempt_at, int(row["id"])))
            c.commit()
        try:
            ok, detail = send_webhook_event(url, event)
            if not ok:
                raise RuntimeError(detail)
            sent_at = now_cl().isoformat(timespec="seconds")
            with db() as c:
                c.execute("UPDATE backup_queue SET status='sent', sent_at=?, last_error=NULL WHERE id=?", (sent_at, int(row["id"])))
                c.commit()
        except Exception as e:
            detail = str(e)[:500]
            if is_transient_backup_error(detail):
                with db() as c:
                    c.execute("UPDATE backup_queue SET status='pending', last_error=? WHERE id=?", (detail, int(row["id"])))
                    c.commit()
                break
            attempts_next = int(row["attempts"] or 0) + 1
            new_status = "failed" if attempts_next >= MAX_BACKUP_ATTEMPTS else "pending"
            with db() as c:
                c.execute(
                    "UPDATE backup_queue SET attempts=?, status=?, last_error=? WHERE id=?",
                    (attempts_next, new_status, detail, int(row["id"])),
                )
                c.commit()


def reconcile_backup_queue_from_sheets(limit: int = 5000) -> int:
    """Marca como enviados eventos que ya están en Sheets aunque Python haya recibido timeout."""
    ok, events, _msg = get_backup_events_from_sheets()
    if not ok or not events:
        return 0

    seen_uids = set()
    seen_keys = set()
    for ev in events:
        base = dict(ev or {})
        raw = base.get("raw_json")
        if raw:
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(parsed, dict):
                    base.update(parsed)
            except Exception:
                pass
        uid = clean_text(base.get("event_uid", ""))
        if uid:
            seen_uids.add(uid)
        key = clean_text(base.get("event_key", ""))
        if key:
            seen_keys.add(key)

    if not seen_uids and not seen_keys:
        return 0

    marked = 0
    with db() as c:
        rows = c.execute(
            """
            SELECT id, event_type, payload_json, created_at
            FROM backup_queue
            WHERE status <> 'sent'
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
            uid = clean_text(payload.get("event_uid", ""))
            queued_event = {
                "event_type": row["event_type"],
                "queue_id": int(row["id"]),
                "queued_at": row["created_at"],
                **payload,
            }
            key = sheet_event_semantic_identity(queued_event)
            if (uid and uid in seen_uids) or (key and key in seen_keys):
                c.execute(
                    "UPDATE backup_queue SET status='sent', sent_at=?, last_error=NULL WHERE id=?",
                    (now_cl().isoformat(timespec="seconds"), int(row["id"])),
                )
                marked += 1
        c.commit()
    return marked




def _backup_sync_worker(limit: int = 50):
    """Worker liviano para drenar backup_queue sin bloquear la operación."""
    global _BACKUP_SYNC_RUNNING
    try:
        flush_backup_queue(limit=int(limit), include_failed=False)
    except Exception:
        # El error queda guardado en backup_queue.last_error; no botamos la app.
        pass
    finally:
        with _BACKUP_SYNC_LOCK:
            _BACKUP_SYNC_RUNNING = False


def trigger_backup_sync_async(limit: int = 50):
    """Lanza sincronización Sheets en segundo plano sin bloquear al operador.

    Streamlit no es un worker permanente, pero este hilo permite que el scan
    quede guardado en SQLite y la cola intente subir a Sheets sin frenar PDA.
    """
    global _BACKUP_SYNC_RUNNING
    if not clean_text(get_backup_webhook_url()):
        return False
    with _BACKUP_SYNC_LOCK:
        if _BACKUP_SYNC_RUNNING:
            return False
        _BACKUP_SYNC_RUNNING = True
    try:
        t = threading.Thread(target=_backup_sync_worker, args=(int(limit),), daemon=True)
        t.start()
        return True
    except Exception:
        with _BACKUP_SYNC_LOCK:
            _BACKUP_SYNC_RUNNING = False
        return False


def retry_failed_backups(limit: int = 1000):
    """Reintenta eventos fallidos sin inflar el contador.

    Primero concilia contra Sheets: muchos 'fallidos' son timeouts donde Apps Script
    sí escribió el evento pero no respondió a tiempo.
    """
    reconcile_backup_queue_from_sheets(limit=max(int(limit), 1000))
    with db() as c:
        c.execute(
            """
            UPDATE backup_queue
            SET status='pending', attempts=0
            WHERE status='failed'
            """
        )
        c.commit()
    # Reintento gradual: si Apps Script está ocupado, flush corta al primer LOCK_BUSY/timeout.
    flush_backup_queue(limit=min(int(limit), 100), include_failed=True)


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
    created_at = now_cl().isoformat(timespec="seconds")
    event = {
        "event_type": "test_webhook",
        "created_at": created_at,
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
    event = attach_event_identity("test_webhook", event, created_at)
    return send_webhook_event(url, event, timeout=25)


def make_backup_lote_key(nombre: str, archivo: str, hoja: str, created_at: str) -> str:
    """Identidad persistente del FULL para eventos nuevos.

    El id SQLite del lote puede reiniciarse después de una caída o rescate. Esta llave
    deja una huella propia en Sheets y evita que futuros eventos dependan del id local.
    """
    raw = "|".join([
        clean_text(nombre),
        clean_text(archivo),
        clean_text(hoja),
        clean_text(created_at),
    ])
    return "LKEY-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def ensure_lote_backup_key(lote_id: int, lote: dict | None = None) -> str:
    """Obtiene o crea la identidad persistente de un lote local."""
    lote = dict(lote or get_lote(lote_id) or {})
    current = clean_text(lote.get("backup_lote_key", ""))
    if current:
        return current
    key = make_backup_lote_key(
        clean_text(lote.get("nombre", "")),
        clean_text(lote.get("archivo", "")),
        clean_text(lote.get("hoja", "")),
        clean_text(lote.get("created_at", "")),
    )
    with db() as c:
        c.execute("UPDATE lotes SET backup_lote_key=? WHERE id=?", (key, int(lote_id)))
        c.commit()
    return key


def build_lote_payload(lote_id: int) -> dict:
    lote = get_lote(lote_id)
    return {
        "lote_id": lote_id,
        "lote_nombre": clean_text(lote.get("nombre", "")),
        "archivo": clean_text(lote.get("archivo", "")),
        "hoja": clean_text(lote.get("hoja", "")),
        "lote_created_at": clean_text(lote.get("created_at", "")),
        "backup_lote_key": ensure_lote_backup_key(lote_id, lote),
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




# ============================================================
# Integridad de lote: reconciliación contra PDF corregido ML
# ============================================================
PDF_RECONCILIATION_EVENT = "lote_pdf_reconciliado"


def _item_identity_maps(items_map: dict[int, dict]) -> dict[str, dict[str, set[int]]]:
    """Índices por código para restauración segura.

    Los item_id locales pueden cambiar entre snapshots/restauraciones. Por eso
    los códigos de negocio (ML/EAN/SKU) tienen prioridad sobre un item_id
    histórico cuando ambos discrepan.
    """
    out = {"codigo_ml": {}, "codigo_universal": {}, "sku": {}}
    for iid, it in (items_map or {}).items():
        try:
            iid_int = int(iid)
        except Exception:
            continue
        for field in out:
            code = norm_code((it or {}).get(field, ""))
            if code and not (field == "codigo_universal" and code == "N/A"):
                out[field].setdefault(code, set()).add(iid_int)
    return out


def resolve_restore_item_identity(items_map: dict[int, dict], raw: dict, original_item_id=None) -> tuple[int | None, str]:
    """Resuelve un item del mismo lote dando prioridad a ML/EAN/SKU.

    Esto evita que un item_id viejo de una lista picking apunte a otro producto
    después de una restauración. Solo se cae al id histórico cuando no hay
    códigos útiles para validar la identidad.
    """
    raw = raw or {}
    maps = _item_identity_maps(items_map)
    supplied = []
    for field in ("codigo_ml", "codigo_universal", "sku"):
        code = norm_code(raw.get(field, ""))
        if code and not (field == "codigo_universal" and code == "N/A"):
            supplied.append((field, code))

    # Código ML es el identificador más fuerte dentro del FULL.
    for field in ("codigo_ml", "sku", "codigo_universal"):
        code = norm_code(raw.get(field, ""))
        if not code or (field == "codigo_universal" and code == "N/A"):
            continue
        ids = maps.get(field, {}).get(code, set())
        if len(ids) == 1:
            return int(next(iter(ids))), "MATCH_CODE_SAME_LOTE"
        if len(ids) > 1:
            return None, "AMBIGUOUS_SAME_LOTE"

    try:
        iid = int(original_item_id if original_item_id is not None else raw.get("item_id", 0))
    except Exception:
        iid = 0
    if iid and iid in (items_map or {}):
        # Sin código verificable, conservar id histórico como último recurso.
        if not supplied:
            return iid, "MATCH_ITEM_ID"
        # Hay códigos pero no encontraron otro candidato: no se inventa un match;
        # el id queda como fallback trazable.
        return iid, "MATCH_ITEM_ID_FALLBACK"
    return None, "UNMATCHED_SAME_LOTE"


def _new_reconciliation_item(raw: dict, lote_id: int, item_id: int) -> dict:
    raw = raw or {}
    now = clean_text(raw.get("created_at", "")) or now_cl().isoformat(timespec="seconds")
    desc = clean_text(raw.get("descripcion_kame", "")) or clean_text(raw.get("descripcion", ""))
    return {
        "id": int(item_id), "lote_id": int(lote_id),
        "area": clean_text(raw.get("area", "")), "nro": clean_text(raw.get("nro", "")),
        "codigo_ml": norm_code(raw.get("codigo_ml", "")),
        "codigo_universal": norm_code(raw.get("codigo_universal", "")),
        "sku": norm_code(raw.get("sku", "")),
        "descripcion": desc, "descripcion_kame": desc,
        "descripcion_ml": clean_text(raw.get("descripcion_ml", "")) or desc,
        "descripcion_fuente": clean_text(raw.get("descripcion_fuente", "")),
        "familia_kame": clean_text(raw.get("familia_kame", "")),
        "maestro_match_status": clean_text(raw.get("maestro_match_status", "")),
        "origen_item": clean_text(raw.get("origen_item", "PDF_FULL")) or "PDF_FULL",
        "motivo_anexo": clean_text(raw.get("motivo_anexo", "")),
        "usuario_anexo": clean_text(raw.get("usuario_anexo", "")),
        "fecha_anexo": clean_text(raw.get("fecha_anexo", "")),
        "anexo_ml_confirmado": to_int(raw.get("anexo_ml_confirmado", 0)),
        "anexo_ml_confirmado_at": clean_text(raw.get("anexo_ml_confirmado_at", "")),
        "anexo_ml_confirmado_by": clean_text(raw.get("anexo_ml_confirmado_by", "")),
        "anexo_ml_confirmado_comment": clean_text(raw.get("anexo_ml_confirmado_comment", "")),
        "anexo_kame_confirmado": to_int(raw.get("anexo_kame_confirmado", 0)),
        "anexo_kame_confirmado_at": clean_text(raw.get("anexo_kame_confirmado_at", "")),
        "anexo_kame_confirmado_by": clean_text(raw.get("anexo_kame_confirmado_by", "")),
        "anexo_kame_confirmado_comment": clean_text(raw.get("anexo_kame_confirmado_comment", "")),
        "unidades": max(0, to_int(raw.get("unidades", raw.get("cantidad", 0)))),
        "acopiadas": max(0, to_int(raw.get("acopiadas", 0))),
        "identificacion": clean_text(raw.get("identificacion", "")),
        "vence": clean_text(raw.get("vence", "")),
        "instrucciones": clean_text(raw.get("instrucciones", "")),
        "dia": clean_text(raw.get("dia", "")), "hora": clean_text(raw.get("hora", "")),
        "created_at": now, "updated_at": now,
        "fuente_rescate": "PDF_RECONCILIACION",
        "rescue_note": "Reconciliado contra PDF corregido de Mercado Libre",
    }


def apply_pdf_reconciliation_events_to_item_map(items_map: dict[int, dict], reconciliation_events: list[dict], lote_id: int, used_ids: set[int] | None = None) -> int:
    """Aplica eventos idempotentes de PDF corregido sobre un mapa de items.

    Las acciones se guardan en Sheets. Por eso un reinicio/restauración no vuelve
    a la versión antigua del FULL aunque existan snapshots históricos previos.
    """
    if not reconciliation_events:
        return 0
    used_ids = used_ids if used_ids is not None else set(items_map.keys())
    applied = 0
    for ev in reconciliation_events:
        if clean_text(ev.get("event_type", "")).lower() != PDF_RECONCILIATION_EVENT:
            continue
        for ch in ev.get("changes", []) or []:
            if not isinstance(ch, dict):
                continue
            action = clean_text(ch.get("action", "")).upper()
            raw = dict(ch.get("item") or ch)
            original_id = to_int(ch.get("item_id", raw.get("item_id", 0)))
            item_id, _ = resolve_restore_item_identity(items_map, raw, original_id)
            if item_id is None:
                requested = to_int(ch.get("item_id", 0))
                if requested and requested not in items_map:
                    item_id = requested
                else:
                    key = _product_key_from_values(raw.get("codigo_ml", ""), raw.get("codigo_universal", ""), raw.get("sku", ""), original_id)
                    item_id = _stable_negative_id(f"PDFREC:{lote_id}:{key}", used_ids)
                used_ids.add(int(item_id))
                items_map[int(item_id)] = _new_reconciliation_item(raw, lote_id, int(item_id))
            item = items_map[int(item_id)]
            if action == "REMOVE":
                item["unidades"] = 0
                item["origen_item"] = "PDF_CORREGIDO_RETIRADO"
                item["motivo_anexo"] = clean_text(ch.get("motivo", "Retirado por PDF corregido Mercado Libre"))
                item["updated_at"] = clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds")
                applied += 1
                continue
            if action in {"UPDATE", "ADD", "SET_QTY"}:
                for field in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "identificacion", "vence", "instrucciones", "dia", "hora"]:
                    if field in raw and clean_text(raw.get(field, "")):
                        item[field] = norm_code(raw[field]) if field in {"codigo_ml", "codigo_universal", "sku"} else clean_text(raw[field])
                if clean_text(raw.get("descripcion", "")):
                    item["descripcion"] = clean_text(raw.get("descripcion", ""))
                    item["descripcion_kame"] = clean_text(raw.get("descripcion_kame", raw.get("descripcion", "")))
                if clean_text(raw.get("descripcion_ml", "")):
                    item["descripcion_ml"] = clean_text(raw.get("descripcion_ml", ""))
                item["unidades"] = max(0, to_int(raw.get("unidades", ch.get("unidades_after", item.get("unidades", 0)))))
                if action == "ADD":
                    item["origen_item"] = "PDF_CORREGIDO_AGREGADO"
                elif clean_text(item.get("origen_item", "")).upper() == "PDF_CORREGIDO_RETIRADO":
                    item["origen_item"] = "PDF_FULL"
                item["updated_at"] = clean_text(ev.get("created_at", "")) or now_cl().isoformat(timespec="seconds")
                applied += 1
    return applied


def _load_raw_lote_items(lote_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            "SELECT * FROM items WHERE lote_id=? ORDER BY area, CAST(nro AS INTEGER), id",
            c, params=(int(lote_id),),
        )


def _match_pdf_row_to_local_item(pdf_row: dict, local_rows: list[dict]) -> dict | None:
    ml = norm_code(pdf_row.get("codigo_ml", ""))
    sku = norm_code(pdf_row.get("sku", ""))
    candidates = []
    for item in local_rows:
        if clean_text(item.get("origen_item", "")).upper() == "ANEXO_MANUAL":
            continue
        ml_match = bool(ml and norm_code(item.get("codigo_ml", "")) == ml)
        sku_match = bool(sku and norm_code(item.get("sku", "")) == sku)
        if ml_match or sku_match:
            candidates.append(item)
    if not candidates:
        return None
    # Si ML y SKU apuntan al mismo registro, es match seguro. Si solo uno
    # coincide y es único, también. En caso ambiguo no se corrige a ciegas.
    exact = [x for x in candidates if (not ml or norm_code(x.get("codigo_ml", "")) == ml) and (not sku or norm_code(x.get("sku", "")) == sku)]
    if len(exact) == 1:
        return exact[0]
    if len(candidates) == 1:
        return candidates[0]
    return None


def build_pdf_reconciliation_plan(lote_id: int, uploaded_pdf) -> dict:
    """Compara el lote local con un PDF oficial corregido sin modificar nada."""
    pdf_df, totals = build_full_input_from_pdf(uploaded_pdf)
    if pdf_df is None or pdf_df.empty:
        return {"ok": False, "error": "No pude extraer productos válidos del PDF."}
    expected_products = to_int(totals.get("expected_products", 0))
    expected_units = to_int(totals.get("expected_units", 0))
    parsed_products = int(len(pdf_df))
    parsed_units = int(pd.to_numeric(pdf_df["unidades"], errors="coerce").fillna(0).sum())
    if expected_products and parsed_products != expected_products:
        return {"ok": False, "error": f"PDF inconsistente: declara {expected_products} productos, pero se extrajeron {parsed_products}."}
    if expected_units and parsed_units != expected_units:
        return {"ok": False, "error": f"PDF inconsistente: declara {expected_units} unidades, pero se extrajeron {parsed_units}."}

    raw_df = _load_raw_lote_items(lote_id)
    if raw_df.empty:
        return {"ok": False, "error": "Primero restaura o carga el lote localmente para reconciliarlo."}
    local_rows = [dict(r) for _, r in raw_df.iterrows()]
    active_local = [r for r in local_rows if to_int(r.get("unidades", 0)) > 0 and clean_text(r.get("origen_item", "")).upper() != "ANEXO_MANUAL"]
    matched_ids = set()
    changes = []
    conflicts = []

    for _, pr in pdf_df.iterrows():
        pdf_item = {k: pr.get(k, "") for k in pdf_df.columns}
        pdf_item["unidades"] = to_int(pdf_item.get("unidades", 0))
        local = _match_pdf_row_to_local_item(pdf_item, local_rows)
        if local is None:
            changes.append({"action": "ADD", "item_id": 0, "unidades_before": 0, "unidades_after": pdf_item["unidades"], "item": pdf_item, "motivo": "Producto presente en PDF corregido y ausente localmente"})
            continue
        iid = int(local["id"])
        matched_ids.add(iid)
        before = to_int(local.get("unidades", 0))
        after = pdf_item["unidades"]
        if before != after:
            if to_int(local.get("acopiadas", 0)) > after:
                conflicts.append(f"{norm_code(local.get('codigo_ml',''))}: PDF={after}, pero ya hay {to_int(local.get('acopiadas',0))} unidades acopiadas.")
            changes.append({"action": "UPDATE", "item_id": iid, "unidades_before": before, "unidades_after": after, "item": pdf_item, "motivo": "Cantidad corregida por PDF oficial de Mercado Libre"})

    for local in active_local:
        iid = int(local["id"])
        if iid in matched_ids:
            continue
        if to_int(local.get("acopiadas", 0)) > 0:
            conflicts.append(f"{norm_code(local.get('codigo_ml',''))}: no aparece en PDF, pero tiene {to_int(local.get('acopiadas',0))} unidades acopiadas.")
        changes.append({
            "action": "REMOVE", "item_id": iid,
            "unidades_before": to_int(local.get("unidades", 0)), "unidades_after": 0,
            "item": {k: local.get(k, "") for k in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "descripcion_kame", "descripcion_ml", "identificacion", "vence", "instrucciones", "dia", "hora"]},
            "motivo": "Producto ausente del PDF oficial corregido de Mercado Libre",
        })

    local_active_units = int(sum(to_int(x.get("unidades", 0)) for x in active_local))
    pdf_hash = hashlib.sha256(uploaded_pdf.getvalue() if hasattr(uploaded_pdf, "getvalue") else uploaded_pdf.read()).hexdigest()
    return {
        "ok": not conflicts,
        "error": " · ".join(conflicts) if conflicts else "",
        "lote_id": int(lote_id),
        "pdf_name": clean_text(getattr(uploaded_pdf, "name", "PDF corregido")),
        "pdf_hash": pdf_hash,
        "expected_products": expected_products or parsed_products,
        "expected_units": expected_units or parsed_units,
        "local_products": int(len(active_local)),
        "local_units": local_active_units,
        "changes": changes,
        "conflicts": conflicts,
    }


def _rebalance_picking_item_quantities(c, lote_id: int, item_id: int, target_qty: int) -> tuple[bool, str]:
    """Ajusta listas activas sin borrar validaciones históricas."""
    rows = c.execute(
        """
        SELECT pli.id, pli.picking_list_id, pli.cantidad
        FROM picking_list_items pli
        JOIN picking_lists pl ON pl.id=pli.picking_list_id
        WHERE pli.lote_id=? AND pli.item_id=? AND UPPER(COALESCE(pl.estado,'')) NOT IN ('ANULADA','COMPLETADA')
        ORDER BY pli.id
        """, (int(lote_id), int(item_id))
    ).fetchall()
    if not rows:
        return True, ""
    scan_by_list = {}
    scans = c.execute(
        "SELECT picking_list_id, COALESCE(SUM(cantidad),0) AS n FROM scans WHERE lote_id=? AND item_id=? GROUP BY picking_list_id",
        (int(lote_id), int(item_id))
    ).fetchall()
    for r in scans:
        scan_by_list[to_int(r["picking_list_id"])] = to_int(r["n"])
    if sum(scan_by_list.get(to_int(r["picking_list_id"]), 0) for r in rows) > int(target_qty):
        return False, "Las validaciones históricas superan la nueva cantidad del PDF. No se modificó la lista."

    remaining = int(target_qty)
    min_later = [0] * len(rows)
    acc = 0
    for idx in range(len(rows)-1, -1, -1):
        min_later[idx] = acc
        acc += scan_by_list.get(to_int(rows[idx]["picking_list_id"]), 0)
    for idx, r in enumerate(rows):
        floor = scan_by_list.get(to_int(r["picking_list_id"]), 0)
        existing = max(to_int(r["cantidad"]), floor)
        desired = max(floor, min(existing, max(floor, remaining - min_later[idx])))
        c.execute("UPDATE picking_list_items SET cantidad=? WHERE id=?", (int(desired), int(r["id"])))
        remaining -= int(desired)
    # Si el PDF aumentó la cantidad, no se infla una lista ya emitida: la
    # diferencia queda disponible para asignar en una lista complementaria.
    return True, ""


def reconcile_all_active_picking_quantities(c, lote_id: int) -> list[str]:
    """Alinea listas activas con las cantidades oficiales ya restauradas.

    No borra listas ni escaneos. Solo reduce cantidades de listas cuando superan
    el objetivo oficial; si un PDF aumentó cantidad, la diferencia queda sin
    asignar para una lista complementaria en vez de inflar una lista ya impresa.
    """
    notes = []
    rows = c.execute(
        "SELECT id, unidades FROM items WHERE lote_id=? ORDER BY id",
        (int(lote_id),),
    ).fetchall()
    for row in rows:
        ok, msg = _rebalance_picking_item_quantities(c, int(lote_id), int(row["id"]), max(0, to_int(row["unidades"])))
        if not ok:
            notes.append(clean_text(msg))
    return [x for x in notes if x]


def apply_pdf_reconciliation_plan(plan: dict, usuario: str, comentario: str) -> tuple[bool, str]:
    """Aplica una reconciliación validada, auditable y restaurable desde Sheets."""
    if not plan or not plan.get("ok"):
        return False, clean_text((plan or {}).get("error", "Plan de reconciliación inválido."))
    lote_id = int(plan.get("lote_id", 0))
    if not lote_id:
        return False, "Lote inválido."
    if is_lote_closed(lote_id):
        return False, "El lote está cerrado. Reábrelo antes de reconciliar contra un PDF corregido."
    usuario = clean_text(usuario) or "SIN_USUARIO"
    comentario = clean_text(comentario) or "Reconciliación contra PDF oficial corregido de Mercado Libre"
    now = now_cl().isoformat(timespec="seconds")
    applied_changes = []

    with db() as c:
        for ch in plan.get("changes", []):
            action = clean_text(ch.get("action", "")).upper()
            item_data = dict(ch.get("item") or {})
            item_id = to_int(ch.get("item_id", 0))
            if action == "ADD":
                desc = clean_text(item_data.get("descripcion_kame", "")) or clean_text(item_data.get("descripcion", ""))
                cur = c.execute(
                    """
                    INSERT INTO items
                    (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml,
                     descripcion_fuente, familia_kame, maestro_match_status, origen_item, motivo_anexo, usuario_anexo, fecha_anexo,
                     unidades, acopiadas, identificacion, vence, instrucciones, dia, hora, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PDF_CORREGIDO_AGREGADO', ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (lote_id, clean_text(item_data.get("area", "")), clean_text(item_data.get("nro", "")), norm_code(item_data.get("codigo_ml", "")),
                     norm_code(item_data.get("codigo_universal", "")), norm_code(item_data.get("sku", "")), desc, desc,
                     clean_text(item_data.get("descripcion_ml", "")) or desc, clean_text(item_data.get("descripcion_fuente", "")),
                     clean_text(item_data.get("familia_kame", "")), clean_text(item_data.get("maestro_match_status", "")),
                     comentario, usuario, now, to_int(ch.get("unidades_after", item_data.get("unidades", 0))),
                     clean_text(item_data.get("identificacion", "")), clean_text(item_data.get("vence", "")), clean_text(item_data.get("instrucciones", "")),
                     clean_text(item_data.get("dia", "")), clean_text(item_data.get("hora", "")), now, now),
                )
                item_id = int(cur.lastrowid)
                ch["item_id"] = item_id
            else:
                row = c.execute("SELECT * FROM items WHERE lote_id=? AND id=?", (lote_id, item_id)).fetchone()
                if not row:
                    c.rollback()
                    return False, f"No encontré el item {item_id} durante la reconciliación."
                if action == "REMOVE":
                    if to_int(row["acopiadas"]) > 0:
                        c.rollback()
                        return False, f"No se puede retirar {norm_code(row['codigo_ml'])}: tiene acopio histórico."
                    c.execute(
                        "UPDATE items SET unidades=0, origen_item='PDF_CORREGIDO_RETIRADO', motivo_anexo=?, usuario_anexo=?, fecha_anexo=?, updated_at=? WHERE lote_id=? AND id=?",
                        (clean_text(ch.get("motivo", "Retirado por PDF corregido")), usuario, now, now, lote_id, item_id),
                    )
                elif action in {"UPDATE", "SET_QTY"}:
                    target = to_int(ch.get("unidades_after", item_data.get("unidades", 0)))
                    if to_int(row["acopiadas"]) > target:
                        c.rollback()
                        return False, f"No se puede bajar {norm_code(row['codigo_ml'])} a {target}: tiene {to_int(row['acopiadas'])} acopiadas."
                    desc = clean_text(item_data.get("descripcion_kame", "")) or clean_text(row["descripcion_kame"]) or clean_text(item_data.get("descripcion", ""))
                    c.execute(
                        """
                        UPDATE items
                        SET codigo_ml=?, codigo_universal=?, sku=?, descripcion=?, descripcion_kame=?, descripcion_ml=?,
                            origen_item=CASE WHEN UPPER(COALESCE(origen_item,''))='PDF_CORREGIDO_RETIRADO' THEN 'PDF_FULL' ELSE origen_item END,
                            unidades=?, identificacion=?, vence=?, instrucciones=?, updated_at=?
                        WHERE lote_id=? AND id=?
                        """,
                        (norm_code(item_data.get("codigo_ml", row["codigo_ml"])), norm_code(item_data.get("codigo_universal", row["codigo_universal"])),
                         norm_code(item_data.get("sku", row["sku"])), desc, desc,
                         clean_text(item_data.get("descripcion_ml", "")) or clean_text(row["descripcion_ml"]), target,
                         clean_text(item_data.get("identificacion", row["identificacion"])), clean_text(item_data.get("vence", row["vence"])),
                         clean_text(item_data.get("instrucciones", row["instrucciones"])), now, lote_id, item_id),
                    )
                else:
                    continue
            ok_pick, msg_pick = _rebalance_picking_item_quantities(c, lote_id, item_id, to_int(ch.get("unidades_after", item_data.get("unidades", 0))))
            if not ok_pick:
                c.rollback()
                return False, msg_pick
            applied_changes.append(ch)
        c.commit()

    payload = {
        **build_lote_payload(lote_id),
        "created_at": now,
        "usuario": usuario,
        "archivo_pdf": clean_text(plan.get("pdf_name", "PDF corregido")),
        "pdf_hash": clean_text(plan.get("pdf_hash", "")),
        "productos_pdf": to_int(plan.get("expected_products", 0)),
        "unidades_pdf": to_int(plan.get("expected_units", 0)),
        "comentario": comentario,
        "changes": applied_changes,
        "tipo": "RECONCILIACION_PDF_CORREGIDO",
    }
    enqueue_backup_event(PDF_RECONCILIATION_EVENT, payload)
    for ch in applied_changes:
        it = ch.get("item") or {}
        log_audit_event(lote_id, to_int(ch.get("item_id", 0)) or None, "RECONCILIACION_PDF_CORREGIDO", clean_text(ch.get("motivo", comentario)), to_int(ch.get("unidades_after", 0)), norm_code(it.get("codigo_ml", "")), norm_code(it.get("sku", "")), usuario)
    queue_lote_snapshot_from_sqlite(lote_id, motivo="RECONCILIACION_PDF_CORREGIDO", usuario=usuario, force=True)
    return True, f"Reconciliación aplicada: {len(applied_changes)} cambio(s). Se encoló evento y snapshot para que futuros rescates recuperen el PDF corregido."


def get_pdf_reconciled_removed_count(lote_id: int) -> int:
    with db() as c:
        row = c.execute("SELECT COUNT(*) AS n FROM items WHERE lote_id=? AND UPPER(COALESCE(origen_item,''))='PDF_CORREGIDO_RETIRADO'", (int(lote_id),)).fetchone()
    return to_int(row["n"] if row else 0)

def get_latest_quantity_adjustments(lote_id: int) -> dict[int, int]:
    """Cantidad objetivo vigente por producto para inventario/listas/etiquetas.

    Esta función NO calcula pendiente físico. Solo responde: ¿cuál es la cantidad
    vigente del producto después de avisos de ajuste de cantidad?

    Regla:
    - Toma el último aviso de "Ajuste de cantidad" con cantidad_nueva.
    - Aplica aunque el aviso esté ACTIVO o RESUELTO, porque resolver el aviso no
      debe revertir el ajuste.
    - No capea por acopiadas. Ese capeo pertenece solamente a Supervisor/Cierre.
    """
    try:
        lid = int(lote_id)
    except Exception:
        return {}
    with db() as c:
        rows = c.execute(
            """
            SELECT item_id, cantidad_nueva
            FROM avisos_operacionales
            WHERE lote_id=?
              AND COALESCE(cantidad_nueva,'') <> ''
              AND LOWER(COALESCE(tipo_aviso,'')) LIKE '%ajuste%'
            ORDER BY id ASC
            """,
            (lid,),
        ).fetchall()
    out = {}
    for r in rows:
        try:
            iid = int(r["item_id"] or 0)
            qty_new = int(float(str(r["cantidad_nueva"]).replace(',', '.')))
        except Exception:
            continue
        if iid:
            out[iid] = max(0, int(qty_new))
    return out


def get_latest_label_quantity_adjustments(lote_id: int) -> dict[int, int]:
    """Cantidad de referencia para etiquetas/reimpresión.

    Es intencionalmente igual al objetivo vigente de ajuste de cantidad, sin
    convertir avisos confirmados en cero ni en acopiadas. Etiquetas necesita saber
    cuántas etiquetas corresponden al producto, no cuánto falta físicamente.
    """
    return get_latest_quantity_adjustments(lote_id)


def get_latest_physical_quantity_adjustments(lote_id: int) -> dict[int, int]:
    """Compatibilidad histórica: la meta física usa la cantidad vigente del lote.

    Un aviso confirmado acredita gestión administrativa, pero no puede convertir
    una cantidad de producto en cero ni restarla por segunda vez. La cantidad
    oficial vive en items/snapshot o en una reconciliación PDF explícita.
    """
    return get_latest_quantity_adjustments(lote_id)

def get_effective_item_units(lote_id: int, item_id: int, default_units: int | None = None) -> int | None:
    """Cantidad oficial actual del item para validar escaneo/lista.

    No descuenta avisos por una segunda vía. El valor de items ya representa el
    objetivo vigente y una reconciliación PDF puede llevarlo a 0 de forma segura.
    """
    try:
        with db() as c:
            row = c.execute("SELECT unidades, origen_item FROM items WHERE lote_id=? AND id=?", (int(lote_id), int(item_id))).fetchone()
        if row:
            if clean_text(row["origen_item"]).upper() == "PDF_CORREGIDO_RETIRADO":
                return 0
            return max(0, to_int(row["unidades"]))
    except Exception:
        pass
    return default_units

def apply_quantity_adjustments_df(lote_id: int, df: pd.DataFrame, item_col: str = 'id', qty_col: str = 'unidades') -> pd.DataFrame:
    """Aplica cantidad vigente de avisos de ajuste, sin capeo físico.

    Uso: inventario, snapshot, etiquetas, reserva y vistas donde la cantidad
    vigente del producto debe conservarse como número real de referencia.
    """
    if df is None or df.empty or item_col not in df.columns or qty_col not in df.columns:
        return df
    adjustments = get_latest_quantity_adjustments(lote_id)
    if not adjustments:
        return df
    out = df.copy()
    def _apply(row):
        try:
            if clean_text(row.get("origen_item", "")).upper() == "PDF_CORREGIDO_RETIRADO":
                return 0
            iid = int(row[item_col])
            if iid in adjustments:
                return int(adjustments[iid])
        except Exception:
            pass
        return to_int(row[qty_col])
    out[qty_col] = out.apply(_apply, axis=1).astype(int)
    return out


def apply_physical_quantity_adjustments_df(lote_id: int, df: pd.DataFrame, item_col: str = 'id', qty_col: str = 'unidades') -> pd.DataFrame:
    """Aplica cantidad física para Supervisor/Cierre/Escaneo.

    Esta es la única ruta que puede capear un ajuste confirmado ML+Kame contra
    acopiadas para eliminar falsos pendientes físicos.
    """
    if df is None or df.empty or item_col not in df.columns or qty_col not in df.columns:
        return df
    adjustments = get_latest_physical_quantity_adjustments(lote_id)
    if not adjustments:
        return df
    out = df.copy()
    def _apply(row):
        try:
            iid = int(row[item_col])
            if iid in adjustments:
                return int(adjustments[iid])
        except Exception:
            pass
        return to_int(row[qty_col])
    out[qty_col] = out.apply(_apply, axis=1).astype(int)
    return out



def get_operationally_blocked_item_ids(lote_id: int) -> set[int]:
    """Productos que no deben entrar al flujo operativo físico.

    Producto retirado del lote es permanente para ese FULL, aunque el aviso sea
    resuelto administrativamente. "No escanear / esperar instrucción" bloquea
    solo mientras esté ACTIVO.
    """
    try:
        lid = int(lote_id)
    except Exception:
        return set()
    with db() as c:
        rows = c.execute(
            """
            SELECT item_id, tipo_aviso, estado
            FROM avisos_operacionales
            WHERE lote_id=?
            """,
            (lid,),
        ).fetchall()
    blocked = set()
    for r in rows:
        tipo = clean_text(r["tipo_aviso"]).upper()
        estado = clean_text(r["estado"]).upper()
        bloquea = False
        if tipo == "PRODUCTO RETIRADO DEL LOTE":
            bloquea = True
        elif tipo == "NO ESCANEAR / ESPERAR INSTRUCCIÓN" and estado == "ACTIVO":
            bloquea = True
        if bloquea:
            try:
                iid = int(r["item_id"] or 0)
                if iid:
                    blocked.add(iid)
            except Exception:
                pass
    return blocked


def apply_operational_exclusions_df(lote_id: int, df: pd.DataFrame, item_col: str = "id", qty_col: str = "unidades") -> pd.DataFrame:
    """Filtra productos fuera del objetivo físico actual.

    No cambia SQLite. Solo evita que retirados, bloqueados o cantidad física 0
    inflen métricas, picking, escaneo y cierre.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if item_col in out.columns:
        blocked = get_operationally_blocked_item_ids(lote_id)
        if blocked:
            out = out[~out[item_col].map(lambda x: to_int(x) in blocked)].copy()
    if qty_col in out.columns:
        out = out[out[qty_col].map(to_int) > 0].copy()
    return out.reset_index(drop=True)


def get_operational_items(lote_id: int) -> pd.DataFrame:
    """Base física del lote para escaneo, picking, Supervisor y cierre.

    Se opera contra la cantidad oficial vigente del lote. Los avisos dejan
    trazabilidad y bloquean cuando corresponde, pero no reducen otra vez el
    total si Mercado Libre/PDF ya incorporó el ajuste.
    """
    df = get_items(lote_id)
    return apply_operational_exclusions_df(lote_id, df, item_col="id", qty_col="unidades")


def get_lote_reference_items(lote_id: int) -> pd.DataFrame:
    """Base de referencia del FULL/lote sin capeo físico.

    Uso exclusivo para mostrar totales reales del lote en Escaneo/diagnóstico.
    No debe usarse para validar si falta preparar producto ni para cierre.

    Regla:
    - Parte de items restaurados desde el último snapshot/lote_item.
    - Aplica ajustes de cantidad como cantidad vigente del producto.
    - No capea avisos confirmados contra acopiadas.
    - No excluye retirados/bloqueados, porque el total del FULL debe quedar
      trazable aunque el pendiente operativo sea menor.
    """
    df = get_items(lote_id)
    if df is None or df.empty:
        return df
    return apply_quantity_adjustments_df(lote_id, df, item_col="id", qty_col="unidades")


def get_label_reprint_items(lote_id: int) -> pd.DataFrame:
    """Base única para etiquetas/reimpresión individual.

    No usa la cantidad física de Supervisor. Por eso evita falsos estados como
    Unidades=0 / Impresas=36 / SOBREIMPRESO cuando el aviso ya estaba confirmado.
    """
    with db() as c:
        df = pd.read_sql_query(
            "SELECT * FROM items WHERE lote_id=? ORDER BY area, CAST(nro AS INTEGER), id",
            c,
            params=(lote_id,),
        )
    if df is None or df.empty:
        return df
    out = apply_quantity_adjustments_df(lote_id, df, item_col="id", qty_col="unidades")
    if "id" in out.columns:
        blocked = get_operationally_blocked_item_ids(lote_id)
        if blocked:
            out = out[~out["id"].map(lambda x: to_int(x) in blocked)].copy()
    if "unidades" in out.columns:
        out = out[out["unidades"].map(to_int) > 0].copy()
    return out.reset_index(drop=True)


def get_adjusted_out_items_count(lote_id: int) -> int:
    """Cantidad de líneas sacadas del objetivo operativo por aviso o cantidad 0."""
    all_items = get_items(lote_id)
    op_items = get_operational_items(lote_id)
    if all_items is None or all_items.empty:
        return 0
    return max(int(len(all_items)) - int(len(op_items) if op_items is not None else 0), 0)

def get_items(lote_id):
    with db() as c:
        df = pd.read_sql_query(
            "SELECT * FROM items WHERE lote_id=? ORDER BY area, CAST(nro AS INTEGER), id",
            c,
            params=(lote_id,),
        )
    return apply_quantity_adjustments_df(lote_id, df, item_col="id", qty_col="unidades")


def get_scans_deduped(lote_id: int, limit: int | None = None) -> pd.DataFrame:
    """Lee escaneos del lote eliminando duplicados técnicos de rescate.

    Motivo: al rescatar desde Sheets podemos recibir el mismo scan desde la hoja
    madre `eventos` y desde hojas estructuradas como `picking_validaciones`.
    Eso no es doble acopio real; es el mismo evento visto por dos rutas.

    No se usa `queue_id` como identidad porque puede repetirse después de reboot.
    Se deduplica por una firma semántica del scan dentro del mismo lote.
    """
    try:
        lote_id = int(lote_id)
    except Exception:
        return pd.DataFrame()
    sql = """
        SELECT id, lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at,
               operador_validador, picking_list_id, picking_code, picker_asignado,
               original_item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, maestro_match_status, restore_match_status
        FROM scans
        WHERE lote_id=?
        ORDER BY id DESC
    """
    if limit:
        # Leemos más que el límite para que la deduplicación no esconda filas recientes.
        sql += f" LIMIT {max(int(limit) * 5, int(limit) + 20)}"
    with db() as c:
        df = pd.read_sql_query(sql, c, params=(lote_id,))
    if df.empty:
        return df

    out = df.copy()
    for col in ["created_at", "scan_primario", "scan_secundario", "modo", "operador_validador", "picking_code", "picker_asignado", "codigo_ml", "codigo_universal", "sku", "descripcion", "descripcion_kame", "descripcion_ml", "familia_kame", "maestro_match_status", "restore_match_status"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(clean_text)
    for col in ["scan_primario", "scan_secundario", "codigo_ml", "codigo_universal", "sku"]:
        out[col] = out[col].map(norm_code)
    for col in ["item_id", "original_item_id", "picking_list_id", "cantidad"]:
        if col not in out.columns:
            out[col] = 0
        out[col] = out[col].map(to_int)

    # Preferimos la fila que logró match con item oficial si existe.
    status_rank = {"MATCH_ITEM_ID": 0, "MATCH_CODE_SAME_LOTE": 1, "ACOPIO_RECUPERADO_SHEETS": 2, "NO_MATCH_SNAPSHOT": 2, "AMBIGUOUS_SAME_LOTE": 3}
    out["_status_rank"] = out["restore_match_status"].map(lambda x: status_rank.get(clean_text(x), 5))
    out["_sig_item"] = out.apply(lambda r: str(int(r.get("original_item_id") or r.get("item_id") or 0)), axis=1)
    out["_sig_code"] = out.apply(lambda r: norm_code(r.get("codigo_ml", "")) or norm_code(r.get("scan_primario", "")) or norm_code(r.get("codigo_universal", "")) or norm_code(r.get("sku", "")), axis=1)
    out["_scan_sig"] = out.apply(lambda r: "|".join([
        str(lote_id),
        clean_text(r.get("created_at", "")),
        clean_text(r.get("_sig_item", "")),
        clean_text(r.get("_sig_code", "")),
        norm_code(r.get("sku", "")),
        str(to_int(r.get("cantidad", 0))),
        clean_text(r.get("modo", "")),
        clean_text(r.get("operador_validador", "")),
        clean_text(r.get("picking_code", "")),
        clean_text(r.get("picker_asignado", "")),
    ]), axis=1)
    out = out.sort_values(["_scan_sig", "_status_rank", "id"], ascending=[True, True, False], kind="mergesort")
    out = out.drop_duplicates(subset=["_scan_sig"], keep="first")
    out = out.sort_values("id", ascending=False, kind="mergesort")
    out = out.drop(columns=["_status_rank", "_sig_item", "_sig_code", "_scan_sig"], errors="ignore")
    if limit:
        out = out.head(int(limit))
    return out.reset_index(drop=True)


def get_last_scans(lote_id):
    scans = get_scans_deduped(lote_id)
    if scans.empty:
        return pd.DataFrame(columns=["item_id", "procesado_at", "escaneado_total"])
    scans = scans.copy()
    scans["cantidad"] = scans["cantidad"].map(to_int)
    grouped = scans.groupby("item_id", as_index=False).agg(
        procesado_at=("created_at", "max"),
        escaneado_total=("cantidad", "sum"),
    )
    return grouped


def get_scanned_total(lote_id: int) -> int:
    """Total acopiado real según scans deduplicados.

    Evita contar dos veces un mismo scan cuando el rescate lo trae desde
    `eventos` y también desde hojas estructuradas.
    """
    scans = get_scans_deduped(lote_id)
    if scans.empty:
        return 0
    try:
        return max(0, int(scans["cantidad"].map(to_int).sum()))
    except Exception:
        return 0


def get_unmatched_scan_count(lote_id: int) -> int:
    with db() as c:
        row = c.execute(
            """
            SELECT COUNT(*) AS n
            FROM scans
            WHERE lote_id=? AND COALESCE(restore_match_status,'') IN ('ACOPIO_RECUPERADO_SHEETS','NO_MATCH_SNAPSHOT','AMBIGUOUS_SAME_LOTE')
            """,
            (int(lote_id),),
        ).fetchone()
    return int(row["n"] or 0) if row else 0



def snapshot_items_from_sqlite(lote_id: int) -> list[dict]:
    """Devuelve snapshot completo del lote desde SQLite.

    Este snapshot es el seguro de restauración: Sheets copia SQLite, no gobierna la operación.
    """
    df_items = get_items(int(lote_id))
    out = []
    if df_items.empty:
        return out
    for r in df_items.itertuples(index=False):
        if int(getattr(r, "unidades", 0) or 0) <= 0 or clean_text(getattr(r, "origen_item", "")).upper() == "PDF_CORREGIDO_RETIRADO":
            continue
        out.append({
            "item_id": int(r.id),
            "area": clean_text(getattr(r, "area", "")),
            "nro": clean_text(getattr(r, "nro", "")),
            "codigo_ml": norm_code(getattr(r, "codigo_ml", "")),
            "codigo_universal": norm_code(getattr(r, "codigo_universal", "")),
            "sku": norm_code(getattr(r, "sku", "")),
            "descripcion": clean_text(getattr(r, "descripcion", "")),
            "descripcion_kame": clean_text(getattr(r, "descripcion_kame", "")) or clean_text(getattr(r, "descripcion", "")),
            "descripcion_ml": clean_text(getattr(r, "descripcion_ml", "")) or clean_text(getattr(r, "descripcion", "")),
            "descripcion_fuente": clean_text(getattr(r, "descripcion_fuente", "")),
            "familia_kame": clean_text(getattr(r, "familia_kame", "")),
            "maestro_match_status": clean_text(getattr(r, "maestro_match_status", "")),
            "origen_item": clean_text(getattr(r, "origen_item", "PDF_FULL")) or "PDF_FULL",
            "motivo_anexo": clean_text(getattr(r, "motivo_anexo", "")),
            "usuario_anexo": clean_text(getattr(r, "usuario_anexo", "")),
            "fecha_anexo": clean_text(getattr(r, "fecha_anexo", "")),
            "anexo_ml_confirmado": int(getattr(r, "anexo_ml_confirmado", 0) or 0),
            "anexo_ml_confirmado_at": clean_text(getattr(r, "anexo_ml_confirmado_at", "")),
            "anexo_ml_confirmado_by": clean_text(getattr(r, "anexo_ml_confirmado_by", "")),
            "anexo_ml_confirmado_comment": clean_text(getattr(r, "anexo_ml_confirmado_comment", "")),
            "anexo_kame_confirmado": int(getattr(r, "anexo_kame_confirmado", 0) or 0),
            "anexo_kame_confirmado_at": clean_text(getattr(r, "anexo_kame_confirmado_at", "")),
            "anexo_kame_confirmado_by": clean_text(getattr(r, "anexo_kame_confirmado_by", "")),
            "anexo_kame_confirmado_comment": clean_text(getattr(r, "anexo_kame_confirmado_comment", "")),
            "unidades": int(getattr(r, "unidades", 0) or 0),
            "identificacion": clean_text(getattr(r, "identificacion", "")),
            "vence": clean_text(getattr(r, "vence", "")),
            "instrucciones": clean_text(getattr(r, "instrucciones", "")),
            "dia": clean_text(getattr(r, "dia", "")),
            "hora": clean_text(getattr(r, "hora", "")),
            "item_created_at": clean_text(getattr(r, "created_at", "")),
            "item_updated_at": clean_text(getattr(r, "updated_at", "")),
        })
    return out


def queue_lote_snapshot_from_sqlite(lote_id: int, motivo: str = "AUTO", usuario: str = "SISTEMA", chunk_size: int = 50, force: bool = False):
    """Encola snapshot completo del lote local hacia Sheets.

    SQLite-first:
    - el snapshot nace desde la base operativa local;
    - no bloquea a los operadores;
    - no se debe repetir por cada escaneo. El hash excluye campos volátiles
      como item_updated_at para evitar llenar Sheets con snapshots duplicados.
    """
    lid = int(lote_id)
    now = now_cl().isoformat(timespec="seconds")
    items = snapshot_items_from_sqlite(lid)
    if not items:
        return []
    lote_payload = build_lote_payload(lid)
    total_productos = len(items)
    total_unidades = int(sum(int(x.get("unidades") or 0) for x in items))

    # Hash estable de estructura base del lote. No incluye campos volátiles
    # como item_updated_at, porque estos cambian con escaneos/acopio y provocan
    # snapshots repetidos innecesarios.
    stable_items = []
    for it in items:
        stable_items.append({
            "item_id": it.get("item_id"),
            "area": it.get("area"),
            "nro": it.get("nro"),
            "codigo_ml": it.get("codigo_ml"),
            "codigo_universal": it.get("codigo_universal"),
            "sku": it.get("sku"),
            "descripcion": it.get("descripcion"),
            "descripcion_kame": it.get("descripcion_kame"),
            "descripcion_ml": it.get("descripcion_ml"),
            "descripcion_fuente": it.get("descripcion_fuente"),
            "familia_kame": it.get("familia_kame"),
            "maestro_match_status": it.get("maestro_match_status"),
            "origen_item": it.get("origen_item"),
            "motivo_anexo": it.get("motivo_anexo"),
            "usuario_anexo": it.get("usuario_anexo"),
            "fecha_anexo": it.get("fecha_anexo"),
            "anexo_ml_confirmado": it.get("anexo_ml_confirmado"),
            "anexo_ml_confirmado_at": it.get("anexo_ml_confirmado_at"),
            "anexo_ml_confirmado_by": it.get("anexo_ml_confirmado_by"),
            "anexo_ml_confirmado_comment": it.get("anexo_ml_confirmado_comment"),
            "anexo_kame_confirmado": it.get("anexo_kame_confirmado"),
            "anexo_kame_confirmado_at": it.get("anexo_kame_confirmado_at"),
            "anexo_kame_confirmado_by": it.get("anexo_kame_confirmado_by"),
            "anexo_kame_confirmado_comment": it.get("anexo_kame_confirmado_comment"),
            "unidades": it.get("unidades"),
            "identificacion": it.get("identificacion"),
            "vence": it.get("vence"),
            "instrucciones": it.get("instrucciones"),
            "dia": it.get("dia"),
            "hora": it.get("hora"),
            "item_created_at": it.get("item_created_at"),
        })
    snapshot_hash = hashlib.sha256(json.dumps(stable_items, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    if not force:
        try:
            with db() as c:
                row = c.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM backup_queue
                    WHERE event_type='lote_snapshot_completo'
                      AND payload_json LIKE ?
                    """,
                    (f"%{snapshot_hash}%",),
                ).fetchone()
            if row and int(row["n"] or 0) > 0:
                return []
        except Exception:
            pass

    chunk_size = max(25, int(chunk_size or 50))
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    events = []
    for idx, chunk in enumerate(chunks, start=1):
        events.append(("lote_snapshot_chunk", {
            **lote_payload,
            "created_at": now,
            "motivo_snapshot": clean_text(motivo) or "AUTO",
            "usuario": clean_text(usuario) or "SISTEMA",
            "chunk_index": idx,
            "chunk_total": len(chunks),
            "productos_total": total_productos,
            "unidades_total": total_unidades,
            "snapshot_hash": snapshot_hash,
            "items": chunk,
        }))
    events.append(("lote_snapshot_completo", {
        **lote_payload,
        "created_at": now,
        "motivo_snapshot": clean_text(motivo) or "AUTO",
        "usuario": clean_text(usuario) or "SISTEMA",
        "productos_total": total_productos,
        "unidades_total": total_unidades,
        "chunk_total": len(chunks),
        "snapshot_hash": snapshot_hash,
    }))
    return enqueue_backup_events_batch(events)


def ensure_active_lote_snapshot_queued(lote_id: int):
    """Encola snapshot del lote activo solo si falta ese hash estable.

    Ya no debe generar snapshot por cada escaneo. El hash estable del lote evita
    duplicados aunque cambie updated_at por movimientos de acopio.
    """
    try:
        return queue_lote_snapshot_from_sqlite(int(lote_id), motivo="AUTO_ACTIVE_LOTE", usuario="SISTEMA", force=False)
    except Exception:
        return []


def create_lote(nombre, archivo, hoja, df):
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        cur = c.execute(
            "INSERT INTO lotes (nombre, archivo, hoja, created_at) VALUES (?, ?, ?, ?)",
            (nombre, archivo, hoja, now),
        )
        lote_id = cur.lastrowid
        backup_lote_key = make_backup_lote_key(nombre, archivo, hoja, now)
        c.execute("UPDATE lotes SET backup_lote_key=? WHERE id=?", (backup_lote_key, int(lote_id)))
        df = apply_kame_description_fields(df)
        rows = []
        for r in df.itertuples(index=False):
            desc_kame = clean_text(getattr(r, "descripcion_kame", "")) or clean_text(getattr(r, "descripcion", ""))
            desc_ml = clean_text(getattr(r, "descripcion_ml", "")) or desc_kame
            rows.append((
                lote_id,
                clean_text(r.area),
                clean_text(r.nro),
                norm_code(r.codigo_ml),
                norm_code(r.codigo_universal),
                norm_code(r.sku),
                desc_kame,
                desc_kame,
                desc_ml,
                clean_text(getattr(r, "descripcion_fuente", "")) or ("KAME" if desc_kame and desc_kame != desc_ml else "ML_FALLBACK"),
                clean_text(getattr(r, "familia_kame", "")),
                clean_text(getattr(r, "maestro_match_status", "")),
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
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml,
             descripcion_fuente, familia_kame, maestro_match_status, unidades, acopiadas,
             identificacion, vence, instrucciones, dia, hora, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        c.commit()

    lote_payload = build_lote_payload(lote_id)
    inserted = get_items(lote_id)

    expected_productos = int(len(df))
    expected_unidades = int(pd.to_numeric(df["unidades"], errors="coerce").fillna(0).sum()) if "unidades" in df.columns else 0
    local_productos = int(len(inserted))
    local_unidades = int(pd.to_numeric(inserted["unidades"], errors="coerce").fillna(0).sum()) if not inserted.empty else 0
    if local_productos != expected_productos or local_unidades != expected_unidades:
        raise RuntimeError(
            f"Carga local incompleta. Excel={expected_productos} productos/{expected_unidades} unidades; "
            f"SQLite={local_productos} productos/{local_unidades} unidades. No se habilita el lote."
        )

    # SQLite ya quedó validado. Sheets se sincroniza como espejo, no bloquea operación.
    enqueue_backup_event("lote_creado", {
        **lote_payload,
        "created_at": now,
        "total_lineas": expected_productos,
        "total_unidades": expected_unidades,
        "snapshot_mode": "sqlite_snapshot_chunks",
        "sqlite_productos": local_productos,
        "sqlite_unidades": local_unidades,
    })
    queue_lote_snapshot_from_sqlite(lote_id, motivo="LOTE_CREADO", usuario="SISTEMA")
    log_audit_event(lote_id, event_type="LOTE_CREADO", detail=f"Lote creado desde {archivo} / {hoja}. SQLite OK: {local_productos} productos / {local_unidades} unidades", qty=expected_unidades)
    return lote_id


# ============================================================
# Anexos manuales al FULL
# ============================================================

def _anexo_bool(v) -> int:
    return 1 if str(v).strip().upper() in {"1", "TRUE", "SI", "SÍ", "YES"} or v is True or v == 1 else 0


def next_anexo_nro(lote_id: int) -> str:
    """Correlativo simple para anexos; evita depender del PDF original."""
    with db() as c:
        row = c.execute(
            """
            SELECT MAX(CAST(nro AS INTEGER)) AS mx
            FROM items
            WHERE lote_id=? AND COALESCE(nro,'') GLOB '[0-9]*'
            """,
            (int(lote_id),),
        ).fetchone()
    try:
        return str(int(row["mx"] or 0) + 1)
    except Exception:
        return "1"


def get_anexos_lote(lote_id: int) -> pd.DataFrame:
    with db() as c:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM items
            WHERE lote_id=? AND UPPER(COALESCE(origen_item,''))='ANEXO_MANUAL'
            ORDER BY id DESC
            """,
            c,
            params=(int(lote_id),),
        )
    return df


def build_item_event_payload(lote_id: int, item_id: int) -> dict:
    with db() as c:
        row = c.execute("SELECT * FROM items WHERE lote_id=? AND id=?", (int(lote_id), int(item_id))).fetchone()
    if not row:
        return {}
    r = dict(row)
    return {
        **build_lote_payload(int(lote_id)),
        "item_id": int(item_id),
        "area": clean_text(r.get("area", "")),
        "nro": clean_text(r.get("nro", "")),
        "codigo_ml": norm_code(r.get("codigo_ml", "")),
        "codigo_universal": norm_code(r.get("codigo_universal", "")),
        "sku": norm_code(r.get("sku", "")),
        "descripcion": clean_text(r.get("descripcion", "")),
        "descripcion_kame": clean_text(r.get("descripcion_kame", "")) or clean_text(r.get("descripcion", "")),
        "descripcion_ml": clean_text(r.get("descripcion_ml", "")) or clean_text(r.get("descripcion", "")),
        "descripcion_fuente": clean_text(r.get("descripcion_fuente", "")),
        "familia_kame": clean_text(r.get("familia_kame", "")),
        "maestro_match_status": clean_text(r.get("maestro_match_status", "")),
        "unidades": to_int(r.get("unidades", 0)),
        "cantidad": to_int(r.get("unidades", 0)),
        "identificacion": clean_text(r.get("identificacion", "")),
        "vence": clean_text(r.get("vence", "")),
        "instrucciones": clean_text(r.get("instrucciones", "")),
        "dia": clean_text(r.get("dia", "")),
        "hora": clean_text(r.get("hora", "")),
        "origen_item": clean_text(r.get("origen_item", "PDF_FULL")) or "PDF_FULL",
        "motivo_anexo": clean_text(r.get("motivo_anexo", "")),
        "usuario_anexo": clean_text(r.get("usuario_anexo", "")),
        "fecha_anexo": clean_text(r.get("fecha_anexo", "")),
        "anexo_ml_confirmado": to_int(r.get("anexo_ml_confirmado", 0)),
        "anexo_ml_confirmado_at": clean_text(r.get("anexo_ml_confirmado_at", "")),
        "anexo_ml_confirmado_by": clean_text(r.get("anexo_ml_confirmado_by", "")),
        "anexo_ml_confirmado_comment": clean_text(r.get("anexo_ml_confirmado_comment", "")),
        "anexo_kame_confirmado": to_int(r.get("anexo_kame_confirmado", 0)),
        "anexo_kame_confirmado_at": clean_text(r.get("anexo_kame_confirmado_at", "")),
        "anexo_kame_confirmado_by": clean_text(r.get("anexo_kame_confirmado_by", "")),
        "anexo_kame_confirmado_comment": clean_text(r.get("anexo_kame_confirmado_comment", "")),
        "item_created_at": clean_text(r.get("created_at", "")),
        "item_updated_at": clean_text(r.get("updated_at", "")),
    }


def create_producto_anexado_lote(lote_id: int, sku: str, codigo_ml: str, codigo_universal: str, descripcion_ml: str,
                                 cantidad: int, identificacion: str, instrucciones: str, motivo: str, usuario: str,
                                 vence: str = "") -> tuple[bool, str, int | None]:
    """Agrega un producto manual al lote activo con trazabilidad y sin tocar reservas Kame."""
    if is_lote_closed(lote_id):
        return False, "El lote está cerrado. Reabre el lote antes de anexar productos.", None
    sku = norm_code(sku)
    codigo_ml = norm_code(codigo_ml)
    codigo_universal = normalize_universal_code(codigo_universal)
    descripcion_ml = clean_text(descripcion_ml)
    motivo = clean_text(motivo)
    usuario = clean_text(usuario) or "SIN_USUARIO"
    cantidad = int(cantidad or 0)
    if not sku:
        return False, "Debes ingresar SKU.", None
    if not codigo_ml:
        return False, "Debes ingresar Código ML para que etiquetas y trazabilidad queden correctas.", None
    if not descripcion_ml:
        return False, "Debes ingresar descripción ML para la etiqueta.", None
    if cantidad <= 0:
        return False, "La cantidad debe ser mayor a 0.", None
    if not motivo:
        return False, "Debes ingresar motivo del anexo.", None

    with db() as c:
        dup = c.execute(
            """
            SELECT id, sku, codigo_ml, codigo_universal, descripcion
            FROM items
            WHERE lote_id=? AND (
                UPPER(COALESCE(sku,''))=? OR UPPER(COALESCE(codigo_ml,''))=? OR
                (? <> 'N/A' AND UPPER(COALESCE(codigo_universal,''))=?)
            )
            LIMIT 1
            """,
            (int(lote_id), sku, codigo_ml, codigo_universal, codigo_universal),
        ).fetchone()
        if dup:
            return False, f"Este producto/código ya existe en el lote como item #{dup['id']}. No se anexa para evitar duplicidad.", None

    desc_map, family_map, barcode_map, _ = load_kame_master_maps()
    desc_kame = clean_text(desc_map.get(sku, ""))
    familia = clean_text(family_map.get(sku, ""))
    if not desc_kame:
        # Permitimos anexar, pero queda marcado para revisión; la operación no se detiene.
        desc_kame = descripcion_ml
        desc_fuente = "ML_FALLBACK"
        match_status = "SKU_NO_ENCONTRADO"
    else:
        desc_fuente = "KAME"
        match_status = "MATCH_SKU"
    if codigo_universal == "N/A":
        master_barcode = normalize_universal_code(barcode_map.get(sku, ""))
        if master_barcode != "N/A":
            codigo_universal = master_barcode

    now = now_cl().isoformat(timespec="seconds")
    nro = next_anexo_nro(lote_id)
    with db() as c:
        cur = c.execute(
            """
            INSERT INTO items
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml,
             descripcion_fuente, familia_kame, maestro_match_status, unidades, acopiadas,
             identificacion, vence, instrucciones, dia, hora, created_at, updated_at,
             origen_item, motivo_anexo, usuario_anexo, fecha_anexo,
             anexo_ml_confirmado, anexo_kame_confirmado)
            VALUES (?, 'ANEXO', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, '', '', ?, ?,
                    'ANEXO_MANUAL', ?, ?, ?, 0, 0)
            """,
            (int(lote_id), nro, codigo_ml, codigo_universal, sku, desc_kame, desc_kame, descripcion_ml,
             desc_fuente, familia, match_status, cantidad, clean_text(identificacion), clean_text(vence), clean_text(instrucciones),
             now, now, motivo, usuario, now),
        )
        item_id = int(cur.lastrowid)
        c.commit()

    payload = build_item_event_payload(lote_id, item_id)
    payload.update({"created_at": now, "motivo": motivo, "usuario": usuario, "tipo": "ANEXO_MANUAL", "comentario": motivo})
    enqueue_backup_event("producto_anexado_lote", payload)
    log_audit_event(lote_id, item_id=item_id, event_type="PRODUCTO_ANEXADO", detail=f"Producto anexado manualmente. Motivo: {motivo}", qty=cantidad, codigo_ml=codigo_ml, sku=sku, mode=usuario)
    queue_lote_snapshot_from_sqlite(lote_id, motivo="PRODUCTO_ANEXADO", usuario=usuario, force=True)
    return True, "Producto anexado correctamente. Queda disponible para crear lista de picking.", item_id


def confirmar_producto_anexado(lote_id: int, item_id: int, tipo: str, usuario: str, comentario: str = "") -> tuple[bool, str]:
    tipo = clean_text(tipo).lower()
    usuario = clean_text(usuario) or "SIN_USUARIO"
    comentario = clean_text(comentario)
    if tipo not in {"ml", "kame"}:
        return False, "Tipo de confirmación inválido."
    now = now_cl().isoformat(timespec="seconds")
    col_prefix = "anexo_ml" if tipo == "ml" else "anexo_kame"
    with db() as c:
        row = c.execute("SELECT * FROM items WHERE lote_id=? AND id=? AND UPPER(COALESCE(origen_item,''))='ANEXO_MANUAL'", (int(lote_id), int(item_id))).fetchone()
        if not row:
            return False, "No encontré este producto anexado en el lote activo."
        c.execute(
            f"""
            UPDATE items
            SET {col_prefix}_confirmado=1,
                {col_prefix}_confirmado_at=?,
                {col_prefix}_confirmado_by=?,
                {col_prefix}_confirmado_comment=?,
                updated_at=?
            WHERE lote_id=? AND id=?
            """,
            (now, usuario, comentario, now, int(lote_id), int(item_id)),
        )
        c.commit()

    event_type = "producto_anexado_ml_confirmado" if tipo == "ml" else "producto_anexado_kame_confirmado"
    audit_type = "ANEXO_ML_CONFIRMADO" if tipo == "ml" else "ANEXO_KAME_CONFIRMADO"
    payload = build_item_event_payload(lote_id, item_id)
    payload.update({"created_at": now, "confirmado_at": now, "confirmado_by": usuario, "usuario": usuario, "comentario": comentario})
    enqueue_backup_event(event_type, payload)
    log_audit_event(lote_id, item_id=item_id, event_type=audit_type, detail=comentario or audit_type, qty=None, codigo_ml=payload.get("codigo_ml", ""), sku=payload.get("sku", ""), mode=usuario)
    queue_lote_snapshot_from_sqlite(lote_id, motivo=audit_type, usuario=usuario, force=True)
    return True, "Confirmación registrada."

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
        if int(item_id) in get_operationally_blocked_item_ids(lote_id):
            return False, "Este producto está retirado/bloqueado por aviso operacional activo. No se permite escanearlo."
        unidades_objetivo = get_effective_item_units(lote_id, item_id, int(item["unidades"] or 0))
        pendiente = int(unidades_objetivo or 0) - int(item["acopiadas"])
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
             operador_validador, picking_list_id, picking_code, picker_asignado,
             original_item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, maestro_match_status, restore_match_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lote_id, item_id, norm_code(scan_primario), norm_code(scan_secundario), cantidad, modo, now,
            clean_text(operador_validador) or "SIN_USUARIO",
            int(pick_id),
            clean_text(picking_meta.get("codigo_lista", "")),
            clean_text(picking_meta.get("asignado_a", "")),
            int(item_id),
            norm_code(item["codigo_ml"]),
            norm_code(item["codigo_universal"]),
            norm_code(item["sku"]),
            descripcion_operativa_value(item),
            descripcion_operativa_value(item),
            descripcion_etiqueta_value(item),
            clean_text(item["familia_kame"] if "familia_kame" in item.keys() else ""),
            clean_text(item["maestro_match_status"] if "maestro_match_status" in item.keys() else ""),
            "LIVE_MATCH",
        ))
        c.commit()

    enqueue_backup_event("scan_agregado", {
        **build_lote_payload(lote_id),
        "item_id": int(item_id),
        "sku": clean_text(item["sku"]),
        "codigo_ml": clean_text(item["codigo_ml"]),
        "codigo_universal": clean_text(item["codigo_universal"]),
        "descripcion": descripcion_operativa_value(item),
        "descripcion_kame": descripcion_operativa_value(item),
        "descripcion_ml": descripcion_etiqueta_value(item),
        "familia_kame": clean_text(item["familia_kame"] if "familia_kame" in item.keys() else ""),
        "maestro_match_status": clean_text(item["maestro_match_status"] if "maestro_match_status" in item.keys() else ""),
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
    log_audit_event(lote_id, item_id, "SKU_ESCANEADO", descripcion_operativa_value(item), int(cantidad), item["codigo_ml"], item["sku"], modo)
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
        "descripcion": descripcion_operativa_value(item_payload),
        "descripcion_kame": descripcion_operativa_value(item_payload),
        "descripcion_ml": descripcion_etiqueta_value(item_payload),
        "familia_kame": clean_text(item_payload.get("familia_kame", "")),
        "maestro_match_status": clean_text(item_payload.get("maestro_match_status", "")),
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
    log_audit_event(lote_id, int(row["item_id"]), "SCAN_DESHECHO", descripcion_operativa_value(item_payload), int(row["cantidad"]), item_payload.get("codigo_ml", ""), item_payload.get("sku", ""), row["modo"])
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
    descripcion_ml_col = col_exact(cols, ["Descripción ML", "Descripcion ML", "Título ML", "Titulo ML", "Producto ML", "Descripcion Mercado Libre", "Descripción Mercado Libre"])
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
        "descripcion_ml": raw[descripcion_ml_col] if descripcion_ml_col else raw[descripcion_col],
        "unidades": raw[unidades_col],
        "identificacion": raw[identificacion_col] if identificacion_col else "",
        "vence": raw[vence_col] if vence_col else "",
        "instrucciones": raw[instrucciones_col] if instrucciones_col else "",
        "dia": raw[dia_col] if dia_col else "",
        "hora": raw[hora_col] if hora_col else "",
    })

    for k in ["area", "nro", "descripcion", "descripcion_ml", "identificacion", "vence", "instrucciones", "dia", "hora"]:
        if k in df.columns:
            df[k] = df[k].map(clean_text)
    for k in ["codigo_ml", "codigo_universal", "sku"]:
        df[k] = df[k].map(norm_code)
    df["unidades"] = df["unidades"].map(to_int)
    df = apply_kame_description_fields(df)

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


def row_get_value(row, key, default=""):
    """Lee valores desde dict, sqlite Row, pandas Series o namedtuple."""
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        if hasattr(row, "get"):
            return row.get(key, default)
        if hasattr(row, key):
            return getattr(row, key)
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
    except Exception:
        pass
    return default


def descripcion_operativa_value(row) -> str:
    """Descripción oficial para operación: Kame; fallback a descripcion."""
    return clean_text(row_get_value(row, "descripcion_kame", "")) or clean_text(row_get_value(row, "descripcion", ""))


def descripcion_etiqueta_value(row) -> str:
    """Descripción oficial para etiquetas: ML; fallback a Kame/descripcion."""
    return (
        clean_text(row_get_value(row, "descripcion_ml", ""))
        or clean_text(row_get_value(row, "descripcion_label", ""))
        or clean_text(row_get_value(row, "descripcion", ""))
        or clean_text(row_get_value(row, "descripcion_kame", ""))
    )


def apply_kame_description_fields(df: pd.DataFrame, master_source=None) -> pd.DataFrame:
    """Normaliza descripciones sin perder la descripción ML.

    Regla matriz:
    - descripcion / descripcion_kame: idioma Kame para picking, escaneo y reserva.
    - descripcion_ml: título original ML para etiquetas.
    - Sheets debe recibir ambas para restaurar sin depender del maestro futuro.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "descripcion_ml" not in out.columns:
        out["descripcion_ml"] = out["descripcion"] if "descripcion" in out.columns else ""
    if "descripcion" not in out.columns:
        out["descripcion"] = out["descripcion_ml"]

    desc_map, family_map, barcode_map, _ = load_kame_master_maps(master_source)
    desc_kame_list = []
    fuente_list = []
    familia_list = []
    status_list = []
    codigo_universal_list = []
    for _, r in out.iterrows():
        sku = norm_code(r.get("sku", ""))
        desc_ml = clean_text(r.get("descripcion_ml", "")) or clean_text(r.get("descripcion", ""))
        desc_kame = clean_text(desc_map.get(sku, "")) if sku else ""
        familia = clean_text(family_map.get(sku, "")) if sku else ""
        if desc_kame:
            desc_final = desc_kame
            fuente = "KAME"
            status = "MATCH_SKU"
        else:
            desc_final = clean_text(r.get("descripcion", "")) or desc_ml
            fuente = "ML_FALLBACK"
            status = "SKU_NO_ENCONTRADO" if sku else "SKU_VACIO"
        desc_kame_list.append(desc_final)
        fuente_list.append(fuente)
        familia_list.append(familia)
        status_list.append(status)

        cu = normalize_universal_code(r.get("codigo_universal", ""))
        master_barcode = normalize_universal_code(barcode_map.get(sku, "")) if sku else "N/A"
        if cu == "N/A" and master_barcode != "N/A":
            codigo_universal_list.append(master_barcode)
        else:
            codigo_universal_list.append(cu)

    out["descripcion_ml"] = out["descripcion_ml"].map(clean_text)
    out["descripcion_kame"] = desc_kame_list
    out["descripcion"] = desc_kame_list
    out["descripcion_fuente"] = fuente_list
    out["familia_kame"] = familia_list
    out["maestro_match_status"] = status_list
    if "codigo_universal" in out.columns:
        out["codigo_universal"] = codigo_universal_list
    return out


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
        maestro_status = "MATCH_SKU" if desc_kame else ("SKU_NO_ENCONTRADO" if sku else "SKU_VACIO")
        descripcion_fuente = "KAME" if desc_kame else "ML_FALLBACK"
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
        # Regla estricta: no basta con que exista cualquier instrucción de preparación.
        # Solo marcamos Vence=SI cuando aparece la instrucción específica de vencimiento
        # del PDF ML: fecha de vencimiento impresa y vigencia mayor a 90 días.
        vence = "SI" if (
            "fecha de vencimiento debe estar impresa" in instr_norm
            and "90 dias" in instr_norm
        ) else ""

        rows.append({
            "area": "",
            "nro": str(idx + 1),
            "codigo_ml": norm_code(r.get("codigo_ml", "")),
            "codigo_universal": codigo_universal,
            "sku": sku,
            "descripcion": desc_final,
            "descripcion_kame": desc_final,
            "descripcion_fuente": descripcion_fuente,
            "maestro_match_status": maestro_status,
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
        ("descripcion_kame", "Descripción Kame"),
        ("descripcion_fuente", "Fuente Descripción"),
        ("maestro_match_status", "Estado Maestro"),
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
    qty = int(copies if copies is not None else row_get_value(row, "unidades", 0))
    qty = max(1, qty)
    # Regla matriz: las etiquetas usan descripción ML, no descripción Kame.
    desc_label = descripcion_etiqueta_value(row)
    return (
        zpl_separator_50x30("INICIO", row_get_value(row, "codigo_ml", ""), row_get_value(row, "sku", ""), desc_label)
        + zpl_ml_label_50x30(row_get_value(row, "codigo_ml", ""), row_get_value(row, "sku", ""), desc_label, qty)
        + zpl_separator_50x30("FIN", row_get_value(row, "codigo_ml", ""), row_get_value(row, "sku", ""), desc_label)
    )


def zpl_for_item_with_separators_exact_pq(row, copies=None) -> str:
    """Genera ZPL individual asegurando que la etiqueta normal salga con ^PQ exacto.

    Es una protección contra estado viejo de UI o fallbacks internos: la lógica sigue siendo
    INICIO + etiqueta normal + FIN, solo se fuerza el ^PQ de la etiqueta normal al valor elegido.
    """
    qty = max(1, int(copies or 1))
    zpl = zpl_for_item_with_separators(row, qty)
    return re.sub(r"\^PQ\d+", f"^PQ{qty}", zpl, count=1)


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


def parse_event_datetime_for_compare(value):
    """Parsea fechas operativas para comparar eventos restaurados desde Sheets/SQLite."""
    s = clean_text(value)
    if not s:
        return None
    try:
        raw = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CHILE_TZ)
        return dt.astimezone(CHILE_TZ)
    except Exception:
        pass
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        if getattr(dt, "tzinfo", None) is None:
            return dt.to_pydatetime().replace(tzinfo=CHILE_TZ)
        return dt.to_pydatetime().astimezone(CHILE_TZ)
    except Exception:
        return None


def get_historical_block_label_coverage(lote_id: int) -> dict:
    """Detecta si el lote ya fue cubierto por impresiones históricas por bloque.

    Aunque el flujo operativo actual es por lista de picking, los lotes impresos antes
    de la migración tienen eventos BLOQUE sin picking_code. Para no mostrarlos como
    pendientes falsos, se usa una cobertura conservadora:
    - toma el último registro por block_index;
    - suma sus etiquetas normales;
    - solo declara cobertura si alcanza las unidades actuales del lote.
    """
    lote_id = int(lote_id)
    with db() as c:
        total_row = c.execute("SELECT COALESCE(SUM(unidades),0) AS total FROM items WHERE lote_id=?", (lote_id,)).fetchone()
        required_total = int(total_row["total"] or 0) if total_row else 0
        blocks = pd.read_sql_query(
            """
            SELECT block_index, block_key, normal_qty, total_qty, last_printed_at, created_at, updated_at, status
            FROM label_blocks
            WHERE lote_id=?
            """,
            c,
            params=(lote_id,),
        )
        prints = pd.read_sql_query(
            """
            SELECT block_index, block_key,
                   SUM(CASE WHEN print_kind='NORMAL' THEN cantidad ELSE 0 END) AS normal_qty,
                   SUM(cantidad) AS total_qty,
                   MAX(created_at) AS last_printed_at
            FROM label_prints
            WHERE lote_id=? AND print_scope='BLOQUE' AND block_index IS NOT NULL
            GROUP BY block_index, block_key
            """,
            c,
            params=(lote_id,),
        )

    frames = []
    if not blocks.empty:
        b = blocks.copy()
        b["normal_qty"] = b["normal_qty"].fillna(0).astype(int)
        b["last_printed_at"] = b["last_printed_at"].fillna(b.get("updated_at", "")).fillna(b.get("created_at", ""))
        frames.append(b[["block_index", "block_key", "normal_qty", "total_qty", "last_printed_at"]])
    if not prints.empty:
        p = prints.copy()
        p["normal_qty"] = p["normal_qty"].fillna(0).astype(int)
        frames.append(p[["block_index", "block_key", "normal_qty", "total_qty", "last_printed_at"]])

    if not frames or required_total <= 0:
        return {"covered": False, "required_total": required_total, "normal_total": 0, "latest_at": "", "latest_dt": None, "blocks_count": 0}

    all_blocks = pd.concat(frames, ignore_index=True)
    all_blocks["block_index_int"] = all_blocks["block_index"].map(to_int)
    all_blocks = all_blocks[all_blocks["block_index_int"] > 0].copy()
    if all_blocks.empty:
        return {"covered": False, "required_total": required_total, "normal_total": 0, "latest_at": "", "latest_dt": None, "blocks_count": 0}

    all_blocks["_dt"] = all_blocks["last_printed_at"].map(parse_event_datetime_for_compare)
    all_blocks["_dt_sort"] = all_blocks["_dt"].map(lambda d: d.timestamp() if d else 0)
    latest_per_index = all_blocks.sort_values(["block_index_int", "_dt_sort"]).groupby("block_index_int", as_index=False).tail(1)
    normal_total = int(latest_per_index["normal_qty"].fillna(0).astype(int).sum())
    latest_dt = None
    for d in latest_per_index["_dt"].tolist():
        if d and (latest_dt is None or d > latest_dt):
            latest_dt = d
    latest_at = latest_dt.isoformat(timespec="seconds") if latest_dt else clean_text(latest_per_index["last_printed_at"].max())

    # Cobertura conservadora: debe alcanzar el total actual del lote.
    covered = normal_total >= required_total
    return {
        "covered": bool(covered),
        "required_total": int(required_total),
        "normal_total": int(normal_total),
        "latest_at": latest_at,
        "latest_dt": latest_dt,
        "blocks_count": int(latest_per_index["block_index_int"].nunique()),
    }


def item_is_covered_by_historical_blocks(item_row: dict, coverage: dict) -> bool:
    if not coverage or not coverage.get("covered"):
        return False
    latest_dt = coverage.get("latest_dt")
    if not latest_dt:
        return True
    item_dt = parse_event_datetime_for_compare(row_get_value(item_row, "created_at", ""))
    if not item_dt:
        return True
    return item_dt <= latest_dt


def picking_list_is_covered_by_historical_blocks(lote_id: int, picking_list_id: int, required: int = 0, coverage: dict | None = None) -> bool:
    coverage = coverage or get_historical_block_label_coverage(lote_id)
    if not coverage or not coverage.get("covered"):
        return False
    latest_dt = coverage.get("latest_dt")
    if not latest_dt:
        return True
    meta = get_picking_list_meta(int(picking_list_id))
    list_dt = parse_event_datetime_for_compare(meta.get("created_at", ""))
    if not list_dt:
        return True
    return list_dt <= latest_dt


def label_control_view(lote_id: int) -> pd.DataFrame:
    # Etiquetas/reimpresión no deben ocultar productos por avisos de ajuste.
    # Se usa una base dedicada: incluye ajustes +10/-10 con cantidad vigente,
    # excluyendo solo retirados/bloqueados o cantidad objetivo 0.
    items = get_label_reprint_items(lote_id)
    if items.empty:
        return items
    summary = get_label_print_summary(lote_id)
    view = items.merge(summary, left_on="id", right_on="item_id", how="left")
    for col in ["printed_normal", "printed_separators", "reprinted_qty"]:
        view[col] = view[col].fillna(0).astype(int)

    # Puente seguro para lotes históricos impresos por BLOQUE antes de migrar a etiquetas por picking.
    # Si el lote completo quedó cubierto por bloques y el producto existía antes de esa impresión,
    # el control individual no debe mostrarlo falsamente como SIN IMPRIMIR.
    coverage = get_historical_block_label_coverage(lote_id)
    if coverage.get("covered"):
        for idx, r in view.iterrows():
            req = int(r.get("unidades", 0) or 0)
            if req > 0 and int(view.at[idx, "printed_normal"] or 0) < req and item_is_covered_by_historical_blocks(r.to_dict(), coverage):
                view.at[idx, "printed_normal"] = req
                if not clean_text(view.at[idx, "last_label_printed_at"] if "last_label_printed_at" in view.columns else ""):
                    view.at[idx, "last_label_printed_at"] = clean_text(coverage.get("latest_at", ""))

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


def zpl_content_hash(zpl_content) -> str:
    """Huella SHA256 del ZPL generado para trazabilidad sin guardar el archivo completo en Sheets."""
    if isinstance(zpl_content, bytes):
        data = zpl_content
    else:
        data = clean_text(zpl_content).encode("utf-8")
    return hashlib.sha256(data).hexdigest() if data else ""


def register_zpl_label_event(
    lote_id: int,
    print_scope: str,
    print_kind: str,
    zpl_content,
    archivo_nombre: str = "",
    block_index=None,
    block_key: str = "",
    picking_list_id=None,
    picking_code: str = "",
    asignado_a: str = "",
    item_id=None,
    codigo_ml: str = "",
    sku: str = "",
    descripcion: str = "",
    productos_count: int = 0,
    cantidad_normal: int = 0,
    cantidad_separadores: int = 0,
    cantidad_total: int = 0,
    usuario: str = "",
):
    """Registra un evento único para impresiones ZPL de etiquetas.

    Este evento es la traza recuperable en Sheets para los tres modos:
    BLOQUE, PICKING e INDIVIDUAL. No guarda producto por producto; guarda el
    acto de generación/descarga del archivo con zpl_hash y totales.
    """
    lote = get_lote(lote_id)
    now = now_cl().isoformat(timespec="seconds")
    scope = clean_text(print_scope).upper() or "BLOQUE"
    kind = clean_text(print_kind).upper() or "NORMAL"
    enqueue_backup_event("zpl_etiquetas_generado", {
        "lote_id": int(lote_id),
        "lote_nombre": clean_text(lote.get("nombre", "")),
        "archivo": clean_text(lote.get("archivo", "")),
        "hoja": clean_text(lote.get("hoja", "")),
        "print_scope": scope,
        "print_kind": kind,
        "block_index": clean_text(block_index if block_index is not None else ""),
        "block_key": clean_text(block_key),
        "picking_list_id": clean_text(picking_list_id if picking_list_id is not None else ""),
        "picking_code": clean_text(picking_code),
        "asignado_a": clean_text(asignado_a),
        "item_id": clean_text(item_id if item_id is not None else ""),
        "codigo_ml": norm_code(codigo_ml),
        "sku": norm_code(sku),
        "descripcion": clean_text(descripcion),
        "productos_count": int(productos_count or 0),
        "cantidad_normal": int(cantidad_normal or 0),
        "cantidad_separadores": int(cantidad_separadores or 0),
        "cantidad_total": int(cantidad_total or 0),
        "zpl_hash": zpl_content_hash(zpl_content),
        "archivo_nombre": clean_text(archivo_nombre),
        "usuario": clean_text(usuario) or get_operator_name(),
        "created_at": now,
        "tipo": "ETIQUETAS",
        "modo": scope,
        "comentario": f"{scope} · {kind} · {clean_text(archivo_nombre)}",
    })


def get_label_block_record(lote_id: int, block_index: int, block_key: str) -> dict:
    """Obtiene el registro de bloque impreso.

    Primero busca por key exacta. Si no encuentra, cae a block_index para
    soportar rescates desde Sheets donde el block_key se recalcula distinto.
    """
    with db() as c:
        row = c.execute(
            "SELECT * FROM label_blocks WHERE lote_id=? AND block_index=? AND block_key=?",
            (int(lote_id), int(block_index), clean_text(block_key)),
        ).fetchone()
        if row:
            return dict(row)
        row = c.execute(
            """
            SELECT * FROM label_blocks
            WHERE lote_id=? AND block_index=?
            ORDER BY last_printed_at DESC, id DESC
            LIMIT 1
            """,
            (int(lote_id), int(block_index)),
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
                descripcion_etiqueta_value(item), int(item.get("unidades", 0)), "BLOQUE", "NORMAL",
                int(block["block_index"]), clean_text(block["block_key"]), is_reprint, now,
            ))
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                descripcion_etiqueta_value(item), LABEL_SEPARATOR_PER_PRODUCT, "BLOQUE", "SEPARADOR",
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
    zpl_content = zpl_for_block(block)
    register_zpl_label_event(
        lote_id,
        print_scope="BLOQUE",
        print_kind="REIMPRESION" if is_reprint else "NORMAL",
        zpl_content=zpl_content,
        archivo_nombre=f"etiquetas_lote_{int(lote_id)}_bloque_{int(block['block_index'])}.zpl",
        block_index=int(block["block_index"]),
        block_key=clean_text(block["block_key"]),
        productos_count=int(block.get("products_count", 0)),
        cantidad_normal=int(block.get("normal_qty", 0)),
        cantidad_separadores=int(block.get("separator_qty", 0)),
        cantidad_total=int(block.get("total_qty", 0)),
        usuario=get_operator_name(),
    )
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
             descripcion_etiqueta_value(item), qty, "INDIVIDUAL", "NORMAL", None, None, is_reprint, now),
            (int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
             descripcion_etiqueta_value(item), LABEL_SEPARATOR_PER_PRODUCT, "INDIVIDUAL", "SEPARADOR", None, None, is_reprint, now),
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
    zpl_content = zpl_for_item_with_separators_exact_pq(item, qty)
    register_zpl_label_event(
        lote_id,
        print_scope="INDIVIDUAL",
        print_kind="REIMPRESION" if is_reprint else "NORMAL",
        zpl_content=zpl_content,
        archivo_nombre=f"etiqueta_lote_{int(lote_id)}_{norm_code(item.get('codigo_ml','')) or norm_code(item.get('sku',''))}.zpl",
        item_id=int(item.get("id")),
        codigo_ml=item.get("codigo_ml", ""),
        sku=item.get("sku", ""),
        descripcion=descripcion_etiqueta_value(item),
        # Kame/ML quedan en raw_json si aplica por item en eventos futuros; el texto visible de etiqueta es ML.
        productos_count=1,
        cantidad_normal=int(qty),
        cantidad_separadores=LABEL_SEPARATOR_PER_PRODUCT,
        cantidad_total=int(qty) + LABEL_SEPARATOR_PER_PRODUCT,
        usuario=get_operator_name(),
    )
    log_audit_event(lote_id, int(item.get("id")), "ZPL_INDIVIDUAL", descripcion_etiqueta_value(item), int(qty), item.get("codigo_ml", ""), item.get("sku", ""), "INDIVIDUAL")


def build_picking_label_block(picking_list_id: int) -> dict:
    """Construye un bloque ZPL desde una lista de picking.

    Usa la cantidad de la lista de picking, no la cantidad total del FULL.
    Esto permite imprimir etiquetas solo para los productos asignados a esa lista.
    """
    meta = get_picking_list_meta(picking_list_id)
    items_df = get_picking_items(picking_list_id)
    items = []
    if not items_df.empty:
        for _, r in items_df.iterrows():
            qty = max(0, to_int(r.get("cantidad", 0)))
            if qty <= 0:
                continue
            items.append({
                "id": to_int(r.get("item_id", r.get("id", 0))),
                "codigo_ml": norm_code(r.get("codigo_ml", "")),
                "codigo_universal": norm_code(r.get("codigo_universal", "")),
                "sku": norm_code(r.get("sku", "")),
                # El título de etiqueta se lee desde items (fuente canónica) antes que
                # desde el snapshot de la lista. Así una lista antigua no puede arrastrar
                # una descripción Kame ni un título ML vacío a la impresión.
                "descripcion": clean_text(r.get("descripcion_kame_item", "")) or clean_text(r.get("descripcion_kame", "")) or clean_text(r.get("descripcion", "")),
                "descripcion_kame": clean_text(r.get("descripcion_kame_item", "")) or clean_text(r.get("descripcion_kame", "")) or clean_text(r.get("descripcion", "")),
                "descripcion_ml": clean_text(r.get("descripcion_ml_item", "")) or clean_text(r.get("descripcion_ml", "")),
                "familia_kame": clean_text(r.get("familia_kame", "")),
                "maestro_match_status": clean_text(r.get("maestro_match_status", "")),
                "unidades": qty,
                "area": clean_text(r.get("area", "")),
                "nro": clean_text(r.get("nro", "")),
            })
    normal = sum(int(x.get("unidades", 0)) for x in items)
    separators = len(items) * LABEL_SEPARATOR_PER_PRODUCT
    key_raw = "|".join(f"{int(x.get('id',0))}:{int(x.get('unidades',0))}" for x in items)
    base_key = f"PICKING:{int(picking_list_id)}:{clean_text(meta.get('codigo_lista',''))}:{key_raw}"
    block_key = "PICK-" + hashlib.sha1(base_key.encode("utf-8")).hexdigest()[:12]
    return {
        "picking_list_id": int(picking_list_id),
        "picking_code": clean_text(meta.get("codigo_lista", "")),
        "asignado_a": clean_text(meta.get("asignado_a", "")),
        "estado": clean_text(meta.get("estado", "")),
        "block_key": block_key,
        "items": items,
        "products_count": len(items),
        "normal_qty": int(normal),
        "separator_qty": int(separators),
        "total_qty": int(normal + separators),
    }


def get_picking_label_print_count(lote_id: int, picking_list_id: int, block_key: str = "") -> int:
    """Retorna registros de impresión para una lista de picking.

    Primero busca por impresión directa por lista. Si no existe, reconoce cobertura
    histórica por BLOQUE para evitar que una lista ya cubierta aparezca como pendiente
    tras la migración del módulo de etiquetas.
    """
    with db() as c:
        row = c.execute(
            """
            SELECT COUNT(*) AS n
            FROM label_prints
            WHERE lote_id=? AND print_scope='PICKING' AND block_index=? AND block_key=?
            """,
            (int(lote_id), int(picking_list_id), clean_text(block_key)),
        ).fetchone()
        n = int(row["n"] or 0) if row else 0
        if n > 0:
            return n
        row = c.execute(
            """
            SELECT COUNT(*) AS n
            FROM label_prints
            WHERE lote_id=? AND print_scope='PICKING' AND block_index=?
            """,
            (int(lote_id), int(picking_list_id)),
        ).fetchone()
        n = int(row["n"] or 0) if row else 0
        if n > 0:
            return n

    if picking_list_is_covered_by_historical_blocks(int(lote_id), int(picking_list_id)):
        return 1
    return 0


def get_picking_label_print_summary(lote_id: int) -> pd.DataFrame:
    with db() as c:
        df = pd.read_sql_query(
            """
            SELECT
                block_index AS picking_list_id,
                SUM(CASE WHEN print_kind='NORMAL' THEN cantidad ELSE 0 END) AS printed_normal,
                SUM(CASE WHEN print_kind='SEPARADOR' THEN cantidad ELSE 0 END) AS printed_separators,
                SUM(CASE WHEN is_reprint=1 AND print_kind='NORMAL' THEN cantidad ELSE 0 END) AS reprinted_normal,
                MAX(created_at) AS last_label_printed_at,
                COUNT(*) AS label_rows
            FROM label_prints
            WHERE lote_id=? AND print_scope='PICKING' AND block_index IS NOT NULL
            GROUP BY block_index
            """,
            c,
            params=(int(lote_id),),
        )
    if df.empty:
        return pd.DataFrame(columns=["picking_list_id", "printed_normal", "printed_separators", "reprinted_normal", "last_label_printed_at", "label_rows"])
    for col in ["picking_list_id", "printed_normal", "printed_separators", "reprinted_normal", "label_rows"]:
        df[col] = df[col].fillna(0).astype(int)
    return df


def get_picking_label_status_df(lote_id: int) -> pd.DataFrame:
    picking_df = get_picking_lists(lote_id)
    cols = [
        "picking_list_id", "codigo_lista", "asignado_a", "estado_lista", "productos", "etiquetas_requeridas",
        "separadores", "total_zpl", "etiquetas_impresas", "reimpresas", "estado_etiquetas", "origen_impresion", "ultima_impresion"
    ]
    if picking_df.empty:
        return pd.DataFrame(columns=cols)
    picking_df = picking_df[picking_df["estado"].astype(str).str.upper() != "ANULADA"].copy()
    if picking_df.empty:
        return pd.DataFrame(columns=cols)
    summary = get_picking_label_print_summary(lote_id)
    summary_map = {int(r["picking_list_id"]): r.to_dict() for _, r in summary.iterrows()} if not summary.empty else {}
    coverage = get_historical_block_label_coverage(lote_id)
    rows = []
    for _, pl in picking_df.sort_values("id", ascending=False).iterrows():
        pid = int(pl["id"])
        block = build_picking_label_block(pid)
        srow = summary_map.get(pid, {})
        printed = int(srow.get("printed_normal", 0) or 0)
        reprinted = int(srow.get("reprinted_normal", 0) or 0)
        required = int(block.get("normal_qty", 0) or 0)
        origen = "PICKING" if printed > 0 else ""
        ultima = clean_text(srow.get("last_label_printed_at", ""))

        # Puente de migración: si una lista existía cuando el lote fue cubierto por impresión
        # histórica por BLOQUE, no debe quedar marcada como pendiente al cambiar el flujo a picking.
        if printed <= 0 and required > 0 and picking_list_is_covered_by_historical_blocks(lote_id, pid, required, coverage):
            printed = required
            origen = "BLOQUE HISTÓRICO"
            ultima = clean_text(coverage.get("latest_at", ""))

        if printed <= 0:
            estado_etq = "PENDIENTE"
        elif reprinted > 0:
            estado_etq = "REIMPRESA"
        else:
            estado_etq = "IMPRESA"
        rows.append({
            "picking_list_id": pid,
            "codigo_lista": clean_text(pl.get("codigo_lista", "")),
            "asignado_a": clean_text(pl.get("asignado_a", "")),
            "estado_lista": clean_text(pl.get("estado", "")),
            "productos": int(block.get("products_count", 0) or 0),
            "etiquetas_requeridas": required,
            "separadores": int(block.get("separator_qty", 0) or 0),
            "total_zpl": int(block.get("total_qty", 0) or 0),
            "etiquetas_impresas": printed,
            "reimpresas": reprinted,
            "estado_etiquetas": estado_etq,
            "origen_impresion": origen,
            "ultima_impresion": ultima,
        })
    return pd.DataFrame(rows, columns=cols)


def register_picking_label_download(lote_id: int, picking_list_id: int, block: dict, motivo: str = "", usuario: str = ""):
    """Registra una descarga ZPL generada desde lista de picking.

    Regla de producción:
    - primera descarga = impresión NORMAL automática;
    - toda descarga posterior = REIMPRESIÓN controlada con motivo;
    - no existe marcado manual como impreso.
    """
    if is_lote_closed(lote_id):
        st.error("Este lote está cerrado. Reabre el lote desde Supervisor antes de imprimir etiquetas.")
        return
    now = now_cl().isoformat(timespec="seconds")
    picking_id = int(picking_list_id)
    block_key = clean_text(block.get("block_key", ""))
    is_reprint = 1 if get_picking_label_print_count(lote_id, picking_id, block_key) > 0 else 0
    motivo = clean_text(motivo)
    usuario = clean_text(usuario) or get_operator_name()
    if is_reprint and not motivo:
        st.warning("Para reimprimir una lista ya impresa debes indicar motivo obligatorio.")
        return
    items = block.get("items") or []
    if not items:
        st.warning("La lista de picking no tiene productos para imprimir.")
        return

    with db() as c:
        rows = []
        for item in items:
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                descripcion_etiqueta_value(item), int(item.get("unidades", 0)), "PICKING", "NORMAL",
                picking_id, block_key, is_reprint, now,
            ))
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                descripcion_etiqueta_value(item), LABEL_SEPARATOR_PER_PRODUCT, "PICKING", "SEPARADOR",
                picking_id, block_key, is_reprint, now,
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
        if is_reprint:
            c.execute(
                """
                INSERT INTO reimpresiones
                (lote_id, item_id, block_index, block_key, scope, cantidad, motivo, usuario, created_at)
                VALUES (?, NULL, ?, ?, 'PICKING_LISTA', ?, ?, ?, ?)
                """,
                (int(lote_id), picking_id, block_key, int(block.get("total_qty", 0)), motivo, usuario, now),
            )
        c.commit()

    zpl_content = zpl_for_block(block)
    register_zpl_label_event(
        lote_id,
        print_scope="PICKING",
        print_kind="REIMPRESION" if is_reprint else "NORMAL",
        zpl_content=zpl_content,
        archivo_nombre=f"etiquetas_lote_{int(lote_id)}_{clean_text(block.get('picking_code','PICKING')).replace(' ', '_')}.zpl",
        block_index=picking_id,
        block_key=block_key,
        picking_list_id=picking_id,
        picking_code=clean_text(block.get("picking_code", "")),
        asignado_a=clean_text(block.get("asignado_a", "")),
        productos_count=int(block.get("products_count", 0)),
        cantidad_normal=int(block.get("normal_qty", 0)),
        cantidad_separadores=int(block.get("separator_qty", 0)),
        cantidad_total=int(block.get("total_qty", 0)),
        usuario=usuario,
    )
    event_name = "ZPL_PICKING_REIMPRESO" if is_reprint else "ZPL_PICKING_DESCARGADO"
    detail = f"Lista {clean_text(block.get('picking_code','')) or picking_id} · {int(block.get('products_count',0))} productos"
    if is_reprint and motivo:
        detail += f" · Motivo: {motivo}"
    log_audit_event(lote_id, event_type=event_name, detail=detail, qty=int(block.get("total_qty", 0)), mode="PICKING")


def register_picking_item_label_reprint(lote_id: int, picking_list_id: int, item: dict, qty: int, motivo: str, usuario: str = ""):
    """Reimprime un producto específico dentro de una lista de picking.

    Queda asociado a la lista, al picker y al producto. Es reposición controlada,
    no impresión normal abierta.
    """
    if is_lote_closed(lote_id):
        st.error("Este lote está cerrado. Reabre el lote desde Supervisor antes de reimprimir etiquetas.")
        return
    motivo = clean_text(motivo)
    usuario = clean_text(usuario) or get_operator_name()
    if not motivo:
        st.warning("Para reimprimir un producto debes indicar motivo obligatorio.")
        return
    qty = max(1, int(qty or 1))
    picking_id = int(picking_list_id)
    meta = get_picking_list_meta(picking_id)
    now = now_cl().isoformat(timespec="seconds")
    item_id = int(item.get("id") or item.get("item_id") or 0)
    block_key = f"PICK-ITEM-{picking_id}-{item_id}-{hashlib.sha1((motivo + now).encode('utf-8')).hexdigest()[:8]}"
    desc_label = descripcion_etiqueta_value(item)

    with db() as c:
        rows = [
            (int(lote_id), item_id, norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
             desc_label, qty, "PICKING", "NORMAL", picking_id, block_key, 1, now),
            (int(lote_id), item_id, norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
             desc_label, LABEL_SEPARATOR_PER_PRODUCT, "PICKING", "SEPARADOR", picking_id, block_key, 1, now),
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
        c.execute(
            """
            INSERT INTO reimpresiones
            (lote_id, item_id, block_index, block_key, scope, cantidad, motivo, usuario, created_at)
            VALUES (?, ?, ?, ?, 'PICKING_PRODUCTO', ?, ?, ?, ?)
            """,
            (int(lote_id), item_id, picking_id, block_key, qty, motivo, usuario, now),
        )
        c.commit()

    zpl_content = zpl_for_item_with_separators_exact_pq(item, qty)
    register_zpl_label_event(
        lote_id,
        print_scope="PICKING",
        print_kind="REIMPRESION",
        zpl_content=zpl_content,
        archivo_nombre=f"reimpresion_{clean_text(meta.get('codigo_lista','PICKING')).replace(' ', '_')}_{norm_code(item.get('codigo_ml','')) or norm_code(item.get('sku',''))}.zpl",
        block_index=picking_id,
        block_key=block_key,
        picking_list_id=picking_id,
        picking_code=clean_text(meta.get("codigo_lista", "")),
        asignado_a=clean_text(meta.get("asignado_a", "")),
        item_id=item_id,
        codigo_ml=item.get("codigo_ml", ""),
        sku=item.get("sku", ""),
        descripcion=desc_label,
        productos_count=1,
        cantidad_normal=int(qty),
        cantidad_separadores=LABEL_SEPARATOR_PER_PRODUCT,
        cantidad_total=int(qty) + LABEL_SEPARATOR_PER_PRODUCT,
        usuario=usuario,
    )
    log_audit_event(
        lote_id,
        item_id,
        "ZPL_PICKING_PRODUCTO_REIMPRESO",
        f"Lista {clean_text(meta.get('codigo_lista','')) or picking_id} · {desc_label} · Motivo: {motivo}",
        int(qty),
        item.get("codigo_ml", ""),
        item.get("sku", ""),
        "PICKING",
    )

# ============================================================
# Auditoría operacional Fase 1
# ============================================================

def log_audit_event(lote_id=None, item_id=None, event_type="", detail="", qty=None, codigo_ml="", sku="", mode=""):
    """Registra auditoría local y exige respaldo externo en Sheets."""
    now = now_cl().isoformat(timespec="seconds")
    lote_id_clean = int(lote_id) if lote_id is not None else None
    item_id_clean = int(item_id) if item_id is not None else None
    event_type_clean = clean_text(event_type)
    detail_clean = clean_text(detail)
    qty_clean = int(qty) if qty is not None else None
    codigo_ml_clean = norm_code(codigo_ml)
    sku_clean = norm_code(sku)
    mode_clean = clean_text(mode)
    with db() as c:
        c.execute(
            """
            INSERT INTO audit_events
            (lote_id, item_id, event_type, detail, qty, codigo_ml, sku, mode, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lote_id_clean,
                item_id_clean,
                event_type_clean,
                detail_clean,
                qty_clean,
                codigo_ml_clean,
                sku_clean,
                mode_clean,
                now,
            ),
        )
        c.commit()

    payload = {
        "lote_id": lote_id_clean or "",
        "item_id": item_id_clean or "",
        "event_type_audit": event_type_clean,
        "detalle": detail_clean,
        "cantidad": qty_clean if qty_clean is not None else "",
        "codigo_ml": codigo_ml_clean,
        "sku": sku_clean,
        "modo": mode_clean,
        "created_at": now,
        "tipo": event_type_clean,
        "comentario": detail_clean,
    }
    if lote_id_clean:
        payload.update(build_lote_payload(lote_id_clean))
    enqueue_backup_event("audit_event", payload)

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
    scans = get_scans_deduped(lote_id, limit=max(int(limit), 8))
    if scans.empty:
        return pd.DataFrame(columns=["created_at", "descripcion", "codigo_ml", "sku", "cantidad", "modo", "operador_validador", "picking_code", "picker_asignado", "estado_rescate"])

    with db() as c:
        items = pd.read_sql_query(
            "SELECT id AS item_id, descripcion AS item_descripcion, codigo_ml AS item_codigo_ml, sku AS item_sku FROM items WHERE lote_id=?",
            c,
            params=(int(lote_id),),
        )
    out = scans.copy()
    if not items.empty:
        out = out.merge(items, on="item_id", how="left")
    else:
        out["item_descripcion"] = ""
        out["item_codigo_ml"] = ""
        out["item_sku"] = ""

    out["descripcion"] = out.apply(lambda r: clean_text(r.get("item_descripcion", "")) or clean_text(r.get("descripcion", "")), axis=1)
    out["codigo_ml"] = out.apply(lambda r: norm_code(r.get("item_codigo_ml", "")) or norm_code(r.get("codigo_ml", "")), axis=1)
    out["sku"] = out.apply(lambda r: norm_code(r.get("item_sku", "")) or norm_code(r.get("sku", "")), axis=1)
    out["estado_rescate"] = out["restore_match_status"].map(lambda x: "ACOPIO_RECUPERADO_SHEETS" if clean_text(x) == "NO_MATCH_SNAPSHOT" else clean_text(x))
    cols = ["created_at", "descripcion", "codigo_ml", "sku", "cantidad", "modo", "operador_validador", "picking_code", "picker_asignado", "estado_rescate"]
    return out[cols].head(int(limit)).reset_index(drop=True)


def render_scan_incident_button(lote_id: int, items: pd.DataFrame, current_item=None):
    """Incidencias creadas desde Escaneo por código real del producto.

    Importante producción/PDA:
    - No usamos st.form aquí. Muchos lectores de código envían ENTER al escanear
      y eso puede disparar el submit del formulario antes de completar tipo/cantidad/comentario.
    - El código se puede escanear/escribir sin guardar la incidencia. Solo el botón
      "Guardar incidencia" ejecuta el registro.
    """
    default_code = ""
    if current_item is not None:
        try:
            default_code = norm_code(current_item.get("codigo_ml", "")) or norm_code(current_item.get("codigo_universal", "")) or norm_code(current_item.get("sku", ""))
        except Exception:
            default_code = ""

    if st.session_state.pop("scan_inc_reset", False):
        # Después de guardar una incidencia, limpiamos el formulario completo.
        # Ojo: si hay un producto escaneado en pantalla, default_code podría volver a rellenar
        # el código en el rerun y permitir guardar accidentalmente la misma incidencia dos veces.
        # Por eso guardamos el código de contexto que debe ignorarse hasta que cambie el producto.
        for k in ["scan_inc_codigo", "scan_inc_qty", "scan_inc_comentario"]:
            st.session_state.pop(k, None)
        st.session_state["scan_inc_tipo"] = INCIDENCIA_TIPOS[0]

    ignored_prefill_code = norm_code(st.session_state.get("scan_inc_ignore_prefill_code", ""))
    if default_code and norm_code(default_code) != ignored_prefill_code and not clean_text(st.session_state.get("scan_inc_codigo", "")):
        # Prefill seguro antes de crear el widget. No se vuelve a pisar si el operador ya escribió/escaneó otro código.
        st.session_state["scan_inc_codigo"] = default_code

    keep_open = bool(clean_text(st.session_state.get("scan_inc_codigo", ""))) or bool(clean_text(st.session_state.get("scan_inc_last_msg", "")))

    with st.expander("Reportar incidencia por código", expanded=keep_open):
        st.caption("Escanea o ingresa Etiqueta ML, Código Universal/EAN o SKU. Escanear el código NO guarda la incidencia; solo la registra el botón Guardar incidencia.")

        codigo_inc = st.text_input(
            "Etiqueta ML / Código Universal / SKU",
            key="scan_inc_codigo",
            placeholder="Escanea o escribe el código afectado",
        )
        c1, c2 = st.columns([2, 1])
        with c1:
            tipo_inc = st.selectbox("Tipo de incidencia", INCIDENCIA_TIPOS, key="scan_inc_tipo")
        with c2:
            qty_inc = st.number_input("Cantidad afectada", min_value=0, max_value=9999, value=0, step=1, key="scan_inc_qty")
        comentario_inc = st.text_area("Comentario", key="scan_inc_comentario", placeholder="Describe qué ocurrió: falta, daño, diferencia, etiqueta, mal embalaje, etc.")

        last_msg = clean_text(st.session_state.pop("scan_inc_last_msg", ""))
        if last_msg:
            st.success(last_msg)

        submit_inc = st.button("Guardar incidencia", type="primary", key="scan_inc_guardar_btn")

        if submit_inc:
            if not norm_code(codigo_inc):
                st.error("Ingresa o escanea el código afectado antes de guardar la incidencia.")
            else:
                ok_inc, msg_inc = create_incidencia_por_codigo(
                    lote_id,
                    codigo_inc,
                    tipo_inc,
                    int(qty_inc),
                    comentario_inc,
                    get_operator_name(),
                )
                if ok_inc:
                    st.session_state["scan_inc_last_msg"] = msg_inc
                    st.session_state["scan_inc_reset"] = True
                    # Evita que el mismo producto/código se vuelva a precargar después del rerun.
                    # El operador deberá escanear o escribir un código nuevo para crear otra incidencia.
                    st.session_state["scan_inc_ignore_prefill_code"] = norm_code(default_code) or norm_code(codigo_inc)
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
    """Usuario operativo actual.

    En PDA solo queda ERICK como validador autorizado.
    Se prefiere scan_operator porque es el selector visible en Escaneo;
    operator_name se conserva como compatibilidad para otros módulos antiguos.
    """
    op = clean_text(st.session_state.get("scan_operator", "")) or clean_text(st.session_state.get("operator_name", ""))
    op = op.upper()
    if op not in SCAN_OPERATORS:
        op = SCAN_OPERATORS[0]
    st.session_state["scan_operator"] = op
    st.session_state["operator_name"] = op
    return op


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



def _snapshot_codigo_incidencia(codigo_reportado: str, item: dict | None = None) -> dict:
    """Arma los datos visibles de una incidencia creada desde Escaneo.

    Regla operativa:
    - Si el código existe en el lote, la incidencia queda asociada al item.
    - Si el código NO existe o justamente no valida, igual debe guardarse como incidencia
      por código reportado. Esto permite reportar errores de etiqueta/EAN/SKU aunque el
      producto no pueda validarse en PDA.
    """
    item = item or {}
    codigo_norm = norm_code(codigo_reportado)
    if item:
        return {
            "codigo_ml": norm_code(item.get("codigo_ml", "")),
            "codigo_universal": norm_code(item.get("codigo_universal", "")),
            "sku": norm_code(item.get("sku", "")),
            "descripcion": clean_text(item.get("descripcion", "")),
            "codigo_reportado": codigo_norm,
            "match_status": "MATCH_ITEM",
        }

    # Sin match: se repite el código en las columnas principales para que sea visible
    # en Supervisor, exportación, auditoría y Sheets, sin depender de item_id.
    return {
        "codigo_ml": codigo_norm,
        "codigo_universal": codigo_norm,
        "sku": codigo_norm,
        "descripcion": f"Código reportado no asociado a producto del lote: {codigo_norm}" if codigo_norm else "Incidencia por código sin producto asociado",
        "codigo_reportado": codigo_norm,
        "match_status": "NO_MATCH_CODE",
    }


def create_incidencia(lote_id: int, item_id, tipo: str, cantidad: int, comentario: str, usuario: str, codigo_reportado: str = ""):
    """Crea una incidencia y su auditoría como una sola operación lógica.

    Importante para Escaneo:
    - No exige que item_id exista. Si el código no valida o no está en el lote, igual se guarda
      la incidencia con el código reportado para que Supervisor pueda resolverla.
    - Guarda incidencia y auditoría local antes de enviar a Sheets.
    - Envía ambos eventos juntos para que aparezcan en eventos, incidencias y auditoría.
    """
    if is_lote_closed(lote_id):
        return False, "Este lote está cerrado. Reabre el lote desde Supervisor antes de registrar incidencias.", {}

    tipo_clean = clean_text(tipo)
    comentario_clean = clean_text(comentario)
    usuario_clean = clean_text(usuario) or "SIN_USUARIO"
    qty_clean = max(0, int(cantidad or 0))
    if len(comentario_clean) < 3:
        return False, "Agrega un comentario mínimo para que la incidencia sea útil.", {}

    item = {}
    item_id_clean = None
    if item_id:
        try:
            item_id_clean = int(item_id)
        except Exception:
            item_id_clean = None
    if item_id_clean:
        with db() as c:
            row = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (item_id_clean, int(lote_id))).fetchone()
            item = dict(row) if row else {}
        if not item:
            item_id_clean = None

    snap = _snapshot_codigo_incidencia(codigo_reportado, item)
    now = now_cl().isoformat(timespec="seconds")
    detail_clean = f"{tipo_clean} · {comentario_clean}"
    if clean_text(snap.get("match_status")) == "NO_MATCH_CODE":
        detail_clean = f"{detail_clean} · Código reportado: {clean_text(snap.get('codigo_reportado'))} · SIN MATCH EN LOTE"

    with db() as c:
        cur = c.execute(
            """
            INSERT INTO incidencias
            (lote_id, item_id, tipo, cantidad, comentario, usuario, status, created_at,
             codigo_ml, codigo_universal, sku, descripcion)
            VALUES (?, ?, ?, ?, ?, ?, 'ABIERTA', ?, ?, ?, ?, ?)
            """,
            (
                int(lote_id),
                item_id_clean,
                tipo_clean,
                qty_clean,
                comentario_clean,
                usuario_clean,
                now,
                norm_code(snap.get("codigo_ml", "")),
                norm_code(snap.get("codigo_universal", "")),
                norm_code(snap.get("sku", "")),
                clean_text(snap.get("descripcion", "")),
            ),
        )
        incidencia_id = int(cur.lastrowid)
        c.execute(
            """
            INSERT INTO audit_events
            (lote_id, item_id, event_type, detail, qty, codigo_ml, sku, mode, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(lote_id),
                item_id_clean,
                "INCIDENCIA_ABIERTA",
                detail_clean,
                qty_clean,
                norm_code(snap.get("codigo_ml", "")),
                norm_code(snap.get("sku", "")),
                usuario_clean,
                now,
            ),
        )
        c.commit()

    lote_payload = build_lote_payload(lote_id)
    incidencia_payload = {
        **lote_payload,
        "incidencia_id": incidencia_id,
        "item_id": item_id_clean or "",
        "codigo_ml": norm_code(snap.get("codigo_ml", "")),
        "codigo_universal": norm_code(snap.get("codigo_universal", "")),
        "sku": norm_code(snap.get("sku", "")),
        "descripcion": clean_text(snap.get("descripcion", "")),
        "codigo_reportado": clean_text(snap.get("codigo_reportado", "")),
        "match_status": clean_text(snap.get("match_status", "")),
        "tipo": tipo_clean,
        "cantidad": qty_clean,
        "comentario": comentario_clean,
        "usuario": usuario_clean,
        "status": "ABIERTA",
        "estado": "ABIERTA",
        "created_at": now,
    }
    audit_payload = {
        **lote_payload,
        "item_id": item_id_clean or "",
        "event_type_audit": "INCIDENCIA_ABIERTA",
        "detail": detail_clean,
        "detalle": detail_clean,
        "cantidad": qty_clean,
        "codigo_ml": norm_code(snap.get("codigo_ml", "")),
        "sku": norm_code(snap.get("sku", "")),
        "modo": usuario_clean,
        "tipo": "INCIDENCIA_ABIERTA",
        "comentario": detail_clean,
        "created_at": now,
    }
    # En Escaneo la rapidez es crítica: la auditoría ya quedó guardada localmente
    # y Apps Script escribe la fila de auditoría estructurada desde este mismo evento.
    # Así evitamos hacer dos llamadas HTTP consecutivas al registrar una incidencia.
    enqueue_backup_event("incidencia_creada", incidencia_payload)
    return True, incidencia_id, snap


def create_incidencia_por_codigo(lote_id: int, codigo: str, tipo: str, cantidad: int, comentario: str, usuario: str = "SIN_USUARIO"):
    """Crea incidencia desde Escaneo usando Etiqueta ML / Código Universal / SKU.

    La búsqueda por código es independiente del producto escaneado. Si el código no calza
    con ningún producto del lote, igualmente se registra la incidencia como NO_MATCH_CODE.
    """
    codigo_norm = norm_code(codigo)
    if not codigo_norm:
        return False, "Ingresa una Etiqueta ML, Código Universal o SKU."
    item = find_item_for_incidencia(lote_id, codigo_norm)
    result = create_incidencia(
        lote_id,
        int(item["id"]) if item else None,
        tipo,
        int(cantidad or 0),
        comentario,
        usuario or "SIN_USUARIO",
        codigo_reportado=codigo_norm,
    )

    # Defensa: create_incidencia debe devolver (ok, incidencia_id_o_msg, snap),
    # pero esto evita pantalla roja si alguna rama antigua devuelve solo (ok, msg).
    if isinstance(result, tuple) and len(result) == 3:
        ok, incidencia_id, snap = result
    elif isinstance(result, tuple) and len(result) == 2:
        ok, incidencia_id = result
        snap = {}
    else:
        return False, "No se pudo registrar la incidencia: respuesta interna inválida."

    if not ok:
        return False, clean_text(incidencia_id) or "No se pudo registrar la incidencia."
    if item:
        return True, f"Incidencia #{incidencia_id} registrada para SKU {clean_text(item.get('sku',''))}."
    return True, f"Incidencia #{incidencia_id} registrada por código {codigo_norm} sin producto asociado. Supervisor debe revisar."


def resolve_incidencia(incidencia_id: int, usuario: str, comentario: str):
    usuario_clean = clean_text(usuario) or "SIN_USUARIO"
    comentario_clean = clean_text(comentario)
    if len(comentario_clean) < 3:
        return False, "Agrega un comentario de resolución."

    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        inc = c.execute("SELECT * FROM incidencias WHERE id=?", (int(incidencia_id),)).fetchone()
        if not inc:
            return False, "Incidencia no encontrada."
        if clean_text(inc["status"]) == "RESUELTA":
            return False, "La incidencia ya estaba resuelta."

        inc_dict = dict(inc)
        c.execute(
            """
            UPDATE incidencias
            SET status='RESUELTA', resolved_at=?, resolved_by=?, resolution_comment=?
            WHERE id=?
            """,
            (now, usuario_clean, comentario_clean, int(incidencia_id)),
        )
        c.execute(
            """
            INSERT INTO audit_events
            (lote_id, item_id, event_type, detail, qty, codigo_ml, sku, mode, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(inc_dict["lote_id"]),
                inc_dict.get("item_id"),
                "INCIDENCIA_RESUELTA",
                comentario_clean,
                int(inc_dict.get("cantidad") or 0),
                norm_code(inc_dict.get("codigo_ml", "")),
                norm_code(inc_dict.get("sku", "")),
                usuario_clean,
                now,
            ),
        )
        c.commit()

    lote_payload = build_lote_payload(int(inc_dict["lote_id"]))
    incidencia_payload = {
        **lote_payload,
        "incidencia_id": int(incidencia_id),
        "item_id": int(inc_dict["item_id"]) if inc_dict.get("item_id") else "",
        "codigo_ml": norm_code(inc_dict.get("codigo_ml", "")),
        "codigo_universal": norm_code(inc_dict.get("codigo_universal", "")),
        "sku": norm_code(inc_dict.get("sku", "")),
        "descripcion": clean_text(inc_dict.get("descripcion", "")),
        "tipo": clean_text(inc_dict.get("tipo", "")),
        "cantidad": int(inc_dict.get("cantidad") or 0),
        "comentario": comentario_clean,
        "usuario": usuario_clean,
        "status": "RESUELTA",
        "estado": "RESUELTA",
        "resolved_at": now,
        "resolved_by": usuario_clean,
        "resolution_comment": comentario_clean,
        "created_at": now,
    }
    audit_payload = {
        **lote_payload,
        "item_id": int(inc_dict["item_id"]) if inc_dict.get("item_id") else "",
        "event_type_audit": "INCIDENCIA_RESUELTA",
        "detail": comentario_clean,
        "detalle": comentario_clean,
        "cantidad": int(inc_dict.get("cantidad") or 0),
        "codigo_ml": norm_code(inc_dict.get("codigo_ml", "")),
        "sku": norm_code(inc_dict.get("sku", "")),
        "modo": usuario_clean,
        "tipo": "INCIDENCIA_RESUELTA",
        "comentario": comentario_clean,
        "created_at": now,
    }
    enqueue_backup_events_batch([
        ("incidencia_resuelta", incidencia_payload),
        ("audit_event", audit_payload),
    ])
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


def label_block_print_markers(lote_id: int) -> tuple[set[str], set[int]]:
    """Retorna marcas de bloques impresos por key y por índice.

    En restauraciones desde Sheets puede cambiar el block_key si el lote fue
    reconstruido con snapshots/ajustes, pero el block_index sigue siendo la
    referencia operativa del bloque descargado. Esta función evita falsos
    pendientes visuales cuando la impresión sí existe en Sheets.
    """
    df = get_label_blocks_df(lote_id)
    if df.empty:
        return set(), set()
    keys = {clean_text(v) for v in df.get("block_key", pd.Series(dtype=str)).astype(str).tolist() if clean_text(v)}
    indexes = set()
    if "block_index" in df.columns:
        for v in df["block_index"].tolist():
            idx = to_int(v)
            if idx:
                indexes.add(idx)
    return keys, indexes


def is_label_block_marked_printed(block: dict, printed_keys: set[str], printed_indexes: set[int]) -> bool:
    """Un bloque se considera impreso si coincide por key o, como respaldo, por índice."""
    return clean_text(block.get("block_key", "")) in printed_keys or to_int(block.get("block_index", 0)) in printed_indexes


def find_restored_label_block(blocks_restore: list[dict], block_index: int, block_key: str) -> dict | None:
    """Busca un bloque restaurable primero por key exacta y luego por índice.

    El fallback por índice es deliberado: evita que un cambio de hash/key del
    bloque tras rescatar desde Sheets haga desaparecer visualmente una impresión
    ya registrada.
    """
    if not blocks_restore:
        return None
    block_key = clean_text(block_key)
    if block_key:
        exact = next((b for b in blocks_restore if to_int(b.get("block_index", 0)) == int(block_index) and clean_text(b.get("block_key", "")) == block_key), None)
        if exact:
            return exact
    same_index = [b for b in blocks_restore if to_int(b.get("block_index", 0)) == int(block_index)]
    return same_index[0] if same_index else None


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
                descripcion_etiqueta_value(item), int(item.get("unidades", 0)), "BLOQUE", "NORMAL",
                int(block["block_index"]), clean_text(block["block_key"]), 1, now,
            ))
            rows.append((
                int(lote_id), int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                descripcion_etiqueta_value(item), LABEL_SEPARATOR_PER_PRODUCT, "BLOQUE", "SEPARADOR",
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
        if tipo_aviso.lower().startswith("ajuste") and cantidad_nueva_int is not None and cantidad_nueva_int >= 0:
            # La cantidad objetivo cambia la meta operacional del lote de inmediato.
            # Las confirmaciones ML/Kame pueden quedar pendientes, pero el PDA y
            # picking no deben seguir trabajando con la cantidad antigua.
            c.execute("UPDATE items SET unidades=?, updated_at=? WHERE id=? AND lote_id=?",
                      (int(cantidad_nueva_int), now, int(item_id), int(lote_id)))
            c.execute(
                """
                UPDATE picking_list_items
                SET cantidad=?
                WHERE lote_id=? AND item_id=?
                  AND picking_list_id IN (
                    SELECT id FROM picking_lists
                    WHERE lote_id=? AND estado NOT IN ('ANULADA','COMPLETADA')
                  )
                """,
                (int(cantidad_nueva_int), int(lote_id), int(item_id), int(lote_id)),
            )
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
    items = get_operational_items(lote_id)
    if items.empty:
        return {"total": 0, "done": 0, "pending": 0, "incidencias_abiertas": 0, "avisos_activos": 0, "label_pending": 0}
    view = items.copy()
    total_units = int(view["unidades"].sum())
    done_units = min(total_units, int(view["acopiadas"].sum()))
    pending_units = max(total_units - done_units, 0)
    labels = label_control_view(lote_id)
    incid = get_incidencias(lote_id, status="ABIERTA")
    avisos = get_avisos_operacionales(lote_id, estado="ACTIVO")
    return {
        "total": total_units,
        "done": done_units,
        "pending": pending_units,
        "incidencias_abiertas": int(len(incid)),
        "avisos_activos": int(len(avisos)),
        "label_pending": int(labels["label_pending"].sum()) if not labels.empty else 0,
    }


def cierre_validaciones(lote_id: int, capacity: int = ROLL_CAPACITY_DEFAULT) -> tuple[bool, list[str], dict]:
    items = get_operational_items(lote_id)
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

    anexos_df = get_anexos_lote(lote_id)
    anexos_pend_ml = 0
    anexos_pend_kame = 0
    if anexos_df is not None and not anexos_df.empty:
        anexos_pend_ml = int((pd.to_numeric(anexos_df.get("anexo_ml_confirmado", 0), errors="coerce").fillna(0).astype(int) == 0).sum())
        anexos_pend_kame = int((pd.to_numeric(anexos_df.get("anexo_kame_confirmado", 0), errors="coerce").fillna(0).astype(int) == 0).sum())
        if anexos_pend_ml > 0:
            issues.append(f"Hay {anexos_pend_ml} producto(s) anexado(s) sin confirmar modificación en ML.")
        if anexos_pend_kame > 0:
            issues.append(f"Hay {anexos_pend_kame} producto(s) anexado(s) sin confirmar reserva Kame realizada.")

    picking_label_status = get_picking_label_status_df(lote_id)
    pending_picking_labels = 0
    if not picking_label_status.empty:
        pending_picking_labels = int((picking_label_status["estado_etiquetas"] == "PENDIENTE").sum())
        if pending_picking_labels > 0:
            issues.append(f"Hay {pending_picking_labels} lista(s) de picking sin etiquetas descargadas/impresas.")

    label_view = label_control_view(lote_id)
    label_pending = int(label_view["label_pending"].sum()) if not label_view.empty else 0

    return len(issues) == 0, issues, {
        "pending_units": pending_units,
        "open_incidents": int(len(inc_abiertas)),
        "active_notices": int(len(avisos_activos)),
        "label_pending": label_pending,
        "expected_blocks": 0,
        "printed_blocks": 0,
        "picking_label_pending": pending_picking_labels,
        "picking_label_total": int(len(picking_label_status)) if not picking_label_status.empty else 0,
        "anexos_total": int(len(anexos_df)) if anexos_df is not None and not anexos_df.empty else 0,
        "anexos_ml_pendientes": anexos_pend_ml,
        "anexos_kame_pendientes": anexos_pend_kame,
    }


def close_lote(lote_id: int, usuario: str, nota: str, force: bool = False, force_reason: str = ""):
    """Cierra un lote.

    Cierre normal: exige validaciones completas.
    Cierre forzado: permitido solo para casos administrativos/práctica, dejando trazabilidad
    explícita en Sheets para no bloquear la operación real cuando hay un lote de prueba activo.
    """
    if is_lote_closed(lote_id):
        return False, "Este lote ya está cerrado."

    ok, issues, validation_data = cierre_validaciones(lote_id)
    force = bool(force)
    nota_limpia = clean_text(nota)
    force_reason_limpio = clean_text(force_reason)

    if not ok and not force:
        return False, "No se puede cerrar: " + " ".join(issues)

    if force and not nota_limpia and not force_reason_limpio:
        return False, "Para cierre administrativo forzado debes dejar una nota o motivo."

    now = now_cl().isoformat(timespec="seconds")
    usuario = clean_text(usuario) or "SIN_USUARIO"
    if force:
        nota_final = nota_limpia or force_reason_limpio
        if issues:
            nota_final = f"CIERRE FORZADO / ADMINISTRATIVO. {nota_final} | Pendientes al cierre: " + " | ".join(issues)
    else:
        nota_final = nota_limpia

    with db() as c:
        c.execute(
            "UPDATE lotes SET status='CERRADO', closed_at=?, closed_by=?, close_note=? WHERE id=?",
            (now, usuario, nota_final, int(lote_id)),
        )
        c.commit()

    enqueue_backup_event("lote_cerrado", {
        **build_lote_payload(lote_id),
        "created_at": now,
        "usuario": usuario,
        "comentario": nota_final,
        "status": "CERRADO",
        "cierre_forzado": 1 if force else 0,
        "cierre_forzado_motivo": force_reason_limpio,
        "bloqueos_cierre": issues,
        "validacion_cierre": validation_data,
    })
    log_audit_event(
        lote_id,
        event_type="LOTE_CERRADO_FORZADO" if force else "LOTE_CERRADO",
        detail=nota_final,
        mode=usuario,
    )
    return True, "Lote cerrado correctamente." if not force else "Lote cerrado por cierre administrativo forzado."


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
        df = pd.read_sql_query(
            """
            SELECT
                pli.*,
                COALESCE(i.identificacion, '') AS identificacion,
                COALESCE(NULLIF(i.descripcion_ml, ''), NULLIF(pli.descripcion_ml, ''), '') AS descripcion_ml_item,
                COALESCE(NULLIF(i.descripcion_kame, ''), NULLIF(pli.descripcion_kame, ''), NULLIF(pli.descripcion, ''), '') AS descripcion_kame_item
            FROM picking_list_items pli
            LEFT JOIN items i ON i.id = pli.item_id AND i.lote_id = pli.lote_id
            WHERE pli.picking_list_id=?
            ORDER BY pli.area, pli.sku, pli.cantidad DESC, pli.id
            """,
            c,
            params=(int(picking_list_id),),
        )
    if not df.empty and "lote_id" in df.columns:
        try:
            lid = int(df["lote_id"].dropna().iloc[0])
            df = apply_quantity_adjustments_df(lid, df, item_col="item_id", qty_col="cantidad")
        except Exception:
            pass
    return df


def get_picking_assigned_qty(lote_id: int) -> pd.DataFrame:
    with db() as c:
        raw = pd.read_sql_query(
            """
            SELECT pli.item_id, pli.cantidad
            FROM picking_list_items pli
            JOIN picking_lists pl ON pl.id=pli.picking_list_id
            WHERE pli.lote_id=? AND pl.estado <> 'ANULADA'
            """,
            c,
            params=(int(lote_id),),
        )
    if raw.empty:
        return pd.DataFrame(columns=["item_id", "asignado"])
    raw = apply_quantity_adjustments_df(lote_id, raw, item_col="item_id", qty_col="cantidad")
    df = raw.groupby("item_id", as_index=False)["cantidad"].sum().rename(columns={"cantidad": "asignado"})
    df["asignado"] = df["asignado"].fillna(0).astype(int)
    return df


def get_picking_available_items(lote_id: int) -> pd.DataFrame:
    """Productos disponibles para listas de picking.

    Regla operacional: un producto/SKU se asigna completo a una sola lista activa.
    No se permite dividir cantidades del mismo producto entre listas, porque eso
    desordena el papel y la trazabilidad. Si ya tiene cualquier cantidad asignada
    en una lista no anulada, queda bloqueado para nuevas listas.
    """
    items = get_operational_items(lote_id)
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



def apply_picking_operational_sort(df: pd.DataFrame, units_desc: bool = True) -> pd.DataFrame:
    """Orden operacional para armar listas de picking.

    La tabla de Streamlit permite ordenar visualmente por una columna, pero la
    operación necesita un orden compuesto y estable: Área → SKU → Unidades.
    """
    if df is None or df.empty:
        return df
    out = df.copy()

    def area_key(v):
        s = clean_text(v).upper()
        if not s:
            return "ZZZ"
        return s

    out["_sort_area"] = out["area"].map(area_key) if "area" in out.columns else "ZZZ"
    out["_sort_sku"] = out["sku"].map(norm_code) if "sku" in out.columns else ""
    out["_sort_unidades"] = out["unidades"].map(to_int) if "unidades" in out.columns else 0
    out["_sort_nro"] = out["nro"].map(to_int) if "nro" in out.columns else 0

    out = out.sort_values(
        by=["_sort_area", "_sort_sku", "_sort_unidades", "_sort_nro"],
        ascending=[True, True, not bool(units_desc), True],
        kind="mergesort",
    )
    return out.drop(columns=["_sort_area", "_sort_sku", "_sort_unidades", "_sort_nro"], errors="ignore").reset_index(drop=True)




def search_picking_assignment(lote_id: int, query: str) -> pd.DataFrame:
    """Busca un producto del lote y muestra si está asignado a una lista de picking.

    Es una consulta operativa solamente: no modifica cantidades, no crea eventos y no toca Sheets.
    Permite buscar por SKU, Código ML, Código Universal/EAN o descripción.
    """
    q = clean_text(query)
    if not lote_id or not q:
        return pd.DataFrame()

    with db() as c:
        df = pd.read_sql_query(
            """
            SELECT
                i.id AS item_id,
                i.area,
                i.nro,
                i.codigo_ml,
                i.codigo_universal,
                i.sku,
                COALESCE(NULLIF(i.descripcion_kame, ''), i.descripcion, '') AS descripcion,
                COALESCE(i.descripcion_ml, '') AS descripcion_ml,
                COALESCE(i.familia_kame, '') AS familia_kame,
                COALESCE(i.maestro_match_status, '') AS maestro_match_status,
                i.unidades AS cantidad_lote,
                pl.id AS picking_list_id,
                pl.codigo_lista AS codigo_lista,
                pl.asignado_a AS asignado_a,
                pl.estado AS estado_lista,
                pli.cantidad AS cantidad_lista,
                COALESCE(SUM(s.cantidad), 0) AS validado_pda
            FROM items i
            LEFT JOIN picking_list_items pli
                ON pli.item_id = i.id
               AND pli.lote_id = i.lote_id
            LEFT JOIN picking_lists pl
                ON pl.id = pli.picking_list_id
               AND pl.estado <> 'ANULADA'
            LEFT JOIN scans s
                ON s.lote_id = i.lote_id
               AND s.item_id = i.id
               AND s.picking_list_id = pli.picking_list_id
            WHERE i.lote_id = ?
            GROUP BY
                i.id, i.area, i.nro, i.codigo_ml, i.codigo_universal, i.sku,
                i.descripcion_kame, i.descripcion, i.descripcion_ml, i.familia_kame,
                i.maestro_match_status, i.unidades,
                pl.id, pl.codigo_lista, pl.asignado_a, pl.estado, pli.cantidad
            ORDER BY i.sku, i.codigo_ml, pl.codigo_lista
            """,
            c,
            params=(int(lote_id),),
        )

    if df.empty:
        return df

    qn = normalize_header(q)
    mask = pd.Series(False, index=df.index)
    for col in ["sku", "codigo_ml", "codigo_universal", "descripcion", "descripcion_ml", "familia_kame"]:
        if col in df.columns:
            mask = mask | df[col].astype(str).map(normalize_header).str.contains(qn, na=False, regex=False)
    df = df[mask].copy()
    if df.empty:
        return df

    for col in ["cantidad_lote", "cantidad_lista", "validado_pda"]:
        if col in df.columns:
            df[col] = df[col].map(to_int)

    # Los productos sin lista quedan con columnas de picking vacías.
    df["picking_list_id"] = df["picking_list_id"].fillna(0).map(to_int)
    df["codigo_lista"] = df["codigo_lista"].fillna("").map(clean_text)
    df["asignado_a"] = df["asignado_a"].fillna("").map(clean_text)
    df["estado_lista"] = df["estado_lista"].fillna("").map(clean_text)
    df["cantidad_lista"] = df["cantidad_lista"].fillna(0).map(to_int)
    df["validado_pda"] = df["validado_pda"].fillna(0).map(to_int)
    df["pendiente_picking"] = (df["cantidad_lista"].astype(int) - df["validado_pda"].astype(int)).clip(lower=0)

    def estado_asig(r):
        if to_int(r.get("picking_list_id", 0)) <= 0:
            return "SIN ASIGNAR"
        return f"ASIGNADO · {clean_text(r.get('codigo_lista',''))} · {clean_text(r.get('asignado_a',''))}"

    def estado_val(r):
        if to_int(r.get("picking_list_id", 0)) <= 0:
            return "SIN LISTA"
        req = to_int(r.get("cantidad_lista", 0))
        val = to_int(r.get("validado_pda", 0))
        if val == 0:
            return "SIN VALIDAR"
        if val < req:
            return "PARCIAL"
        if val == req:
            return "COMPLETO"
        return "SOBREVALIDADO"

    df["estado_asignacion"] = df.apply(estado_asig, axis=1)
    df["estado_validacion"] = df.apply(estado_val, axis=1)
    return df.reset_index(drop=True)

def get_picking_validation_summary(picking_list_id: int) -> pd.DataFrame:
    items = get_picking_items(picking_list_id)
    if items.empty:
        return items
    try:
        lote_id = int(items["lote_id"].iloc[0])
    except Exception:
        lote_id = 0
    scans_all = get_scans_deduped(lote_id) if lote_id else pd.DataFrame()
    if scans_all.empty:
        scans = pd.DataFrame(columns=["item_id", "validado_pda", "ultimo_validado"])
    else:
        scans_all = scans_all[scans_all["picking_list_id"].map(to_int) == int(picking_list_id)].copy()
        if scans_all.empty:
            scans = pd.DataFrame(columns=["item_id", "validado_pda", "ultimo_validado"])
        else:
            scans_all["cantidad"] = scans_all["cantidad"].map(to_int)
            scans = scans_all.groupby("item_id", as_index=False).agg(
                validado_pda=("cantidad", "sum"),
                ultimo_validado=("created_at", "max"),
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


def render_scan_picking_progress_dropdown(picking_list_id: int):
    """Muestra en Escaneo el avance de la lista picking activa.

    Es solo visual: no cambia estado, no escribe eventos y no toca Sheets.
    Sirve para que el operador vea rápido qué productos de SU lista ya están
    completos, cuáles van parcial y cuáles faltan por validar.
    """
    if not picking_list_id:
        return
    try:
        pick_id = int(picking_list_id)
    except Exception:
        return

    meta = get_picking_list_meta(pick_id)
    summary = get_picking_validation_summary(pick_id)
    if summary.empty:
        st.caption("La lista de picking activa no tiene productos asignados.")
        return

    summary = summary.copy()
    for col in ["cantidad", "validado_pda", "pendiente_picking"]:
        if col in summary.columns:
            summary[col] = summary[col].map(to_int)

    total_productos = int(len(summary))
    productos_completos = int((summary["estado_validacion"].astype(str) == "COMPLETO").sum())
    productos_pendientes = int((summary["pendiente_picking"].map(to_int) > 0).sum())
    total_unidades = int(summary["cantidad"].sum())
    validado_unidades = int(summary["validado_pda"].sum())
    pendiente_unidades = int(summary["pendiente_picking"].sum())
    codigo_lista = clean_text(meta.get("codigo_lista", "")) or f"Lista {pick_id}"
    asignado_a = clean_text(meta.get("asignado_a", ""))

    title = f"📋 Avance lista {codigo_lista}"
    if asignado_a:
        title += f" · {asignado_a}"
    title += f" · faltan {pendiente_unidades}/{total_unidades}"

    with st.expander(title, expanded=False):
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Productos", total_productos)
        m2.metric("Completos", productos_completos)
        m3.metric("Pendientes", productos_pendientes)
        m4.metric("Unidades faltantes", pendiente_unidades)

        estado_vista = st.selectbox(
            "Ver productos",
            ["Faltan validar", "Validados completos", "Parciales", "Todos"],
            key=f"scan_pick_progress_view_{pick_id}",
            help="Vista rápida de la lista activa. No modifica escaneos ni cantidades.",
        )

        if estado_vista == "Faltan validar":
            view = summary[summary["pendiente_picking"].map(to_int) > 0].copy()
        elif estado_vista == "Validados completos":
            view = summary[summary["estado_validacion"].astype(str) == "COMPLETO"].copy()
        elif estado_vista == "Parciales":
            view = summary[summary["estado_validacion"].astype(str).isin(["PARCIAL", "SOBREVALIDADO"])].copy()
        else:
            view = summary.copy()

        if view.empty:
            st.success("No hay productos en esta vista.")
            return

        view = view.sort_values(["pendiente_picking", "sku"], ascending=[False, True])
        cols = [
            "estado_validacion", "codigo_ml", "codigo_universal", "sku", "descripcion",
            "cantidad", "validado_pda", "pendiente_picking", "ultimo_validado"
        ]
        cols = [c for c in cols if c in view.columns]
        show = view[cols].copy()
        show = show.rename(columns={
            "estado_validacion": "Estado",
            "codigo_ml": "Código ML",
            "codigo_universal": "Código universal",
            "sku": "SKU",
            "descripcion": "Descripción",
            "cantidad": "Lista",
            "validado_pda": "Validado",
            "pendiente_picking": "Falta",
            "ultimo_validado": "Último validado",
        })
        if "Último validado" in show.columns:
            show["Último validado"] = show["Último validado"].map(fmt_dt)
        st.dataframe(show, use_container_width=True, hide_index=True, height=320)

        # Selector compacto para ver un producto puntual sin buscar en la tabla.
        options = {}
        for r in view.itertuples(index=False):
            try:
                estado = clean_text(getattr(r, "estado_validacion"))
                falta = to_int(getattr(r, "pendiente_picking"))
                sku = clean_text(getattr(r, "sku"))
                ml = clean_text(getattr(r, "codigo_ml"))
                desc = clean_text(getattr(r, "descripcion"))
                iid = int(getattr(r, "item_id"))
                label = f"{estado} · Falta {falta} · {sku or ml} · {desc[:70]}"
                options[label] = iid
            except Exception:
                continue
        if options:
            selected_label = st.selectbox(
                "Producto rápido",
                ["Seleccionar..."] + list(options.keys()),
                key=f"scan_pick_progress_product_{pick_id}_{estado_vista}",
            )
            if selected_label != "Seleccionar...":
                selected_id = options[selected_label]
                selected = view[view["item_id"].map(to_int) == int(selected_id)]
                if not selected.empty:
                    row = selected.iloc[0]
                    st.info(
                        f"{clean_text(row.get('descripcion',''))} · "
                        f"ML {norm_code(row.get('codigo_ml',''))} · SKU {norm_code(row.get('sku',''))} · "
                        f"Validado {to_int(row.get('validado_pda',0))}/{to_int(row.get('cantidad',0))} · "
                        f"Falta {to_int(row.get('pendiente_picking',0))}"
                    )


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
            """
            SELECT pli.cantidad, pli.lote_id
            FROM picking_list_items pli
            WHERE pli.picking_list_id=? AND pli.item_id=?
            """,
            (int(picking_list_id), int(item_id)),
        ).fetchone()
        if not item:
            return {"cantidad": 0, "validado_pda": 0, "pendiente": 0}
    cantidad = int(item["cantidad"] or 0)
    effective = get_effective_item_units(int(item["lote_id"] or 0), int(item_id), cantidad)
    if effective is not None:
        cantidad = int(effective)
    scans = get_scans_deduped(int(item["lote_id"] or 0))
    if scans.empty:
        validado = 0
    else:
        scans = scans[(scans["picking_list_id"].map(to_int) == int(picking_list_id)) & (scans["item_id"].map(to_int) == int(item_id))].copy()
        validado = int(scans["cantidad"].map(to_int).sum()) if not scans.empty else 0
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
                (picking_list_id, lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, maestro_match_status,
                 cantidad, area, nro, estado, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDIENTE', ?)
                """,
                (
                    list_id, int(lote_id), int(item_id), norm_code(item["codigo_ml"]), norm_code(item["codigo_universal"]),
                    norm_code(item["sku"]), descripcion_operativa_value(item), descripcion_operativa_value(item), descripcion_etiqueta_value(item),
                    clean_text(item["familia_kame"] if "familia_kame" in item.keys() else ""),
                    clean_text(item["maestro_match_status"] if "maestro_match_status" in item.keys() else ""),
                    int(cantidad), clean_text(item["area"]), clean_text(item["nro"]), now,
                ),
            )
            inserted_items.append({
                "item_id": int(item_id),
                "codigo_ml": norm_code(item["codigo_ml"]),
                "codigo_universal": norm_code(item["codigo_universal"]),
                "sku": norm_code(item["sku"]),
                "descripcion": descripcion_operativa_value(item),
                "descripcion_kame": descripcion_operativa_value(item),
                "descripcion_ml": descripcion_etiqueta_value(item),
                "familia_kame": clean_text(item["familia_kame"] if "familia_kame" in item.keys() else ""),
                "maestro_match_status": clean_text(item["maestro_match_status"] if "maestro_match_status" in item.keys() else ""),
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
          <td class="tagcell">{esc(getattr(r, 'identificacion', ''))}</td>
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
  col.col-tag {{ width:118px; }}
  col.col-obs {{ width:192px; }}
  .check {{ font-size:18px; text-align:center; padding:2px; }}
  .qty {{ font-size:16px; font-weight:900; text-align:center; padding:3px 2px; }}
  .codecell {{ font-size:11px; line-height:1.25; word-break:break-word; }}
  .skucell {{ font-size:11px; line-height:1.25; word-break:break-word; }}
  .desccell {{ font-size:12px; line-height:1.25; }}
  .tagcell {{ font-size:11px; line-height:1.2; font-weight:700; text-align:center; }}
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
  <col class="col-tag">
  <col class="col-obs">
</colgroup>
<thead>
<tr>
  <th>OK</th><th>Código ML / Universal</th><th>SKU</th><th>Descripción</th><th>Cant.</th><th>Etiquetado</th><th>Obs.</th>
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

    tab_resumen, tab_crear, tab_buscar, tab_detalle = st.tabs(["Resumen", "Crear lista", "Buscar SKU", "Detalle / impresión"])

    with tab_resumen:
        if lists_progress.empty:
            st.info("Aún no hay listas de picking para este lote.")
        else:
            st.dataframe(lists_progress.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True, height=330)
        if not items_av.empty:
            sin_asignar = items_av[items_av["disponible_asignar"].astype(int) > 0]
            sin_asignar = apply_picking_operational_sort(sin_asignar, units_desc=True)
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
            col_ord1, col_ord2 = st.columns([2, 1])
            with col_ord1:
                orden_operativo = st.selectbox(
                    "Orden operativo",
                    ["Área → SKU → Unidades", "SKU → Área → Unidades", "Unidades → Área → SKU"],
                    index=0,
                    key="pick_orden_operativo",
                    help="Ordena la tabla antes de seleccionar. Así no dependes del orden manual de una sola columna.",
                )
            with col_ord2:
                unidades_desc = st.selectbox(
                    "Unidades",
                    ["Mayor a menor", "Menor a mayor"],
                    index=0,
                    key="pick_unidades_orden",
                ) == "Mayor a menor"
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
            if orden_operativo == "Área → SKU → Unidades":
                base = apply_picking_operational_sort(base, units_desc=unidades_desc)
            elif orden_operativo == "SKU → Área → Unidades":
                base = base.copy()
                base["_sort_sku"] = base["sku"].map(norm_code)
                base["_sort_area"] = base["area"].map(lambda x: clean_text(x).upper() or "ZZZ")
                base["_sort_unidades"] = base["unidades"].map(to_int)
                base = base.sort_values(
                    by=["_sort_sku", "_sort_area", "_sort_unidades"],
                    ascending=[True, True, not bool(unidades_desc)],
                    kind="mergesort",
                ).drop(columns=["_sort_sku", "_sort_area", "_sort_unidades"], errors="ignore").reset_index(drop=True)
            else:
                base = base.copy()
                base["_sort_unidades"] = base["unidades"].map(to_int)
                base["_sort_area"] = base["area"].map(lambda x: clean_text(x).upper() or "ZZZ")
                base["_sort_sku"] = base["sku"].map(norm_code)
                base = base.sort_values(
                    by=["_sort_unidades", "_sort_area", "_sort_sku"],
                    ascending=[not bool(unidades_desc), True, True],
                    kind="mergesort",
                ).drop(columns=["_sort_unidades", "_sort_area", "_sort_sku"], errors="ignore").reset_index(drop=True)
            st.caption(f"Orden aplicado: {orden_operativo} · Unidades {'mayor a menor' if unidades_desc else 'menor a mayor'}")
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


    with tab_buscar:
        st.markdown("### Buscar asignación por SKU / código")
        st.caption("Consulta rápida para saber si un producto del FULL ya está asignado a un pickeador y cuánto lleva validado. No modifica datos ni envía eventos.")
        q_asig = st.text_input(
            "Buscar producto",
            key="pick_assignment_search",
            placeholder="Ingresa SKU, Código ML, EAN/código universal o parte de la descripción",
        )
        if not clean_text(q_asig):
            st.info("Escribe un SKU o código para buscar en el lote activo.")
        else:
            result = search_picking_assignment(active_lote, q_asig)
            if result.empty:
                st.warning("No encontré productos en este lote con ese dato.")
            else:
                productos_encontrados = int(result["item_id"].nunique()) if "item_id" in result.columns else len(result)
                asignados = int((result["picking_list_id"].map(to_int) > 0).sum()) if "picking_list_id" in result.columns else 0
                sin_asignar = int((result["picking_list_id"].map(to_int) <= 0).sum()) if "picking_list_id" in result.columns else 0
                unidades_lista = int(result["cantidad_lista"].sum()) if "cantidad_lista" in result.columns else 0
                validado_lista = int(result["validado_pda"].sum()) if "validado_pda" in result.columns else 0
                b1, b2, b3, b4 = st.columns(4)
                b1.metric("Productos encontrados", productos_encontrados)
                b2.metric("Filas asignadas", asignados)
                b3.metric("Sin asignar", sin_asignar)
                b4.metric("Validado", f"{validado_lista}/{unidades_lista}")

                show_cols = [
                    "estado_asignacion", "estado_validacion", "codigo_lista", "asignado_a", "estado_lista",
                    "codigo_ml", "codigo_universal", "sku", "descripcion", "cantidad_lote", "cantidad_lista",
                    "validado_pda", "pendiente_picking", "area", "nro"
                ]
                show_cols = [c for c in show_cols if c in result.columns]
                show = result[show_cols].copy().rename(columns={
                    "estado_asignacion": "Asignación",
                    "estado_validacion": "Validación",
                    "codigo_lista": "Lista",
                    "asignado_a": "Pickeador",
                    "estado_lista": "Estado lista",
                    "codigo_ml": "Código ML",
                    "codigo_universal": "Código universal",
                    "sku": "SKU",
                    "descripcion": "Descripción Kame",
                    "cantidad_lote": "Cant. lote",
                    "cantidad_lista": "Cant. lista",
                    "validado_pda": "Validado PDA",
                    "pendiente_picking": "Falta validar",
                    "area": "Área",
                    "nro": "N°",
                })
                st.dataframe(show, use_container_width=True, hide_index=True, height=360)

                # Resumen simple por producto para no obligar a leer toda la tabla.
                with st.expander("Resumen por producto", expanded=False):
                    for item_id, grp in result.groupby("item_id", sort=False):
                        r = grp.iloc[0]
                        desc = clean_text(r.get("descripcion", ""))
                        sku = norm_code(r.get("sku", ""))
                        ml = norm_code(r.get("codigo_ml", ""))
                        if (grp["picking_list_id"].map(to_int) > 0).any():
                            asignaciones = []
                            for rr in grp.itertuples(index=False):
                                pid = to_int(getattr(rr, "picking_list_id", 0))
                                if pid <= 0:
                                    continue
                                asignaciones.append(
                                    f"{clean_text(getattr(rr, 'codigo_lista', ''))} · {clean_text(getattr(rr, 'asignado_a', ''))} · "
                                    f"{clean_text(getattr(rr, 'estado_lista', ''))} · "
                                    f"validado {to_int(getattr(rr, 'validado_pda', 0))}/{to_int(getattr(rr, 'cantidad_lista', 0))}"
                                )
                            st.success(f"{sku or ml} · {desc} — " + " | ".join(asignaciones))
                        else:
                            st.warning(f"{sku or ml} · {desc} — SIN ASIGNAR A PICKING")

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
# Reservas Kame: importador inventario con expansión de packs
# ============================================================

KAME_RESERVA_HEADERS = [
    "Tipo Movimiento",
    "Motivo Movimiento",
    "FolioAuto",
    "Folio",
    "Bodega Entrada",
    "Bodega Salida",
    "Ficha",
    "Fecha",
    "Glosa",
    "SKU",
    "Nombre Unidad de Negocio",
    "Cantidad",
    "PrecioUnitario",
]


def to_float_qty(v) -> float:
    s = clean_text(v)
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".") if re.search(r"\d\.\d{3}", s) else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def format_kame_qty(v) -> str:
    try:
        n = float(v)
    except Exception:
        n = 0.0
    if abs(n - round(n)) < 0.0000001:
        return str(int(round(n)))
    return (f"{n:.6f}").rstrip("0").rstrip(".")


def kame_csv_field(v) -> str:
    # Kame permite CSV/TXT separado por ;. Evitamos saltos y ; dentro de campos.
    s = clean_text(v).replace(";", ",")
    s = re.sub(r"[\r\n]+", " ", s).strip()
    return s


def load_pack_components(path: Path = PACKS_PATH) -> dict:
    """Devuelve {PACK SKU: [componentes]} usando data/packs.xlsx.

    Cada componente trae ART. SKU y ART. Cantidad, que es la cantidad del artículo
    unitario necesaria por 1 unidad del pack vendido en FULL.
    """
    if not Path(path).exists():
        raise FileNotFoundError(f"No encontré {path}. Debe existir data/packs.xlsx en el repositorio.")
    raw = pd.read_excel(path, dtype=object)
    raw = raw.dropna(how="all")
    if raw.empty:
        return {}
    raw.columns = [clean_text(c) for c in raw.columns]
    cols = list(raw.columns)
    pack_col = col_required(cols, "PACK SKU", ["PACK SKU", "Pack SKU", "SKU Pack", "SKU PACK"])
    art_col = col_required(cols, "ART. SKU", ["ART. SKU", "ART SKU", "SKU Art", "SKU Articulo", "SKU Artículo"])
    qty_col = col_required(cols, "ART. Cantidad", ["ART. Cantidad", "ART Cantidad", "Cantidad", "Cantidad Articulo", "Cantidad Artículo"])
    desc_pack_col = col_exact(cols, ["PACK Descripción", "PACK Descripcion", "Descripción Pack", "Descripcion Pack"])
    desc_art_col = col_exact(cols, ["ART. Descripción", "ART. Descripcion", "ART Descripción", "ART Descripcion"])

    pack_map = {}
    for _, row in raw.iterrows():
        pack_sku = norm_code(row.get(pack_col, ""))
        art_sku = norm_code(row.get(art_col, ""))
        factor = to_float_qty(row.get(qty_col, 0))
        if not pack_sku:
            continue
        pack_map.setdefault(pack_sku, []).append({
            "pack_sku": pack_sku,
            "pack_descripcion": clean_text(row.get(desc_pack_col, "")) if desc_pack_col else "",
            "art_sku": art_sku,
            "art_descripcion": clean_text(row.get(desc_art_col, "")) if desc_art_col else "",
            "factor": factor,
        })
    return pack_map


def build_kame_reserva_data(lote_id: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Expande packs y genera preview + agrupado final para reserva Kame."""
    items = get_items(lote_id)
    alerts = []
    if items.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame([{"tipo": "ERROR", "detalle": "El lote no tiene productos."}])

    pack_map = load_pack_components(PACKS_PATH)
    rows = []
    for it in items.itertuples(index=False):
        sku_full = norm_code(getattr(it, "sku", ""))
        unidades_full = to_float_qty(getattr(it, "unidades", 0))
        desc_full = clean_text(getattr(it, "descripcion", ""))
        codigo_ml = norm_code(getattr(it, "codigo_ml", ""))
        item_id = int(getattr(it, "id", 0) or 0)

        if not sku_full:
            alerts.append({"tipo": "ERROR", "detalle": f"Item {item_id} no tiene SKU. No se puede reservar en Kame."})
            continue

        components = pack_map.get(sku_full, [])
        if components:
            valid_components = 0
            for comp in components:
                art_sku = norm_code(comp.get("art_sku", ""))
                factor = float(comp.get("factor") or 0)
                if not art_sku:
                    alerts.append({"tipo": "ERROR", "detalle": f"Pack {sku_full} tiene un componente sin ART. SKU."})
                    continue
                if factor <= 0:
                    alerts.append({"tipo": "ERROR", "detalle": f"Pack {sku_full} → ART. SKU {art_sku} tiene ART. Cantidad inválida: {factor}."})
                    continue
                cantidad_reserva = unidades_full * factor
                if abs(cantidad_reserva - round(cantidad_reserva)) > 0.0000001:
                    alerts.append({"tipo": "AVISO", "detalle": f"Cantidad decimal: pack {sku_full} → {art_sku} = {format_kame_qty(cantidad_reserva)}."})
                rows.append({
                    "item_id": item_id,
                    "codigo_ml": codigo_ml,
                    "sku_full": sku_full,
                    "descripcion_full": desc_full,
                    "unidades_full": unidades_full,
                    "tipo_origen": "PACK_EXPANDIDO",
                    "sku_reserva": art_sku,
                    "descripcion_reserva": clean_text(comp.get("art_descripcion", "")),
                    "factor_pack": factor,
                    "cantidad_reserva": cantidad_reserva,
                })
                valid_components += 1
            if valid_components == 0:
                alerts.append({"tipo": "ERROR", "detalle": f"Pack {sku_full} no tiene componentes válidos. No se genera reserva para ese item."})
        else:
            rows.append({
                "item_id": item_id,
                "codigo_ml": codigo_ml,
                "sku_full": sku_full,
                "descripcion_full": desc_full,
                "unidades_full": unidades_full,
                "tipo_origen": "DIRECTO",
                "sku_reserva": sku_full,
                "descripcion_reserva": desc_full,
                "factor_pack": 1.0,
                "cantidad_reserva": unidades_full,
            })

    expanded = pd.DataFrame(rows)
    if expanded.empty:
        return expanded, pd.DataFrame(), pd.DataFrame(alerts or [{"tipo": "ERROR", "detalle": "No se pudo generar ninguna línea de reserva."}])

    grouped = (
        expanded.groupby("sku_reserva", as_index=False)
        .agg(
            descripcion_reserva=("descripcion_reserva", "first"),
            cantidad_reserva=("cantidad_reserva", "sum"),
            lineas_origen=("sku_full", "count"),
            packs_expandidos=("tipo_origen", lambda s: int((s == "PACK_EXPANDIDO").sum())),
        )
        .sort_values("sku_reserva", kind="mergesort")
        .reset_index(drop=True)
    )
    grouped["cantidad_kame"] = grouped["cantidad_reserva"].map(format_kame_qty)
    alerts_df = pd.DataFrame(alerts, columns=["tipo", "detalle"]) if alerts else pd.DataFrame(columns=["tipo", "detalle"])
    return expanded, grouped, alerts_df


def build_kame_reserva_csv(grouped: pd.DataFrame, folio: str, ficha: str, fecha_doc, glosa: str, bodega_salida: str, unidad_negocio: str, folio_auto: str = "S") -> bytes:
    if hasattr(fecha_doc, "strftime"):
        fecha_txt = fecha_doc.strftime("%d/%m/%Y")
    else:
        fecha_txt = clean_text(fecha_doc)
    lines = [";".join(KAME_RESERVA_HEADERS)]
    for r in grouped.itertuples(index=False):
        row = [
            "SALIDA",
            "reserva",
            kame_csv_field(folio_auto or "S"),
            kame_csv_field(folio),
            "",
            kame_csv_field(bodega_salida or "BODEGA UNIVERSAL"),
            kame_csv_field(ficha),
            kame_csv_field(fecha_txt),
            kame_csv_field(glosa),
            kame_csv_field(getattr(r, "sku_reserva", "")),
            kame_csv_field(unidad_negocio or "Casa Matriz"),
            kame_csv_field(getattr(r, "cantidad_kame", format_kame_qty(getattr(r, "cantidad_reserva", 0)))),
            "",
        ]
        lines.append(";".join(row))
    text = "\r\n".join(lines) + "\r\n"
    # Kame no reconoce correctamente la primera cabecera si el CSV trae BOM.
    # Debe ser UTF-8 simple, separado por punto y coma y con cabeceras exactas.
    return text.encode("utf-8")


def register_reserva_kame(lote_id: int, folio: str, folio_auto: str, ficha: str, fecha_doc, glosa: str, bodega_salida: str, unidad_negocio: str, archivo_nombre: str, usuario: str, expanded: pd.DataFrame, grouped: pd.DataFrame):
    """Registra la generación/descarga de reserva en Sheets como fuente única."""
    lote_payload = build_lote_payload(lote_id)
    created_at = now_cl().isoformat(timespec="seconds")
    if hasattr(fecha_doc, "strftime"):
        fecha_txt = fecha_doc.strftime("%d/%m/%Y")
    else:
        fecha_txt = clean_text(fecha_doc)
    usuario = clean_text(usuario) or "ADMIN"
    sku_count = int(grouped["sku_reserva"].nunique()) if not grouped.empty else 0
    unidades_total = float(grouped["cantidad_reserva"].sum()) if not grouped.empty else 0.0
    packs_expandidos = int((expanded["tipo_origen"] == "PACK_EXPANDIDO").sum()) if not expanded.empty else 0
    productos_full = int(expanded["item_id"].nunique()) if not expanded.empty else 0
    csv_bytes = build_kame_reserva_csv(grouped, folio, ficha, fecha_doc, glosa, bodega_salida, unidad_negocio, folio_auto) if not grouped.empty else b""
    csv_hash = hashlib.sha256(csv_bytes).hexdigest() if csv_bytes else ""

    with db() as c:
        c.execute(
            """
            INSERT INTO reservas_kame
            (lote_id, folio, folio_auto, ficha, fecha, glosa, bodega_salida, unidad_negocio,
             sku_count, unidades_total, productos_full, packs_expandidos, lineas_csv, archivo_nombre, csv_hash, usuario, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(lote_id), clean_text(folio), clean_text(folio_auto), clean_text(ficha), fecha_txt, clean_text(glosa), clean_text(bodega_salida), clean_text(unidad_negocio),
             sku_count, unidades_total, productos_full, packs_expandidos, int(len(grouped)), clean_text(archivo_nombre), csv_hash, usuario, created_at),
        )
        c.commit()

    events = [("reserva_kame_generada", {
        **lote_payload,
        "created_at": created_at,
        "folio": clean_text(folio),
        "folio_auto": clean_text(folio_auto),
        "ficha": clean_text(ficha),
        "fecha": fecha_txt,
        "glosa": clean_text(glosa),
        "bodega_salida": clean_text(bodega_salida),
        "unidad_negocio": clean_text(unidad_negocio),
        "sku_count": sku_count,
        "unidades_total": format_kame_qty(unidades_total),
        "productos_full": productos_full,
        "packs_expandidos": packs_expandidos,
        "lineas_csv": int(len(grouped)),
        "archivo_nombre": clean_text(archivo_nombre),
        "csv_hash": csv_hash,
        "usuario": usuario,
        "detalle_guardado": "RESUMEN_SIN_ITEMS",
    })]

    # No se envía reserva_kame_item producto por producto.
    # La reserva Kame es un archivo exportado: la trazabilidad queda en el evento resumen
    # + nombre de archivo + hash del CSV generado + totales.
    enqueue_backup_events_batch(events)
    log_audit_event(lote_id, event_type="RESERVA_KAME_GENERADA", detail=f"Reserva Kame folio {clean_text(folio)} · {sku_count} SKU · {format_kame_qty(unidades_total)} unidades", qty=int(round(unidades_total)), mode=usuario)


def render_reservas_kame_module(active_lote: int):
    st.subheader("Reservas Kame")
    st.caption("Genera el archivo masivo para reservar en Kame los productos del lote FULL. Si un SKU es pack, se expande al ART. SKU unitario usando data/packs.xlsx.")
    if not active_lote:
        st.warning("No hay lote activo.")
        return

    lote = get_lote(active_lote)
    if not PACKS_PATH.exists():
        st.error("Falta data/packs.xlsx en el repo. No genero reserva Kame sin la matriz de packs, porque se podrían reservar SKUs incorrectos.")
        return

    try:
        expanded, grouped, alerts = build_kame_reserva_data(active_lote)
    except Exception as e:
        st.error(f"No pude construir la reserva Kame: {e}")
        return

    has_errors = (not alerts.empty and alerts["tipo"].astype(str).str.upper().eq("ERROR").any())
    total_full = int(get_items(active_lote)["unidades"].sum()) if not get_items(active_lote).empty else 0
    total_reserva = float(grouped["cantidad_reserva"].sum()) if not grouped.empty else 0.0
    packs_count = int((expanded["tipo_origen"] == "PACK_EXPANDIDO").sum()) if not expanded.empty else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Lote", clean_text(lote.get("nombre", ""))[:24])
    m2.metric("Unidades FULL", total_full)
    m3.metric("SKU reserva", int(grouped["sku_reserva"].nunique()) if not grouped.empty else 0)
    m4.metric("Packs expandidos", packs_count)

    with st.expander("Parámetros del archivo Kame", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            folio = st.text_input("Folio", key="kame_res_folio", placeholder="Ej: 1000006")
            folio_auto = st.selectbox("FolioAuto", ["S", "A", ""], index=0, key="kame_res_folio_auto")
        with c2:
            ficha_opt = st.selectbox("Ficha", ["16.092.564-7", "16.092.564-8", "Otra"], index=0, key="kame_res_ficha_opt")
            ficha = st.text_input("Ficha personalizada", key="kame_res_ficha_custom", placeholder="XX.XXX.XXX-X") if ficha_opt == "Otra" else ficha_opt
        with c3:
            fecha_doc = st.date_input("Fecha", value=now_cl().date(), format="DD/MM/YYYY", key="kame_res_fecha")
            usuario = st.text_input("Usuario", value=get_operator_name() or "ADMIN", key="kame_res_usuario")
        c4, c5 = st.columns(2)
        with c4:
            bodega_salida = st.text_input("Bodega Salida", value="BODEGA UNIVERSAL", key="kame_res_bodega")
            unidad_negocio = st.text_input("Nombre Unidad de Negocio", value="Casa Matriz", key="kame_res_unidad")
        with c5:
            glosa = st.text_input("Glosa", value=f"Reserva FULL Mercado Libre {clean_text(lote.get('nombre',''))}", key="kame_res_glosa")

    if not alerts.empty:
        if has_errors:
            st.error("Hay errores que bloquean la reserva Kame.")
        else:
            st.warning("Hay avisos en la expansión de packs. Revisa antes de importar en Kame.")
        st.dataframe(alerts, use_container_width=True, hide_index=True, height=180)

    tab_prev, tab_final = st.tabs(["Expansión packs", "CSV final Kame"])
    with tab_prev:
        if expanded.empty:
            st.warning("Sin líneas para mostrar.")
        else:
            show_expanded = expanded[["codigo_ml", "sku_full", "descripcion_full", "unidades_full", "tipo_origen", "sku_reserva", "descripcion_reserva", "factor_pack", "cantidad_reserva"]].copy()
            show_expanded["unidades_full"] = show_expanded["unidades_full"].map(format_kame_qty)
            show_expanded["factor_pack"] = show_expanded["factor_pack"].map(format_kame_qty)
            show_expanded["cantidad_reserva"] = show_expanded["cantidad_reserva"].map(format_kame_qty)
            st.dataframe(show_expanded, use_container_width=True, hide_index=True, height=360)
    with tab_final:
        if grouped.empty:
            st.warning("Sin líneas finales para CSV.")
        else:
            show_grouped = grouped[["sku_reserva", "descripcion_reserva", "cantidad_kame", "lineas_origen", "packs_expandidos"]].copy()
            st.dataframe(show_grouped, use_container_width=True, hide_index=True, height=360)
            st.caption(f"Total reserva Kame: {format_kame_qty(total_reserva)} unidades finales después de expandir packs y agrupar SKU.")

    archivo_nombre = f"reserva_kame_lote_{int(active_lote):03d}_folio_{clean_text(folio) or 'SIN_FOLIO'}.csv"
    can_download = bool(clean_text(folio) and clean_text(ficha) and not grouped.empty and not has_errors)
    if not clean_text(folio):
        st.info("Ingresa un folio Kame para habilitar la descarga.")
    if has_errors:
        st.info("Corrige los errores de packs/SKU antes de descargar el archivo.")

    csv_bytes = build_kame_reserva_csv(grouped, folio, ficha, fecha_doc, glosa, bodega_salida, unidad_negocio, folio_auto) if not grouped.empty else b""
    st.download_button(
        "Descargar CSV reserva Kame",
        data=csv_bytes,
        file_name=archivo_nombre,
        mime="text/csv",
        type="primary",
        disabled=not can_download,
        on_click=register_reserva_kame,
        args=(active_lote, folio, folio_auto, ficha, fecha_doc, glosa, bodega_salida, unidad_negocio, archivo_nombre, usuario, expanded, grouped),
        key=f"download_reserva_kame_{active_lote}_{clean_text(folio)}",
    )

    with db() as c:
        hist = pd.read_sql_query(
            """
            SELECT created_at, folio, ficha, fecha, sku_count, unidades_total, productos_full, packs_expandidos, lineas_csv, archivo_nombre, csv_hash, usuario
            FROM reservas_kame
            WHERE lote_id=?
            ORDER BY id DESC
            LIMIT 20
            """,
            c,
            params=(int(active_lote),),
        )
    if not hist.empty:
        with st.expander("Historial local de reservas Kame generadas", expanded=False):
            hist["created_at"] = hist["created_at"].map(fmt_dt)
            hist["unidades_total"] = hist["unidades_total"].map(format_kame_qty)
            if "csv_hash" in hist.columns:
                hist["csv_hash"] = hist["csv_hash"].astype(str).str.slice(0, 12)
            st.dataframe(hist, use_container_width=True, hide_index=True, height=220)



# ============================================================
# Rescate controlado desde Sheets
# ============================================================

def normalize_sheet_event(raw_ev: dict) -> dict:
    """Normaliza una fila leída desde la hoja eventos.

    Apps Script devuelve columnas visibles + raw_json. Para restaurar de forma confiable
    usamos raw_json cuando existe, pero conservamos las columnas visibles como respaldo.
    """
    base = dict(raw_ev or {})
    raw = base.get("raw_json")
    if raw:
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                base.update(parsed)
        except Exception:
            pass
    return base


def sheet_event_timestamp(ev: dict) -> str:
    return clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", ""))


def make_sheet_lote_scope_key(lote_id, lote_nombre="", archivo="", hoja="") -> str:
    """Llave de rescate para separar FULL con el mismo id SQLite histórico.

    Antes el rescate agrupaba solo por lote_id. Cuando una instalación nueva volvió a
    crear el id 2, Sheets contenía también un FULL antiguo con id 2 y ambos quedaron
    mezclados. La firma usa los metadatos reales del FULL, que sí viajan en los eventos.
    """
    try:
        lid = int(lote_id)
    except Exception:
        lid = 0
    raw = "|".join([
        str(lid),
        clean_text(lote_nombre).upper(),
        clean_text(archivo).upper(),
        clean_text(hoja).upper(),
    ])
    return "SCOPE-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def sheet_event_lote_scope_key(ev: dict) -> str:
    lote_nombre = clean_text(ev.get("lote_nombre", ""))
    archivo = clean_text(ev.get("archivo", ""))
    hoja = clean_text(ev.get("hoja", ""))
    # Eventos nuevos llevan backup_lote_key. Solo se usa como fallback cuando una
    # fila estructurada de Sheets no trae los metadatos del FULL.
    if not (lote_nombre or archivo or hoja):
        explicit = clean_text(ev.get("backup_lote_key", ""))
        if explicit:
            return "SCOPEKEY-" + hashlib.sha256(explicit.encode("utf-8")).hexdigest()[:24]
    return make_sheet_lote_scope_key(ev.get("lote_id", ""), lote_nombre, archivo, hoja)


def local_lote_scope_map() -> dict:
    """Mapa de FULL locales por identidad de rescate, no solo por id SQLite."""
    out = {}
    with db() as c:
        rows = c.execute("SELECT id, nombre, archivo, hoja FROM lotes").fetchall()
    for r in rows:
        data = dict(r)
        scope = make_sheet_lote_scope_key(data.get("id"), data.get("nombre", ""), data.get("archivo", ""), data.get("hoja", ""))
        out.setdefault(scope, []).append(int(data.get("id")))
    return out


def get_sheet_events_normalized() -> tuple[bool, list[dict], str]:
    ok, events, msg = get_backup_events_from_sheets()
    if not ok:
        return False, [], msg
    normalized = []
    seen_event_ids = set()
    for raw_ev in events:
        ev = normalize_sheet_event(raw_ev)
        if sheet_event_seen_or_mark(ev, seen_event_ids):
            continue
        normalized.append(ev)

    def key(ev):
        qid = clean_text(ev.get("queue_id", ""))
        try:
            qorder = int(qid)
        except Exception:
            qorder = 0
        return (sheet_event_timestamp(ev), qorder, sheet_event_semantic_identity(ev))

    normalized.sort(key=key)
    return True, normalized, f"Eventos normalizados: {len(normalized)}"


def summarize_sheet_lotes(events: list[dict]) -> pd.DataFrame:
    """Resume candidatos Sheets por identidad real de FULL, no por lote_id aislado."""
    local_scopes = local_lote_scope_map()
    lotes = {}

    def ensure(scope: str, lote_id: int) -> dict:
        if scope not in lotes:
            local_ids = local_scopes.get(scope, [])
            lotes[scope] = {
                "lote_scope_key": scope,
                "lote_id": int(lote_id),
                "lote_nombre": f"Lote {int(lote_id)}",
                "archivo": "",
                "hoja": "",
                "estado": "ACTIVO",
                "created_at": "",
                "ultimo_evento": "",
                "productos": 0,
                "unidades": 0,
                "escaneos": 0,
                "unidades_escaneadas": 0,
                "picking_listas": 0,
                "incidencias": 0,
                "avisos": 0,
                "reimpresiones": 0,
                "reservas_kame": 0,
                "existe_local": "SI" if local_ids else "NO",
                "local_ids": ",".join(str(x) for x in local_ids),
            }
        return lotes[scope]

    for ev in events:
        lote_id = _event_lote_id(ev)
        if not lote_id:
            continue
        et = clean_text(ev.get("event_type", ""))
        if not et or et == "test_webhook":
            continue
        scope = sheet_event_lote_scope_key(ev)
        rec = ensure(scope, lote_id)
        if clean_text(ev.get("lote_nombre", "")):
            rec["lote_nombre"] = clean_text(ev.get("lote_nombre", ""))
        if clean_text(ev.get("archivo", "")):
            rec["archivo"] = clean_text(ev.get("archivo", ""))
        if clean_text(ev.get("hoja", "")):
            rec["hoja"] = clean_text(ev.get("hoja", ""))
        ts = sheet_event_timestamp(ev)
        if ts and not rec["created_at"]:
            rec["created_at"] = ts
        if ts:
            rec["ultimo_evento"] = max(clean_text(rec.get("ultimo_evento", "")), ts)

        et_low = et.lower()
        if et == "lote_creado":
            rec["estado"] = clean_text(ev.get("status", rec["estado"])) or rec["estado"]
        elif et == "lote_cerrado":
            rec["estado"] = "CERRADO"
        elif et == "lote_reabierto":
            rec["estado"] = "ACTIVO"
        elif et == "lote_eliminado":
            rec["estado"] = "ELIMINADO"
        elif "reimpresion" in et_low:
            rec["reimpresiones"] = int(rec["reimpresiones"] or 0) + 1

    for scope, rec in lotes.items():
        try:
            state = build_sheet_lote_state_clean(events, int(rec["lote_id"]), lote_scope_key=scope)
            items_state = state.get("items", []) or []
            rec["productos"] = len(items_state)
            rec["unidades"] = sum(float(to_float_qty(x.get("unidades", 0))) for x in items_state if isinstance(x, dict))
            rec["escaneos"] = len(state.get("scans", []) or [])
            rec["unidades_escaneadas"] = sum(float(to_int(x.get("acopiadas", 0))) for x in items_state if isinstance(x, dict))
            rec["picking_listas"] = len(state.get("picking_lists", []) or [])
            rec["incidencias"] = len(state.get("incidencias", []) or [])
            rec["avisos"] = len(state.get("avisos", []) or [])
            rec["reservas_kame"] = len(state.get("reservas", []) or [])
        except Exception:
            # El listado se mantiene disponible incluso si un lote histórico tiene eventos incompletos.
            pass

    if not lotes:
        return pd.DataFrame()
    df = pd.DataFrame(list(lotes.values()))
    df["created_at_fmt"] = df["created_at"].map(fmt_dt)
    df["ultimo_evento_fmt"] = df["ultimo_evento"].map(fmt_dt)
    return df.sort_values(["ultimo_evento", "lote_nombre", "lote_id"], ascending=[False, True, False]).reset_index(drop=True)


def get_sheet_lote_items_from_events(events: list[dict], lote_id: int) -> pd.DataFrame:
    rows = []
    for ev in events:
        try:
            ev_lote_id = int(ev.get("lote_id"))
        except Exception:
            continue
        if ev_lote_id != int(lote_id):
            continue
        et = clean_text(ev.get("event_type", ""))
        if et == "lote_item":
            rows.append({
                "item_id": ev.get("item_id", ""),
                "area": clean_text(ev.get("area", "")),
                "nro": clean_text(ev.get("nro", "")),
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_kame": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_ml": clean_text(ev.get("descripcion_ml", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_fuente": clean_text(ev.get("descripcion_fuente", "")),
                "familia_kame": clean_text(ev.get("familia_kame", "")),
                "maestro_match_status": clean_text(ev.get("maestro_match_status", "")),
                "origen_item": clean_text(ev.get("origen_item", "PDF_FULL")) or "PDF_FULL",
                "motivo_anexo": clean_text(ev.get("motivo_anexo", "")),
                "usuario_anexo": clean_text(ev.get("usuario_anexo", "")),
                "fecha_anexo": clean_text(ev.get("fecha_anexo", "")),
                "anexo_ml_confirmado": to_int(ev.get("anexo_ml_confirmado", 0)),
                "anexo_ml_confirmado_at": clean_text(ev.get("anexo_ml_confirmado_at", "")),
                "anexo_ml_confirmado_by": clean_text(ev.get("anexo_ml_confirmado_by", "")),
                "anexo_ml_confirmado_comment": clean_text(ev.get("anexo_ml_confirmado_comment", "")),
                "anexo_kame_confirmado": to_int(ev.get("anexo_kame_confirmado", 0)),
                "anexo_kame_confirmado_at": clean_text(ev.get("anexo_kame_confirmado_at", "")),
                "anexo_kame_confirmado_by": clean_text(ev.get("anexo_kame_confirmado_by", "")),
                "anexo_kame_confirmado_comment": clean_text(ev.get("anexo_kame_confirmado_comment", "")),
                "unidades": to_int(ev.get("unidades", 0)),
                "identificacion": clean_text(ev.get("identificacion", "")),
                "vence": clean_text(ev.get("vence", "")),
            })
        elif et == "lote_snapshot_chunk":
            for item in ev.get("items") or []:
                if isinstance(item, dict):
                    rows.append({
                        "item_id": item.get("item_id", ""),
                        "area": clean_text(item.get("area", "")),
                        "nro": clean_text(item.get("nro", "")),
                        "codigo_ml": norm_code(item.get("codigo_ml", "")),
                        "codigo_universal": norm_code(item.get("codigo_universal", "")),
                        "sku": norm_code(item.get("sku", "")),
                        "descripcion": clean_text(item.get("descripcion", "")),
                        "unidades": to_int(item.get("unidades", 0)),
                        "identificacion": clean_text(item.get("identificacion", "")),
                        "vence": clean_text(item.get("vence", "")),
                    })
    return pd.DataFrame(rows)


def get_sheet_lote_events_df(events: list[dict], lote_id: int, lote_scope_key: str = "") -> pd.DataFrame:
    rows = []
    target_scope = clean_text(lote_scope_key)
    for ev in events:
        try:
            ev_lote_id = int(ev.get("lote_id"))
        except Exception:
            continue
        if ev_lote_id != int(lote_id):
            continue
        if target_scope and sheet_event_lote_scope_key(ev) != target_scope:
            continue
        rows.append({
            "fecha": fmt_dt(sheet_event_timestamp(ev)),
            "event_type": clean_text(ev.get("event_type", "")),
            "queue_id": clean_text(ev.get("queue_id", "")),
            "item_id": clean_text(ev.get("item_id", "")),
            "codigo_ml": clean_text(ev.get("codigo_ml", "")),
            "sku": clean_text(ev.get("sku", "")),
            "cantidad": clean_text(ev.get("cantidad", ev.get("unidades", ""))),
            "detalle": clean_text(ev.get("descripcion", "")) or clean_text(ev.get("comentario", "")) or clean_text(ev.get("detail", "")),
        })
    return pd.DataFrame(rows)


def render_rescate_sheets():
    st.subheader("Rescate desde Sheets")
    st.caption("Sheets muestra candidatos, revisas el FULL y la app restaura solo el lote que elijas. No se cambia el lote activo automáticamente.")

    c1, c2 = st.columns([1, 3])
    with c1:
        load_now = st.button("Actualizar candidatos", type="primary", use_container_width=True)
    with c2:
        st.info("Usa este módulo para revisar FULL antiguos o recuperar un lote real cuando SQLite/Streamlit tenga una base local parcial.")

    if load_now or "sheet_rescue_events" not in st.session_state:
        ok, events, msg = get_sheet_events_normalized()
        if not ok:
            st.error(msg)
            return
        st.session_state["sheet_rescue_events"] = events
        st.session_state["sheet_rescue_msg"] = msg

    events = st.session_state.get("sheet_rescue_events") or []
    if not events:
        st.warning("No hay eventos disponibles desde Sheets.")
        return

    candidates = summarize_sheet_lotes(events)
    if candidates.empty:
        st.warning("No encontré lotes candidatos en Sheets.")
        return

    f1, f2, f3 = st.columns([2, 1, 1])
    with f1:
        buscar = st.text_input("Buscar FULL", placeholder="Ej: Full 1405, PDF ML, lote...", key="sheet_rescue_buscar")
    with f2:
        estado = st.selectbox("Estado", ["Todos", "ACTIVO", "CERRADO", "ELIMINADO"], key="sheet_rescue_estado")
    with f3:
        mostrar_locales = st.checkbox("Mostrar ya locales", value=True, key="sheet_rescue_locales")

    show = candidates.copy()
    if clean_text(buscar):
        q = clean_text(buscar).lower()
        show = show[show["lote_nombre"].astype(str).str.lower().str.contains(re.escape(q), na=False)]
    if estado != "Todos":
        show = show[show["estado"] == estado]
    if not mostrar_locales:
        show = show[show["existe_local"] != "SI"]

    table_cols = ["estado", "lote_id", "lote_nombre", "archivo", "created_at_fmt", "ultimo_evento_fmt", "productos", "unidades", "unidades_escaneadas", "picking_listas", "incidencias", "avisos", "reservas_kame", "existe_local"]
    st.dataframe(show[table_cols].rename(columns={
        "estado": "Estado",
        "lote_id": "Lote ID",
        "lote_nombre": "Lote",
        "archivo": "Archivo",
        "created_at_fmt": "Creado",
        "ultimo_evento_fmt": "Último evento",
        "productos": "Productos",
        "unidades": "Unidades",
        "unidades_escaneadas": "Escaneado",
        "picking_listas": "Picking",
        "incidencias": "Incidencias",
        "avisos": "Avisos",
        "reservas_kame": "Reservas Kame",
        "existe_local": "Local",
    }), use_container_width=True, hide_index=True, height=320)

    if show.empty:
        st.warning("Sin candidatos con esos filtros.")
        return

    option_map = {}
    options = []
    for r in show.itertuples(index=False):
        label = (
            f"{r.lote_id} · {r.estado} · {r.lote_nombre} · {clean_text(r.archivo) or 'SIN ARCHIVO'} "
            f"· último {fmt_dt(r.ultimo_evento)} · local {r.existe_local}"
        )
        options.append(label)
        option_map[label] = clean_text(r.lote_scope_key)

    selected_label = st.selectbox("Lote a revisar/restaurar", options, key="sheet_rescue_selected")
    selected_scope_key = option_map[selected_label]
    selected_row = candidates[candidates["lote_scope_key"] == selected_scope_key].iloc[0].to_dict()
    selected_lote_id = int(selected_row["lote_id"])
    selected_exists_local = clean_text(selected_row.get("existe_local", "")) == "SI"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Productos", int(selected_row.get("productos") or 0))
    m2.metric("Unidades", format_kame_qty(float(selected_row.get("unidades") or 0)) if 'format_kame_qty' in globals() else int(selected_row.get("unidades") or 0))
    m3.metric("Escaneado", format_kame_qty(float(selected_row.get("unidades_escaneadas") or 0)) if 'format_kame_qty' in globals() else int(selected_row.get("unidades_escaneadas") or 0))
    m4.metric("Estado", clean_text(selected_row.get("estado", "")))

    state_preview = build_sheet_lote_state_clean(events, selected_lote_id, lote_scope_key=selected_scope_key)
    items_df = get_sheet_lote_items_from_events(events, selected_lote_id, lote_scope_key=selected_scope_key)
    events_df = get_sheet_lote_events_df(events, selected_lote_id, lote_scope_key=selected_scope_key)
    integ_preview = state_preview.get("integrity", {}) or {}
    if integ_preview.get("snapshot_selection_warning"):
        st.warning(
            "Protección de integridad activa: "
            + clean_text(integ_preview.get("snapshot_selection_warning", ""))
        )
    if integ_preview.get("fallback_from_picking"):
        st.warning(f"Este rescate incorporará {integ_preview.get('fallback_from_picking')} producto(s) que están en eventos de picking pero no aparecían en lote_item. Quedarán marcados como PICKING_FALLBACK para trazabilidad.")
    if integ_preview.get("unmatched_scans") or integ_preview.get("ambiguous_scans"):
        st.info(f"Acopios recuperables desde Sheets: {integ_preview.get('unmatched_scans',0)}. Ambiguos: {integ_preview.get('ambiguous_scans',0)}. La app los conserva como válidos del mismo lote y no mezcla otros FULL.")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Productos", "Eventos", "Picking/escaneos", "Incidencias y avisos", "Diagnóstico rescate"])
    with tab1:
        if items_df.empty:
            st.warning("Este lote no tiene snapshot de productos visible en Sheets. No se puede restaurar con seguridad.")
        else:
            st.dataframe(items_df, use_container_width=True, hide_index=True, height=360)
    with tab2:
        st.dataframe(events_df, use_container_width=True, hide_index=True, height=360)
    with tab3:
        mov = events_df[events_df["event_type"].isin(["scan_agregado", "scan_deshacer", "picking_lista_creada", "picking_lista_impresa", "picking_lista_completada", "picking_lista_anulada"])] if not events_df.empty else pd.DataFrame()
        if mov.empty:
            st.info("Sin eventos de picking/escaneo para este lote.")
        else:
            st.dataframe(mov, use_container_width=True, hide_index=True, height=320)
    with tab4:
        ia = events_df[events_df["event_type"].astype(str).str.contains("incidencia|aviso_operacional|postventa_full|reserva_kame|zpl_etiquetas", case=False, na=False)] if not events_df.empty else pd.DataFrame()
        if ia.empty:
            st.info("Sin incidencias, avisos, postventa, reservas ni etiquetas para este lote.")
        else:
            st.dataframe(ia, use_container_width=True, hide_index=True, height=320)
    with tab5:
        diag_df = diagnose_sheet_lote_state(state_preview)
        st.caption("Compara lo que existe en Sheets con lo que la app reconstruirá localmente. Sirve para detectar módulos que no se levantan visualmente después del rescate.")
        st.dataframe(diag_df, use_container_width=True, hide_index=True, height=360)

    st.divider()
    with st.expander("Reconciliar lote contra PDF corregido de Mercado Libre", expanded=False):
        st.warning("Usa esto solo cuando Mercado Libre entregue un PDF corregido. No borra escaneos, listas ni auditoría: compara, muestra diferencias y exige confirmación antes de aplicar.")
        if not selected_exists_local:
            st.info("Primero restaura este lote localmente con el botón de abajo. Después vuelve aquí para reconciliarlo contra el PDF corregido.")
        else:
            pdf_corr = st.file_uploader("PDF corregido de Mercado Libre", type=["pdf"], key=f"pdf_reconcile_{selected_lote_id}")
            usuario_corr = st.selectbox("Usuario que autoriza la reconciliación", SCAN_OPERATORS + ["ADMIN"], key=f"pdf_reconcile_user_{selected_lote_id}")
            comentario_corr = st.text_input("Motivo / respaldo", value="PDF corregido por Mercado Libre", key=f"pdf_reconcile_note_{selected_lote_id}")
            if pdf_corr is not None:
                try:
                    plan = build_pdf_reconciliation_plan(int(selected_lote_id), pdf_corr)
                except Exception as e:
                    plan = {"ok": False, "error": f"No pude comparar el PDF: {e}"}
                if not plan.get("ok"):
                    st.error(clean_text(plan.get("error", "No se pudo construir el plan de reconciliación.")))
                else:
                    q1, q2, q3, q4 = st.columns(4)
                    q1.metric("Local actual", f"{plan.get('local_products',0)} productos / {plan.get('local_units',0)} unid.")
                    q2.metric("PDF corregido", f"{plan.get('expected_products',0)} productos / {plan.get('expected_units',0)} unid.")
                    q3.metric("Cambios", len(plan.get("changes", [])))
                    q4.metric("Diferencia", int(plan.get("expected_units",0)) - int(plan.get("local_units",0)))
                    diff_rows = []
                    for ch in plan.get("changes", []):
                        it = ch.get("item") or {}
                        diff_rows.append({
                            "Acción": clean_text(ch.get("action", "")),
                            "Código ML": norm_code(it.get("codigo_ml", "")),
                            "SKU": norm_code(it.get("sku", "")),
                            "Producto": clean_text(it.get("descripcion_ml", "")) or clean_text(it.get("descripcion", "")),
                            "Antes": to_int(ch.get("unidades_before", 0)),
                            "Después": to_int(ch.get("unidades_after", 0)),
                            "Motivo": clean_text(ch.get("motivo", "")),
                        })
                    if diff_rows:
                        st.dataframe(pd.DataFrame(diff_rows), use_container_width=True, hide_index=True)
                    else:
                        st.success("El lote ya coincide con este PDF; no hay cambios que aplicar.")
                    confirm_pdf = st.checkbox(
                        f"Confirmo aplicar esta reconciliación documental al lote {selected_lote_id}. El PDF declara {plan.get('expected_products')} productos y {plan.get('expected_units')} unidades.",
                        key=f"pdf_reconcile_confirm_{selected_lote_id}",
                    )
                    if st.button("Aplicar reconciliación PDF", type="primary", disabled=(not confirm_pdf or not diff_rows), key=f"pdf_reconcile_apply_{selected_lote_id}"):
                        ok_apply, msg_apply = apply_pdf_reconciliation_plan(plan, usuario_corr, comentario_corr)
                        if ok_apply:
                            st.success(msg_apply)
                            st.info("El respaldo quedó en cola. Espera que sincronice Sheets antes de usar Rescate de nuevo; el próximo snapshot debe reflejar exactamente el PDF corregido.")
                            st.rerun()
                        else:
                            st.error(msg_apply)

    if items_df.empty:
        st.error("No se puede restaurar: falta snapshot de productos lote_item/lote_snapshot_chunk en Sheets.")
        return

    exists_local = selected_exists_local
    rescue_key_suffix = hashlib.sha1(selected_scope_key.encode("utf-8")).hexdigest()[:10]
    confirm = st.checkbox(
        f"Confirmo restaurar {'y reemplazar localmente' if exists_local else ''} el lote {selected_lote_id}: {clean_text(selected_row.get('lote_nombre',''))}",
        key=f"confirm_rescue_{selected_lote_id}_{rescue_key_suffix}",
    )
    btn_label = "Re-sincronizar lote seleccionado" if exists_local else "Restaurar lote seleccionado"
    if st.button(btn_label, type="primary", disabled=not confirm, key=f"restore_sheet_lote_{selected_lote_id}_{rescue_key_suffix}"):
        ok_restore, msg_restore = restore_lote_from_sheet_events_clean(
            events,
            int(selected_lote_id),
            replace_existing=True,
            lote_scope_key=selected_scope_key,
        )
        st.session_state["_auto_restore_ok"] = ok_restore
        st.session_state["_auto_restore_msg"] = msg_restore
        if ok_restore:
            st.success(msg_restore)
            st.info("Ahora selecciona este lote en el selector de Lote activo. No se cambió automáticamente.")
            st.rerun()
        else:
            st.error(msg_restore)



# ============================================================
# Rescate Sheets limpio / event-sourced
# ============================================================

def _event_lote_id(ev) -> int | None:
    try:
        lid = int(ev.get("lote_id"))
        return lid if lid > 0 else None
    except Exception:
        return None


def _event_key(ev) -> tuple:
    qid = clean_text(ev.get("queue_id", ""))
    try:
        qorder = int(qid)
    except Exception:
        qorder = 0
    return (sheet_event_timestamp(ev), qorder, sheet_event_semantic_identity(ev))


def _product_key_from_values(codigo_ml="", codigo_universal="", sku="", item_id="") -> str:
    ml = norm_code(codigo_ml)
    ean = norm_code(codigo_universal)
    sk = norm_code(sku)
    if ml:
        return f"ML:{ml}"
    if ean and ean != "N/A":
        return f"EAN:{ean}"
    if sk:
        return f"SKU:{sk}"
    iid = clean_text(item_id)
    return f"ITEM:{iid}" if iid else ""


def _product_key_from_event(ev: dict) -> str:
    return _product_key_from_values(ev.get("codigo_ml", ""), ev.get("codigo_universal", ""), ev.get("sku", ""), ev.get("item_id", ""))


def _stable_negative_id(key: str, used_ids: set[int]) -> int:
    base = -1 * (int(hashlib.sha1(clean_text(key).encode("utf-8")).hexdigest()[:8], 16) % 900000000 + 100000)
    candidate = base
    while candidate in used_ids:
        candidate -= 1
    return candidate


def _to_bool_flag(v) -> int:
    s = clean_text(v).upper()
    return 1 if v in [1, True] or s in {"1", "TRUE", "SI", "SÍ", "YES"} else 0


def build_sheet_lote_state_clean(events: list[dict], lote_id: int, lote_scope_key: str = "") -> dict:
    """Reconstruye un lote desde el journal de eventos, sin cruzar datos de otros FULL.

    Reglas:
    - Filtra estrictamente por lote_id seleccionado.
    - Lote_item/lote_snapshot_chunk son snapshot primario.
    - Si picking/scan referencia un producto que no existe en el snapshot primario,
      se crea un producto de respaldo con fuente_rescate=PICKING_FALLBACK/SCAN_FALLBACK.
    - No se suman duplicados de picking: si un producto aparece varias veces fuera del
      snapshot, se conserva una sola ficha y se toma la mayor cantidad vista para no
      duplicar unidades por reimpresiones o recreaciones.
    """
    target = int(lote_id)
    target_scope = clean_text(lote_scope_key)
    lote_events = []
    _seen_lote_event_ids = set()
    for _raw_ev in (events or []):
        _ev = normalize_sheet_event(_raw_ev)
        if _event_lote_id(_ev) != target:
            continue
        if target_scope and sheet_event_lote_scope_key(_ev) != target_scope:
            continue
        if sheet_event_seen_or_mark(_ev, _seen_lote_event_ids):
            continue
        lote_events.append(_ev)
    lote_events.sort(key=_event_key)

    # Elegir un único snapshot completo sin aceptar una contaminación de otro FULL.
    # En un lote sin conciliación documental, el snapshot debe conservar los totales
    # declarados al crear el FULL. Si aparece luego un AUTO_ACTIVE_LOTE con más
    # productos/unidades y no existe una reconciliación PDF, se descarta como mezcla.
    snapshot_groups = {}
    declared_totals = []
    has_documented_reconciliation = any(
        clean_text(ev.get("event_type", "")).lower() == PDF_RECONCILIATION_EVENT
        for ev in lote_events
    )
    for ev in lote_events:
        et = clean_text(ev.get("event_type", ""))
        if et == "lote_creado" or (et == "lote_snapshot_completo" and clean_text(ev.get("motivo_snapshot", "")).upper() == "LOTE_CREADO"):
            p = to_int(ev.get("total_lineas", ev.get("productos_total", 0)))
            u = to_int(ev.get("total_unidades", ev.get("unidades_total", 0)))
            if p > 0 or u > 0:
                declared_totals.append((sheet_event_timestamp(ev), p, u))

    for idx, ev in enumerate(lote_events):
        if clean_text(ev.get("event_type", "")) != "lote_snapshot_chunk":
            continue
        total_chunks = max(1, to_int(ev.get("chunk_total", 0)) or 1)
        chunk_index = to_int(ev.get("chunk_index", 0)) or 1
        snap_hash = clean_text(ev.get("snapshot_hash", "")) or ("SNAP:" + "|".join([
            clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")),
            clean_text(ev.get("productos_total", "")), clean_text(ev.get("unidades_total", "")), str(total_chunks),
        ]))
        g = snapshot_groups.setdefault(snap_hash, {
            "chunks": {},
            "total": total_chunks,
            "order": idx,
            "ts": sheet_event_timestamp(ev),
            "productos_total": to_int(ev.get("productos_total", 0)),
            "unidades_total": to_int(ev.get("unidades_total", 0)),
        })
        g["total"] = max(int(g["total"]), total_chunks)
        g["chunks"][chunk_index] = ev
        g["order"] = max(int(g["order"]), idx)
        g["ts"] = max(clean_text(g.get("ts", "")), clean_text(sheet_event_timestamp(ev)))
        g["productos_total"] = to_int(ev.get("productos_total", g.get("productos_total", 0)))
        g["unidades_total"] = to_int(ev.get("unidades_total", g.get("unidades_total", 0)))

    valid_snapshots = []
    for h, g in snapshot_groups.items():
        if len(g.get("chunks", {})) >= int(g.get("total", 1)):
            valid_snapshots.append((clean_text(g.get("ts", "")), int(g.get("order", 0)), h, g))

    selected_snapshot_hash = ""
    selected_snapshot_events = set()
    snapshot_selection_warning = ""
    if valid_snapshots:
        candidates = valid_snapshots
        if declared_totals and not has_documented_reconciliation:
            _, expected_products, expected_units = sorted(declared_totals, key=lambda x: x[0])[0]
            anchored = [
                row for row in valid_snapshots
                if to_int(row[3].get("productos_total", 0)) == int(expected_products)
                and to_int(row[3].get("unidades_total", 0)) == int(expected_units)
            ]
            if anchored:
                candidates = anchored
                rejected = len(valid_snapshots) - len(anchored)
                if rejected:
                    snapshot_selection_warning = (
                        f"Se descartaron {rejected} snapshot(s) posterior(es) con totales distintos "
                        f"al FULL creado ({expected_products} productos / {expected_units} unidades) y sin conciliación PDF."
                    )
        _, _, selected_snapshot_hash, selected_group = max(candidates, key=lambda x: (x[0], x[1]))
        selected_snapshot_events = {
            sheet_event_semantic_identity(ev) for ev in selected_group.get("chunks", {}).values()
        }

    meta = {
        "id": target,
        "nombre": f"Lote {target}",
        "archivo": "",
        "hoja": "",
        "created_at": "",
        "status": "ACTIVO",
        "closed_at": "",
        "closed_by": "",
        "close_note": "",
        "backup_lote_key": "",
        "lote_scope_key": target_scope,
    }

    def touch_meta(ev: dict):
        if clean_text(ev.get("lote_nombre", "")):
            meta["nombre"] = clean_text(ev.get("lote_nombre", ""))
        if clean_text(ev.get("archivo", "")):
            meta["archivo"] = clean_text(ev.get("archivo", ""))
        if clean_text(ev.get("hoja", "")):
            meta["hoja"] = clean_text(ev.get("hoja", ""))
        if clean_text(ev.get("backup_lote_key", "")):
            meta["backup_lote_key"] = clean_text(ev.get("backup_lote_key", ""))
        ts = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", ""))
        if ts and not meta["created_at"]:
            meta["created_at"] = ts

    # Producto canónico del lote reconstruido.
    items_by_id: dict[int, dict] = {}
    key_to_id: dict[str, int] = {}
    used_ids: set[int] = set()
    warnings = []
    if snapshot_selection_warning:
        warnings.append(snapshot_selection_warning)

    def add_or_update_item(raw: dict, source: str, note: str = "") -> int | None:
        codigo_ml = norm_code(raw.get("codigo_ml", ""))
        codigo_universal = norm_code(raw.get("codigo_universal", ""))
        sku = norm_code(raw.get("sku", ""))
        descripcion = clean_text(raw.get("descripcion", ""))
        key = _product_key_from_values(codigo_ml, codigo_universal, sku, raw.get("item_id", ""))
        if not key:
            return None

        requested_id = to_int(raw.get("item_id", 0))
        existing_id = key_to_id.get(key)
        if existing_id is not None:
            item = items_by_id[existing_id]
            # Nunca duplicar unidades por reimpresiones/reasignaciones. Si el producto
            # vino desde fallback, conservar la mayor cantidad completa observada.
            if item.get("fuente_rescate") != "LOTE_ITEM" and source != "LOTE_ITEM":
                item["unidades"] = max(to_int(item.get("unidades", 0)), to_int(raw.get("unidades", raw.get("cantidad", 0))))
            # El snapshot primario manda por sobre cualquier fallback.
            if source == "LOTE_ITEM" and item.get("fuente_rescate") != "LOTE_ITEM":
                item.update({
                    "area": clean_text(raw.get("area", item.get("area", ""))),
                    "nro": clean_text(raw.get("nro", item.get("nro", ""))),
                    "codigo_ml": codigo_ml,
                    "codigo_universal": codigo_universal,
                    "sku": sku,
                    "descripcion": clean_text(raw.get("descripcion_kame", "")) or descripcion or item.get("descripcion", ""),
                    "descripcion_kame": clean_text(raw.get("descripcion_kame", "")) or descripcion or item.get("descripcion", ""),
                    "descripcion_ml": clean_text(raw.get("descripcion_ml", "")) or item.get("descripcion_ml", "") or descripcion,
                    "descripcion_fuente": clean_text(raw.get("descripcion_fuente", item.get("descripcion_fuente", ""))),
                    "familia_kame": clean_text(raw.get("familia_kame", item.get("familia_kame", ""))),
                    "maestro_match_status": clean_text(raw.get("maestro_match_status", item.get("maestro_match_status", ""))),
                    "origen_item": clean_text(raw.get("origen_item", item.get("origen_item", "PDF_FULL"))) or "PDF_FULL",
                    "motivo_anexo": clean_text(raw.get("motivo_anexo", item.get("motivo_anexo", ""))),
                    "usuario_anexo": clean_text(raw.get("usuario_anexo", item.get("usuario_anexo", ""))),
                    "fecha_anexo": clean_text(raw.get("fecha_anexo", item.get("fecha_anexo", ""))),
                    "anexo_ml_confirmado": to_int(raw.get("anexo_ml_confirmado", item.get("anexo_ml_confirmado", 0))),
                    "anexo_ml_confirmado_at": clean_text(raw.get("anexo_ml_confirmado_at", item.get("anexo_ml_confirmado_at", ""))),
                    "anexo_ml_confirmado_by": clean_text(raw.get("anexo_ml_confirmado_by", item.get("anexo_ml_confirmado_by", ""))),
                    "anexo_ml_confirmado_comment": clean_text(raw.get("anexo_ml_confirmado_comment", item.get("anexo_ml_confirmado_comment", ""))),
                    "anexo_kame_confirmado": to_int(raw.get("anexo_kame_confirmado", item.get("anexo_kame_confirmado", 0))),
                    "anexo_kame_confirmado_at": clean_text(raw.get("anexo_kame_confirmado_at", item.get("anexo_kame_confirmado_at", ""))),
                    "anexo_kame_confirmado_by": clean_text(raw.get("anexo_kame_confirmado_by", item.get("anexo_kame_confirmado_by", ""))),
                    "anexo_kame_confirmado_comment": clean_text(raw.get("anexo_kame_confirmado_comment", item.get("anexo_kame_confirmado_comment", ""))),
                    "unidades": to_int(raw.get("unidades", raw.get("cantidad", 0))),
                    "identificacion": clean_text(raw.get("identificacion", item.get("identificacion", ""))),
                    "vence": clean_text(raw.get("vence", item.get("vence", ""))),
                    "instrucciones": clean_text(raw.get("instrucciones", item.get("instrucciones", ""))),
                    "fuente_rescate": "LOTE_ITEM",
                    "rescue_note": "snapshot primario desde lote_item",
                })
            return existing_id

        if requested_id and requested_id not in used_ids:
            item_id = requested_id
        else:
            item_id = _stable_negative_id(key, used_ids)
        used_ids.add(item_id)
        key_to_id[key] = item_id
        if codigo_ml:
            key_to_id.setdefault(f"ML:{codigo_ml}", item_id)
        if codigo_universal and codigo_universal != "N/A":
            key_to_id.setdefault(f"EAN:{codigo_universal}", item_id)
        if sku:
            key_to_id.setdefault(f"SKU:{sku}", item_id)

        items_by_id[item_id] = {
            "id": item_id,
            "lote_id": target,
            "area": clean_text(raw.get("area", "")),
            "nro": clean_text(raw.get("nro", "")),
            "codigo_ml": codigo_ml,
            "codigo_universal": codigo_universal,
            "sku": sku,
            "descripcion": clean_text(raw.get("descripcion_kame", "")) or descripcion,
            "descripcion_kame": clean_text(raw.get("descripcion_kame", "")) or descripcion,
            "descripcion_ml": clean_text(raw.get("descripcion_ml", "")) or descripcion,
            "descripcion_fuente": clean_text(raw.get("descripcion_fuente", "")),
            "familia_kame": clean_text(raw.get("familia_kame", "")),
            "maestro_match_status": clean_text(raw.get("maestro_match_status", "")),
            "origen_item": clean_text(raw.get("origen_item", "PDF_FULL")) or "PDF_FULL",
            "motivo_anexo": clean_text(raw.get("motivo_anexo", "")),
            "usuario_anexo": clean_text(raw.get("usuario_anexo", "")),
            "fecha_anexo": clean_text(raw.get("fecha_anexo", "")),
            "anexo_ml_confirmado": to_int(raw.get("anexo_ml_confirmado", 0)),
            "anexo_ml_confirmado_at": clean_text(raw.get("anexo_ml_confirmado_at", "")),
            "anexo_ml_confirmado_by": clean_text(raw.get("anexo_ml_confirmado_by", "")),
            "anexo_ml_confirmado_comment": clean_text(raw.get("anexo_ml_confirmado_comment", "")),
            "anexo_kame_confirmado": to_int(raw.get("anexo_kame_confirmado", 0)),
            "anexo_kame_confirmado_at": clean_text(raw.get("anexo_kame_confirmado_at", "")),
            "anexo_kame_confirmado_by": clean_text(raw.get("anexo_kame_confirmado_by", "")),
            "anexo_kame_confirmado_comment": clean_text(raw.get("anexo_kame_confirmado_comment", "")),
            "unidades": to_int(raw.get("unidades", raw.get("cantidad", 0))),
            "acopiadas": 0,
            "identificacion": clean_text(raw.get("identificacion", "")),
            "vence": clean_text(raw.get("vence", "")),
            "instrucciones": clean_text(raw.get("instrucciones", "")),
            "dia": clean_text(raw.get("dia", "")),
            "hora": clean_text(raw.get("hora", "")),
            "created_at": clean_text(raw.get("item_created_at", "")) or clean_text(raw.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
            "updated_at": clean_text(raw.get("item_updated_at", "")) or clean_text(raw.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
            "fuente_rescate": source,
            "rescue_note": clean_text(note),
        }
        return item_id

    # 1) Snapshot primario.
    for ev in lote_events:
        touch_meta(ev)
        et = clean_text(ev.get("event_type", ""))
        if et == "lote_creado":
            meta["status"] = clean_text(ev.get("status", meta["status"])) or meta["status"]
        elif et == "lote_item":
            # lote_item es respaldo legado: solo se usa si no existe snapshot chunk completo.
            if not selected_snapshot_hash:
                add_or_update_item({**ev, "unidades": ev.get("unidades", ev.get("cantidad", 0))}, "LOTE_ITEM", "snapshot primario desde lote_item")
        elif et == "lote_snapshot_chunk":
            if selected_snapshot_hash and sheet_event_semantic_identity(ev) not in selected_snapshot_events:
                continue
            for it in ev.get("items") or []:
                if isinstance(it, dict):
                    add_or_update_item({**it, "created_at": ev.get("created_at", "")}, "LOTE_ITEM", "snapshot primario desde lote_snapshot_chunk")
        elif et == "producto_anexado_lote":
            add_or_update_item({**ev, "unidades": ev.get("unidades", ev.get("cantidad", 0)), "origen_item": "ANEXO_MANUAL"}, "LOTE_ITEM", "producto anexado manualmente")
        elif et == "lote_cerrado":
            meta["status"] = "CERRADO"
            meta["closed_at"] = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", ""))
            meta["closed_by"] = clean_text(ev.get("usuario", "")) or clean_text(ev.get("closed_by", ""))
            meta["close_note"] = clean_text(ev.get("comentario", "")) or clean_text(ev.get("close_note", ""))
        elif et == "lote_reabierto":
            meta["status"] = "ACTIVO"
            meta["closed_at"] = ""
            meta["closed_by"] = ""
            meta["close_note"] = ""
        elif et == "lote_eliminado":
            meta["status"] = "ELIMINADO"

    # Aplicar reconciliaciones oficiales de PDF después del snapshot base y antes de
    # reconstruir picking/scans. Así un reboot conserva la corrección de ML.
    reconciliation_events = [ev for ev in lote_events if clean_text(ev.get("event_type", "")).lower() == PDF_RECONCILIATION_EVENT]
    reconciliation_count = apply_pdf_reconciliation_events_to_item_map(items_by_id, reconciliation_events, target, used_ids)
    if reconciliation_count:
        key_to_id.clear()
        for iid, it in items_by_id.items():
            for key in [
                _product_key_from_values(it.get("codigo_ml", ""), "", "", ""),
                _product_key_from_values("", it.get("codigo_universal", ""), "", ""),
                _product_key_from_values("", "", it.get("sku", ""), ""),
            ]:
                if key:
                    key_to_id[key] = int(iid)
        warnings.append(f"Se aplicaron {reconciliation_count} cambio(s) de PDF corregido.")

    # 2) Picking con estado final por código/lista. Anulación es terminal.
    picking_lists = {}
    anuladas = set()
    for ev in lote_events:
        et = clean_text(ev.get("event_type", ""))
        code = clean_text(ev.get("picking_code", "")) or clean_text(ev.get("codigo_lista", "")) or clean_text(ev.get("picking_list_id", ""))
        if not code:
            continue
        if et in {"picking_lista_creada", "PICKING_LISTA_CREADA"}:
            try:
                plid = int(ev.get("picking_list_id")) if clean_text(ev.get("picking_list_id", "")) else None
            except Exception:
                plid = None
            if not plid:
                plid = _stable_negative_id(f"PL:{target}:{code}", used_ids)
                used_ids.add(plid)
            picking_lists[code] = {
                "id": plid,
                "lote_id": target,
                "codigo_lista": code,
                "asignado_a": clean_text(ev.get("asignado_a", "")) or "SIN_ASIGNAR",
                "estado": clean_text(ev.get("estado", "CREADA")) or "CREADA",
                "created_by": clean_text(ev.get("created_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "comentario": clean_text(ev.get("comentario", "")),
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "printed_at": "",
                "completed_at": "",
                "anulada_at": "",
                "anulada_by": "",
                "anulada_motivo": "",
                "raw_items": ev.get("items") or [],
            }
        elif et in {"picking_lista_impresa", "PICKING_LISTA_IMPRESA"}:
            picking_lists.setdefault(code, {"id": _stable_negative_id(f"PL:{target}:{code}", used_ids), "lote_id": target, "codigo_lista": code, "asignado_a": "SIN_ASIGNAR", "estado": "CREADA", "created_by": "SIN_USUARIO", "comentario": "", "created_at": clean_text(ev.get("created_at", "")), "printed_at": "", "completed_at": "", "anulada_at": "", "anulada_by": "", "anulada_motivo": "", "raw_items": []})
            if code not in anuladas:
                picking_lists[code]["estado"] = "IMPRESA"
            picking_lists[code]["printed_at"] = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", ""))
        elif et in {"picking_lista_completada", "PICKING_LISTA_COMPLETADA"}:
            picking_lists.setdefault(code, {"id": _stable_negative_id(f"PL:{target}:{code}", used_ids), "lote_id": target, "codigo_lista": code, "asignado_a": "SIN_ASIGNAR", "estado": "CREADA", "created_by": "SIN_USUARIO", "comentario": "", "created_at": clean_text(ev.get("created_at", "")), "printed_at": "", "completed_at": "", "anulada_at": "", "anulada_by": "", "anulada_motivo": "", "raw_items": []})
            if code not in anuladas:
                picking_lists[code]["estado"] = "COMPLETADA"
            picking_lists[code]["completed_at"] = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", ""))
        elif et in {"picking_lista_anulada", "PICKING_LISTA_ANULADA"}:
            anuladas.add(code)
            picking_lists.setdefault(code, {"id": _stable_negative_id(f"PL:{target}:{code}", used_ids), "lote_id": target, "codigo_lista": code, "asignado_a": "SIN_ASIGNAR", "estado": "ANULADA", "created_by": "SIN_USUARIO", "comentario": "", "created_at": clean_text(ev.get("created_at", "")), "printed_at": "", "completed_at": "", "anulada_at": "", "anulada_by": "", "anulada_motivo": "", "raw_items": []})
            picking_lists[code]["estado"] = "ANULADA"
            picking_lists[code]["anulada_at"] = clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", ""))
            picking_lists[code]["anulada_by"] = clean_text(ev.get("usuario", "")) or "SIN_USUARIO"
            picking_lists[code]["anulada_motivo"] = clean_text(ev.get("comentario", ""))

    # 3) Agregar productos faltantes desde picking NO anulado. Esto no inventa: usa líneas completas que están en eventos.
    picking_item_rows = []
    fallback_from_picking = 0
    for code, pl in picking_lists.items():
        if pl.get("estado") == "ANULADA":
            continue
        for pit in pl.get("raw_items") or []:
            if not isinstance(pit, dict):
                continue
            before_count = len(items_by_id)
            item_id = add_or_update_item({**pit, "unidades": pit.get("cantidad", 0), "created_at": pl.get("created_at", "")}, "PICKING_FALLBACK", f"producto reconstruido desde lista {code}")
            if item_id is None:
                continue
            if len(items_by_id) > before_count:
                fallback_from_picking += 1
            picking_item_rows.append({
                "picking_list_id": int(pl["id"]),
                "lote_id": target,
                "item_id": int(item_id),
                "codigo_ml": norm_code(pit.get("codigo_ml", "")),
                "codigo_universal": norm_code(pit.get("codigo_universal", "")),
                "sku": norm_code(pit.get("sku", "")),
                "descripcion": clean_text(pit.get("descripcion", "")),
                "cantidad": to_int(pit.get("cantidad", 0)),
                "area": clean_text(pit.get("area", "")),
                "nro": clean_text(pit.get("nro", "")),
                "estado": "PENDIENTE",
                "created_at": clean_text(pl.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
            })

    # 4) Scans, con match solo dentro del lote reconstruido.
    movement_by_item = {}
    scan_rows = []
    unmatched_scans = 0
    ambiguous_scans = 0
    seen_scan_ids = set()
    for ev in lote_events:
        et = clean_text(ev.get("event_type", ""))
        if et not in {"scan_agregado", "scan_deshacer"}:
            continue
        scan_identity = sheet_event_semantic_identity(ev)
        if not scan_identity:
            scan_identity = "SCAN:" + "|".join([
                clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or clean_text(ev.get("received_at", "")),
                clean_text(ev.get("item_id", "")), norm_code(ev.get("codigo_ml", "")),
                norm_code(ev.get("codigo_universal", "")), norm_code(ev.get("sku", "")),
                clean_text(ev.get("cantidad", "")), clean_text(ev.get("picking_code", "")),
                clean_text(ev.get("scan_primario", "")), clean_text(ev.get("scan_secundario", "")),
            ])
        if scan_identity in seen_scan_ids:
            continue
        seen_scan_ids.add(scan_identity)
        qty = to_int(ev.get("cantidad", 0))
        sign = -1 if et == "scan_deshacer" else 1
        original_id = to_int(ev.get("item_id", 0))
        resolved_id = None
        status = "ACOPIO_RECUPERADO_SHEETS"
        resolved_id, status = resolve_restore_item_identity(items_by_id, ev, original_id)
        if status == "AMBIGUOUS_SAME_LOTE":
            ambiguous_scans += 1
        if resolved_id is None:
            before_count = len(items_by_id)
            resolved_id = add_or_update_item({**ev, "unidades": max(qty, 0)}, "SCAN_FALLBACK", "producto reconstruido desde scan sin snapshot") or original_id or 0
            if len(items_by_id) > before_count:
                warnings.append("Se creó producto SCAN_FALLBACK desde un escaneo sin snapshot.")
            unmatched_scans += 1
        if et == "scan_agregado" and qty > 0:
            movement_by_item[resolved_id] = movement_by_item.get(resolved_id, 0) + qty
            scan_rows.append({
                "lote_id": target,
                "item_id": int(resolved_id),
                "scan_primario": norm_code(ev.get("scan_primario", "")),
                "scan_secundario": norm_code(ev.get("scan_secundario", "")),
                "cantidad": qty,
                "modo": clean_text(ev.get("modo", "")),
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "operador_validador": clean_text(ev.get("operador_validador", "")) or "SIN_USUARIO",
                "picking_list_id": to_int(ev.get("picking_list_id", 0)) or None,
                "picking_code": clean_text(ev.get("picking_code", "")),
                "picker_asignado": clean_text(ev.get("picker_asignado", "")),
                "original_item_id": original_id or None,
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion", "")),
                "restore_match_status": status,
            })
        elif et == "scan_deshacer" and qty > 0:
            movement_by_item[resolved_id] = max(0, movement_by_item.get(resolved_id, 0) - qty)

    # Aplicar acopiado a items reconstruidos.
    for iid, item in items_by_id.items():
        item["acopiadas"] = max(0, min(to_int(item.get("unidades", 0)), int(movement_by_item.get(iid, 0))))
        if item["acopiadas"]:
            item["updated_at"] = now_cl().isoformat(timespec="seconds")

    # 5) Resto de eventos operativos.
    incidencias = []
    reimpresiones = []
    avisos = {}
    avisos_updates = {}
    auditoria = []
    reservas = []
    label_events = []
    postventa_errores = {}
    postventa_cierres = []

    def resolve_event_item(ev: dict):
        resolved, _ = resolve_restore_item_identity(items_by_id, ev, to_int(ev.get("item_id", 0)))
        return resolved

    for ev in lote_events:
        et = clean_text(ev.get("event_type", ""))
        et_low = et.lower()
        if et == "producto_anexado_lote":
            resolved_id = add_or_update_item({**ev, "unidades": ev.get("unidades", ev.get("cantidad", 0)), "origen_item": "ANEXO_MANUAL"}, "LOTE_ITEM", "producto anexado manualmente")
            if resolved_id and resolved_id in items_by_id:
                items_by_id[resolved_id].update({
                    "origen_item": "ANEXO_MANUAL",
                    "motivo_anexo": clean_text(ev.get("motivo_anexo", ev.get("motivo", ev.get("comentario", "")))),
                    "usuario_anexo": clean_text(ev.get("usuario_anexo", ev.get("usuario", ""))) or "SIN_USUARIO",
                    "fecha_anexo": clean_text(ev.get("fecha_anexo", ev.get("created_at", ev.get("queued_at", "")))),
                    "anexo_ml_confirmado": _to_bool_flag(ev.get("anexo_ml_confirmado", 0)),
                    "anexo_kame_confirmado": _to_bool_flag(ev.get("anexo_kame_confirmado", 0)),
                })
        elif et == "producto_anexado_ml_confirmado":
            iid = resolve_event_item(ev)
            if iid and iid in items_by_id:
                items_by_id[iid].update({
                    "anexo_ml_confirmado": 1,
                    "anexo_ml_confirmado_at": clean_text(ev.get("confirmado_at", ev.get("created_at", ev.get("queued_at", "")))),
                    "anexo_ml_confirmado_by": clean_text(ev.get("confirmado_by", ev.get("usuario", ""))) or "SIN_USUARIO",
                    "anexo_ml_confirmado_comment": clean_text(ev.get("comentario", "")),
                })
        elif et == "producto_anexado_kame_confirmado":
            iid = resolve_event_item(ev)
            if iid and iid in items_by_id:
                items_by_id[iid].update({
                    "anexo_kame_confirmado": 1,
                    "anexo_kame_confirmado_at": clean_text(ev.get("confirmado_at", ev.get("created_at", ev.get("queued_at", "")))),
                    "anexo_kame_confirmado_by": clean_text(ev.get("confirmado_by", ev.get("usuario", ""))) or "SIN_USUARIO",
                    "anexo_kame_confirmado_comment": clean_text(ev.get("comentario", "")),
                })
        elif et in {"incidencia_creada", "INCIDENCIA_ABIERTA"}:
            incidencias.append({
                "lote_id": target,
                "item_id": resolve_event_item(ev),
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
        elif "reimpresion" in et_low:
            reimpresiones.append({
                "lote_id": target,
                "item_id": resolve_event_item(ev),
                "block_index": to_int(ev.get("block_index", 0)) or None,
                "block_key": clean_text(ev.get("block_key", "")),
                "scope": clean_text(ev.get("scope", "")) or ("BLOQUE" if clean_text(ev.get("block_key", "")) else "PRODUCTO"),
                "cantidad": max(1, to_int(ev.get("cantidad", 1))),
                "motivo": clean_text(ev.get("motivo", "")) or clean_text(ev.get("comentario", "")) or "Restaurado desde eventos",
                "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
            })
        elif et in {"aviso_operacional_creado", "AVISO_OPERACIONAL_CREADO"}:
            aviso_id = to_int(ev.get("aviso_id", 0)) or _stable_negative_id(f"AVISO:{target}:{clean_text(ev.get('queue_id',''))}:{clean_text(ev.get('item_id',''))}", used_ids)
            used_ids.add(aviso_id)
            avisos[aviso_id] = {
                "id": aviso_id,
                "lote_id": target,
                "item_id": resolve_event_item(ev) or to_int(ev.get("item_id", 0)) or 0,
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion", "")),
                "tipo_aviso": clean_text(ev.get("tipo_aviso", "")) or "Preparar con observación",
                "mensaje_operador": clean_text(ev.get("mensaje_operador", "")),
                "cantidad_original": to_int(ev.get("cantidad_original", 0)),
                "cantidad_nueva": to_int(ev.get("cantidad_nueva", 0)) if clean_text(ev.get("cantidad_nueva", "")) else None,
                "requiere_ajuste_ml": _to_bool_flag(ev.get("requiere_ajuste_ml", 0)),
                "requiere_ajuste_inventario": _to_bool_flag(ev.get("requiere_ajuste_inventario", 0)),
                "confirmado_ml": _to_bool_flag(ev.get("confirmado_ml", 0)),
                "confirmado_inventario": _to_bool_flag(ev.get("confirmado_inventario", ev.get("confirmado_kame", 0))),
                "confirmado_ml_at": clean_text(ev.get("confirmado_ml_at", "")),
                "confirmado_ml_by": clean_text(ev.get("confirmado_ml_by", "")),
                "confirmado_inventario_at": clean_text(ev.get("confirmado_inventario_at", "")) or clean_text(ev.get("confirmado_kame_at", "")),
                "confirmado_inventario_by": clean_text(ev.get("confirmado_inventario_by", "")) or clean_text(ev.get("confirmado_kame_by", "")),
                "visible_operador": 0 if clean_text(ev.get("visible_operador", "")).upper() in {"0", "NO", "FALSE"} else 1,
                "estado": clean_text(ev.get("estado", "ACTIVO")) or "ACTIVO",
                "comentario_interno": clean_text(ev.get("comentario_interno", "")),
                "created_by": clean_text(ev.get("created_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "resolved_at": clean_text(ev.get("resolved_at", "")),
                "resolved_by": clean_text(ev.get("resolved_by", "")),
                "resolution_comment": clean_text(ev.get("resolution_comment", "")),
            }
        elif et in {"aviso_operacional_ml_confirmado", "AVISO_OPERACIONAL_ML_CONFIRMADO", "aviso_operacional_kame_confirmado", "AVISO_OPERACIONAL_KAME_CONFIRMADO", "aviso_operacional_resuelto", "AVISO_OPERACIONAL_RESUELTO"}:
            aviso_id = to_int(ev.get("aviso_id", 0))
            if aviso_id:
                upd = avisos_updates.setdefault(aviso_id, {})
                if "ml_confirmado" in et_low:
                    upd["confirmado_ml"] = 1
                    upd["confirmado_ml_at"] = clean_text(ev.get("confirmado_at", "")) or clean_text(ev.get("created_at", ""))
                    upd["confirmado_ml_by"] = clean_text(ev.get("confirmado_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO"
                elif "kame_confirmado" in et_low:
                    upd["confirmado_inventario"] = 1
                    upd["confirmado_inventario_at"] = clean_text(ev.get("confirmado_at", "")) or clean_text(ev.get("created_at", ""))
                    upd["confirmado_inventario_by"] = clean_text(ev.get("confirmado_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO"
                elif "resuelto" in et_low:
                    upd["estado"] = "RESUELTO"
                    upd["visible_operador"] = 0
                    upd["resolved_at"] = clean_text(ev.get("resolved_at", "")) or clean_text(ev.get("created_at", ""))
                    upd["resolved_by"] = clean_text(ev.get("resolved_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO"
                    upd["resolution_comment"] = clean_text(ev.get("resolution_comment", "")) or clean_text(ev.get("comentario", ""))
        elif et in {"zpl_etiquetas_generado", "ZPL_ETIQUETAS_GENERADO"}:
            label_events.append({
                "lote_id": target,
                "print_scope": clean_text(ev.get("print_scope", ev.get("scope", ""))).upper(),
                "print_kind": clean_text(ev.get("print_kind", "NORMAL")).upper() or "NORMAL",
                "block_index": to_int(ev.get("block_index", 0)) or None,
                "block_key": clean_text(ev.get("block_key", "")),
                "picking_list_id": to_int(ev.get("picking_list_id", 0)) or None,
                "picking_code": clean_text(ev.get("picking_code", "")) or clean_text(ev.get("codigo_lista", "")),
                "asignado_a": clean_text(ev.get("asignado_a", "")),
                "item_id": resolve_event_item(ev) or to_int(ev.get("item_id", 0)) or None,
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion", "")),
                "productos_count": to_int(ev.get("productos_count", 0)),
                "cantidad_normal": to_int(ev.get("cantidad_normal", ev.get("normal_qty", 0))),
                "cantidad_separadores": to_int(ev.get("cantidad_separadores", ev.get("separator_qty", 0))),
                "cantidad_total": to_int(ev.get("cantidad_total", ev.get("total_qty", ev.get("cantidad", 0)))),
                "archivo_nombre": clean_text(ev.get("archivo_nombre", "")),
                "zpl_hash": clean_text(ev.get("zpl_hash", "")),
                "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
            })
        elif et in {"postventa_full_error_creado", "POSTVENTA_FULL_ERROR_CREADO"}:
            err_id = to_int(ev.get("error_id", 0))
            if not err_id:
                err_id = _stable_negative_id(f"POSTVENTA:{target}:{sheet_event_semantic_identity(ev)}", used_ids)
                used_ids.add(err_id)
            postventa_errores[err_id] = {
                "id": err_id,
                "lote_id": target,
                "item_id": resolve_event_item(ev),
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                "sku": norm_code(ev.get("sku", "")),
                "descripcion": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_kame": clean_text(ev.get("descripcion_kame", "")) or clean_text(ev.get("descripcion", "")),
                "descripcion_ml": clean_text(ev.get("descripcion_ml", "")),
                "familia_kame": clean_text(ev.get("familia_kame", "")),
                "tipo_error": clean_text(ev.get("tipo_error", "")) or clean_text(ev.get("tipo", "")) or "Otro",
                "cantidad_solicitada": to_int(ev.get("cantidad_solicitada", 0)),
                "cantidad_preparada": to_int(ev.get("cantidad_preparada", 0)),
                "cantidad_reportada_full": to_int(ev.get("cantidad_reportada_full", 0)) if clean_text(ev.get("cantidad_reportada_full", "")) else None,
                "cantidad_diferencia": to_int(ev.get("cantidad_diferencia", 0)),
                "cantidad_afectada": to_int(ev.get("cantidad_afectada", ev.get("cantidad", 0))),
                "comentario": clean_text(ev.get("comentario", "")),
                "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "estado": clean_text(ev.get("estado", "ACTIVO")) or "ACTIVO",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                "anulado_at": clean_text(ev.get("anulado_at", "")),
                "anulado_by": clean_text(ev.get("anulado_by", "")),
                "anulado_motivo": clean_text(ev.get("anulado_motivo", "")),
            }
        elif et in {"postventa_full_error_anulado", "POSTVENTA_FULL_ERROR_ANULADO"}:
            err_id = to_int(ev.get("error_id", 0))
            if err_id and err_id in postventa_errores:
                postventa_errores[err_id].update({
                    "estado": "ANULADO",
                    "anulado_at": clean_text(ev.get("anulado_at", "")) or clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")),
                    "anulado_by": clean_text(ev.get("anulado_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                    "anulado_motivo": clean_text(ev.get("anulado_motivo", "")) or clean_text(ev.get("comentario", "")),
                })
            elif err_id:
                postventa_errores[err_id] = {
                    "id": err_id, "lote_id": target, "item_id": resolve_event_item(ev),
                    "codigo_ml": norm_code(ev.get("codigo_ml", "")), "codigo_universal": norm_code(ev.get("codigo_universal", "")),
                    "sku": norm_code(ev.get("sku", "")), "descripcion": clean_text(ev.get("descripcion", "")),
                    "descripcion_kame": clean_text(ev.get("descripcion_kame", ev.get("descripcion", ""))), "descripcion_ml": clean_text(ev.get("descripcion_ml", "")),
                    "familia_kame": clean_text(ev.get("familia_kame", "")), "tipo_error": clean_text(ev.get("tipo_error", ev.get("tipo", "Otro"))),
                    "cantidad_solicitada": 0, "cantidad_preparada": 0, "cantidad_reportada_full": None, "cantidad_diferencia": 0,
                    "cantidad_afectada": to_int(ev.get("cantidad_afectada", ev.get("cantidad", 0))), "comentario": clean_text(ev.get("comentario", "")),
                    "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO", "estado": "ANULADO",
                    "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
                    "anulado_at": clean_text(ev.get("anulado_at", "")) or clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")),
                    "anulado_by": clean_text(ev.get("anulado_by", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                    "anulado_motivo": clean_text(ev.get("anulado_motivo", "")) or clean_text(ev.get("comentario", "")),
                }
        elif et in {"postventa_full_revision_cerrada", "POSTVENTA_FULL_REVISION_CERRADA"}:
            cierre_id = to_int(ev.get("cierre_id", 0)) or _stable_negative_id(f"POSTVENTA_CIERRE:{target}:{sheet_event_semantic_identity(ev)}", used_ids)
            used_ids.add(cierre_id)
            postventa_cierres.append({
                "id": cierre_id,
                "lote_id": target,
                "lote_nombre": clean_text(ev.get("lote_nombre", meta.get("nombre", f"Lote {target}"))),
                "total_errores": to_int(ev.get("total_errores", 0)),
                "errores_activos": to_int(ev.get("errores_activos", 0)),
                "unidades_afectadas": to_int(ev.get("unidades_afectadas", 0)),
                "cerrado_por": clean_text(ev.get("cerrado_por", "")) or clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "comentario": clean_text(ev.get("comentario", "")),
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
            })
        elif et == "audit_event":
            auditoria.append({
                "lote_id": target,
                "item_id": resolve_event_item(ev),
                "event_type": clean_text(ev.get("event_type_audit", "")) or clean_text(ev.get("tipo", "")) or clean_text(ev.get("modo", "")) or "AUDIT_EVENT",
                "detail": clean_text(ev.get("detalle", "")) or clean_text(ev.get("comentario", "")) or clean_text(ev.get("descripcion", "")),
                "qty": to_int(ev.get("cantidad", 0)) if clean_text(ev.get("cantidad", "")) else None,
                "codigo_ml": norm_code(ev.get("codigo_ml", "")),
                "sku": norm_code(ev.get("sku", "")),
                "mode": clean_text(ev.get("modo", "")),
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
            })
        elif et == "reserva_kame_generada":
            reservas.append({
                "lote_id": target,
                "folio": clean_text(ev.get("folio", "")) or "SIN_FOLIO",
                "folio_auto": clean_text(ev.get("folio_auto", "S")) or "S",
                "ficha": clean_text(ev.get("ficha", "")),
                "fecha": clean_text(ev.get("fecha", "")),
                "glosa": clean_text(ev.get("glosa", "")),
                "bodega_salida": clean_text(ev.get("bodega_salida", "")),
                "unidad_negocio": clean_text(ev.get("unidad_negocio", "")),
                "sku_count": to_int(ev.get("sku_count", 0)),
                "unidades_total": to_float_qty(ev.get("unidades_total", 0)),
                "productos_full": to_int(ev.get("productos_full", 0)),
                "packs_expandidos": to_int(ev.get("packs_expandidos", 0)),
                "lineas_csv": to_int(ev.get("lineas_csv", 0)),
                "archivo_nombre": clean_text(ev.get("archivo_nombre", "")),
                "usuario": clean_text(ev.get("usuario", "")) or "SIN_USUARIO",
                "created_at": clean_text(ev.get("created_at", "")) or clean_text(ev.get("queued_at", "")) or now_cl().isoformat(timespec="seconds"),
            })

    for aviso_id, upd in avisos_updates.items():
        if aviso_id in avisos:
            avisos[aviso_id].update(upd)

    integrity = {
        "fallback_from_picking": fallback_from_picking,
        "unmatched_scans": unmatched_scans,
        "ambiguous_scans": ambiguous_scans,
        "warnings": warnings,
        "snapshot_hash": selected_snapshot_hash,
        "lote_scope_key": target_scope or make_sheet_lote_scope_key(target, meta.get("nombre", ""), meta.get("archivo", ""), meta.get("hoja", "")),
        "snapshot_selection_warning": snapshot_selection_warning,
        "pdf_reconciliation_changes": reconciliation_count,
    }
    return {
        "meta": meta,
        "items": list(items_by_id.values()),
        "picking_lists": list(picking_lists.values()),
        "picking_items": picking_item_rows,
        "scans": scan_rows,
        "incidencias": incidencias,
        "reimpresiones": reimpresiones,
        "avisos": list(avisos.values()),
        "auditoria": auditoria,
        "reservas": reservas,
        "label_events": label_events,
        "postventa_errores": list(postventa_errores.values()),
        "postventa_cierres": postventa_cierres,
        "integrity": integrity,
        "events": lote_events,
    }


def get_sheet_lote_items_from_events(events: list[dict], lote_id: int, lote_scope_key: str = "") -> pd.DataFrame:
    state = build_sheet_lote_state_clean(events, int(lote_id), lote_scope_key=lote_scope_key)
    rows = []
    for it in state.get("items", []):
        rows.append({
            "item_id": it.get("id"),
            "area": it.get("area", ""),
            "nro": it.get("nro", ""),
            "codigo_ml": it.get("codigo_ml", ""),
            "codigo_universal": it.get("codigo_universal", ""),
            "sku": it.get("sku", ""),
            "descripcion": it.get("descripcion", ""),
            "descripcion_kame": it.get("descripcion_kame", it.get("descripcion", "")),
            "descripcion_ml": it.get("descripcion_ml", ""),
            "familia_kame": it.get("familia_kame", ""),
            "maestro_match_status": it.get("maestro_match_status", ""),
            "origen_item": it.get("origen_item", "PDF_FULL"),
            "motivo_anexo": it.get("motivo_anexo", ""),
            "usuario_anexo": it.get("usuario_anexo", ""),
            "fecha_anexo": it.get("fecha_anexo", ""),
            "anexo_ml_confirmado": it.get("anexo_ml_confirmado", 0),
            "anexo_kame_confirmado": it.get("anexo_kame_confirmado", 0),
            "unidades": it.get("unidades", 0),
            "identificacion": it.get("identificacion", ""),
            "vence": it.get("vence", ""),
            "fuente_rescate": it.get("fuente_rescate", ""),
            "rescue_note": it.get("rescue_note", ""),
        })
    return pd.DataFrame(rows)


def diagnose_sheet_lote_state(state: dict) -> pd.DataFrame:
    """Diagnóstico rápido de lo que el rescate reconstruirá para cada módulo."""
    rows = []
    def add(modulo, sheets_count, local_count=None, estado=None, detalle=""):
        if estado is None:
            estado = "OK" if (local_count is None or int(local_count) == int(sheets_count)) else "REVISAR"
        rows.append({
            "Módulo": modulo,
            "Eventos/estado Sheets": int(sheets_count or 0),
            "Registros a reconstruir": "" if local_count is None else int(local_count or 0),
            "Estado": estado,
            "Detalle": clean_text(detalle),
        })
    add("Productos", len(state.get("items", []) or []), len(state.get("items", []) or []), detalle="Snapshot limpio deduplicado")
    add("Escaneo", len(state.get("scans", []) or []), len(state.get("scans", []) or []), detalle="Acopiadas se recalculan desde scans")
    add("Picking listas", len(state.get("picking_lists", []) or []), len(state.get("picking_lists", []) or []))
    add("Picking items", len(state.get("picking_items", []) or []), len(state.get("picking_items", []) or []))
    add("Etiquetas", len(state.get("label_events", []) or []), len(state.get("label_events", []) or []), detalle="Se reconstruye label_prints/label_blocks desde zpl_etiquetas_generado")
    add("Incidencias", len(state.get("incidencias", []) or []), len(state.get("incidencias", []) or []))
    add("Avisos operacionales", len(state.get("avisos", []) or []), len(state.get("avisos", []) or []))
    add("Reservas Kame", len(state.get("reservas", []) or []), len(state.get("reservas", []) or []))
    add("Postventa FULL errores", len(state.get("postventa_errores", []) or []), len(state.get("postventa_errores", []) or []))
    add("Postventa FULL cierres", len(state.get("postventa_cierres", []) or []), len(state.get("postventa_cierres", []) or []))
    add("Auditoría", len(state.get("auditoria", []) or []), len(state.get("auditoria", []) or []))
    integ = state.get("integrity", {}) or {}
    if integ.get("fallback_from_picking"):
        add("Integridad", integ.get("fallback_from_picking"), integ.get("fallback_from_picking"), "REVISAR", "Productos reconstruidos desde picking porque no estaban en snapshot")
    if integ.get("unmatched_scans") or integ.get("ambiguous_scans"):
        add("Scans recuperados", int(integ.get("unmatched_scans", 0)) + int(integ.get("ambiguous_scans", 0)), None, "REVISAR", "Hay scans con match recuperado/ambiguo; revisar detalle si el número es alto")
    return pd.DataFrame(rows)


def _restore_labels_from_state(c, lote_id: int, state: dict) -> int:
    """Reconstruye el estado visual de Etiquetas después del rescate."""
    restored = 0
    lid = int(lote_id)
    for evp in state.get("label_events", []) or []:
        scope = clean_text(evp.get("print_scope", "")).upper()
        kind = clean_text(evp.get("print_kind", "NORMAL")).upper() or "NORMAL"
        is_reprint = 1 if kind == "REIMPRESION" else 0
        created_at = clean_text(evp.get("created_at", "")) or now_cl().isoformat(timespec="seconds")
        try:
            if scope == "BLOQUE":
                block_index = to_int(evp.get("block_index", 0))
                block_key = clean_text(evp.get("block_key", ""))
                if not block_index or not block_key:
                    continue
                labels_view_restore = label_control_view(lid)
                blocks_restore = build_label_blocks(labels_view_restore, ROLL_CAPACITY_DEFAULT) if not labels_view_restore.empty else []
                # El block_key histórico puede cambiar después de rescatar porque los ids locales/snapshot
                # pueden variar. Primero buscamos por block_key exacto y luego por block_index.
                block = find_restored_label_block(blocks_restore, block_index, block_key)
                restored_block_key = clean_text(block.get("block_key", "")) if block else block_key

                if not block:
                    # Fallback seguro: aunque no podamos repartir producto por producto, conservamos
                    # el bloque histórico con sus cantidades del evento. Esto permite que el control
                    # por lista de picking reconozca cobertura histórica y no muestre pendientes falsos.
                    normal_qty = to_int(evp.get("cantidad_normal", evp.get("normal_qty", 0)))
                    sep_qty = to_int(evp.get("cantidad_separadores", evp.get("separator_qty", 0)))
                    total_qty = to_int(evp.get("cantidad_total", evp.get("total_qty", normal_qty + sep_qty)))
                    products_count = to_int(evp.get("productos_count", 0))
                    if normal_qty <= 0 and total_qty <= 0:
                        continue
                    c.execute(
                        """
                        INSERT OR REPLACE INTO label_blocks
                        (lote_id, block_index, block_key, products_count, normal_qty, separator_qty, total_qty,
                         status, download_count, last_printed_at, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                        """,
                        (lid, block_index, restored_block_key, products_count, normal_qty, sep_qty, total_qty,
                         "REIMPRESO" if is_reprint else "IMPRESO", created_at, created_at, created_at),
                    )
                    restored += 1
                    continue

                c.execute(
                    """
                    INSERT OR REPLACE INTO label_blocks
                    (lote_id, block_index, block_key, products_count, normal_qty, separator_qty, total_qty,
                     status, download_count, last_printed_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                    """,
                    (lid, block_index, restored_block_key, int(block.get("products_count", 0)), int(block.get("normal_qty", 0)),
                     int(block.get("separator_qty", 0)), int(block.get("total_qty", 0)), "REIMPRESO" if is_reprint else "IMPRESO",
                     created_at, created_at, created_at),
                )
                for item in block.get("items", []):
                    for pkind, qty in [("NORMAL", int(item.get("unidades", 0))), ("SEPARADOR", LABEL_SEPARATOR_PER_PRODUCT)]:
                        c.execute(
                            """
                            INSERT INTO label_prints
                            (lote_id, item_id, codigo_ml, sku, descripcion, descripcion_kame, descripcion_ml, cantidad, print_scope, print_kind,
                             block_index, block_key, is_reprint, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'BLOQUE', ?, ?, ?, ?, ?)
                            """,
                            (lid, int(item.get("id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                             descripcion_etiqueta_value(item), clean_text(item.get("descripcion_kame", item.get("descripcion", ""))),
                             clean_text(item.get("descripcion_ml", item.get("descripcion", ""))), qty, pkind, block_index, restored_block_key, is_reprint, created_at),
                        )
                restored += 1
            elif scope == "PICKING":
                # El id histórico de picking_list_id puede cambiar al rescatar desde Sheets.
                # Por eso se valida que exista en este lote; si no, se resuelve por codigo_lista/picking_code.
                picking_id = to_int(evp.get("picking_list_id", 0)) or to_int(evp.get("block_index", 0))
                block_key = clean_text(evp.get("block_key", "")) or clean_text(evp.get("picking_code", ""))
                code = clean_text(evp.get("picking_code", "")) or clean_text(evp.get("codigo_lista", ""))

                valid_pick = None
                if picking_id:
                    valid_pick = c.execute(
                        "SELECT id FROM picking_lists WHERE lote_id=? AND id=? LIMIT 1",
                        (lid, int(picking_id)),
                    ).fetchone()
                if not valid_pick and code:
                    valid_pick = c.execute(
                        "SELECT id FROM picking_lists WHERE lote_id=? AND codigo_lista=? LIMIT 1",
                        (lid, code),
                    ).fetchone()
                    picking_id = int(valid_pick["id"]) if valid_pick else 0

                if not picking_id:
                    continue

                items_df = pd.read_sql_query(
                    "SELECT * FROM picking_list_items WHERE lote_id=? AND picking_list_id=? ORDER BY id",
                    c,
                    params=(lid, int(picking_id)),
                )
                if items_df.empty and code:
                    row_pick = c.execute(
                        "SELECT id FROM picking_lists WHERE lote_id=? AND codigo_lista=? LIMIT 1",
                        (lid, code),
                    ).fetchone()
                    if row_pick:
                        picking_id = int(row_pick["id"])
                        items_df = pd.read_sql_query(
                            "SELECT * FROM picking_list_items WHERE lote_id=? AND picking_list_id=? ORDER BY id",
                            c,
                            params=(lid, int(picking_id)),
                        )
                if items_df.empty:
                    continue
                for _, item in items_df.iterrows():
                    for pkind, qty in [("NORMAL", to_int(item.get("cantidad", 0))), ("SEPARADOR", LABEL_SEPARATOR_PER_PRODUCT)]:
                        c.execute(
                            """
                            INSERT INTO label_prints
                            (lote_id, item_id, codigo_ml, sku, descripcion, descripcion_kame, descripcion_ml, cantidad, print_scope, print_kind,
                             block_index, block_key, is_reprint, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PICKING', ?, ?, ?, ?, ?)
                            """,
                            (lid, int(item.get("item_id")), norm_code(item.get("codigo_ml", "")), norm_code(item.get("sku", "")),
                             descripcion_etiqueta_value(item), clean_text(item.get("descripcion_kame", item.get("descripcion", ""))),
                             clean_text(item.get("descripcion_ml", item.get("descripcion", ""))), qty, pkind, int(picking_id), block_key, is_reprint, created_at),
                        )
                restored += 1
            elif scope == "INDIVIDUAL":
                item_id = to_int(evp.get("item_id", 0))
                if not item_id:
                    # Resolver por código si el id histórico cambió.
                    key_item = None
                    for q, val in [("codigo_ml", norm_code(evp.get("codigo_ml", ""))), ("sku", norm_code(evp.get("sku", "")))]:
                        if val:
                            key_item = c.execute(f"SELECT * FROM items WHERE lote_id=? AND {q}=? LIMIT 1", (lid, val)).fetchone()
                            if key_item:
                                break
                    item_id = int(key_item["id"]) if key_item else 0
                if not item_id:
                    continue
                row_item = c.execute("SELECT * FROM items WHERE id=? AND lote_id=?", (item_id, lid)).fetchone()
                item = dict(row_item) if row_item else {}
                qty_normal = max(1, to_int(evp.get("cantidad_normal", evp.get("cantidad_total", 1))))
                for pkind, qty in [("NORMAL", qty_normal), ("SEPARADOR", LABEL_SEPARATOR_PER_PRODUCT)]:
                    c.execute(
                        """
                        INSERT INTO label_prints
                        (lote_id, item_id, codigo_ml, sku, descripcion, descripcion_kame, descripcion_ml, cantidad, print_scope, print_kind,
                         block_index, block_key, is_reprint, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'INDIVIDUAL', ?, NULL, NULL, ?, ?)
                        """,
                        (lid, item_id, norm_code(item.get("codigo_ml", evp.get("codigo_ml", ""))), norm_code(item.get("sku", evp.get("sku", ""))),
                         descripcion_etiqueta_value(item) or clean_text(evp.get("descripcion", "")), clean_text(item.get("descripcion_kame", item.get("descripcion", ""))),
                         clean_text(item.get("descripcion_ml", item.get("descripcion", ""))), qty, pkind, is_reprint, created_at),
                    )
                restored += 1
        except Exception:
            # No bloquea el rescate completo por un evento visual de etiqueta incompleto.
            continue
    return restored


def restore_lote_from_sheet_events_clean(events: list[dict], lote_id: int, replace_existing: bool = True, lote_scope_key: str = "") -> tuple[bool, str]:
    state = build_sheet_lote_state_clean(events, int(lote_id), lote_scope_key=lote_scope_key)
    meta = state.get("meta", {})
    items = state.get("items", [])
    if not items:
        return False, f"No encontré productos reconstruibles para el lote {lote_id}. Revisa eventos lote_item, picking_lista_creada o scan_agregado."

    with db() as c:
        # Nunca reemplazar un lote local distinto solo porque comparte el mismo id SQLite.
        existing = c.execute("SELECT id, nombre, archivo, hoja FROM lotes WHERE id=?", (int(lote_id),)).fetchone()
        selected_scope = clean_text(lote_scope_key) or clean_text((state.get("integrity", {}) or {}).get("lote_scope_key", ""))
        if existing and selected_scope:
            existing_data = dict(existing)
            existing_scope = make_sheet_lote_scope_key(
                existing_data.get("id"),
                existing_data.get("nombre", ""),
                existing_data.get("archivo", ""),
                existing_data.get("hoja", ""),
            )
            if existing_scope != selected_scope:
                return False, (
                    "No se reemplazó el lote local: el mismo ID SQLite pertenece a otro FULL. "
                    "Selecciona el FULL correcto por nombre/archivo o trabaja desde una base limpia."
                )
        if replace_existing:
            lid = int(lote_id)
            c.execute("DELETE FROM scans WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM incidencias WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM reimpresiones WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM avisos_operacionales WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM picking_list_items WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM picking_lists WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM label_prints WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM label_blocks WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM reservas_kame WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM postventa_full_errores WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM postventa_full_cierres WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM audit_events WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM items WHERE lote_id=?", (lid,))
            c.execute("DELETE FROM lotes WHERE id=?", (lid,))

        c.execute(
            """
            INSERT OR REPLACE INTO lotes
            (id, nombre, archivo, hoja, created_at, status, closed_at, closed_by, close_note, backup_lote_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(lote_id),
                clean_text(meta.get("nombre", "")) or f"Lote {int(lote_id)}",
                clean_text(meta.get("archivo", "")),
                clean_text(meta.get("hoja", "")),
                clean_text(meta.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                clean_text(meta.get("status", "ACTIVO")) or "ACTIVO",
                clean_text(meta.get("closed_at", "")),
                clean_text(meta.get("closed_by", "")),
                clean_text(meta.get("close_note", "")),
                clean_text(meta.get("backup_lote_key", "")) or make_backup_lote_key(
                    clean_text(meta.get("nombre", "")),
                    clean_text(meta.get("archivo", "")),
                    clean_text(meta.get("hoja", "")),
                    clean_text(meta.get("created_at", "")),
                ),
            ),
        )

        for it in items:
            c.execute(
                """
                INSERT OR REPLACE INTO items
                (id, lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml,
                 descripcion_fuente, familia_kame, maestro_match_status, origen_item, motivo_anexo, usuario_anexo, fecha_anexo,
                 anexo_ml_confirmado, anexo_ml_confirmado_at, anexo_ml_confirmado_by, anexo_ml_confirmado_comment,
                 anexo_kame_confirmado, anexo_kame_confirmado_at, anexo_kame_confirmado_by, anexo_kame_confirmado_comment,
                 unidades, acopiadas, identificacion, vence, instrucciones, dia, hora, created_at, updated_at, fuente_rescate, rescue_note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(it.get("id")), int(lote_id), clean_text(it.get("area", "")), clean_text(it.get("nro", "")),
                    norm_code(it.get("codigo_ml", "")), norm_code(it.get("codigo_universal", "")), norm_code(it.get("sku", "")),
                    clean_text(it.get("descripcion", "")), clean_text(it.get("descripcion_kame", it.get("descripcion", ""))), clean_text(it.get("descripcion_ml", it.get("descripcion", ""))),
                    clean_text(it.get("descripcion_fuente", "")), clean_text(it.get("familia_kame", "")), clean_text(it.get("maestro_match_status", "")),
                    clean_text(it.get("origen_item", "PDF_FULL")) or "PDF_FULL", clean_text(it.get("motivo_anexo", "")),
                    clean_text(it.get("usuario_anexo", "")), clean_text(it.get("fecha_anexo", "")),
                    to_int(it.get("anexo_ml_confirmado", 0)), clean_text(it.get("anexo_ml_confirmado_at", "")),
                    clean_text(it.get("anexo_ml_confirmado_by", "")), clean_text(it.get("anexo_ml_confirmado_comment", "")),
                    to_int(it.get("anexo_kame_confirmado", 0)), clean_text(it.get("anexo_kame_confirmado_at", "")),
                    clean_text(it.get("anexo_kame_confirmado_by", "")), clean_text(it.get("anexo_kame_confirmado_comment", "")),
                    to_int(it.get("unidades", 0)), to_int(it.get("acopiadas", 0)),
                    clean_text(it.get("identificacion", "")), clean_text(it.get("vence", "")), clean_text(it.get("instrucciones", "")),
                    clean_text(it.get("dia", "")), clean_text(it.get("hora", "")),
                    clean_text(it.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                    clean_text(it.get("updated_at", "")) or now_cl().isoformat(timespec="seconds"),
                    clean_text(it.get("fuente_rescate", "")), clean_text(it.get("rescue_note", "")),
                ),
            )

        for pl in state.get("picking_lists", []):
            c.execute(
                """
                INSERT OR REPLACE INTO picking_lists
                (id, lote_id, codigo_lista, asignado_a, estado, created_by, comentario, created_at,
                 printed_at, completed_at, anulada_at, anulada_by, anulada_motivo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(pl.get("id")), int(lote_id), clean_text(pl.get("codigo_lista", "")), clean_text(pl.get("asignado_a", "")) or "SIN_ASIGNAR",
                    clean_text(pl.get("estado", "CREADA")) or "CREADA", clean_text(pl.get("created_by", "")) or "SIN_USUARIO",
                    clean_text(pl.get("comentario", "")), clean_text(pl.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                    clean_text(pl.get("printed_at", "")), clean_text(pl.get("completed_at", "")), clean_text(pl.get("anulada_at", "")),
                    clean_text(pl.get("anulada_by", "")), clean_text(pl.get("anulada_motivo", "")),
                ),
            )
        for pit in state.get("picking_items", []):
            c.execute(
                """
                INSERT INTO picking_list_items
                (picking_list_id, lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, maestro_match_status,
                 cantidad, area, nro, estado, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(pit.get("picking_list_id")), int(lote_id), int(pit.get("item_id")), norm_code(pit.get("codigo_ml", "")),
                    norm_code(pit.get("codigo_universal", "")), norm_code(pit.get("sku", "")), clean_text(pit.get("descripcion_kame", "")) or clean_text(pit.get("descripcion", "")),
                    clean_text(pit.get("descripcion_kame", "")) or clean_text(pit.get("descripcion", "")), clean_text(pit.get("descripcion_ml", "")) or clean_text(pit.get("descripcion", "")),
                    clean_text(pit.get("familia_kame", "")), clean_text(pit.get("maestro_match_status", "")),
                    to_int(pit.get("cantidad", 0)), clean_text(pit.get("area", "")), clean_text(pit.get("nro", "")),
                    clean_text(pit.get("estado", "PENDIENTE")) or "PENDIENTE", clean_text(pit.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                ),
            )
        for sr in state.get("scans", []):
            c.execute(
                """
                INSERT INTO scans
                (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at,
                 operador_validador, picking_list_id, picking_code, picker_asignado,
                 original_item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, maestro_match_status, restore_match_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(lote_id), int(sr.get("item_id", 0)), norm_code(sr.get("scan_primario", "")), norm_code(sr.get("scan_secundario", "")),
                    to_int(sr.get("cantidad", 0)), clean_text(sr.get("modo", "")), clean_text(sr.get("created_at", "")) or now_cl().isoformat(timespec="seconds"),
                    clean_text(sr.get("operador_validador", "")) or "SIN_USUARIO", sr.get("picking_list_id"), clean_text(sr.get("picking_code", "")), clean_text(sr.get("picker_asignado", "")),
                    sr.get("original_item_id"), norm_code(sr.get("codigo_ml", "")), norm_code(sr.get("codigo_universal", "")), norm_code(sr.get("sku", "")),
                    clean_text(sr.get("descripcion", "")), clean_text(sr.get("descripcion_kame", sr.get("descripcion", ""))), clean_text(sr.get("descripcion_ml", sr.get("descripcion", ""))),
                    clean_text(sr.get("familia_kame", "")), clean_text(sr.get("maestro_match_status", "")), clean_text(sr.get("restore_match_status", "")),
                ),
            )
        _notes = reconcile_all_active_picking_quantities(c, int(lote_id))
        for _note in _notes:
            c.execute(
                "INSERT INTO audit_events (lote_id, event_type, detail, created_at) VALUES (?, ?, ?, ?)",
                (int(lote_id), "RECONCILIACION_PICKING_RESTAURACION", _note, now_cl().isoformat(timespec="seconds")),
            )
        for inc in state.get("incidencias", []):
            c.execute(
                """
                INSERT INTO incidencias
                (lote_id, item_id, tipo, cantidad, comentario, usuario, status, created_at,
                 codigo_ml, codigo_universal, sku, descripcion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(lote_id), inc.get("item_id"), clean_text(inc.get("tipo", "")) or "Otro", to_int(inc.get("cantidad", 0)), clean_text(inc.get("comentario", "")), clean_text(inc.get("usuario", "")) or "SIN_USUARIO", clean_text(inc.get("status", "ABIERTA")) or "ABIERTA", clean_text(inc.get("created_at", "")) or now_cl().isoformat(timespec="seconds"), norm_code(inc.get("codigo_ml", "")), norm_code(inc.get("codigo_universal", "")), norm_code(inc.get("sku", "")), clean_text(inc.get("descripcion", ""))),
            )
        for rep in state.get("reimpresiones", []):
            c.execute(
                """
                INSERT INTO reimpresiones
                (lote_id, item_id, block_index, block_key, scope, cantidad, motivo, usuario, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(lote_id), rep.get("item_id"), rep.get("block_index"), clean_text(rep.get("block_key", "")), clean_text(rep.get("scope", "")), to_int(rep.get("cantidad", 1)), clean_text(rep.get("motivo", "")), clean_text(rep.get("usuario", "")) or "SIN_USUARIO", clean_text(rep.get("created_at", "")) or now_cl().isoformat(timespec="seconds")),
            )
        for av in state.get("avisos", []):
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
                (int(av.get("id")), int(lote_id), int(av.get("item_id") or 0), norm_code(av.get("codigo_ml", "")), norm_code(av.get("codigo_universal", "")), norm_code(av.get("sku", "")), clean_text(av.get("descripcion", "")), clean_text(av.get("tipo_aviso", "")), clean_text(av.get("mensaje_operador", "")), to_int(av.get("cantidad_original", 0)), av.get("cantidad_nueva"), to_int(av.get("requiere_ajuste_ml", 0)), to_int(av.get("requiere_ajuste_inventario", 0)), to_int(av.get("confirmado_ml", 0)), to_int(av.get("confirmado_inventario", 0)), clean_text(av.get("confirmado_ml_at", "")), clean_text(av.get("confirmado_ml_by", "")), clean_text(av.get("confirmado_inventario_at", "")), clean_text(av.get("confirmado_inventario_by", "")), to_int(av.get("visible_operador", 1)), clean_text(av.get("estado", "ACTIVO")), clean_text(av.get("comentario_interno", "")), clean_text(av.get("created_by", "")) or "SIN_USUARIO", clean_text(av.get("created_at", "")) or now_cl().isoformat(timespec="seconds"), clean_text(av.get("resolved_at", "")), clean_text(av.get("resolved_by", "")), clean_text(av.get("resolution_comment", ""))),
            )
        for au in state.get("auditoria", []):
            c.execute(
                """
                INSERT INTO audit_events
                (lote_id, item_id, event_type, detail, qty, codigo_ml, sku, mode, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(lote_id), au.get("item_id"), clean_text(au.get("event_type", "AUDIT_EVENT")), clean_text(au.get("detail", "")), au.get("qty"), norm_code(au.get("codigo_ml", "")), norm_code(au.get("sku", "")), clean_text(au.get("mode", "")), clean_text(au.get("created_at", "")) or now_cl().isoformat(timespec="seconds")),
            )
        for res in state.get("reservas", []):
            c.execute(
                """
                INSERT INTO reservas_kame
                (lote_id, folio, folio_auto, ficha, fecha, glosa, bodega_salida, unidad_negocio,
                 sku_count, unidades_total, productos_full, packs_expandidos, lineas_csv, archivo_nombre, csv_hash, usuario, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(lote_id), clean_text(res.get("folio", "SIN_FOLIO")), clean_text(res.get("folio_auto", "S")), clean_text(res.get("ficha", "")), clean_text(res.get("fecha", "")), clean_text(res.get("glosa", "")), clean_text(res.get("bodega_salida", "")), clean_text(res.get("unidad_negocio", "")), to_int(res.get("sku_count", 0)), to_float_qty(res.get("unidades_total", 0)), to_int(res.get("productos_full", 0)), to_int(res.get("packs_expandidos", 0)), to_int(res.get("lineas_csv", 0)), clean_text(res.get("archivo_nombre", "")), clean_text(res.get("csv_hash", "")), clean_text(res.get("usuario", "")) or "SIN_USUARIO", clean_text(res.get("created_at", "")) or now_cl().isoformat(timespec="seconds")),
            )

        for pe in state.get("postventa_errores", []):
            c.execute(
                """
                INSERT OR REPLACE INTO postventa_full_errores
                (id, lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, tipo_error,
                 cantidad_solicitada, cantidad_preparada, cantidad_reportada_full, cantidad_diferencia, cantidad_afectada,
                 comentario, usuario, estado, created_at, anulado_at, anulado_by, anulado_motivo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (to_int(pe.get("id", 0)) or None, int(lote_id), pe.get("item_id"), norm_code(pe.get("codigo_ml", "")), norm_code(pe.get("codigo_universal", "")), norm_code(pe.get("sku", "")),
                 clean_text(pe.get("descripcion", "")), clean_text(pe.get("descripcion_kame", pe.get("descripcion", ""))), clean_text(pe.get("descripcion_ml", "")), clean_text(pe.get("familia_kame", "")),
                 clean_text(pe.get("tipo_error", "Otro")) or "Otro", to_int(pe.get("cantidad_solicitada", 0)), to_int(pe.get("cantidad_preparada", 0)),
                 pe.get("cantidad_reportada_full"), to_int(pe.get("cantidad_diferencia", 0)), to_int(pe.get("cantidad_afectada", 0)),
                 clean_text(pe.get("comentario", "")), clean_text(pe.get("usuario", "")) or "SIN_USUARIO", clean_text(pe.get("estado", "ACTIVO")) or "ACTIVO",
                 clean_text(pe.get("created_at", "")) or now_cl().isoformat(timespec="seconds"), clean_text(pe.get("anulado_at", "")), clean_text(pe.get("anulado_by", "")), clean_text(pe.get("anulado_motivo", ""))),
            )

        for pc in state.get("postventa_cierres", []):
            c.execute(
                """
                INSERT OR REPLACE INTO postventa_full_cierres
                (id, lote_id, lote_nombre, total_errores, errores_activos, unidades_afectadas, cerrado_por, comentario, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (to_int(pc.get("id", 0)) or None, int(lote_id), clean_text(pc.get("lote_nombre", meta.get("nombre", ""))),
                 to_int(pc.get("total_errores", 0)), to_int(pc.get("errores_activos", 0)), to_int(pc.get("unidades_afectadas", 0)),
                 clean_text(pc.get("cerrado_por", "")) or "SIN_USUARIO", clean_text(pc.get("comentario", "")),
                 clean_text(pc.get("created_at", "")) or now_cl().isoformat(timespec="seconds")),
            )

        restored_label_prints = _restore_labels_from_state(c, int(lote_id), state)
        c.commit()

    integ = state.get("integrity", {})
    msg = (
        f"Rescate limpio completo: 1 lote, {len(state.get('items', []))} producto(s), "
        f"{len(state.get('scans', []))} escaneo(s), {len(state.get('picking_lists', []))} lista(s) picking, "
        f"{len(state.get('label_events', []))} impresión(es) etiqueta, "
        f"{len(state.get('reservas', []))} reserva(s) Kame, "
        f"{len(state.get('postventa_errores', []))} error(es) postventa, "
        f"{len(state.get('auditoria', []))} evento(s) auditoría."
    )
    if integ.get("fallback_from_picking"):
        msg += f" Se incorporaron {integ.get('fallback_from_picking')} producto(s) desde picking porque no estaban en lote_item."
    if integ.get("unmatched_scans") or integ.get("ambiguous_scans"):
        msg += f" Acopios recuperados desde Sheets: {integ.get('unmatched_scans',0)}; ambiguos: {integ.get('ambiguous_scans',0)}."
    return True, msg



# ============================================================
# Postventa FULL: errores informados por bodega FULL
# ============================================================

POSTVENTA_FULL_TIPOS = [
    "Diferencia de cantidad",
    "Producto dañado",
    "Etiqueta / código incorrecto",
    "Producto rechazado",
    "Producto no recibido por FULL",
    "Producto sobrante",
    "Otro",
]


def get_postventa_full_errores(lote_id=None, estado=None) -> pd.DataFrame:
    with db() as c:
        where = []
        params = []
        if lote_id:
            where.append("p.lote_id=?")
            params.append(int(lote_id))
        if estado and clean_text(estado) != "Todos":
            where.append("p.estado=?")
            params.append(clean_text(estado))
        sql_where = ("WHERE " + " AND ".join(where)) if where else ""
        return pd.read_sql_query(
            f"""
            SELECT p.*, l.nombre AS lote_nombre
            FROM postventa_full_errores p
            LEFT JOIN lotes l ON l.id=p.lote_id
            {sql_where}
            ORDER BY p.id DESC
            """,
            c,
            params=params,
        )


def get_postventa_full_cierres(lote_id=None) -> pd.DataFrame:
    with db() as c:
        if lote_id:
            return pd.read_sql_query(
                "SELECT * FROM postventa_full_cierres WHERE lote_id=? ORDER BY id DESC",
                c,
                params=(int(lote_id),),
            )
        return pd.read_sql_query("SELECT * FROM postventa_full_cierres ORDER BY id DESC", c)


def get_item_snapshot_for_postventa(lote_id: int, item_id: int) -> dict:
    with db() as c:
        row = c.execute("SELECT * FROM items WHERE lote_id=? AND id=?", (int(lote_id), int(item_id))).fetchone()
    if not row:
        return {}
    item = dict(row)
    return {
        "item_id": int(item.get("id") or 0),
        "codigo_ml": norm_code(item.get("codigo_ml", "")),
        "codigo_universal": norm_code(item.get("codigo_universal", "")),
        "sku": norm_code(item.get("sku", "")),
        "descripcion": descripcion_operativa_value(item),
        "descripcion_kame": descripcion_operativa_value(item),
        "descripcion_ml": descripcion_etiqueta_value(item),
        "familia_kame": clean_text(item.get("familia_kame", "")),
        "maestro_match_status": clean_text(item.get("maestro_match_status", "")),
        "cantidad_solicitada": to_int(item.get("unidades", 0)),
        "cantidad_preparada": to_int(item.get("acopiadas", 0)),
    }


def create_postventa_full_error(lote_id: int, item_id: int, tipo_error: str, cantidad_reportada_full, cantidad_afectada, comentario: str, usuario: str):
    """Crea un ítem de error postventa FULL.

    No modifica escaneos, picking, etiquetas ni cantidades del lote. Solo registra
    lo que FULL reportó después de recibir/revisar la carga.
    """
    try:
        lid = int(lote_id)
        iid = int(item_id)
    except Exception:
        return False, "Selecciona un lote y un producto válido del FULL."

    tipo = clean_text(tipo_error)
    if tipo not in POSTVENTA_FULL_TIPOS:
        return False, "Selecciona un tipo de error válido."

    comment = clean_text(comentario)
    if len(comment) < 3:
        return False, "Agrega un comentario breve para dejar trazabilidad."

    snap = get_item_snapshot_for_postventa(lid, iid)
    if not snap:
        return False, "No encontré el producto dentro del lote seleccionado."

    enviada = to_int(snap.get("cantidad_solicitada", 0))
    preparada = to_int(snap.get("cantidad_preparada", 0))
    reportada = None
    if clean_text(cantidad_reportada_full) != "":
        reportada = to_int(cantidad_reportada_full)
    diferencia = (reportada - preparada) if reportada is not None else 0
    afectada = to_int(cantidad_afectada)
    if tipo == "Diferencia de cantidad" and reportada is not None:
        afectada = abs(int(diferencia))
    afectada = max(0, int(afectada))
    if afectada <= 0:
        return False, "La cantidad afectada debe ser mayor a 0."

    user = clean_text(usuario) or get_operator_name()
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        cur = c.execute(
            """
            INSERT INTO postventa_full_errores
            (lote_id, item_id, codigo_ml, codigo_universal, sku, descripcion, descripcion_kame, descripcion_ml, familia_kame, tipo_error,
             cantidad_solicitada, cantidad_preparada, cantidad_reportada_full, cantidad_diferencia,
             cantidad_afectada, comentario, usuario, estado, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVO', ?)
            """,
            (
                lid,
                iid,
                snap.get("codigo_ml", ""),
                snap.get("codigo_universal", ""),
                snap.get("sku", ""),
                snap.get("descripcion", ""),
                snap.get("descripcion_kame", snap.get("descripcion", "")),
                snap.get("descripcion_ml", ""),
                snap.get("familia_kame", ""),
                tipo,
                enviada,
                preparada,
                reportada,
                diferencia,
                afectada,
                comment,
                user,
                now,
            ),
        )
        error_id = int(cur.lastrowid)
        c.commit()

    payload = build_lote_payload(lid)
    payload.update({
        "error_id": error_id,
        "item_id": iid,
        "codigo_ml": snap.get("codigo_ml", ""),
        "codigo_universal": snap.get("codigo_universal", ""),
        "sku": snap.get("sku", ""),
        "descripcion": snap.get("descripcion", ""),
        "descripcion_kame": snap.get("descripcion_kame", snap.get("descripcion", "")),
        "descripcion_ml": snap.get("descripcion_ml", ""),
        "familia_kame": snap.get("familia_kame", ""),
        "maestro_match_status": snap.get("maestro_match_status", ""),
        "tipo_error": tipo,
        "tipo": tipo,
        "cantidad_solicitada": enviada,
        "cantidad_preparada": preparada,
        "cantidad_reportada_full": reportada if reportada is not None else "",
        "cantidad_diferencia": diferencia,
        "cantidad_afectada": afectada,
        "cantidad": afectada,
        "comentario": comment,
        "usuario": user,
        "estado": "ACTIVO",
        "created_at": now,
    })
    audit_payload = build_lote_payload(lid)
    audit_payload.update({
        "item_id": iid,
        "event_type_audit": "POSTVENTA_FULL_ERROR_CREADO",
        "detalle": f"{tipo} · {snap.get('sku','')} · afectadas {afectada}. {comment}",
        "cantidad": afectada,
        "codigo_ml": snap.get("codigo_ml", ""),
        "sku": snap.get("sku", ""),
        "modo": "POSTVENTA_FULL",
        "tipo": "POSTVENTA_FULL_ERROR_CREADO",
        "comentario": comment,
        "created_at": now,
    })
    enqueue_backup_events_batch([
        ("postventa_full_error_creado", payload),
        ("audit_event", audit_payload),
    ])
    return True, f"Error postventa FULL #{error_id} registrado."


def anular_postventa_full_error(error_id: int, motivo: str, usuario: str):
    try:
        eid = int(error_id)
    except Exception:
        return False, "ID inválido."
    mot = clean_text(motivo)
    if len(mot) < 3:
        return False, "Indica un motivo de anulación."
    user = clean_text(usuario) or get_operator_name()
    now = now_cl().isoformat(timespec="seconds")
    with db() as c:
        row = c.execute("SELECT * FROM postventa_full_errores WHERE id=?", (eid,)).fetchone()
        if not row:
            return False, "No encontré el error postventa."
        err = dict(row)
        if clean_text(err.get("estado", "")) == "ANULADO":
            return False, "Este error ya estaba anulado."
        c.execute(
            """
            UPDATE postventa_full_errores
            SET estado='ANULADO', anulado_at=?, anulado_by=?, anulado_motivo=?
            WHERE id=?
            """,
            (now, user, mot, eid),
        )
        c.commit()

    lid = int(err.get("lote_id") or 0)
    payload = build_lote_payload(lid)
    payload.update({
        "error_id": eid,
        "item_id": err.get("item_id") or "",
        "codigo_ml": err.get("codigo_ml") or "",
        "codigo_universal": err.get("codigo_universal") or "",
        "sku": err.get("sku") or "",
        "descripcion": err.get("descripcion") or "",
        "tipo_error": err.get("tipo_error") or "",
        "tipo": err.get("tipo_error") or "",
        "cantidad_afectada": err.get("cantidad_afectada") or 0,
        "cantidad": err.get("cantidad_afectada") or 0,
        "estado": "ANULADO",
        "comentario": mot,
        "usuario": user,
        "anulado_at": now,
        "anulado_by": user,
        "anulado_motivo": mot,
        "created_at": now,
    })
    audit_payload = build_lote_payload(lid)
    audit_payload.update({
        "item_id": err.get("item_id") or "",
        "event_type_audit": "POSTVENTA_FULL_ERROR_ANULADO",
        "detalle": f"Anulado error postventa #{eid}: {mot}",
        "cantidad": err.get("cantidad_afectada") or 0,
        "codigo_ml": err.get("codigo_ml") or "",
        "sku": err.get("sku") or "",
        "modo": "POSTVENTA_FULL",
        "tipo": "POSTVENTA_FULL_ERROR_ANULADO",
        "comentario": mot,
        "created_at": now,
    })
    enqueue_backup_events_batch([
        ("postventa_full_error_anulado", payload),
        ("audit_event", audit_payload),
    ])
    return True, f"Error postventa FULL #{eid} anulado."


def cerrar_revision_postventa_full(lote_id: int, comentario: str, usuario: str):
    try:
        lid = int(lote_id)
    except Exception:
        return False, "Lote inválido."
    comment = clean_text(comentario) or "Revisión postventa FULL cerrada."
    user = clean_text(usuario) or get_operator_name()
    now = now_cl().isoformat(timespec="seconds")
    df = get_postventa_full_errores(lid)
    activos = df[df["estado"].astype(str).str.upper() == "ACTIVO"] if not df.empty else pd.DataFrame()
    total_errores = int(len(df[df["estado"].astype(str).str.upper() != "ANULADO"])) if not df.empty else 0
    errores_activos = int(len(activos)) if not activos.empty else 0
    unidades_afectadas = int(pd.to_numeric(activos.get("cantidad_afectada", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not activos.empty else 0
    lote = get_lote(lid)
    lote_nombre = clean_text(lote.get("nombre", f"Lote {lid}"))
    with db() as c:
        cur = c.execute(
            """
            INSERT INTO postventa_full_cierres
            (lote_id, lote_nombre, total_errores, errores_activos, unidades_afectadas, cerrado_por, comentario, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (lid, lote_nombre, total_errores, errores_activos, unidades_afectadas, user, comment, now),
        )
        cierre_id = int(cur.lastrowid)
        c.commit()
    payload = build_lote_payload(lid)
    payload.update({
        "cierre_id": cierre_id,
        "total_errores": total_errores,
        "errores_activos": errores_activos,
        "unidades_afectadas": unidades_afectadas,
        "cerrado_por": user,
        "comentario": comment,
        "created_at": now,
    })
    audit_payload = build_lote_payload(lid)
    audit_payload.update({
        "event_type_audit": "POSTVENTA_FULL_REVISION_CERRADA",
        "detalle": f"Revisión postventa cerrada. Errores activos: {errores_activos}. {comment}",
        "cantidad": unidades_afectadas,
        "modo": "POSTVENTA_FULL",
        "tipo": "POSTVENTA_FULL_REVISION_CERRADA",
        "comentario": comment,
        "created_at": now,
    })
    enqueue_backup_events_batch([
        ("postventa_full_revision_cerrada", payload),
        ("audit_event", audit_payload),
    ])
    return True, "Revisión postventa FULL cerrada."


def render_postventa_full_module(active_lote=None):
    st.subheader("Postventa FULL")
    st.caption("Registra errores informados por bodega FULL después del envío. No modifica acopios, picking ni etiquetas.")
    lotes_df = list_lotes()
    if lotes_df.empty:
        st.warning("No hay lotes cargados.")
        return

    tab_reg, tab_full, tab_kpi = st.tabs(["Registrar error", "Errores por FULL", "KPI global"])

    lote_options = {}
    default_idx = 0
    for idx, r in enumerate(lotes_df.itertuples(index=False)):
        label = f"{int(r.id)} · {clean_text(r.nombre)} · {int(r.acopiadas)}/{int(r.unidades)}"
        lote_options[label] = int(r.id)
        if active_lote and int(r.id) == int(active_lote):
            default_idx = idx

    with tab_reg:
        lote_label = st.selectbox("FULL / lote", list(lote_options.keys()), index=default_idx, key="postventa_lote_reg")
        lote_id = lote_options[lote_label]
        items = get_items(lote_id)
        if items.empty:
            st.warning("Este FULL no tiene productos.")
        else:
            search = st.text_input("Buscar producto", placeholder="Código ML, EAN, SKU o descripción", key="postventa_search_producto")
            show_items = items.copy()
            if clean_text(search):
                q = clean_text(search).upper()
                mask = (
                    show_items["codigo_ml"].astype(str).str.upper().str.contains(q, na=False) |
                    show_items["codigo_universal"].astype(str).str.upper().str.contains(q, na=False) |
                    show_items["sku"].astype(str).str.upper().str.contains(q, na=False) |
                    show_items["descripcion"].astype(str).str.upper().str.contains(q, na=False)
                )
                show_items = show_items[mask]
            show_items = show_items.head(250)
            if show_items.empty:
                st.info("No encontré productos con ese criterio en el FULL seleccionado.")
            else:
                opt_map = {}
                labels = []
                for _, r in show_items.iterrows():
                    label = f"{clean_text(r.get('descripcion',''))[:70]} | SKU {clean_text(r.get('sku',''))} | ML {clean_text(r.get('codigo_ml',''))} | EAN {clean_text(r.get('codigo_universal',''))} | Prep {to_int(r.get('acopiadas',0))}/{to_int(r.get('unidades',0))}"
                    labels.append(label)
                    opt_map[label] = int(r["id"])
                selected = st.selectbox("Producto del FULL", labels, key="postventa_producto_select")
                item_id = opt_map[selected]
                snap = get_item_snapshot_for_postventa(lote_id, item_id)
                st.info(f"Solicitado FULL: {snap.get('cantidad_solicitada',0)} · Preparado WMS: {snap.get('cantidad_preparada',0)}")

                tipo = st.selectbox("Tipo de error", POSTVENTA_FULL_TIPOS, key="postventa_tipo_error")
                cantidad_reportada = ""
                cantidad_afectada = 1
                if tipo == "Diferencia de cantidad":
                    cantidad_reportada = st.number_input("Cantidad reportada por FULL", min_value=0, value=int(snap.get("cantidad_preparada", 0)), step=1, key="postventa_cantidad_reportada")
                    diferencia = int(cantidad_reportada) - int(snap.get("cantidad_preparada", 0))
                    cantidad_afectada = abs(diferencia)
                    st.metric("Diferencia calculada", diferencia)
                    st.caption("Negativo = FULL reporta menos de lo preparado. Positivo = FULL reporta más.")
                else:
                    cantidad_afectada = st.number_input("Cantidad afectada", min_value=1, value=1, step=1, key="postventa_cantidad_afectada")
                comentario = st.text_area("Comentario", placeholder="Ej: FULL reporta 2 unidades dañadas en recepción", key="postventa_comentario")
                usuario = get_operator_name()
                st.caption(f"Usuario: {usuario}")
                if st.button("Agregar error postventa FULL", type="primary", key="postventa_btn_crear"):
                    ok, msg = create_postventa_full_error(lote_id, item_id, tipo, cantidad_reportada, cantidad_afectada, comentario, usuario)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

    with tab_full:
        lote_label2 = st.selectbox("Ver FULL", list(lote_options.keys()), index=default_idx, key="postventa_lote_ver")
        lote_id2 = lote_options[lote_label2]
        df = get_postventa_full_errores(lote_id2)
        active_df = df[df["estado"].astype(str).str.upper() == "ACTIVO"] if not df.empty else pd.DataFrame()
        c1, c2, c3 = st.columns(3)
        c1.metric("Errores activos", int(len(active_df)))
        c2.metric("Unidades afectadas", int(pd.to_numeric(active_df.get("cantidad_afectada", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not active_df.empty else 0)
        c3.metric("Histórico lote", int(len(df)))
        if df.empty:
            st.info("Este FULL no tiene errores postventa registrados.")
        else:
            out = df.rename(columns={
                "id": "ID",
                "created_at": "Fecha",
                "lote_nombre": "FULL",
                "codigo_ml": "Código ML",
                "codigo_universal": "EAN",
                "sku": "SKU",
                "descripcion": "Producto",
                "tipo_error": "Tipo error",
                "cantidad_solicitada": "Solicitado",
                "cantidad_preparada": "Preparado WMS",
                "cantidad_reportada_full": "Reportado FULL",
                "cantidad_diferencia": "Diferencia",
                "cantidad_afectada": "Afectadas",
                "comentario": "Comentario",
                "usuario": "Usuario",
                "estado": "Estado",
                "anulado_at": "Anulado",
                "anulado_by": "Anulado por",
                "anulado_motivo": "Motivo anulación",
            })
            cols = [c for c in ["ID", "Fecha", "Código ML", "EAN", "SKU", "Producto", "Tipo error", "Solicitado", "Preparado WMS", "Reportado FULL", "Diferencia", "Afectadas", "Comentario", "Usuario", "Estado", "Motivo anulación"] if c in out.columns]
            st.dataframe(out[cols], use_container_width=True, hide_index=True, height=420)
            active_rows = df[df["estado"].astype(str).str.upper() == "ACTIVO"]
            if not active_rows.empty:
                with st.expander("Anular error registrado"):
                    error_opts = {f"#{int(r.id)} · {clean_text(r.tipo_error)} · SKU {clean_text(r.sku)} · {clean_text(r.descripcion)[:55]}": int(r.id) for r in active_rows.itertuples(index=False)}
                    sel_error = st.selectbox("Error a anular", list(error_opts.keys()), key="postventa_anular_select")
                    motivo = st.text_input("Motivo de anulación", key="postventa_anular_motivo")
                    if st.button("Anular error", key="postventa_anular_btn"):
                        ok, msg = anular_postventa_full_error(error_opts[sel_error], motivo, get_operator_name())
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
        st.divider()
        with st.expander("Cerrar revisión postventa de este FULL"):
            st.caption("Esto no borra errores. Solo deja constancia de que el FULL fue revisado para cierre y KPI.")
            comentario_cierre = st.text_area("Comentario de cierre", value="Revisión postventa FULL cerrada.", key="postventa_cierre_comment")
            if st.button("Cerrar revisión postventa FULL", key="postventa_cierre_btn"):
                ok, msg = cerrar_revision_postventa_full(lote_id2, comentario_cierre, get_operator_name())
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
        cierres = get_postventa_full_cierres(lote_id2)
        if not cierres.empty:
            with st.expander("Historial de cierres de revisión"):
                st.dataframe(cierres.rename(columns={"created_at":"Fecha", "cerrado_por":"Cerrado por", "errores_activos":"Errores activos", "unidades_afectadas":"Unidades afectadas", "comentario":"Comentario"}), use_container_width=True, hide_index=True)

    with tab_kpi:
        df_all = get_postventa_full_errores()
        cierres_all = get_postventa_full_cierres()
        if df_all.empty and cierres_all.empty:
            st.info("Aún no hay datos de postventa FULL para KPI.")
            return
        active = df_all[df_all["estado"].astype(str).str.upper() == "ACTIVO"] if not df_all.empty else pd.DataFrame()
        valid = df_all[df_all["estado"].astype(str).str.upper() != "ANULADO"] if not df_all.empty else pd.DataFrame()
        total_unidades_full = int(pd.to_numeric(lotes_df.get("unidades", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not lotes_df.empty else 0
        unidades_afectadas = int(pd.to_numeric(active.get("cantidad_afectada", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not active.empty else 0
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("FULL revisados", int(cierres_all["lote_id"].nunique()) if not cierres_all.empty else 0)
        k2.metric("FULL con errores", int(valid["lote_id"].nunique()) if not valid.empty else 0)
        k3.metric("Errores activos", int(len(active)))
        k4.metric("Unidades afectadas", unidades_afectadas)
        tasa = (unidades_afectadas / total_unidades_full * 100) if total_unidades_full else 0
        k5.metric("Afectadas / 100 uds", f"{tasa:.2f}")

        if not valid.empty:
            st.markdown("### Ranking global")
            r1, r2 = st.columns(2)
            with r1:
                st.caption("Tipos de error")
                tipo_rank = valid.groupby("tipo_error", dropna=False).agg(errores=("id", "count"), unidades_afectadas=("cantidad_afectada", "sum")).reset_index().sort_values(["errores", "unidades_afectadas"], ascending=False)
                st.dataframe(tipo_rank.rename(columns={"tipo_error":"Tipo error", "errores":"Errores", "unidades_afectadas":"Unidades afectadas"}), use_container_width=True, hide_index=True)
            with r2:
                st.caption("SKU con más unidades afectadas")
                sku_rank = valid.groupby(["sku", "descripcion"], dropna=False).agg(errores=("id", "count"), unidades_afectadas=("cantidad_afectada", "sum")).reset_index().sort_values(["unidades_afectadas", "errores"], ascending=False).head(20)
                st.dataframe(sku_rank.rename(columns={"sku":"SKU", "descripcion":"Producto", "errores":"Errores", "unidades_afectadas":"Unidades afectadas"}), use_container_width=True, hide_index=True)
            st.caption("FULL con más errores")
            lote_rank = valid.groupby(["lote_id", "lote_nombre"], dropna=False).agg(errores=("id", "count"), unidades_afectadas=("cantidad_afectada", "sum")).reset_index().sort_values(["errores", "unidades_afectadas"], ascending=False)
            st.dataframe(lote_rank.rename(columns={"lote_id":"Lote ID", "lote_nombre":"FULL", "errores":"Errores", "unidades_afectadas":"Unidades afectadas"}), use_container_width=True, hide_index=True)
            with st.expander("Detalle completo"):
                cols = ["created_at", "lote_nombre", "codigo_ml", "sku", "descripcion", "tipo_error", "cantidad_preparada", "cantidad_reportada_full", "cantidad_diferencia", "cantidad_afectada", "comentario", "usuario", "estado"]
                st.dataframe(valid[[c for c in cols if c in valid.columns]].rename(columns={
                    "created_at":"Fecha", "lote_nombre":"FULL", "codigo_ml":"Código ML", "sku":"SKU", "descripcion":"Producto", "tipo_error":"Tipo error", "cantidad_preparada":"Preparado", "cantidad_reportada_full":"Reportado FULL", "cantidad_diferencia":"Diferencia", "cantidad_afectada":"Afectadas", "comentario":"Comentario", "usuario":"Usuario", "estado":"Estado"
                }), use_container_width=True, hide_index=True, height=500)


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
    postventa_errores = get_postventa_full_errores(lote_id)
    postventa_cierres = get_postventa_full_cierres(lote_id)
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
        postventa_errores.to_excel(writer, sheet_name="postventa_full_errores", index=False)
        postventa_cierres.to_excel(writer, sheet_name="postventa_full_cierres", index=False)
    return out.getvalue()



# ============================================================
# Vistas integradas de Supervisor
# ============================================================

def render_control_integrado(active_lote: int):
    """Control operativo integrado al panel Supervisor."""
    lote = get_lote(active_lote)
    items = get_operational_items(active_lote)
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
    # Producción: no restaurar a ciegas. Sheets puede contener FULL antiguos o de prueba.
    # El rescate ahora se hace desde el módulo "Rescate Sheets", donde el usuario revisa
    # candidatos y elige explícitamente cuál restaurar.
    st.session_state["_auto_restore_msg"] = "Restauración automática desactivada. Usa Rescate Sheets para elegir el FULL a restaurar."
    st.session_state["_auto_restore_ok"] = False

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
    page = st.radio("Vista", ["Escaneo", "Cargar lote FULL", "Picking", "Reservas Kame", "Postventa FULL", "Rescate Sheets", "Supervisor", "Etiquetas"], label_visibility="collapsed")
    st.divider()
    lotes = list_lotes()
    if lotes.empty:
        active_lote = None
        st.session_state.pop("active_lote_id", None)
        st.session_state.pop("active_lote_select_label", None)
        st.info("Sin lotes creados.")
    else:
        # Selector persistente por ID real de lote + URL.
        # Problema real en producción:
        # - list_lotes() ordena por id DESC, por eso el FULL nuevo queda primero.
        # - En un rerun/recarga, el selectbox puede volver a su valor interno anterior
        #   o al primer índice y terminar cambiando al lote nuevo.
        # Solución:
        # 1) El lote activo vive en st.session_state["active_lote_id"].
        # 2) También se escribe en la URL (?lote_id=...), para que un refresh duro del navegador
        #    mantenga el FULL elegido en ese PDA/PC.
        # 3) El widget del selectbox se sincroniza desde el ID, no al revés.
        lote_options = []
        id_by_label = {}
        label_by_id = {}
        for r in lotes.itertuples(index=False):
            lid = int(r.id)
            label = f"#{lid} · {r.nombre} · {int(r.acopiadas)}/{int(r.unidades)}"
            lote_options.append(label)
            id_by_label[label] = lid
            label_by_id[lid] = label

        valid_lote_ids = set(label_by_id.keys())

        def _safe_int(v):
            try:
                if isinstance(v, (list, tuple)):
                    v = v[0] if v else None
                return int(v)
            except Exception:
                return None

        def _query_lote_id():
            try:
                return _safe_int(st.query_params.get("lote_id", None))
            except Exception:
                return None

        session_lote_id = _safe_int(st.session_state.get("active_lote_id"))
        query_lote_id = _query_lote_id()

        # Prioridad: sesión actual > URL > primer lote.
        # Así, mientras el usuario está trabajando, no lo pisa la URL ni el orden DESC.
        if session_lote_id in valid_lote_ids:
            saved_lote_id = session_lote_id
        elif query_lote_id in valid_lote_ids:
            saved_lote_id = query_lote_id
        else:
            saved_lote_id = id_by_label[lote_options[0]]

        st.session_state["active_lote_id"] = int(saved_lote_id)
        selector_key = "active_lote_select_widget"
        desired_label = label_by_id[int(saved_lote_id)]

        # Sincroniza el valor visual del selectbox ANTES de crearlo.
        # Esto evita que Streamlit conserve visualmente el FULL nuevo aunque active_lote_id sea otro.
        current_widget_label = st.session_state.get(selector_key)
        if current_widget_label not in id_by_label:
            st.session_state[selector_key] = desired_label
        elif int(id_by_label[current_widget_label]) != int(saved_lote_id):
            st.session_state[selector_key] = desired_label

        def _on_active_lote_changed(_id_by_label):
            selected = st.session_state.get(selector_key)
            if selected in _id_by_label:
                new_lid = int(_id_by_label[selected])
                old_lid = _safe_int(st.session_state.get("active_lote_id"))
                if old_lid != new_lid:
                    st.session_state["active_lote_id"] = new_lid
                    st.session_state["_active_lote_changed"] = True

        selected_label = st.selectbox(
            "Lote activo",
            lote_options,
            key=selector_key,
            on_change=_on_active_lote_changed,
            args=(id_by_label,),
        )

        active_lote = int(st.session_state.get("active_lote_id") or id_by_label[selected_label])

        # Si por alguna razón el callback no corrió, sincroniza igual desde el widget.
        if selected_label in id_by_label and int(id_by_label[selected_label]) != int(active_lote):
            old_lid = active_lote
            active_lote = int(id_by_label[selected_label])
            st.session_state["active_lote_id"] = active_lote
            st.session_state["_active_lote_changed"] = True

        # Mantener lote activo en la URL del navegador. Esto permite refrescar la página
        # sin saltar al lote más nuevo.
        try:
            if _query_lote_id() != int(active_lote):
                st.query_params["lote_id"] = str(int(active_lote))
        except Exception:
            pass

        if st.session_state.pop("_active_lote_changed", False):
            # Limpieza liviana de controles dependientes del lote para evitar que una lista
            # de picking o búsqueda del lote anterior quede pegada visualmente al cambiar de FULL.
            for _k in [
                "scan_picking_select", "scan_primary", "scan_secondary", "scan_qty",
                "pick_detail_select", "picking_search_query", "label_picking_select",
                "active_lote_select_label",
            ]:
                st.session_state.pop(_k, None)
        st.caption(f"Trabajando en lote #{active_lote}")
        # Snapshot automático del lote activo hacia Sheets, sin esperar respuesta.
        ensure_active_lote_snapshot_queued(active_lote)
        with st.expander("Respaldo SQLite → Sheets", expanded=False):
            st.caption("La operación usa SQLite. Sheets copia el snapshot y eventos desde la cola local.")
            if st.button("Encolar snapshot completo del lote activo"):
                ids_snap = queue_lote_snapshot_from_sqlite(active_lote, motivo="MANUAL_SNAPSHOT", usuario="ADMIN", force=True)
                st.success(f"Snapshot encolado: {len(ids_snap)} evento(s).")
                st.rerun()

    st.divider()
    bs = backup_status()
    pending_backup = int(bs.get("pending") or 0)
    # Intento de sincronización automático y no bloqueante.
    if pending_backup:
        # Sync liviano y con freno: evita lanzar 100 eventos en cada rerun/PDA.
        last_auto = float(st.session_state.get("_last_auto_backup_sync", 0) or 0)
        if time.time() - last_auto > 30:
            trigger_backup_sync_async(limit=10)
            st.session_state["_last_auto_backup_sync"] = time.time()
    failed_backup = int(bs.get("failed") or 0)
    sent_backup = int(bs.get("sent") or 0)
    if failed_backup:
        st.warning(f"Respaldo Sheets requiere conciliación: {failed_backup} evento(s)")
        st.caption("La operación local sigue registrada en SQLite. Estos eventos se pueden conciliar o reintentar sin detener el escaneo.")
        if st.button("Conciliar contra Sheets"):
            n_marked = reconcile_backup_queue_from_sheets(limit=5000)
            st.success(f"Conciliados como enviados: {n_marked}")
            st.rerun()
        if st.button("Reintentar pendientes"):
            retry_failed_backups(limit=250)
            st.rerun()
    if pending_backup:
        st.info(f"Respaldo Sheets pendiente: {pending_backup} evento(s) en cola")
        with st.expander("Diagnóstico técnico respaldo", expanded=False):
            last_error_txt = clean_text(bs.get("last_error"))
            if last_error_txt:
                st.caption("Último detalle técnico registrado:")
                st.code(last_error_txt[:1200])
            err_df = get_backup_error_rows(limit=20)
            if err_df.empty:
                st.caption("Sin detalles técnicos registrados.")
            else:
                st.dataframe(err_df, use_container_width=True, hide_index=True, height=220)
        if st.button("Sincronizar respaldo Sheets ahora"):
            flush_backup_queue(limit=250)
            st.rerun()
    else:
        st.success(f"SQLite operativo · Sheets sincronizado · enviados: {sent_backup}")
    if bs.get("last_sent"):
        st.caption(f"Último respaldo: {fmt_dt(bs.get('last_sent'))}")
    if st.session_state.get("_auto_restore_msg"):
        if st.session_state.get("_auto_restore_ok"):
            st.success(st.session_state.get("_auto_restore_msg"))
        else:
            st.caption(f"Restauración: {st.session_state.get('_auto_restore_msg')}")
    st.caption("Para recuperar un FULL desde Sheets, entra al módulo Rescate Sheets y elige el lote manualmente.")
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
                        new_lote_id = create_lote(nombre, full_file.name, selected_sheet, df)
                        st.session_state["active_lote_id"] = int(new_lote_id)
                        st.session_state.pop("active_lote_select_label", None)
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
                        new_lote_id = create_lote(nombre, pdf_file.name, "PDF Mercado Libre", df_pdf)
                        st.session_state["active_lote_id"] = int(new_lote_id)
                        st.session_state.pop("active_lote_select_label", None)
                        reset_scan_state()
                        st.success("Lote creado correctamente desde PDF.")
                        st.rerun()
            except Exception as e:
                st.error(f"No pude procesar el PDF: {e}")


    st.divider()
    st.subheader("Anexar producto al lote activo")
    st.caption("Uso controlado: producto agregado después del PDF/Excel original. No genera reserva Kame; solo deja checks operacionales para ML y Kame.")
    if not active_lote:
        st.warning("Selecciona un lote activo para anexar productos.")
    elif is_lote_closed(active_lote):
        st.warning("El lote activo está cerrado. Reabre el lote antes de anexar productos.")
    else:
        with st.expander("➕ Agregar producto anexado", expanded=False):
            st.info("El producto anexado quedará disponible para crear una lista de picking nueva. No se agrega automáticamente a listas ya impresas o escaneadas.")
            ca1, ca2, ca3 = st.columns(3)
            with ca1:
                anexo_sku = st.text_input("SKU", key="anexo_sku")
                anexo_codigo_ml = st.text_input("Código ML", key="anexo_codigo_ml")
                anexo_ean = st.text_input("Código universal / EAN", value="N/A", key="anexo_ean")
            with ca2:
                anexo_qty = st.number_input("Cantidad a anexar", min_value=1, value=1, step=1, key="anexo_qty")
                anexo_ident = st.selectbox("Identificación", ["", "Etiquetado obligatorio", "Supermercado"], key="anexo_ident")
                anexo_vence = st.text_input("Vence / VCTO opcional", key="anexo_vence")
            with ca3:
                anexo_user = st.text_input("Usuario", value=get_operator_name(), key="anexo_user")
                anexo_motivo = st.text_input("Motivo del anexo", placeholder="Ej: producto agregado posterior al envío FULL", key="anexo_motivo")
            anexo_desc_ml = st.text_area("Descripción ML para etiqueta", key="anexo_desc_ml", placeholder="Debe ser la descripción/título que corresponde usar en la etiqueta.")
            anexo_instr = st.text_area("Instrucciones opcionales", key="anexo_instr", placeholder="Ej: etiquetar / embalar separado / revisar unidades.")
            if st.button("Anexar producto al lote", type="primary", key="btn_anexar_producto"):
                ok_ax, msg_ax, item_ax = create_producto_anexado_lote(
                    active_lote, anexo_sku, anexo_codigo_ml, anexo_ean, anexo_desc_ml,
                    int(anexo_qty), anexo_ident, anexo_instr, anexo_motivo, anexo_user, anexo_vence,
                )
                if ok_ax:
                    st.success(msg_ax)
                    # Limpiar campos del formulario de anexos para evitar doble creación accidental.
                    for k in ["anexo_sku", "anexo_codigo_ml", "anexo_ean", "anexo_desc_ml", "anexo_qty", "anexo_ident", "anexo_vence", "anexo_user", "anexo_motivo", "anexo_instr"]:
                        st.session_state.pop(k, None)
                    st.rerun()
                else:
                    st.error(msg_ax)

        anexos_view = get_anexos_lote(active_lote)
        if anexos_view.empty:
            st.info("Este lote no tiene productos anexados.")
        else:
            st.warning(f"Este lote tiene {len(anexos_view)} producto(s) anexado(s). Revisa los checks operacionales antes del cierre.")
            show = anexos_view.copy()
            show["ML OK"] = pd.to_numeric(show.get("anexo_ml_confirmado", 0), errors="coerce").fillna(0).astype(int).map(lambda x: "Sí" if x else "Pendiente")
            show["Kame OK"] = pd.to_numeric(show.get("anexo_kame_confirmado", 0), errors="coerce").fillna(0).astype(int).map(lambda x: "Sí" if x else "Pendiente")
            out = show.rename(columns={"created_at":"Fecha", "codigo_ml":"Código ML", "codigo_universal":"EAN", "sku":"SKU", "descripcion":"Descripción Kame", "descripcion_ml":"Descripción ML", "unidades":"Cantidad", "motivo_anexo":"Motivo", "usuario_anexo":"Usuario"})
            cols = ["Fecha", "Código ML", "EAN", "SKU", "Descripción Kame", "Descripción ML", "Cantidad", "ML OK", "Kame OK", "Motivo", "Usuario"]
            st.dataframe(out[[c for c in cols if c in out.columns]], use_container_width=True, hide_index=True, height=320)

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
        # Escaneo necesita dos lecturas separadas:
        # 1) referencia real del FULL restaurado, sin capeo físico por avisos;
        # 2) pendiente operativo, que sí considera avisos confirmados y bloqueos.
        items_ref = get_lote_reference_items(active_lote)
        items = get_operational_items(active_lote)
        total_full = int(items_ref["unidades"].sum()) if items_ref is not None and not items_ref.empty else 0
        done_full = int(items_ref["acopiadas"].sum()) if items_ref is not None and not items_ref.empty else 0
        total_operativo = int(items["unidades"].sum()) if items is not None and not items.empty else 0
        done_operativo = int(items["acopiadas"].sum()) if items is not None and not items.empty else 0
        pendiente_operativo = 0
        if items is not None and not items.empty:
            pendiente_operativo = int((items["unidades"].astype(int) - items["acopiadas"].astype(int)).clip(lower=0).sum())
        excluidos_operativos = get_adjusted_out_items_count(active_lote)
        st.progress(min(done_operativo, total_operativo) / total_operativo if total_operativo else 0)
        a, b, c = st.columns(3)
        a.metric("Solicitado FULL", total_full)
        b.metric("Acopiado", done_full)
        c.metric("Pendiente operativo", pendiente_operativo)
        if total_operativo != total_full or excluidos_operativos > 0:
            st.caption(
                f"Objetivo operativo: {total_operativo}. "
                f"Solicitado FULL conserva el total oficial restaurado. "
                f"{excluidos_operativos} producto(s) retirado(s), bloqueado(s) o corregido(s) a 0 no suman como pendiente."
            )
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
                # Solo ERICK queda habilitado como validador PDA.
                st.session_state["scan_operator"] = SCAN_OPERATORS[0]
                st.session_state["operator_name"] = SCAN_OPERATORS[0]
                st.caption("Validador")
                st.markdown(f"**{SCAN_OPERATORS[0]}**")
            with sx2:
                if labels_pick:
                    chosen_pick = st.selectbox("Lista picking activa", labels_pick, index=default_idx_pick, key="scan_picking_select")
                    st.session_state["scan_picking_list_id"] = int(picking_options[chosen_pick])
                else:
                    st.session_state["scan_picking_list_id"] = 0
                    st.warning("No hay listas de picking activas para este lote. Crea una lista en el módulo Picking antes de escanear.")

        # Vista rápida desplegable del avance de la lista activa:
        # muestra qué productos ya están completos y cuáles faltan por validar.
        if int(st.session_state.get("scan_picking_list_id") or 0):
            render_scan_picking_progress_dropdown(int(st.session_state.get("scan_picking_list_id") or 0))

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
                "estado_rescate": "Estado rescate",
            })
            if "Estado rescate" in recientes.columns:
                recientes["Estado rescate"] = recientes["Estado rescate"].replace({
                    "NO_MATCH_SNAPSHOT": "ACOPIO_RECUPERADO_SHEETS",
                    "ACOPIO_RECUPERADO_SHEETS": "ACOPIO_RECUPERADO_SHEETS",
                })
            st.dataframe(recientes, use_container_width=True, hide_index=True, height=260)

elif page == "Picking":
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        render_picking_module(active_lote)

elif page == "Reservas Kame":
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        render_reservas_kame_module(active_lote)

elif page == "Postventa FULL":
    render_postventa_full_module(active_lote)

elif page == "Rescate Sheets":
    render_rescate_sheets()

elif page == "Supervisor":
    st.subheader("Panel supervisor")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        lote = get_lote(active_lote)
        items = get_operational_items(active_lote)
        # La impresión por bloques fue retirada del flujo operativo.
        # Se conserva la variable solo por compatibilidad con validaciones históricas.
        capacity_sup = ROLL_CAPACITY_DEFAULT
        ok_cierre, issues, cierre_data = cierre_validaciones(active_lote)
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

        tab_resumen, tab_control, tab_pendientes, tab_incid, tab_avisos, tab_anexos, tab_bloques, tab_reimp, tab_cierre, tab_auditoria = st.tabs(["Resumen", "Control operativo", "Pendientes", "Incidencias", "Avisos operacionales", "Anexos", "Etiquetas picking", "Reimpresión", "Cierre", "Auditoría"])

        with tab_resumen:
            view = items.copy()
            if not view.empty:
                view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
                resumen = pd.DataFrame([{
                    "Unidades objetivo operativo": int(view["unidades"].sum()),
                    "Unidades acopiadas": int(view["acopiadas"].sum()),
                    "Unidades pendientes físicas": int(view["pendiente"].sum()),
                    "Productos fuera del objetivo operativo": get_adjusted_out_items_count(active_lote),
                    "Líneas totales": int(len(view)),
                    "Líneas pendientes": int((view["pendiente"] > 0).sum()),
                    "Listas etiquetas pendientes": cierre_data.get("picking_label_pending", 0),
                    "Listas picking controladas": cierre_data.get("picking_label_total", 0),
                    "Incidencias abiertas": cierre_data.get("open_incidents", 0),
                    "Avisos operacionales activos": cierre_data.get("active_notices", 0),
                    "Productos anexados": cierre_data.get("anexos_total", 0),
                    "Anexos ML pendientes": cierre_data.get("anexos_ml_pendientes", 0),
                    "Anexos Kame pendientes": cierre_data.get("anexos_kame_pendientes", 0),
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
            st.info("Aquí puedes revisar y cerrar incidencias del lote activo. Las abiertas bloquean el cierre del lote.")
            sub_inc_abiertas, sub_inc_historial = st.tabs(["Abiertas / resolver", "Historial"])
            with sub_inc_abiertas:
                inc_open = get_incidencias(active_lote, status="ABIERTA")
                if inc_open.empty:
                    st.success("No hay incidencias abiertas.")
                else:
                    st.warning(f"Hay {len(inc_open)} incidencia(s) abierta(s). Resuélvelas para poder cerrar el lote.")
                    for _, r in inc_open.iterrows():
                        inc_id = int(r["id"])
                        titulo_prod = clean_text(r.get("descripcion", "")) or clean_text(r.get("codigo_ml", "")) or clean_text(r.get("sku", "")) or "Código sin match / revisar"
                        st.markdown(f"""
                        <div class='control-card'>
                            <div class='control-title'>#{inc_id} · {esc(r.get('tipo',''))} · {esc(titulo_prod)}</div>
                            <div class='control-meta'>
                                <b>Fecha:</b> {esc(fmt_dt(r.get('created_at','')))} ·
                                <b>Cantidad:</b> {int(r.get('cantidad') or 0)} ·
                                <b>Usuario:</b> {esc(r.get('usuario',''))}
                            </div>
                            <div class='control-meta'>
                                <b>ML:</b> {esc(r.get('codigo_ml',''))} ·
                                <b>EAN:</b> {esc(r.get('codigo_universal',''))} ·
                                <b>SKU:</b> {esc(r.get('sku',''))}
                            </div>
                            <div>{esc(r.get('comentario',''))}</div>
                        </div>
                        """, unsafe_allow_html=True)
                        with st.expander(f"Resolver incidencia #{inc_id}", expanded=False):
                            res_user = st.text_input("Resuelto por", value=get_operator_name(), key=f"sup_res_user_{inc_id}")
                            res_comment = st.text_area("Comentario de resolución", key=f"sup_res_comment_{inc_id}", placeholder="Ej: se corrigió etiqueta / se separó producto / se ajustó cantidad / se confirmó con bodega.")
                            if st.button("Marcar como resuelta", key=f"sup_resolve_{inc_id}", type="primary"):
                                ok_res, msg_res = resolve_incidencia(inc_id, res_user, res_comment)
                                st.success(msg_res) if ok_res else st.error(msg_res)
                                if ok_res:
                                    st.rerun()
            with sub_inc_historial:
                inc = get_incidencias(active_lote)
                if inc.empty:
                    st.success("Sin incidencias registradas.")
                else:
                    out = inc.rename(columns={"created_at": "Fecha", "tipo": "Tipo", "cantidad": "Cantidad", "comentario": "Comentario", "usuario": "Usuario", "status": "Estado", "resolved_at": "Fecha resolución", "resolved_by": "Resuelto por", "resolution_comment": "Comentario resolución", "codigo_ml": "Código ML", "codigo_universal": "Código Universal", "sku": "SKU", "descripcion": "Producto"})
                    cols = ["Fecha", "Estado", "Tipo", "Cantidad", "Código ML", "Código Universal", "SKU", "Producto", "Comentario", "Usuario", "Fecha resolución", "Resuelto por", "Comentario resolución"]
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


        with tab_anexos:
            st.info("Control de productos anexados manualmente al FULL. Estos checks no generan reserva Kame; solo confirman que el movimiento operacional fue realizado.")
            anexos_df = get_anexos_lote(active_lote)
            if anexos_df.empty:
                st.success("No hay productos anexados en este lote.")
            else:
                anexos_df = anexos_df.copy()
                anexos_df["ML"] = pd.to_numeric(anexos_df.get("anexo_ml_confirmado", 0), errors="coerce").fillna(0).astype(int).map(lambda x: "OK" if x else "PENDIENTE")
                anexos_df["Reserva Kame"] = pd.to_numeric(anexos_df.get("anexo_kame_confirmado", 0), errors="coerce").fillna(0).astype(int).map(lambda x: "OK" if x else "PENDIENTE")
                out_ax = anexos_df.rename(columns={
                    "id":"ID", "created_at":"Fecha", "codigo_ml":"Código ML", "codigo_universal":"EAN", "sku":"SKU",
                    "descripcion":"Descripción Kame", "descripcion_ml":"Descripción ML", "unidades":"Cantidad",
                    "motivo_anexo":"Motivo", "usuario_anexo":"Usuario anexo", "fecha_anexo":"Fecha anexo",
                })
                cols_ax = ["ID", "Fecha anexo", "Código ML", "EAN", "SKU", "Descripción Kame", "Descripción ML", "Cantidad", "ML", "Reserva Kame", "Motivo", "Usuario anexo"]
                st.dataframe(out_ax[[c for c in cols_ax if c in out_ax.columns]], use_container_width=True, hide_index=True, height=360)
                st.divider()
                st.subheader("Confirmar tareas de anexos")
                pendientes = anexos_df[(pd.to_numeric(anexos_df.get("anexo_ml_confirmado", 0), errors="coerce").fillna(0).astype(int) == 0) | (pd.to_numeric(anexos_df.get("anexo_kame_confirmado", 0), errors="coerce").fillna(0).astype(int) == 0)].copy()
                if pendientes.empty:
                    st.success("Todos los anexos tienen ML y Reserva Kame confirmados.")
                else:
                    opts = {}
                    for _, r in pendientes.iterrows():
                        label = f"#{int(r['id'])} · SKU {clean_text(r.get('sku',''))} · ML {clean_text(r.get('codigo_ml',''))} · {clean_text(r.get('descripcion',''))[:65]}"
                        opts[label] = int(r["id"])
                    sel_ax = st.selectbox("Producto anexado", list(opts.keys()), key="sup_anexo_select")
                    item_ax_id = opts[sel_ax]
                    row_ax = anexos_df[anexos_df["id"].astype(int) == int(item_ax_id)].iloc[0].to_dict()
                    conf_user_ax = st.text_input("Confirmado por", value=get_operator_name(), key="sup_anexo_conf_user")
                    conf_comment_ax = st.text_input("Comentario opcional", key="sup_anexo_conf_comment")
                    cc1, cc2 = st.columns(2)
                    with cc1:
                        st.caption("Modificado en ML: " + ("OK" if to_int(row_ax.get("anexo_ml_confirmado", 0)) else "PENDIENTE"))
                        if not to_int(row_ax.get("anexo_ml_confirmado", 0)):
                            if st.button("Confirmar modificado en ML", key=f"conf_ml_anexo_{item_ax_id}", type="primary"):
                                ok_conf, msg_conf = confirmar_producto_anexado(active_lote, item_ax_id, "ml", conf_user_ax, conf_comment_ax)
                                st.success(msg_conf) if ok_conf else st.error(msg_conf)
                                if ok_conf:
                                    st.rerun()
                    with cc2:
                        st.caption("Reserva Kame realizada: " + ("OK" if to_int(row_ax.get("anexo_kame_confirmado", 0)) else "PENDIENTE"))
                        if not to_int(row_ax.get("anexo_kame_confirmado", 0)):
                            if st.button("Confirmar reserva Kame realizada", key=f"conf_kame_anexo_{item_ax_id}", type="primary"):
                                ok_conf, msg_conf = confirmar_producto_anexado(active_lote, item_ax_id, "kame", conf_user_ax, conf_comment_ax)
                                st.success(msg_conf) if ok_conf else st.error(msg_conf)
                                if ok_conf:
                                    st.rerun()

        with tab_bloques:
            st.caption("Control de etiquetas por lista de picking. La impresión por bloques ya no forma parte del flujo operativo.")
            pick_label_df = get_picking_label_status_df(active_lote)
            if pick_label_df.empty:
                st.info("No hay listas de picking activas para controlar etiquetas.")
            else:
                out_pick_labels = pick_label_df.rename(columns={
                    "codigo_lista": "Lista", "asignado_a": "Picker", "estado_lista": "Estado lista",
                    "productos": "Productos", "etiquetas_requeridas": "Etiquetas requeridas",
                    "etiquetas_impresas": "Etiquetas impresas", "reimpresas": "Reimpresas",
                    "estado_etiquetas": "Estado etiquetas", "ultima_impresion": "Última impresión",
                })
                cols_pick_labels = ["Lista", "Picker", "Estado lista", "Productos", "Etiquetas requeridas", "Etiquetas impresas", "Reimpresas", "Estado etiquetas", "Última impresión"]
                st.dataframe(out_pick_labels[[c for c in cols_pick_labels if c in out_pick_labels.columns]], use_container_width=True, hide_index=True, height=520)

        with tab_reimp:
            st.info("La reimpresión operativa de etiquetas se gestiona desde Etiquetas → Por lista picking. La impresión por bloques fue retirada del flujo operativo.")
            st.caption("Desde el módulo Etiquetas puedes reimprimir una lista completa o un producto específico de una lista, siempre con motivo obligatorio.")
            hist_rep = get_reimpresiones(active_lote)
            if hist_rep.empty:
                st.info("Sin reimpresiones registradas.")
            else:
                st.subheader("Historial de reimpresiones")
                out_rep = hist_rep.rename(columns={"created_at": "Fecha", "scope": "Alcance", "block_index": "Lista/Bloque", "cantidad": "Cantidad", "motivo": "Motivo", "usuario": "Usuario", "codigo_ml": "Código ML", "sku": "SKU", "descripcion": "Producto"})
                st.dataframe(out_rep, use_container_width=True, hide_index=True, height=360)

        with tab_cierre:
            lote_close = get_lote(active_lote)
            ok_close2, issues2, data_close2 = cierre_validaciones(active_lote, int(capacity_sup))
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Estado actual", clean_text(lote_close.get("status", "ACTIVO")))
            c2.metric("Unidades pendientes", data_close2.get("pending_units", 0))
            c3.metric("Incidencias abiertas", data_close2.get("open_incidents", 0))
            c4.metric("Avisos activos", data_close2.get("active_notices", 0))
            c5.metric("Listas etiquetas pendientes", data_close2.get("picking_label_pending", 0))
            if data_close2.get("anexos_total", 0):
                st.caption(f"Anexos: {data_close2.get('anexos_total', 0)} · ML pendientes: {data_close2.get('anexos_ml_pendientes', 0)} · Kame pendientes: {data_close2.get('anexos_kame_pendientes', 0)}")
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
                force_close2 = False
                force_reason2 = ""
                if not ok_close2:
                    with st.expander("Cierre administrativo forzado"):
                        st.warning("Usar solo para lotes de práctica, lotes cargados por error o casos autorizados por administración. Quedará registrado en Sheets con los pendientes existentes.")
                        force_close2 = st.checkbox("Cerrar igualmente este lote", key="sup_force_close_chk")
                        force_reason2 = st.text_input("Motivo del cierre forzado", key="sup_force_close_reason", placeholder="Ej: lote de práctica / no operativo")
                can_close2 = ok_close2 or force_close2
                if st.button("Cerrar lote", type="primary", disabled=not can_close2, key="sup_close_btn"):
                    if not clean_text(close_user):
                        st.error("Ingresa quién cierra el lote.")
                    else:
                        ok_final, msg_final = close_lote(active_lote, close_user, close_note, force=force_close2, force_reason=force_reason2)
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
        st.info("La reimpresión por bloques fue retirada. Usa Etiquetas → Por lista picking para reimprimir listas o productos con motivo obligatorio.")
        hist = get_reimpresiones(active_lote)
        if hist.empty:
            st.info("Sin reimpresiones registradas.")
        else:
            st.subheader("Historial de reimpresiones")
            out = hist.rename(columns={"created_at": "Fecha", "scope": "Alcance", "block_index": "Lista/Bloque", "cantidad": "Cantidad", "motivo": "Motivo", "usuario": "Usuario", "codigo_ml": "Código ML", "sku": "SKU", "descripcion": "Producto"})
            st.dataframe(out, use_container_width=True, hide_index=True, height=360)

elif page == "Cierre de lote":
    st.subheader("Cierre formal de lote")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        lote = get_lote(active_lote)
        ok_close, issues, data_close = cierre_validaciones(active_lote)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Estado actual", clean_text(lote.get("status", "ACTIVO")))
        c2.metric("Unidades pendientes", data_close.get("pending_units", 0))
        c3.metric("Incidencias abiertas", data_close.get("open_incidents", 0))
        c4.metric("Listas etiquetas pendientes", data_close.get("picking_label_pending", 0))
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
            force_close = False
            force_reason = ""
            if not ok_close:
                with st.expander("Cierre administrativo forzado"):
                    st.warning("Usar solo para lotes de práctica, lotes cargados por error o casos autorizados por administración. Quedará registrado en Sheets con los pendientes existentes.")
                    force_close = st.checkbox("Cerrar igualmente este lote", key="force_close_chk")
                    force_reason = st.text_input("Motivo del cierre forzado", key="force_close_reason", placeholder="Ej: lote de práctica / no operativo")
            can_close = ok_close or force_close
            if st.button("Cerrar lote", type="primary", disabled=not can_close):
                ok_final, msg_final = close_lote(active_lote, close_user, close_note, force=force_close, force_reason=force_reason)
                st.success(msg_final) if ok_final else st.error(msg_final)
                if ok_final:
                    st.rerun()


elif page == "Etiquetas":
    st.subheader("Etiquetas Zebra 50x30")
    st.caption("Flujo productivo: etiquetas por lista de picking. Toda descarga queda registrada automáticamente; no existe marcado manual como impreso.")

    if not active_lote:
        st.warning("Primero crea o selecciona un lote FULL.")
    else:
        lote = get_lote(active_lote)
        if clean_text(lote.get("status", "ACTIVO")).upper() == "CERRADO":
            st.error(f"Lote cerrado por {clean_text(lote.get('closed_by',''))} el {fmt_dt(lote.get('closed_at',''))}. No se permite impresión normal ni reimpresión sin reapertura.")
            st.stop()

        pick_status_df = get_picking_label_status_df(active_lote)
        view = label_control_view(active_lote)

        total_lists = int(len(pick_status_df)) if not pick_status_df.empty else 0
        pending_lists = int((pick_status_df["estado_etiquetas"] == "PENDIENTE").sum()) if not pick_status_df.empty else 0
        printed_lists = int((pick_status_df["estado_etiquetas"] == "IMPRESA").sum()) if not pick_status_df.empty else 0
        reprinted_lists = int((pick_status_df["estado_etiquetas"] == "REIMPRESA").sum()) if not pick_status_df.empty else 0
        total_required = int(pick_status_df["etiquetas_requeridas"].sum()) if not pick_status_df.empty else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Listas picking", total_lists)
        c2.metric("Pendientes", pending_lists)
        c3.metric("Impresas", printed_lists)
        c4.metric("Reimpresas", reprinted_lists)
        c5.metric("Etiquetas requeridas", total_required)
        st.caption(f"Lote: {lote.get('nombre','')} · Archivo: {lote.get('archivo','')} · Hoja: {lote.get('hoja','')}")

        tab_picking_labels, tab_individual, tab_historial = st.tabs(["Por lista picking", "Reposición individual", "Historial y control"])

        with tab_picking_labels:
            st.info("Descargar ZPL lista = queda automáticamente registrada como impresa. Si la lista ya fue impresa, solo permite reimpresión con motivo obligatorio.")
            if pick_status_df.empty:
                st.warning("Aún no hay listas de picking activas para este lote.")
            else:
                filtro_lista = st.selectbox("Filtro", ["Pendientes", "Impresas", "Reimpresas", "Completadas", "Todas"], key="label_pick_filter")
                show_lists = pick_status_df.copy()
                if filtro_lista == "Pendientes":
                    show_lists = show_lists[show_lists["estado_etiquetas"] == "PENDIENTE"]
                elif filtro_lista == "Impresas":
                    show_lists = show_lists[show_lists["estado_etiquetas"] == "IMPRESA"]
                elif filtro_lista == "Reimpresas":
                    show_lists = show_lists[show_lists["estado_etiquetas"] == "REIMPRESA"]
                elif filtro_lista == "Completadas":
                    show_lists = show_lists[show_lists["estado_lista"].astype(str).str.upper() == "COMPLETADA"]

                if show_lists.empty:
                    st.info("No hay listas para este filtro.")
                else:
                    label_options = []
                    label_map = {}
                    for _, r in show_lists.iterrows():
                        label = (
                            f"{clean_text(r.get('codigo_lista',''))} · Picker {clean_text(r.get('asignado_a',''))} · "
                            f"{clean_text(r.get('estado_etiquetas',''))} · {int(r.get('productos',0))} productos · "
                            f"{int(r.get('etiquetas_requeridas',0))} etiquetas"
                        )
                        label_options.append(label)
                        label_map[label] = int(r["picking_list_id"])
                    selected_pick_label = st.selectbox("Lista de picking", label_options, key="label_pick_select_v2")
                    selected_pick_id = label_map.get(selected_pick_label)

                    if selected_pick_id:
                        block_pick = build_picking_label_block(int(selected_pick_id))
                        already_pick = get_picking_label_print_count(active_lote, int(selected_pick_id), clean_text(block_pick.get("block_key", "")))
                        is_printed = already_pick > 0
                        p1, p2, p3, p4, p5 = st.columns(5)
                        p1.metric("Productos", int(block_pick.get("products_count", 0)))
                        p2.metric("Etiquetas producto", int(block_pick.get("normal_qty", 0)))
                        p3.metric("Inicio/Fin", int(block_pick.get("separator_qty", 0)))
                        p4.metric("Total ZPL", int(block_pick.get("total_qty", 0)))
                        p5.metric("Estado", "IMPRESA" if is_printed else "PENDIENTE")
                        st.caption(
                            f"Lista: {clean_text(block_pick.get('picking_code',''))} · "
                            f"Asignado a: {clean_text(block_pick.get('asignado_a',''))} · "
                            f"Estado lista: {clean_text(block_pick.get('estado',''))}"
                        )

                        zpl_pick = zpl_for_block(block_pick).encode("utf-8")
                        fname_pick = f"etiquetas_lote_{active_lote}_{clean_text(block_pick.get('picking_code','PICKING')).replace(' ', '_')}.zpl"
                        if not is_printed:
                            st.download_button(
                                "Descargar ZPL lista",
                                data=zpl_pick,
                                file_name=fname_pick,
                                mime="text/plain",
                                key=f"download_pick_labels_normal_{active_lote}_{selected_pick_id}_{block_pick.get('block_key','')}",
                                on_click=register_picking_label_download,
                                args=(active_lote, int(selected_pick_id), block_pick),
                            )
                            st.caption("Al descargar, la lista queda registrada como IMPRESA automáticamente.")
                        else:
                            st.warning("Esta lista ya fue impresa. Para volver a descargar, registra una reimpresión controlada con motivo obligatorio.")
                            rep_usuario = st.text_input("Usuario reimpresión", value=get_operator_name(), key=f"pick_rep_user_{selected_pick_id}")
                            rep_motivo = st.selectbox(
                                "Motivo de reimpresión",
                                ["", "Rollo dañado", "Etiqueta cortada", "Error de impresora", "Reposición parcial", "Solicitud supervisor", "Otro"],
                                key=f"pick_rep_reason_{selected_pick_id}",
                            )
                            rep_motivo_otro = ""
                            if rep_motivo == "Otro":
                                rep_motivo_otro = st.text_input("Detalle motivo", key=f"pick_rep_reason_other_{selected_pick_id}")
                            motivo_final = rep_motivo_otro if rep_motivo == "Otro" else rep_motivo
                            if clean_text(motivo_final):
                                st.download_button(
                                    "Reimprimir lista",
                                    data=zpl_pick,
                                    file_name=f"reimpresion_{fname_pick}",
                                    mime="text/plain",
                                    key=f"download_pick_labels_reprint_{active_lote}_{selected_pick_id}_{block_pick.get('block_key','')}_{hashlib.sha1(clean_text(motivo_final).encode()).hexdigest()[:8]}",
                                    on_click=register_picking_label_download,
                                    args=(active_lote, int(selected_pick_id), block_pick, motivo_final, rep_usuario),
                                )
                            else:
                                st.caption("Selecciona o escribe motivo para habilitar la reimpresión.")

                        with st.expander("Ver productos / reimprimir producto de esta lista"):
                            pick_items_df = pd.DataFrame(block_pick.get("items") or [])
                            if pick_items_df.empty:
                                st.info("Sin productos.")
                            else:
                                cols_pick = [c for c in ["codigo_ml", "sku", "descripcion_ml", "descripcion", "unidades", "area", "nro"] if c in pick_items_df.columns]
                                st.dataframe(pick_items_df[cols_pick].rename(columns={"unidades": "cantidad_lista", "descripcion_ml": "descripcion_etiqueta", "descripcion": "descripcion_kame"}), use_container_width=True, hide_index=True)
                                st.divider()
                                product_options = []
                                product_map = {}
                                for i, item in pick_items_df.iterrows():
                                    label_prod = f"{norm_code(item.get('codigo_ml',''))} · SKU {norm_code(item.get('sku',''))} · {clean_text(item.get('descripcion_ml',''))[:80]}"
                                    product_options.append(label_prod)
                                    product_map[label_prod] = item.to_dict()
                                selected_prod = st.selectbox("Producto a reimprimir desde esta lista", product_options, key=f"pick_item_reprint_select_{selected_pick_id}")
                                selected_item = product_map.get(selected_prod)
                                if selected_item:
                                    qty_prod = st.number_input("Cantidad etiquetas normales", min_value=1, max_value=9999, value=1, step=1, key=f"pick_item_reprint_qty_{selected_pick_id}_{selected_item.get('id')}")
                                    prod_usuario = st.text_input("Usuario", value=get_operator_name(), key=f"pick_item_reprint_user_{selected_pick_id}_{selected_item.get('id')}")
                                    prod_motivo = st.text_input("Motivo obligatorio", key=f"pick_item_reprint_reason_{selected_pick_id}_{selected_item.get('id')}", placeholder="Ej: etiqueta dañada, reposición por corte, error de impresora")
                                    zpl_prod = zpl_for_item_with_separators_exact_pq(selected_item, int(qty_prod)).encode("utf-8")
                                    fname_prod = f"reimpresion_{clean_text(block_pick.get('picking_code','PICKING')).replace(' ', '_')}_{norm_code(selected_item.get('codigo_ml','')) or norm_code(selected_item.get('sku',''))}.zpl"
                                    if clean_text(prod_motivo):
                                        st.download_button(
                                            "Reimprimir producto de la lista",
                                            data=zpl_prod,
                                            file_name=fname_prod,
                                            mime="text/plain",
                                            key=f"pick_item_reprint_btn_{active_lote}_{selected_pick_id}_{selected_item.get('id')}_{qty_prod}_{hashlib.sha1(clean_text(prod_motivo).encode()).hexdigest()[:8]}",
                                            on_click=register_picking_item_label_reprint,
                                            args=(active_lote, int(selected_pick_id), selected_item, int(qty_prod), prod_motivo, prod_usuario),
                                        )
                                    else:
                                        st.caption("Ingresa motivo para habilitar la reimpresión del producto.")

        with tab_individual:
            st.info("Uso excepcional para reposiciones fuera de una lista. También queda registrado automáticamente al descargar.")

            # Buscador robusto para reposición individual.
            # Regla de seguridad:
            # - Primero busca dentro del lote activo y permite imprimir solo si pertenece a ese lote.
            # - Si no existe en el lote activo, muestra en qué otro FULL aparece para evitar imprimir desde el FULL equivocado.
            # - Incluye fallback desde items y picking_list_items para que un ajuste operacional no oculte productos válidos.
            search_ind = st.text_input(
                "Buscar producto",
                key=f"label_individual_search_{active_lote}",
                placeholder="Escribe Código ML, EAN, SKU o parte de la descripción",
            )
            q_ind = clean_text(search_ind).upper()

            def _row_matches_query(row, q):
                if not q:
                    return True
                haystack = " | ".join([
                    clean_text(row.get("codigo_ml", "")),
                    clean_text(row.get("codigo_universal", "")),
                    clean_text(row.get("sku", "")),
                    clean_text(row.get("descripcion", "")),
                    clean_text(row.get("descripcion_ml", "")),
                    clean_text(row.get("descripcion_kame", "")),
                ]).upper()
                return q in haystack

            def _enrich_label_row_from_item(row_dict):
                """Completa columnas mínimas para imprimir aunque el producto venga de fallback."""
                out = dict(row_dict or {})
                out["id"] = to_int(out.get("id", out.get("item_id", 0)))
                out["unidades"] = max(0, to_int(out.get("unidades", out.get("cantidad", 0))))
                out["descripcion"] = clean_text(out.get("descripcion_kame", "")) or clean_text(out.get("descripcion", ""))
                if not clean_text(out.get("descripcion_ml", "")):
                    out["descripcion_ml"] = clean_text(out.get("descripcion", ""))
                summary = get_label_print_summary(active_lote)
                printed_normal = printed_separators = reprinted_qty = 0
                last_printed = ""
                if not summary.empty and out.get("id"):
                    m = summary[summary["item_id"].astype(int) == int(out.get("id"))]
                    if not m.empty:
                        printed_normal = int(m.iloc[0].get("printed_normal", 0) or 0)
                        printed_separators = int(m.iloc[0].get("printed_separators", 0) or 0)
                        reprinted_qty = int(m.iloc[0].get("reprinted_qty", 0) or 0)
                        last_printed = clean_text(m.iloc[0].get("last_label_printed_at", ""))
                out["printed_normal"] = printed_normal
                out["printed_separators"] = printed_separators
                out["reprinted_qty"] = reprinted_qty
                out["last_label_printed_at"] = last_printed
                req = int(out.get("unidades", 0) or 0)
                out["label_pending"] = max(req - printed_normal, 0)
                if printed_normal == 0:
                    out["label_status"] = "SIN IMPRIMIR"
                elif printed_normal < req:
                    out["label_status"] = "PARCIAL"
                elif printed_normal == req:
                    out["label_status"] = "COMPLETO"
                else:
                    out["label_status"] = "SOBREIMPRESO"
                return out

            base_view = view.copy() if not view.empty else pd.DataFrame()
            if q_ind and not base_view.empty:
                view_ind = base_view[base_view.apply(lambda r: _row_matches_query(r, q_ind), axis=1)].copy()
            else:
                view_ind = base_view.copy()

            fallback_notice = ""
            if q_ind and view_ind.empty:
                # Fallback 1: items directos del lote activo. Esto recupera productos que quedaron fuera
                # del control visual de etiquetas por alguna exclusión, pero siguen perteneciendo al FULL.
                try:
                    raw_items = get_label_reprint_items(active_lote)
                except Exception:
                    raw_items = pd.DataFrame()
                if raw_items is not None and not raw_items.empty:
                    raw_matches = raw_items[raw_items.apply(lambda r: _row_matches_query(r, q_ind), axis=1)].copy()
                    if not raw_matches.empty:
                        view_ind = pd.DataFrame([_enrich_label_row_from_item(r.to_dict()) for _, r in raw_matches.iterrows()])
                        fallback_notice = "Producto recuperado desde los items del lote activo. Se permite imprimir porque pertenece a este FULL."

            if q_ind and view_ind.empty:
                # Fallback 2: picking_list_items del lote activo. Útil si el producto llegó por rescate/picking.
                try:
                    with db() as c:
                        pitems = pd.read_sql_query(
                            """
                            SELECT pli.item_id AS id, pli.codigo_ml, pli.codigo_universal, pli.sku,
                                   pli.descripcion, pli.descripcion_kame, pli.descripcion_ml,
                                   pli.familia_kame, pli.maestro_match_status,
                                   SUM(COALESCE(pli.cantidad,0)) AS unidades
                            FROM picking_list_items pli
                            JOIN picking_lists pl ON pl.id = pli.picking_list_id
                            WHERE pli.lote_id=? AND COALESCE(pl.estado,'') != 'ANULADA'
                            GROUP BY pli.item_id, pli.codigo_ml, pli.codigo_universal, pli.sku, pli.descripcion,
                                     pli.descripcion_kame, pli.descripcion_ml, pli.familia_kame, pli.maestro_match_status
                            """,
                            c,
                            params=(int(active_lote),),
                        )
                    if not pitems.empty:
                        pm = pitems[pitems.apply(lambda r: _row_matches_query(r, q_ind), axis=1)].copy()
                        if not pm.empty:
                            view_ind = pd.DataFrame([_enrich_label_row_from_item(r.to_dict()) for _, r in pm.iterrows()])
                            fallback_notice = "Producto recuperado desde una lista de picking del lote activo. Se permite imprimir porque pertenece a este FULL."
                except Exception:
                    pass

            if fallback_notice:
                st.warning(fallback_notice)

            selected_id = None
            if q_ind and view_ind.empty:
                # Diagnóstico seguro: si el código existe en otro FULL, avisamos claramente.
                # No permitimos imprimir desde el lote equivocado.
                try:
                    qlike = f"%{q_ind}%"
                    with db() as c:
                        other = pd.read_sql_query(
                            """
                            SELECT l.id AS lote_id, l.nombre AS lote_nombre, i.codigo_ml, i.codigo_universal, i.sku,
                                   i.descripcion, i.descripcion_ml, i.unidades
                            FROM items i
                            JOIN lotes l ON l.id = i.lote_id
                            WHERE i.lote_id != ?
                              AND (
                                UPPER(COALESCE(i.codigo_ml,'')) LIKE ? OR
                                UPPER(COALESCE(i.codigo_universal,'')) LIKE ? OR
                                UPPER(COALESCE(i.sku,'')) LIKE ? OR
                                UPPER(COALESCE(i.descripcion,'')) LIKE ? OR
                                UPPER(COALESCE(i.descripcion_ml,'')) LIKE ? OR
                                UPPER(COALESCE(i.descripcion_kame,'')) LIKE ?
                              )
                            ORDER BY l.id DESC
                            LIMIT 10
                            """,
                            c,
                            params=(int(active_lote), qlike, qlike, qlike, qlike, qlike, qlike),
                        )
                    if not other.empty:
                        st.error("Ese código no pertenece al lote activo. Lo encontré en otro FULL; cambia el lote activo para imprimirlo con seguridad.")
                        show_other = other.rename(columns={
                            "lote_id": "Lote ID", "lote_nombre": "FULL", "codigo_ml": "Código ML",
                            "codigo_universal": "EAN", "sku": "SKU", "descripcion": "Producto",
                            "descripcion_ml": "Descripción ML", "unidades": "Unidades",
                        })
                        st.dataframe(show_other, use_container_width=True, hide_index=True, height=180)
                    else:
                        st.warning("No encontré productos con ese código en este lote. Revisa que el lote activo sea el correcto o busca por SKU/EAN.")
                except Exception:
                    st.warning("No encontré productos con ese código en este lote. Revisa que el lote activo sea el correcto o busca por SKU/EAN.")
            elif view_ind.empty:
                st.warning("El lote activo no tiene productos disponibles para reposición individual.")
            else:
                if q_ind:
                    st.caption(f"Coincidencias encontradas en este FULL: {len(view_ind)}")
                options = []
                option_map = {}
                for _, r in view_ind.iterrows():
                    label = (
                        f"{clean_text(r.get('descripcion',''))[:80]} | "
                        f"ML {clean_text(r.get('codigo_ml',''))} | "
                        f"EAN {clean_text(r.get('codigo_universal',''))} | "
                        f"SKU {clean_text(r.get('sku',''))} | "
                        f"Estado {clean_text(r.get('label_status',''))}"
                    )
                    options.append(label)
                    option_map[label] = int(r["id"])
                selected = st.selectbox(
                    "Producto encontrado",
                    options,
                    index=0 if options else None,
                    key=f"label_individual_select_{active_lote}_{hashlib.sha1(q_ind.encode()).hexdigest()[:8]}",
                )
                selected_id = option_map.get(selected) if selected else None

            if selected_id:
                source_df = view_ind if not view_ind.empty else view
                row = source_df[source_df["id"].astype(int) == int(selected_id)].iloc[0].to_dict()
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
                st.caption(
                    f"Código ML: {clean_text(row.get('codigo_ml',''))} · "
                    f"EAN: {clean_text(row.get('codigo_universal',''))} · "
                    f"SKU: {clean_text(row.get('sku',''))}"
                )
                qty_key_ind = f"qty_individual_{active_lote}_{selected_id}"
                qty_ind = st.number_input("Cantidad de etiquetas normales a descargar", min_value=1, max_value=9999, value=1, step=1, key=qty_key_ind)
                qty_ind = max(1, int(st.session_state.get(qty_key_ind, qty_ind) or 1))
                if printed >= req:
                    st.warning("Este producto ya tiene todas sus etiquetas normales impresas. La descarga se registrará como REIMPRESIÓN.")
                elif int(qty_ind) > pending:
                    st.warning(f"La cantidad supera lo pendiente ({pending}). Puede dejar el producto SOBREIMPRESO.")

                prep_key = f"prepared_individual_zpl_{active_lote}_{selected_id}"
                if st.button("Preparar ZPL individual con esta cantidad", key=f"prepare_individual_zpl_{active_lote}_{selected_id}_{qty_ind}"):
                    zpl_text = zpl_for_item_with_separators_exact_pq(row, int(qty_ind))
                    st.session_state[prep_key] = {
                        "qty": int(qty_ind),
                        "zpl": zpl_text.encode("utf-8"),
                        "fname": f"etiqueta_{norm_code(row.get('codigo_ml','')) or 'producto'}_{norm_code(row.get('sku',''))}.zpl",
                        "row": row,
                    }

                prepared = st.session_state.get(prep_key)
                if prepared:
                    prepared_qty = int(prepared.get("qty") or 1)
                    st.success(f"ZPL preparado con ^PQ{prepared_qty}. Al descargar, queda registrado automáticamente.")
                    st.download_button(
                        "Descargar ZPL individual",
                        data=prepared.get("zpl"),
                        file_name=prepared.get("fname"),
                        mime="text/plain",
                        key=f"download_individual_prepared_{active_lote}_{selected_id}_{prepared_qty}",
                        on_click=register_individual_download,
                        args=(active_lote, prepared.get("row") or row, prepared_qty),
                    )
                else:
                    st.caption("Primero prepara el ZPL para fijar la cantidad exacta. No hay marcado manual.")

        with tab_historial:
            st.subheader("Control por lista de picking")
            if pick_status_df.empty:
                st.info("Sin listas de picking activas.")
            else:
                out_lists = pick_status_df.rename(columns={
                    "codigo_lista": "Lista", "asignado_a": "Picker", "estado_lista": "Estado lista",
                    "productos": "Productos", "etiquetas_requeridas": "Etiquetas requeridas",
                    "etiquetas_impresas": "Etiquetas impresas", "reimpresas": "Reimpresas",
                    "estado_etiquetas": "Estado etiquetas", "origen_impresion": "Origen", "ultima_impresion": "Última impresión",
                })
                cols_list = ["Lista", "Picker", "Estado lista", "Productos", "Etiquetas requeridas", "Etiquetas impresas", "Reimpresas", "Estado etiquetas", "Origen", "Última impresión"]
                st.dataframe(out_lists[[c for c in cols_list if c in out_lists.columns]], use_container_width=True, hide_index=True, height=360)

            st.divider()
            st.subheader("Historial de impresiones de etiquetas")
            with db() as c:
                hist = pd.read_sql_query(
                    """
                    SELECT created_at, print_scope, print_kind, block_index, codigo_ml, sku, descripcion,
                           cantidad, is_reprint, block_key
                    FROM label_prints
                    WHERE lote_id=?
                    ORDER BY created_at DESC, id DESC
                    LIMIT 800
                    """,
                    c,
                    params=(int(active_lote),),
                )
            if hist.empty:
                st.info("Sin impresiones registradas.")
            else:
                hist = hist.rename(columns={
                    "created_at": "Fecha", "print_scope": "Alcance", "print_kind": "Tipo",
                    "block_index": "Lista/Bloque", "codigo_ml": "Código ML", "sku": "SKU",
                    "descripcion": "Producto", "cantidad": "Cantidad", "is_reprint": "Es reimpresión", "block_key": "Key",
                })
                st.dataframe(hist, use_container_width=True, hide_index=True, height=420)

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
        items = get_operational_items(active_lote)
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
