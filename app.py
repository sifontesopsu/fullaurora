import os
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import sqlite3
from datetime import datetime
import re
import html
import json
import random
import string
import requests
from contextlib import contextmanager
# =========================
# CONFIG
# =========================
DB_NAME = "aurora_ml.db"
ADMIN_PASSWORD = "somosdelmadrid"  # clave general admin
NUM_MESAS = 4


# =========================
# TABLAS POR MÓDULO (para respaldo parcial)
# =========================
PICKING_TABLES = [
    "orders",
    "order_items",
    "pickers",
    "picking_ots",
    "picking_tasks",
    "picking_incidences",
    "cortes_tasks",
    "ot_orders",
]
FULL_TABLES = [
    "full_batches","full_batch_items","full_incidences"
]
SORTING_TABLES = [
    # Sorting v2 (único)
    "s2_manifests","s2_files","s2_sales","s2_items","s2_page_assign","s2_labels","s2_packing","s2_pack_ship","s2_dispatch",
]


PACKING_TABLES = [
    "s2_packing"
]
DISPATCH_TABLES = [
    "s2_dispatch"
]

# Maestro SKU/EAN en la misma carpeta que app.py
MASTER_FILE = "maestro_sku_ean.xlsx"



# Maestro de SKUs para CORTES (rollos / corte manual)
CORTES_FILE = "CORTES.xlsx"

# Links de publicaciones (SKU -> item/link/fotos)
# Debe estar en el repo, en la misma carpeta que app.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PUBLICATIONS_FILE = os.path.join(BASE_DIR, "links_con_imagenes.xlsx")
STOCK_KAME_FILE = os.path.join(BASE_DIR, "stock_kame.json")
# =========================
# SFX (Sistema A: CLICK + OK/ERR) — estable para Chrome/Android
# =========================
def _sfx_init_state():
    ss = st.session_state
    if "sfx_enabled" not in ss:
        ss["sfx_enabled"] = True
    if "sfx_volume" not in ss:
        ss["sfx_volume"] = 0.55  # 0..1
    if "sfx_unlocked" not in ss:
        ss["sfx_unlocked"] = False
    if "_sfx_kind" not in ss:
        ss["_sfx_kind"] = ""
    if "_sfx_nonce" not in ss:
        ss["_sfx_nonce"] = 0

def sfx_sidebar():
    _sfx_init_state()
    with st.sidebar.expander("🔊 Sonidos", expanded=True):
        st.session_state["sfx_enabled"] = st.toggle("Sonido", value=st.session_state["sfx_enabled"], key="sfx_enabled_toggle")
        vol_pct = st.slider("Volumen", min_value=0, max_value=100, value=int(st.session_state["sfx_volume"]*100), step=5, key="sfx_volume_slider")
        st.session_state["sfx_volume"] = max(0.0, min(1.0, vol_pct/100.0))

        if st.button("Activar sonido", disabled=bool(st.session_state.get("sfx_unlocked", False)), use_container_width=True):
            st.session_state["sfx_unlocked"] = True
            st.rerun()

        if st.session_state.get("sfx_unlocked", False):
            st.success("Audio habilitado ✅")
        else:
            st.info("En Chrome debes tocar “Activar sonido” una vez.")

def _sfx_unlock_render():
    _sfx_init_state()
    if not st.session_state.get("sfx_enabled", True):
        return
    if not st.session_state.get("sfx_unlocked", False):
        return

    components.html(
        '''
        <script>
        (function(){
          try{
            const root = window.parent || window;
            const AC = root.AudioContext || root.webkitAudioContext;
            if(!root.__auroraAudio && AC){
              root.__auroraAudio = new AC();
            }
            if(root.__auroraAudio && root.__auroraAudio.state === "suspended"){
              root.__auroraAudio.resume();
            }
          }catch(e){}
        })();
        </script>
        ''',
        height=0,
    )

def _sfx_global_click_hook():
    _sfx_init_state()
    enabled = bool(st.session_state.get("sfx_enabled", True))
    unlocked = bool(st.session_state.get("sfx_unlocked", False))
    vol = float(st.session_state.get("sfx_volume", 0.55))

    cfg_js = json.dumps({"enabled": enabled, "unlocked": unlocked, "volume": vol})

    components.html(
        '''
        <script>
        (function(){
          try{
            const root = window.parent || window;
            root.__auroraSfxCfg = __CFG__;
            const doc = root.document;
            if(root.__auroraClickHookInstalled) return;
            root.__auroraClickHookInstalled = true;

            function playClick(){
              try{
                const cfg = root.__auroraSfxCfg || {enabled:false, unlocked:false, volume:0.5};
                if(!cfg.enabled || !cfg.unlocked) return;
                const ctx = root.__auroraAudio;
                if(!ctx) return;
                const now = ctx.currentTime;
                const o = ctx.createOscillator();
                const g = ctx.createGain();
                o.type = "square";
                o.frequency.setValueAtTime(1200, now);
                g.gain.setValueAtTime(0.0001, now);
                g.gain.exponentialRampToValueAtTime(Math.max(0.02, cfg.volume*0.10), now+0.005);
                g.gain.exponentialRampToValueAtTime(0.0001, now+0.03);
                o.connect(g); g.connect(ctx.destination);
                o.start(now); o.stop(now+0.04);
              }catch(e){}
            }

            doc.addEventListener("click", function(ev){
              const t = ev.target;
              if(!t) return;
              const btn = t.closest ? t.closest("button") : null;
              if(!btn) return;
              playClick();
            }, true);
          }catch(e){}
        })();
        </script>
        '''.replace("__CFG__", cfg_js),
        height=0,
    )

def sfx_emit(kind: str):
    _sfx_init_state()
    if not st.session_state.get("sfx_enabled", True):
        return
    if not st.session_state.get("sfx_unlocked", False):
        return
    kind = (kind or "").upper().strip()
    if kind not in ("OK", "ERR"):
        kind = "ERR"
    st.session_state["_sfx_kind"] = kind
    st.session_state["_sfx_nonce"] = int(st.session_state.get("_sfx_nonce", 0)) + 1

def sfx_render_pending():
    _sfx_init_state()
    if not st.session_state.get("sfx_enabled", True):
        return
    if not st.session_state.get("sfx_unlocked", False):
        return
    kind = (st.session_state.get("_sfx_kind") or "").upper().strip()
    if not kind:
        return

    st.session_state["_sfx_kind"] = ""
    nonce = int(st.session_state.get("_sfx_nonce", 0))

    kind_js = json.dumps(kind)

    components.html(
        '''
        <script>
        (function(){
          try{
            const root = window.parent || window;
            const cfg = root.__auroraSfxCfg || {enabled:false, unlocked:false, volume:0.5};
            if(!cfg.enabled || !cfg.unlocked) return;
            const ctx = root.__auroraAudio;
            if(!ctx) return;

            const kind = __KIND__;
            const vol = Math.max(0.0, Math.min(1.0, cfg.volume || 0.5));
            const now = ctx.currentTime;

            function tone(freq, t0, dur, type, gain){
              const o = ctx.createOscillator();
              const g = ctx.createGain();
              o.type = type || "square";
              o.frequency.setValueAtTime(freq, t0);
              g.gain.setValueAtTime(0.0001, t0);
              g.gain.exponentialRampToValueAtTime(Math.max(0.02, vol*(gain||0.12)), t0+0.01);
              g.gain.exponentialRampToValueAtTime(0.0001, t0+dur);
              o.connect(g); g.connect(ctx.destination);
              o.start(t0); o.stop(t0+dur+0.02);
            }

            function ok(){
              tone(988,  now+0.00, 0.06, "square", 0.14);
              tone(1319, now+0.07, 0.06, "square", 0.13);
              tone(1760, now+0.14, 0.06, "square", 0.12);
            }
            function err(){
              tone(220, now+0.00, 0.16, "square", 0.12);
              tone(180, now+0.10, 0.18, "square", 0.10);
            }

            if(kind === "OK") ok();
            else err();
          }catch(e){}
        })();
        </script>
        <!-- nonce:__NONCE__ -->
        '''.replace("__KIND__", kind_js).replace("__NONCE__", str(nonce)),
        height=0,
    )

# =========================
# TIMEZONE CHILE
# =========================
try:
    from zoneinfo import ZoneInfo  # py3.9+
    CL_TZ = ZoneInfo("America/Santiago")
    UTC_TZ = ZoneInfo("UTC")
except Exception:
    CL_TZ = None
    UTC_TZ = None


# PDF manifiestos
try:
    import pdfplumber
    HAS_PDF_LIB = True
except ImportError:
    HAS_PDF_LIB = False


# =========================
# UTILIDADES
# =========================
def now_iso():
    """ISO timestamp in Chile time (America/Santiago) with UTC offset."""
    if CL_TZ is not None:
        return datetime.now(CL_TZ).isoformat(timespec="seconds")
    return datetime.now().isoformat(timespec="seconds")



# =========================
# TEXT HELPERS
# =========================
UBC_RE = re.compile(r"\[\s*UBC\s*:\s*([^\]]+)\]", re.IGNORECASE)


def to_chile_display(iso_str: str) -> str:
    """Muestra timestamps en hora Chile.

    - Si el ISO trae zona/offset, se convierte a America/Santiago.
    - Si es naive (sin zona), se muestra tal cual (asumido ya en hora Chile).
    """
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(str(iso_str))
        if CL_TZ is None:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is not None:
            dt = dt.astimezone(CL_TZ)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(iso_str)


_STOCK_KAME_CACHE = {"path": None, "mtime": None, "data": None}


def load_stock_kame(path: str = STOCK_KAME_FILE) -> tuple[dict[str, float], str]:
    """Carga stock_kame.json desde disco con cache por mtime.

    Espera un JSON con:
    - updated_at: fecha/hora última actualización
    - stock: {sku: cantidad}
    """
    if not path or not os.path.exists(path):
        return {}, ""

    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = None

    cached = _STOCK_KAME_CACHE
    if cached.get("path") == path and cached.get("mtime") == mtime and cached.get("data") is not None:
        payload = cached["data"]
    else:
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            return {}, ""
        cached.update({"path": path, "mtime": mtime, "data": payload})

    stock_raw = payload.get("stock") or {}
    stock_map: dict[str, float] = {}
    for k, v in stock_raw.items():
        sku = normalize_sku(k)
        if not sku:
            continue
        try:
            stock_map[sku] = float(v)
        except Exception:
            continue

    updated_at = str(payload.get("updated_at") or "").strip()
    return stock_map, updated_at


def obtener_stock_kame(sku: str, path: str = STOCK_KAME_FILE):
    stock_map, _ = load_stock_kame(path)
    sku_n = normalize_sku(sku)
    if not sku_n:
        return None
    return stock_map.get(sku_n)


def obtener_fecha_stock_kame(path: str = STOCK_KAME_FILE) -> str:
    _, updated_at = load_stock_kame(path)
    return updated_at


def format_stock_kame(value) -> str:
    if value is None:
        return "N/D"
    try:
        num = float(value)
        if num.is_integer():
            return f"{int(num):,}".replace(",", ".")
        return f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(value)


def normalize_sku(value) -> str:
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    if re.fullmatch(r"\d+(\.\d+)?[eE][+-]?\d+", s):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    return s


def only_digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or ""))


def split_barcodes(cell_value) -> list[str]:
    if cell_value is None:
        return []
    s = str(cell_value).strip()
    if not s or s.lower() == "nan":
        return []
    parts = re.split(r"[\s,;]+", s)
    out = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        d = only_digits(p)
        if d:
            out.append(d)
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def get_conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)


# =========================
# DB HELPERS (centralizados)
# =========================
@contextmanager
def db_conn(commit: bool = False):
    """Context manager para SQLite.
    - commit=True: hace commit al salir si no hubo excepción; si hubo, rollback.
    """
    conn = get_conn()
    try:
        yield conn
        if commit:
            conn.commit()
    except Exception:
        if commit:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def db_fetchone(sql: str, params: tuple = ()):
    with db_conn(commit=False) as conn:
        return conn.execute(sql, params).fetchone()


def db_fetchall(sql: str, params: tuple = ()):
    with db_conn(commit=False) as conn:
        return conn.execute(sql, params).fetchall()


def db_exec(sql: str, params: tuple = (), commit: bool = False):
    with db_conn(commit=commit) as conn:
        cur = conn.execute(sql, params)
        return cur



# =========================
# BACKUP/RESTORE POR MÓDULO (SQLite parcial)
# =========================
def _db_table_exists(conn, table: str) -> bool:
    try:
        row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,)).fetchone()
        return bool(row)
    except Exception:
        return False

def _export_tables_to_db_bytes(tables: list[str]) -> bytes:
    """Exporta SOLO las tablas indicadas a un .db (bytes). No toca el DB actual."""
    import tempfile
    conn_src = get_conn()
    csrc = conn_src.cursor()
    # Crear DB temporal
    fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn_out = sqlite3.connect(tmp_path, check_same_thread=False)
    cout = conn_out.cursor()
    try:
        for tname in tables:
            if not _db_table_exists(conn_src, tname):
                continue
            row = csrc.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (tname,)).fetchone()
            create_sql = row[0] if row and row[0] else None
            if not create_sql:
                continue
            cout.execute(create_sql)
            rows = csrc.execute(f"SELECT * FROM {tname};").fetchall()
            if rows:
                ncols = len(rows[0])
                ph = ",".join(["?"] * ncols)
                cout.executemany(f"INSERT INTO {tname} VALUES ({ph});", rows)
        conn_out.commit()
        conn_out.close()
        conn_src.close()
        with open(tmp_path, "rb") as f:
            data = f.read()
        return data
    finally:
        try:
            conn_out.close()
        except Exception:
            pass
        try:
            conn_src.close()
        except Exception:
            pass
        try:
            os.remove(tmp_path)
        except Exception:
            pass

def _restore_tables_from_db_bytes(db_bytes: bytes, tables: list[str]) -> tuple[bool, str|None]:
    """Restaura SOLO las tablas indicadas desde un .db (bytes). Mantiene el resto intacto."""
    import tempfile
    # Guardar uploaded db a temp
    fd, up_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    with open(up_path, "wb") as f:
        f.write(db_bytes)

    conn_src = sqlite3.connect(up_path, check_same_thread=False)
    csrc = conn_src.cursor()
    conn_dst = get_conn()
    cdst = conn_dst.cursor()

    try:
        # Validación mínima: que exista al menos 1 de las tablas esperadas
        any_ok = False
        for tname in tables:
            if _db_table_exists(conn_src, tname):
                any_ok = True
                break
        if not any_ok:
            return False, "El respaldo no contiene las tablas esperadas para este módulo."

        # Transacción de reemplazo parcial
        cdst.execute("BEGIN;")
        for tname in tables:
            if not _db_table_exists(conn_src, tname):
                continue

            # Leer schema desde respaldo
            row = csrc.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (tname,)).fetchone()
            create_sql = row[0] if row and row[0] else None
            if not create_sql:
                continue

            # Reemplazar tabla
            cdst.execute(f"DROP TABLE IF EXISTS {tname};")
            cdst.execute(create_sql)

            # Copiar filas
            rows = csrc.execute(f"SELECT * FROM {tname};").fetchall()
            if rows:
                ncols = len(rows[0])
                ph = ",".join(["?"] * ncols)
                cdst.executemany(f"INSERT INTO {tname} VALUES ({ph});", rows)

        conn_dst.commit()
        return True, None
    except Exception as e:
        try:
            conn_dst.rollback()
        except Exception:
            pass
        return False, str(e)
    finally:
        try:
            conn_src.close()
        except Exception:
            pass
        try:
            conn_dst.close()
        except Exception:
            pass
        try:
            os.remove(up_path)
        except Exception:
            pass

def require_admin_access():
    """Clave general única para entrar a cualquier módulo de administrador."""
    if st.session_state.get("admin_access_granted", False):
        return True

    st.warning("🔒 Acceso restringido a administradores")
    pwd = st.text_input("Ingresa clave general de administrador", type="password", key="admin_access_password")

    if pwd:
        if pwd == ADMIN_PASSWORD:
            st.session_state["admin_access_granted"] = True
            st.success("Acceso autorizado ✅")
            st.rerun()
        else:
            st.error("Clave incorrecta ❌")

    return False



def _render_module_backup_ui(scope_key: str, scope_label: str, tables: list[str]):
    """UI para respaldar/restaurar SOLO un módulo (tablas específicas)."""
    with st.expander(f"💾 Respaldo / Restauración — {scope_label}", expanded=False):
        st.caption(
            "Este respaldo es SOLO de este módulo (tablas específicas). "
            "No toca datos de otros módulos. "
            "Nota: el mapa común de códigos (sku_barcodes) no se incluye aquí."
        )

        # Backup
        try:
            data = _export_tables_to_db_bytes(tables)
            st.download_button(
                f"⬇️ Descargar respaldo ({scope_key}.db)",
                data=data,
                file_name=f"aurora_{scope_key}.db",
                mime="application/octet-stream",
                use_container_width=True,
                key=f"dl_{scope_key}",
            )
        except Exception as e:
            st.warning(f"No se pudo preparar el respaldo: {e}")

        st.divider()

        up = st.file_uploader(
            f"⬆️ Restaurar respaldo de {scope_label} (.db)",
            type=["db"],
            key=f"up_{scope_key}",
        )
        col1, col2 = st.columns([2, 1])
        with col1:
            confirm = st.text_input("Escribe RESTAURAR para confirmar", value="", key=f"cf_{scope_key}")
        with col2:
            do = st.button(
                "♻️ Restaurar",
                type="primary",
                disabled=not (up and confirm.strip().upper() == "RESTAURAR"),
                key=f"do_{scope_key}",
            )
        if do and up is not None:
            ok, err = _restore_tables_from_db_bytes(up.getvalue(), tables)
            if ok:
                st.success("✅ Restaurado. Recargando…")
                st.rerun()
            else:
                st.error(f"No se pudo restaurar: {err}")





def force_tel_keyboard(label: str):
    """Fuerza teclado numérico tipo 'teléfono' para el input con aria-label=label."""
    safe = label.replace("\\", "\\\\").replace('"', '\\"')
    components.html(
        f"""
        <script>
        (function() {{
          const label = "{safe}";
          let tries = 0;
          function apply() {{
            const inputs = window.parent.document.querySelectorAll('input[aria-label="' + label + '"]');
            if (!inputs || inputs.length === 0) {{
              tries++;
              if (tries < 30) setTimeout(apply, 200);
              return;
            }}
            inputs.forEach((el) => {{
              try {{
                el.setAttribute('type', 'tel');
                el.setAttribute('inputmode', 'numeric');
                el.setAttribute('pattern', '[0-9]*');
                el.setAttribute('autocomplete', 'off');
              }} catch (e) {{}}
            }});
          }}
          apply();
          setTimeout(apply, 500);
          setTimeout(apply, 1200);
        }})();
        </script>
        """,
        height=0,
    )


def autofocus_input(label: str):
    """Pone foco inmediato en un input por aria-label."""
    safe = label.replace("\\", "\\\\").replace('"', '\\"')
    components.html(
        f"""
        <script>
        (function() {{
          const label = "{safe}";
          let tries = 0;
          function focusIt() {{
            const el = window.parent.document.querySelector('input[aria-label="' + label + '"]');
            if (!el) {{
              tries++;
              if (tries < 40) setTimeout(focusIt, 120);
              return;
            }}
            try {{
              el.focus();
              el.select();
            }} catch (e) {{}}
          }}
          focusIt();
          setTimeout(focusIt, 300);
          setTimeout(focusIt, 900);
        }})();
        </script>
        """,
        height=0,
    )


# =========================
# DB INIT
# =========================
def init_db():
    conn = get_conn()
    c = conn.cursor()

    # --- FLEX/COLECTA ---
    c.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ml_order_id TEXT UNIQUE,
        buyer TEXT,
        created_at TEXT
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER,
        sku_ml TEXT,
        title_ml TEXT,
        title_tec TEXT,
        qty INTEGER
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pickers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS picking_ots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_code TEXT UNIQUE,
        picker_id INTEGER,
        status TEXT,
        created_at TEXT,
        closed_at TEXT
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS picking_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id INTEGER,
        sku_ml TEXT,
        title_ml TEXT,
        title_tec TEXT,
        qty_total INTEGER,
        qty_picked INTEGER DEFAULT 0,
        status TEXT DEFAULT 'PENDING',
        decided_at TEXT,
        confirm_mode TEXT,
        defer_rank INTEGER DEFAULT 0,
        defer_at TEXT
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS picking_incidences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id INTEGER,
        sku_ml TEXT,
        qty_total INTEGER,
        qty_picked INTEGER,
        qty_missing INTEGER,
        reason TEXT,
        note TEXT,
        created_at TEXT
    );
    """)

    # --- CORTES (rollos / corte manual) ---
    c.execute("""
    CREATE TABLE IF NOT EXISTS cortes_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id INTEGER,
        sku_ml TEXT,
        title_ml TEXT,
        title_tec TEXT,
        qty_total INTEGER,
        created_at TEXT
    );
    """)


    c.execute("""
    CREATE TABLE IF NOT EXISTS ot_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id INTEGER,
        order_id INTEGER
    );
    """)
    # Maestro EAN/SKU (común)
    # Maestro EAN/SKU (común)
    c.execute("""
    CREATE TABLE IF NOT EXISTS sku_barcodes (
        barcode TEXT PRIMARY KEY,
        sku_ml TEXT
    );
    """)

    # --- Reparación robusta de schema para sku_barcodes (BD antiguas en Streamlit Cloud) ---
    # Esta tabla se puede reconstruir desde el maestro, así que es seguro "normalizarla".
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(sku_barcodes);").fetchall()]
        cols_set = set(cols or [])
        if "barcode" not in cols_set:
            # Algunos DB viejos tenían 'ean' u otro nombre: recrear
            c.execute("ALTER TABLE sku_barcodes RENAME TO sku_barcodes_old;")
            c.execute("CREATE TABLE IF NOT EXISTS sku_barcodes (barcode TEXT PRIMARY KEY, sku_ml TEXT);")
        else:
            # Asegurar sku_ml
            if "sku_ml" not in cols_set and "sku" in cols_set:
                try:
                    c.execute("ALTER TABLE sku_barcodes ADD COLUMN sku_ml TEXT;")
                    c.execute("UPDATE sku_barcodes SET sku_ml=sku WHERE (sku_ml IS NULL OR sku_ml='');")
                except Exception:
                    pass
            cols_now = [r[1] for r in c.execute("PRAGMA table_info(sku_barcodes);").fetchall()]
            if "sku_ml" not in set(cols_now):
                # no se pudo agregar -> recrear limpia
                c.execute("ALTER TABLE sku_barcodes RENAME TO sku_barcodes_old;")
                c.execute("CREATE TABLE IF NOT EXISTS sku_barcodes (barcode TEXT PRIMARY KEY, sku_ml TEXT);")
        # Si quedó una tabla old, intentar copiar lo que se pueda
        if _db_table_exists(conn, "sku_barcodes_old"):
            old_cols = [r[1] for r in c.execute("PRAGMA table_info(sku_barcodes_old);").fetchall()]
            if "barcode" in old_cols:
                src_sku = "sku_ml" if "sku_ml" in old_cols else ("sku" if "sku" in old_cols else None)
                if src_sku:
                    try:
                        c.execute(f"INSERT OR IGNORE INTO sku_barcodes(barcode, sku_ml) SELECT barcode, {src_sku} FROM sku_barcodes_old;")
                    except Exception:
                        pass
            # no borramos old automáticamente; si quieres limpiar, se puede en un mantenimiento futuro
    except Exception:
        # Si algo falla aquí, no botamos la app; solo dejamos la tabla como esté.
        pass


    # Links / publicaciones (para ver fotos)
    c.execute("""
    CREATE TABLE IF NOT EXISTS sku_publications (
        sku_ml TEXT PRIMARY KEY,
        ml_item_id TEXT,
        title TEXT,
        link TEXT,
        image_url TEXT,
        updated_at TEXT
    );
    """)

    # --- CONTADOR DE PAQUETES (Flex/Colecta) ---
    c.execute("""
    CREATE TABLE IF NOT EXISTS pkg_counter_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT,               -- FLEX / COLECTA
    status TEXT DEFAULT 'OPEN',
    created_at TEXT,
    closed_at TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS pkg_counter_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    label_key TEXT,
    raw TEXT,
    scanned_at TEXT,
    UNIQUE(run_id, label_key)
    );
    """)

    # --- FULL: Acopio ---
    c.execute("""
    CREATE TABLE IF NOT EXISTS full_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_name TEXT,
        status TEXT DEFAULT 'OPEN',
        created_at TEXT,
        closed_at TEXT
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS full_batch_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER,
        sku_ml TEXT,
        title TEXT,
        areas TEXT,
        nros TEXT,
        etiquetar TEXT,
        es_pack TEXT,
        instruccion TEXT,
        vence TEXT,
        qty_required INTEGER DEFAULT 0,
        qty_checked INTEGER DEFAULT 0,
        status TEXT DEFAULT 'PENDING',
        updated_at TEXT,
        UNIQUE(batch_id, sku_ml)
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS full_incidences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER,
        sku_ml TEXT,
        qty_required INTEGER,
        qty_checked INTEGER,
        diff INTEGER,
        reason TEXT,
        created_at TEXT
    );
    """)
    # --- MIGRACIONES SUAVES (para BD antiguas) ---
    def _cols(table: str) -> set:
        try:
            c.execute(f"PRAGMA table_info({table});")
            return {r[1] for r in c.fetchall()}
        except Exception:
            return set()

    def _ensure_col(table: str, col: str, ddl: str):
        cols = _cols(table)
        if col in cols:
            return
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl};")
        except Exception:
            # Si falla (por locks o tablas raras), no botar la app.
            pass

        # picking_tasks (nuevas columnas para reordenar por "Surtido en venta")
    _ensure_col("picking_tasks", "defer_rank", "INTEGER DEFAULT 0")
    _ensure_col("picking_tasks", "defer_at", "TEXT")
    _ensure_col("picking_tasks", "family", "TEXT")
    _ensure_col("picking_ots", "model", "TEXT")
    _ensure_col("picking_ots", "batch_key", "TEXT")
    _ensure_col("picking_ots", "batch_label", "TEXT")
    _ensure_col("picking_incidences", "note", "TEXT")


    # sku_publications
    _ensure_col("sku_publications", "sku_ml", "TEXT")
    _ensure_col("sku_publications", "ml_item_id", "TEXT")
    _ensure_col("sku_publications", "title", "TEXT")
    _ensure_col("sku_publications", "link", "TEXT")
    _ensure_col("sku_publications", "image_url", "TEXT")
    _ensure_col("sku_publications", "updated_at", "TEXT")


    conn.commit()
    conn.close()

# =========================
# MAESTRO SKU/EAN (AUTO)
# =========================
def load_master_from_path(path: str) -> tuple[dict, dict, dict, list]:
    inv_map_sku: dict[str, str] = {}
    familia_map_sku: dict[str, str] = {}
    barcode_to_sku: dict[str, str] = {}
    conflicts: list = []

    if not path or not os.path.exists(path):
        return inv_map_sku, familia_map_sku, barcode_to_sku, conflicts

    df = pd.read_excel(path, dtype=str)
    cols = df.columns.tolist()
    lower = [str(c).strip().lower() for c in cols]

    sku_col = None
    if "sku" in lower:
        sku_col = cols[lower.index("sku")]

    tech_col = None
    for cand in ["artículo", "articulo", "descripcion", "descripción", "nombre", "producto", "detalle"]:
        if cand in lower:
            tech_col = cols[lower.index(cand)]
            break

    fam_col = None
    # columna nueva: "Familia"
    for cand in ["familia", "family"]:
        if cand in lower:
            fam_col = cols[lower.index(cand)]
            break

    barcode_col = None
    for cand in ["codigos de barras", "códigos de barras", "codigo de barras", "código de barras", "barcode", "ean", "eans"]:
        if cand in lower:
            barcode_col = cols[lower.index(cand)]
            break

    # Fallback por si el archivo no trae headers claros
    if sku_col is None or tech_col is None:
        df0 = pd.read_excel(path, header=None, dtype=str)
        if df0.shape[1] >= 2:
            a, b = df0.columns[0], df0.columns[1]
            sample = df0.head(200)

            def score(series):
                s = 0
                for v in series:
                    if re.fullmatch(r"\d{4,}", normalize_sku(v)):
                        s += 1
                return s

            sa, sb = score(sample[a]), score(sample[b])
            if sb >= sa:
                sku_col, tech_col = b, a
            else:
                sku_col, tech_col = a, b
            df = df0
            barcode_col = None
            fam_col = None

    for _, r in df.iterrows():
        sku = normalize_sku(r.get(sku_col, ""))
        if not sku:
            continue

        tech = str(r.get(tech_col, "")).strip() if tech_col is not None else ""
        if tech and tech.lower() != "nan":
            inv_map_sku[sku] = tech

        if fam_col is not None:
            fam = str(r.get(fam_col, "")).strip()
            if fam and fam.lower() != "nan":
                familia_map_sku[sku] = fam

        if barcode_col is not None:
            codes = split_barcodes(r.get(barcode_col, ""))
            for code in codes:
                if code in barcode_to_sku and barcode_to_sku[code] != sku:
                    conflicts.append((code, barcode_to_sku[code], sku))
                    continue
                barcode_to_sku[code] = sku

    return inv_map_sku, familia_map_sku, barcode_to_sku, conflicts



# Cache extra: lookup directo del título "tal cual" en el maestro (sin limpiar)
_MASTER_DF_CACHE = {"path": None, "mtime": None, "df": None}

def _load_master_df_cached(path: str):
    """Carga el Excel del maestro una sola vez (por mtime) para poder buscar el texto crudo."""
    if not path or not os.path.exists(path):
        return None
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = None

    if (_MASTER_DF_CACHE.get("path") == path and _MASTER_DF_CACHE.get("mtime") == mtime
            and _MASTER_DF_CACHE.get("df") is not None):
        return _MASTER_DF_CACHE["df"]

    try:
        dfm = pd.read_excel(path, dtype=str)
    except Exception:
        return None

    _MASTER_DF_CACHE.update({"path": path, "mtime": mtime, "df": dfm})
    return dfm

def master_raw_title_lookup(path: str, sku: str) -> str:
    """Devuelve el texto EXACTO del maestro para ese SKU (tal cual viene en la celda)."""
    dfm = _load_master_df_cached(path)
    if dfm is None or dfm.empty:
        return ""
    cols = list(dfm.columns)
    lower = [str(c).strip().lower() for c in cols]

    # columna SKU
    sku_col = None
    if "sku" in lower:
        sku_col = cols[lower.index("sku")]
    if sku_col is None:
        return ""

    # preferir columnas típicas de descripción/título
    pref = [
        "descripción", "descripcion", "artículo", "articulo",
        "detalle", "producto", "nombre", "descripción pack", "nombre pack"
    ]
    title_col = None
    for cand in pref:
        if cand in lower:
            title_col = cols[lower.index(cand)]
            break
    # si no hay, tomar la primera no-SKU
    if title_col is None:
        for c in cols:
            if c != sku_col:
                title_col = c
                break
    if title_col is None:
        return ""

    target = normalize_sku(sku)
    if not target:
        return ""

    try:
        ser = dfm[sku_col].astype(str).map(normalize_sku)
        hits = dfm.loc[ser == target]
    except Exception:
        return ""

    if hits.empty:
        return ""

    val = hits.iloc[0][title_col]
    if val is None:
        return ""
    sval = str(val)
    if sval.lower() == "nan":
        return ""
    return sval


def upsert_barcodes_to_db(barcode_to_sku: dict):
    """Guarda el mapa EAN->SKU en SQLite.

    Importante: en Streamlit Cloud la DB puede quedar con esquemas antiguos. Esta función es defensiva:
    - verifica que exista la tabla y columnas esperadas (barcode, sku_ml)
    - si no calza, intenta recrearla (es seguro: se reconstruye desde el maestro)
    """
    if not barcode_to_sku:
        return

    # TODO QUIRÚRGICO: mantener toda la lógica original, pero asegurando que el cursor
    # se use dentro de la conexión (antes se cerraba el conn al salir del context manager).
    with db_conn(commit=True) as conn:
        c = conn.cursor()
        try:
            # asegurar tabla/columnas
            c.execute("""CREATE TABLE IF NOT EXISTS sku_barcodes (
                barcode TEXT PRIMARY KEY,
                sku_ml TEXT
            );""")
            cols = [r[1] for r in c.execute("PRAGMA table_info(sku_barcodes);").fetchall()]
            if "barcode" not in cols or ("sku_ml" not in cols):
                try:
                    c.execute("ALTER TABLE sku_barcodes RENAME TO sku_barcodes_old;")
                except Exception:
                    pass
                c.execute("DROP TABLE IF EXISTS sku_barcodes;")
                c.execute("""CREATE TABLE IF NOT EXISTS sku_barcodes (
                    barcode TEXT PRIMARY KEY,
                    sku_ml TEXT
                );""")
        except Exception:
            # si no podemos asegurar schema, no bloqueamos la app
            return

        try:
            c.execute("BEGIN;")
            for bc, sku in (barcode_to_sku or {}).items():
                bc = only_digits(bc)
                if not bc:
                    continue
                c.execute(
                    "INSERT OR REPLACE INTO sku_barcodes (barcode, sku_ml) VALUES (?, ?)",
                    (bc, str(sku).strip()),
                )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass


def resolve_scan_to_sku(scan: str, barcode_to_sku: dict) -> str:
    raw = str(scan).strip()
    digits = only_digits(raw)

    # 1) Prefer in-memory map loaded from maestro
    if digits and digits in (barcode_to_sku or {}):
        return barcode_to_sku[digits]

    # 2) Fallback to DB map (persists across reruns)
    if digits:
        try:
            row = db_fetchone("SELECT sku_ml FROM sku_barcodes WHERE barcode=?", (digits,))
            if row and row[0]:
                return str(row[0]).strip()
        except Exception:
            pass

    # 3) As last resort: treat scan as SKU text
    return normalize_sku(raw)


def extract_location_suffix(text: str) -> str:
    """Extracts location/UBC suffix like '[UBC: 1234]' from a title."""
    t = str(text or "").strip()
    if not t:
        return ""
    # Common pattern in Aurora: '[UBC: 2260]' or '[ubc: 2260]'
    m = re.search(r"(\[\s*UBC\s*:\s*[^\]]+\])\s*$", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Sometimes without brackets: 'UBC: 2260' at end
    m = re.search(r"(UBC\s*:\s*\d+)\s*$", t, flags=re.IGNORECASE)
    if m:
        return f"[{m.group(1).strip()}]"
    return ""






@st.cache_data(show_spinner=False)
def get_master_cached(master_path: str) -> tuple[dict, dict, dict, list]:
    return load_master_from_path(master_path)


def master_bootstrap(master_path: str):
    inv_map_sku, familia_map_sku, barcode_to_sku, conflicts = get_master_cached(master_path)
    upsert_barcodes_to_db(barcode_to_sku)
    return inv_map_sku, familia_map_sku, barcode_to_sku, conflicts



# =========================
# PUBLICACIONES (Links + Fotos por SKU)
# =========================
ML_ITEM_RE = re.compile(r"\b(ML[A-Z]{1,3}[-]?(\d+))\b", re.IGNORECASE)

def extract_ml_item_id(value: str) -> str:
    """Extrae un ID tipo MLC123 o MLC-123 desde un link o celda."""
    s = str(value or "").strip()
    if not s:
        return ""
    m = ML_ITEM_RE.search(s)
    if not m:
        return ""
    prefix = m.group(1).upper().replace("-", "")
    # Normalizar: MLC123456789 (sin guión)
    return prefix

def import_publication_links_excel(file) -> pd.DataFrame:
    """Lee Excel de publicaciones/imágenes.

    Soporta formatos:
      - Antiguo: SKU, Id, Título, Link
      - Nuevo: SKU, Link, Imagen (y opcional ImgStatus / Título / Id)

    Devuelve columnas: sku_ml, ml_item_id, title, link, image_url
    """
    df = pd.read_excel(file, dtype=str)
    cols = {str(c).strip().lower(): c for c in df.columns}

    sku_c = cols.get("sku") or cols.get("codigo") or cols.get("código") or cols.get("sku_ml")
    id_c = cols.get("id") or cols.get("item") or cols.get("item_id") or cols.get("ml_item_id")
    title_c = cols.get("título") or cols.get("titulo") or cols.get("title")
    link_c = cols.get("link") or cols.get("url") or cols.get("enlace")
    image_c = cols.get("imagen") or cols.get("image_url") or cols.get("image") or cols.get("foto")

    if not sku_c:
        raise ValueError("No encuentro columna SKU en el Excel.")
    if not link_c and not id_c:
        raise ValueError("El Excel debe traer Link (url) o Id (item).")

    out = pd.DataFrame()
    out["sku_ml"] = df[sku_c].astype(str).map(normalize_sku)
    out["title"] = df[title_c].astype(str).fillna("").map(lambda x: str(x).strip()) if title_c else ""
    out["link"] = df[link_c].astype(str).fillna("").map(lambda x: str(x).strip()) if link_c else ""

    out["image_url"] = df[image_c].astype(str).fillna("").map(lambda x: str(x).strip()) if image_c else ""

    if id_c:
        out["ml_item_id"] = df[id_c].astype(str).fillna("").map(extract_ml_item_id)
    else:
        out["ml_item_id"] = ""
    need = out["ml_item_id"].eq("")
    if need.any():
        out.loc[need, "ml_item_id"] = out.loc[need, "link"].map(extract_ml_item_id)

    out = out[out["sku_ml"].ne("")].copy()
    out = out.drop_duplicates(subset=["sku_ml"], keep="last")
    return out[["sku_ml", "ml_item_id", "title", "link", "image_url"]]

def upsert_publications_to_db(df_pub: pd.DataFrame) -> tuple[int, int]:
    """Inserta/actualiza tabla sku_publications desde el Excel.

    Retorna (ok_count, missing_id_count).
    """
    if df_pub is None or df_pub.empty:
        return 0, 0

    ok = 0
    noid = 0

    with db_conn(commit=True) as conn:
        c = conn.cursor()

        for _, r in df_pub.iterrows():
            sku = normalize_sku(r.get("sku_ml", ""))
            if not sku:
                continue

            item_id = str(r.get("ml_item_id", "") or "").strip().upper().replace("-", "")
            title = str(r.get("title", "") or "").strip()
            link = str(r.get("link", "") or "").strip()
            image_url = str(r.get("image_url", "") or "").strip()

            if not item_id:
                noid += 1

            c.execute(
                """INSERT INTO sku_publications (sku_ml, ml_item_id, title, link, image_url, updated_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(sku_ml) DO UPDATE SET
                     ml_item_id=excluded.ml_item_id,
                     title=excluded.title,
                     link=excluded.link,
                     image_url=excluded.image_url,
                     updated_at=excluded.updated_at
                """,
                (sku, item_id, title, link, image_url, now_iso())
            )
            ok += 1

    return ok, noid

def get_publication_row(sku: str) -> dict:
    sku = normalize_sku(sku)
    if not sku:
        return {}
    row = db_fetchone("SELECT sku_ml, ml_item_id, title, link, image_url, updated_at FROM sku_publications WHERE sku_ml=?", (sku,))
    if not row:
        return {}
    return {"sku_ml": row[0], "ml_item_id": row[1], "title": row[2], "link": row[3], "image_url": row[4], "updated_at": row[5]}

OG_IMAGE_RE_1 = re.compile(
    r'<meta[^>]+(?:property|name)=["\']og:image(?::secure_url)?["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE
)
OG_IMAGE_RE_2 = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']og:image(?::secure_url)?["\']',
    re.IGNORECASE
)
TWITTER_IMAGE_RE_1 = re.compile(
    r'<meta[^>]+(?:property|name)=["\']twitter:image(?::src)?["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE
)
TWITTER_IMAGE_RE_2 = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']twitter:image(?::src)?["\']',
    re.IGNORECASE
)

def _extract_main_image_from_html(html_text: str) -> str:
    """Extrae una URL de imagen principal desde HTML sin depender de un único formato.

    Prioridad: og:image (incluye secure_url) -> twitter:image.
    """
    if not html_text:
        return ""
    for rx in (OG_IMAGE_RE_1, OG_IMAGE_RE_2, TWITTER_IMAGE_RE_1, TWITTER_IMAGE_RE_2):
        m = rx.search(html_text)
        if m:
            return (m.group(1) or "").strip()
    return ""


@st.cache_data(show_spinner=False, ttl=24*3600)
def publication_main_image_from_html(link: str) -> str:
    """Devuelve 1 URL de imagen principal leyendo metatags desde el HTML.

    Sin API: usa el link público de la publicación (puede fallar si ML bloquea el request).
    """
    url = (link or "").strip()
    if not url:
        return ""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            # Referer ayuda a evitar ciertos bloqueos básicos
            "Referer": "https://www.google.com/",
        }
        r = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
        if r.status_code != 200:
            return ""
        html_text = r.text or ""
        return _extract_main_image_from_html(html_text)
    except Exception:
        return ""



def get_picture_urls_for_sku(sku: str) -> tuple[list[str], str]:
    """Retorna (urls, link_publicacion).

    Nuevo modo estable:
      - Usa image_url desde la tabla sku_publications (cargada desde links_con_imagenes.xlsx).
      - No hace scraping HTML ni requests a Mercado Libre.
    """
    row = get_publication_row(sku)
    if not row:
        return [], ""
    link = (row.get("link") or "").strip()
    image_url = (row.get("image_url") or "").strip()
    if image_url:
        return [image_url], link
    return [], link

def load_cortes_set(path: str = CORTES_FILE) -> set:
    """Carga listado de SKUs que requieren corte manual desde Excel (defensivo)."""
    # Cache en session_state para evitar leer el Excel en cada rerun
    try:
        ss = st.session_state
        if ss.get("_cortes_cache_path") == path and ss.get("_cortes_cache_skus") is not None:
            return set(ss.get("_cortes_cache_skus") or [])
    except Exception:
        pass

    try:
        if not path or not os.path.exists(path):
            return set()
        df = pd.read_excel(path, dtype=str)
    except Exception:
        return set()

    try:
        cols = {str(c).strip().upper(): c for c in df.columns}
        col_sku = cols.get("SKU") or cols.get("SKUS") or cols.get("CODIGO") or cols.get("CÓDIGO")
        if not col_sku:
            col_sku = df.columns[0]

        skus = set()
        for v in df[col_sku].fillna("").tolist():
            s = normalize_sku(v)
            if s:
                skus.add(s)

        try:
            st.session_state["_cortes_cache_path"] = path
            st.session_state["_cortes_cache_skus"] = list(skus)
        except Exception:
            pass

        return skus
    except Exception:
        return set()
# =========================
# PARSER PDF MANIFIESTO
# =========================

def parse_manifest_pdf(uploaded_file) -> pd.DataFrame:
    """
    Parser robusto para Manifiesto PDF (etiquetas).

    Cubre casos reales de ML donde el PDF puede traer, en cualquier orden:
      - "Venta: <id> SKU:<sku>" en el mismo renglón
      - "Pack ID: ... SKU:<sku>" en un renglón y luego "Venta: <id> Cantidad: <n>" en el siguiente
      - "SKU:<sku>" en un renglón y "Cantidad:<n>" en el siguiente
      - Varias ocurrencias de SKU/Cantidad dentro de un mismo renglón

    Regla: cada vez que se detecta una Cantidad, si existe un SKU "vigente" y una Venta vigente,
    se crea un registro (línea) para esa venta+sku.
    """
    if not HAS_PDF_LIB:
        raise RuntimeError("Falta pdfplumber. Agrega 'pdfplumber' a requirements.txt")

    records: list[dict] = []

    re_venta = re.compile(r"\bVenta\s*[:#]?\s*([0-9]+)\b", re.IGNORECASE)
    re_sku = re.compile(r"\bSKU\s*[:#]?\s*([0-9A-Za-z.\-]+)\b", re.IGNORECASE)
    re_qty = re.compile(r"\bCantidad\s*[:#]?\s*([0-9]+)\b", re.IGNORECASE)

    def _is_noise_line(s: str) -> bool:
        low = (s or "").strip().lower()
        if not low:
            return True
        bad = [
            "código carrier", "codigo carrier", "firma carrier",
            "fecha y hora", "despacha tus productos", "identifi",
        ]
        if any(b in low for b in bad):
            return True
        if re.fullmatch(r"[0-9 .:/\-]+", low):
            return True
        return False

    def _maybe_buyer(line: str) -> str:
        # Quitamos cosas típicas que se pegan al nombre (ej: "Diámetro de la cupla: ...")
        # sin ser demasiado agresivos.
        cut_tokens = ["diámetro", "diametro", "color:", "acabado:", "pack id", "sku", "cantidad", "venta:"]
        low = line.lower()
        for tok in cut_tokens:
            idx = low.find(tok)
            if idx > 0:
                return line[:idx].strip()
        return line.strip()

    with pdfplumber.open(uploaded_file) as pdf:
        for page in pdf.pages:
            text = (page.extract_text() or "").replace("\r", "\n")
            lines = [ln.strip() for ln in text.splitlines() if ln and str(ln).strip()]

            current_order: str | None = None
            current_buyer: str = ""

            # SKU "vigente" para el próximo "Cantidad"
            sku_current: str | None = None

            # SKU visto antes de que aparezca la Venta (caso: "Pack ID ... SKU:xxxx" y luego "Venta ... Cantidad ...")
            pending_sku_before_order: str | None = None

            for line in lines:
                if _is_noise_line(line):
                    continue

                # Capturar Venta (no reseteamos SKU aquí; hay PDFs donde el SKU viene en la línea anterior)
                mv = re_venta.search(line)
                if mv:
                    current_order = mv.group(1).strip()
                    current_buyer = ""
                    # Si hay un SKU pendiente (visto antes de la venta), lo activamos
                    if pending_sku_before_order and not sku_current:
                        sku_current = pending_sku_before_order
                        pending_sku_before_order = None

                # Buyer: primera línea razonable después de "Venta:" que no sea metadata
                if current_order and not current_buyer:
                    low = line.lower()
                    if (not _is_noise_line(line)
                        and "venta" not in low
                        and "sku" not in low
                        and "cantidad" not in low
                        and ":" not in line  # evita "Color:" etc
                        and len(line) <= 120):
                        cand = _maybe_buyer(line)
                        if cand and len(cand) >= 3:
                            current_buyer = cand

                # Tokenizar SKU y Cantidad en orden de aparición en el renglón
                tokens = []
                for ms in re_sku.finditer(line):
                    tokens.append((ms.start(), "SKU", normalize_sku(ms.group(1))))
                for mq in re_qty.finditer(line):
                    try:
                        qv = int(mq.group(1))
                    except Exception:
                        qv = 0
                    tokens.append((mq.start(), "QTY", qv))
                tokens.sort(key=lambda x: x[0])

                for _, kind, val in tokens:
                    if kind == "SKU":
                        if current_order:
                            sku_current = val
                        else:
                            pending_sku_before_order = val
                    elif kind == "QTY":
                        qty = int(val) if val is not None else 0
                        if current_order and sku_current and qty > 0:
                            records.append(
                                {
                                    "ml_order_id": str(current_order).strip(),
                                    "buyer": str(current_buyer or "").strip(),
                                    "sku_ml": str(sku_current).strip(),
                                    "title_ml": "",
                                    "qty": qty,
                                }
                            )
                            # Importante: NO limpiamos sku_current aquí, porque puede venir otra Cantidad asociada
                            # al mismo SKU en el mismo bloque (raro, pero seguro).

    return pd.DataFrame(records, columns=["ml_order_id", "buyer", "sku_ml", "title_ml", "qty"])



# =========================
# AUTO-CARGA PUBLICACIONES (desde repo)
# =========================
def publications_bootstrap(path: str = PUBLICATIONS_FILE):
    """Carga/actualiza automáticamente los links de publicaciones desde el repo.
    - Evita recargar en cada rerun usando mtime.
    - No requiere upload manual desde el panel administrador.
    """
    ss = st.session_state
    cache_key = "_pub_links_mtime"

    if not path or not os.path.exists(path):
        ss["_pub_links_status"] = ("missing", str(path or ""))
        return 0, 0, False

    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = None

    if ss.get(cache_key) == mtime and ss.get("_pub_links_loaded", False):
        return int(ss.get("_pub_links_ok", 0) or 0), int(ss.get("_pub_links_noid", 0) or 0), False

    try:
        dfp = import_publication_links_excel(path)
        ok_n, noid_n = upsert_publications_to_db(dfp)
        ss[cache_key] = mtime
        ss["_pub_links_loaded"] = True
        ss["_pub_links_ok"] = int(ok_n or 0)
        ss["_pub_links_noid"] = int(noid_n or 0)
        ss["_pub_links_status"] = ("ok", str(path))
        return int(ok_n or 0), int(noid_n or 0), True
    except Exception as e:
        ss["_pub_links_status"] = ("err", str(e))
        return 0, 0, False



# =========================
# IMPORTAR VENTAS (FLEX)
# =========================
def import_sales_excel(file) -> pd.DataFrame:
    """Importa reporte de ventas ML.

    Importante: en los reportes de ML, los envíos con varios productos vienen con una fila
    de cabecera 'Paquete de X productos' (sin SKU / sin unidades) y luego X filas con los ítems.
    Para que el KPI 'Ventas' refleje lo que tú ves por colores (paquetes/envíos), agrupamos esos
    ítems bajo el ID de la fila cabecera.
    """
    df = pd.read_excel(file, header=[4, 5])
    df.columns = [" | ".join([str(x) for x in col if str(x) != "nan"]) for col in df.columns]

    COLUMN_ORDER_ID = "Ventas | # de venta"
    COLUMN_STATUS = "Ventas | Estado"
    COLUMN_QTY = "Ventas | Unidades"
    COLUMN_SKU = "Publicaciones | SKU"
    COLUMN_TITLE = "Publicaciones | Título de la publicación"
    COLUMN_BUYER = "Compradores | Comprador"

    required = [COLUMN_ORDER_ID, COLUMN_STATUS, COLUMN_QTY, COLUMN_SKU, COLUMN_TITLE, COLUMN_BUYER]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas: {missing}")

    # Normalizamos a texto para trabajar seguro
    work = df[required].copy()
    work.columns = ["ml_order_id", "status", "qty", "sku_ml", "title_ml", "buyer"]

    # Helpers
    def _clean_str(x) -> str:
        if pd.isna(x):
            return ""
        return str(x).strip()

    records = []
    current_pkg_id = None
    current_pkg_buyer = ""
    remaining_items = 0

    pkg_re = re.compile(r"^Paquete\s+de\s+(\d+)\s+productos?$", re.IGNORECASE)

    for _, r in work.iterrows():
        status = _clean_str(r.get("status"))
        ml_id = _clean_str(r.get("ml_order_id"))
        buyer = _clean_str(r.get("buyer"))
        sku = _clean_str(r.get("sku_ml"))
        title = _clean_str(r.get("title_ml"))
        qty = pd.to_numeric(r.get("qty"), errors="coerce")

        # Detecta fila cabecera del paquete (no trae SKU/qty)
        m = pkg_re.match(status)
        if m:
            try:
                remaining_items = int(m.group(1))
            except Exception:
                remaining_items = 0
            current_pkg_id = ml_id if ml_id else None
            current_pkg_buyer = buyer
            continue

        # Filas sin SKU/qty -> se ignoran
        if not sku or pd.isna(qty):
            continue

        qty_int = int(qty) if not pd.isna(qty) else 0
        if qty_int <= 0:
            continue

        sku_norm = normalize_sku(sku)

        # Si estamos dentro de un paquete, agrupamos bajo el ID del paquete
        if current_pkg_id and remaining_items > 0:
            records.append(
                {
                    "ml_order_id": current_pkg_id,
                    "buyer": current_pkg_buyer or buyer,
                    "sku_ml": sku_norm,
                    "title_ml": title,
                    "qty": qty_int,
                }
            )
            remaining_items -= 1
            if remaining_items <= 0:
                current_pkg_id = None
                current_pkg_buyer = ""
            continue

        # Venta normal (1 producto)
        records.append(
            {
                "ml_order_id": ml_id,
                "buyer": buyer,
                "sku_ml": sku_norm,
                "title_ml": title,
                "qty": qty_int,
            }
        )

    out = pd.DataFrame(records, columns=["ml_order_id", "buyer", "sku_ml", "title_ml", "qty"])
    return out
def _next_picker_numbers(existing_names: list[str], qty: int) -> list[int]:
    nums = []
    for pname in existing_names or []:
        m = re.fullmatch(r"P(\d+)", str(pname or "").strip().upper())
        if m:
            nums.append(int(m.group(1)))
    start_n = (max(nums) + 1) if nums else 1
    return list(range(start_n, start_n + int(qty)))


def _get_current_picker_names() -> list[str]:
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("SELECT name FROM pickers ORDER BY id")
        rows = [str(r[0]) for r in c.fetchall()]
    except Exception:
        rows = []
    conn.close()
    return rows


def _build_picking_batch_label(source_label: str | None, model: str, picker_names: list[str]) -> str:
    src = str(source_label or "Manifiesto").strip() or "Manifiesto"
    picks = ", ".join(picker_names) if picker_names else "Sin pickeadores"
    model_txt = "Por ventas" if str(model or "VENTAS").upper().strip() == "VENTAS" else "Por SKU"
    return f"{src} · {model_txt} · {picks}"


def _get_picking_batches_summary() -> list[dict]:
    conn = get_conn()
    c = conn.cursor()
    rows = []
    try:
        c.execute("""
            SELECT
                COALESCE(po.batch_key, po.ot_code, 'SIN_LOTE') AS batch_key,
                COALESCE(MAX(NULLIF(po.batch_label,'')), MAX(po.ot_code), 'Lote') AS batch_label,
                MIN(po.created_at) AS created_at,
                GROUP_CONCAT(DISTINCT pk.name) AS pickers,
                COUNT(DISTINCT po.id) AS ots,
                COUNT(DISTINCT oo.order_id) AS orders_count,
                SUM(CASE WHEN pt.status='PENDING' THEN 1 ELSE 0 END) AS pending_tasks,
                SUM(CASE WHEN pt.status IN ('DONE','INCIDENCE') THEN 1 ELSE 0 END) AS done_tasks,
                COUNT(DISTINCT CASE WHEN po.status='OPEN' THEN po.id END) AS open_ots
            FROM picking_ots po
            JOIN pickers pk ON pk.id = po.picker_id
            LEFT JOIN ot_orders oo ON oo.ot_id = po.id
            LEFT JOIN picking_tasks pt ON pt.ot_id = po.id
            GROUP BY COALESCE(po.batch_key, po.ot_code, 'SIN_LOTE')
            ORDER BY MIN(po.created_at) DESC, batch_key DESC
        """)
        for batch_key, batch_label, created_at, pickers, ots, orders_count, pending_tasks, done_tasks, open_ots in c.fetchall():
            pending_tasks = int(pending_tasks or 0)
            done_tasks = int(done_tasks or 0)
            total_tasks = pending_tasks + done_tasks
            pct = 0.0 if total_tasks == 0 else round((done_tasks * 100.0) / total_tasks, 1)
            rows.append({
                "batch_key": str(batch_key),
                "batch_label": str(batch_label or "Lote"),
                "created_at": created_at,
                "pickers": str(pickers or ""),
                "ots": int(ots or 0),
                "orders_count": int(orders_count or 0),
                "pending_tasks": pending_tasks,
                "done_tasks": done_tasks,
                "total_tasks": total_tasks,
                "open_ots": int(open_ots or 0),
                "progress_pct": pct,
            })
    except Exception:
        rows = []
    conn.close()
    return rows


def save_orders_and_build_ots(
    sales_df: pd.DataFrame,
    inv_map_sku: dict,
    num_pickers: int,
    model: str = "VENTAS",
    familia_map_sku: dict | None = None,
):
    """
    Genera la tanda de picking.

    model:
      - "VENTAS" (actual): reparte ventas por OT y crea tareas por SKU dentro de esas ventas.
      - "SKU" (nuevo): agrupa SKUs por Familia (desde maestro) y asigna familias a OTs (batch picking).
        Nota: para evitar conflictos con pantallas antiguas, igual se mantiene la asignación de ventas->OT
        en ot_orders (registro de ventas por OT), pero las tareas de picking se construyen por familia/SKU.
    """
    model = (model or "VENTAS").upper().strip()
    if model not in ("VENTAS", "SKU"):
        model = "VENTAS"

    familia_map_sku = familia_map_sku or {}

    conn = get_conn()
    c = conn.cursor()

    # SKUs que se van a CORTES (no aparecen en picking)
    cortes_set = load_cortes_set()

    # Reset corrida (no borra histórico; eso lo hace admin reset total)
    c.execute("DELETE FROM picking_tasks;")
    c.execute("DELETE FROM picking_incidences;")
    c.execute("DELETE FROM cortes_tasks;")
    c.execute("DELETE FROM ot_orders;")
    c.execute("DELETE FROM picking_ots;")
    c.execute("DELETE FROM pickers;")

    order_id_by_ml = {}
    for ml_order_id, g in sales_df.groupby("ml_order_id"):
        ml_order_id = str(ml_order_id).strip()
        buyer = str(g["buyer"].iloc[0]) if "buyer" in g.columns else ""
        created = now_iso()

        c.execute("SELECT id FROM orders WHERE ml_order_id = ?", (ml_order_id,))
        row = c.fetchone()
        if row:
            order_id = row[0]
            c.execute("UPDATE orders SET buyer=?, created_at=? WHERE id=?", (buyer, created, order_id))
            c.execute("DELETE FROM order_items WHERE order_id=?", (order_id,))
        else:
            c.execute("INSERT INTO orders (ml_order_id, buyer, created_at) VALUES (?,?,?)", (ml_order_id, buyer, created))
            order_id = c.lastrowid

        order_id_by_ml[ml_order_id] = order_id

        for _, r in g.iterrows():
            sku = normalize_sku(r["sku_ml"])
            qty = int(r["qty"])
            title_ml = str(r.get("title_ml", "") or "").strip()
            title_tec = inv_map_sku.get(sku, "")
            title_eff = title_tec if title_tec else title_ml

            c.execute(
                "INSERT INTO order_items (order_id, sku_ml, title_ml, title_tec, qty) VALUES (?,?,?,?,?)",
                (order_id, sku, title_eff, title_tec, qty)
            )

    # pickers
    picker_ids = []
    for i in range(int(num_pickers)):
        name = f"P{i+1}"
        c.execute("INSERT INTO pickers (name) VALUES (?)", (name,))
        picker_ids.append(c.lastrowid)

    # ots
    ot_ids = []
    for pid in picker_ids:
        c.execute(
            "INSERT INTO picking_ots (ot_code, picker_id, status, created_at, closed_at, model) VALUES (?,?,?,?,?,?)",
            ("", pid, "OPEN", now_iso(), None, model)
        )
        ot_id = c.lastrowid
        ot_code = f"OT{ot_id:06d}"
        c.execute("UPDATE picking_ots SET ot_code=? WHERE id=?", (ot_code, ot_id))
        ot_ids.append(ot_id)

    # Mantener asignación de ventas -> OT (para compatibilidad con módulos existentes)
    unique_orders = sales_df[["ml_order_id"]].drop_duplicates().reset_index(drop=True)
    assignments = {}
    for idx, row in unique_orders.iterrows():
        ot_id = ot_ids[idx % len(ot_ids)]
        assignments[str(row["ml_order_id"]).strip()] = ot_id

    for idx, (ml_order_id, ot_id) in enumerate(assignments.items()):
        order_id = order_id_by_ml[ml_order_id]
        mesa = (idx % NUM_MESAS) + 1
        c.execute("INSERT INTO ot_orders (ot_id, order_id) VALUES (?,?)", (ot_id, order_id))

    if model == "VENTAS":
        # === Modelo actual (sin cambios) ===
        for ot_id in ot_ids:
            c.execute("""
                SELECT oi.sku_ml,
                       COALESCE(NULLIF(oi.title_tec,''), oi.title_ml) AS title,
                       MAX(COALESCE(oi.title_tec,'')) AS title_tec_any,
                       SUM(oi.qty) as total
                FROM ot_orders oo
                JOIN order_items oi ON oi.order_id = oo.order_id
                WHERE oo.ot_id = ?
                GROUP BY oi.sku_ml, title
                ORDER BY CAST(oi.sku_ml AS INTEGER), oi.sku_ml
            """, (ot_id,))
            rows = c.fetchall()
            for sku, title, title_tec_any, total in rows:
                if sku in cortes_set:
                    c.execute(
                        "INSERT INTO cortes_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, created_at) VALUES (?,?,?,?,?,?)",
                        (ot_id, sku, title, title_tec_any, int(total), now_iso())
                    )
                else:
                    c.execute("""
                    INSERT INTO picking_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, qty_picked, status, decided_at, confirm_mode, family)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (ot_id, sku, title, title_tec_any, int(total), 0, "PENDING", None, None, None))

        conn.commit()
        conn.close()
        return

    # === Modelo nuevo: por SKU + Familia ===
    # 1) Preparar totales por SKU y familia
    dfw = sales_df.copy()
    dfw["sku_ml"] = dfw["sku_ml"].map(normalize_sku)
    dfw = dfw[dfw["sku_ml"].ne("")].copy()

    # título ML preferido por SKU (si no hay título técnico)
    title_ml_by_sku = {}
    if "title_ml" in dfw.columns:
        for sku, g in dfw.groupby("sku_ml"):
            t = ""
            for v in g["title_ml"].tolist():
                v = str(v or "").strip()
                if v and v.lower() != "nan":
                    t = v
                    break
            title_ml_by_sku[sku] = t

        # Prefijo 6 -> Familia para inferir SKUs sin familia
        _fam_prefix6 = {}
        try:
            fam_counts = {}
            for k, v in (familia_map_sku or {}).items():
                base_sku = normalize_sku(k)
                fam = str(v or "").strip()
                if not base_sku or len(base_sku) < 6 or not fam or fam.lower() == "nan":
                    continue
                pref6 = base_sku[:6]
                fam_counts.setdefault(pref6, {})
                fam_counts[pref6][fam] = fam_counts[pref6].get(fam, 0) + 1

            for pref6, fam_map in fam_counts.items():
                _fam_prefix6[pref6] = sorted(
                    fam_map.items(),
                    key=lambda kv: (-kv[1], kv[0])
                )[0][0]
        except Exception:
            _fam_prefix6 = {}

    def _fam_for_sku(sku: str) -> str:
        # 1) Familia directa en maestro
        f = str(familia_map_sku.get(sku, "") or "").strip()
        if f and f.lower() != "nan":
            return f

        # 2) Fallback: usar los primeros 6 dígitos del SKU
        ssku = normalize_sku(sku)
        if not ssku:
            return "Sin Familia"

        fam6 = _fam_prefix6.get(ssku[:6], "")
        if fam6:
            return fam6

        return "Sin Familia"

    dfw["family"] = dfw["sku_ml"].map(_fam_for_sku)

    # totales por familia+sku
    grp = dfw.groupby(["family", "sku_ml"], as_index=False)["qty"].sum()
    grp["qty"] = grp["qty"].astype(int)

    # 2) asignar familias a OTs (greedy balance por unidades)
    fam_weights = grp.groupby("family")["qty"].sum().to_dict()
    fam_list = sorted(fam_weights.items(), key=lambda x: x[1], reverse=True)

    ot_load = {ot_id: 0 for ot_id in ot_ids}
    ot_fams = {ot_id: [] for ot_id in ot_ids}

    for fam, w in fam_list:
        # ot menos cargada
        target_ot = min(ot_load.items(), key=lambda kv: kv[1])[0]
        ot_fams[target_ot].append(fam)
        ot_load[target_ot] += int(w or 0)

    # 3) Insertar tareas por OT
    for ot_id in ot_ids:
        fams = ot_fams.get(ot_id, [])
        if not fams:
            continue
        sub = grp[grp["family"].isin(fams)].copy()
        # orden: familia, sku
        try:
            sub["sku_int"] = sub["sku_ml"].map(lambda x: int(x) if str(x).isdigit() else 10**18)
            sub = sub.sort_values(["family", "sku_int", "sku_ml"], ascending=[True, True, True])
        except Exception:
            sub = sub.sort_values(["family", "sku_ml"], ascending=[True, True])

        for _, r in sub.iterrows():
            fam = str(r["family"])
            sku = str(r["sku_ml"])
            total = int(r["qty"] or 0)
            title_tec = inv_map_sku.get(sku, "") or ""
            title_ml = title_ml_by_sku.get(sku, "") or ""
            title_eff = title_tec.strip() if title_tec.strip() else title_ml.strip()

            if sku in cortes_set:
                c.execute(
                    "INSERT INTO cortes_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, created_at) VALUES (?,?,?,?,?,?)",
                    (ot_id, sku, title_eff, title_tec, total, now_iso())
                )
            else:
                c.execute("""
                    INSERT INTO picking_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, qty_picked, status, decided_at, confirm_mode, family)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (ot_id, sku, title_eff, title_tec, total, 0, "PENDING", None, None, fam))

    conn.commit()
    conn.close()


def append_orders_and_build_ots(
    sales_df: pd.DataFrame,
    inv_map_sku: dict,
    num_pickers: int,
    model: str = "VENTAS",
    familia_map_sku: dict | None = None,
    source_label: str | None = None,
):
    """Agrega una nueva carga de picking sin borrar la tanda actual."""
    model = (model or "VENTAS").upper().strip()
    if model not in ("VENTAS", "SKU"):
        model = "VENTAS"

    familia_map_sku = familia_map_sku or {}
    sales_df = sales_df.copy()
    if sales_df.empty:
        return {"created": False, "reason": "empty", "new_orders": 0, "picker_names": []}

    sales_df["ml_order_id"] = sales_df["ml_order_id"].astype(str).str.strip()

    conn = get_conn()
    c = conn.cursor()

    try:
        c.execute("""
            SELECT DISTINCT TRIM(o.ml_order_id)
            FROM ot_orders oo
            JOIN orders o ON o.id = oo.order_id
        """)
        existing_loaded = {str(r[0]).strip() for r in c.fetchall() if str(r[0]).strip()}
    except Exception:
        existing_loaded = set()

    new_order_ids = [oid for oid in sales_df["ml_order_id"].drop_duplicates().tolist() if str(oid).strip() and str(oid).strip() not in existing_loaded]
    if not new_order_ids:
        conn.close()
        return {"created": False, "reason": "duplicate", "new_orders": 0, "picker_names": []}

    sales_df = sales_df[sales_df["ml_order_id"].isin(new_order_ids)].copy()

    existing_picker_names = _get_current_picker_names()
    picker_numbers = _next_picker_numbers(existing_picker_names, int(num_pickers))
    picker_names = [f"P{n}" for n in picker_numbers]
    batch_key = f"PK-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))}"
    batch_label = _build_picking_batch_label(source_label, model, picker_names)

    cortes_set = load_cortes_set()
    order_id_by_ml = {}
    for ml_order_id, g in sales_df.groupby("ml_order_id"):
        ml_order_id = str(ml_order_id).strip()
        buyer = str(g["buyer"].iloc[0]) if "buyer" in g.columns else ""
        created = now_iso()

        c.execute("SELECT id FROM orders WHERE ml_order_id = ?", (ml_order_id,))
        row = c.fetchone()
        if row:
            order_id = row[0]
            c.execute("UPDATE orders SET buyer=?, created_at=? WHERE id=?", (buyer, created, order_id))
            c.execute("DELETE FROM order_items WHERE order_id=?", (order_id,))
        else:
            c.execute("INSERT INTO orders (ml_order_id, buyer, created_at) VALUES (?,?,?)", (ml_order_id, buyer, created))
            order_id = c.lastrowid

        order_id_by_ml[ml_order_id] = order_id

        for _, r in g.iterrows():
            sku = normalize_sku(r["sku_ml"])
            qty = int(r["qty"])
            title_ml = str(r.get("title_ml", "") or "").strip()
            title_tec = inv_map_sku.get(sku, "")
            title_eff = title_tec if title_tec else title_ml
            c.execute(
                "INSERT INTO order_items (order_id, sku_ml, title_ml, title_tec, qty) VALUES (?,?,?,?,?)",
                (order_id, sku, title_eff, title_tec, qty)
            )

    picker_ids = []
    for name in picker_names:
        c.execute("INSERT OR IGNORE INTO pickers (name) VALUES (?)", (name,))
        c.execute("SELECT id FROM pickers WHERE name=?", (name,))
        row = c.fetchone()
        if row:
            picker_ids.append(int(row[0]))

    ot_ids = []
    for pid in picker_ids:
        c.execute(
            "INSERT INTO picking_ots (ot_code, picker_id, status, created_at, closed_at, model, batch_key, batch_label) VALUES (?,?,?,?,?,?,?,?)",
            ("", pid, "OPEN", now_iso(), None, model, batch_key, batch_label)
        )
        ot_id = c.lastrowid
        ot_code = f"OT{ot_id:06d}"
        c.execute("UPDATE picking_ots SET ot_code=? WHERE id=?", (ot_code, ot_id))
        ot_ids.append(ot_id)

    unique_orders = sales_df[["ml_order_id"]].drop_duplicates().reset_index(drop=True)
    assignments = {}
    for idx, row in unique_orders.iterrows():
        ot_id = ot_ids[idx % len(ot_ids)]
        assignments[str(row["ml_order_id"]).strip()] = ot_id

    for ml_order_id, ot_id in assignments.items():
        order_id = order_id_by_ml[ml_order_id]
        c.execute("INSERT INTO ot_orders (ot_id, order_id) VALUES (?,?)", (ot_id, order_id))

    if model == "VENTAS":
        for ot_id in ot_ids:
            c.execute("""
                SELECT oi.sku_ml,
                       COALESCE(NULLIF(oi.title_tec,''), oi.title_ml) AS title,
                       MAX(COALESCE(oi.title_tec,'')) AS title_tec_any,
                       SUM(oi.qty) as total
                FROM ot_orders oo
                JOIN order_items oi ON oi.order_id = oo.order_id
                WHERE oo.ot_id = ?
                GROUP BY oi.sku_ml, title
                ORDER BY CAST(oi.sku_ml AS INTEGER), oi.sku_ml
            """, (ot_id,))
            rows = c.fetchall()
            for sku, title, title_tec_any, total in rows:
                if sku in cortes_set:
                    c.execute(
                        "INSERT INTO cortes_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, created_at) VALUES (?,?,?,?,?,?)",
                        (ot_id, sku, title, title_tec_any, int(total), now_iso())
                    )
                else:
                    c.execute("""
                    INSERT INTO picking_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, qty_picked, status, decided_at, confirm_mode, family)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (ot_id, sku, title, title_tec_any, int(total), 0, "PENDING", None, None, None))

        conn.commit()
        conn.close()
        return {"created": True, "reason": "ok", "new_orders": len(new_order_ids), "picker_names": picker_names, "batch_label": batch_label}

    dfw = sales_df.copy()
    dfw["sku_ml"] = dfw["sku_ml"].map(normalize_sku)
    dfw = dfw[dfw["sku_ml"].ne("")].copy()

    title_ml_by_sku = {}
    if "title_ml" in dfw.columns:
        for sku, g in dfw.groupby("sku_ml"):
            t = ""
            for v in g["title_ml"].tolist():
                v = str(v or "").strip()
                if v and v.lower() != "nan":
                    t = v
                    break
            title_ml_by_sku[sku] = t

    _fam_prefix6 = {}
    try:
        fam_counts = {}
        for k, v in (familia_map_sku or {}).items():
            base_sku = normalize_sku(k)
            fam = str(v or "").strip()
            if not base_sku or len(base_sku) < 6 or not fam or fam.lower() == "nan":
                continue
            pref6 = base_sku[:6]
            fam_counts.setdefault(pref6, {})
            fam_counts[pref6][fam] = fam_counts[pref6].get(fam, 0) + 1
        for pref6, fam_map in fam_counts.items():
            _fam_prefix6[pref6] = sorted(fam_map.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    except Exception:
        _fam_prefix6 = {}

    def _fam_for_sku(sku: str) -> str:
        f = str(familia_map_sku.get(sku, "") or "").strip()
        if f and f.lower() != "nan":
            return f
        ssku = normalize_sku(sku)
        if not ssku:
            return "Sin Familia"
        fam6 = _fam_prefix6.get(ssku[:6], "")
        if fam6:
            return fam6
        return "Sin Familia"

    dfw["family"] = dfw["sku_ml"].map(_fam_for_sku)
    grp = dfw.groupby(["family", "sku_ml"], as_index=False)["qty"].sum()
    grp["qty"] = grp["qty"].astype(int)

    fam_weights = grp.groupby("family")["qty"].sum().to_dict()
    fam_list = sorted(fam_weights.items(), key=lambda x: x[1], reverse=True)
    ot_load = {ot_id: 0 for ot_id in ot_ids}
    ot_fams = {ot_id: [] for ot_id in ot_ids}
    for fam, w in fam_list:
        target_ot = min(ot_load.items(), key=lambda kv: kv[1])[0]
        ot_fams[target_ot].append(fam)
        ot_load[target_ot] += int(w or 0)

    for ot_id in ot_ids:
        fams = ot_fams.get(ot_id, [])
        if not fams:
            continue
        sub = grp[grp["family"].isin(fams)].copy()
        try:
            sub["sku_int"] = sub["sku_ml"].map(lambda x: int(x) if str(x).isdigit() else 10**18)
            sub = sub.sort_values(["family", "sku_int", "sku_ml"], ascending=[True, True, True])
        except Exception:
            sub = sub.sort_values(["family", "sku_ml"], ascending=[True, True])

        for _, r in sub.iterrows():
            fam = str(r["family"])
            sku = str(r["sku_ml"])
            total = int(r["qty"] or 0)
            title_tec = inv_map_sku.get(sku, "") or ""
            title_ml = title_ml_by_sku.get(sku, "") or ""
            title_eff = title_tec.strip() if title_tec.strip() else title_ml.strip()
            if sku in cortes_set:
                c.execute(
                    "INSERT INTO cortes_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, created_at) VALUES (?,?,?,?,?,?)",
                    (ot_id, sku, title_eff, title_tec, total, now_iso())
                )
            else:
                c.execute("""
                    INSERT INTO picking_tasks (ot_id, sku_ml, title_ml, title_tec, qty_total, qty_picked, status, decided_at, confirm_mode, family)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (ot_id, sku, title_eff, title_tec, total, 0, "PENDING", None, None, fam))

    conn.commit()
    conn.close()
    return {"created": True, "reason": "ok", "new_orders": len(new_order_ids), "picker_names": picker_names, "batch_label": batch_label}



# =========================
# UI: LOBBY APP (MODO)
# =========================
def page_app_lobby():
    st.markdown("## Ferretería Aurora – WMS")
    st.caption("Selecciona el flujo de trabajo")

    st.markdown(
        """
        <style>
        .lobbybtn button {
            width: 100% !important;
            padding: 22px 14px !important;
            font-size: 22px !important;
            font-weight: 900 !important;
            border-radius: 18px !important;
        }
        .lobbywrap { max-width: 1100px; margin: 0 auto; }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown('<div class="lobbywrap">', unsafe_allow_html=True)
    colA, colB, colC = st.columns(3)

    with colA:
        st.markdown('<div class="lobbybtn">', unsafe_allow_html=True)
        if st.button("📦 Picking pedidos Flex y Colecta", key="mode_flex_pick"):
            st.session_state.app_mode = "FLEX_PICK"
            st.session_state.pop("selected_picker", None)
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        st.caption("Picking por OT, incidencias, admin, etc.")

    with colB:
        st.markdown('<div class="lobbybtn">', unsafe_allow_html=True)
        if st.button("🧾 Sorting pedidos Flex y Colecta", key="mode_sorting"):
            st.session_state.app_mode = "SORTING"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        st.caption("Camarero por mesa/página (1 página = 1 mesa).")

    with colC:
        st.markdown('<div class="lobbybtn">', unsafe_allow_html=True)
        if st.button("🏷️ Preparación productos Full", key="mode_full"):
            st.session_state.app_mode = "FULL"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        st.caption("Control de acopio Full (escaneo + chequeo vs Excel).")

    st.markdown("</div>", unsafe_allow_html=True)

    # Segunda fila (Sorting -> Embalador -> Despacho)
    row1, row2, row3 = st.columns(3)
    with row1:
        st.markdown('<div class="lobbybtn">', unsafe_allow_html=True)
        if st.button("🎁 Embalador (desde Sorting)", key="mode_packing"):
            st.session_state.app_mode = "PACKING"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        st.caption("Marca ventas embaladas escaneando etiqueta en orden del manifiesto.")
    with row2:
        st.markdown('<div class="lobbybtn">', unsafe_allow_html=True)
        if st.button("🚚 Despacho (desde Embalador)", key="mode_dispatch"):
            st.session_state.app_mode = "DISPATCH"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        st.caption("Marca ventas despachadas (requiere embalaje previo).")
    with row3:
        st.caption("")

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="lobbybtn">', unsafe_allow_html=True)
    if st.button("🧮 Contador de paquetes", key="mode_pkg_counter"):
        st.session_state.app_mode = "PKG_COUNT"
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    st.caption("Escanea etiquetas y cuenta paquetes; evita duplicados.")
def page_import(inv_map_sku: dict, familia_map_sku: dict):
    st.header("Importar ventas")

    flash = st.session_state.get("picking_import_flash")
    if flash:
        if isinstance(flash, dict):
            level = str(flash.get("level") or "success").lower().strip()
            msg = str(flash.get("message") or "").strip()
        else:
            level = "success"
            msg = str(flash).strip()
        if msg:
            if level == "warning":
                st.warning(msg)
            elif level == "error":
                st.error(msg)
            else:
                st.success(msg)
        st.session_state.pop("picking_import_flash", None)

    batches = _get_picking_batches_summary()
    if batches:
        st.subheader("Tandas de picking activas")
        cols = st.columns(min(3, len(batches)))
        for i, batch in enumerate(batches):
            with cols[i % len(cols)]:
                with st.container(border=True):
                    st.markdown(f"**{batch['batch_label']}**")
                    st.caption(f"Creada: {to_chile_display(batch['created_at'])}")
                    total = int(batch.get('total_tasks', 0) or 0)
                    done = int(batch.get('done_tasks', 0) or 0)
                    pct = float(batch.get('progress_pct', 0.0) or 0.0)
                    st.progress(min(max(pct / 100.0, 0.0), 1.0))
                    st.caption(f"{done}/{total} tareas resueltas · {pct:.1f}%")
                    st.write(f"**Pickeadores:** {batch.get('pickers') or '-'}")
                    a, b = st.columns(2)
                    a.metric("Ventas", int(batch.get('orders_count', 0) or 0))
                    b.metric("OTs abiertas", int(batch.get('open_ots', 0) or 0))
        st.divider()

    origen = st.radio("Origen", ["Excel Mercado Libre", "Manifiesto PDF (etiquetas)"], horizontal=True)
    num_pickers = st.number_input("Cantidad de pickeadores nuevos para esta carga", min_value=1, max_value=20, value=3 if batches else 5, step=1)
    model_pick = st.radio("Elegir modelo", ["Por ventas", "Por sku"], horizontal=True)

    next_names = [f"P{n}" for n in _next_picker_numbers(_get_current_picker_names(), int(num_pickers))]
    st.info(f"Esta carga creará: **{', '.join(next_names)}**")

    source_label = ""
    if origen == "Excel Mercado Libre":
        file = st.file_uploader("Ventas ML (xlsx)", type=["xlsx"], key="ml_excel")
        if not file:
            st.info("Sube el Excel de ventas.")
            return
        source_label = getattr(file, "name", "Excel ML")
        sales_df = import_sales_excel(file)
    else:
        pdf_file = st.file_uploader("Manifiesto PDF", type=["pdf"], key="ml_pdf")
        if not pdf_file:
            st.info("Sube el PDF.")
            return
        source_label = getattr(pdf_file, "name", "Manifiesto PDF")
        sales_df = parse_manifest_pdf(pdf_file)

    st.subheader("Vista previa")
    st.dataframe(sales_df.head(30))

    action_label = "Agregar carga y generar nuevas OTs" if batches else "Cargar y generar OTs"
    if st.button(action_label):
        model = "VENTAS" if model_pick.startswith("Por ventas") else "SKU"
        if batches:
            result = append_orders_and_build_ots(
                sales_df,
                inv_map_sku,
                int(num_pickers),
                model=model,
                familia_map_sku=familia_map_sku,
                source_label=source_label,
            )
            if not result.get("created"):
                if result.get("reason") == "duplicate":
                    st.session_state["picking_import_flash"] = {
                        "level": "warning",
                        "message": "No se agregó una nueva tanda porque todas las ventas de este archivo ya estaban cargadas en la corrida actual.",
                    }
                    st.rerun()
                else:
                    st.session_state["picking_import_flash"] = {
                        "level": "warning",
                        "message": "No se pudo crear una nueva tanda con este archivo.",
                    }
                    st.rerun()
            else:
                picker_names = [str(x).strip() for x in (result.get('picker_names') or []) if str(x).strip()]
                ots_created = int(result.get('ots_created') or len(picker_names) or 0)
                pickers_txt = ", ".join(picker_names) if picker_names else f"{ots_created} OT{'s' if ots_created != 1 else ''}"
                st.session_state["picking_import_flash"] = {
                    "level": "success",
                    "message": f"Nueva carga creada correctamente: {pickers_txt}. Ya puedes ir a Picking.",
                }
                st.rerun()
        else:
            save_orders_and_build_ots(sales_df, inv_map_sku, int(num_pickers), model=model, familia_map_sku=familia_map_sku)
            st.session_state["picking_import_flash"] = {
                "level": "success",
                "message": "OTs creadas. Anda a Picking y selecciona P1, P2, ...",
            }
            st.rerun()


# =========================
# UI: CORTES (PDF de la tanda)
# =========================
def page_cortes_pdf_batch():
    st.header("Cortes de la tanda (PDF)")
    st.caption("Lista de productos que requieren corte manual (rollos). No aparecen en el picking PDA.")

    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT po.ot_code,
               ct.sku_ml,
               COALESCE(NULLIF(ct.title_tec,''), ct.title_ml) AS title,
               ct.qty_total
        FROM cortes_tasks ct
        JOIN picking_ots po ON po.id = ct.ot_id
        ORDER BY po.ot_code, CAST(ct.sku_ml AS INTEGER), ct.sku_ml
    """)
    rows = c.fetchall()
    conn.close()

    if not rows:
        st.info("No hay SKUs de corte en la tanda actual.")
        return

    df_raw = pd.DataFrame(rows, columns=["OT", "SKU", "Producto", "Cantidad"])

    # Consolidar por SKU (mismo producto) sumando cantidades
    df = (
        df_raw.groupby(["SKU", "Producto"], as_index=False)
        .agg(Cantidad=("Cantidad", "sum"), OTs=("OT", lambda s: sorted(set(map(str, s)))))
    )
    df["OTs"] = df["OTs"].apply(lambda xs: ", ".join(xs))
    # Orden por SKU numérico si aplica
    try:
        df["_sku_num"] = pd.to_numeric(df["SKU"], errors="coerce")
        df = df.sort_values(["_sku_num", "SKU"]).drop(columns=["_sku_num"])
    except Exception:
        df = df.sort_values(["SKU"])

    st.dataframe(df[["SKU", "Producto", "Cantidad"]], use_container_width=True, hide_index=True)

    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    import textwrap

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    w, h = A4
    y = h - 40

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(40, y, "Ferretería Aurora - Cortes")
    y -= 18
    pdf.setFont("Helvetica", 10)
    pdf.drawString(40, y, f"Generado: {to_chile_display(now_iso())}")
    y -= 22

    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(40, y, "SKU")
    pdf.drawString(140, y, "Producto")
    pdf.drawString(520, y, "Cant.")
    y -= 14

    pdf.setFont("Helvetica", 10)
    for _, r in df.iterrows():
        if y < 60:
            pdf.showPage()
            y = h - 40
            pdf.setFont("Helvetica-Bold", 14)
            pdf.drawString(40, y, "Ferretería Aurora - Cortes")
            y -= 18
            pdf.setFont("Helvetica", 10)
            pdf.drawString(40, y, f"Generado: {to_chile_display(now_iso())}")
            y -= 22
            pdf.setFont("Helvetica-Bold", 10)
            pdf.drawString(40, y, "SKU")
            pdf.drawString(140, y, "Producto")
            pdf.drawString(460, y, "OTs")
            pdf.drawString(540, y, "Cant.")
            y -= 14
            pdf.setFont("Helvetica", 10)

        sku = str(r["SKU"])
        title_full = str(r["Producto"])
        qty = str(int(r["Cantidad"]))

        # Envolver título en 2 líneas máximo para que no se corte ni se mezcle con la cantidad
        wrap_width = 62  # aprox. caracteres para la columna de Producto en A4
        lines = textwrap.wrap(title_full, width=wrap_width)[:2]
        if not lines:
            lines = [""]

        # Línea 1: SKU + Producto + Cantidad
        pdf.drawString(40, y, sku)
        pdf.drawString(140, y, lines[0])
        pdf.drawRightString(565, y, qty)
        y -= 12

        # Línea 2 (si aplica): continuación del producto (sin tocar cantidad)
        if len(lines) > 1:
            pdf.drawString(140, y, lines[1])
            y -= 12

    pdf.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()

    st.download_button(
        "⬇️ Descargar PDF de Cortes (tanda)",
        data=pdf_bytes,
        file_name=f"cortes_tanda_{now_iso().replace(':','-')}.pdf",
        mime="application/pdf",
        use_container_width=True,
    )


# =========================
# UI: PICKING (FLEX)
# =========================
def picking_lobby():
    st.markdown("### Picking")
    st.caption("Selecciona tu pickeador")

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT name FROM pickers ORDER BY name")
    rows = c.fetchall()
    conn.close()

    if not rows:
        st.info("Aún no hay pickeadores. Primero importa ventas y genera OTs.")
        return False

    pickers = [r[0] for r in rows]

    st.markdown(
        """
        <style>
        .bigbtn button {
            width: 100% !important;
            padding: 18px 10px !important;
            font-size: 22px !important;
            font-weight: 900 !important;
            border-radius: 16px !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    cols = st.columns(3)
    chosen = None
    for i, p in enumerate(pickers):
        with cols[i % 3]:
            st.markdown('<div class="bigbtn">', unsafe_allow_html=True)
            if st.button(p, key=f"pick_{p}"):
                chosen = p
            st.markdown('</div>', unsafe_allow_html=True)

    if chosen:
        st.session_state.selected_picker = chosen
        st.rerun()

    return "selected_picker" in st.session_state


def page_picking():
    if "selected_picker" not in st.session_state:
        ok = picking_lobby()
        if not ok:
            return

    picker_name = st.session_state.get("selected_picker", "")
    if not picker_name:
        st.session_state.pop("selected_picker", None)
        st.rerun()

    topA, topB = st.columns([2, 1])
    with topA:
        st.markdown(f"### Picking (PDA) — {picker_name}")
    with topB:
        if st.button("Cambiar pickeador"):
            st.session_state.pop("selected_picker", None)
            st.rerun()

    st.markdown(
        """
        <style>
        div.block-container { padding-top: 0.6rem; padding-bottom: 1rem; }
        .hero { padding: 10px 12px; border-radius: 12px; background: rgba(0,0,0,0.04); margin: 6px 0 8px 0; }
        .hero .sku { font-size: 26px; font-weight: 900; margin: 0; }
        .hero .prod { font-size: 22px; font-weight: 800; margin: 6px 0 0 0; line-height: 1.15; }
        .hero .qty { font-size: 26px; font-weight: 900; margin: 8px 0 0 0; }
.hero .loc { font-size: 18px; font-weight: 900; margin: 6px 0 0 0; opacity: 0.9; }
        .smallcap { font-size: 12px; opacity: 0.75; margin: 0 0 4px 0; }
        .scanok { display:inline-block; padding: 6px 10px; border-radius: 10px; font-weight: 900; }
        .ok { background: rgba(0, 200, 0, 0.15); }
        .bad { background: rgba(255, 0, 0, 0.12); }
        </style>
        """,
        unsafe_allow_html=True
    )

    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT barcode, sku_ml FROM sku_barcodes")
    barcode_to_sku = {r[0]: r[1] for r in c.fetchall()}

    c.execute("""
        SELECT po.id, po.ot_code, po.status
        FROM picking_ots po
        JOIN pickers pk ON pk.id = po.picker_id
        WHERE pk.name = ?
        ORDER BY po.ot_code
    """, (picker_name,))
    ots = c.fetchall()
    if not ots:
        st.error(f"No existe OT para {picker_name}. Importa ventas y genera OTs.")
        conn.close()
        return

    ot_row = None
    for r in ots:
        if r[2] != "PICKED":
            ot_row = r
            break
    if ot_row is None:
        ot_row = ots[0]

    ot_id, ot_code, ot_status = ot_row

    if ot_status == "PICKED":
        st.success("OT cerrada (PICKED).")
        conn.close()
        return

    c.execute("""
        SELECT id, sku_ml, title_ml, title_tec,
               qty_total, qty_picked, status
        FROM picking_tasks
        WHERE ot_id=?
        ORDER BY COALESCE(defer_rank,0) ASC, CAST(sku_ml AS INTEGER), sku_ml
    """, (ot_id,))
    tasks = c.fetchall()

    total_tasks = len(tasks)
    done_small = sum(1 for t in tasks if t[6] in ("DONE", "INCIDENCE"))
    st.caption(f"Resueltos: {done_small}/{total_tasks}")

    current = next((t for t in tasks if t[6] == "PENDING"), None)
    if current is None:
        st.success("No quedan SKUs pendientes.")
        if st.button("Cerrar OT"):
            c.execute("UPDATE picking_ots SET status='PICKED', closed_at=? WHERE id=?", (now_iso(), ot_id))
            conn.commit()
            st.success("OT cerrada.")
        conn.close()
        return

    task_id, sku_expected, title_ml, title_tec, qty_total, qty_picked, status = current

    # Título: prioridad absoluta al texto crudo del maestro (tal cual). Si no existe, cae a title_tec/title_ml.
    raw_master = master_raw_title_lookup(MASTER_FILE, sku_expected)
    producto_show = raw_master if raw_master else (title_tec if title_tec not in (None, "") else (title_ml or ""))
    if "pick_state" not in st.session_state:
        st.session_state.pick_state = {}
    state = st.session_state.pick_state
    if str(task_id) not in state:
        state[str(task_id)] = {
            "confirmed": False,
            "confirm_mode": None,
            "scan_value": "",
            "qty_input": "",
            "needs_decision": False,
            "missing": 0,
            "show_manual_confirm": False,
            "scan_status": "idle",
            "scan_msg": "",
            "last_sku_expected": None
        }
    s = state[str(task_id)]

    if s.get("last_sku_expected") != sku_expected:
        s["last_sku_expected"] = sku_expected
        s["confirmed"] = False
        s["confirm_mode"] = None
        s["needs_decision"] = False
        s["missing"] = 0
        s["show_manual_confirm"] = False
        s["scan_status"] = "idle"
        s["scan_msg"] = ""
        s["qty_input"] = ""
        s["scan_value"] = ""

    # Tarjeta principal: mostrar el título tal cual (incluye UBC/ubicación aunque venga al inicio/medio/final)
    st.caption(f"OT: {ot_code}")
    st.markdown(f"### SKU: {sku_expected}")

    st.markdown(
        f'<div class="hero"><div class="prod" style="white-space: normal; overflow-wrap: anywhere; word-break: break-word;">{html.escape(str(producto_show))}</div></div>',
        unsafe_allow_html=True,
    )

    # Fotos del producto (si existe match por SKU en publicaciones)
    try:
        pics, pub_link = get_picture_urls_for_sku(sku_expected)
    except Exception:
        pics, pub_link = [], ""
    if pics:
        st.image(pics[0], use_container_width=True)
        if len(pics) > 1:
            with st.expander(f"Ver más fotos ({len(pics)})", expanded=False):
                st.image(pics, use_container_width=True)

    st.markdown(f"### Solicitado: {qty_total}")

    try:
        stock_kame = obtener_stock_kame(sku_expected)
        fecha_stock_kame = obtener_fecha_stock_kame()
    except Exception:
        stock_kame = None
        fecha_stock_kame = ""

    fecha_stock_label = to_chile_display(fecha_stock_kame) if fecha_stock_kame else "N/D"
    if stock_kame is None:
        st.info(f"Stock Kame: N/D · Actualizado: {fecha_stock_label}")
    elif float(stock_kame) <= 0:
        st.error(f"Stock Kame: {format_stock_kame(stock_kame)} · Actualizado: {fecha_stock_label}")
    elif float(stock_kame) < float(qty_total):
        st.warning(f"Stock Kame: {format_stock_kame(stock_kame)} · Actualizado: {fecha_stock_label}")
    else:
        st.success(f"Stock Kame: {format_stock_kame(stock_kame)} · Actualizado: {fecha_stock_label}")

    if s["scan_status"] == "ok":
        st.markdown(
            f'<span class="scanok ok">✅ OK</span> {s["scan_msg"]}',
            unsafe_allow_html=True,
        )
    elif s["scan_status"] == "bad":
        st.markdown(
            f'<span class="scanok bad">❌ ERROR</span> {s["scan_msg"]}',
            unsafe_allow_html=True,
        )
        st.markdown(f'<span class="scanok bad">❌ ERROR</span> {s["scan_msg"]}', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

    with col1:
        scan_label = "Escaneo"
        scan = st.text_input(scan_label, value=s["scan_value"], key=f"scan_{task_id}")

        # Autofocus en PDA: después de elegir desde la lista, dejar listo el campo de escaneo
        if st.session_state.get("focus_scan", False):
            components.html(
                "<script>"
                "setTimeout(function(){"
                "const el=document.querySelector('input[type=\"text\"]');"
                "if(el){el.focus(); if(el.select){el.select();}}"
                "}, 50);"
                "</script>",
                height=0,
            )
            st.session_state["focus_scan"] = False
        force_tel_keyboard(scan_label)
        # Autofocus inteligente:
        # - Si ya validó el producto (confirmed), llevar el foco a "Cantidad"
        # - Si no, mantener foco en "Escaneo"
        if s.get("confirmed", False):
            autofocus_input("Cantidad")
        else:
            autofocus_input(scan_label)

    with col2:
        if st.button("Validar"):
            sku_detected = resolve_scan_to_sku(scan, barcode_to_sku)
            if not sku_detected:
                s["scan_status"] = "bad"
                s["scan_msg"] = "No se pudo leer el código."
                s["confirmed"] = False
                s["confirm_mode"] = None
            elif sku_detected != sku_expected:
                s["scan_status"] = "bad"
                s["scan_msg"] = f"Leído: {sku_detected}"
                s["confirmed"] = False
                s["confirm_mode"] = None
            else:
                s["scan_status"] = "ok"
                s["scan_msg"] = "Producto correcto."
                s["confirmed"] = True
                s["confirm_mode"] = "SCAN"
                s["scan_value"] = scan
            if s.get("scan_status") == "ok":
                sfx_emit("OK")
            elif s.get("scan_status") == "bad":
                sfx_emit("ERR")
            st.rerun()

    with col3:
        if st.button("Sin EAN"):
            s["show_manual_confirm"] = True
            st.rerun()

    with col4:
        if st.button("Siguiente"):
            # Siempre manda este SKU al final de la fila (rotación circular).
            # Implementación: defer_rank = (máximo defer_rank en esta OT) + 1
            try:
                c.execute("SELECT COALESCE(MAX(defer_rank), 0) FROM picking_tasks WHERE ot_id=?", (ot_id,))
                max_rank = c.fetchone()[0] or 0
                new_rank = int(max_rank) + 1
                c.execute(
                    "UPDATE picking_tasks SET defer_rank=?, defer_at=? WHERE id=?",
                    (new_rank, now_iso(), task_id)
                )
                conn.commit()
            except Exception:
                pass
            # Limpiar estado UI de este task y seguir con el siguiente
            state.pop(str(task_id), None)
            st.rerun()

    if s.get("show_manual_confirm", False) and not s["confirmed"]:
        st.info("Confirmación manual")
        st.write(f"✅ {producto_show}")
        if st.button("Confirmar", key=f"confirm_manual_{task_id}"):
            s["confirmed"] = True
            s["confirm_mode"] = "MANUAL_NO_EAN"
            s["show_manual_confirm"] = False
            s["scan_status"] = "ok"
            s["scan_msg"] = "Confirmado manual."
            st.rerun()

    qty_label = "Cantidad"
    qty_in = st.text_input(
        qty_label,
        value=s["qty_input"],
        disabled=not s["confirmed"],
        key=f"qty_{task_id}"
    )
    force_tel_keyboard(qty_label)

    if st.button("Confirmar cantidad", disabled=not s["confirmed"]):
        try:
            q = int(str(qty_in).strip())
        except Exception:
            st.error("Ingresa un número válido.")
            sfx_emit("ERR")
            q = None

        if q is not None:
            s["qty_input"] = str(q)

            if q > int(qty_total):
                st.error(f"La cantidad ({q}) supera solicitado ({qty_total}).")
                s["needs_decision"] = False

            elif q == int(qty_total):
                # Si el picker usó "Sin EAN", lo registramos en incidencias para trazabilidad
                if str(s.get("confirm_mode") or "") == "MANUAL_NO_EAN":
                    try:
                        c.execute("""INSERT INTO picking_incidences
                                     (ot_id, sku_ml, qty_total, qty_picked, qty_missing, reason, note, created_at)
                                     VALUES (?,?,?,?,?,?,?,?)""",
                                  (ot_id, sku_expected, int(qty_total), int(q), 0, "SIN_EAN", "", now_iso()))
                    except Exception:
                        pass

                c.execute("""
                    UPDATE picking_tasks
                    SET qty_picked=?, status='DONE', decided_at=?, confirm_mode=?
                    WHERE id=?
                """, (q, now_iso(), s["confirm_mode"], task_id))
                conn.commit()
                state.pop(str(task_id), None)
                st.success("OK. Siguiente…")
                sfx_emit("OK")
                st.rerun()
            else:
                missing = int(qty_total) - q
                s["needs_decision"] = True
                s["missing"] = missing
                st.warning(f"Faltan {missing}. Debes decidir (incidencias o reintentar).")

    if s["needs_decision"]:
        st.error(f"DECISIÓN: faltan {s['missing']} unidades.")
        colA, colB = st.columns(2)

        with colA:
            # Incidencia con nota (igual que Sorting): pedir motivo antes de guardar
            if "pick_inc_pending" not in st.session_state:
                st.session_state["pick_inc_pending"] = None

            pending = st.session_state.get("pick_inc_pending")
            is_pending = bool(pending and pending.get("task_id") == task_id)

            if (not is_pending) and st.button("A incidencias y seguir"):
                st.session_state["pick_inc_pending"] = {"task_id": task_id}
                st.rerun()

            if is_pending:
                st.warning("Incidencia: escribe el motivo antes de guardar.")
                note_val = st.text_area("Motivo / Nota", key=f"pick_inc_note_{task_id}", height=90,
                                        placeholder="Ej: Falta producto, no se encontró en ubicación, etc.")
                c1, c2 = st.columns([1, 1])
                if c1.button("💾 Guardar incidencia", key=f"pick_inc_save_{task_id}"):
                    q = int(s["qty_input"])
                    missing = int(qty_total) - q

                    c.execute("""INSERT INTO picking_incidences
                                 (ot_id, sku_ml, qty_total, qty_picked, qty_missing, reason, note, created_at)
                                 VALUES (?,?,?,?,?,?,?,?)""",
                              (ot_id, sku_expected, int(qty_total), q, missing, "FALTANTE", note_val or "", now_iso()))

                    c.execute("""UPDATE picking_tasks
                                 SET qty_picked=?, status='INCIDENCE', decided_at=?, confirm_mode=?
                                 WHERE id=?""",
                              (q, now_iso(), s["confirm_mode"], task_id))

                    conn.commit()
                    st.session_state["pick_inc_pending"] = None
                    state.pop(str(task_id), None)
                    st.success("Enviado a incidencias. Siguiente…")
                    st.rerun()

                if c2.button("Cancelar", key=f"pick_inc_cancel_{task_id}"):
                    st.session_state["pick_inc_pending"] = None
                    st.rerun()


        with colB:
            if st.button("Reintentar"):
                s["needs_decision"] = False
                st.info("Ajusta cantidad y confirma nuevamente.")
    # =========================
    
    # =========================
    # LISTA DE SKUS DE ESTA OT
    # =========================
    st.markdown("---")


    force_close_key = f"pick_force_close_list_{ot_id}"
    if force_close_key not in st.session_state:
        st.session_state[force_close_key] = False

    label_list = "📋 Lista de SKUs de esta OT" + ("\u200b" if st.session_state.get(force_close_key, False) else "")
    with st.expander(label_list, expanded=False):

        # Forzar cierre en la próxima recarga (especial PDA)
        st.session_state[force_close_key] = False

        st.caption("Toca un SKU pendiente para ponerlo como el próximo a escanear. Luego sigues normal.")

        # Pendientes primero
        ordered = sorted(
            tasks,
            key=lambda t: (0 if t[6] == "PENDING" else 1, str(t[1]))
        )

        for t in ordered:
            _tid, _sku, _title_ml, _title_tec, _qty_total, _qty_picked, _status = t

            raw_master_t = master_raw_title_lookup(MASTER_FILE, _sku)
            _title_show = raw_master_t if raw_master_t else (
                _title_tec if _title_tec not in (None, "") else (_title_ml or "")
            )

            disabled = (_status != "PENDING") or (_tid == task_id)
            label = f"{_title_show} [{_sku}]"

            if st.button(label, disabled=disabled, key=f"jump_{ot_id}_{_tid}"):

                try:
                    pending_order = [int(x[0]) for x in ordered if str(x[6]) == "PENDING"]
                    if _tid in pending_order:
                        idx_sel = pending_order.index(_tid)
                        rotated = pending_order[idx_sel:] + pending_order[:idx_sel]
                        base_rank = -len(rotated)
                        for i, tid_rot in enumerate(rotated):
                            c.execute(
                                "UPDATE picking_tasks SET defer_rank=?, defer_at=? WHERE id=?",
                                (base_rank + i, now_iso(), tid_rot)
                            )
                        conn.commit()

                except Exception:
                    pass

                if "pick_state" in st.session_state:
                    st.session_state.pick_state.pop(str(task_id), None)
                    st.session_state.pick_state.pop(str(_tid), None)

                st.session_state[force_close_key] = True
                st.session_state['focus_scan'] = True
                st.rerun()

    conn.close()



# =========================
# FULL: Importar Excel -> Batch
# =========================
def _pick_col(cols_lower: list[str], cols_orig: list[str], candidates: list[str]):
    for cand in candidates:
        if cand in cols_lower:
            return cols_orig[cols_lower.index(cand)]
    return None


def _safe_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    return s

def _cell_to_str(x) -> str:
    """Convierte celdas que pueden venir como Series (por columnas duplicadas) a string limpio."""
    try:
        # Si por error hay columnas duplicadas, pandas puede entregar Series en vez de escalar
        if isinstance(x, pd.Series):
            for v in x.tolist():
                s = _safe_str(v)
                if s:
                    return s
            return ""
    except Exception:
        pass
    return _safe_str(x)


def read_full_excel(file) -> pd.DataFrame:
    """
    Lee todas las hojas y devuelve un DF normalizado:
    sku_ml, title, qty_required, area, nro, etiquetar, es_pack, instruccion, vence, sheet
    """
    xls = pd.ExcelFile(file)
    all_rows = []
    for sh in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sh, dtype=str)
        if df is None or df.empty:
            continue

        cols_orig = df.columns.tolist()
        cols_lower = [str(c).strip().lower() for c in cols_orig]

        sku_col = _pick_col(cols_lower, cols_orig, ["sku", "sku_ml", "codigo", "código", "cod", "ubc", "cod sku"])
        qty_col = _pick_col(cols_lower, cols_orig, ["cantidad", "qty", "unidades", "cant", "cant.", "cantidad total"])
        title_col = _pick_col(cols_lower, cols_orig, ["articulo", "artículo", "descripcion", "descripción", "producto", "detalle", "artículo / producto"])

        area_col = _pick_col(cols_lower, cols_orig, ["area", "área", "zona", "ubicacion", "ubicación"])
        nro_col = _pick_col(cols_lower, cols_orig, ["nro", "n°", "numero", "número", "num", "#", "n"])

        etiquetar_col = _pick_col(cols_lower, cols_orig, ["etiquetar", "etiqueta"])
        pack_col = _pick_col(cols_lower, cols_orig, ["es pack", "pack", "es_pack", "espack"])
        instr_col = _pick_col(cols_lower, cols_orig, ["instruccion", "instrucción", "obs", "observacion", "observación", "nota", "notas"])
        vence_col = _pick_col(cols_lower, cols_orig, ["vence", "vencimiento", "fecha vence", "fecha_vencimiento"])

        # Fallback mínimo: si no hay columnas clave, intentar por posición
        if sku_col is None or qty_col is None:
            if df.shape[1] >= 3:
                # intento: col0 area, col1 nro, col2 sku, col3 desc, col4 qty
                sku_col = sku_col or cols_orig[min(2, len(cols_orig) - 1)]
                qty_col = qty_col or cols_orig[min(4, len(cols_orig) - 1)]
                title_col = title_col or cols_orig[min(3, len(cols_orig) - 1)]
                area_col = area_col or cols_orig[0]
                nro_col = nro_col or cols_orig[min(1, len(cols_orig) - 1)]

        for _, r in df.iterrows():
            sku = normalize_sku(r.get(sku_col, "")) if sku_col else ""
            if not sku:
                continue

            qty_raw = r.get(qty_col, "") if qty_col else ""
            try:
                qty = int(float(str(qty_raw).strip())) if str(qty_raw).strip() else 0
            except Exception:
                qty = 0
            if qty <= 0:
                continue

            title = _safe_str(r.get(title_col, "")) if title_col else ""
            area = _safe_str(r.get(area_col, "")) if area_col else ""
            nro = _safe_str(r.get(nro_col, "")) if nro_col else ""
            etiquetar = _safe_str(r.get(etiquetar_col, "")) if etiquetar_col else ""
            es_pack = _safe_str(r.get(pack_col, "")) if pack_col else ""
            instruccion = _safe_str(r.get(instr_col, "")) if instr_col else ""
            vence = _safe_str(r.get(vence_col, "")) if vence_col else ""

            all_rows.append({
                "sheet": sh,
                "sku_ml": sku,
                "title": title,
                "qty_required": qty,
                "area": area,
                "nro": nro,
                "etiquetar": etiquetar,
                "es_pack": es_pack,
                "instruccion": instruccion,
                "vence": vence,
            })

    return pd.DataFrame(all_rows)




def get_open_full_batches():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id, batch_name, status, created_at FROM full_batches WHERE status='OPEN' ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()
    return rows


def upsert_full_batch_from_df(df: pd.DataFrame, batch_name: str):
    """
    Crea un batch y carga items agregados por SKU.
    """
    if df is None or df.empty:
        raise ValueError("El Excel no tiene filas válidas (SKU/Cantidad).")

    # Agregar por SKU
    agg = {}
    for _, r in df.iterrows():
        sku = normalize_sku(r.get("sku_ml", ""))
        if not sku:
            continue

        qty = int(r.get("qty_required", 0) or 0)
        if qty <= 0:
            continue

        if sku not in agg:
            agg[sku] = {
                "sku_ml": sku,
                "title": _cell_to_str(r.get("title", "")),
                "qty_required": 0,
                "areas": set(),
                "nros": set(),
                "etiquetar": "",
                "es_pack": "",
                "instruccion": "",
                "vence": "",
            }

        a = agg[sku]
        a["qty_required"] += qty

        area = _safe_str(r.get("area", ""))
        nro = _safe_str(r.get("nro", ""))
        if area:
            a["areas"].add(area)
        if nro:
            a["nros"].add(nro)

        # En campos opcionales, guardamos el primero no vacío (si hay)
        for k in ["etiquetar", "es_pack", "instruccion", "vence"]:
            v = _safe_str(r.get(k, ""))
            if v and not a.get(k):
                a[k] = v

        # si no hay título, intentar completar después con maestro (en UI)
        if not a["title"]:
            a["title"] = _cell_to_str(r.get("title", ""))

    conn = get_conn()
    c = conn.cursor()

    created = now_iso()
    c.execute(
        "INSERT INTO full_batches (batch_name, status, created_at, closed_at) VALUES (?,?,?,?)",
        (batch_name, "OPEN", created, None)
    )
    batch_id = c.lastrowid

    for sku, a in agg.items():
        areas_txt = " / ".join(sorted(a["areas"])) if a["areas"] else ""
        nros_txt = " / ".join(sorted(a["nros"])) if a["nros"] else ""
        c.execute("""
            INSERT INTO full_batch_items
            (batch_id, sku_ml, title, areas, nros, etiquetar, es_pack, instruccion, vence, qty_required, qty_checked, status, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            batch_id, sku, a["title"], areas_txt, nros_txt,
            a.get("etiquetar", ""), a.get("es_pack", ""), a.get("instruccion", ""), a.get("vence", ""),
            int(a["qty_required"]), 0, "PENDING", now_iso()
        ))

    conn.commit()
    conn.close()
    return batch_id


def get_full_batch_summary(batch_id: int):
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT batch_name, status, created_at, closed_at FROM full_batches WHERE id=?", (batch_id,))
    b = c.fetchone()

    c.execute("""
        SELECT
            COUNT(*) as n_skus,
            SUM(qty_required) as req_units,
            SUM(qty_checked) as chk_units,
            SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) as ok_skus,
            SUM(CASE WHEN status IN ('PARTIAL','INCIDENCE','OVER','OK_WITH_ISSUES') THEN 1 ELSE 0 END) as touched_skus,
            SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) as pending_skus
        FROM full_batch_items
        WHERE batch_id=?
    """, (batch_id,))
    s = c.fetchone()

    conn.close()
    return b, s


# =========================
# UI: FULL - CARGA EXCEL
# =========================
def page_full_upload(inv_map_sku: dict):
    st.header("Full – Cargar Excel")

    if st.session_state.get("scroll_to_scan", False):
        components.html(
            "<script>const el=document.getElementById('scan_top'); if(el){el.scrollIntoView({behavior:'smooth', block:'start'});}</script>",
            height=0,
        )
        st.session_state["scroll_to_scan"] = False

    # Confirmación (mensaje flash)
    if st.session_state.get("full_flash"):
        st.success(st.session_state.get("full_flash"))
        st.session_state["full_flash"] = ""

    # Solo 1 corrida a la vez: si hay lote abierto, no permitir cargar otro
    open_batches = get_open_full_batches()
    if open_batches:
        active_id, active_name, active_status, active_created = open_batches[0]
        st.warning(
            f"Ya hay un lote Full en curso (#{active_id}). "
            "Para cargar uno nuevo, ve a **Full – Admin** y usa **Reiniciar corrida (BORRA TODO)**."
        )
        return

    # Nombre de lote automático (no se muestra)
    batch_name = f"FULL_{(datetime.now(CL_TZ) if CL_TZ else datetime.now()).strftime('%Y-%m-%d_%H%M')}"

    file = st.file_uploader("Excel de preparación Full (xlsx)", type=["xlsx"], key="full_excel")
    if not file:
        st.info("Sube el Excel que usan para enviar hojas a auxiliares.")
        return

    try:
        df = read_full_excel(file)
    except Exception as e:
        st.error(f"No pude leer el Excel: {e}")
        return

    if df.empty:
        st.warning("El archivo se leyó, pero no encontré filas válidas (SKU/Cantidad).")
        return

    # Completar título desde maestro si está vacío
    df2 = df.copy()
    df2["title_eff"] = df2.apply(lambda r: r["title"] if str(r["title"]).strip() else inv_map_sku.get(r["sku_ml"], ""), axis=1)

    st.subheader("Vista previa (primeras 50 filas)")
    st.dataframe(df2.head(50))

    st.caption("Se agregará por SKU (sumando cantidades de todas las hojas).")

    if st.button("✅ Crear lote y cargar"):
        try:
            # Guardar SOLO un 'title' (evita duplicar columnas y que se muestre como Series)
            df_save = df2.copy()
            if "title_eff" in df_save.columns:
                if "title" in df_save.columns:
                    df_save = df_save.drop(columns=["title"])
                df_save = df_save.rename(columns={"title_eff": "title"})

            batch_id = upsert_full_batch_from_df(df_save, str(batch_name).strip())

            # Mostrar confirmación aunque hagamos rerun
            st.session_state["full_flash"] = f"✅ Lote Full cargado correctamente (#{batch_id})."
            st.session_state.full_selected_batch = batch_id
            st.rerun()
        except Exception as e:
            st.error(str(e))




def page_full_supervisor(inv_map_sku: dict):
    st.header("Full – Supervisor de acopio")

    # Resolver lote activo: debe existir un lote OPEN (solo trabajamos con 1 a la vez)
    open_batches = get_open_full_batches()
    if not open_batches:
        st.info("No hay un lote Full en curso. Ve a **Full – Cargar Excel** para crear la corrida.")
        return

    batch_id, _batch_name, _status, _created_at = open_batches[0]

    # Map barcode->sku desde DB (maestro ya lo cargó)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT barcode, sku_ml FROM sku_barcodes")
    barcode_to_sku = {r[0]: r[1] for r in c.fetchall()}
    conn.close()

    st.markdown(
        """
        <style>
        .hero2 { padding: 10px 12px; border-radius: 12px; background: rgba(0,0,0,0.04); margin: 8px 0; }
        .hero2 .sku { font-size: 26px; font-weight: 900; margin: 0; }
        .hero2 .prod { font-size: 22px; font-weight: 800; margin: 6px 0 0 0; line-height: 1.15; }
        .hero2 .qty { font-size: 20px; font-weight: 900; margin: 8px 0 0 0; }
        .hero2 .meta { font-size: 14px; font-weight: 700; margin: 6px 0 0 0; opacity: 0.85; line-height: 1.2; }
        .tag { display:inline-block; padding: 6px 10px; border-radius: 10px; font-weight: 900; }
        .ok { background: rgba(0, 200, 0, 0.15); }
        .bad { background: rgba(255, 0, 0, 0.12); }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Estado UI supervisor (por lote)
    if "full_sup_state" not in st.session_state:
        st.session_state.full_sup_state = {}
    state = st.session_state.full_sup_state
    if str(batch_id) not in state:
        state[str(batch_id)] = {
            "sku_current": "",
            "msg": "",
            "msg_kind": "idle",
            "confirm_partial": False,
            "pending_qty": None,
            "scan_nonce": 0,
            "qty_nonce": 0
        }
    sst = state[str(batch_id)]

    scan_key = f"full_scan_{batch_id}_{sst.get('scan_nonce',0)}"
    qty_key  = f"full_qty_{batch_id}_{sst.get('qty_nonce',0)}"

    # Mensaje flash (se muestra una vez)
    flash_key = f"full_flash_{batch_id}"
    if flash_key in st.session_state:
        kind, msg = st.session_state.get(flash_key, ("info", ""))
        if msg:
            if kind == "warning":
                st.warning(msg)
            elif kind == "success":
                st.success(msg)
            else:
                st.info(msg)
        st.session_state.pop(flash_key, None)

    scan_label = "Escaneo"
    scan = st.text_input(scan_label, key=scan_key)
    force_tel_keyboard(scan_label)
    autofocus_input(scan_label)

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("🔎 Buscar / Validar", key=f"full_find_{batch_id}"):
            sku = resolve_scan_to_sku(scan, barcode_to_sku)
            sst["sku_current"] = sku
            sst["confirm_partial"] = False
            sst["pending_qty"] = None
            try:
                st.session_state[qty_key] = ""
            except Exception:
                pass

            if not sku:
                sst["msg_kind"] = "bad"
                sst["msg"] = "No se pudo leer el código."
                sfx_emit("ERR")
                st.rerun()

            conn = get_conn()
            c = conn.cursor()
            c.execute("""
                SELECT 1
                FROM full_batch_items
                WHERE batch_id=? AND sku_ml=?
            """, (batch_id, sku))
            ok = c.fetchone()
            conn.close()

            if not ok:
                sst["msg_kind"] = "bad"
                sst["msg"] = f"{sku} no pertenece a este lote."
                sst["sku_current"] = ""
                sfx_emit("ERR")
            else:
                sst["msg_kind"] = "ok"
                sst["msg"] = "SKU encontrado."
                sfx_emit("OK")
            st.rerun()

    with colB:
        if st.button("🧹 Limpiar", key=f"full_clear_{batch_id}"):
            sst["sku_current"] = ""
            sst["msg_kind"] = "idle"
            sst["msg"] = ""
            sst["confirm_partial"] = False
            sst["pending_qty"] = None
            sst["scan_nonce"] = int(sst.get("scan_nonce",0)) + 1
            sst["qty_nonce"]  = int(sst.get("qty_nonce",0)) + 1
            st.rerun()

    if sst.get("msg_kind") == "ok":
        st.markdown(f'<span class="tag ok">✅ OK</span> {sst.get("msg","")}', unsafe_allow_html=True)
    elif sst.get("msg_kind") == "bad":
        st.markdown(f'<span class="tag bad">❌ ERROR</span> {sst.get("msg","")}', unsafe_allow_html=True)

    sku_cur = normalize_sku(sst.get("sku_current", ""))
    if not sku_cur:
        st.info("Escanea un producto para ver datos.")
        return

    # Traer datos del SKU desde el lote
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT sku_ml, COALESCE(NULLIF(title,''),''), qty_required, COALESCE(qty_checked,0), COALESCE(etiquetar,''), COALESCE(es_pack,''), COALESCE(instruccion,''), COALESCE(vence,'')
        FROM full_batch_items
        WHERE batch_id=? AND sku_ml=?
    """, (batch_id, sku_cur))
    row = c.fetchone()
    conn.close()

    if not row:
        st.warning("El SKU no está en el lote (vuelve a validar).")
        return

    sku_db, title_db, qty_req, qty_chk, etiquetar_db, es_pack_db, instruccion_db, vence_db = row
    title_clean = str(title_db or "").strip()
    # Seguridad: si por algún motivo title viene como Series/objeto raro
    if hasattr(title_db, "iloc"):
        try:
            title_clean = str(title_db.iloc[0] or "").strip()
        except Exception:
            title_clean = str(title_db).strip()
    if not title_clean:
        title_clean = inv_map_sku.get(sku_db, "")

    pending = int(qty_req) - int(qty_chk)
    if pending < 0:
        pending = 0

    # Campos extra del Excel Full
    etiquetar_txt = str(etiquetar_db or "").strip() or "-"
    es_pack_txt = str(es_pack_db or "").strip() or "-"
    instruccion_txt = str(instruccion_db or "").strip() or "-"
    vence_txt = str(vence_db or "").strip() or "-"

    st.markdown(
        f"""
        <div class="hero2">
            <div class="sku">SKU: {sku_db}</div>
            <div class="prod">{title_clean}</div>
            <div class="qty">Solicitado: {int(qty_req)} • Acopiado: {int(qty_chk)} • Pendiente: {pending}</div>
            <div class="meta">ETIQUETAR: {etiquetar_txt} • ES PACK: {es_pack_txt}<br/>INSTRUCCIÓN: {instruccion_txt} • VENCE: {vence_txt}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    qty_label = "Cantidad a acopiar"
    qty_in = st.text_input(qty_label, key=qty_key)
    force_tel_keyboard(qty_label)

    def do_acopio(q: int):
        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
            UPDATE full_batch_items
            SET qty_checked = COALESCE(qty_checked,0) + ?,
                status = CASE WHEN (COALESCE(qty_checked,0) + ?) >= COALESCE(qty_required,0) THEN 'OK' ELSE 'PENDING' END,
                updated_at = ?
            WHERE batch_id=? AND sku_ml=?
        """, (q, q, now_iso(), batch_id, sku_db))
        conn2.commit()
        conn2.close()

        # Limpiar campos para siguiente escaneo
        sst["sku_current"] = ""
        sst["msg_kind"] = "idle"
        sst["msg"] = ""
        sst["confirm_partial"] = False
        sst["pending_qty"] = None
        sst["scan_nonce"] = int(sst.get("scan_nonce",0)) + 1
        sst["qty_nonce"]  = int(sst.get("qty_nonce",0)) + 1

        st.session_state[flash_key] = ("success", f"✅ Acopio registrado: {q} unidad(es).")
        st.rerun()

    # Si está pendiente confirmación parcial, mostrar confirmación ANTES de acopiar
    if sst.get("confirm_partial") and sst.get("pending_qty") is not None:
        q_pending = int(sst["pending_qty"])
        st.warning(f"Vas a acopiar **{q_pending}** unidad(es), pero el pendiente actual es **{pending}**. ¿Confirmas acopio parcial?")
        colP1, colP2 = st.columns(2)
        with colP1:
            if st.button("✅ Sí, confirmar acopio parcial", key=f"full_confirm_partial_yes_{batch_id}"):
                # Revalidar pendiente para evitar carrera
                if q_pending <= 0:
                    st.error("Cantidad inválida.")
                    return
                if q_pending > pending:
                    st.error(f"No puedes acopiar {q_pending}. Pendiente actual: {pending}.")
                    sst["confirm_partial"] = False
                    sst["pending_qty"] = None
                    return
                do_acopio(q_pending)
        with colP2:
            if st.button("Cancelar", key=f"full_confirm_partial_no_{batch_id}"):
                sst["confirm_partial"] = False
                sst["pending_qty"] = None
                st.session_state[flash_key] = ("info", "Acopio parcial cancelado. Ajusta cantidad y confirma nuevamente.")
                st.rerun()

        # Importante: no mostrar el botón normal mientras espera confirmación
        return

    colC, colD = st.columns([1, 1])
    with colC:
        if st.button("✅ Confirmar acopio", key=f"full_confirm_{batch_id}"):
            try:
                q = int(str(qty_in).strip())
            except Exception:
                st.error("Ingresa un número válido.")
                return

            if q <= 0:
                st.error("La cantidad debe ser mayor a 0.")
                return

            # No permitimos sobrantes: no puede superar el pendiente
            if q > pending:
                st.error(f"No puedes acopiar {q}. Pendiente actual: {pending}.")
                return

            # Si es menor al pendiente, pedir confirmación ANTES de acopiar
            if q < pending:
                sst["confirm_partial"] = True
                sst["pending_qty"] = q
                st.rerun()

            # Si es exacto, acopia directo
            do_acopio(q)

    with colD:
        if st.button("🧹 Limpiar campos", key=f"full_clear2_{batch_id}"):
            sst["sku_current"] = ""
            sst["msg_kind"] = "idle"
            sst["msg"] = ""
            sst["confirm_partial"] = False
            sst["pending_qty"] = None
            sst["scan_nonce"] = int(sst.get("scan_nonce",0)) + 1
            sst["qty_nonce"]  = int(sst.get("qty_nonce",0)) + 1
            st.rerun()


def page_full_admin():
    if not require_admin_access():
        return

    st.header("Full – Administrador (progreso)")

    # Respaldo/Restauración SOLO FULL (no afecta otros módulos)
    _render_module_backup_ui("full", "Full", FULL_TABLES)


    batches = get_open_full_batches()
    if not batches:
        st.info("No hay lotes Full cargados aún.")
        return

    options = [f"#{bid} — {name} ({status})" for bid, name, status, _ in batches]
    default_idx = 0
    if "full_selected_batch" in st.session_state:
        for i, (bid, *_rest) in enumerate(batches):
            if bid == st.session_state.full_selected_batch:
                default_idx = i
                break

    sel = st.selectbox("Lote", options, index=default_idx)
    batch_id = batches[options.index(sel)][0]
    st.session_state.full_selected_batch = batch_id

    b, s = get_full_batch_summary(batch_id)
    if not b:
        st.error("No se encontró el lote.")
        return

    batch_name, bstatus, created_at, closed_at = b
    n_skus, req_units, chk_units, ok_skus, touched_skus, pending_skus = s
    n_skus = int(n_skus or 0)
    req_units = int(req_units or 0)
    chk_units = int(chk_units or 0)
    ok_skus = int(ok_skus or 0)
    pending_skus = int(pending_skus or 0)

    prog = (chk_units / req_units) if req_units else 0.0

    st.caption(f"Lote: {batch_name} • Creado: {to_chile_display(created_at)} • Estado: {bstatus}")
    st.progress(min(max(prog, 0.0), 1.0))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Progreso unidades", f"{prog*100:.1f}%")
    c2.metric("Unidades acopiadas", f"{chk_units}/{req_units}")
    c3.metric("SKUs OK", f"{ok_skus}/{n_skus}")
    c4.metric("SKUs pendientes", pending_skus)

    conn = get_conn()
    c = conn.cursor()

    st.subheader("Detalle por SKU")
    c.execute("""
        SELECT sku_ml, COALESCE(NULLIF(title,''),''), qty_required, qty_checked,
               (qty_required - qty_checked) as pendiente,
               status, updated_at, areas, nros
        FROM full_batch_items
        WHERE batch_id=?
        ORDER BY status, CAST(sku_ml AS INTEGER), sku_ml
    """, (batch_id,))
    rows = c.fetchall()
    df = pd.DataFrame(rows, columns=["SKU", "Artículo", "Solicitado", "Acopiado", "Pendiente", "Estado", "Actualizado", "Áreas", "Nros"])
    df["Actualizado"] = df["Actualizado"].apply(to_chile_display)
    st.dataframe(df, use_container_width=True)

    st.subheader("Incidencias")
    c.execute("""
        SELECT sku_ml, qty_required, qty_checked, diff, reason, created_at
        FROM full_incidences
        WHERE batch_id=?
        ORDER BY created_at DESC
    """, (batch_id,))
    inc = c.fetchall()
    if inc:
        df_inc = pd.DataFrame(inc, columns=["SKU", "Req", "Chk", "Diff", "Motivo", "Hora"])
        df_inc["Hora"] = df_inc["Hora"].apply(to_chile_display)
        # Producto (nombre técnico): usar maestro si existe, si no SKU
        if isinstance(inv_map_sku, dict) and not df_inc.empty:
            def _pname(sku):
                k = str(sku).strip()
                return inv_map_sku.get(k) or master_raw_title_lookup(MASTER_FILE, k) or k
            df_inc["Producto"] = df_inc["SKU"].apply(_pname)
        else:
            df_inc["Producto"] = df_inc["SKU"].astype(str)
        df_inc = df_inc[["OT","Picker","SKU","Producto","Solicitado","Pickeado","Faltante","Motivo","Nota","Hora"]]
        st.dataframe(df_inc, use_container_width=True)
    else:
        st.info("Sin incidencias registradas para este lote.")

    st.divider()

    st.subheader("Acciones")

    # Reiniciar corrida FULL (borrar todo lo cargado para Full)
    if "full_confirm_reset" not in st.session_state:
        st.session_state.full_confirm_reset = False

    if not st.session_state.full_confirm_reset:
        if st.button("🔄 Reiniciar corrida (BORRA TODO Full)"):
            st.session_state.full_confirm_reset = True
            st.warning("⚠️ Esto borrará TODOS los datos de Full (lote, items y registros de acopio). Confirma abajo.")
            st.rerun()
    else:
        st.error("CONFIRMACIÓN: se borrará TODO lo relacionado a Full.")
        colA, colB = st.columns(2)
        with colA:
            if st.button("✅ Sí, borrar todo y reiniciar Full"):
                conn2 = get_conn()
                c2 = conn2.cursor()
                c2.execute("DELETE FROM full_incidences;")
                c2.execute("DELETE FROM full_batch_items;")
                c2.execute("DELETE FROM full_batches;")
                conn2.commit()
                conn2.close()

                st.session_state.full_confirm_reset = False
                st.session_state.pop("full_selected_batch", None)

                # limpiar estados UI del supervisor
                if "full_supervisor_state" in st.session_state:
                    st.session_state.pop("full_supervisor_state", None)

                st.success("Full reiniciado (todo borrado).")
                st.rerun()
        with colB:
            if st.button("Cancelar"):
                st.session_state.full_confirm_reset = False
                st.info("Reinicio cancelado.")
                st.rerun()

    conn.close()


# =========================
# UI: ADMIN (FLEX)
# =========================
def page_admin():
    if not require_admin_access():
        return

    st.header("Administrador")


    # =========================
    # PERSISTENCIA (Streamlit Community Cloud)
    # =========================
    st.subheader("Persistencia / Respaldo — PICKING")
    _render_module_backup_ui("picking", "Picking", PICKING_TABLES)

    st.divider()

    conn = get_conn()
    c = conn.cursor()

    st.subheader("Resumen")
    c.execute("SELECT COUNT(*) FROM orders")
    n_orders = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM order_items")
    n_items = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM picking_ots")
    n_ots = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM picking_incidences")
    n_inc = c.fetchone()[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Ventas", n_orders)
    col2.metric("Líneas", n_items)
    col3.metric("OTs", n_ots)
    col4.metric("Incidencias", n_inc)

    st.subheader("Estado OTs")
    c.execute("""
        SELECT po.ot_code, pk.name, po.status, po.created_at, po.closed_at,
               SUM(CASE WHEN pt.status='PENDING' THEN 1 ELSE 0 END) as pendientes,
               SUM(CASE WHEN pt.status IN ('DONE','INCIDENCE') THEN 1 ELSE 0 END) as resueltas,
               SUM(CASE WHEN pt.confirm_mode='MANUAL_NO_EAN' THEN 1 ELSE 0 END) as manual_no_ean
        FROM picking_ots po
        JOIN pickers pk ON pk.id = po.picker_id
        LEFT JOIN picking_tasks pt ON pt.ot_id = po.id
        GROUP BY po.ot_code, pk.name, po.status, po.created_at, po.closed_at
        ORDER BY po.ot_code
    """)
    df = pd.DataFrame(c.fetchall(), columns=[
        "OT", "Picker", "Estado", "Creada", "Cerrada",
        "Pendientes", "Resueltas", "Sin EAN"
    ])
    df["Creada"] = df["Creada"].apply(to_chile_display)
    df["Cerrada"] = df["Cerrada"].apply(to_chile_display)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Fotos de productos (Publicaciones)")
    st.caption(f"Se carga automáticamente desde **{PUBLICATIONS_FILE}** (incluido en el repo).")
    
    if not os.path.exists(PUBLICATIONS_FILE):
        st.warning(f"No se encontró el archivo: {PUBLICATIONS_FILE}. (No se mostrarán fotos por SKU)")
    else:
        # Estado simple desde DB
        conn2 = get_conn()
        c2 = conn2.cursor()
        try:
            c2.execute("SELECT COUNT(1), MAX(updated_at) FROM sku_publications;")
            n_pubs, last_upd = c2.fetchone()
        except Exception:
            n_pubs, last_upd = 0, None
        conn2.close()
    
        st.info(f"Links cargados: **{int(n_pubs or 0)}** SKUs. Última actualización: **{to_chile_display(last_upd) if last_upd else '-'}**")

    st.divider()

    st.divider()
    st.subheader("Liberar y repartir tareas pendientes (por SKU)")

    # Nota: aquí "OT" se refiere a la tarea/línea (SKU + cantidad). Solo mueve tareas PENDING (sin avance).
    try:
        c.execute("SELECT id, name FROM pickers ORDER BY id")
        pickers_rows = c.fetchall()
    except Exception:
        pickers_rows = []
    if not pickers_rows:
        st.info("No hay pickeadores creados todavía.")
    else:
        picker_id_to_name = {int(pid): pname for pid, pname in pickers_rows}
        picker_names = [pname for _, pname in pickers_rows]
        picker_name_to_id = {pname: int(pid) for pid, pname in pickers_rows}

        colA, colB = st.columns([1, 2])
        with colA:
            src_name = st.selectbox("Picker origen", picker_names, key="adm_reassign_src_picker")
        src_id = picker_name_to_id.get(src_name)

        # Traer tareas PENDING del picker origen (sin avance)
        c.execute("""
            SELECT pt.id,
                   po.ot_code,
                   pt.sku_ml,
                   COALESCE(NULLIF(pt.title_tec,''), NULLIF(pt.title_ml,''), pt.sku_ml) AS producto,
                   pt.qty_total
            FROM picking_tasks pt
            JOIN picking_ots po ON po.id = pt.ot_id
            WHERE po.picker_id = ?
              AND pt.status = 'PENDING'
              AND COALESCE(pt.qty_picked,0) = 0
            ORDER BY pt.id
        """, (src_id,))
        task_rows = c.fetchall()

        if not task_rows:
            st.info(f"No hay tareas pendientes para {src_name}.")
        else:
            df_tasks = pd.DataFrame(task_rows, columns=["task_id","OT_origen","SKU","Producto","Cantidad"])
            # Editor con selección
            df_edit = df_tasks.copy()
            df_edit.insert(0, "Mover", False)

            # Botón: seleccionar todo (solo afecta la columna "Mover")
            col_sel1, _ = st.columns([1, 5])
            with col_sel1:
                if st.button("Seleccionar todo", key="adm_reassign_select_all"):
                    st.session_state["adm_reassign_select_all_flag"] = True

            if st.session_state.get("adm_reassign_select_all_flag"):
                df_edit["Mover"] = True
                st.session_state["adm_reassign_select_all_flag"] = False

            edited = st.data_editor(
                df_edit,
                use_container_width=True,
                hide_index=True,
                key="adm_reassign_editor",
                column_order=["Mover", "OT_origen", "SKU", "Producto", "Cantidad"],
                column_config={
                    "Mover": st.column_config.CheckboxColumn("Mover", help="Marca para mover esta tarea a otro pickeador"),
                    "task_id": st.column_config.NumberColumn("ID", disabled=True),
                    "OT_origen": st.column_config.TextColumn("OT origen", disabled=True),
                    "SKU": st.column_config.TextColumn("SKU", disabled=True),
                    "Producto": st.column_config.TextColumn("Producto", disabled=True),
                    "Cantidad": st.column_config.NumberColumn("Cant.", disabled=True),
                },
                disabled=["task_id","OT_origen","SKU","Producto","Cantidad"]
            )

            selected_ids = [int(r["task_id"]) for _, r in edited.iterrows() if bool(r.get("Mover"))]
            n_sel = len(selected_ids)
            st.write(f"**Seleccionadas:** {n_sel}")

            if n_sel > 0:
                def _next_picker_numbers(existing_names: list[str], qty: int) -> list[int]:
                    nums = []
                    for pname in existing_names:
                        m = re.fullmatch(r"P(\d+)", str(pname or "").strip().upper())
                        if m:
                            nums.append(int(m.group(1)))
                    start_n = (max(nums) + 1) if nums else 2
                    return list(range(start_n, start_n + int(qty)))

                with st.expander("➕ Crear pickeadores destino", expanded=(len(picker_names) <= 1)):
                    qty_new_pickers = st.number_input(
                        "Cuántos crear",
                        min_value=1,
                        max_value=20,
                        value=1,
                        step=1,
                        key="adm_new_picker_qty"
                    )

                    preview_nums = _next_picker_numbers(picker_names, int(qty_new_pickers))
                    preview_names = [f"P{n}" for n in preview_nums]
                    st.caption(f"Se crearán automáticamente: {', '.join(preview_names)}")

                    if st.button("Crear pickeador(es)", key="adm_create_picker_btn"):
                        created_names = []
                        try:
                            for n in _next_picker_numbers(picker_names, int(qty_new_pickers)):
                                pname = f"P{n}"
                                cur = conn.execute("INSERT OR IGNORE INTO pickers (name) VALUES (?)", (pname,))
                                if getattr(cur, "rowcount", 0) > 0:
                                    created_names.append(pname)
                            conn.commit()

                            if created_names:
                                st.success(f"Creados: {', '.join(created_names)}. Ya puedes repartir tareas.")
                                st.rerun()
                            else:
                                st.warning("No se creó ningún pickeador nuevo.")
                        except Exception as e:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            st.error(f"No se pudieron crear pickeadores: {e}")

                try:
                    c.execute("SELECT id, name FROM pickers ORDER BY id")
                    pickers_rows_live = c.fetchall()
                except Exception:
                    pickers_rows_live = pickers_rows

                picker_names_live = [pname for _, pname in pickers_rows_live]
                picker_name_to_id_live = {pname: int(pid) for pid, pname in pickers_rows_live}
                other_picker_names = [n for n in picker_names_live if n != src_name]

                if not other_picker_names:
                    st.warning("No hay pickeadores destino disponibles. Crea uno arriba para repartir las tareas.")
                else:
                    dests = st.multiselect("Pickeadores destino", other_picker_names, default=other_picker_names, key="adm_reassign_dests")
                    if not dests:
                        st.info("Elige al menos un pickeador destino.")
                    else:
                        st.caption("Define cuántas tareas mover a cada destino. La suma debe ser igual a las seleccionadas.")
                        dest_counts = {}
                        cols = st.columns(min(4, len(dests)))
                        for i, dname in enumerate(dests):
                            with cols[i % len(cols)]:
                                dest_counts[dname] = st.number_input(f"{dname}", min_value=0, max_value=n_sel, value=0, step=1, key=f"adm_reassign_cnt_{dname}")

                        total_move = int(sum(dest_counts.values()))
                        st.write(f"**Total a mover según reparto:** {total_move} / {n_sel}")

                        def _new_ot_code(prefix: str = "LIB"):
                            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                            rnd = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))
                            return f"{prefix}-{ts}-{rnd}"

                        if st.button("Repartir tareas seleccionadas", type="primary"):
                            if total_move != n_sel:
                                sfx_emit("ERR")
                                st.error("La suma por destino debe ser exactamente igual al número de tareas seleccionadas.")
                            else:
                                # Reparto determinístico: por id ascendente
                                selected_ids_sorted = sorted(selected_ids)
                                cursor = 0
                                moved_total = 0
                                try:
                                    for dname, cnt in dest_counts.items():
                                        cnt = int(cnt)
                                        if cnt <= 0:
                                            continue
                                        chunk = selected_ids_sorted[cursor:cursor+cnt]
                                        cursor += cnt
                                        if not chunk:
                                            continue
                                        dest_id = picker_name_to_id.get(dname)
                                        # Crear OT nueva para el destino
                                        new_code = _new_ot_code("LIB")
                                        now_iso_ts = now_iso()
                                        c.execute(
                                            "INSERT INTO picking_ots (ot_code, picker_id, status, created_at, closed_at) VALUES (?,?,?,?,NULL)",
                                            (new_code, dest_id, "OPEN", now_iso_ts)
                                        )
                                        new_ot_id = int(c.lastrowid)

                                        qmarks = ",".join(["?"] * len(chunk))
                                        c.execute(f"UPDATE picking_tasks SET ot_id=? WHERE id IN ({qmarks})", [new_ot_id] + chunk)
                                        moved_total += len(chunk)

                                    conn.commit()
                                    sfx_emit("OK")
                                    st.success(f"Listo: movidas {moved_total} tareas desde {src_name}. Se crearon OTs 'LIB-*' para los destinos.")
                                    st.rerun()
                                except Exception as e:
                                    conn.rollback()
                                    sfx_emit("ERR")
                                    st.error(f"No se pudo repartir: {e}")
            else:
                st.caption("Marca al menos una tarea para habilitar el reparto.")


    st.subheader("Incidencias")
    c.execute("""
        SELECT po.ot_code, pk.name, pi.sku_ml, pi.qty_total, pi.qty_picked, pi.qty_missing, pi.reason, pi.note, pi.created_at
        FROM picking_incidences pi
        JOIN picking_ots po ON po.id = pi.ot_id
        JOIN pickers pk ON pk.id = po.picker_id
        ORDER BY pi.created_at DESC
    """)
    inc_rows = c.fetchall()
    if inc_rows:
        df_inc = pd.DataFrame(inc_rows, columns=["OT","Picker","SKU","Solicitado","Pickeado","Faltante","Motivo","Nota","Hora"])
        # Producto (título técnico): maestro si existe; si no, SKU
        try:
            df_inc["Producto"] = df_inc["SKU"].apply(lambda x: (master_raw_title_lookup(MASTER_FILE, str(x).strip()) or str(x).strip()))
        except Exception:
            df_inc["Producto"] = df_inc["SKU"].astype(str)

        df_inc["Hora"] = df_inc["Hora"].apply(to_chile_display)
        # Orden de columnas más útil
        try:
            df_inc = df_inc[["OT","Picker","SKU","Producto","Solicitado","Pickeado","Faltante","Motivo","Nota","Hora"]]
        except Exception:
            pass
        st.dataframe(df_inc, use_container_width=True, hide_index=True)
    else:
        st.info("Sin incidencias en la corrida actual.")

    st.divider()
    st.subheader("Acciones")

    if "confirm_reset" not in st.session_state:
        st.session_state.confirm_reset = False

    if not st.session_state.confirm_reset:
        if st.button("Reiniciar corrida (BORRA TODO)"):
            st.session_state.confirm_reset = True
            st.warning("⚠️ Esto borrará TODA la información (OTs, tareas, incidencias y ventas). Confirma abajo.")
            st.rerun()
    else:
        st.error("CONFIRMACIÓN: se borrarán TODOS los datos del sistema.")
        colA, colB = st.columns(2)
        with colA:
            if st.button("✅ Sí, borrar todo y reiniciar"):
                c.execute("DELETE FROM picking_tasks;")
                c.execute("DELETE FROM picking_incidences;")
                c.execute("DELETE FROM ot_orders;")
                c.execute("DELETE FROM picking_ots;")
                c.execute("DELETE FROM pickers;")
                c.execute("DELETE FROM order_items;")
                c.execute("DELETE FROM orders;")
                conn.commit()
                st.session_state.confirm_reset = False
                st.success("Sistema reiniciado (todo borrado).")
                st.session_state.pop("selected_picker", None)
                st.rerun()
        with colB:
            if st.button("Cancelar"):
                st.session_state.confirm_reset = False
                st.info("Reinicio cancelado.")
                st.rerun()

    conn.close()

def _s2_now_iso():
    # Timestamp en hora Chile con offset
    if CL_TZ is not None:
        return datetime.now(CL_TZ).isoformat(timespec="seconds")
    return datetime.now().isoformat(timespec="seconds")

def _s2_create_tables():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS s2_manifests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL DEFAULT 'ACTIVE',
        created_at TEXT NOT NULL
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS s2_files (
        manifest_id INTEGER PRIMARY KEY,
        control_pdf BLOB,
        labels_txt BLOB,
        control_name TEXT,
        labels_name TEXT,
        updated_at TEXT NOT NULL
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS s2_page_assign (
        manifest_id INTEGER NOT NULL,
        page_no INTEGER NOT NULL,
        mesa INTEGER NOT NULL,
        PRIMARY KEY (manifest_id, page_no)
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS s2_mesa_status (
        manifest_id INTEGER NOT NULL,
        mesa INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'OPEN',
        closed_at TEXT,
        PRIMARY KEY (manifest_id, mesa)
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS s2_sales (
        manifest_id INTEGER NOT NULL,
        sale_id TEXT NOT NULL,
        shipment_id TEXT,
        page_no INTEGER NOT NULL,
        row_no INTEGER NOT NULL DEFAULT 0,
        mesa INTEGER,
        status TEXT NOT NULL DEFAULT 'NEW',
        opened_at TEXT,
        closed_at TEXT,
        PRIMARY KEY (manifest_id, sale_id)
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS s2_items (
        manifest_id INTEGER NOT NULL,
        sale_id TEXT NOT NULL,
        sku TEXT NOT NULL,
        description TEXT,
        qty INTEGER NOT NULL,
        picked INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'PENDING',
        PRIMARY KEY (manifest_id, sale_id, sku)
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS s2_labels (
        manifest_id INTEGER NOT NULL,
        shipment_id TEXT NOT NULL,
        raw TEXT,
        PRIMARY KEY (manifest_id, shipment_id)
    );""")

    # --- Migraciones suaves (SQLite) ---
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(s2_sales);").fetchall()]
        if "row_no" not in cols:
            c.execute("ALTER TABLE s2_sales ADD COLUMN row_no INTEGER NOT NULL DEFAULT 0;")
        if "pack_id" not in cols:
            c.execute("ALTER TABLE s2_sales ADD COLUMN pack_id TEXT;")
        if "customer" not in cols:
            c.execute("ALTER TABLE s2_sales ADD COLUMN customer TEXT;")
        if "destino" not in cols:
            c.execute("ALTER TABLE s2_sales ADD COLUMN destino TEXT;")
        if "comuna" not in cols:
            c.execute("ALTER TABLE s2_sales ADD COLUMN comuna TEXT;")
        if "ciudad_destino" not in cols:
            c.execute("ALTER TABLE s2_sales ADD COLUMN ciudad_destino TEXT;")
    except Exception:
        pass

    # s2_items: guardar confirm_mode para trazabilidad (ej: MANUAL_NO_EAN)
    try:
        cols_i = [r[1] for r in c.execute("PRAGMA table_info(s2_items);").fetchall()]
        if "confirm_mode" not in cols_i:
            c.execute("ALTER TABLE s2_items ADD COLUMN confirm_mode TEXT;")
        if "updated_at" not in cols_i:
            c.execute("ALTER TABLE s2_items ADD COLUMN updated_at TEXT;")
    except Exception:
        pass


    # s2_mesa_status: compatibilidad / migración suave
    try:
        cols_ms = [r[1] for r in c.execute("PRAGMA table_info(s2_mesa_status);").fetchall()]
        if "status" not in cols_ms:
            c.execute("ALTER TABLE s2_mesa_status ADD COLUMN status TEXT NOT NULL DEFAULT 'OPEN';")
        if "closed_at" not in cols_ms:
            c.execute("ALTER TABLE s2_mesa_status ADD COLUMN closed_at TEXT;")
    except Exception:
        pass

    # Mapa Pack ID -> Shipment ID (necesario para Colecta)
    c.execute("""CREATE TABLE IF NOT EXISTS s2_pack_ship (
        manifest_id INTEGER NOT NULL,
        pack_id TEXT NOT NULL,
        shipment_id TEXT NOT NULL,
        PRIMARY KEY (manifest_id, pack_id)
    );""")

    conn.commit()
    conn.close()

def _s2_get_active_manifest_id():
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM s2_manifests WHERE status='ACTIVE' ORDER BY id DESC LIMIT 1;")
    row = c.fetchone()
    if row:
        mid = int(row[0])
        conn.close()
        return mid
    c.execute("INSERT INTO s2_manifests(status, created_at) VALUES('ACTIVE', ?);", (_s2_now_iso(),))
    mid = int(c.lastrowid)
    conn.commit()
    conn.close()
    return mid


def _s2_manifest_files_state(mid: int) -> dict:
    """Return whether the active manifest already has Control and/or Labels loaded."""
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    row = c.execute(
        "SELECT (control_pdf IS NOT NULL AND length(control_pdf)>0) AS has_control, "
        "       (labels_txt  IS NOT NULL AND length(labels_txt)>0)  AS has_labels "
        "FROM s2_files WHERE manifest_id=?;",
        (mid,),
    ).fetchone()
    conn.close()
    if not row:
        return {"has_control": False, "has_labels": False}
    return {"has_control": bool(row[0]), "has_labels": bool(row[1])}

def _s2_close_manifest(mid: int):
    """Marks current manifest as DONE (archived)."""
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE s2_manifests SET status='DONE' WHERE id=?;", (int(mid),))
    conn.commit()
    conn.close()

def _s2_create_new_manifest(status: str = 'ACTIVE') -> int:
    """Crea un lote/manifiesto nuevo.
    En la operación actual, confirmar carga = entrar directo a producción.
    """
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    c.execute("INSERT INTO s2_manifests(status, created_at) VALUES(?, ?);", (str(status or 'ACTIVE').upper(), _s2_now_iso()))
    mid = int(c.lastrowid)
    conn.commit()
    conn.close()
    return mid


def _s2_manifest_has_content(mid: int) -> bool:
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    sales = int(c.execute("SELECT COUNT(*) FROM s2_sales WHERE manifest_id=?;", (int(mid),)).fetchone()[0] or 0)
    items = int(c.execute("SELECT COUNT(*) FROM s2_items WHERE manifest_id=?;", (int(mid),)).fetchone()[0] or 0)
    labels = int(c.execute("SELECT COUNT(*) FROM s2_labels WHERE manifest_id=?;", (int(mid),)).fetchone()[0] or 0)
    pages = int(c.execute("SELECT COUNT(*) FROM s2_page_assign WHERE manifest_id=?;", (int(mid),)).fetchone()[0] or 0)
    files = c.execute("SELECT control_name, labels_name FROM s2_files WHERE manifest_id=?;", (int(mid),)).fetchone()
    conn.close()
    has_files = bool(files and ((files[0] and str(files[0]).strip()) or (files[1] and str(files[1]).strip())))
    return any([sales, items, labels, pages, has_files])


def _s2_list_work_manifests():
    return _s2_list_open_manifests()


def _s2_list_open_manifests():
    _s2_migrate_staged_to_active()
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT id, status, created_at FROM s2_manifests WHERE status='ACTIVE' ORDER BY created_at, id;").fetchall()
    out = []
    for mid, status, created_at in rows:
        mesas = [int(r[0]) for r in c.execute("SELECT DISTINCT mesa FROM s2_page_assign WHERE manifest_id=? ORDER BY mesa;", (int(mid),)).fetchall() if r[0] is not None]
        sales_total = int(c.execute("SELECT COUNT(*) FROM s2_sales WHERE manifest_id=?;", (int(mid),)).fetchone()[0] or 0)
        sales_pending = int(c.execute("SELECT COUNT(*) FROM s2_sales WHERE manifest_id=? AND status!='DONE';", (int(mid),)).fetchone()[0] or 0)
        items_total = int(c.execute("SELECT COUNT(*) FROM s2_items WHERE manifest_id=?;", (int(mid),)).fetchone()[0] or 0)
        items_pending = int(c.execute("SELECT COUNT(*) FROM s2_items WHERE manifest_id=? AND status='PENDING';", (int(mid),)).fetchone()[0] or 0)
        files = c.execute("SELECT control_name, labels_name FROM s2_files WHERE manifest_id=?;", (int(mid),)).fetchone()
        out.append({
            'id': int(mid),
            'status': str(status),
            'created_at': str(created_at or ''),
            'mesas': mesas,
            'mesas_text': _s2_mesas_text_from_list(mesas),
            'lot_label': '',
            'sales_total': sales_total,
            'sales_pending': sales_pending,
            'items_total': items_total,
            'items_pending': items_pending,
            'control_name': str(files[0] or '') if files else '',
            'labels_name': str(files[1] or '') if files else '',
            'has_content': bool(sales_total or items_total or mesas or (files and ((files[0] and str(files[0]).strip()) or (files[1] and str(files[1]).strip())))),
        })
    conn.close()
    for mf in out:
        mf['lot_label'] = _s2_lot_label(int(mf['id']))
    return out


def _s2_get_locked_mesas(exclude_mid: int | None = None):
    _s2_migrate_staged_to_active()
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    params = []
    where_ex = ""
    if exclude_mid is not None:
        where_ex = " AND p.manifest_id<>?"
        params.append(int(exclude_mid))
    rows = c.execute(f"""SELECT DISTINCT p.mesa
                        FROM s2_page_assign p
                        JOIN s2_manifests m ON m.id = p.manifest_id
                        LEFT JOIN s2_mesa_status ms ON ms.manifest_id = p.manifest_id AND ms.mesa = p.mesa
                        WHERE m.status='ACTIVE'
                          AND COALESCE(ms.status, 'OPEN')='OPEN'{where_ex}
                        ORDER BY p.mesa;""", tuple(params)).fetchall()
    conn.close()
    return [int(r[0]) for r in rows if r[0] is not None]

def _s2_next_global_mesa_block(default_count: int = 3, exclude_mid: int | None = None):
    locked = _s2_get_locked_mesas(exclude_mid=exclude_mid)
    start = int(max(locked) + 1) if locked else 1
    return start, max(1, int(default_count or 1))


def _s2_find_manifest_by_mesa(mesa: int):
    _s2_migrate_staged_to_active()
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    row = c.execute("""SELECT p.manifest_id
                       FROM s2_page_assign p
                       JOIN s2_manifests m ON m.id = p.manifest_id
                       LEFT JOIN s2_mesa_status ms ON ms.manifest_id = p.manifest_id AND ms.mesa = p.mesa
                       WHERE m.status='ACTIVE' AND p.mesa=? AND COALESCE(ms.status, 'OPEN')='OPEN'
                       ORDER BY p.manifest_id DESC
                       LIMIT 1;""", (int(mesa),)).fetchone()
    conn.close()
    return int(row[0]) if row else None

def _s2_delete_manifest(mid: int):
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    mid = int(mid)
    for tbl in ['s2_dispatch', 's2_pack_ship', 's2_packing', 's2_labels', 's2_items', 's2_sales', 's2_page_assign', 's2_files']:
        c.execute(f"DELETE FROM {tbl} WHERE manifest_id=?;", (mid,))
    c.execute("DELETE FROM s2_manifests WHERE id=?;", (mid,))
    conn.commit()
    conn.close()


def _s2_migrate_staged_to_active():
    """Compatibilidad: cualquier manifiesto STAGED con contenido pasa a ACTIVE.
    Los STAGED vacíos de versiones anteriores se ignoran.
    """
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT id FROM s2_manifests WHERE status='STAGED';").fetchall()
    changed = False
    for (mid,) in rows:
        mid = int(mid)
        has_content = bool(c.execute("SELECT 1 FROM s2_sales WHERE manifest_id=? LIMIT 1;", (mid,)).fetchone())
        has_content = has_content or bool(c.execute("SELECT 1 FROM s2_items WHERE manifest_id=? LIMIT 1;", (mid,)).fetchone())
        has_content = has_content or bool(c.execute("SELECT 1 FROM s2_page_assign WHERE manifest_id=? LIMIT 1;", (mid,)).fetchone())
        has_content = has_content or bool(c.execute("SELECT 1 FROM s2_labels WHERE manifest_id=? LIMIT 1;", (mid,)).fetchone())
        has_content = has_content or bool(c.execute("SELECT 1 FROM s2_files WHERE manifest_id=? AND ((control_name IS NOT NULL AND TRIM(control_name)!='') OR (labels_name IS NOT NULL AND TRIM(labels_name)!=''));", (mid,)).fetchone())
        if has_content:
            c.execute("UPDATE s2_manifests SET status='ACTIVE' WHERE id=?;", (mid,))
            changed = True
    if changed:
        conn.commit()
    conn.close()


def _s2_find_empty_active_manifest():
    _s2_migrate_staged_to_active()
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT id FROM s2_manifests WHERE status='ACTIVE' ORDER BY id DESC;").fetchall()
    out = None
    for (mid,) in rows:
        mid = int(mid)
        if not _s2_manifest_has_content(mid):
            out = mid
            break
    conn.close()
    return out


def _s2_mesas_text_from_list(mesas: list[int]) -> str:
    mesas = sorted({int(m) for m in (mesas or []) if m is not None})
    if not mesas:
        return "Sin mesas"
    if len(mesas) == 1:
        return f"Mesa {mesas[0]}"
    return f"Mesas {mesas[0]} a {mesas[-1]}"


def _s2_lot_label(mid: int) -> str:
    _s2_migrate_staged_to_active()
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT id FROM s2_manifests WHERE status='ACTIVE' ORDER BY created_at, id;").fetchall()
    active_ids = [int(r[0]) for r in rows]
    idx = active_ids.index(int(mid)) + 1 if int(mid) in active_ids else None
    mesas = [int(r[0]) for r in c.execute("SELECT DISTINCT mesa FROM s2_page_assign WHERE manifest_id=? ORDER BY mesa;", (int(mid),)).fetchall() if r[0] is not None]
    conn.close()
    block = _s2_mesas_text_from_list(mesas)
    return f"Lote {idx} · {block}" if idx else block



def _s2_zpl_underscore_decode(s: str) -> str:
    """
    ZPL suele venir con secuencias tipo _C3_A9 para representar bytes UTF-8.
    Esto las convierte a texto normal (P_C3_A9rez -> Pérez).
    """
    if not s:
        return ""
    import re
    out = []
    buf = bytearray()
    i = 0
    while i < len(s):
        if s[i] == "_" and i + 2 < len(s):
            m = re.match(r"_([0-9A-Fa-f]{2})", s[i:])
            if m:
                buf.append(int(m.group(1), 16))
                i += 3
                continue
        if buf:
            try:
                out.append(buf.decode("utf-8", errors="ignore"))
            except Exception:
                out.append(buf.decode("latin1", errors="ignore"))
            buf = bytearray()
        out.append(s[i])
        i += 1
    if buf:
        try:
            out.append(buf.decode("utf-8", errors="ignore"))
        except Exception:
            out.append(buf.decode("latin1", errors="ignore"))
    return "".join(out)

def _s2_parse_label_raw_info(raw: str):
    """Extrae info visible de una etiqueta (Flex/Colecta) desde el texto raw/ZPL.

    Campos:
      - destinatario
      - domicilio (alias: direccion)
      - comuna (FLEX)
      - ciudad_destino (COLECTA y a veces FLEX)
    """
    import re
    if not raw:
        return {}
    s = str(raw).replace("\r", "\n")
    s = _s2_zpl_underscore_decode(s)
    info = {}

    # FLEX: "Destinatario: Nombre (user)"
    m = re.search(r"Destinatario\s*:\s*([^\n\^]{3,140})", s, flags=re.IGNORECASE)
    if m:
        info["destinatario"] = m.group(1).strip()

    # COLECTA: línea tipo "NOMBRE (USER)"
    if "destinatario" not in info:
        m = re.search(r"^\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ0-9 .,'-]{3,120})\s*\([^\n]{2,80}\)\s*$", s, flags=re.M)
        if m:
            info["destinatario"] = m.group(1).strip()

    # Heurística extra (FLEX/COLECTA): el destinatario suele ser la línea inmediatamente anterior a Domicilio/Direccion
    if "destinatario" not in info:
        m_dom = re.search(r"(Domicilio|Direccion)\s*:\s*([^\n\^]{3,200})", s, flags=re.IGNORECASE)
        if m_dom:
            before = s[:m_dom.start()].splitlines()

            def _fd_content(line: str) -> str:
                mm = re.search(r"\^FD(.*?)(?:\^FS|$)", line)
                return (mm.group(1) if mm else line).strip()

            prev = ""
            for ln in reversed(before[-12:]):  # mirar hacia atrás pocas líneas
                ln = (ln or "").strip()
                if not ln:
                    continue
                cand = _fd_content(ln)
                cand = re.sub(r"\s*\([^\)]{2,120}\)\s*$", "", cand).strip()  # recortar (USER)
                low = cand.lower()
                if any(k in low for k in ["pack id", "venta", "envio", "envío", "shipment", "codigo", "código", "rut", "telefono", "teléfono", "receiver zone", "domicilio", "direccion"]):
                    continue
                # requiere letras y largo razonable
                if len(cand) < 3 or len(cand) > 140:
                    continue
                if not re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", cand):
                    continue
                prev = cand
                break

            if prev:
                info["destinatario"] = prev

# Dirección / Domicilio (Flex: Direccion, Colecta: Domicilio)
    m = re.search(r"(Domicilio|Direccion)\s*:\s*([^\n\^]{3,200})", s, flags=re.IGNORECASE)
    if m:
        info["domicilio"] = m.group(2).strip()
        # alias por compatibilidad (hay pantallas que buscan 'direccion')
        info["direccion"] = info["domicilio"]

    # Ciudad de destino (Colecta y a veces Flex)
    m = re.search(r"Ciudad\s+de\s+destino\s*:\s*([^\n\^]{3,160})", s, flags=re.IGNORECASE)
    if m:
        info["ciudad_destino"] = m.group(1).strip()

    # FLEX: comuna (a veces viene implícita en Domicilio "... , Comuna")
    m = re.search(r"\bComuna\b\s*:\s*([^\n\^]{2,80})", s, flags=re.IGNORECASE)
    if m:
        info["comuna"] = m.group(1).strip()
    elif info.get("domicilio"):
        # Heurística: tomar la última sección después de coma
        dom = info.get("domicilio", "")
        if "," in dom:
            comuna = dom.split(",")[-1].strip()
            # evita capturar basura
            if comuna and len(comuna) <= 60 and re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", comuna):
                info["comuna"] = comuna

    # Limpieza: jamás aceptar el mensaje promocional como destino
    promo_re = re.compile(r"\bDespacha\s+tu[s]?\s+productos\b", re.I)
    for k in list(info.keys()):
        v = (info.get(k) or "").strip()
        if v and promo_re.search(v):
            info.pop(k, None)

    return info
def _s2_get_label_raw(mid:int, shipment_id:str):
    conn=get_conn()
    c=conn.cursor()
    row=c.execute("SELECT raw FROM s2_labels WHERE manifest_id=? AND shipment_id=?;", (mid, str(shipment_id))).fetchone()
    conn.close()
    return row[0] if row else ""

def _s2_extract_shipment_id(scan_raw: str):
    """Lee el identificador desde el escaneo de etiqueta.

    - Flex: a veces viene como JSON con {"id":"..."}
    - Colecta: puede venir como shipment (10-15 dígitos, suele empezar por 46)
      o como Pack ID (más largo, 10-20 dígitos)

    Devuelve el mejor candidato numérico (string) o None.
    """
    import re, json
    if not scan_raw:
        return None
    s = str(scan_raw).strip()

    # 1) JSON (Flex QR)
    if s.startswith("{") and "id" in s:
        try:
            obj = json.loads(s)
            sid = obj.get("id")
            if sid and re.fullmatch(r"\d{8,20}", str(sid)):
                return str(sid)
        except Exception:
            pass

    # 2) Números: extraer todos los grupos (incluye prefijos tipo >: )
    nums = re.findall(r"(\d{6,20})", s)
    if not nums:
        return None

    # Preferir shipment_id típico (10-15, empieza por 46) si existe
    ship_like = [n for n in nums if 10 <= len(n) <= 15]
    if ship_like:
        ship_like = sorted(ship_like, key=lambda x: (0 if x.startswith("46") else 1, -len(x)))
        return ship_like[0]

    # Si no, devolver el más largo (útil si escanean Pack ID)
    nums_sorted = sorted(nums, key=lambda x: -len(x))
    return nums_sorted[0]



def _s2_parse_control_pdf(pdf_bytes: bytes):
    """Parse Control.pdf (Flex/Colecta) into sales with items.

    Importante (Colecta): el Control a veces NO trae shipment_id al inicio de línea.
    Por eso este parser NO exige shipment_id para contar ventas; lo completa luego
    usando Etiquetas (por Pack ID o por shipment_id cuando venga en el Control).

    Returns: list of dicts:
      {page_no:int, shipment_id:str|None, sale_id:str, pack_id:str|None,
       customer:str|None, destino:str|None,
       items:[{sku:str, qty:int}]}
    """
    import io, re, pdfplumber

    def ship_from_line(s: str):
        # Flex suele venir como número al inicio (p.ej. 4636...)
        m = re.match(r"^(46\d{8,13})\b", (s or "").strip())  # evita capturar códigos no-shipment (ej: 30119784...)
        return m.group(1) if m else None

    def sale_from_line(s: str):
        m = re.search(r"\bVenta\s*:\s*(\d{10,20})\b", s or "", flags=re.IGNORECASE)
        return m.group(1) if m else None

    def pack_from_line(s: str):
        m = re.search(r"\bPack\s*ID\s*:\s*(\d{10,20})\b", s or "", flags=re.IGNORECASE)
        return m.group(1) if m else None

    def skus_from_line(s: str):
        # SKU puede venir con guiones/letras en algunos casos internos, pero en Control suele ser numérico
        return re.findall(r"\bSKU\s*:\s*([0-9A-Za-z_-]{6,20})\b", s or "", flags=re.IGNORECASE)

    def qty_from_line(s: str):
        m = re.search(r"\bCantidad\s*:\s*(\d+)\b", s or "", flags=re.IGNORECASE)
        return int(m.group(1)) if m else None

    def looks_like_name(s: str):
        s = (s or "").strip()
        if not s or len(s) > 70:
            return False
        # No debe contener dígitos (evita capturar líneas tipo Venta/SKU/IDs)
        if re.search(r"\d", s):
            return False
        # Evitar líneas de atributos
        if re.search(r"\b(color|acabado|modelo|di[aá]metro|voltaje|dise[nñ]o|tipo)\b\s*:", s, flags=re.I):
            return False
        # Evitar palabras típicas del control
        if re.search(r"\b(venta|sku|cantidad|pack|env[ií]o|shipment)\b", s, flags=re.I):
            return False
        # Debe tener letras
        return bool(re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", s))

    def customer_from_line(s: str):
        # Preferimos SOLO etiquetas explícitas para evitar confundir con descripción de producto.
        m = re.search(r"\b(Cliente|Comprador|Destinatario)\s*:\s*(.+)$", s or "", flags=re.I)
        if not m:
            return None
        name = (m.group(2) or "").strip()
        name = re.sub(r"\s{2,}", " ", name)
        return name[:70] if name else None

    def destino_from_line(s: str):
        # En el Control suele venir como "Despacha ..." o "Despacha:".
        m = re.match(r"^Despacha\s*:\s*(.+)$", (s or "").strip(), flags=re.I)
        if not m:
            return None
        dest = (m.group(1) or "").strip()
        dest = re.sub(r"\s{2,}", " ", dest)
        return dest[:80] if dest else None

    sales = []
    cur = {"page_no": None, "shipment_id": None, "sale_id": None, "pack_id": None,
           "customer": None, "destino": None, "items": []}
    sku_queue = []

    def flush():
        nonlocal cur, sku_queue
        if cur.get("sale_id") and cur.get("items"):
            # sale_id + items es suficiente para contar venta
            sales.append(cur)
        cur = {"page_no": None, "shipment_id": None, "sale_id": None, "pack_id": None,
               "customer": None, "destino": None, "items": []}
        sku_queue = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for pidx, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln and ln.strip()]
            for ln in lines:
                low = ln.lower()
                # Destino (NO saltar, guardarlo)
                dest = destino_from_line(ln)
                if dest and not cur.get("destino"):
                    cur["destino"] = dest

                # Cliente explícito
                cust = customer_from_line(ln)
                if cust and not cur.get("customer"):
                    cur["customer"] = cust

                # Algunas líneas de cabecera que no aportan
                if low.startswith("identifi"):
                    continue

                # Flex shipment id en línea
                ship = ship_from_line(ln)
                if ship:
                    if cur.get("shipment_id") and ship != cur.get("shipment_id") and cur.get("sale_id"):
                        flush()
                    if not cur.get("shipment_id"):
                        cur["shipment_id"] = ship
                        if not cur.get("page_no"):
                            cur["page_no"] = pidx

                # Pack ID (ojo: en Colecta a veces Pack+SKU viene ANTES de "Venta:",
                # así que si aparece un Pack ID nuevo y ya tenemos una venta completa, hacemos flush aquí)
                pid = pack_from_line(ln)
                if pid:
                    if cur.get("sale_id") and cur.get("items"):
                        if (cur.get("pack_id") and pid != cur.get("pack_id")) or (cur.get("pack_id") is None):
                            flush()
                    cur["pack_id"] = pid
                    if not cur.get("page_no"):
                        cur["page_no"] = pidx


                # Venta (si cambia, flush)
                sid = sale_from_line(ln)
                if sid:
                    if cur.get("sale_id") and sid != cur.get("sale_id") and cur.get("items"):
                        flush()
                    cur["sale_id"] = sid
                    if not cur.get("page_no"):
                        cur["page_no"] = pidx

                # SKU en línea
                skus = skus_from_line(ln)
                if skus:
                    sku_queue.extend(skus)

                # Cantidad: asigna a primer SKU pendiente
                q = qty_from_line(ln)
                if q is not None:
                    if sku_queue:
                        sku = sku_queue.pop(0)
                        cur["items"].append({"sku": sku, "qty": int(q)})
                else:
                    # Cliente en línea sola (solo si aún NO hemos visto SKUs/ítems de esta venta)
                    if (cur.get("sale_id") and not cur.get("customer")
                            and (not sku_queue) and (not cur.get("items"))
                            and ("sku" not in low) and ("cantidad" not in low)
                            and (not low.startswith("despacha"))
                            and looks_like_name(ln)):
                        cur["customer"] = ln[:70]

    flush()
    return sales



def _s2_clean_person_text(s: str, max_len: int):
    """Limpieza defensiva para campos de persona/destino extraídos del PDF."""
    t = (s or "").strip()
    if not t:
        return None
    # Quitar fragmentos típicos que vienen pegados por extracción PDF
    t = re.sub(r"\b(Venta|SKU|Cantidad|Pack\s*ID|Env[ií]o)\s*:\s*\S+", "", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t).strip(" -•|,;")
    if not t:
        return None
    return t[:max_len]
def _s2_parse_labels_txt(raw_bytes: bytes):
    """Parsea etiquetas TXT/ZPL de Flex y Colecta.

    Devuelve:
      - pack_to_ship: dict {pack_id(str) -> shipment_id(str)}
      - sale_to_ship: dict {sale_id(str) -> shipment_id(str)}  (fallback cuando no hay Pack ID en Control)
      - shipment_ids: sorted list de shipment_id detectados

    Nota: En Colecta el Pack ID / Venta suelen venir PARTIDOS en dos ^FD:
        ^FDPack ID: 20000^FS  y luego ^FD1128....^FS  -> 200001128....
        ^FDVenta: 20000^FS    y luego ^FD1498....^FS  -> 200001498....
    """
    import re

    try:
        txt = raw_bytes.decode("utf-8", errors="ignore")
    except Exception:
        txt = str(raw_bytes)

    # separar etiquetas por bloque ^XA ... ^XZ
    blocks = re.split(r"\^XA", txt)
    pack_to_ship = {}
    sale_to_ship = {}
    shipment_ids = set()

    def clean_num(s):
        return re.sub(r"\D", "", s or "")

    def rebuild_split_id(kind: str, b: str):
        """
        kind: 'Pack' o 'Venta'
        Busca:
          - Completo:  kind ID: 2000011363....
          - Partido:   kind ID: 20000  + siguiente ^FD 11363....
        """
        kind_re = kind
        full = None

        m_full = re.search(rf"{kind_re}\s*(?:ID)?\s*:\s*(\d{{10,20}})", b, flags=re.I)
        if m_full:
            full = clean_num(m_full.group(1))

        if not full:
            m_part = re.search(rf"{kind_re}\s*(?:ID)?\s*:\s*(\d{{4,10}})\s*\^FS", b, flags=re.I)
            if m_part:
                head = clean_num(m_part.group(1))
                tailm = re.search(r"\^FD\s*([0-9 ]{6,20})\s*\^FS", b[m_part.end():])
                if tailm:
                    tail = clean_num(tailm.group(1))
                    cand = head + tail
                    if 10 <= len(cand) <= 20:
                        full = cand
        return full

    for b in blocks:
        if not b.strip():
            continue

        pack_full = rebuild_split_id("Pack", b)
        sale_full = rebuild_split_id("Venta", b)

        # shipment id: preferir JSON con "id":"4638..."
        ship = None
        jm = re.search(r"\"id\"\s*:\s*\"(\d{8,15})\"", b)
        if jm:
            ship = jm.group(1)

        if not ship:
            # buscar números candidatos, priorizando 10-15 dígitos y que empiecen por 46
            nums = re.findall(r"\b\d{10,15}\b", b)
            if nums:
                nums_sorted = sorted(nums, key=lambda x: (0 if x.startswith("46") else 1, -len(x)))
                ship = nums_sorted[0]

        if ship:
            shipment_ids.add(ship)
            if pack_full:
                pack_to_ship[str(pack_full)] = str(ship)
            if sale_full:
                sale_to_ship[str(sale_full)] = str(ship)

    return pack_to_ship, sale_to_ship, sorted(shipment_ids)

def _s2_upsert_control(mid: int, pdf_name: str, pdf_bytes: bytes):
    pages_sales = _s2_parse_control_pdf(pdf_bytes)
    conn = get_conn()
    c = conn.cursor()
    # store file
    c.execute("""INSERT INTO s2_files(manifest_id, control_pdf, control_name, updated_at)
                 VALUES(?, ?, ?, ?)
                 ON CONFLICT(manifest_id) DO UPDATE SET
                    control_pdf=excluded.control_pdf,
                    control_name=excluded.control_name,
                    updated_at=excluded.updated_at;""", (mid, pdf_bytes, pdf_name, _s2_now_iso()))
    # clear previous parsed sales/items
    c.execute("DELETE FROM s2_items WHERE manifest_id=?;", (mid,))
    c.execute("DELETE FROM s2_sales WHERE manifest_id=?;", (mid,))

    n_sales = 0
    page_counters = {}
    for s in pages_sales:
        sale_id = str(s.get("sale_id") or "")
        if not sale_id:
            continue
        n_sales += 1
        shipment_id = s.get("shipment_id")
        page_no = int(s.get("page_no") or 1)
        pack_id = s.get("pack_id")
        row_no = int(page_counters.get(page_no, 0) + 1)
        page_counters[page_no] = row_no
        customer = _s2_clean_person_text(s.get("customer"), 70)
        destino = _s2_clean_person_text(s.get("destino"), 80)

        c.execute("""INSERT INTO s2_sales(manifest_id, sale_id, shipment_id, page_no, row_no, status, pack_id, customer, destino)
                     VALUES(?,?,?,?,?, 'NEW', ?, ?, ?)
                     ON CONFLICT(manifest_id, sale_id) DO UPDATE SET
                        shipment_id=excluded.shipment_id,
                        page_no=excluded.page_no,
                        status='NEW',
                        mesa=NULL,
                        opened_at=NULL,
                        closed_at=NULL,
                        pack_id=excluded.pack_id,
                        customer=excluded.customer,
                        destino=excluded.destino;""",
                  (mid, sale_id, (str(shipment_id) if shipment_id else None), page_no, row_no,
                   (str(pack_id) if pack_id else None), (str(customer) if customer else None),
                   (str(destino) if destino else None)))

        for it in s.get("items", []):
            try:
                sku = str(it.get("sku"))
                qty = int(it.get("qty") or 0)
            except Exception:
                continue
            if not sku or qty <= 0:
                continue
            c.execute("""INSERT INTO s2_items(manifest_id, sale_id, sku, description, qty, picked, status)
                         VALUES(?,?,?,?,?,0,'PENDING')
                         ON CONFLICT(manifest_id, sale_id, sku) DO UPDATE SET
                            description=excluded.description,
                            qty=excluded.qty,
                            picked=0,
                            status='PENDING';""", (mid, sale_id, sku, it.get("desc",""), qty))

    conn.commit()
    conn.close()
    return n_sales



def _s2_get_max_page(mid: int) -> int:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT MAX(page_no) FROM s2_sales WHERE manifest_id=?;", (mid,))
    v = c.fetchone()[0]
    conn.close()
    try:
        return int(v or 0)
    except Exception:
        return 0


def _s2_append_control(mid: int, pdf_name: str, pdf_bytes: bytes, page_offset: int = 0) -> int:
    """Append a control PDF into an existing manifest, offsetting page numbers to avoid mixing."""
    pages_sales = _s2_parse_control_pdf(pdf_bytes)
    conn = get_conn()
    c = conn.cursor()

    n_sales = 0
    page_counters = {}
    for s in pages_sales:
        sale_id = str(s.get("sale_id") or "")
        if not sale_id:
            continue
        n_sales += 1
        shipment_id = s.get("shipment_id")
        raw_page_no = int(s.get("page_no") or 1)
        page_no = int(raw_page_no + int(page_offset or 0))
        pack_id = s.get("pack_id")
        row_no = int(page_counters.get(page_no, 0) + 1)
        page_counters[page_no] = row_no
        customer = _s2_clean_person_text(s.get("customer"), 70)
        destino = _s2_clean_person_text(s.get("destino"), 80)

        c.execute("""INSERT INTO s2_sales(manifest_id, sale_id, shipment_id, page_no, row_no, status, pack_id, customer, destino)
                     VALUES(?,?,?,?,?, 'NEW', ?, ?, ?)
                     ON CONFLICT(manifest_id, sale_id) DO UPDATE SET
                        shipment_id=excluded.shipment_id,
                        page_no=excluded.page_no,
                        status='NEW',
                        mesa=NULL,
                        opened_at=NULL,
                        closed_at=NULL,
                        pack_id=excluded.pack_id,
                        customer=excluded.customer,
                        destino=excluded.destino;""",
                  (mid, sale_id, (str(shipment_id) if shipment_id else None), page_no, row_no,
                   (str(pack_id) if pack_id else None), (str(customer) if customer else None),
                   (str(destino) if destino else None)))

        for it in s.get("items", []):
            try:
                sku = str(it.get("sku"))
                qty = int(it.get("qty") or 0)
            except Exception:
                continue
            if not sku or qty <= 0:
                continue
            c.execute("""INSERT INTO s2_items(manifest_id, sale_id, sku, description, qty, picked, status)
                         VALUES(?,?,?,?,?,0,'PENDING')
                         ON CONFLICT(manifest_id, sale_id, sku) DO UPDATE SET
                            description=excluded.description,
                            qty=excluded.qty,
                            picked=0,
                            status='PENDING';""", (mid, sale_id, sku, it.get("desc",""), qty))

    conn.commit()
    conn.close()
    return n_sales



def _s2_upsert_labels(mid: int, labels_name: str, labels_bytes: bytes):
    # Detectar ship ids y relaciones pack/venta -> ship
    pack_to_ship, sale_to_ship, shipment_ids = _s2_parse_labels_txt(labels_bytes)

    try:
        txt = labels_bytes.decode("utf-8", errors="ignore")
    except Exception:
        txt = str(labels_bytes)

    conn = get_conn()
    c = conn.cursor()
    c.execute("""INSERT INTO s2_files(manifest_id, labels_txt, labels_name, updated_at)
                 VALUES(?, ?, ?, ?)
                 ON CONFLICT(manifest_id) DO UPDATE SET
                    labels_txt=excluded.labels_txt,
                    labels_name=excluded.labels_name,
                    updated_at=excluded.updated_at;""", (mid, labels_bytes, labels_name, _s2_now_iso()))

    # limpiar y reinsertar shipment ids
    c.execute("DELETE FROM s2_labels WHERE manifest_id=?;", (mid,))
    for sid in shipment_ids:
        c.execute("INSERT OR REPLACE INTO s2_labels(manifest_id, shipment_id, raw) VALUES(?,?,NULL);", (mid, str(sid)))

    # guardar pack->ship para Colecta
    if pack_to_ship:
        for pack_id, ship_id in pack_to_ship.items():
            c.execute("INSERT OR REPLACE INTO s2_pack_ship(manifest_id, pack_id, shipment_id) VALUES(?,?,?);",
                      (mid, str(pack_id), str(ship_id)))

        # completar shipment_id en ventas usando pack_id si falta
        try:
            c.execute("""UPDATE s2_sales
                           SET shipment_id = (
                               SELECT ps.shipment_id FROM s2_pack_ship ps
                               WHERE ps.manifest_id=s2_sales.manifest_id AND ps.pack_id=s2_sales.pack_id
                           )
                           WHERE manifest_id=? AND (shipment_id IS NULL OR shipment_id='') AND pack_id IS NOT NULL AND pack_id!='';""", (mid,))
        except Exception:
            pass

    # fallback: completar shipment_id por sale_id (cuando el Control no trae Pack ID)
    if sale_to_ship:
        try:
            for sale_id, ship_id in sale_to_ship.items():
                c.execute("""UPDATE s2_sales
                             SET shipment_id=?
                             WHERE manifest_id=? AND sale_id=? AND (shipment_id IS NULL OR shipment_id='');""",
                          (str(ship_id), mid, str(sale_id)))
        except Exception:
            pass

    # Guardar RAW por shipment_id y derivar Cliente/Destino desde la etiqueta (Flex/Colecta)
    import re
    blocks = re.split(r"\^XA", txt)
    for b in blocks:
        if not b.strip():
            continue
        raw_block = "^XA" + b

        ship = None
        jm = re.search(r"\"id\"\s*:\s*\"(\d{8,15})\"", raw_block)
        if jm:
            ship = jm.group(1)
        if not ship:
            nums = re.findall(r"\b\d{10,15}\b", raw_block)
            if nums:
                nums_sorted = sorted(nums, key=lambda x: (0 if x.startswith("46") else 1, -len(x)))
                ship = nums_sorted[0]
        if not ship:
            continue

        # guardar raw completo
        c.execute("""UPDATE s2_labels SET raw=? WHERE manifest_id=? AND shipment_id=?;""",
                  (raw_block, mid, str(ship)))

        info = _s2_parse_label_raw_info(raw_block)
        if not info:
            continue

        customer = _s2_clean_person_text(info.get("destinatario"), 70)

        destino_parts = []
        dom = _s2_clean_person_text(info.get("domicilio"), 120)
        city = _s2_clean_person_text(info.get("ciudad_destino"), 80)
        if dom:
            destino_parts.append(dom)
        if city:
            destino_parts.append(city)
        destino = " - ".join([p for p in destino_parts if p]) if destino_parts else None
        destino = _s2_clean_person_text(destino, 160) if destino else None

        # actualizar ventas por shipment_id
        comuna = _s2_clean_person_text(info.get("comuna"), 60)
        ciudad_dest = _s2_clean_person_text(info.get("ciudad_destino"), 80)

        fields = []
        params = []
        if customer:
            fields.append("customer=?")
            params.append(customer)
        if destino:
            fields.append("destino=?")
            params.append(destino)
        if comuna:
            fields.append("comuna=?")
            params.append(comuna)
        if ciudad_dest:
            fields.append("ciudad_destino=?")
            params.append(ciudad_dest)

        if fields:
            params.extend([mid, str(ship)])
            c.execute(f"""UPDATE s2_sales
                             SET {', '.join(fields)}
                             WHERE manifest_id=? AND shipment_id=?;""", tuple(params))
    conn.commit()
    conn.close()
    return len(shipment_ids)

def _s2_get_stats(mid: int):
    """
    Stats del manifiesto (Sorting v2).
    Incluye aliases para UI: ventas/items/etiquetas/... para evitar KeyError.
    Tolerante a cambios de esquema.
    """
    conn = get_conn()
    c = conn.cursor()

    def has_col(table: str, col: str) -> bool:
        try:
            cols = [r[1] for r in c.execute(f"PRAGMA table_info({table});").fetchall()]
            return col in cols
        except Exception:
            return False

    stats = {}

    # Core counts
    sales_total = int(c.execute("SELECT COUNT(*) FROM s2_sales WHERE manifest_id=?;", (mid,)).fetchone()[0] or 0)
    items_total = int(c.execute("SELECT COUNT(*) FROM s2_items WHERE manifest_id=?;", (mid,)).fetchone()[0] or 0)
    labels_total = int(c.execute("SELECT COUNT(*) FROM s2_labels WHERE manifest_id=?;", (mid,)).fetchone()[0] or 0)

    sales_pending = int(c.execute("SELECT COUNT(*) FROM s2_sales WHERE manifest_id=? AND status='PENDING';", (mid,)).fetchone()[0] or 0)
    sales_done    = int(c.execute("SELECT COUNT(*) FROM s2_sales WHERE manifest_id=? AND status='DONE';", (mid,)).fetchone()[0] or 0)

    items_pending = int(c.execute("SELECT COUNT(*) FROM s2_items WHERE manifest_id=? AND status='PENDING';", (mid,)).fetchone()[0] or 0)
    items_done    = int(c.execute("SELECT COUNT(*) FROM s2_items WHERE manifest_id=? AND status='DONE';", (mid,)).fetchone()[0] or 0)
    items_incid   = int(c.execute("SELECT COUNT(*) FROM s2_items WHERE manifest_id=? AND status='INCIDENCE';", (mid,)).fetchone()[0] or 0)

    # Labels with shipment_id
    if has_col("s2_labels", "shipment_id"):
        labels_with_ship = int(c.execute(
            "SELECT COUNT(*) FROM s2_labels WHERE manifest_id=? AND shipment_id IS NOT NULL AND shipment_id!='';",
            (mid,)
        ).fetchone()[0] or 0)
        distinct_ship_labels = int(c.execute(
            "SELECT COUNT(DISTINCT shipment_id) FROM s2_labels WHERE manifest_id=? AND shipment_id IS NOT NULL AND shipment_id!='';",
            (mid,)
        ).fetchone()[0] or 0)
    else:
        labels_with_ship = 0
        distinct_ship_labels = 0

    # Pack ID availability
    # In control, pack_id usually lives in s2_sales.pack_id (not in labels).
    sales_with_pack = 0
    distinct_packs = 0
    if has_col("s2_sales", "pack_id"):
        sales_with_pack = int(c.execute(
            "SELECT COUNT(*) FROM s2_sales WHERE manifest_id=? AND pack_id IS NOT NULL AND pack_id!='';",
            (mid,)
        ).fetchone()[0] or 0)
        distinct_packs = int(c.execute(
            "SELECT COUNT(DISTINCT pack_id) FROM s2_sales WHERE manifest_id=? AND pack_id IS NOT NULL AND pack_id!='';",
            (mid,)
        ).fetchone()[0] or 0)

    labels_with_pack = 0
    if has_col("s2_labels", "pack_id"):
        labels_with_pack = int(c.execute(
            "SELECT COUNT(*) FROM s2_labels WHERE manifest_id=? AND pack_id IS NOT NULL AND pack_id!='';",
            (mid,)
        ).fetchone()[0] or 0)

    labels_with_sale = 0
    if has_col("s2_labels", "sale_id"):
        labels_with_sale = int(c.execute(
            "SELECT COUNT(*) FROM s2_labels WHERE manifest_id=? AND sale_id IS NOT NULL AND sale_id!='';",
            (mid,)
        ).fetchone()[0] or 0)

    # Sales with shipment_id (after matching)
    if has_col("s2_sales", "shipment_id"):
        sales_with_ship = int(c.execute(
            "SELECT COUNT(*) FROM s2_sales WHERE manifest_id=? AND shipment_id IS NOT NULL AND shipment_id!='';",
            (mid,)
        ).fetchone()[0] or 0)
    else:
        sales_with_ship = 0

    missing_ship = sales_total - sales_with_ship

    # Matches by pack (if mapping table exists)
    matched_by_pack = 0
    if "s2_pack_ship" in [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]:
        matched_by_pack = int(c.execute(
            "SELECT COUNT(DISTINCT pack_id) FROM s2_pack_ship WHERE manifest_id=? AND pack_id IS NOT NULL AND pack_id!='' AND shipment_id IS NOT NULL AND shipment_id!='';",
            (mid,)
        ).fetchone()[0] or 0)

    # Fill canonical keys
    stats.update({
        "sales_total": sales_total,
        "sales_pending": sales_pending,
        "sales_done": sales_done,
        "items_total": items_total,
        "items_pending": items_pending,
        "items_done": items_done,
        "items_incidence": items_incid,
        "labels_total": labels_total,
        "labels_with_ship": labels_with_ship,
        "labels_unique_ship": distinct_ship_labels,
        "sales_with_pack": sales_with_pack,
        "distinct_packs": distinct_packs,
        "sales_with_ship": sales_with_ship,
        "sales_missing_ship": missing_ship,
        "labels_with_pack": labels_with_pack,
        "labels_with_sale": labels_with_sale,
        "matched_by_pack": matched_by_pack,
    })

    # Aliases expected by UI (legacy naming)
    stats.update({
        "ventas": sales_total,
        "items": items_total,
        "etiquetas": labels_total,
        "distinct_ship_labels": distinct_ship_labels,
        "ventas_with_pack": sales_with_pack,
        "ventas_with_ship": sales_with_ship,
        "missing_ship": missing_ship,
        "matched_by_pack": matched_by_pack,
    })

    conn.close()
    return stats

def _s2_reset_all_sorting():
    """Hard reset of Sorting module only (keeps other modules intact)."""
    conn = get_conn()
    c = conn.cursor()
    # New (s2_*) tables (some deployments may not have all of them yet)
    s2_tables = [
        "s2_page_assign",
        "s2_pack_ship",
        "s2_labels",
        "s2_items",
        "s2_sales",
        "s2_files",
        "s2_manifests",
        "s2_packing",
        "s2_dispatch",
    ]
    for t in s2_tables:
        try:
            c.execute(f"DELETE FROM {t};")
        except Exception:
            # table may not exist in older DBs
            pass
    conn.commit()
    conn.close()


def _s2_get_pages(mid:int):
    conn=get_conn()
    c=conn.cursor()
    c.execute("SELECT DISTINCT page_no FROM s2_sales WHERE manifest_id=? ORDER BY page_no;", (mid,))
    pages=[int(r[0]) for r in c.fetchall()]
    conn.close()
    return pages

def _s2_auto_assign_pages(mid:int, num_mesas:int=10):
    pages=_s2_get_pages(mid)
    if not pages:
        return 0
    conn=get_conn()
    c=conn.cursor()
    for i,p in enumerate(pages):
        mesa = (i % num_mesas) + 1
        c.execute("""INSERT INTO s2_page_assign(manifest_id, page_no, mesa)
                     VALUES(?,?,?)
                     ON CONFLICT(manifest_id, page_no) DO UPDATE SET mesa=excluded.mesa;""", (mid, p, mesa))
    conn.commit()
    conn.close()
    return len(pages)

def _s2_next_mesa_block(mid: int, default_count: int = 3):
    assigns = _s2_get_assignments(mid)
    if not assigns:
        return 1, int(default_count)
    mesas = sorted({int(m) for _, m in assigns if m is not None})
    mesas_count = int(len(mesas) or default_count)
    next_start = int(max(mesas) + 1) if mesas else 1
    return next_start, mesas_count


def _s2_auto_assign_specific_pages(mid: int, pages: list[int], start_mesa: int = 1, mesas_count: int = 3):
    pages = [int(p) for p in pages or []]
    if not pages:
        return 0
    mesas_count = max(1, int(mesas_count or 1))
    start_mesa = max(1, int(start_mesa or 1))
    conn = get_conn()
    c = conn.cursor()
    for i, p in enumerate(sorted(set(pages))):
        mesa = start_mesa + (i % mesas_count)
        c.execute("""INSERT INTO s2_page_assign(manifest_id, page_no, mesa)
                     VALUES(?,?,?)
                     ON CONFLICT(manifest_id, page_no) DO UPDATE SET mesa=excluded.mesa;""", (mid, p, mesa))
    conn.commit()
    conn.close()
    return len(set(pages))


def _s2_build_assignment_preview(pages: list[int], start_mesa: int = 1, mesas_count: int = 3):
    pages = sorted({int(p) for p in (pages or [])})
    mesas_count = max(1, int(mesas_count or 1))
    start_mesa = max(1, int(start_mesa or 1))
    preview = []
    for i, p in enumerate(pages):
        preview.append({"Página": int(p), "Mesa": int(start_mesa + (i % mesas_count))})
    return preview


def _s2_build_default_page_assignment(pages: list[int], exclude_mid: int | None = None):
    pages = sorted({int(p) for p in (pages or [])})
    locked = set(_s2_get_locked_mesas(exclude_mid=exclude_mid))
    used = set()
    current = 1
    mapping = {}
    for p in pages:
        while current in locked or current in used:
            current += 1
        mapping[int(p)] = int(current)
        used.add(int(current))
        current += 1
    return mapping


def _s2_render_assignment_editor(pages: list[int], widget_prefix: str, exclude_mid: int | None = None):
    pages = sorted({int(p) for p in (pages or [])})
    locked = set(_s2_get_locked_mesas(exclude_mid=exclude_mid))
    defaults = _s2_build_default_page_assignment(pages, exclude_mid=exclude_mid)

    assignments = {}
    warnings = []
    if not pages:
        return assignments, warnings, locked

    st.markdown("**Asignación manual Página → Mesa**")
    st.caption("Define exactamente qué página irá a qué mesa. Las mesas bloqueadas por trabajo activo no se pueden reutilizar.")

    seen_mesas = {}
    for p in pages:
        key = f"{widget_prefix}_page_{int(p)}"
        default_mesa = int(st.session_state.get(key, defaults.get(int(p), 1)))
        mesa = st.number_input(
            f"Página {int(p)} → Mesa",
            min_value=1,
            max_value=200,
            value=default_mesa,
            step=1,
            key=key,
        )
        mesa = int(mesa)
        assignments[int(p)] = mesa

        if mesa in locked:
            warnings.append(f"La mesa {mesa} está bloqueada por un lote activo.")
        if mesa in seen_mesas:
            warnings.append(f"La mesa {mesa} está repetida en las páginas {seen_mesas[mesa]} y {int(p)}.")
        else:
            seen_mesas[mesa] = int(p)

    return assignments, warnings, locked


def _s2_apply_page_assignments(mid: int, page_to_mesa: dict[int, int]):
    page_to_mesa = {int(p): int(m) for p, m in (page_to_mesa or {}).items()}
    if not page_to_mesa:
        return 0
    conn = get_conn()
    c = conn.cursor()
    for p, mesa in sorted(page_to_mesa.items()):
        c.execute("""INSERT INTO s2_page_assign(manifest_id, page_no, mesa)
                     VALUES(?,?,?)
                     ON CONFLICT(manifest_id, page_no) DO UPDATE SET mesa=excluded.mesa;""", (int(mid), int(p), int(mesa)))
    conn.commit()
    conn.close()
    return len(page_to_mesa)


def _s2_preview_batch_pages(pdfs):
    preview_pages = []
    running_offset = 0
    for pdf in (pdfs or []):
        try:
            parsed = _s2_parse_control_pdf(pdf.getvalue())
            pages_local = sorted({int(r.get("page_no") or 1) for r in parsed})
        except Exception:
            pages_local = []
        for p in pages_local:
            preview_pages.append(int(p) + int(running_offset))
        if pages_local:
            running_offset += max(pages_local)
    return sorted({int(p) for p in preview_pages})


def _s2_close_mesa(mid: int, mesa: int):
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    pending = int(c.execute("""SELECT COUNT(*)
                               FROM s2_sales
                               WHERE manifest_id=? AND mesa=? AND status!='DONE';""", (int(mid), int(mesa))).fetchone()[0] or 0)
    if pending > 0:
        conn.close()
        return False, f"La mesa {int(mesa)} todavía tiene {pending} venta(s) pendiente(s)."

    c.execute("""INSERT INTO s2_mesa_status(manifest_id, mesa, status, closed_at)
                 VALUES(?,?, 'CLOSED', ?)
                 ON CONFLICT(manifest_id, mesa) DO UPDATE SET
                    status='CLOSED',
                    closed_at=excluded.closed_at;""", (int(mid), int(mesa), _s2_now_iso()))

    open_rows = c.execute("""SELECT DISTINCT p.mesa
                             FROM s2_page_assign p
                             LEFT JOIN s2_mesa_status ms ON ms.manifest_id = p.manifest_id AND ms.mesa = p.mesa
                             WHERE p.manifest_id=? AND COALESCE(ms.status, 'OPEN')='OPEN';""", (int(mid),)).fetchall()
    if not open_rows:
        c.execute("UPDATE s2_manifests SET status='DONE' WHERE id=?;", (int(mid),))

    conn.commit()
    conn.close()
    return True, None


def _s2_is_mesa_closed(mid: int, mesa: int) -> bool:
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    row = c.execute("""SELECT status
                       FROM s2_mesa_status
                       WHERE manifest_id=? AND mesa=?;""", (int(mid), int(mesa))).fetchone()
    conn.close()
    return bool(row and str(row[0]).upper() == 'CLOSED')


def _s2_append_labels(mid: int, labels_name: str, labels_bytes: bytes):
    pack_to_ship, sale_to_ship, shipment_ids = _s2_parse_labels_txt(labels_bytes)
    try:
        txt = labels_bytes.decode("utf-8", errors="ignore")
    except Exception:
        txt = str(labels_bytes)

    conn = get_conn()
    c = conn.cursor()

    prev = c.execute("SELECT labels_name FROM s2_files WHERE manifest_id=?;", (mid,)).fetchone()
    prev_name = str(prev[0] or "").strip() if prev else ""
    merged_name = labels_name if not prev_name else f"{prev_name} + {labels_name}"

    c.execute("""INSERT INTO s2_files(manifest_id, labels_txt, labels_name, updated_at)
                 VALUES(?, ?, ?, ?)
                 ON CONFLICT(manifest_id) DO UPDATE SET
                    labels_name=excluded.labels_name,
                    updated_at=excluded.updated_at;""", (mid, labels_bytes, merged_name, _s2_now_iso()))

    for sid in shipment_ids:
        c.execute("INSERT OR IGNORE INTO s2_labels(manifest_id, shipment_id, raw) VALUES(?,?,NULL);", (mid, str(sid)))

    if pack_to_ship:
        for pack_id, ship_id in pack_to_ship.items():
            c.execute("INSERT OR REPLACE INTO s2_pack_ship(manifest_id, pack_id, shipment_id) VALUES(?,?,?);", (mid, str(pack_id), str(ship_id)))
        try:
            c.execute("""UPDATE s2_sales
                           SET shipment_id = (
                               SELECT ps.shipment_id FROM s2_pack_ship ps
                               WHERE ps.manifest_id=s2_sales.manifest_id AND ps.pack_id=s2_sales.pack_id
                           )
                           WHERE manifest_id=? AND (shipment_id IS NULL OR shipment_id='') AND pack_id IS NOT NULL AND pack_id!='';""", (mid,))
        except Exception:
            pass

    if sale_to_ship:
        try:
            for sale_id, ship_id in sale_to_ship.items():
                c.execute("""UPDATE s2_sales
                             SET shipment_id=?
                             WHERE manifest_id=? AND sale_id=? AND (shipment_id IS NULL OR shipment_id='');""",
                          (str(ship_id), mid, str(sale_id)))
        except Exception:
            pass

    import re
    blocks = re.split(r"\^XA", txt)
    for b in blocks:
        if not b.strip():
            continue
        raw_block = "^XA" + b

        ship = None
        jm = re.search(r'"id"\s*:\s*"(\d{8,15})"', raw_block)
        if jm:
            ship = jm.group(1)
        if not ship:
            nums = re.findall(r"\d{10,15}", raw_block)
            if nums:
                nums_sorted = sorted(nums, key=lambda x: (0 if x.startswith("46") else 1, -len(x)))
                ship = nums_sorted[0]
        if not ship:
            continue

        c.execute("""UPDATE s2_labels SET raw=? WHERE manifest_id=? AND shipment_id=?;""", (raw_block, mid, str(ship)))
        info = _s2_parse_label_raw_info(raw_block)
        if not info:
            continue
        customer = _s2_clean_person_text(info.get("destinatario"), 70)
        destino_parts = []
        dom = _s2_clean_person_text(info.get("domicilio"), 120)
        city = _s2_clean_person_text(info.get("ciudad_destino"), 80)
        if dom:
            destino_parts.append(dom)
        if city:
            destino_parts.append(city)
        destino = " - ".join([p for p in destino_parts if p]) if destino_parts else None
        destino = _s2_clean_person_text(destino, 160) if destino else None
        comuna = _s2_clean_person_text(info.get("comuna"), 60)
        ciudad_dest = _s2_clean_person_text(info.get("ciudad_destino"), 80)
        fields, params = [], []
        if customer:
            fields.append("customer=?")
            params.append(customer)
        if destino:
            fields.append("destino=?")
            params.append(destino)
        if comuna:
            fields.append("comuna=?")
            params.append(comuna)
        if ciudad_dest:
            fields.append("ciudad_destino=?")
            params.append(ciudad_dest)
        if fields:
            params.extend([mid, str(ship)])
            c.execute(f"""UPDATE s2_sales
                             SET {', '.join(fields)}
                             WHERE manifest_id=? AND shipment_id=?;""", tuple(params))

    conn.commit()
    conn.close()
    return len(shipment_ids)


def _s2_get_assignments(mid:int):
    conn=get_conn()
    c=conn.cursor()
    c.execute("SELECT page_no, mesa FROM s2_page_assign WHERE manifest_id=? ORDER BY page_no;", (mid,))
    rows=[(int(r[0]), int(r[1])) for r in c.fetchall()]
    conn.close()
    return rows

def _s2_set_assignment(mid:int, page_no:int, mesa:int):
    conn=get_conn()
    c=conn.cursor()
    c.execute("""INSERT INTO s2_page_assign(manifest_id, page_no, mesa)
                 VALUES(?,?,?)
                 ON CONFLICT(manifest_id, page_no) DO UPDATE SET mesa=excluded.mesa;""", (mid, int(page_no), int(mesa)))
    conn.commit()
    conn.close()

def _s2_create_corridas(mid:int):
    # apply mesa from page assignments to sales y liberar a producción
    conn=get_conn()
    c=conn.cursor()
    c.execute("SELECT page_no, mesa FROM s2_page_assign WHERE manifest_id=?;", (mid,))
    page_to_mesa = {int(p): int(m) for p,m in c.fetchall()}

    mesas_assigned = sorted({int(m) for m in page_to_mesa.values() if m is not None})
    for mesa in mesas_assigned:
        c.execute("""INSERT INTO s2_mesa_status(manifest_id, mesa, status, closed_at)
                     VALUES(?,?, 'OPEN', NULL)
                     ON CONFLICT(manifest_id, mesa) DO UPDATE SET
                        status='OPEN',
                        closed_at=NULL;""", (int(mid), int(mesa)))

    # update sales
    c.execute("SELECT sale_id, page_no FROM s2_sales WHERE manifest_id=?;", (mid,))
    sales = c.fetchall()
    updated=0
    for sale_id, page_no in sales:
        mesa = page_to_mesa.get(int(page_no))
        if mesa is None:
            continue
        c.execute("""UPDATE s2_sales
                     SET mesa=?, status='PENDING', opened_at=NULL, closed_at=NULL
                     WHERE manifest_id=? AND sale_id=?;""", (mesa, mid, sale_id))
        updated += 1
    c.execute("UPDATE s2_manifests SET status='ACTIVE' WHERE id=?;", (int(mid),))
    conn.commit()
    conn.close()
    return updated

def _s2_next_pending_sale_in_sequence(mid:int, mesa:int):
    """Devuelve la próxima venta pendiente (secuencia obligatoria) para una mesa,
    ordenada por página y luego por sale_id (orden estable)."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""SELECT sale_id, shipment_id, pack_id, page_no
                 FROM s2_sales
                 WHERE manifest_id=? AND mesa=? AND status='PENDING'
                 ORDER BY page_no, row_no, sale_id
                 LIMIT 1;""", (mid, int(mesa)))
    row = c.fetchone()
    conn.close()
    return row  # (sale_id, shipment_id, pack_id, page_no) o None


def _s2_sale_items(mid:int, sale_id:str):
    conn=get_conn()
    c=conn.cursor()
    c.execute("""SELECT sku, description, qty, picked, status
                 FROM s2_items WHERE manifest_id=? AND sale_id=? ORDER BY sku;""", (mid, sale_id))
    rows=c.fetchall()
    conn.close()
    return rows

def _s2_apply_pick(mid:int, sale_id:str, sku:str, add_qty:int):
    conn=get_conn()
    c=conn.cursor()
    c.execute("""SELECT qty, picked FROM s2_items
                 WHERE manifest_id=? AND sale_id=? AND sku=?;""", (mid, sale_id, sku))
    row=c.fetchone()
    if not row:
        conn.close()
        return False, "SKU no pertenece a esta venta"
    qty, picked = int(row[0]), int(row[1])
    new_picked = min(qty, picked + int(add_qty))
    status = "DONE" if new_picked >= qty else "PENDING"
    c.execute("""UPDATE s2_items SET picked=?, status=? WHERE manifest_id=? AND sale_id=? AND sku=?;""", 
              (new_picked, status, mid, sale_id, sku))
    # if all done, allow close
    conn.commit()
    conn.close()
    return True, None


def _s2_mark_incidence(mid:int, sale_id:str, sku:str, note:str=""):
    conn=get_conn()
    c=conn.cursor()
    c.execute("UPDATE s2_items SET status='INCIDENCE', confirm_mode='INCIDENCE', updated_at=? WHERE manifest_id=? AND sale_id=? AND sku=?;", (_s2_now_iso(), mid, sale_id, sku))
    conn.commit()
    conn.close()

def _s2_force_done_no_ean(mid:int, sale_id:str, sku:str):
    conn=get_conn()
    c=conn.cursor()
    c.execute("SELECT qty, picked FROM s2_items WHERE manifest_id=? AND sale_id=? AND sku=?;", (mid, sale_id, sku))
    row=c.fetchone()
    if not row:
        conn.close()
        return False
    qty=int(row[0] or 0)
    c.execute("UPDATE s2_items SET picked=?, status='DONE', confirm_mode='MANUAL_NO_EAN', updated_at=? WHERE manifest_id=? AND sale_id=? AND sku=?;", (qty, _s2_now_iso(), mid, sale_id, sku))
    conn.commit()
    conn.close()
    return True

def _s2_is_sale_done(mid:int, sale_id:str):
    conn=get_conn()
    c=conn.cursor()
    c.execute("""SELECT COUNT(1) FROM s2_items WHERE manifest_id=? AND sale_id=? AND status NOT IN ('DONE','INCIDENCE');""", (mid, sale_id))
    rem=int(c.fetchone()[0] or 0)
    conn.close()
    return rem==0

def _s2_close_sale(mid:int, sale_id:str):
    conn=get_conn()
    c=conn.cursor()
    c.execute("""UPDATE s2_sales SET status='DONE', closed_at=? WHERE manifest_id=? AND sale_id=?;""", (_s2_now_iso(), mid, sale_id))
    conn.commit()
    conn.close()


def page_sorting_upload(inv_map_sku, barcode_to_sku):
    _s2_create_tables()
    _s2_migrate_staged_to_active()
    st.title("Sorting - Carga y Corridas")

    open_manifests = _s2_list_open_manifests()
    locked_mesas_all = _s2_get_locked_mesas()

    if st.session_state.get("s2_upload_flash"):
        st.success(st.session_state.get("s2_upload_flash"))
        st.session_state["s2_upload_flash"] = ""

    if open_manifests:
        resumen = []
        for mf in open_manifests:
            progreso = f"{max(0, mf['sales_total'] - mf['sales_pending'])}/{mf['sales_total']}" if mf['sales_total'] else "0/0"
            resumen.append({
                "Lote": mf["lot_label"],
                "Mesas": mf["mesas_text"],
                "Ventas": mf["sales_total"],
                "Avance ventas": progreso,
                "Control": mf["control_name"] or "-",
                "Etiquetas": mf["labels_name"] or "-",
            })
        with st.expander("Lotes activos", expanded=True):
            st.dataframe(resumen, use_container_width=True, hide_index=True)
    else:
        st.info("No hay lotes activos todavía. Carga el primer Control + Etiquetas para comenzar.")

    if locked_mesas_all:
        st.caption(f"Mesas bloqueadas por lotes activos abiertos: {', '.join(map(str, locked_mesas_all))}")

    mode = st.radio(
        "Modo de carga",
        ["Uno (1 Control + 1 Etiquetas)", "Varios (lote: varios Controles + varias Etiquetas)"],
        horizontal=True,
        key="s2_upload_mode",
    )

    if mode.startswith("Uno"):
        col1, col2 = st.columns(2)
        with col1:
            pdf = st.file_uploader("Control (PDF)", type=["pdf"], key="s2_control_pdf")
        with col2:
            zpl = st.file_uploader("Etiquetas de envío (TXT/ZPL)", type=["txt", "zpl"], key="s2_labels_txt")

        st.caption("Al confirmar, la carga entra directo a producción, pero ahora tú eliges exactamente qué página va a cada mesa.")

        single_preview_pages = []
        single_assignments = {}
        single_warnings = []
        if pdf is not None:
            try:
                parsed_preview = _s2_parse_control_pdf(pdf.getvalue())
                single_preview_pages = sorted({int(r.get("page_no") or 1) for r in parsed_preview})
            except Exception:
                single_preview_pages = []

        if pdf is not None or zpl is not None:
            with st.container(border=True):
                st.markdown("**Resumen de la carga lista para confirmar**")
                st.write(f"**Control:** {getattr(pdf, 'name', '-') if pdf is not None else '-'}")
                st.write(f"**Etiquetas:** {getattr(zpl, 'name', '-') if zpl is not None else '-'}")

                if single_preview_pages:
                    st.write(f"**Páginas detectadas en el Control:** {len(single_preview_pages)}")
                    single_assignments, single_warnings, _ = _s2_render_assignment_editor(
                        single_preview_pages,
                        widget_prefix="s2_single_assign",
                    )
                    st.dataframe(
                        [{"Página": int(p), "Mesa": int(m)} for p, m in sorted(single_assignments.items())],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info("No pude previsualizar las páginas del Control antes de confirmar.")

                if single_warnings:
                    for msg in single_warnings:
                        st.warning(msg)

        process_one = st.button(
            "Confirmar carga de Control + Etiquetas",
            type="primary",
            disabled=(pdf is None and zpl is None),
            key="s2_process_single_upload",
        )

        if process_one:
            if pdf is None:
                st.error("Debes subir el Control (PDF) antes de confirmar la carga.")
            elif zpl is None:
                st.error("Debes subir también las Etiquetas (TXT/ZPL) antes de confirmar la carga.")
            elif not single_preview_pages:
                st.error("No pude detectar páginas del Control para asignarlas manualmente.")
            elif single_warnings:
                st.error("Corrige la asignación Página → Mesa antes de confirmar.")
            else:
                pdf_name = getattr(pdf, "name", "control.pdf")
                pdf_bytes = pdf.getvalue()
                zpl_name = getattr(zpl, "name", "etiquetas.txt")
                zpl_bytes = zpl.getvalue()

                target_mid = _s2_find_empty_active_manifest() or _s2_create_new_manifest('ACTIVE')
                n_sales = _s2_upsert_control(target_mid, pdf_name, pdf_bytes)
                _s2_apply_page_assignments(target_mid, single_assignments)
                n_labels = _s2_upsert_labels(target_mid, zpl_name, zpl_bytes)
                created = _s2_create_corridas(target_mid)

                lot_label = _s2_lot_label(target_mid)
                st.session_state["sorting_manifest_id"] = target_mid
                st.session_state["s2_upload_flash"] = f"{lot_label} cargado correctamente: {n_sales} ventas, {n_labels} etiquetas y {created} corridas activas."

                for k in ("s2_control_pdf", "s2_labels_txt"):
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

    else:
        st.info("📦 Lote: se crea un lote nuevo, se cargan todos los Controles y Etiquetas y tú defines manualmente la página que irá a cada mesa.")
        col1, col2 = st.columns(2)
        with col1:
            pdfs = st.file_uploader(
                "Controles (PDF) — puedes subir varios",
                type=["pdf"],
                accept_multiple_files=True,
                key="s2_control_pdfs",
            )
        with col2:
            zpls = st.file_uploader(
                "Etiquetas (TXT/ZPL) — puedes subir varios",
                type=["txt", "zpl"],
                accept_multiple_files=True,
                key="s2_labels_txts",
            )

        batch_preview_pages = _s2_preview_batch_pages(pdfs)
        batch_assignments = {}
        batch_warnings = []

        if pdfs or zpls:
            with st.container(border=True):
                st.markdown("**Resumen del lote listo para confirmar**")
                st.write(f"**Controles:** {', '.join(getattr(x, 'name', '-') for x in (pdfs or [])) or '-'}")
                st.write(f"**Etiquetas:** {', '.join(getattr(x, 'name', '-') for x in (zpls or [])) or '-'}")
                if batch_preview_pages:
                    st.write(f"**Páginas detectadas en el lote:** {len(batch_preview_pages)}")
                    batch_assignments, batch_warnings, _ = _s2_render_assignment_editor(
                        batch_preview_pages,
                        widget_prefix="s2_batch_assign",
                    )
                    st.dataframe(
                        [{"Página": int(p), "Mesa": int(m)} for p, m in sorted(batch_assignments.items())],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info("Sube los Controles para poder definir la asignación manual de páginas.")
                if batch_warnings:
                    for msg in batch_warnings:
                        st.warning(msg)

        do_batch = st.button(
            "Procesar lote en una sola tanda",
            type="primary",
            disabled=(not pdfs and not zpls),
            key="s2_do_batch",
        )

        if do_batch:
            if not pdfs:
                st.error("Debes subir al menos un Control (PDF).")
            elif not zpls:
                st.error("Debes subir al menos un archivo de Etiquetas (TXT/ZPL).")
            elif not batch_preview_pages:
                st.error("No pude detectar páginas en los Controles del lote.")
            elif batch_warnings:
                st.error("Corrige la asignación Página → Mesa antes de procesar el lote.")
            else:
                target_mid = _s2_find_empty_active_manifest() or _s2_create_new_manifest('ACTIVE')
                total_sales = 0
                offset = 0
                control_names = []
                for i, pdf in enumerate(pdfs):
                    name = getattr(pdf, "name", f"control_{i+1}.pdf")
                    control_names.append(name)
                    if i == 0:
                        total_sales += int(_s2_upsert_control(target_mid, name, pdf.getvalue()) or 0)
                    else:
                        offset = _s2_get_max_page(target_mid)
                        total_sales += int(_s2_append_control(target_mid, name, pdf.getvalue(), page_offset=offset) or 0)

                _s2_apply_page_assignments(target_mid, batch_assignments)

                total_labels = 0
                label_names = []
                for i, z in enumerate(zpls):
                    name = getattr(z, "name", f"etiquetas_{i+1}.txt")
                    label_names.append(name)
                    if i == 0:
                        total_labels += int(_s2_upsert_labels(target_mid, name, z.getvalue()) or 0)
                    else:
                        total_labels += int(_s2_append_labels(target_mid, name, z.getvalue()) or 0)

                try:
                    conn = get_conn()
                    c = conn.cursor()
                    row = c.execute("SELECT control_pdf, labels_txt FROM s2_files WHERE manifest_id=?;", (target_mid,)).fetchone()
                    c.execute(
                        """INSERT INTO s2_files(manifest_id, control_pdf, labels_txt, control_name, labels_name, updated_at)
                             VALUES(?, ?, ?, ?, ?, ?)
                             ON CONFLICT(manifest_id) DO UPDATE SET
                                 control_name=excluded.control_name,
                                 labels_name=excluded.labels_name,
                                 updated_at=excluded.updated_at;""",
                        (target_mid, row[0] if row else None, row[1] if row else None, " + ".join(control_names), " + ".join(label_names), _s2_now_iso())
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass

                created = _s2_create_corridas(target_mid)
                lot_label = _s2_lot_label(target_mid)
                st.session_state["s2_upload_flash"] = f"{lot_label} cargado correctamente: {total_sales} ventas, {total_labels} etiquetas y {created} corridas activas."
                for k in ("s2_control_pdfs", "s2_labels_txts"):
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

    st.subheader("Resumen de conciliación")
    if open_manifests:
        opts = {mf['lot_label']: int(mf['id']) for mf in open_manifests}
        view_label = st.selectbox("Ver conciliación de lote", options=list(opts.keys()), key="s2_summary_lot_view")
        view_mid = opts[view_label]
        stats = _s2_get_stats(view_mid)
        with st.expander("Ver detalle de conciliación", expanded=False):
            st.write(
                {
                    "Lote": view_label,
                    "Ventas con Pack ID": stats["ventas_with_pack"],
                    "Packs distintos (Control)": stats["distinct_packs"],
                    "Ventas con Envío (Control)": stats["ventas_with_ship"],
                    "Etiquetas con Pack ID": stats["labels_with_pack"],
                    "Etiquetas con Venta": stats["labels_with_sale"],
                    "Ventas matcheadas por Pack": stats["matched_by_pack"],
                    "Ventas sin Envío asignado": stats["missing_ship"],
                }
            )

        st.subheader("Asignación Página → Mesa")
        assigns = _s2_get_assignments(view_mid)
        if assigns:
            st.dataframe(
                [{"Página": int(p), "Mesa": int(m)} for p, m in assigns],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Este lote todavía no tiene páginas asignadas.")

def page_sorting_camarero(inv_map_sku, barcode_to_sku):
    _s2_create_tables()
    st.title("Camarero")
    st.caption("Escaneo por etiqueta (Flex/Colecta) y productos por SKU/EAN")

    mesa = st.number_input("Mesa", min_value=1, max_value=50, value=int(st.session_state.get("s2_mesa", 1)), key="s2_mesa")
    st.session_state["s2_mesa_int"] = int(mesa)  # store separately; do not overwrite widget key

    # State: current sale
    if "s2_sale_open" not in st.session_state:
        st.session_state["s2_sale_open"] = None
    if "s2_sale_open_manifest_id" not in st.session_state:
        st.session_state["s2_sale_open_manifest_id"] = None

    if st.session_state["s2_sale_open"] is not None and st.session_state.get("s2_sale_open_manifest_id"):
        mid = int(st.session_state["s2_sale_open_manifest_id"])
    else:
        mid = _s2_find_manifest_by_mesa(int(mesa))
        if not mid:
            st.warning(f"La mesa {int(mesa)} no está asignada a ningún manifiesto abierto.")
            return
    st.session_state["sorting_manifest_id"] = mid
    st.caption(f"Lote detectado para esta mesa: {_s2_lot_label(mid)}")

    if _s2_is_mesa_closed(mid, int(mesa)):
        st.success(f"La mesa {int(mesa)} ya fue cerrada en camarero.")
        return

    if st.session_state["s2_sale_open"] is None:
        st.subheader("Escanea etiqueta (QR Flex o barra Colecta)")
        # Limpieza segura del campo de escaneo (evita StreamlitAPIException)
        if st.session_state.get("s2_clear_label_scan"):
            st.session_state["s2_label_scan_widget"] = ""
            st.session_state["s2_clear_label_scan"] = False

        scan = st.text_input("Etiqueta", key="s2_label_scan_widget")
        if scan:
            sid = _s2_extract_shipment_id(scan)
            if not sid:
                st.error("No pude leer el ID de envío desde el escaneo.")
                sfx_emit("ERR")
            else:
                # Secuencia obligatoria: solo se puede abrir la PRÓXIMA venta pendiente según manifiesto (página -> venta)
                nxt = _s2_next_pending_sale_in_sequence(mid, int(mesa))
                if not nxt:
                    st.success("No hay más ventas pendientes en esta mesa.")
                    close_cols = st.columns([1, 1])
                    if close_cols[0].button("🔒 Cerrar mesa", key=f"s2_close_mesa_btn_{mid}_{int(mesa)}", use_container_width=True):
                        ok_close, err_close = _s2_close_mesa(mid, int(mesa))
                        if ok_close:
                            sfx_emit("OK")
                            st.session_state["s2_clear_label_scan"] = True
                            st.rerun()
                        else:
                            st.error(err_close or "No se pudo cerrar la mesa.")
                            sfx_emit("ERR")
                    if close_cols[1].button("↻ Refrescar", key=f"s2_refresh_mesa_btn_{mid}_{int(mesa)}", use_container_width=True):
                        st.session_state["s2_clear_label_scan"] = True
                        st.rerun()
                    return
                expected_sale_id, expected_ship, expected_pack, expected_page = nxt
                sale_id = None
                if sid and expected_ship and str(sid) == str(expected_ship):
                    sale_id = expected_sale_id
                elif sid and expected_pack and str(sid) == str(expected_pack):
                    sale_id = expected_sale_id
                if not sale_id:
                    # Existe en el manifiesto pero NO es la siguiente venta => bloquear por secuencia
                    conn=get_conn(); c=conn.cursor()
                    c.execute("""SELECT mesa, page_no, status, shipment_id, pack_id
                                 FROM s2_sales
                                 WHERE manifest_id=? AND (shipment_id=? OR pack_id=?)
                                 LIMIT 10;""", (mid, sid, sid))
                    info=c.fetchall(); conn.close()
                    if info:
                        # ¿Está en esta mesa?
                        in_same_mesa = [r for r in info if int(r[0] or 0) == int(mesa)]
                        if in_same_mesa:
                            exp_id = str(expected_ship or expected_pack or "")
                            st.error(f"Secuencia obligatoria: la próxima venta de esta mesa es de la página {expected_page}.\n\nDebes escanear la siguiente etiqueta del manifiesto (ID esperado: {exp_id}).")
                            sfx_emit("ERR")
                        else:
                            st.warning(f"Etiqueta encontrada, pero corresponde a otra mesa/página: {[(r[0], r[1], r[2]) for r in info]}")
                            sfx_emit("ERR")
                    else:
                        st.error("No encontré esta etiqueta en corridas del manifiesto activo.")
                        sfx_emit("ERR")
                else:
                    st.session_state["s2_sale_open"] = sale_id
                    st.session_state["s2_sale_open_manifest_id"] = mid
                    st.session_state["s2_clear_label_scan"] = True
                    sfx_emit("OK")
                    st.rerun()
        return

    sale_id = st.session_state["s2_sale_open"]
    st.info(f"Venta abierta: {sale_id}")


    # Información de la etiqueta / envío
    conn=get_conn(); c=conn.cursor()
    sale_row = c.execute("SELECT shipment_id, pack_id, customer, destino, comuna, ciudad_destino, page_no, mesa, status FROM s2_sales WHERE manifest_id=? AND sale_id=?;", (mid, sale_id)).fetchone()
    conn.close()
    shipment_id = sale_row[0] if sale_row else ""
    pack_id = sale_row[1] if sale_row else ""
    customer = sale_row[2] if sale_row else ""
    destino_db = sale_row[3] if sale_row else ""
    comuna_db = sale_row[4] if sale_row else ""
    ciudad_dest_db = sale_row[5] if sale_row else ""
    page_no = sale_row[6] if sale_row else ""
    mesa_db = sale_row[7] if sale_row else ""

    raw_label = _s2_get_label_raw(mid, shipment_id) if shipment_id else ""
    info = _s2_parse_label_raw_info(raw_label)

    st.markdown("### Etiqueta / Envío")
    a,b,cx = st.columns(3)
    a.metric("Envío", str(shipment_id) if shipment_id else "-")
    b.metric("Pack ID", str(pack_id) if pack_id else "-")
    cx.metric("Mesa / Página", f"{mesa_db}/{page_no}" if page_no else str(mesa_db))

    # Datos de destino / cliente (desde etiqueta si existe; fallback a DB)
    name = (info.get("destinatario") or customer or "-").strip() if isinstance(info, dict) else (customer or "-")
    dom = (info.get("domicilio") or info.get("direccion") or destino_db or "-").strip() if isinstance(info, dict) else (destino_db or "-")
    comuna = (info.get("comuna") or comuna_db or "-").strip() if isinstance(info, dict) else (comuna_db or "-")
    ciudad_dest = (info.get("ciudad_destino") or ciudad_dest_db or "-").strip() if isinstance(info, dict) else (ciudad_dest_db or "-")

    # Presentación compacta
    st.write(f"**Cliente:** {name}")
    if dom and dom != "-":
        st.write(f"**Domicilio:** {dom}")
    # FLEX: mostrar Comuna + Ciudad/Región (si vienen). COLECTA: ciudad_destino suele venir siempre.
    if comuna and comuna != "-":
        st.write(f"**Comuna:** {comuna}")
    if ciudad_dest and ciudad_dest != "-":
        st.write(f"**Ciudad destino:** {ciudad_dest}")
    items = _s2_sale_items(mid, sale_id)

    st.markdown("### Productos de la venta")
    total_items = len(items)
    done_items = sum(1 for _sku,_d,_q,_p,stx in items if stx in ("DONE","INCIDENCE"))
    st.progress(0 if total_items==0 else done_items/total_items)
    st.caption(f"{done_items}/{total_items} ítems finalizados (DONE o INCIDENCE)")

    for sku, desc, qty, picked, status in items:
        title = None
        if isinstance(inv_map_sku, dict):
            k = str(sku).strip()
            title = inv_map_sku.get(k)
            if title is None and k.isdigit():
                try:
                    title = inv_map_sku.get(str(int(k)))
                except Exception:
                    pass
        title = title or desc or str(sku)

        remaining = max(0, int(qty) - int(picked))
        row1 = st.columns([6, 2, 2])
        row1[0].markdown(f"### {title}  \nSKU: `{sku}`")
        row1[1].markdown(f"## {int(qty)}")
        row1[2].metric("Hecho", int(picked))

        # Imagen (solo bajo demanda para no ocupar espacio)
        _img_state_key = f"s2_showimg_{sale_id}_{sku}"
        if _img_state_key not in st.session_state:
            st.session_state[_img_state_key] = False

        if st.button("🖼️ Ver imagen", key=f"s2_btnimg_{sale_id}_{sku}"):
            st.session_state[_img_state_key] = not bool(st.session_state.get(_img_state_key, False))

        if st.session_state.get(_img_state_key, False):
            try:
                pics, _pub_link = get_picture_urls_for_sku(str(sku))
            except Exception:
                pics, _pub_link = [], ""
            if pics:
                st.image(pics[0], use_container_width=True)
            else:
                st.caption("Sin imagen disponible")

        if status != "DONE" and remaining > 0:
            bcols = st.columns([1,1,6])
            if bcols[0].button("⚠️ Incidencia", key=f"s2_inc_{sale_id}_{sku}"):
                _s2_mark_incidence(mid, sale_id, str(sku))
                st.rerun()
            if bcols[1].button("📝 Sin EAN", key=f"s2_noean_{sale_id}_{sku}"):
                _s2_force_done_no_ean(mid, sale_id, str(sku))
                st.rerun()
        st.divider()

    st.subheader("Escanea SKU/EAN del producto")
    st.caption("Escanea **1 vez**. Luego verificas la cantidad solicitada (sin digitar).")

    # Estado de confirmación por producto
    if "s2_pending_sku" not in st.session_state:
        st.session_state["s2_pending_sku"] = None
        st.session_state["s2_pending_qty"] = 0
        st.session_state["s2_pending_title"] = ""

    pending_sku = st.session_state.get("s2_pending_sku")

    # Limpieza segura del campo de producto (evita StreamlitAPIException)
    if st.session_state.get("s2_clear_prod_scan"):
        st.session_state["s2_prod_scan_widget"] = ""
        st.session_state["s2_clear_prod_scan"] = False

    sku_scan = st.text_input(
        "Producto",
        key="s2_prod_scan_widget",
        disabled=bool(pending_sku)  # mientras confirmas, bloquea nuevo escaneo
    )

    # 1) Al escanear: identificamos el SKU y preparamos la verificación automática de cantidad pendiente
    sku_scan = st.session_state.get("s2_prod_scan_widget", "").strip()
    if sku_scan and not pending_sku:
        sku = resolve_scan_to_sku(sku_scan, barcode_to_sku)

        # Buscar qty/picked del ítem dentro de esta venta
        connx = get_conn()
        cx = connx.cursor()
        cx.execute(
            "SELECT qty, picked, description FROM s2_items WHERE manifest_id=? AND sale_id=? AND sku=?;",
            (mid, sale_id, str(sku))
        )
        row = cx.fetchone()
        connx.close()

        if not row:
            st.error("SKU/EAN no pertenece a esta venta.")
            sfx_emit("ERR")
        else:
            qty_req, picked_now, desc_ml = int(row[0]), int(row[1]), row[2]
            remaining = max(0, qty_req - picked_now)

            # Resolver título visible (maestro > descripción > SKU)
            title_show = ""
            if isinstance(inv_map_sku, dict):
                k = str(sku).strip()
                title_show = inv_map_sku.get(k) or inv_map_sku.get(normalize_sku(k)) or ""
            title_show = title_show or (desc_ml or "") or str(sku)

            if remaining <= 0:
                st.info(f"✅ Ya está completo: {title_show}")
                st.session_state["s2_clear_prod_scan"] = True
                sfx_emit("ERR")
                st.rerun()
            else:
                sfx_emit("OK")
                st.session_state["s2_pending_sku"] = str(sku)
                st.session_state["s2_pending_qty"] = int(remaining)
                st.session_state["s2_pending_title"] = str(title_show)
                st.session_state["s2_clear_prod_scan"] = True
                st.rerun()

    # 2) Si hay un SKU pendiente: mostrar verificación de cantidad (sin digitar)
    pending_sku = st.session_state.get("s2_pending_sku")
    if pending_sku:
        pending_qty = int(st.session_state.get("s2_pending_qty", 0) or 0)
        pending_title = st.session_state.get("s2_pending_title", "") or pending_sku

        st.warning(f"Verificar **{pending_qty}** unidad(es) para: **{pending_title}**")
        cA, cB = st.columns([2, 1])
        with cA:
            if st.button(f"✅ Verificar {pending_qty} y cerrar producto", key=f"s2_verify_{sale_id}_{pending_sku}", use_container_width=True):
                ok, msg = _s2_apply_pick(mid, sale_id, str(pending_sku), int(pending_qty))
                if not ok:
                    st.error(msg or "No se pudo aplicar.")
                    sfx_emit("ERR")
                else:
                    sfx_emit("OK")
                    st.session_state["s2_pending_sku"] = None
                    st.session_state["s2_pending_qty"] = 0
                    st.session_state["s2_pending_title"] = ""
                    st.rerun()
        with cB:
            if st.button("Cancelar", key=f"s2_verify_cancel_{sale_id}_{pending_sku}", use_container_width=True):
                st.session_state["s2_pending_sku"] = None
                st.session_state["s2_pending_qty"] = 0
                st.session_state["s2_pending_title"] = ""
                st.rerun()

    done = _s2_is_sale_done(mid, sale_id)

    st.subheader("Cerrar venta")
    if done:
        c1, c2 = st.columns([1,2])
        with c1:
            confirm_close = st.checkbox("Confirmo cierre", key=f"s2_confirm_close_{sale_id}")
        with c2:
            if st.button("✅ Cerrar venta y volver a escanear etiqueta", key=f"s2_close_{sale_id}", use_container_width=True, disabled=not confirm_close):
                _s2_close_sale(mid, sale_id)
                st.session_state["s2_sale_open"] = None
                st.session_state["s2_sale_open_manifest_id"] = None
                st.session_state["s2_clear_prod_scan"] = True
                st.session_state["s2_clear_label_scan"] = True
                st.rerun()
    else:
        st.info("Para cerrar: completa todos los productos o márcalos como Incidencia / Sin EAN.")



def page_sorting_admin(inv_map_sku, barcode_to_sku):
    if not require_admin_access():
        return

    _s2_create_tables()
    _s2_migrate_staged_to_active()
    st.title("Administrador")

    _render_module_backup_ui("sorting", "Sorting", SORTING_TABLES)

    work_manifests = _s2_list_open_manifests()
    if not work_manifests:
        st.info("No hay lotes activos en Sorting.")
        if st.button("🧹 Reiniciar todo Sorting", key="s2_reset_no_lots"):
            _s2_reset_all_sorting()
            st.success("Sorting reiniciado.")
            st.rerun()
        return

    st.subheader("Lotes activos")
    resumen = []
    for mf in work_manifests:
        progreso = f"{max(0, mf['sales_total'] - mf['sales_pending'])}/{mf['sales_total']}" if mf['sales_total'] else "0/0"
        resumen.append({
            "Lote": mf["lot_label"],
            "Mesas": mf["mesas_text"],
            "Ventas": mf["sales_total"],
            "Pendientes": mf["sales_pending"],
            "Avance": progreso,
            "Control": mf["control_name"] or "-",
            "Etiquetas": mf["labels_name"] or "-",
        })
    st.dataframe(resumen, use_container_width=True, hide_index=True)

    options = {mf['lot_label']: int(mf['id']) for mf in work_manifests}
    default_label = st.session_state.get("s2_admin_lot_label")
    label_keys = list(options.keys())
    if default_label not in label_keys:
        default_label = label_keys[0]
    selected_label = st.selectbox("Ver lote", options=label_keys, index=label_keys.index(default_label), key="s2_admin_lot_label")
    mid = int(options[selected_label])

    conn = get_conn()
    c = conn.cursor()
    f = c.execute("SELECT control_name, labels_name, updated_at FROM s2_files WHERE manifest_id=?", (mid,)).fetchone()
    stats = _s2_get_stats(mid)

    st.subheader("Estado del lote")
    colA, colB, colC, colD = st.columns(4)
    colA.metric("Lote", selected_label)
    colB.metric("Ventas (Control)", stats.get("ventas", 0))
    colC.metric("Items", stats.get("items", 0))
    colD.metric("Etiquetas", stats.get("etiquetas", 0))

    if f:
        control_name, labels_name, updated_at = f
        st.caption(f"Mesas: {_s2_mesas_text_from_list(next((mf['mesas'] for mf in work_manifests if int(mf['id']) == mid), []))} · Control: {control_name or '-'} · Etiquetas: {labels_name or '-'} · Actualizado: {updated_at or '-'}")
    else:
        st.caption(f"Mesas: {_s2_mesas_text_from_list(next((mf['mesas'] for mf in work_manifests if int(mf['id']) == mid), []))}")

    st.divider()
    st.subheader("Trazabilidad")
    rows = c.execute(
        "SELECT mesa, COUNT(*) as ventas, SUM(CASE WHEN status='DONE' THEN 1 ELSE 0 END) as done FROM s2_sales WHERE manifest_id=? GROUP BY mesa ORDER BY mesa;",
        (mid,)
    ).fetchall()
    if rows:
        mesa_data = []
        for mesa, ventas, done in rows:
            ventas = int(ventas or 0)
            done = int(done or 0)
            mesa_data.append({
                "Mesa": int(mesa or 0),
                "Ventas": ventas,
                "Cerradas": done,
                "%": 0 if ventas == 0 else round(done * 100 / ventas, 1),
            })
        st.dataframe(mesa_data, use_container_width=True, hide_index=True)
    else:
        st.info("No hay ventas asignadas a mesas todavía.")

    st.divider()
    st.subheader("Incidencias")
    inc_rows = c.execute(
        """SELECT s.sale_id, s.mesa, s.shipment_id,
                  i.sku, i.description, i.qty, i.picked, i.status,
                  COALESCE(i.confirm_mode,'') as confirm_mode,
                  COALESCE(i.updated_at,'') as updated_at
             FROM s2_items i
             JOIN s2_sales s
               ON s.manifest_id=i.manifest_id AND s.sale_id=i.sale_id
            WHERE i.manifest_id=?
              AND (i.status='INCIDENCE' OR i.confirm_mode='MANUAL_NO_EAN')
            ORDER BY s.mesa, s.page_no, s.row_no, s.sale_id, i.sku;""",
        (mid,),
    ).fetchall()

    if inc_rows:
        df_inc = pd.DataFrame(inc_rows, columns=["Venta", "Mesa", "Envío", "SKU", "Descripción Control", "Solicitado", "Verificado", "Estado", "Modo", "Hora"])
        def _title_tec_for_sku(sku_val, fallback_desc=""):
            try:
                if isinstance(inv_map_sku, dict):
                    k = str(sku_val).strip()
                    t = inv_map_sku.get(k) or inv_map_sku.get(normalize_sku(k)) or ""
                    if t:
                        return t
            except Exception:
                pass
            return str(fallback_desc or sku_val or "")
        try:
            df_inc["Producto (técnico)"] = df_inc.apply(lambda r: _title_tec_for_sku(r["SKU"], r["Descripción Control"]), axis=1)
            df_inc = df_inc[["Mesa", "Venta", "Envío", "SKU", "Producto (técnico)", "Solicitado", "Verificado", "Estado", "Modo", "Hora"]]
        except Exception:
            pass
        st.dataframe(df_inc, use_container_width=True, hide_index=True)
    else:
        st.info("Sin incidencias ni productos marcados como Sin EAN en este lote.")

    st.divider()
    st.subheader("Ventas pendientes")
    pend = c.execute(
        "SELECT sale_id, mesa, shipment_id, status FROM s2_sales WHERE manifest_id=? AND status!='DONE' ORDER BY mesa, row_no, sale_id LIMIT 200;",
        (mid,),
    ).fetchall()
    if pend:
        pend_data = []
        for sale_id, mesa, shipment_id, status in pend:
            it = c.execute("SELECT COUNT(*), SUM(CASE WHEN status IN ('DONE','INCIDENCE') THEN 1 ELSE 0 END) FROM s2_items WHERE manifest_id=? AND sale_id=?;", (mid, sale_id)).fetchone()
            total = int(it[0] or 0)
            done = int(it[1] or 0)
            pend_data.append({"Venta": str(sale_id), "Mesa": int(mesa or 0), "Envío": str(shipment_id or ""), "Estado": str(status), "Items": f"{done}/{total}"})
        st.dataframe(pend_data, use_container_width=True, hide_index=True)
    else:
        st.success("No hay ventas pendientes: todo está cerrado.")

    with st.expander("Conciliación ventas ↔ etiquetas", expanded=False):
        st.write({
            "Lote": selected_label,
            "Envíos únicos (labels)": stats.get("distinct_ship_labels"),
            "Ventas con Pack ID": stats.get("ventas_with_pack"),
            "Packs distintos (Control)": stats.get("distinct_packs"),
            "Etiquetas con Pack ID": stats.get("labels_with_pack"),
            "Etiquetas con Venta": stats.get("labels_with_sale"),
            "Ventas matcheadas por Pack": stats.get("matched_by_pack"),
            "Ventas sin Envío asignado": stats.get("missing_ship"),
        })

    st.divider()
    st.subheader("Acciones")
    close_ok = (
        int(stats.get("sales_total", 0) or 0) > 0
        and int(stats.get("sales_pending", 0) or 0) == 0
        and int(stats.get("items_pending", 0) or 0) == 0
    )
    btn_close = st.button("✅ Cerrar lote", disabled=not close_ok)
    if not close_ok and int(stats.get("sales_total", 0) or 0) > 0:
        st.info("Este lote se podrá cerrar cuando todas las ventas estén cerradas y no queden ítems pendientes.")

    if btn_close:
        mesas_liberadas = sorted({int(r[1]) for r in _s2_get_assignments(mid)})
        _s2_close_manifest(mid)
        st.session_state["s2_upload_flash"] = f"{selected_label} cerrado. Mesas liberadas: {', '.join(map(str, mesas_liberadas)) if mesas_liberadas else '-'}"
        for k in ["s2_sale_open", "s2_sale_open_manifest_id", "sorting_manifest_id", "s2_admin_lot_label"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

    st.divider()
    st.subheader("Eliminar lote")
    st.caption("Esto borra solo el lote seleccionado y libera sus mesas para reutilizarlas.")
    if f"s2_delete_confirm_{mid}" not in st.session_state:
        st.session_state[f"s2_delete_confirm_{mid}"] = False
    arm_delete = st.checkbox("Quiero eliminar este lote", key=f"s2_delete_confirm_{mid}")
    do_delete = st.button("🗑️ Eliminar lote seleccionado", disabled=not arm_delete)
    if do_delete:
        mesas_liberadas = sorted({int(r[1]) for r in _s2_get_assignments(mid)})
        _s2_delete_manifest(mid)
        st.session_state["s2_upload_flash"] = f"{selected_label} eliminado. Mesas liberadas: {', '.join(map(str, mesas_liberadas)) if mesas_liberadas else '-'}"
        for k in ["s2_sale_open", "s2_sale_open_manifest_id", "sorting_manifest_id", "s2_admin_lot_label"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

    if "s2_reset_armed" not in st.session_state:
        st.session_state["s2_reset_armed"] = False

    arm = st.checkbox("Quiero reiniciar Sorting (entiendo que se borra todo)", value=st.session_state["s2_reset_armed"])
    st.session_state["s2_reset_armed"] = bool(arm)

    confirm_txt = st.text_input("Escribe BORRAR para confirmar", value="", disabled=not arm)
    do_reset = st.button("💥 Reiniciar Sorting completo", type="primary", disabled=not (arm and confirm_txt.strip().upper() == "BORRAR"))
    if do_reset:
        _s2_reset_all_sorting()
        for k in ["s2_sale_open", "s2_sale_open_manifest_id", "sorting_manifest_id", "s2_admin_lot_label", "s2_upload_flash"]:
            if k in st.session_state:
                del st.session_state[k]
        st.success("Sorting reiniciado correctamente.")
        st.rerun()


def _s2_pack_dispatch_create_tables():
    """Tablas auxiliares para Embalador y Despacho (no toca lógica Sorting)."""
    _s2_create_tables()
    conn = get_conn()
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS s2_packing (
        manifest_id INTEGER NOT NULL,
        sale_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PACKED',
        packed_at TEXT,
        packer TEXT,
        note TEXT,
        PRIMARY KEY (manifest_id, sale_id)
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS s2_dispatch (
        manifest_id INTEGER NOT NULL,
        sale_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'DISPATCHED',
        dispatched_at TEXT,
        dispatcher TEXT,
        note TEXT,
        PRIMARY KEY (manifest_id, sale_id)
    );""")
    conn.commit()
    conn.close()

def _s2_pick_manifest_for_packing() -> int:
    """Devuelve el manifest_id más antiguo que aún tenga cola de embalaje.
    Si no hay cola, cae al manifiesto activo.
    """
    _s2_pack_dispatch_create_tables()
    conn = get_conn(); c = conn.cursor()
    row = c.execute("""SELECT s.manifest_id
                         FROM s2_sales s
                         LEFT JOIN s2_packing p
                           ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                         WHERE s.status='DONE' AND p.sale_id IS NULL
                         GROUP BY s.manifest_id
                         ORDER BY s.manifest_id ASC
                         LIMIT 1;""").fetchone()
    conn.close()
    if row:
        return int(row[0])
    return _s2_get_active_manifest_id()

def _s2_pick_manifest_for_dispatch() -> int:
    """Devuelve el manifest_id más antiguo que aún tenga cola de despacho.
    (embaladas pero no despachadas). Si no hay cola, cae al manifiesto activo.
    """
    _s2_pack_dispatch_create_tables()
    conn = get_conn(); c = conn.cursor()
    row = c.execute("""SELECT s.manifest_id
                         FROM s2_sales s
                         JOIN s2_packing p
                           ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                         LEFT JOIN s2_dispatch d
                           ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                         WHERE s.status='DONE' AND d.sale_id IS NULL
                         GROUP BY s.manifest_id
                         ORDER BY s.manifest_id ASC
                         LIMIT 1;""").fetchone()
    conn.close()
    if row:
        return int(row[0])
    return _s2_get_active_manifest_id()


def _s2_list_mesas(mid:int):
    conn=get_conn(); c=conn.cursor()
    rows=c.execute("""SELECT DISTINCT mesa FROM s2_sales
                        WHERE manifest_id=? AND mesa IS NOT NULL
                        ORDER BY mesa;""", (mid,)).fetchall()
    conn.close()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def _s2_pack_stats_for_sales(mid:int, sale_ids:list):
    """Devuelve dict sale_id -> (n_items, units_total)"""
    if not sale_ids:
        return {}
    conn=get_conn(); c=conn.cursor()
    # SQLite: IN (...) seguro usando placeholders
    ph = ",".join(["?"]*len(sale_ids))
    q = f"""SELECT sale_id,
                    COUNT(*) as n_items,
                    SUM(COALESCE(qty,0)) as units
             FROM s2_items
             WHERE manifest_id=? AND sale_id IN ({ph})
             GROUP BY sale_id;"""
    rows = c.execute(q, [mid, *sale_ids]).fetchall()
    conn.close()
    out={}
    for sid, n_items, units in rows:
        out[str(sid)] = (int(n_items or 0), int(units or 0))
    return out


def _s2_find_done_sale_for_scan(mid:int, mesa, shipment_id:str):
    """Encuentra venta DONE (cerrada por Camarero) aún NO embalada."""
    _s2_pack_dispatch_create_tables()
    conn=get_conn(); c=conn.cursor()
    if mesa is None:
        row = c.execute("""SELECT s.sale_id
                             FROM s2_sales s
                             LEFT JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                             WHERE s.manifest_id=? AND s.shipment_id=? AND s.status='DONE'
                               AND p.sale_id IS NULL
                             ORDER BY s.page_no, s.row_no, s.sale_id
                             LIMIT 1;""", (mid, str(shipment_id))).fetchone()
    else:
        row = c.execute("""SELECT s.sale_id
                             FROM s2_sales s
                             LEFT JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                             WHERE s.manifest_id=? AND s.mesa=? AND s.shipment_id=? AND s.status='DONE'
                               AND p.sale_id IS NULL
                             ORDER BY s.page_no, s.row_no, s.sale_id
                             LIMIT 1;""", (mid, int(mesa), str(shipment_id))).fetchone()
    conn.close()
    return row[0] if row else None


def _s2_find_done_sale_for_pack_scan(mid:int, mesa, pack_id:str):
    """Fallback para Colecta: escaneo devuelve Pack ID."""
    _s2_pack_dispatch_create_tables()
    conn=get_conn(); c=conn.cursor()
    if mesa is None:
        row = c.execute("""SELECT s.sale_id
                             FROM s2_sales s
                             LEFT JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                             WHERE s.manifest_id=? AND s.pack_id=? AND s.status='DONE'
                               AND p.sale_id IS NULL
                             ORDER BY s.page_no, s.row_no, s.sale_id
                             LIMIT 1;""", (mid, str(pack_id))).fetchone()
    else:
        row = c.execute("""SELECT s.sale_id
                             FROM s2_sales s
                             LEFT JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                             WHERE s.manifest_id=? AND s.mesa=? AND s.pack_id=? AND s.status='DONE'
                               AND p.sale_id IS NULL
                             ORDER BY s.page_no, s.row_no, s.sale_id
                             LIMIT 1;""", (mid, int(mesa), str(pack_id))).fetchone()
    conn.close()
    return row[0] if row else None


def _s2_mark_packed(mid:int, sale_id:str, packer:str=""):
    _s2_pack_dispatch_create_tables()
    conn=get_conn(); c=conn.cursor()
    c.execute("""INSERT INTO s2_packing(manifest_id, sale_id, status, packed_at, packer)
                 VALUES(?,?,?,?,?)
                 ON CONFLICT(manifest_id, sale_id) DO UPDATE SET
                    status=excluded.status,
                    packed_at=excluded.packed_at,
                    packer=excluded.packer;""", (mid, str(sale_id), "PACKED", _s2_now_iso(), (packer or "")))
    conn.commit(); conn.close()


def _s2_mark_dispatched(mid:int, sale_id:str, dispatcher:str=""):
    _s2_pack_dispatch_create_tables()
    conn=get_conn(); c=conn.cursor()
    c.execute("""INSERT INTO s2_dispatch(manifest_id, sale_id, status, dispatched_at, dispatcher)
                 VALUES(?,?,?,?,?)
                 ON CONFLICT(manifest_id, sale_id) DO UPDATE SET
                    status=excluded.status,
                    dispatched_at=excluded.dispatched_at,
                    dispatcher=excluded.dispatcher;""", (mid, str(sale_id), "DISPATCHED", _s2_now_iso(), (dispatcher or "")))
    conn.commit(); conn.close()


def _s2_list_sales_to_pack(mid:int, mesa=None):
    _s2_pack_dispatch_create_tables()
    conn=get_conn(); c=conn.cursor()
    if mesa is None:
        rows = c.execute("""SELECT s.sale_id, s.shipment_id, s.pack_id, s.page_no, s.mesa, s.customer, s.destino, s.comuna, s.ciudad_destino
                            FROM s2_sales s
                            LEFT JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                            WHERE s.manifest_id=? AND s.status='DONE' AND p.sale_id IS NULL
                            ORDER BY s.page_no, s.row_no, s.sale_id;""", (mid,)).fetchall()
    else:
        rows = c.execute("""SELECT s.sale_id, s.shipment_id, s.pack_id, s.page_no, s.mesa, s.customer, s.destino, s.comuna, s.ciudad_destino
                            FROM s2_sales s
                            LEFT JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                            WHERE s.manifest_id=? AND s.mesa=? AND s.status='DONE' AND p.sale_id IS NULL
                            ORDER BY s.page_no, s.row_no, s.sale_id;""", (mid, int(mesa))).fetchall()
    conn.close()
    return rows


def _s2_list_sales_to_dispatch(mid:int, mesa=None):
    _s2_pack_dispatch_create_tables()
    conn=get_conn(); c=conn.cursor()
    if mesa is None:
        rows = c.execute("""SELECT s.sale_id, s.shipment_id, s.pack_id, s.page_no, s.mesa, s.customer, s.destino, s.comuna, s.ciudad_destino,
                                   p.packed_at, p.packer
                            FROM s2_sales s
                            JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                            LEFT JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                            WHERE s.manifest_id=? AND s.status='DONE' AND d.sale_id IS NULL
                            ORDER BY s.page_no, s.row_no, s.sale_id;""", (mid,)).fetchall()
    else:
        rows = c.execute("""SELECT s.sale_id, s.shipment_id, s.pack_id, s.page_no, s.mesa, s.customer, s.destino, s.comuna, s.ciudad_destino,
                                   p.packed_at, p.packer
                            FROM s2_sales s
                            JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                            LEFT JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                            WHERE s.manifest_id=? AND s.mesa=? AND s.status='DONE' AND d.sale_id IS NULL
                            ORDER BY s.page_no, s.row_no, s.sale_id;""", (mid, int(mesa))).fetchall()
    conn.close()
    return rows

def _s2_list_sales_dispatched(mid:int, mesa=None):
    """Lista ventas ya despachadas (historial) para un manifiesto/mesa."""
    _s2_pack_dispatch_create_tables()
    conn=get_conn(); c=conn.cursor()
    if mesa is None:
        rows = c.execute("""SELECT s.sale_id, s.shipment_id, s.pack_id, s.page_no, s.mesa, s.customer, s.destino, s.comuna, s.ciudad_destino,
                                       d.dispatched_at
                                FROM s2_sales s
                                JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                                WHERE s.manifest_id=?
                                ORDER BY d.dispatched_at DESC, s.page_no, s.row_no, s.sale_id;""", (mid,)).fetchall()
    else:
        rows = c.execute("""SELECT s.sale_id, s.shipment_id, s.pack_id, s.page_no, s.mesa, s.customer, s.destino, s.comuna, s.ciudad_destino,
                                       d.dispatched_at
                                FROM s2_sales s
                                JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                                WHERE s.manifest_id=? AND s.mesa=?
                                ORDER BY d.dispatched_at DESC, s.page_no, s.row_no, s.sale_id;""", (mid, int(mesa))).fetchall()
    conn.close()
    return rows



def page_packing(inv_map_sku: dict):
    _s2_pack_dispatch_create_tables()
    st.title("Embalador")
    st.caption("Flujo desde Sorting: solo aparecen ventas **cerradas por Camarero** (DONE) y aún **no embaladas**. **Se respeta estrictamente el orden de página del manifiesto.**")
    mid = _s2_pick_manifest_for_packing()

    # --- UI compacta (PDA) ---
    st.markdown("""
    <style>
      .pack-mini { font-size: 0.92rem; line-height: 1.15; }
      .pack-row { padding: 2px 0; border-bottom: 1px solid rgba(128,128,128,0.15); }
      .pack-sku { font-weight: 700; }
      .pack-desc { opacity: 0.85; }
    </style>
    """, unsafe_allow_html=True)

    mesas = _s2_list_mesas(mid)
    mesa_opt = ["Todas"] + [f"Mesa {m}" for m in mesas]
    sel = st.selectbox("Filtrar por mesa", mesa_opt, index=0)
    mesa = None
    if sel != "Todas":
        try:
            mesa = int(sel.split()[-1])
        except Exception:
            mesa = None

    # Lista pendiente (orden manifiesto: page_no, sale_id)
    rows = _s2_list_sales_to_pack(mid, mesa=mesa)
    sale_ids = [str(r[0]) for r in rows]
    stats = _s2_pack_stats_for_sales(mid, sale_ids)

    expected_sale = str(rows[0][0]) if rows else None
    if expected_sale:
        st.info(f"Siguiente por embalar (orden manifiesto): **{expected_sale}**  ·  Pendientes: **{len(rows)}**")
    else:
        st.success("No hay ventas pendientes de embalaje 🎉")

    # Estado: venta "bloqueada" para confirmar embalaje
    active_sale = st.session_state.get("pack_active_sale")

    # Limpieza segura del campo
    if st.session_state.get("pack_clear_scan"):
        st.session_state["pack_scan_widget"] = ""
        st.session_state["pack_clear_scan"] = False

    # Cache de thumbnails por SKU (evita repetir requests)
    thumb_cache = st.session_state.setdefault("pack_thumb_cache", {})

    def _thumb_for_sku(sku: str) -> str:
        sku = str(sku or "").strip()
        if not sku:
            return ""
        if sku in thumb_cache:
            return thumb_cache[sku] or ""
        try:
            pics, _ = get_picture_urls_for_sku(sku)
            thumb_cache[sku] = (pics[0] if pics else "")
        except Exception:
            thumb_cache[sku] = ""
        return thumb_cache[sku] or ""

    st.subheader("Escaneo (estricto por orden de página)")
    if not expected_sale:
        return

    # Si ya hay una venta activa, bloqueamos el escaneo de otra hasta confirmar/cancelar
    if active_sale:
        st.warning(f"Tienes una venta pendiente de confirmar embalaje: **{active_sale}**. Confirma para continuar.")
    scan = st.text_input(
        "Etiqueta (QR Flex / barra Colecta)",
        key="pack_scan_widget",
        disabled=bool(active_sale),
        help="Solo se acepta la siguiente venta del manifiesto."
    )

    if scan and (not active_sale):
        sid = _s2_extract_shipment_id(scan)
        sale_id = None

        if sid:
            sale_id = _s2_find_done_sale_for_scan(mid, mesa, sid)
        if not sale_id:
            # fallback pack_id: usamos el escaneo limpio como pack_id
            pack_id = str(scan).strip()
            sale_id = _s2_find_done_sale_for_pack_scan(mid, mesa, pack_id)

        if not sale_id:
            st.error("No encontré esta etiqueta para embalaje (¿no está cerrada en Sorting o ya fue embalada?).")
            sfx_emit("ERR")
        else:
            sale_id = str(sale_id)
            # Orden STRICTO: solo se acepta la primera venta pendiente (page_no asc)
            if sale_id != expected_sale:
                st.error(f"Fuera de orden. **Esperado:** {expected_sale} · **Escaneado:** {sale_id}")
                sfx_emit("ERR")
            else:
                st.session_state["pack_active_sale"] = sale_id
                st.success(f"✅ Etiqueta correcta: {sale_id}. Revisa el resumen y confirma el embalaje.")
                sfx_emit("OK")

        st.session_state["pack_clear_scan"] = True
        st.rerun()

    # Confirmación: mostrar productos + cantidades (con fotos pequeñas) antes de marcar como embalado
    active_sale = st.session_state.get("pack_active_sale")
    if active_sale:
        st.divider()
        st.subheader("Confirmación de embalaje")

        # Meta (etiqueta / cliente / destino)
        conn = get_conn(); c = conn.cursor()
        meta = c.execute("""SELECT sale_id, shipment_id, pack_id, page_no, mesa, customer, destino, comuna, ciudad_destino
                             FROM s2_sales WHERE manifest_id=? AND sale_id=? LIMIT 1;""",
                         (mid, str(active_sale))).fetchone()
        conn.close()

        if meta:
            _sale_id, _ship, _pack, _page, _mesa, _cust, _dest, _comuna, _ciudad = meta
            cols = st.columns(2)
            with cols[0]:
                st.write(f"**Venta:** {_sale_id}")
                if _ship:
                    st.write(f"**Envío:** {_ship}")
                if _pack:
                    st.write(f"**Pack:** {_pack}")
            with cols[1]:
                if _dest:
                    st.write(f"**Destino:** {_dest}")
                if _comuna:
                    st.write(f"**Comuna:** {_comuna}")
                if _ciudad:
                    st.write(f"**Ciudad destino:** {_ciudad}")
                if _cust:
                    st.write(f"**Cliente:** {_cust}")

        # Items de la venta
        items = _s2_sale_items(mid, str(active_sale))  # (sku, description, qty, picked, status)
        if not items:
            st.warning("No pude leer productos para esta venta (items vacíos). Aun así puedes confirmar el embalaje.")
        else:
            st.markdown('<div class="pack-mini">', unsafe_allow_html=True)
            for sku, desc, qty, picked, status in items:
                sku_s = str(sku)
                # Preferir título del maestro si existe
                maestro_title = ""
                try:
                    maestro_title = str(inv_map_sku.get(sku_s, "") or "").strip()
                except Exception:
                    maestro_title = ""

                desc_s = str(desc or "").strip()
                if maestro_title:
                    desc_s = maestro_title
                qty_s = int(qty) if str(qty).isdigit() else qty
                thumb = _thumb_for_sku(sku_s)

                c1, c2, c3 = st.columns([1.2, 6.6, 1.2], gap="small")
                with c1:
                    if thumb:
                        st.image(thumb, width=55)
                    else:
                        st.caption(" ")
                with c2:
                    # Resaltar incidencias en rojo (cuando Camarero marcó el SKU como INCIDENCE)
                    is_incid = (str(status).upper() == "INCIDENCE")
                    badge = ' <span style="color:#b00020;font-weight:800;">⚠ INCIDENCIA</span>' if is_incid else ""
                    desc_style = ' style="color:#b00020;font-weight:800;"' if is_incid else ""

                    st.markdown(
                        f'<div class="pack-row"><div class="pack-sku">{html.escape(sku_s)}</div>'
                        f'<div class="pack-desc"{desc_style}>{html.escape(desc_s)}{badge}</div></div>',
                        unsafe_allow_html=True
                    )
                with c3:
                    st.markdown(f"**x{qty_s}**")
            st.markdown("</div>", unsafe_allow_html=True)

        colA, colB = st.columns(2)
        with colA:
            if st.button("✅ Confirmar embalaje y pasar al siguiente", use_container_width=True):
                _s2_mark_packed(mid, str(active_sale), packer="")
                st.session_state["pack_active_sale"] = None
                sfx_emit("OK")
                st.rerun()
        with colB:
            if st.button("↩️ Cancelar", use_container_width=True):
                st.session_state["pack_active_sale"] = None
                sfx_emit("ERR")
                st.rerun()

    st.divider()
    st.subheader("Pendientes de embalaje")
    if not rows:
        return

    data = []
    for sale_id, shipment_id, pack_id, page_no, mesa_db, customer, destino, comuna, ciudad_destino in rows:
        n_items, units = stats.get(str(sale_id), (0, 0))
        cust = (customer or "").strip()
        # Sanitizar valores claramente erróneos (cuando el parser antiguo dejó basura)
        if re.search(r"\bSKU\s*:\b|\bVenta\s*:\b", cust, flags=re.I):
            cust = ""
        data.append({
            "Mesa": mesa_db,
            "Página": page_no,
            "Venta": sale_id,
            "Envío": shipment_id or "",
            "Pack": pack_id or "",
            "Destino": destino or "",
            "Comuna/Ciudad": (", ".join([x for x in [(comuna or "").strip(), (ciudad_destino or "").strip()] if x])) ,
            "Cliente": cust,
            "Productos": n_items,
            "Unidades": units,
        })
    st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
def page_dispatch():
    _s2_pack_dispatch_create_tables()
    st.title("Despacho")
    st.caption("Flujo desde Embalador: solo aparecen ventas **embaladas** y aún **no despachadas**.")
    mid = _s2_pick_manifest_for_dispatch()

    mesas = _s2_list_mesas(mid)
    mesa_opt = ["Todas"] + [f"Mesa {m}" for m in mesas]
    sel = st.selectbox("Filtrar por mesa", mesa_opt, index=0, key="dispatch_mesa_filter")
    mesa = None
    if sel != "Todas":
        try:
            mesa = int(sel.split()[-1])
        except Exception:
            mesa = None

    # En despacho NO obligamos el orden del manifiesto.
    # Solo debe calzar el total de ventas del control con el total despachado.
    enforce = False
    
    rows = _s2_list_sales_to_dispatch(mid, mesa=mesa)
    sale_ids = [str(r[0]) for r in rows]
    stats = _s2_pack_stats_for_sales(mid, sale_ids)

    # Totales según el control
    conn = get_conn(); c = conn.cursor()
    total_control = int(c.execute("SELECT COUNT(1) FROM s2_sales WHERE manifest_id=?;", (mid,)).fetchone()[0] or 0)
    total_despachadas = int(c.execute("SELECT COUNT(1) FROM s2_dispatch WHERE manifest_id=?;", (mid,)).fetchone()[0] or 0)
    conn.close()

    st.info(f"Control: **{total_control}** ventas · Despachadas: **{total_despachadas}** · Pendientes: **{max(0, total_control-total_despachadas)}**")


    if not rows:
        if total_control and total_despachadas >= total_control:
            st.success("Despacho completo ✅")
        else:
            st.warning("Aún no hay ventas disponibles para despacho (deben estar embaladas primero).")

    if st.session_state.get("dispatch_clear_scan"):
        st.session_state["dispatch_scan_widget"] = ""
        st.session_state["dispatch_clear_scan"] = False

    st.subheader("Escaneo de etiqueta para marcar DESPACHADO")
    scan = st.text_input("Etiqueta (QR Flex / barra Colecta)", key="dispatch_scan_widget")
    if scan:
        sid = _s2_extract_shipment_id(scan)
        sale_id = None
        conn=get_conn(); c=conn.cursor()
        if sid:
            if mesa is None:
                row = c.execute("""SELECT s.sale_id FROM s2_sales s
                                     JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                                     LEFT JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                                     WHERE s.manifest_id=? AND s.shipment_id=? AND s.status='DONE' AND d.sale_id IS NULL
                                     ORDER BY s.page_no, s.row_no, s.sale_id LIMIT 1;""", (mid, str(sid))).fetchone()
            else:
                row = c.execute("""SELECT s.sale_id FROM s2_sales s
                                     JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                                     LEFT JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                                     WHERE s.manifest_id=? AND s.mesa=? AND s.shipment_id=? AND s.status='DONE' AND d.sale_id IS NULL
                                     ORDER BY s.page_no, s.row_no, s.sale_id LIMIT 1;""", (mid, int(mesa), str(sid))).fetchone()
            sale_id = row[0] if row else None

        if not sale_id:
            # fallback pack_id
            pack_id = str(scan).strip()
            if mesa is None:
                row = c.execute("""SELECT s.sale_id FROM s2_sales s
                                     JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                                     LEFT JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                                     WHERE s.manifest_id=? AND s.pack_id=? AND s.status='DONE' AND d.sale_id IS NULL
                                     ORDER BY s.page_no, s.row_no, s.sale_id LIMIT 1;""", (mid, str(pack_id))).fetchone()
            else:
                row = c.execute("""SELECT s.sale_id FROM s2_sales s
                                     JOIN s2_packing p ON p.manifest_id=s.manifest_id AND p.sale_id=s.sale_id
                                     LEFT JOIN s2_dispatch d ON d.manifest_id=s.manifest_id AND d.sale_id=s.sale_id
                                     WHERE s.manifest_id=? AND s.mesa=? AND s.pack_id=? AND s.status='DONE' AND d.sale_id IS NULL
                                     ORDER BY s.page_no, s.row_no, s.sale_id LIMIT 1;""", (mid, int(mesa), str(pack_id))).fetchone()
            sale_id = row[0] if row else None
        conn.close()

        if not sale_id:
            st.error("No encontré esta etiqueta para despacho (¿no está embalada o ya fue despachada?).")
            sfx_emit("ERR")
        else:
            _s2_mark_dispatched(mid, str(sale_id), dispatcher=None)
            st.success(f"🚚 Despachado: {sale_id}")
            sfx_emit("OK")
        st.session_state["dispatch_clear_scan"] = True
        st.rerun()

    st.divider()
    st.subheader("Pendientes de despacho")
    if rows:
        data = []
        for sale_id, shipment_id, pack_id, page_no, mesa_db, customer, destino, comuna, ciudad_destino, packed_at, packer in rows:
            n_items, units = stats.get(str(sale_id), (0,0))
            data.append({
                "Mesa": mesa_db,
                "Página": page_no,
                "Venta": sale_id,
                "Envío": shipment_id or "",
                "Pack": pack_id or "",
                "Destino": destino or "",
                "Comuna/Ciudad": (", ".join([x for x in [(comuna or "").strip(), (ciudad_destino or "").strip()] if x])),
                "Cliente": customer or "",
                "Productos": n_items,
                "Unidades": units,
                "Embalado": packed_at or "",
                "Embalador": packer or "",
            })
        st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
    else:
        st.info("No hay pendientes de despacho para este filtro.")

    st.divider()
    st.subheader("Despachadas")
    done_rows = _s2_list_sales_dispatched(mid, mesa=mesa)
    if not done_rows:
        st.info("Aún no hay ventas despachadas.")
    else:
        out = []
        for sale_id, shipment_id, pack_id, page_no, mesa_db, customer, destino, comuna, ciudad_destino, dispatched_at in done_rows:
            out.append({
                "Mesa": mesa_db,
                "Página": page_no,
                "Venta": sale_id,
                "Envío": shipment_id or "",
                "Pack": pack_id or "",
                "Destino": destino or "",
                "Comuna/Ciudad": (", ".join([x for x in [(comuna or "").strip(), (ciudad_destino or "").strip()] if x])),
                "Cliente": customer or "",
                "Despachado": dispatched_at or "",
            })
        st.dataframe(pd.DataFrame(out), use_container_width=True, hide_index=True)



def main():

    st.set_page_config(page_title="Aurora ML – WMS", layout="wide")

    # 🔊 Sonidos globales (Sistema A)
    sfx_sidebar()
    _sfx_unlock_render()
    _sfx_global_click_hook()
    sfx_render_pending()
    init_db()

    # Auto-carga maestro desde repo (sirve para ambos modos)
    inv_map_sku, familia_map_sku, barcode_to_sku, conflicts = master_bootstrap(MASTER_FILE)

    # Auto-carga links de publicaciones (fotos por SKU) desde repo
    _ = publications_bootstrap(PUBLICATIONS_FILE)


    # Si no hay modo seleccionado, mostramos lobby y salimos
    if "app_mode" not in st.session_state:
        page_app_lobby()
        return

    # Sidebar común
    st.sidebar.title("Ferretería Aurora – WMS")

    # Botón para volver al lobby
    if st.sidebar.button("⬅️ Cambiar modo"):
        st.session_state.pop("app_mode", None)
        st.session_state.pop("selected_picker", None)
        st.session_state.pop("full_selected_batch", None)
        st.rerun()

    # Estado maestro (lo dejamos en sidebar, bajo el título)
    if os.path.exists(MASTER_FILE):
        st.sidebar.success(f"Maestro OK: {len(inv_map_sku)} SKUs / {len(barcode_to_sku)} EAN")
        if conflicts:
            st.sidebar.warning(f"Conflictos EAN: {len(conflicts)} (se usa el primero)")
    else:
        st.sidebar.warning(f"No se encontró {MASTER_FILE}. (La app funciona, pero sin maestro)")

    mode = st.session_state.get("app_mode", "FLEX_PICK")

    # ==========
    # MODO FLEX / COLECTA (lo actual)
    # ==========
    if mode == "FLEX_PICK":
        pages = [
            "1) Picking",
            "2) Importar ventas",
            "3) Cortes de la tanda (PDF)",
            "4) Administrador",
        ]
        page = st.sidebar.radio("Menú", pages, index=0)

        if page.startswith("1"):
            page_picking()
        elif page.startswith("2"):
            page_import(inv_map_sku, familia_map_sku)
        elif page.startswith("3"):
            page_cortes_pdf_batch()
        else:
            page_admin()

    elif mode == "SORTING":
        pages = [
            "1) Camarero",
            "2) Cargar manifiesto y asignar mesas",
            "3) Administrador",
        ]
        page = st.sidebar.radio("Menú", pages, index=0)

        if page.startswith("1"):
            page_sorting_camarero(inv_map_sku, barcode_to_sku)
        elif page.startswith("2"):
            page_sorting_upload(inv_map_sku, barcode_to_sku)
        else:
            page_sorting_admin(inv_map_sku, barcode_to_sku)

    elif mode == "PACKING":
        pages = [
            "1) Embalador",
        ]
        _ = st.sidebar.radio("Menú", pages, index=0)
        page_packing(inv_map_sku)

    elif mode == "DISPATCH":
        pages = [
            "1) Despacho",
        ]
        _ = st.sidebar.radio("Menú", pages, index=0)
        page_dispatch()

    # ==========
    # MODO FULL (nuevo módulo completo)
    # ==========
    elif mode == "PKG_COUNT":
        pages = [
            "1) Contador de paquetes",
        ]
        _ = st.sidebar.radio("Menú", pages, index=0)
        page_pkg_counter()

    else:
        pages = [
            "1) Cargar Excel Full",
            "2) Supervisor de acopio",
            "3) Admin Full (progreso)",
        ]
        page = st.sidebar.radio("Menú", pages, index=0)

        if page.startswith("1"):
            page_full_upload(inv_map_sku)
        elif page.startswith("2"):
            page_full_supervisor(inv_map_sku)
        else:
            page_full_admin()


if __name__ == "__main__":
    main()