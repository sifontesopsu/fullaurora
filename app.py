import io
import re
import html
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "Control FULL Aurora"
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "aurora_full.db"
MAESTRO_PATH = DATA_DIR / "maestro_sku_ean.xlsx"

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
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return re.sub(r"\s+", " ", s)


def normalize_header(v) -> str:
    s = clean_text(v).lower()
    trans = str.maketrans("áéíóúüñ", "aeiouun")
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


def find_col(columns, aliases):
    normalized = {normalize_header(c): c for c in columns}
    alias_norms = [normalize_header(a) for a in aliases]

    for a in alias_norms:
        if a in normalized:
            return normalized[a]

    for a in alias_norms:
        for n, original in normalized.items():
            if a and a in n:
                return original
    return None


def find_col_exact(columns, aliases):
    """Busca columnas solo por coincidencia exacta normalizada.
    Evita mezclar campos parecidos como Identificación / Vence.
    """
    normalized = {normalize_header(c): c for c in columns}
    for a in aliases:
        key = normalize_header(a)
        if key in normalized:
            return normalized[key]
    return None


def find_col_safe_contains(columns, aliases, forbidden=()):
    """Fallback controlado: permite contains, pero descarta encabezados conflictivos."""
    exact = find_col_exact(columns, aliases)
    if exact:
        return exact
    alias_norms = [normalize_header(a) for a in aliases]
    forbidden_norms = [normalize_header(f) for f in forbidden]
    for c in columns:
        n = normalize_header(c)
        if any(f and f in n for f in forbidden_norms):
            continue
        if any(a and a in n for a in alias_norms):
            return c
    return None


def split_codes(v):
    text = clean_text(v)
    if not text:
        return []
    parts = re.split(r"[,;/|\n\t ]+", text)
    codes = []
    for p in parts:
        c = norm_code(p)
        if c:
            codes.append(c)
    return list(dict.fromkeys(codes))


def is_supermercado(v) -> bool:
    return "SUPERMERCADO" in clean_text(v).upper()


# ============================================================
# Base de datos
# ============================================================

def ensure_column(conn, table: str, column: str, definition: str):
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db():
    with db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS full_lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                archivo TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS full_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL
            )
        """)
        ensure_column(c, "full_items", "area", "TEXT")
        ensure_column(c, "full_items", "nro", "TEXT")
        ensure_column(c, "full_items", "codigo_ml", "TEXT")
        ensure_column(c, "full_items", "codigo_universal", "TEXT")
        ensure_column(c, "full_items", "sku", "TEXT")
        ensure_column(c, "full_items", "descripcion", "TEXT")
        ensure_column(c, "full_items", "unidades", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "full_items", "acopiadas", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "full_items", "identificacion", "TEXT")
        ensure_column(c, "full_items", "vence", "TEXT")
        ensure_column(c, "full_items", "dia", "TEXT")
        ensure_column(c, "full_items", "hora", "TEXT")
        ensure_column(c, "full_items", "created_at", "TEXT")
        ensure_column(c, "full_items", "updated_at", "TEXT")
        c.execute("""
            CREATE TABLE IF NOT EXISTS full_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                scan_ml TEXT,
                scan_secundario TEXT,
                cantidad INTEGER NOT NULL,
                modo TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS full_maestro (
                code TEXT PRIMARY KEY,
                sku TEXT NOT NULL,
                descripcion TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        c.commit()

def list_lotes() -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT l.id, l.nombre, l.archivo, l.created_at,
                   COALESCE(SUM(i.unidades), 0) AS unidades,
                   COALESCE(SUM(i.acopiadas), 0) AS acopiadas,
                   COUNT(i.id) AS lineas
            FROM full_lotes l
            LEFT JOIN full_items i ON i.lote_id = l.id
            GROUP BY l.id
            ORDER BY l.id DESC
            """,
            c,
        )




def get_lote_info(lote_id: int):
    with db() as c:
        row = c.execute(
            "SELECT id, nombre, archivo, created_at FROM full_lotes WHERE id=?",
            (lote_id,),
        ).fetchone()
    return dict(row) if row else None


def get_last_scans(lote_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT s.item_id, MAX(s.created_at) AS procesado_at, COALESCE(SUM(s.cantidad), 0) AS escaneado_total
            FROM full_scans s
            WHERE s.lote_id=?
            GROUP BY s.item_id
            """,
            c,
            params=(lote_id,),
        )


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

def get_items(lote_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            "SELECT * FROM full_items WHERE lote_id=? ORDER BY area, CAST(nro AS INTEGER), id",
            c,
            params=(lote_id,),
        )


def create_lote(nombre: str, archivo: str, df: pd.DataFrame):
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for r in df.itertuples(index=False):
        rows.append((
            None,
            clean_text(getattr(r, "area", "")),
            clean_text(getattr(r, "nro", "")),
            norm_code(getattr(r, "codigo_ml", "")),
            norm_code(getattr(r, "codigo_universal", "")),
            norm_code(getattr(r, "sku", "")),
            clean_text(getattr(r, "descripcion", "")),
            int(getattr(r, "unidades", 0)),
            0,
            clean_text(getattr(r, "identificacion", "")),
            clean_text(getattr(r, "vence", "")),
            clean_text(getattr(r, "dia", "")),
            clean_text(getattr(r, "hora", "")),
            now,
            now,
        ))

    with db() as c:
        cur = c.execute("INSERT INTO full_lotes (nombre, archivo, created_at) VALUES (?, ?, ?)", (nombre, archivo, now))
        lote_id = cur.lastrowid
        rows = [(lote_id,) + row[1:] for row in rows]
        c.executemany(
            """
            INSERT INTO full_items
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades, acopiadas,
             identificacion, vence, dia, hora, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()
    return lote_id


def delete_lote(lote_id: int):
    with db() as c:
        c.execute("DELETE FROM full_scans WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM full_items WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM full_lotes WHERE id=?", (lote_id,))
        c.commit()


def add_acopio(lote_id: int, item_id: int, cantidad: int, scan_ml: str, scan_sec: str, modo: str):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        item = c.execute("SELECT * FROM full_items WHERE id=? AND lote_id=?", (item_id, lote_id)).fetchone()
        if not item:
            return False, "Producto no encontrado."
        pendiente = int(item["unidades"]) - int(item["acopiadas"])
        if pendiente <= 0:
            return False, "Este producto ya está completo."
        if cantidad <= 0:
            return False, "La cantidad debe ser mayor a cero."
        if cantidad > pendiente:
            return False, f"No puedes agregar {cantidad}. Solo quedan {pendiente} pendientes."

        c.execute(
            "UPDATE full_items SET acopiadas=acopiadas+?, updated_at=? WHERE id=?",
            (cantidad, now, item_id),
        )
        c.execute(
            """
            INSERT INTO full_scans (lote_id, item_id, scan_ml, scan_secundario, cantidad, modo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (lote_id, item_id, norm_code(scan_ml), norm_code(scan_sec), cantidad, modo, now),
        )
        c.commit()
    return True, "Cantidad agregada."


def undo_last_scan(lote_id: int):
    with db() as c:
        row = c.execute(
            "SELECT * FROM full_scans WHERE lote_id=? ORDER BY id DESC LIMIT 1",
            (lote_id,),
        ).fetchone()
        if not row:
            return False, "No hay escaneos para deshacer."
        now = datetime.now().isoformat(timespec="seconds")
        c.execute(
            "UPDATE full_items SET acopiadas=MAX(acopiadas-?, 0), updated_at=? WHERE id=?",
            (int(row["cantidad"]), now, int(row["item_id"])),
        )
        c.execute("DELETE FROM full_scans WHERE id=?", (int(row["id"]),))
        c.commit()
    return True, "Último escaneo deshecho."


# ============================================================
# Lectura Excel FULL: SIN vista previa y sin cruzar columnas
# ============================================================

def read_full_excel(uploaded_file) -> tuple[pd.DataFrame, list[str]]:
    warnings = []
    xls = pd.ExcelFile(uploaded_file)
    frames = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, dtype=object)
        raw = raw.dropna(how="all")
        if raw.empty:
            continue

        raw.columns = [clean_text(c) for c in raw.columns]

        col = {
            "area": find_col_safe_contains(raw.columns, ["Area", "Area."]),
            "nro": find_col_safe_contains(raw.columns, ["Nº", "N°", "Nro", "Numero", "Número"]),
            "codigo_ml": find_col_safe_contains(raw.columns, ["Código ML", "Codigo ML", "Cod ML", "COD ML"]),
            "codigo_universal": find_col_safe_contains(raw.columns, ["Código Universal", "Codigo Universal", "Cod Universal", "COD UNIVERSAL", "EAN"]),
            "sku": find_col_exact(raw.columns, ["SKU", "SKU ML"]),
            "descripcion": find_col_safe_contains(raw.columns, ["Descripción", "Descripcion", "Producto", "Title", "Titulo", "Título"]),
            "unidades": find_col_exact(raw.columns, ["Unidades", "Cantidad", "Cant"]),
            # FULL viene con estructura fija. Se fuerzan por posición para no mezclar columnas:
            # H = Identificación / Etiquetado, I = Vence / Vencimiento.
            "identificacion": raw.columns[7] if len(raw.columns) > 7 else find_col_exact(raw.columns, ["Identificación", "Identificacion"]),
            "vence": raw.columns[8] if len(raw.columns) > 8 else find_col_exact(raw.columns, ["Vence", "Vencimiento"]),
            "dia": raw.columns[9] if len(raw.columns) > 9 else find_col_exact(raw.columns, ["Dia", "Día"]),
            "hora": raw.columns[10] if len(raw.columns) > 10 else find_col_exact(raw.columns, ["Hora"]),
        }

        if not col["unidades"] or not (col["sku"] or col["codigo_ml"] or col["codigo_universal"]):
            warnings.append(f"Hoja omitida: {sheet}")
            continue

        df = pd.DataFrame()
        for k in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]:
            if col.get(k):
                df[k] = raw[col[k]]
            else:
                df[k] = ""

        for k in ["area", "nro", "descripcion", "identificacion", "vence", "dia", "hora"]:
            df[k] = df[k].map(clean_text)
        for k in ["codigo_ml", "codigo_universal", "sku"]:
            df[k] = df[k].map(norm_code)
        df["unidades"] = df["unidades"].map(to_int)

        df = df[(df["unidades"] > 0) & ((df["sku"] != "") | (df["codigo_ml"] != "") | (df["codigo_universal"] != ""))]
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]), warnings

    return pd.concat(frames, ignore_index=True), warnings


# ============================================================
# Maestro SKU/EAN local en data/maestro_sku_ean.xlsx
# ============================================================

def parse_maestro(file_or_path) -> pd.DataFrame:
    xls = pd.ExcelFile(file_or_path)
    frames = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, dtype=object).dropna(how="all")
        if raw.empty:
            continue
        raw.columns = [clean_text(c) for c in raw.columns]

        sku_col = find_col(raw.columns, ["SKU", "SKU ML", "sku_ml"])
        desc_col = find_col(raw.columns, ["Descripción", "Descripcion", "Producto", "Title", "Titulo"])
        barcode_cols = []
        for c in raw.columns:
            h = normalize_header(c)
            if any(x in h for x in ["ean", "barra", "barcode", "codigo universal", "cod universal", "codigo de barras"]):
                barcode_cols.append(c)

        if not sku_col:
            continue
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


def import_maestro(df: pd.DataFrame):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        c.execute("DELETE FROM full_maestro")
        c.executemany(
            "INSERT OR REPLACE INTO full_maestro (code, sku, descripcion, updated_at) VALUES (?, ?, ?, ?)",
            [(norm_code(r.code), norm_code(r.sku), clean_text(r.descripcion), now) for r in df.itertuples(index=False)],
        )
        c.commit()


def load_maestro_from_repo() -> tuple[int, str]:
    if not MAESTRO_PATH.exists():
        return 0, "no encontrado"
    df = parse_maestro(MAESTRO_PATH)
    if df.empty:
        return 0, "archivo vacío o columnas no reconocidas"
    import_maestro(df)
    return len(df), "data/maestro_sku_ean.xlsx"


def maestro_lookup(code: str) -> str:
    code_n = norm_code(code)
    if not code_n:
        return ""
    with db() as c:
        row = c.execute("SELECT sku FROM full_maestro WHERE code=?", (code_n,)).fetchone()
    return clean_text(row["sku"]) if row else ""


# ============================================================
# Matching
# ============================================================

def pending_items(items: pd.DataFrame) -> pd.DataFrame:
    if items.empty:
        return items
    p = items.copy()
    p["pendiente"] = (p["unidades"].astype(int) - p["acopiadas"].astype(int)).clip(lower=0)
    return p[p["pendiente"] > 0]


def match_ml(items: pd.DataFrame, scan_ml: str) -> pd.DataFrame:
    code = norm_code(scan_ml)
    p = pending_items(items)
    return p[p["codigo_ml"].map(norm_code) == code] if code else p.iloc[0:0]


def match_secondary(items: pd.DataFrame, scan_sec: str, only_super=None) -> pd.DataFrame:
    code = norm_code(scan_sec)
    if not code:
        return items.iloc[0:0]
    sku_from_master = norm_code(maestro_lookup(code))
    p = pending_items(items)
    if only_super is True:
        p = p[p["identificacion"].map(is_supermercado)]
    elif only_super is False:
        p = p[~p["identificacion"].map(is_supermercado)]
    mask = (p["sku"].map(norm_code) == code) | (p["codigo_universal"].map(norm_code) == code)
    if sku_from_master:
        mask = mask | (p["sku"].map(norm_code) == sku_from_master)
    return p[mask]


def best_match(matches: pd.DataFrame):
    if matches.empty:
        return None
    m = matches.copy()
    m["pendiente"] = (m["unidades"].astype(int) - m["acopiadas"].astype(int)).clip(lower=0)
    return m.sort_values(["pendiente", "id"], ascending=[False, True]).iloc[0]


def reset_scan_state():
    st.session_state["scan_ml"] = ""
    st.session_state["scan_sec"] = ""
    st.session_state["sin_ean"] = False
    st.session_state["primary_validated"] = False
    st.session_state["primary_code"] = ""
    st.session_state["candidate_id"] = None
    st.session_state["candidate_mode"] = ""


def etiqueta_operativa(v: str) -> str:
    s = clean_text(v)
    u = s.upper()
    if not s:
        return ""
    if "SUPERMERCADO" in u:
        return "SUPERMERCADO"
    if "OBLIGATORIO" in u:
        return "Etiquetado obligatorio"
    # Importante: "SI" pertenece a Vencimiento, no a Etiquetado.
    # No se convierte en etiquetado obligatorio para evitar mezclar columnas H/I.
    return s


def get_item_row(items: pd.DataFrame, item_id):
    try:
        iid = int(item_id)
    except Exception:
        return None
    m = items[items["id"].astype(int) == iid]
    if m.empty:
        return None
    return m.iloc[0]


# ============================================================
# Exportación
# ============================================================

def export_lote(lote_id: int) -> bytes:
    items = get_items(lote_id)
    if not items.empty:
        items["pendiente"] = (items["unidades"].astype(int) - items["acopiadas"].astype(int)).clip(lower=0)
        items["estado"] = items["pendiente"].apply(lambda x: "COMPLETO" if int(x) == 0 else "PENDIENTE")
    with db() as c:
        scans = pd.read_sql_query(
            "SELECT created_at, item_id, scan_ml, scan_secundario, cantidad, modo FROM full_scans WHERE lote_id=? ORDER BY id DESC",
            c,
            params=(lote_id,),
        )
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        items.to_excel(writer, sheet_name="control_full", index=False)
        scans.to_excel(writer, sheet_name="escaneos", index=False)
    return out.getvalue()


# ============================================================
# UI
# ============================================================

init_db()
maestro_count, maestro_source = load_maestro_from_repo()

st.markdown("""
<style>
/* Ajustes PDA solo para operación: campos, botones y métricas más grandes */
div[data-testid="stTextInput"] label, div[data-testid="stNumberInput"] label {
    font-size: 1.25rem !important;
    font-weight: 800 !important;
}
div[data-testid="stTextInput"] input, div[data-testid="stNumberInput"] input {
    font-size: 1.55rem !important;
    min-height: 3.3rem !important;
}
.stButton > button {
    font-size: 1.25rem !important;
    min-height: 3.2rem !important;
    width: 100%;
    font-weight: 800 !important;
}
div[data-testid="stMetricValue"] {
    font-size: 1.85rem !important;
}
div[data-testid="stMetricLabel"] {
    font-size: 1.05rem !important;
}
.operador-producto {
    font-size: 1.35rem;
    font-weight: 800;
    line-height: 1.25;
}
.operador-ok {
    font-size: 1.3rem;
    font-weight: 800;
}

.control-card {border:1px solid #E5E7EB;border-radius:14px;padding:14px 16px;margin:10px 0;background:#FFFFFF;}
.control-title {font-size:1.05rem;font-weight:800;line-height:1.3;margin-bottom:8px;}
.control-meta {font-size:.92rem;color:#374151;margin-bottom:8px;}
.badge {display:inline-block;padding:5px 9px;border-radius:999px;background:#F3F4F6;margin:3px 4px 3px 0;font-size:.9rem;font-weight:700;}
.badge-alert {background:#FFF7ED;}
.badge-ok {background:#ECFDF5;}
.badge-pending {background:#FEF2F2;}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("Menú")
    page = st.radio("Vista", ["Escaneo", "Cargar lote FULL", "Control"], label_visibility="collapsed")
    st.divider()
    lotes = list_lotes()
    if lotes.empty:
        active_lote = None
        st.info("Sin lotes creados.")
    else:
        options = {f"{r.nombre} · {int(r.acopiadas)}/{int(r.unidades)}": int(r.id) for r in lotes.itertuples(index=False)}
        active_lote = options[st.selectbox("Lote activo", list(options.keys()))]

if page == "Cargar lote FULL":
    st.subheader("Cargar lote FULL")
    full_file = st.file_uploader("Excel FULL", type=["xlsx"])
    if full_file:
        df, warns = read_full_excel(full_file)
        for w in warns:
            st.warning(w)
        if df.empty:
            st.error("No se pudieron leer productos válidos desde el Excel.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Líneas leídas", len(df))
            c2.metric("Unidades", int(df["unidades"].sum()))
            c3.metric("SKUs únicos", df["sku"].nunique())

            nombre = st.text_input("Nombre del lote", value=f"FULL {datetime.now().strftime('%d-%m-%Y %H:%M')}")
            if st.button("Crear lote", type="primary"):
                create_lote(nombre, full_file.name, df)
                reset_scan_state()
                st.success("Lote creado correctamente.")
                st.rerun()

elif page == "Escaneo":
    if not active_lote:
        st.warning("Primero crea un lote FULL.")
    else:
        items = get_items(active_lote)
        if items.empty:
            st.warning("El lote activo no tiene productos.")
        else:
            total = int(items["unidades"].sum())
            done = int(items["acopiadas"].sum())
            st.progress(done / total if total else 0)
            a, b, c = st.columns(3)
            a.metric("Solicitado", total)
            b.metric("Acopiado", done)
            c.metric("Pendiente", max(total - done, 0))

            st.divider()

            # Estados mínimos para que la PDA no valide automáticamente al escanear.
            if "primary_validated" not in st.session_state:
                st.session_state["primary_validated"] = False
            if "primary_code" not in st.session_state:
                st.session_state["primary_code"] = ""
            if "candidate_id" not in st.session_state:
                st.session_state["candidate_id"] = None
            if "candidate_mode" not in st.session_state:
                st.session_state["candidate_mode"] = ""

            st.text_input("Código ML o EAN supermercado", key="scan_ml")

            col_val, col_limpiar = st.columns([2, 1])
            with col_val:
                validar_primario = st.button("Validar código", type="primary")
            with col_limpiar:
                limpiar = st.button("Limpiar")

            if limpiar:
                reset_scan_state()
                st.rerun()

            scan_ml_v = st.session_state.get("scan_ml", "")
            scan_sec_v = st.session_state.get("scan_sec", "")
            candidate = None
            modo = ""

            if validar_primario:
                st.session_state["candidate_id"] = None
                st.session_state["candidate_mode"] = ""
                st.session_state["primary_validated"] = False
                st.session_state["primary_code"] = norm_code(scan_ml_v)
                st.session_state["scan_sec"] = ""
                st.session_state["sin_ean"] = False

                if not norm_code(scan_ml_v):
                    st.error("Escanea o ingresa un código antes de validar.")
                else:
                    # SUPERMERCADO: único caso en que el primer campo acepta EAN/Código Universal/SKU.
                    sm = match_secondary(items, scan_ml_v, only_super=True)
                    if not sm.empty:
                        cand = best_match(sm)
                        st.session_state["candidate_id"] = int(cand["id"])
                        st.session_state["candidate_mode"] = "SUPERMERCADO"
                        st.session_state["primary_validated"] = True
                    else:
                        m1 = match_ml(items, scan_ml_v)
                        if m1.empty:
                            st.error("Código no encontrado en productos pendientes.")
                        else:
                            st.session_state["primary_validated"] = True

            primary_ok = bool(st.session_state.get("primary_validated", False))
            primary_code = st.session_state.get("primary_code", "")
            candidate_id = st.session_state.get("candidate_id", None)
            candidate_mode = st.session_state.get("candidate_mode", "")

            if candidate_id:
                candidate = get_item_row(items, candidate_id)
                modo = candidate_mode

            elif primary_ok and primary_code:
                m1 = match_ml(items, primary_code)
                if not m1.empty:
                    preview = best_match(m1)
                    if preview is not None:
                        p_preview = int(preview["unidades"]) - int(preview["acopiadas"])
                        st.markdown(f"<div class='operador-producto'>{esc(preview['descripcion'])}</div>", unsafe_allow_html=True)
                        q1, q2, q3 = st.columns(3)
                        q1.metric("Solicitadas", int(preview["unidades"]))
                        q2.metric("Acopiadas", int(preview["acopiadas"]))
                        q3.metric("Pendientes", max(p_preview, 0))

                    st.text_input("SKU / EAN / Código Universal", key="scan_sec")
                    b1, b2 = st.columns(2)
                    with b1:
                        validar_sec = st.button("Validar SKU/EAN", type="primary")
                    with b2:
                        sin_ean_btn = st.button("Sin EAN")

                    if sin_ean_btn:
                        m_no_super = m1[~m1["identificacion"].map(is_supermercado)]
                        if m_no_super.empty:
                            st.error("No encontré ese Código ML pendiente para usar Sin EAN.")
                        else:
                            cand = best_match(m_no_super)
                            st.session_state["candidate_id"] = int(cand["id"])
                            st.session_state["candidate_mode"] = "SIN_EAN"
                            st.session_state["sin_ean"] = True
                            st.rerun()

                    if validar_sec:
                        scan_sec_v = st.session_state.get("scan_sec", "")
                        if not norm_code(scan_sec_v):
                            st.error("Escanea o ingresa el SKU/EAN antes de validar.")
                        else:
                            m2 = match_secondary(m1, scan_sec_v, only_super=False)
                            if m2.empty:
                                st.error("El SKU/EAN/Código Universal no corresponde a este producto.")
                            else:
                                cand = best_match(m2)
                                st.session_state["candidate_id"] = int(cand["id"])
                                st.session_state["candidate_mode"] = "ML+SECUNDARIO"
                                st.rerun()

            if candidate is not None:
                pendiente = int(candidate["unidades"]) - int(candidate["acopiadas"])
                st.markdown("<div class='operador-ok'>Producto validado</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='operador-producto'>{esc(candidate['descripcion'])}</div>", unsafe_allow_html=True)
                x1, x2, x3, x4 = st.columns(4)
                x1.metric("SKU", candidate["sku"])
                x2.metric("Solicitadas", int(candidate["unidades"]))
                x3.metric("Acopiadas", int(candidate["acopiadas"]))
                x4.metric("Pendientes", max(pendiente, 0))
                qty = st.number_input("Cantidad a agregar", min_value=1, max_value=max(pendiente, 1), value=1, step=1)
                if st.button("Agregar cantidad", type="primary"):
                    ok, msg = add_acopio(active_lote, int(candidate["id"]), int(qty), st.session_state.get("scan_ml", ""), st.session_state.get("scan_sec", ""), modo)
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
                if ok:
                    st.rerun()

elif page == "Control":
    st.subheader("Control de lote")
    if not active_lote:
        st.warning("No hay lote activo.")
    else:
        items = get_items(active_lote)
        if items.empty:
            st.warning("El lote no tiene productos.")
        else:
            view = items.copy()
            view["pendiente"] = (view["unidades"].astype(int) - view["acopiadas"].astype(int)).clip(lower=0)
            view["estado"] = view["pendiente"].apply(lambda x: "COMPLETO" if int(x) == 0 else "PENDIENTE")
            total = int(view["unidades"].sum())
            done = int(view["acopiadas"].sum())
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Unidades", total)
            c2.metric("Acopiadas", done)
            c3.metric("Pendientes", max(total - done, 0))
            c4.metric("Avance", f"{(done / total * 100) if total else 0:.1f}%")

            filtro = st.selectbox("Filtro", ["Todos", "Pendientes", "Completos", "Supermercado"])
            show = view
            if filtro == "Pendientes":
                show = view[view["pendiente"] > 0]
            elif filtro == "Completos":
                show = view[view["pendiente"] == 0]
            elif filtro == "Supermercado":
                show = view[view["identificacion"].map(is_supermercado)]

            lote_info = get_lote_info(active_lote) or {}
            scans = get_last_scans(active_lote)
            show_clean = show.copy()
            if not scans.empty:
                show_clean = show_clean.merge(scans, left_on="id", right_on="item_id", how="left")
            else:
                show_clean["procesado_at"] = ""
                show_clean["escaneado_total"] = 0

            cargado_el = fmt_dt(lote_info.get("created_at", ""))
            archivo = clean_text(lote_info.get("archivo", ""))
            nombre_lote = clean_text(lote_info.get("nombre", ""))
            ultima_accion = fmt_dt(show_clean["updated_at"].dropna().max()) if "updated_at" in show_clean.columns and not show_clean.empty else ""

            st.markdown("### Información del lote")
            m1, m2, m3 = st.columns(3)
            m1.metric("Lote", nombre_lote or active_lote)
            m2.metric("Cargado", cargado_el or "-")
            m3.metric("Último movimiento", ultima_accion or "-")
            if archivo:
                st.caption(f"Archivo cargado: {archivo}")

            show_clean["Producto"] = show_clean["descripcion"].map(clean_text)
            show_clean["SKU"] = show_clean["sku"].map(norm_code)
            show_clean["Código ML"] = show_clean["codigo_ml"].map(norm_code)
            show_clean["EAN / Código universal"] = show_clean["codigo_universal"].map(norm_code)
            show_clean["Unidades"] = show_clean["unidades"].astype(int)
            show_clean["Acopiadas"] = show_clean["acopiadas"].astype(int)
            show_clean["Pendiente"] = show_clean["pendiente"].astype(int)
            show_clean["Etiquetado"] = show_clean["identificacion"].map(etiqueta_operativa)
            show_clean["Vencimiento"] = show_clean["vence"].map(clean_text)
            show_clean["Procesado"] = show_clean["procesado_at"].map(fmt_dt)
            show_clean["Estado"] = show_clean["estado"].map(clean_text)

            modo_vista = st.radio("Vista", ["Tarjetas operativas", "Tabla"], horizontal=True)

            if modo_vista == "Tarjetas operativas":
                for _, r in show_clean.iterrows():
                    sku = clean_text(r.get("SKU", ""))
                    cod_ml = clean_text(r.get("Código ML", ""))
                    producto = clean_text(r.get("Producto", ""))
                    unidades = int(r.get("Unidades", 0))
                    acopiadas = int(r.get("Acopiadas", 0))
                    pendiente = int(r.get("Pendiente", 0))
                    etiquetado = etiqueta_operativa(r.get("Etiquetado", ""))
                    procesado = clean_text(r.get("Procesado", "")) or "Sin procesar"
                    badge_etiqueta = "badge-alert" if etiquetado else ""
                    st.markdown(
                        f"""
                        <div class="control-card">
                            <div class="control-title">{esc(producto)}</div>
                            <div class="control-meta"><b>SKU:</b> {esc(sku)} &nbsp; | &nbsp; <b>Código ML:</b> {esc(cod_ml or '-')}</div>
                            <span class="badge">Unidades: {unidades}</span>
                            <span class="badge">Acopiadas: {acopiadas}</span>
                            <span class="badge">Pendiente: {pendiente}</span>
                            {f'<span class="badge {badge_etiqueta}">Etiquetado: {esc(etiquetado)}</span>' if etiquetado else ''}
                            <span class="badge">Procesado: {esc(procesado)}</span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
            else:
                display_cols = ["SKU", "Código ML", "EAN / Código universal", "Producto", "Unidades", "Acopiadas", "Pendiente", "Etiquetado", "Vencimiento", "Procesado", "Estado"]
                st.dataframe(
                    show_clean[display_cols],
                    use_container_width=True,
                    hide_index=True,
                    height=620,
                    column_config={
                        "SKU": st.column_config.TextColumn("SKU", width="medium"),
                        "Código ML": st.column_config.TextColumn("Código ML", width="medium"),
                        "EAN / Código universal": st.column_config.TextColumn("EAN / Código universal", width="medium"),
                        "Producto": st.column_config.TextColumn("Producto", width="large"),
                        "Unidades": st.column_config.NumberColumn("Unidades", width="small"),
                        "Acopiadas": st.column_config.NumberColumn("Acopiadas", width="small"),
                        "Pendiente": st.column_config.NumberColumn("Pendiente", width="small"),
                        "Etiquetado": st.column_config.TextColumn("Etiquetado", width="large"),
                        "Vencimiento": st.column_config.TextColumn("Vencimiento", width="medium"),
                        "Procesado": st.column_config.TextColumn("Procesado", width="medium"),
                        "Estado": st.column_config.TextColumn("Estado", width="medium"),
                    },
                )

            st.download_button("Exportar control Excel", data=export_lote(active_lote), file_name="control_full_aurora.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            st.divider()
            if st.button("Eliminar lote activo"):
                delete_lote(active_lote)
                st.success("Lote eliminado.")
                st.rerun()
