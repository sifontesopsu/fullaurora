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
DB_PATH = DATA_DIR / "aurora_full_v2.db"   # Base nueva para no arrastrar datos contaminados
MAESTRO_PATH = DATA_DIR / "maestro_sku_ean.xlsx"

st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")

# ============================================================
# Limpieza y normalización
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


def is_supermercado(v) -> bool:
    return "SUPERMERCADO" in clean_text(v).upper()


# ============================================================
# Mapeo de columnas FULL: estricto y sin mezclar Identificación/Vence
# ============================================================

COLUMN_ALIASES = {
    "area": ["area", "area."],
    "nro": ["n", "no", "nro", "numero", "número", "nº", "n°"],
    "codigo_ml": ["codigo ml", "cod ml", "código ml"],
    "codigo_universal": ["codigo universal", "código universal", "cod universal", "ean"],
    "sku": ["sku", "sku ml"],
    "descripcion": ["descripcion", "descripción", "producto", "titulo", "título", "title"],
    "unidades": ["unidades", "cantidad", "cant"],
    # Importante: Identificación NO se cruza con Vence.
    "identificacion": ["identificacion", "identificación", "etiqueta", "etiq", "instrucciones de preparacion", "instrucciones de preparación"],
    "vence": ["vence", "vencimiento", "vcto", "fecha vencimiento", "fecha de vencimiento"],
    "dia": ["dia", "día"],
    "hora": ["hora"],
}


def find_col_exact(columns, aliases):
    normalized = {normalize_header(c): c for c in columns}
    for alias in aliases:
        key = normalize_header(alias)
        if key in normalized:
            return normalized[key]
    return None


def map_full_columns(columns):
    mapped = {}
    for key, aliases in COLUMN_ALIASES.items():
        mapped[key] = find_col_exact(columns, aliases)
    return mapped


def read_full_excel(uploaded_file) -> tuple[pd.DataFrame, list[str]]:
    warnings = []
    xls = pd.ExcelFile(uploaded_file)
    frames = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, dtype=object).dropna(how="all")
        if raw.empty:
            continue

        raw.columns = [clean_text(c) for c in raw.columns]
        col = map_full_columns(raw.columns)

        missing = []
        for required in ["codigo_ml", "sku", "descripcion", "unidades"]:
            if not col.get(required):
                missing.append(required)

        if missing:
            warnings.append(f"Hoja omitida '{sheet}': faltan columnas obligatorias: {', '.join(missing)}")
            continue

        df = pd.DataFrame()
        for k in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]:
            source_col = col.get(k)
            df[k] = raw[source_col] if source_col else ""

        for k in ["area", "nro", "descripcion", "identificacion", "vence", "dia", "hora"]:
            df[k] = df[k].map(clean_text)
        for k in ["codigo_ml", "codigo_universal", "sku"]:
            df[k] = df[k].map(norm_code)
        df["unidades"] = df["unidades"].map(to_int)

        df = df[(df["unidades"] > 0) & ((df["sku"] != "") | (df["codigo_ml"] != "") | (df["codigo_universal"] != ""))]
        if not df.empty:
            df["hoja_origen"] = sheet
            frames.append(df)

    columns = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora", "hoja_origen"]
    if not frames:
        return pd.DataFrame(columns=columns), warnings
    return pd.concat(frames, ignore_index=True)[columns], warnings


# ============================================================
# Base de datos nueva
# ============================================================

def init_db():
    with db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                archivo TEXT,
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
                hoja_origen TEXT,
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
        c.commit()


def list_lotes() -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT l.id, l.nombre, l.archivo, l.created_at,
                   COALESCE(SUM(i.unidades), 0) AS unidades,
                   COALESCE(SUM(i.acopiadas), 0) AS acopiadas,
                   COUNT(i.id) AS lineas
            FROM lotes l
            LEFT JOIN items i ON i.lote_id = l.id
            GROUP BY l.id
            ORDER BY l.id DESC
            """,
            c,
        )


def get_lote(lote_id: int):
    with db() as c:
        row = c.execute("SELECT * FROM lotes WHERE id=?", (lote_id,)).fetchone()
    return dict(row) if row else None


def get_items(lote_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            "SELECT * FROM items WHERE lote_id=? ORDER BY area, CAST(nro AS INTEGER), id",
            c,
            params=(lote_id,),
        )


def create_lote(nombre: str, archivo: str, df: pd.DataFrame):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        cur = c.execute("INSERT INTO lotes (nombre, archivo, created_at) VALUES (?, ?, ?)", (nombre, archivo, now))
        lote_id = cur.lastrowid
        rows = []
        for r in df.itertuples(index=False):
            rows.append((
                lote_id,
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
                clean_text(getattr(r, "hoja_origen", "")),
                now,
                now,
            ))
        c.executemany(
            """
            INSERT INTO items
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades, acopiadas,
             identificacion, vence, dia, hora, hoja_origen, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()
    return lote_id


def delete_lote(lote_id: int):
    with db() as c:
        c.execute("DELETE FROM scans WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM items WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM lotes WHERE id=?", (lote_id,))
        c.commit()


def add_acopio(lote_id: int, item_id: int, cantidad: int, scan_primario: str, scan_secundario: str, modo: str):
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
        c.execute(
            """
            INSERT INTO scans (lote_id, item_id, scan_primario, scan_secundario, cantidad, modo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (lote_id, item_id, norm_code(scan_primario), norm_code(scan_secundario), cantidad, modo, now),
        )
        c.commit()
    return True, "Cantidad agregada."


def undo_last_scan(lote_id: int):
    with db() as c:
        row = c.execute("SELECT * FROM scans WHERE lote_id=? ORDER BY id DESC LIMIT 1", (lote_id,)).fetchone()
        if not row:
            return False, "No hay escaneos para deshacer."
        now = datetime.now().isoformat(timespec="seconds")
        c.execute("UPDATE items SET acopiadas=MAX(acopiadas-?, 0), updated_at=? WHERE id=?", (int(row["cantidad"]), now, int(row["item_id"])))
        c.execute("DELETE FROM scans WHERE id=?", (int(row["id"]),))
        c.commit()
    return True, "Último escaneo deshecho."


def get_last_scans(lote_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT item_id, MAX(created_at) AS procesado_at, COALESCE(SUM(cantidad), 0) AS escaneado_total
            FROM scans
            WHERE lote_id=?
            GROUP BY item_id
            """,
            c,
            params=(lote_id,),
        )


# ============================================================
# Maestro SKU/EAN desde repo
# ============================================================

def split_codes(v):
    text = clean_text(v)
    if not text:
        return []
    parts = re.split(r"[,;/|\n\t ]+", text)
    return list(dict.fromkeys([norm_code(p) for p in parts if norm_code(p)]))


def parse_maestro(file_or_path) -> pd.DataFrame:
    xls = pd.ExcelFile(file_or_path)
    rows = []
    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, dtype=object).dropna(how="all")
        if raw.empty:
            continue
        raw.columns = [clean_text(c) for c in raw.columns]
        sku_col = find_col_exact(raw.columns, ["SKU", "SKU ML", "sku_ml"])
        desc_col = find_col_exact(raw.columns, ["Descripción", "Descripcion", "Producto", "Title", "Titulo"])
        if not sku_col:
            continue
        barcode_cols = []
        for c in raw.columns:
            h = normalize_header(c)
            if any(x in h for x in ["ean", "barra", "barcode", "codigo universal", "cod universal", "codigo de barras"]):
                barcode_cols.append(c)
        if sku_col not in barcode_cols:
            barcode_cols.append(sku_col)
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
    if not rows:
        return pd.DataFrame(columns=["code", "sku", "descripcion"])
    return pd.DataFrame(rows).drop_duplicates(subset=["code"])


def import_maestro(df: pd.DataFrame):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        c.execute("DELETE FROM maestro")
        c.executemany(
            "INSERT OR REPLACE INTO maestro (code, sku, descripcion, updated_at) VALUES (?, ?, ?, ?)",
            [(norm_code(r.code), norm_code(r.sku), clean_text(r.descripcion), now) for r in df.itertuples(index=False)],
        )
        c.commit()


def load_maestro_from_repo():
    if not MAESTRO_PATH.exists():
        return 0
    df = parse_maestro(MAESTRO_PATH)
    if df.empty:
        return 0
    import_maestro(df)
    return len(df)


def maestro_lookup(code: str) -> str:
    code_n = norm_code(code)
    if not code_n:
        return ""
    with db() as c:
        row = c.execute("SELECT sku FROM maestro WHERE code=?", (code_n,)).fetchone()
    return clean_text(row["sku"]) if row else ""


# ============================================================
# Matching operativo
# ============================================================

def pending_items(items: pd.DataFrame) -> pd.DataFrame:
    if items.empty:
        return items
    p = items.copy()
    p["pendiente"] = (p["unidades"].astype(int) - p["acopiadas"].astype(int)).clip(lower=0)
    return p[p["pendiente"] > 0]


def best_match(matches: pd.DataFrame):
    if matches.empty:
        return None
    m = matches.copy()
    m["pendiente"] = (m["unidades"].astype(int) - m["acopiadas"].astype(int)).clip(lower=0)
    return m.sort_values(["pendiente", "id"], ascending=[False, True]).iloc[0]


def match_primary(items: pd.DataFrame, code: str) -> tuple[pd.DataFrame, str]:
    """Primer campo: Código ML normal. Para supermercado, también acepta SKU/EAN/Código Universal."""
    c = norm_code(code)
    p = pending_items(items)
    if not c:
        return p.iloc[0:0], ""

    super_mask = p["identificacion"].map(is_supermercado)
    p_super = p[super_mask]
    sku_from_master = norm_code(maestro_lookup(c))
    m_super = p_super[(p_super["sku"].map(norm_code) == c) | (p_super["codigo_universal"].map(norm_code) == c)]
    if sku_from_master:
        m_super = p_super[(p_super["sku"].map(norm_code) == sku_from_master) | (p_super["sku"].map(norm_code) == c) | (p_super["codigo_universal"].map(norm_code) == c)]
    if not m_super.empty:
        return m_super, "SUPERMERCADO"

    m_ml = p[p["codigo_ml"].map(norm_code) == c]
    return m_ml, "ML"


def match_secondary(items_for_ml: pd.DataFrame, code: str) -> pd.DataFrame:
    c = norm_code(code)
    if not c:
        return items_for_ml.iloc[0:0]
    sku_from_master = norm_code(maestro_lookup(c))
    p = pending_items(items_for_ml)
    mask = (p["sku"].map(norm_code) == c) | (p["codigo_universal"].map(norm_code) == c)
    if sku_from_master:
        mask = mask | (p["sku"].map(norm_code) == sku_from_master)
    return p[mask]


def reset_scan_state():
    for k in ["scan_primary", "scan_secondary", "primary_validated", "primary_code", "primary_mode", "candidate_id", "candidate_mode"]:
        if k in ["primary_validated"]:
            st.session_state[k] = False
        elif k in ["candidate_id"]:
            st.session_state[k] = None
        else:
            st.session_state[k] = ""


def get_item_row(items: pd.DataFrame, item_id):
    try:
        iid = int(item_id)
    except Exception:
        return None
    m = items[items["id"].astype(int) == iid]
    return None if m.empty else m.iloc[0]


# ============================================================
# Exportación
# ============================================================

def export_lote(lote_id: int) -> bytes:
    items = get_items(lote_id)
    if not items.empty:
        items["pendiente"] = (items["unidades"].astype(int) - items["acopiadas"].astype(int)).clip(lower=0)
        items["estado"] = items["pendiente"].apply(lambda x: "COMPLETO" if int(x) == 0 else "PENDIENTE")
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

st.markdown("""
<style>
.block-container {padding-top: 1.2rem;}
div[data-testid="stTextInput"] label, div[data-testid="stNumberInput"] label {font-size:1.25rem!important;font-weight:800!important;}
div[data-testid="stTextInput"] input, div[data-testid="stNumberInput"] input {font-size:1.55rem!important;min-height:3.25rem!important;}
.stButton > button {font-size:1.18rem!important;min-height:3rem!important;width:100%;font-weight:800!important;}
div[data-testid="stMetricValue"] {font-size:1.8rem!important;}
.product-title {font-size:1.35rem;font-weight:900;line-height:1.25;margin:.4rem 0;}
.product-sub {font-size:1rem;color:#374151;margin-bottom:.4rem;}
.card {border:1px solid #E5E7EB;border-radius:14px;padding:14px 16px;margin:10px 0;background:#fff;}
.card-title {font-size:1.05rem;font-weight:850;line-height:1.3;margin-bottom:8px;}
.card-meta {font-size:.92rem;color:#374151;margin-bottom:8px;}
.badge {display:inline-block;padding:5px 9px;border-radius:999px;background:#F3F4F6;margin:3px 4px 3px 0;font-size:.9rem;font-weight:750;}
.badge-att {background:#FFF7ED;}
.badge-super {background:#ECFDF5;}
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
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Líneas", len(df))
            c2.metric("Unidades", int(df["unidades"].sum()))
            c3.metric("Con identificación", int((df["identificacion"].map(clean_text) != "").sum()))
            c4.metric("Con vence", int((df["vence"].map(clean_text) != "").sum()))

            with st.expander("Revisión rápida de columnas leídas", expanded=True):
                st.dataframe(
                    df[["codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence"]].head(20),
                    use_container_width=True,
                    hide_index=True,
                )

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
            m1, m2, m3 = st.columns(3)
            m1.metric("Solicitado", total)
            m2.metric("Acopiado", done)
            m3.metric("Pendiente", max(total - done, 0))
            st.divider()

            if "primary_validated" not in st.session_state:
                reset_scan_state()

            st.text_input("Código ML o EAN supermercado", key="scan_primary")
            b1, b2 = st.columns([2, 1])
            with b1:
                validate_primary = st.button("Validar código", type="primary")
            with b2:
                clear = st.button("Limpiar")

            if clear:
                reset_scan_state()
                st.rerun()

            if validate_primary:
                st.session_state["candidate_id"] = None
                st.session_state["candidate_mode"] = ""
                st.session_state["primary_validated"] = False
                st.session_state["primary_code"] = norm_code(st.session_state.get("scan_primary", ""))
                st.session_state["primary_mode"] = ""
                st.session_state["scan_secondary"] = ""

                if not st.session_state["primary_code"]:
                    st.error("Ingresa o escanea un código.")
                else:
                    matches, mode = match_primary(items, st.session_state["primary_code"])
                    if matches.empty:
                        st.error("Código no encontrado en productos pendientes.")
                    elif mode == "SUPERMERCADO":
                        cand = best_match(matches)
                        st.session_state["candidate_id"] = int(cand["id"])
                        st.session_state["candidate_mode"] = "SUPERMERCADO"
                        st.session_state["primary_validated"] = True
                        st.session_state["primary_mode"] = mode
                        st.rerun()
                    else:
                        st.session_state["primary_validated"] = True
                        st.session_state["primary_mode"] = mode
                        st.rerun()

            candidate = None
            candidate_mode = st.session_state.get("candidate_mode", "")
            if st.session_state.get("candidate_id"):
                candidate = get_item_row(items, st.session_state.get("candidate_id"))

            elif st.session_state.get("primary_validated") and st.session_state.get("primary_code"):
                matches, mode = match_primary(items, st.session_state["primary_code"])
                preview = best_match(matches)
                if preview is not None:
                    pendiente_preview = int(preview["unidades"]) - int(preview["acopiadas"])
                    st.markdown(f"<div class='product-title'>{esc(preview['descripcion'])}</div>", unsafe_allow_html=True)
                    st.markdown(f"<div class='product-sub'><b>SKU:</b> {esc(preview['sku'])} &nbsp; | &nbsp; <b>Código ML:</b> {esc(preview['codigo_ml'])}</div>", unsafe_allow_html=True)
                    q1, q2, q3 = st.columns(3)
                    q1.metric("Solicitadas", int(preview["unidades"]))
                    q2.metric("Acopiadas", int(preview["acopiadas"]))
                    q3.metric("Pendientes", max(pendiente_preview, 0))

                st.text_input("SKU / EAN / Código Universal", key="scan_secondary")
                s1, s2 = st.columns(2)
                with s1:
                    validate_secondary = st.button("Validar SKU/EAN", type="primary")
                with s2:
                    no_ean = st.button("Sin EAN")

                if validate_secondary:
                    if not norm_code(st.session_state.get("scan_secondary", "")):
                        st.error("Ingresa o escanea el SKU/EAN.")
                    else:
                        m2 = match_secondary(matches, st.session_state["scan_secondary"])
                        if m2.empty:
                            st.error("El SKU/EAN/Código Universal no corresponde a este producto.")
                        else:
                            cand = best_match(m2)
                            st.session_state["candidate_id"] = int(cand["id"])
                            st.session_state["candidate_mode"] = "ML+SECUNDARIO"
                            st.rerun()

                if no_ean:
                    normal = matches[~matches["identificacion"].map(is_supermercado)]
                    if normal.empty:
                        st.error("No encontré producto normal pendiente para usar Sin EAN.")
                    else:
                        cand = best_match(normal)
                        st.session_state["candidate_id"] = int(cand["id"])
                        st.session_state["candidate_mode"] = "SIN_EAN"
                        st.rerun()

            if candidate is not None:
                pendiente = int(candidate["unidades"]) - int(candidate["acopiadas"])
                st.success("Producto validado")
                st.markdown(f"<div class='product-title'>{esc(candidate['descripcion'])}</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='product-sub'><b>SKU:</b> {esc(candidate['sku'])} &nbsp; | &nbsp; <b>Código ML:</b> {esc(candidate['codigo_ml'])}</div>", unsafe_allow_html=True)
                x1, x2, x3 = st.columns(3)
                x1.metric("Solicitadas", int(candidate["unidades"]))
                x2.metric("Acopiadas", int(candidate["acopiadas"]))
                x3.metric("Pendientes", max(pendiente, 0))
                qty = st.number_input("Cantidad a agregar", min_value=1, max_value=max(pendiente, 1), value=1, step=1)
                if st.button("Agregar cantidad", type="primary"):
                    ok, msg = add_acopio(active_lote, int(candidate["id"]), int(qty), st.session_state.get("scan_primary", ""), st.session_state.get("scan_secondary", ""), candidate_mode)
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
            lote = get_lote(active_lote) or {}
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

            st.caption(f"Lote: {clean_text(lote.get('nombre',''))} · Cargado: {fmt_dt(lote.get('created_at',''))} · Archivo: {clean_text(lote.get('archivo',''))}")

            filtro = st.selectbox("Filtro", ["Todos", "Pendientes", "Completos", "Supermercado", "Con identificación", "Con vencimiento"])
            show = view
            if filtro == "Pendientes":
                show = view[view["pendiente"] > 0]
            elif filtro == "Completos":
                show = view[view["pendiente"] == 0]
            elif filtro == "Supermercado":
                show = view[view["identificacion"].map(is_supermercado)]
            elif filtro == "Con identificación":
                show = view[view["identificacion"].map(clean_text) != ""]
            elif filtro == "Con vencimiento":
                show = view[view["vence"].map(clean_text) != ""]

            scans = get_last_scans(active_lote)
            if not scans.empty:
                show = show.merge(scans, left_on="id", right_on="item_id", how="left")
            else:
                show["procesado_at"] = ""

            modo_vista = st.radio("Vista", ["Tarjetas operativas", "Tabla"], horizontal=True)
            if modo_vista == "Tarjetas operativas":
                for _, r in show.iterrows():
                    pendiente = int(r["unidades"]) - int(r["acopiadas"])
                    ident = clean_text(r.get("identificacion", ""))
                    processed = fmt_dt(r.get("procesado_at", "")) or "Sin procesar"
                    ident_badge = "badge-super" if is_supermercado(ident) else "badge-att"
                    ident_html = f'<span class="badge {ident_badge}">Identificación: {esc(ident)}</span>' if ident else ''
                    st.markdown(
                        f"""
                        <div class="card">
                            <div class="card-title">{esc(r.get('descripcion', ''))}</div>
                            <div class="card-meta"><b>SKU:</b> {esc(r.get('sku', ''))} &nbsp; | &nbsp; <b>Código ML:</b> {esc(r.get('codigo_ml', ''))}</div>
                            <span class="badge">Unidades: {int(r['unidades'])}</span>
                            <span class="badge">Acopiadas: {int(r['acopiadas'])}</span>
                            <span class="badge">Pendiente: {max(pendiente, 0)}</span>
                            {ident_html}
                            <span class="badge">Procesado: {esc(processed)}</span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
            else:
                table = show.copy()
                table["Producto"] = table["descripcion"].map(clean_text)
                table["SKU"] = table["sku"].map(norm_code)
                table["Código ML"] = table["codigo_ml"].map(norm_code)
                table["EAN / Código universal"] = table["codigo_universal"].map(norm_code)
                table["Unidades"] = table["unidades"].astype(int)
                table["Acopiadas"] = table["acopiadas"].astype(int)
                table["Pendiente"] = table["pendiente"].astype(int)
                table["Identificación"] = table["identificacion"].map(clean_text)
                table["Vence"] = table["vence"].map(clean_text)
                table["Procesado"] = table["procesado_at"].map(fmt_dt)
                table["Estado"] = table["estado"].map(clean_text)
                display_cols = ["SKU", "Código ML", "EAN / Código universal", "Producto", "Unidades", "Acopiadas", "Pendiente", "Identificación", "Vence", "Procesado", "Estado"]
                st.dataframe(table[display_cols], use_container_width=True, hide_index=True, height=620)

            st.download_button("Exportar control Excel", data=export_lote(active_lote), file_name="control_full_aurora.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.divider()
            if st.button("Eliminar lote activo"):
                delete_lote(active_lote)
                st.success("Lote eliminado.")
                st.rerun()
