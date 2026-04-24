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
DB_PATH = DATA_DIR / "aurora_full_v3.db"
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
        c.commit()


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
    return lote_id


def delete_lote(lote_id):
    with db() as c:
        c.execute("DELETE FROM scans WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM items WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM lotes WHERE id=?", (lote_id,))
        c.commit()


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
    return True, "Cantidad agregada."


def undo_last_scan(lote_id):
    with db() as c:
        row = c.execute("SELECT * FROM scans WHERE lote_id=? ORDER BY id DESC LIMIT 1", (lote_id,)).fetchone()
        if not row:
            return False, "No hay escaneos para deshacer."
        now = datetime.now().isoformat(timespec="seconds")
        c.execute("UPDATE items SET acopiadas=MAX(acopiadas-?,0), updated_at=? WHERE id=?", (int(row["cantidad"]), now, int(row["item_id"])))
        c.execute("DELETE FROM scans WHERE id=?", (int(row["id"]),))
        c.commit()
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
    for k in ["scan_primary", "scan_secondary", "primary_validated", "primary_code", "candidate_id", "candidate_mode"]:
        if k in ["primary_validated"]:
            st.session_state[k] = False
        elif k == "candidate_id":
            st.session_state[k] = None
        else:
            st.session_state[k] = ""


def get_item_row(items, item_id):
    try:
        iid = int(item_id)
    except Exception:
        return None
    m = items[items["id"].astype(int) == iid]
    return None if m.empty else m.iloc[0]


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

st.markdown("""
<style>
div[data-testid="stTextInput"] label, div[data-testid="stNumberInput"] label {font-size:1.25rem!important;font-weight:800!important;}
div[data-testid="stTextInput"] input, div[data-testid="stNumberInput"] input {font-size:1.55rem!important;min-height:3.3rem!important;}
.stButton > button {font-size:1.2rem!important;min-height:3.1rem!important;width:100%;font-weight:800!important;}
div[data-testid="stMetricValue"] {font-size:1.8rem!important;}
.product-title {font-size:1.3rem;font-weight:850;line-height:1.25;margin:8px 0;}
.control-card {border:1px solid #E5E7EB;border-radius:16px;padding:15px 17px;margin:12px 0;background:#FFF;}
.control-title {font-size:1.05rem;font-weight:850;line-height:1.35;margin-bottom:8px;}
.control-meta {font-size:.92rem;color:#374151;margin-bottom:8px;}
.badge {display:inline-block;padding:6px 10px;border-radius:999px;background:#F3F4F6;margin:3px 4px 3px 0;font-size:.92rem;font-weight:750;}
.badge-alert {background:#FFF7ED;}
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

        for k, v in {"primary_validated": False, "primary_code": "", "candidate_id": None, "candidate_mode": ""}.items():
            if k not in st.session_state:
                st.session_state[k] = v

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
                # Regla operativa:
                # - SUPERMERCADO se confirma únicamente por SKU/EAN/Código Universal en este primer campo.
                # - Los productos normales se validan por Código ML y luego por SKU/EAN.
                sm = match_secondary(items, code, only_super=True)
                if not sm.empty:
                    cand = best_match(sm)
                    st.session_state["candidate_id"] = int(cand["id"])
                    st.session_state["candidate_mode"] = "SUPERMERCADO"
                    st.session_state["primary_validated"] = True
                else:
                    m1 = match_ml(items, code)
                    if not m1.empty:
                        # Si el Código ML pertenece solo a productos SUPERMERCADO pendientes, no se acepta.
                        m1_no_super = m1[~m1["identificacion"].map(is_supermercado)]
                        if m1_no_super.empty:
                            st.error("Producto SUPERMERCADO: debes validar con SKU/EAN/Código Universal, no con Código ML.")
                        else:
                            st.session_state["primary_validated"] = True
                    else:
                        st.error("Código no encontrado en productos pendientes.")

        candidate = None
        modo = st.session_state.get("candidate_mode", "")
        if st.session_state.get("candidate_id"):
            candidate = get_item_row(items, st.session_state["candidate_id"])
        elif st.session_state.get("primary_validated") and st.session_state.get("primary_code"):
            m1 = match_ml(items, st.session_state["primary_code"])
            preview = best_match(m1)
            if preview is not None:
                pendiente = int(preview["unidades"]) - int(preview["acopiadas"])
                st.markdown(f"<div class='product-title'>{esc(preview['descripcion'])}</div>", unsafe_allow_html=True)
                q1, q2, q3 = st.columns(3)
                q1.metric("Solicitadas", int(preview["unidades"]))
                q2.metric("Acopiadas", int(preview["acopiadas"]))
                q3.metric("Pendientes", max(pendiente, 0))
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
                        st.rerun()
                if validar_sec:
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
                            st.rerun()

        if candidate is not None:
            pendiente = int(candidate["unidades"]) - int(candidate["acopiadas"])
            st.success("Producto validado")
            st.markdown(f"<div class='product-title'>{esc(candidate['descripcion'])}</div>", unsafe_allow_html=True)
            x1, x2, x3, x4 = st.columns(4)
            x1.metric("SKU", candidate["sku"])
            x2.metric("Solicitadas", int(candidate["unidades"]))
            x3.metric("Acopiadas", int(candidate["acopiadas"]))
            x4.metric("Pendientes", max(pendiente, 0))
            qty = st.number_input("Cantidad a agregar", min_value=1, max_value=max(pendiente, 1), value=1, step=1)
            if st.button("Agregar cantidad", type="primary"):
                ok, msg = add_acopio(active_lote, int(candidate["id"]), int(qty), st.session_state.get("scan_primary", ""), st.session_state.get("scan_secondary", ""), modo)
                if ok:
                    reset_scan_state(); st.success(msg); st.rerun()
                else:
                    st.error(msg)

        st.divider()
        if st.button("Deshacer último escaneo"):
            ok, msg = undo_last_scan(active_lote)
            st.success(msg) if ok else st.warning(msg)
            if ok: st.rerun()

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

            modo_vista = st.radio("Vista", ["Tarjetas operativas", "Tabla"], horizontal=True)
            if modo_vista == "Tarjetas operativas":
                for _, r in show.iterrows():
                    ident = clean_text(r.get("identificacion", ""))
                    vence = clean_text(r.get("vence", ""))
                    proc = fmt_dt(r.get("procesado_at", "")) or "Sin procesar"
                    badges = (
                        f"<span class='badge'>Unidades: {int(r['unidades'])}</span>"
                        f"<span class='badge'>Acopiadas: {int(r['acopiadas'])}</span>"
                        f"<span class='badge'>Pendiente: {int(r['pendiente'])}</span>"
                    )
                    if ident:
                        badges += f"<span class='badge badge-alert'>Identificación: {esc(ident)}</span>"
                    if vence:
                        badges += f"<span class='badge badge-alert'>Vence: {esc(vence)}</span>"
                    badges += f"<span class='badge'>Procesado: {esc(proc)}</span>"
                    card_html = (
                        "<div class='control-card'>"
                        f"<div class='control-title'>{esc(r['descripcion'])}</div>"
                        f"<div class='control-meta'><b>SKU:</b> {esc(r['sku'])} &nbsp; | &nbsp; <b>Código ML:</b> {esc(r['codigo_ml'])}</div>"
                        f"{badges}"
                        "</div>"
                    )
                    st.markdown(card_html, unsafe_allow_html=True)
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
