import io
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "Control FULL Aurora"
DB_PATH = Path("data/aurora_full.db")
MAESTRO_LOCAL_PATH = Path("data/maestro_sku_ean.xlsx")

st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")


# =========================
# Utilidades
# =========================
def ensure_dirs():
    Path("data").mkdir(exist_ok=True)


def only_digits_or_text(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.strip()


def norm_key(value):
    text = only_digits_or_text(value).upper()
    text = text.replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
    text = re.sub(r"[^A-Z0-9]+", "", text)
    return text


def norm_header(value):
    text = str(value).strip().lower()
    text = text.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    text = text.replace("ñ", "n")
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text


def int_qty(value, default=0):
    try:
        if pd.isna(value):
            return default
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return default


COLUMN_ALIASES = {
    "area": ["area", "area_"],
    "nro": ["n", "nro", "numero", "nº", "n°"],
    "codigo_ml": ["codigo_ml", "cod_ml", "código_ml", "ml", "codigo_meli", "publicacion"],
    "codigo_universal": ["codigo_universal", "cod_universal", "código_universal", "ean", "codigo_barra", "codigo_barras"],
    "sku": ["sku", "sku_ml", "codigo_sku"],
    "descripcion": ["descripcion", "descripción", "title", "titulo", "producto"],
    "unidades": ["unidades", "cantidad", "cant", "qty_required"],
    "identificacion": ["identificacion", "identificación", "etiqueta", "etiq", "etiquetar"],
    "vence": ["vence", "vencimiento", "vcto"],
    "dia": ["dia", "día"],
    "hora": ["hora"],
}


def find_col(columns, target):
    normalized = {norm_header(c): c for c in columns}
    for alias in COLUMN_ALIASES[target]:
        a = norm_header(alias)
        if a in normalized:
            return normalized[a]
    return None


# =========================
# Base de datos
# =========================
def conn():
    ensure_dirs()
    c = sqlite3.connect(DB_PATH)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lote TEXT NOT NULL,
            area TEXT,
            nro TEXT,
            codigo_ml TEXT,
            codigo_universal TEXT,
            sku TEXT,
            descripcion TEXT,
            unidades INTEGER DEFAULT 0,
            identificacion TEXT,
            vence TEXT,
            dia TEXT,
            hora TEXT,
            acopiadas INTEGER DEFAULT 0,
            updated_at TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lote TEXT NOT NULL,
            item_id INTEGER,
            codigo_ml TEXT,
            codigo_secundario TEXT,
            qty INTEGER,
            modo TEXT,
            created_at TEXT
        )
        """
    )
    c.commit()
    return c


def clear_lote(lote):
    with conn() as c:
        c.execute("DELETE FROM scans WHERE lote=?", (lote,))
        c.execute("DELETE FROM items WHERE lote=?", (lote,))
        c.commit()


def insert_lote(df, lote):
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            lote,
            only_digits_or_text(r.get("area", "")),
            only_digits_or_text(r.get("nro", "")),
            only_digits_or_text(r.get("codigo_ml", "")),
            only_digits_or_text(r.get("codigo_universal", "")),
            only_digits_or_text(r.get("sku", "")),
            only_digits_or_text(r.get("descripcion", "")),
            int_qty(r.get("unidades", 0)),
            only_digits_or_text(r.get("identificacion", "")),
            only_digits_or_text(r.get("vence", "")),
            only_digits_or_text(r.get("dia", "")),
            only_digits_or_text(r.get("hora", "")),
            0,
            now,
        ))
    with conn() as c:
        c.executemany(
            """
            INSERT INTO items
            (lote, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades,
             identificacion, vence, dia, hora, acopiadas, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()


def lotes_disponibles():
    with conn() as c:
        return [r[0] for r in c.execute("SELECT DISTINCT lote FROM items ORDER BY lote DESC").fetchall()]


def load_items(lote):
    with conn() as c:
        return pd.read_sql_query("SELECT * FROM items WHERE lote=? ORDER BY id", c, params=(lote,))


def update_acopio(item_id, lote, codigo_ml, codigo_secundario, qty, modo):
    now = datetime.now().isoformat(timespec="seconds")
    with conn() as c:
        current = c.execute("SELECT unidades, acopiadas FROM items WHERE id=?", (item_id,)).fetchone()
        if not current:
            return False, "Producto no encontrado."
        unidades, acopiadas = current
        pendiente = max(int(unidades) - int(acopiadas), 0)
        if qty <= 0:
            return False, "La cantidad debe ser mayor a cero."
        if qty > pendiente:
            return False, f"No puedes agregar {qty}. Pendiente actual: {pendiente}."
        c.execute("UPDATE items SET acopiadas=acopiadas+?, updated_at=? WHERE id=?", (qty, now, item_id))
        c.execute(
            "INSERT INTO scans (lote, item_id, codigo_ml, codigo_secundario, qty, modo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (lote, item_id, codigo_ml, codigo_secundario, qty, modo, now),
        )
        c.commit()
    return True, "Cantidad acopiada correctamente."


# =========================
# Lectura Excel FULL
# =========================
def read_full_excel(uploaded_file):
    xls = pd.ExcelFile(uploaded_file)
    frames = []
    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, dtype=str)
        if raw.empty:
            continue
        raw.columns = [str(c).strip() for c in raw.columns]

        out = pd.DataFrame()
        for target in COLUMN_ALIASES:
            col = find_col(raw.columns, target)
            out[target] = raw[col] if col else ""

        out["unidades"] = out["unidades"].apply(int_qty)
        out = out[(out["sku"].astype(str).str.strip() != "") | (out["codigo_ml"].astype(str).str.strip() != "")]
        out = out[out["unidades"] > 0]
        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=list(COLUMN_ALIASES.keys()))

    df = pd.concat(frames, ignore_index=True)
    for col in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "identificacion", "vence", "dia", "hora"]:
        df[col] = df[col].apply(only_digits_or_text)

    # Agrupa por producto para evitar duplicados entre hojas o líneas repetidas.
    group_cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "identificacion", "vence", "dia", "hora"]
    df = df.groupby(group_cols, dropna=False, as_index=False)["unidades"].sum()
    return df[["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]]


# =========================
# Maestro SKU/EAN
# =========================
@st.cache_data(ttl=300, show_spinner=False)
def load_maestro_local_cached(path_str):
    return pd.read_excel(path_str, dtype=str)


def normalize_maestro(df):
    if df is None or df.empty:
        return {}
    cols = list(df.columns)
    sku_col = None
    ean_col = None
    for c in cols:
        h = norm_header(c)
        if h in ["sku", "sku_ml", "codigo_sku"] and sku_col is None:
            sku_col = c
        if h in ["ean", "codigo_barra", "codigo_barras", "codigo_universal", "barcode"] and ean_col is None:
            ean_col = c
    if sku_col is None:
        sku_col = cols[0]
    if ean_col is None and len(cols) > 1:
        ean_col = cols[1]

    mapping = {}
    if ean_col is None:
        return mapping
    for _, r in df.iterrows():
        sku = norm_key(r.get(sku_col, ""))
        ean = norm_key(r.get(ean_col, ""))
        if sku and ean:
            mapping.setdefault(sku, set()).add(ean)
            mapping.setdefault(ean, set()).add(sku)
    return mapping


def load_maestro():
    if MAESTRO_LOCAL_PATH.exists():
        df = load_maestro_local_cached(str(MAESTRO_LOCAL_PATH))
        st.sidebar.success(f"Maestro local cargado: {len(df)} filas")
        return normalize_maestro(df)

    st.sidebar.warning("No se encontró data/maestro_sku_ean.xlsx")
    uploaded = st.sidebar.file_uploader("Subir maestro SKU/EAN", type=["xlsx"])
    if uploaded:
        df = pd.read_excel(uploaded, dtype=str)
        st.sidebar.success(f"Maestro subido: {len(df)} filas")
        return normalize_maestro(df)
    return {}


def secondary_matches(item, code, maestro_map):
    code_n = norm_key(code)
    sku = norm_key(item.get("sku", ""))
    universal = norm_key(item.get("codigo_universal", ""))
    if not code_n:
        return False
    if code_n == sku or code_n == universal:
        return True
    if sku and code_n in maestro_map.get(sku, set()):
        return True
    if universal and code_n in maestro_map.get(universal, set()):
        return True
    return False


def find_by_ml(df, ml_code):
    ml = norm_key(ml_code)
    if not ml:
        return pd.DataFrame()
    return df[df["codigo_ml"].apply(norm_key) == ml]


# =========================
# UI
# =========================
ensure_dirs()
maestro_map = load_maestro()

st.title("📦 Control FULL Aurora")
st.caption("App limpia para validar acopio FULL por Código ML + SKU/EAN/Código Universal.")

menu = st.sidebar.radio("Menú", ["Cargar lote", "Escaneo", "Control"], index=0)

if menu == "Cargar lote":
    st.header("Cargar lote FULL")
    uploaded = st.file_uploader("Excel FULL", type=["xlsx"])

    if uploaded:
        df = read_full_excel(uploaded)
        if df.empty:
            st.error("No se encontraron filas válidas en el Excel.")
            st.stop()

        st.subheader("Vista previa")
        st.dataframe(df.head(50), use_container_width=True, hide_index=True)

        default_lote = Path(uploaded.name).stem
        lote = st.text_input("Nombre del lote", value=default_lote)
        replace = st.checkbox("Reemplazar lote si ya existe", value=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs/líneas", len(df))
        c2.metric("Unidades totales", int(df["unidades"].sum()))
        c3.metric("Con código universal", int((df["codigo_universal"].astype(str).str.strip() != "").sum()))

        if st.button("Guardar lote", type="primary"):
            if not lote.strip():
                st.error("Debes indicar un nombre de lote.")
            else:
                if replace:
                    clear_lote(lote.strip())
                insert_lote(df, lote.strip())
                st.success(f"Lote guardado: {lote.strip()}")

elif menu == "Escaneo":
    st.header("Escaneo FULL")
    lotes = lotes_disponibles()
    if not lotes:
        st.warning("Primero carga un lote FULL.")
        st.stop()

    lote = st.selectbox("Lote", lotes)
    df = load_items(lote)

    if "ml_validado" not in st.session_state:
        st.session_state.ml_validado = ""
    if "item_validado_id" not in st.session_state:
        st.session_state.item_validado_id = None
    if "sec_validado" not in st.session_state:
        st.session_state.sec_validado = ""
    if "sin_ean" not in st.session_state:
        st.session_state.sin_ean = False

    st.subheader("1) Escanear etiqueta ML")
    ml_code = st.text_input("Código ML", key="scan_ml")
    if st.button("Validar Código ML", type="primary"):
        matches = find_by_ml(df, ml_code)
        if matches.empty:
            st.error("Código ML no encontrado en el lote.")
            st.session_state.ml_validado = ""
            st.session_state.item_validado_id = None
        elif len(matches) > 1:
            st.warning("Ese Código ML aparece más de una vez. Revisa el lote.")
            st.dataframe(matches[["area", "nro", "codigo_ml", "sku", "descripcion", "unidades", "acopiadas"]], hide_index=True)
        else:
            item = matches.iloc[0]
            st.session_state.ml_validado = only_digits_or_text(item["codigo_ml"])
            st.session_state.item_validado_id = int(item["id"])
            st.session_state.sec_validado = ""
            st.session_state.sin_ean = False
            st.success("Código ML validado. Ahora escanea SKU/EAN/Código Universal.")

    item_df = df[df["id"] == st.session_state.item_validado_id] if st.session_state.item_validado_id else pd.DataFrame()

    if not item_df.empty:
        item = item_df.iloc[0]
        pendiente = max(int(item["unidades"]) - int(item["acopiadas"]), 0)
        st.info(f"{item['descripcion']}")
        c1, c2, c3 = st.columns(3)
        c1.metric("Solicitadas", int(item["unidades"]))
        c2.metric("Acopiadas", int(item["acopiadas"]))
        c3.metric("Pendientes", pendiente)

        st.subheader("2) Escanear SKU / EAN / Código Universal")
        sec_code = st.text_input("SKU / EAN / Código Universal", key="scan_sec")
        col_a, col_b = st.columns([1, 1])
        with col_a:
            if st.button("Validar SKU/EAN/Código Universal"):
                if secondary_matches(item, sec_code, maestro_map):
                    st.session_state.sec_validado = only_digits_or_text(sec_code)
                    st.session_state.sin_ean = False
                    st.success("Producto validado por segundo código.")
                else:
                    st.error("El segundo código no coincide con el SKU/EAN/Código Universal del producto.")
        with col_b:
            if st.button("Sin EAN / sin código universal"):
                st.session_state.sec_validado = "SIN_EAN"
                st.session_state.sin_ean = True
                st.warning("Modo sin EAN activado. Se permitirá acopiar solo con Código ML validado.")

        puede_acopiar = bool(st.session_state.ml_validado and st.session_state.item_validado_id and st.session_state.sec_validado)

        st.subheader("3) Agregar cantidad")
        if not puede_acopiar:
            st.warning("Primero debes validar Código ML + SKU/EAN/Código Universal, o usar el botón Sin EAN.")
        qty = st.number_input("Cantidad a agregar", min_value=1, max_value=max(pendiente, 1), value=1, step=1, disabled=not puede_acopiar or pendiente <= 0)
        if st.button("Agregar acopio", disabled=not puede_acopiar or pendiente <= 0, type="primary"):
            ok, msg = update_acopio(
                int(item["id"]),
                lote,
                st.session_state.ml_validado,
                st.session_state.sec_validado,
                int(qty),
                "SIN_EAN" if st.session_state.sin_ean else "DOBLE_VALIDACION",
            )
            if ok:
                st.success(msg)
                st.session_state.ml_validado = ""
                st.session_state.item_validado_id = None
                st.session_state.sec_validado = ""
                st.session_state.sin_ean = False
                st.rerun()
            else:
                st.error(msg)

    st.divider()
    st.subheader("Pendientes del lote")
    view = df.copy()
    view["pendiente"] = view["unidades"] - view["acopiadas"]
    st.dataframe(
        view[view["pendiente"] > 0][["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "acopiadas", "pendiente"]].head(100),
        use_container_width=True,
        hide_index=True,
    )

elif menu == "Control":
    st.header("Control de lote")
    lotes = lotes_disponibles()
    if not lotes:
        st.warning("Primero carga un lote FULL.")
        st.stop()

    lote = st.selectbox("Lote", lotes)
    df = load_items(lote)
    df["pendiente"] = df["unidades"] - df["acopiadas"]
    df["estado"] = df["pendiente"].apply(lambda x: "COMPLETO" if x <= 0 else "PENDIENTE")

    total_req = int(df["unidades"].sum())
    total_acop = int(df["acopiadas"].sum())
    avance = 0 if total_req == 0 else round(total_acop / total_req * 100, 2)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unidades solicitadas", total_req)
    c2.metric("Unidades acopiadas", total_acop)
    c3.metric("Pendientes", int(df["pendiente"].clip(lower=0).sum()))
    c4.metric("Avance", f"{avance}%")

    filtro = st.radio("Ver", ["Todos", "Pendientes", "Completos"], horizontal=True)
    show = df.copy()
    if filtro == "Pendientes":
        show = show[show["pendiente"] > 0]
    elif filtro == "Completos":
        show = show[show["pendiente"] <= 0]

    cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "acopiadas", "pendiente", "estado", "identificacion", "vence", "dia", "hora"]
    st.dataframe(show[cols], use_container_width=True, hide_index=True)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df[cols].to_excel(writer, index=False, sheet_name="control_full")
    st.download_button(
        "Descargar control Excel",
        data=buffer.getvalue(),
        file_name=f"control_full_{lote}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()
    if st.button("Eliminar lote", type="secondary"):
        clear_lote(lote)
        st.success("Lote eliminado.")
        st.rerun()
