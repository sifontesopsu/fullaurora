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

# ============================================================
# Utilidades generales
# ============================================================
def ensure_dirs():
    Path("data").mkdir(exist_ok=True)


def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    return text.strip()


def norm_header(value) -> str:
    text = clean_text(value).lower()
    replacements = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        "º": "", "°": "",
    }
    for a, b in replacements.items():
        text = text.replace(a, b)
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def norm_key(value) -> str:
    text = clean_text(value).upper()
    replacements = {"Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ñ": "N"}
    for a, b in replacements.items():
        text = text.replace(a, b)
    if re.fullmatch(r"\d+(\.\d+)?[eE][+-]?\d+", text):
        try:
            text = str(int(float(text)))
        except Exception:
            pass
    return re.sub(r"[^A-Z0-9]+", "", text)


def int_qty(value, default=0) -> int:
    try:
        txt = clean_text(value).replace(".", "").replace(",", ".")
        if txt == "":
            return default
        return int(float(txt))
    except Exception:
        return default


def moneyless_filename(value: str) -> str:
    value = clean_text(value)
    value = re.sub(r"[^A-Za-z0-9_\- ]+", "", value).strip()
    return value or "lote_full"


def is_supermercado(value) -> bool:
    return "SUPERMERCADO" in norm_key(value)


def is_identificacion_text(value) -> bool:
    k = norm_key(value)
    if not k:
        return False
    return any(x in k for x in ["ETIQUET", "SUPERMERCADO", "SINEAN", "SINETIQUETA"])


# ============================================================
# Base SQLite limpia SOLO FULL
# ============================================================
def get_conn():
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


def clear_lote(lote: str):
    with get_conn() as c:
        c.execute("DELETE FROM scans WHERE lote=?", (lote,))
        c.execute("DELETE FROM items WHERE lote=?", (lote,))
        c.commit()


def insert_lote(df: pd.DataFrame, lote: str):
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            lote,
            clean_text(r.get("area", "")),
            clean_text(r.get("nro", "")),
            clean_text(r.get("codigo_ml", "")),
            clean_text(r.get("codigo_universal", "")),
            clean_text(r.get("sku", "")),
            clean_text(r.get("descripcion", "")),
            int_qty(r.get("unidades", 0)),
            clean_text(r.get("identificacion", "")),
            clean_text(r.get("vence", "")),
            clean_text(r.get("dia", "")),
            clean_text(r.get("hora", "")),
            0,
            now,
        ))
    with get_conn() as c:
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


def list_lotes():
    with get_conn() as c:
        return [r[0] for r in c.execute("SELECT DISTINCT lote FROM items ORDER BY lote DESC").fetchall()]


def load_items(lote: str) -> pd.DataFrame:
    with get_conn() as c:
        return pd.read_sql_query("SELECT * FROM items WHERE lote=? ORDER BY id", c, params=(lote,))


def add_acopio(item_id: int, lote: str, codigo_ml: str, codigo_secundario: str, qty: int, modo: str):
    now = datetime.now().isoformat(timespec="seconds")
    with get_conn() as c:
        row = c.execute("SELECT unidades, acopiadas FROM items WHERE id=?", (item_id,)).fetchone()
        if not row:
            return False, "Producto no encontrado."
        unidades, acopiadas = int(row[0] or 0), int(row[1] or 0)
        pendiente = max(unidades - acopiadas, 0)
        if qty <= 0:
            return False, "La cantidad debe ser mayor a cero."
        if qty > pendiente:
            return False, f"No puedes agregar {qty}. Pendiente actual: {pendiente}."
        c.execute("UPDATE items SET acopiadas=acopiadas+?, updated_at=? WHERE id=?", (qty, now, item_id))
        c.execute(
            """
            INSERT INTO scans (lote, item_id, codigo_ml, codigo_secundario, qty, modo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (lote, item_id, codigo_ml, codigo_secundario, qty, modo, now),
        )
        c.commit()
    return True, "Cantidad acopiada correctamente."


# ============================================================
# Lectura Excel FULL robusta
# ============================================================
COLUMN_ALIASES = {
    "area": ["area"],
    "nro": ["n", "nro", "numero", "num"],
    "codigo_ml": ["codigo_ml", "cod_ml", "codigo_meli", "publicacion", "ml"],
    "codigo_universal": ["codigo_universal", "cod_universal", "ean", "codigo_barra", "codigo_barras", "barcode"],
    "sku": ["sku", "sku_ml", "codigo_sku"],
    "descripcion": ["descripcion", "title", "titulo", "producto"],
    "unidades": ["unidades", "cantidad", "cant", "qty_required", "qty"],
    "identificacion": ["identificacion", "etiqueta", "etiq", "etiquetar"],
    "vence": ["vence", "vencimiento", "vcto"],
    "dia": ["dia"],
    "hora": ["hora"],
}


def find_alias(columns, target):
    normalized = {norm_header(c): c for c in columns}
    for alias in COLUMN_ALIASES[target]:
        a = norm_header(alias)
        if a in normalized:
            return normalized[a]
    return None


def detect_header_row(raw_no_header: pd.DataFrame) -> int | None:
    """Detecta la fila de encabezados si el Excel trae filas superiores vacías/títulos."""
    best_row = None
    best_score = 0
    for idx in range(min(25, len(raw_no_header))):
        values = [norm_header(v) for v in raw_no_header.iloc[idx].tolist()]
        joined = "|".join(values)
        score = 0
        for must in ["sku", "unidades", "descripcion"]:
            if must in joined:
                score += 2
        if "codigo_ml" in joined or "cod_ml" in joined:
            score += 3
        if "identificacion" in joined or "etiqueta" in joined:
            score += 2
        if score > best_score:
            best_score = score
            best_row = idx
    return best_row if best_score >= 5 else None


def read_sheet_safely(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    raw0 = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
    header_row = detect_header_row(raw0)
    if header_row is None:
        return pd.DataFrame()

    headers = [clean_text(x) for x in raw0.iloc[header_row].tolist()]
    df = raw0.iloc[header_row + 1:].copy()
    df.columns = headers
    df = df.dropna(how="all")
    df.columns = [clean_text(c) for c in df.columns]
    return df


def fix_identificacion_vence(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige corrimientos frecuentes entre Identificación y Vence.

    Regla principal:
    - Si 'vence' trae texto propio de identificación y 'identificacion' viene vacía, se mueve a identificación.
    - Si 'identificacion' trae algo tipo fecha/hora y 'vence' está vacío, se mueve a vence.
    - Nunca copia Código ML a Código Universal.
    """
    df = df.copy()
    for col in ["identificacion", "vence", "dia", "hora"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].apply(clean_text)

    mask_vence_es_ident = df["vence"].apply(is_identificacion_text) & (df["identificacion"].str.strip() == "")
    df.loc[mask_vence_es_ident, "identificacion"] = df.loc[mask_vence_es_ident, "vence"]
    df.loc[mask_vence_es_ident, "vence"] = ""

    # Si identificación viene vacía y día/hora traen textos de identificación, rescata también.
    for source_col in ["dia", "hora"]:
        mask = df[source_col].apply(is_identificacion_text) & (df["identificacion"].str.strip() == "")
        df.loc[mask, "identificacion"] = df.loc[mask, source_col]
        df.loc[mask, source_col] = ""

    return df


def read_full_excel(uploaded_file) -> pd.DataFrame:
    xls = pd.ExcelFile(uploaded_file)
    frames = []

    for sheet in xls.sheet_names:
        raw = read_sheet_safely(xls, sheet)
        if raw.empty:
            continue

        out = pd.DataFrame()
        for target in COLUMN_ALIASES.keys():
            col = find_alias(raw.columns, target)
            out[target] = raw[col] if col else ""

        for col in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "identificacion", "vence", "dia", "hora"]:
            out[col] = out[col].apply(clean_text)
        out["unidades"] = out["unidades"].apply(int_qty)

        out = fix_identificacion_vence(out)
        out = out[(out["sku"].str.strip() != "") | (out["codigo_ml"].str.strip() != "") | (out["codigo_universal"].str.strip() != "")]
        out = out[out["unidades"] > 0]
        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"])

    df = pd.concat(frames, ignore_index=True)

    # Agrupa solo cuando realmente es el mismo producto/línea.
    group_cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "identificacion", "vence", "dia", "hora"]
    df = df.groupby(group_cols, dropna=False, as_index=False)["unidades"].sum()
    return df[["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]]


# ============================================================
# Maestro SKU/EAN local + fallback manual
# ============================================================
@st.cache_data(ttl=300, show_spinner=False)
def read_excel_cached(path_str: str) -> pd.DataFrame:
    return pd.read_excel(path_str, dtype=str)


def load_maestro(uploaded=None):
    if MAESTRO_LOCAL_PATH.exists():
        try:
            df = read_excel_cached(str(MAESTRO_LOCAL_PATH))
            return normalize_maestro(df), f"Maestro cargado desde repo: {MAESTRO_LOCAL_PATH}"
        except Exception as e:
            st.warning(f"Se encontró maestro local, pero no se pudo leer: {e}")

    if uploaded is not None:
        try:
            df = pd.read_excel(uploaded, dtype=str)
            return normalize_maestro(df), "Maestro cargado manualmente."
        except Exception as e:
            st.warning(f"No se pudo leer el maestro manual: {e}")

    return pd.DataFrame(columns=["sku", "ean"]), "Sin maestro SKU/EAN."


def normalize_maestro(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["sku", "ean"])

    df = df.copy()
    df.columns = [clean_text(c) for c in df.columns]

    def find_col_by_alias(aliases):
        normalized = {norm_header(c): c for c in df.columns}
        for a in aliases:
            key = norm_header(a)
            if key in normalized:
                return normalized[key]
        return None

    sku_col = find_col_by_alias(["sku", "sku_ml", "codigo_sku", "código sku"])
    ean_col = find_col_by_alias(["ean", "codigo universal", "código universal", "codigo_barra", "codigo_barras", "barcode", "codigos de barras", "códigos de barras"])

    if sku_col is None:
        return pd.DataFrame(columns=["sku", "ean"])

    out = pd.DataFrame()
    out["sku"] = df[sku_col].apply(clean_text)
    out["ean"] = df[ean_col].apply(clean_text) if ean_col else ""

    # Si una celda tiene varios códigos separados por coma/punto y coma/espacio, explota filas.
    rows = []
    for _, r in out.iterrows():
        sku = clean_text(r.get("sku", ""))
        eans_raw = clean_text(r.get("ean", ""))
        if not sku:
            continue
        parts = re.split(r"[,;\s]+", eans_raw) if eans_raw else [""]
        for p in parts:
            p = clean_text(p)
            if p:
                rows.append({"sku": sku, "ean": p})
        if not eans_raw:
            rows.append({"sku": sku, "ean": ""})
    return pd.DataFrame(rows).drop_duplicates()


def build_secondary_index(items: pd.DataFrame, maestro: pd.DataFrame) -> dict[str, list[int]]:
    sku_to_eans: dict[str, set[str]] = {}
    if maestro is not None and not maestro.empty:
        for _, r in maestro.iterrows():
            sku_key = norm_key(r.get("sku", ""))
            ean_key = norm_key(r.get("ean", ""))
            if sku_key and ean_key:
                sku_to_eans.setdefault(sku_key, set()).add(ean_key)

    index: dict[str, list[int]] = {}
    for _, r in items.iterrows():
        item_id = int(r["id"])
        keys = set()
        sku_key = norm_key(r.get("sku", ""))
        universal_key = norm_key(r.get("codigo_universal", ""))
        if sku_key:
            keys.add(sku_key)
        if universal_key:
            keys.add(universal_key)
        for ean in sku_to_eans.get(sku_key, set()):
            keys.add(ean)
        for k in keys:
            index.setdefault(k, []).append(item_id)
    return index


def build_ml_index(items: pd.DataFrame) -> dict[str, list[int]]:
    index: dict[str, list[int]] = {}
    for _, r in items.iterrows():
        k = norm_key(r.get("codigo_ml", ""))
        if k:
            index.setdefault(k, []).append(int(r["id"]))
    return index


def get_item(items: pd.DataFrame, item_id: int):
    row = items[items["id"] == item_id]
    if row.empty:
        return None
    return row.iloc[0]


def remaining(row) -> int:
    return max(int(row.get("unidades", 0) or 0) - int(row.get("acopiadas", 0) or 0), 0)


# ============================================================
# UI
# ============================================================
ensure_dirs()
st.title("📦 Control FULL Aurora")
st.caption("App limpia para validar envíos FULL por Código ML + SKU/EAN/Código Universal. Supermercado se valida solo por SKU/EAN/Código Universal.")

with st.sidebar:
    st.header("Menú")
    pantalla = st.radio("Ir a", ["Cargar lote", "Escaneo", "Control"], label_visibility="collapsed")
    st.divider()
    st.caption("Base de datos")
    st.code(str(DB_PATH))

if "pending_ml_item_id" not in st.session_state:
    st.session_state.pending_ml_item_id = None
if "validated_item_id" not in st.session_state:
    st.session_state.validated_item_id = None
if "validated_mode" not in st.session_state:
    st.session_state.validated_mode = ""
if "last_secondary_code" not in st.session_state:
    st.session_state.last_secondary_code = ""

# -------------------------
# Cargar lote
# -------------------------
if pantalla == "Cargar lote":
    st.subheader("Cargar lote FULL")
    full_file = st.file_uploader("Excel FULL", type=["xlsx"], key="full_file")

    maestro_upload = None
    if not MAESTRO_LOCAL_PATH.exists():
        maestro_upload = st.file_uploader("Maestro SKU/EAN opcional", type=["xlsx"], key="maestro_file")
    else:
        st.success(f"Maestro SKU/EAN detectado automáticamente: {MAESTRO_LOCAL_PATH}")

    maestro_df, maestro_msg = load_maestro(maestro_upload)
    st.caption(f"{maestro_msg} Registros: {len(maestro_df):,}".replace(",", "."))

    if full_file:
        try:
            df_preview = read_full_excel(full_file)
        except Exception as e:
            st.error(f"No se pudo leer el Excel FULL: {e}")
            st.stop()

        if df_preview.empty:
            st.error("No encontré filas válidas en el Excel. Revisa que tenga SKU/Código ML y Unidades.")
            st.stop()

        st.subheader("Vista previa")
        st.dataframe(df_preview.head(50), use_container_width=True, hide_index=True)

        default_lote = Path(full_file.name).stem
        lote_name = st.text_input("Nombre del lote", value=default_lote)
        col1, col2 = st.columns([1, 3])
        with col1:
            replace = st.checkbox("Reemplazar si existe", value=True)
        with col2:
            st.caption("Se guardará una base local SQLite independiente para esta app FULL.")

        if st.button("Crear / actualizar lote", type="primary"):
            lote_name = clean_text(lote_name) or default_lote
            if replace:
                clear_lote(lote_name)
            insert_lote(df_preview, lote_name)
            st.success(f"Lote '{lote_name}' cargado con {len(df_preview)} líneas.")

# -------------------------
# Escaneo
# -------------------------
elif pantalla == "Escaneo":
    st.subheader("Escaneo FULL")
    lotes = list_lotes()
    if not lotes:
        st.warning("Primero carga un lote FULL.")
        st.stop()

    lote = st.selectbox("Lote", lotes)
    items = load_items(lote)
    maestro_df, maestro_msg = load_maestro()
    ml_index = build_ml_index(items)
    secondary_index = build_secondary_index(items, maestro_df)

    total_unidades = int(items["unidades"].sum()) if not items.empty else 0
    total_acopiadas = int(items["acopiadas"].sum()) if not items.empty else 0
    pendientes = max(total_unidades - total_acopiadas, 0)

    c1, c2, c3 = st.columns(3)
    c1.metric("Solicitadas", total_unidades)
    c2.metric("Acopiadas", total_acopiadas)
    c3.metric("Pendientes", pendientes)
    st.progress(0 if total_unidades == 0 else min(total_acopiadas / total_unidades, 1.0))

    st.info("Flujo normal: escanea Código ML y luego SKU/EAN/Código Universal. Si es SUPERMERCADO, escanea solo SKU/EAN/Código Universal.")

    scan_code = st.text_input("Escanear código", key="scan_code_input", placeholder="Código ML, SKU, EAN o Código Universal")
    col_scan, col_reset = st.columns([1, 1])
    with col_scan:
        do_scan = st.button("Validar escaneo", type="primary")
    with col_reset:
        if st.button("Limpiar validación"):
            st.session_state.pending_ml_item_id = None
            st.session_state.validated_item_id = None
            st.session_state.validated_mode = ""
            st.session_state.last_secondary_code = ""
            st.rerun()

    if do_scan:
        code_key = norm_key(scan_code)
        if not code_key:
            st.warning("Escanea o ingresa un código válido.")
        else:
            # 1) Si hay Código ML pendiente, el siguiente código debe ser secundario del mismo producto.
            if st.session_state.pending_ml_item_id:
                pending_id = int(st.session_state.pending_ml_item_id)
                matches = secondary_index.get(code_key, [])
                if pending_id in matches:
                    st.session_state.validated_item_id = pending_id
                    st.session_state.validated_mode = "ML + SKU/EAN"
                    st.session_state.last_secondary_code = scan_code
                    st.session_state.pending_ml_item_id = None
                    st.success("Producto validado: Código ML + SKU/EAN coinciden.")
                else:
                    row = get_item(items, pending_id)
                    st.error(f"El segundo código no coincide con el producto escaneado por ML. Producto pendiente: {row.get('sku', '') if row is not None else ''}")

            # 2) Si no hay ML pendiente, revisar si el código es ML.
            elif code_key in ml_index:
                candidates = ml_index[code_key]
                if len(candidates) > 1:
                    st.warning("Este Código ML aparece en más de una línea. Revisa el lote antes de acopiar.")
                item_id = candidates[0]
                row = get_item(items, item_id)
                if row is not None and is_supermercado(row.get("identificacion", "")):
                    st.warning("Este producto está marcado como SUPERMERCADO. No requiere Código ML; valida por SKU/EAN/Código Universal.")
                else:
                    st.session_state.pending_ml_item_id = item_id
                    st.session_state.validated_item_id = None
                    st.session_state.validated_mode = ""
                    st.session_state.last_secondary_code = ""
                    st.success("Código ML validado. Ahora escanea SKU/EAN/Código Universal del producto.")

            # 3) Si no es ML, puede ser secundario. Solo permite directo si es SUPERMERCADO.
            elif code_key in secondary_index:
                candidates = secondary_index[code_key]
                supermarket_candidates = []
                normal_candidates = []
                for item_id in candidates:
                    row = get_item(items, item_id)
                    if row is not None and is_supermercado(row.get("identificacion", "")):
                        supermarket_candidates.append(item_id)
                    else:
                        normal_candidates.append(item_id)

                if supermarket_candidates:
                    item_id = supermarket_candidates[0]
                    st.session_state.validated_item_id = item_id
                    st.session_state.validated_mode = "SUPERMERCADO"
                    st.session_state.last_secondary_code = scan_code
                    st.session_state.pending_ml_item_id = None
                    st.success("Producto SUPERMERCADO validado por SKU/EAN/Código Universal.")
                else:
                    st.warning("Este código pertenece a un producto normal. Primero debes escanear el Código ML y luego este SKU/EAN/Código Universal.")
            else:
                st.error("Código no encontrado en el lote ni en el maestro SKU/EAN.")

    if st.session_state.pending_ml_item_id:
        row = get_item(items, int(st.session_state.pending_ml_item_id))
        if row is not None:
            st.warning("Código ML escaneado. Falta escanear SKU/EAN/Código Universal.")
            st.dataframe(pd.DataFrame([{
                "codigo_ml": row.get("codigo_ml", ""),
                "sku": row.get("sku", ""),
                "codigo_universal": row.get("codigo_universal", ""),
                "descripcion": row.get("descripcion", ""),
                "solicitadas": int(row.get("unidades", 0)),
                "acopiadas": int(row.get("acopiadas", 0)),
                "pendientes": remaining(row),
                "identificacion": row.get("identificacion", ""),
            }]), use_container_width=True, hide_index=True)

    if st.session_state.validated_item_id:
        # recargar para mostrar cantidades actuales
        items = load_items(lote)
        row = get_item(items, int(st.session_state.validated_item_id))
        if row is not None:
            st.success("Producto listo para agregar cantidad.")
            st.dataframe(pd.DataFrame([{
                "codigo_ml": row.get("codigo_ml", ""),
                "codigo_universal": row.get("codigo_universal", ""),
                "sku": row.get("sku", ""),
                "descripcion": row.get("descripcion", ""),
                "solicitadas": int(row.get("unidades", 0)),
                "acopiadas": int(row.get("acopiadas", 0)),
                "pendientes": remaining(row),
                "identificacion": row.get("identificacion", ""),
                "modo": st.session_state.validated_mode,
            }]), use_container_width=True, hide_index=True)

            qty_default = 1 if remaining(row) > 0 else 0
            qty = st.number_input("Cantidad a agregar", min_value=0, max_value=max(remaining(row), 0), value=qty_default, step=1)
            col_a, col_b = st.columns([1, 1])
            with col_a:
                if st.button("Agregar cantidad", type="primary"):
                    ok, msg = add_acopio(
                        int(row["id"]),
                        lote,
                        clean_text(row.get("codigo_ml", "")),
                        clean_text(st.session_state.last_secondary_code),
                        int(qty),
                        clean_text(st.session_state.validated_mode),
                    )
                    if ok:
                        st.success(msg)
                        st.session_state.validated_item_id = None
                        st.session_state.validated_mode = ""
                        st.session_state.last_secondary_code = ""
                        st.rerun()
                    else:
                        st.error(msg)
            with col_b:
                if st.button("Sin EAN / sin código universal"):
                    if is_supermercado(row.get("identificacion", "")):
                        st.error("SUPERMERCADO no usa esta excepción. Debe validarse por SKU/EAN/Código Universal.")
                    else:
                        st.session_state.last_secondary_code = "SIN_EAN"
                        st.session_state.validated_mode = "ML + SIN_EAN"
                        st.info("Excepción aplicada. Puedes agregar cantidad solo con Código ML validado.")

# -------------------------
# Control
# -------------------------
else:
    st.subheader("Control de lotes")
    lotes = list_lotes()
    if not lotes:
        st.warning("Primero carga un lote FULL.")
        st.stop()

    lote = st.selectbox("Lote", lotes)
    items = load_items(lote)
    if items.empty:
        st.warning("Lote vacío.")
        st.stop()

    items["pendiente"] = items["unidades"].astype(int) - items["acopiadas"].astype(int)
    items["estado"] = items["pendiente"].apply(lambda x: "COMPLETO" if x <= 0 else "PENDIENTE")

    total_unidades = int(items["unidades"].sum())
    total_acopiadas = int(items["acopiadas"].sum())
    total_pendientes = max(total_unidades - total_acopiadas, 0)
    lineas_pendientes = int((items["pendiente"] > 0).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unidades solicitadas", total_unidades)
    c2.metric("Unidades acopiadas", total_acopiadas)
    c3.metric("Unidades pendientes", total_pendientes)
    c4.metric("Líneas pendientes", lineas_pendientes)

    filtro = st.radio("Filtro", ["Todos", "Pendientes", "Completos"], horizontal=True)
    view = items.copy()
    if filtro == "Pendientes":
        view = view[view["pendiente"] > 0]
    elif filtro == "Completos":
        view = view[view["pendiente"] <= 0]

    cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "acopiadas", "pendiente", "identificacion", "vence", "dia", "hora", "estado"]
    st.dataframe(view[cols], use_container_width=True, hide_index=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        items[cols].to_excel(writer, sheet_name="control_full", index=False)
    st.download_button(
        "Descargar control Excel",
        data=output.getvalue(),
        file_name=f"control_{moneyless_filename(lote)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()
    if st.button("Borrar lote", type="secondary"):
        clear_lote(lote)
        st.success("Lote borrado.")
        st.rerun()
