# app.py
# Aurora FULL - App limpia para control de envíos FULL
# Flujo: cargar lote FULL + maestro SKU/EAN + validar por Código ML + SKU/EAN antes de acopiar.

from __future__ import annotations

import io
import re
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

APP_TITLE = "Control FULL Aurora"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "aurora_full.db"

# =========================================================
# Helpers
# =========================================================

def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    s = str(value).strip()
    if s.lower() in {"nan", "none", "nat"}:
        return ""
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s.strip()


def normalize_key(value) -> str:
    s = clean_text(value).lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def normalize_code(value) -> str:
    s = clean_text(value).upper().replace(" ", "")
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    if re.fullmatch(r"\d+(\.\d+)?[eE][+-]?\d+", s):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    return s


def to_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(".", "").replace(",", ".").strip()
        return int(float(value))
    except Exception:
        return default


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def first_non_empty(values: Iterable) -> str:
    for v in values:
        s = clean_text(v)
        if s:
            return s
    return ""


# =========================================================
# DB
# =========================================================

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                source_file TEXT,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'abierto'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lot_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lot_id INTEGER NOT NULL,
                area TEXT,
                nro TEXT,
                codigo_ml TEXT,
                codigo_universal TEXT,
                sku TEXT NOT NULL,
                descripcion TEXT,
                unidades INTEGER NOT NULL DEFAULT 0,
                acopiado INTEGER NOT NULL DEFAULT 0,
                identificacion TEXT,
                vence TEXT,
                dia TEXT,
                hora TEXT,
                updated_at TEXT,
                UNIQUE(lot_id, sku),
                FOREIGN KEY(lot_id) REFERENCES lots(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lot_id INTEGER NOT NULL,
                item_id INTEGER,
                code TEXT NOT NULL,
                scan_type TEXT,
                qty INTEGER NOT NULL DEFAULT 0,
                result TEXT NOT NULL,
                message TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(lot_id) REFERENCES lots(id),
                FOREIGN KEY(item_id) REFERENCES lot_items(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS code_map (
                code TEXT PRIMARY KEY,
                sku TEXT NOT NULL,
                source TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        # Migraciones suaves para versiones previas
        cols = {r[1] for r in conn.execute("PRAGMA table_info(lot_items)").fetchall()}
        if "acopiado" not in cols:
            conn.execute("ALTER TABLE lot_items ADD COLUMN acopiado INTEGER NOT NULL DEFAULT 0")
        if "codigo_universal" not in cols:
            conn.execute("ALTER TABLE lot_items ADD COLUMN codigo_universal TEXT")
        conn.commit()


# =========================================================
# Excel FULL
# =========================================================

COLUMN_ALIASES: Dict[str, List[str]] = {
    "area": ["area", "area_", "area."],
    "nro": ["n", "nro", "numero", "nº", "n°", "no", "n_"],
    "codigo_ml": ["codigo_ml", "cod_ml", "código_ml", "codigo_meli", "mlc", "codigo_publicacion"],
    "codigo_universal": ["codigo_universal", "cod_universal", "código_universal", "ean", "codigo_barra", "codigo_barras", "barcode"],
    "sku": ["sku", "sku_ml", "codigo_sku"],
    "descripcion": ["descripcion", "descripción", "title", "producto", "nombre", "detalle"],
    "unidades": ["unidades", "cant", "cantidad", "qty", "qty_required"],
    "identificacion": ["identificacion", "identificación", "etiqueta", "etiq", "etiquetar"],
    "vence": ["vence", "vcto", "vencimiento"],
    "dia": ["dia", "día"],
    "hora": ["hora"],
}

FULL_COLS = [
    "area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion",
    "unidades", "identificacion", "vence", "dia", "hora"
]


def find_header_row(raw: pd.DataFrame) -> Optional[int]:
    aliases = {normalize_key(a) for vals in COLUMN_ALIASES.values() for a in vals}
    best_idx, best_score = None, 0
    for idx in range(min(len(raw), 30)):
        row = [normalize_key(x) for x in raw.iloc[idx].tolist()]
        score = sum(1 for cell in row if cell in aliases)
        if score > best_score:
            best_idx, best_score = idx, score
    return best_idx if best_score >= 2 else None


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = {col: normalize_key(col) for col in df.columns}
    rename = {}
    for final_col, aliases in COLUMN_ALIASES.items():
        alias_set = {normalize_key(a) for a in aliases}
        for original, norm in normalized.items():
            if norm in alias_set and final_col not in rename.values():
                rename[original] = final_col
                break
    return df.rename(columns=rename)


def read_full_excel(uploaded_file) -> pd.DataFrame:
    xls = pd.ExcelFile(uploaded_file)
    frames: List[pd.DataFrame] = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(uploaded_file, sheet_name=sheet, header=None, dtype=object)
        header_row = find_header_row(raw)
        if header_row is None:
            continue

        df = pd.read_excel(uploaded_file, sheet_name=sheet, header=header_row, dtype=object)
        df = df.dropna(how="all")
        df = rename_columns(df)
        if "sku" not in df.columns or "unidades" not in df.columns:
            continue

        out = pd.DataFrame()
        for col in FULL_COLS:
            out[col] = df[col] if col in df.columns else ""

        for col in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "identificacion", "vence", "dia", "hora"]:
            out[col] = out[col].map(clean_text)
        out["codigo_ml"] = out["codigo_ml"].map(normalize_code)
        out["codigo_universal"] = out["codigo_universal"].map(normalize_code)
        out["sku"] = out["sku"].map(normalize_code)
        out["unidades"] = out["unidades"].map(lambda x: to_int(x, 0))
        out = out[(out["sku"] != "") & (out["unidades"] > 0)]
        if not out.empty:
            frames.append(out)

    if not frames:
        return pd.DataFrame(columns=FULL_COLS)

    full = pd.concat(frames, ignore_index=True)
    grouped = full.groupby("sku", as_index=False).agg({
        "area": first_non_empty,
        "nro": first_non_empty,
        "codigo_ml": first_non_empty,
        "codigo_universal": first_non_empty,
        "descripcion": first_non_empty,
        "unidades": "sum",
        "identificacion": first_non_empty,
        "vence": first_non_empty,
        "dia": first_non_empty,
        "hora": first_non_empty,
    })
    return grouped[FULL_COLS]


# =========================================================
# Maestro SKU / EAN
# =========================================================

MASTER_ALIASES = {
    "sku": ["sku", "sku_ml", "codigo_sku", "código sku"],
    "ean": ["ean", "codigo universal", "código universal", "codigo_universal", "barcode", "codigo barras", "codigo de barras", "codigos de barras", "códigos de barras"],
}


def split_codes(value) -> List[str]:
    s = clean_text(value)
    if not s:
        return []
    parts = re.split(r"[,;|\s]+", s)
    out = []
    for p in parts:
        p = normalize_code(p)
        if p:
            out.append(p)
    return list(dict.fromkeys(out))


def read_master_sku_ean(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return pd.DataFrame(columns=["code", "sku", "source"])
    xls = pd.ExcelFile(uploaded_file)
    rows = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(uploaded_file, sheet_name=sheet, dtype=object)
        df = df.dropna(how="all")
        if df.empty:
            continue
        norm_cols = {c: normalize_key(c) for c in df.columns}
        sku_col = None
        ean_col = None
        sku_alias = {normalize_key(a) for a in MASTER_ALIASES["sku"]}
        ean_alias = {normalize_key(a) for a in MASTER_ALIASES["ean"]}
        for c, n in norm_cols.items():
            if sku_col is None and n in sku_alias:
                sku_col = c
            if ean_col is None and n in ean_alias:
                ean_col = c
        if sku_col is None:
            continue
        for _, r in df.iterrows():
            sku = normalize_code(r.get(sku_col, ""))
            if not sku:
                continue
            rows.append({"code": sku, "sku": sku, "source": "sku_maestro"})
            if ean_col is not None:
                for code in split_codes(r.get(ean_col, "")):
                    rows.append({"code": code, "sku": sku, "source": "ean_maestro"})
    return pd.DataFrame(rows).drop_duplicates("code") if rows else pd.DataFrame(columns=["code", "sku", "source"])


def import_code_map(map_df: pd.DataFrame) -> int:
    if map_df.empty:
        return 0
    with get_conn() as conn:
        n = 0
        for _, row in map_df.iterrows():
            code = normalize_code(row["code"])
            sku = normalize_code(row["sku"])
            if not code or not sku:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO code_map(code, sku, source, updated_at) VALUES (?, ?, ?, ?)",
                (code, sku, clean_text(row.get("source", "maestro")), now_str()),
            )
            n += 1
        conn.commit()
        return n


# =========================================================
# Lotes
# =========================================================

def create_lot(name: str, source_file: str, df: pd.DataFrame) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO lots(name, source_file, created_at, status) VALUES (?, ?, ?, 'abierto')",
            (name, source_file, now_str()),
        )
        lot_id = int(cur.lastrowid)
        for _, r in df.iterrows():
            conn.execute(
                """
                INSERT OR REPLACE INTO lot_items(
                    lot_id, area, nro, codigo_ml, codigo_universal, sku, descripcion,
                    unidades, acopiado, identificacion, vence, dia, hora, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
                """,
                (
                    lot_id,
                    clean_text(r.get("area")), clean_text(r.get("nro")), normalize_code(r.get("codigo_ml")),
                    normalize_code(r.get("codigo_universal")), normalize_code(r.get("sku")), clean_text(r.get("descripcion")),
                    to_int(r.get("unidades"), 0), clean_text(r.get("identificacion")), clean_text(r.get("vence")),
                    clean_text(r.get("dia")), clean_text(r.get("hora")), now_str(),
                ),
            )
        conn.commit()
        return lot_id


def get_lots() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query("SELECT * FROM lots ORDER BY id DESC", conn)


def get_items(lot_id: int) -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT *,
                   CASE
                       WHEN acopiado >= unidades THEN 'completo'
                       WHEN acopiado > 0 THEN 'parcial'
                       ELSE 'pendiente'
                   END AS estado,
                   MAX(unidades - acopiado, 0) AS pendiente
            FROM lot_items
            WHERE lot_id = ?
            ORDER BY estado DESC, area, nro, sku
            """,
            conn,
            params=(lot_id,),
        )
    return df


def log_scan(lot_id: int, item_id: Optional[int], code: str, scan_type: str, result: str, message: str, qty: int = 0) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO scans(lot_id, item_id, code, scan_type, qty, result, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (lot_id, item_id, normalize_code(code), scan_type, qty, result, message, now_str()),
        )
        conn.commit()


def find_by_ml(lot_id: int, code: str) -> Optional[sqlite3.Row]:
    code = normalize_code(code)
    with get_conn() as conn:
        return conn.execute("SELECT * FROM lot_items WHERE lot_id = ? AND codigo_ml = ?", (lot_id, code)).fetchone()


def find_by_product_code(lot_id: int, code: str) -> Optional[sqlite3.Row]:
    code = normalize_code(code)
    with get_conn() as conn:
        # 1) SKU directo o código universal del lote
        row = conn.execute(
            "SELECT * FROM lot_items WHERE lot_id = ? AND (sku = ? OR codigo_universal = ?)",
            (lot_id, code, code),
        ).fetchone()
        if row:
            return row
        # 2) Maestro SKU/EAN
        mapped = conn.execute("SELECT sku FROM code_map WHERE code = ?", (code,)).fetchone()
        if mapped:
            return conn.execute("SELECT * FROM lot_items WHERE lot_id = ? AND sku = ?", (lot_id, mapped["sku"])).fetchone()
    return None


def get_item_by_id(item_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM lot_items WHERE id = ?", (item_id,)).fetchone()


def add_qty(item_id: int, qty: int) -> Tuple[bool, str]:
    qty = max(1, int(qty))
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM lot_items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            return False, "Producto no encontrado."
        pendiente = int(row["unidades"]) - int(row["acopiado"])
        if pendiente <= 0:
            return False, "Este producto ya está completo."
        if qty > pendiente:
            return False, f"No puedes acopiar {qty}. Solo quedan pendientes {pendiente}."
        conn.execute(
            "UPDATE lot_items SET acopiado = acopiado + ?, updated_at = ? WHERE id = ?",
            (qty, now_str(), item_id),
        )
        conn.commit()
        return True, f"Cantidad agregada: {qty}."


def reset_scan_state() -> None:
    for k in ["ml_item_id", "prod_item_id", "verified_item_id", "ml_code", "prod_code", "sin_ean"]:
        st.session_state.pop(k, None)


# =========================================================
# UI
# =========================================================

st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")
st.title("📦 Control FULL Aurora")
st.caption("App limpia para validar envíos FULL por Código ML + SKU/EAN/Código Universal.")
init_db()

lots_df = get_lots()
with st.sidebar:
    st.header("Menú")
    page = st.radio("Ir a", ["1. Cargar lote", "2. Escaneo FULL", "3. Control"], label_visibility="collapsed")
    st.divider()
    if lots_df.empty:
        st.info("Todavía no hay lotes creados.")
        selected_lot_id = None
    else:
        lot_options = {f"#{r.id} - {r.name}": int(r.id) for r in lots_df.itertuples()}
        selected_lot_label = st.selectbox("Lote activo", list(lot_options.keys()))
        selected_lot_id = lot_options[selected_lot_label]

# -------------------------
# Cargar lote
# -------------------------
if page == "1. Cargar lote":
    st.subheader("Cargar lote FULL")
    full_file = st.file_uploader("Excel FULL", type=["xlsx"], key="full_file")
    master_file = st.file_uploader("Maestro SKU/EAN opcional", type=["xlsx"], key="master_file")

    if master_file is not None:
        master_df = read_master_sku_ean(master_file)
        n = import_code_map(master_df)
        st.success(f"Maestro cargado: {n} códigos disponibles para escaneo SKU/EAN.")
        with st.expander("Vista previa maestro", expanded=False):
            st.dataframe(master_df.head(50), use_container_width=True)

    if full_file is not None:
        df = read_full_excel(full_file)
        if df.empty:
            st.error("No pude leer productos válidos del Excel FULL. Revisa encabezados y hoja.")
        else:
            st.success(f"Archivo leído correctamente: {len(df)} SKUs / {int(df['unidades'].sum())} unidades.")
            st.markdown("#### Vista previa")
            st.dataframe(df.head(50), use_container_width=True, hide_index=True)
            st.caption("La columna 'sheet' fue eliminada porque no aporta al flujo FULL.")

            default_name = f"FULL {datetime.now().strftime('%d-%m-%Y %H:%M')}"
            lot_name = st.text_input("Nombre del lote", value=default_name)
            if st.button("Crear lote FULL", type="primary"):
                lot_id = create_lot(lot_name, full_file.name, df)
                reset_scan_state()
                st.success(f"Lote creado correctamente: #{lot_id}")
                st.rerun()

# -------------------------
# Escaneo
# -------------------------
elif page == "2. Escaneo FULL":
    st.subheader("Escaneo FULL")
    if selected_lot_id is None:
        st.warning("Primero crea o selecciona un lote.")
        st.stop()

    items = get_items(selected_lot_id)
    total_req = int(items["unidades"].sum()) if not items.empty else 0
    total_acop = int(items["acopiado"].sum()) if not items.empty else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Solicitado", total_req)
    c2.metric("Acopiado", total_acop)
    c3.metric("Pendiente", max(total_req - total_acop, 0))

    st.info("Regla: para agregar cantidad debes escanear Código ML + SKU/EAN/Código Universal. Si el producto no tiene EAN, escanea Código ML y usa el botón 'Sin EAN'.")

    with st.form("scan_form", clear_on_submit=True):
        scan_code = st.text_input("Escanear Código ML, SKU o EAN")
        submitted = st.form_submit_button("Procesar escaneo")

    if submitted:
        code = normalize_code(scan_code)
        if not code:
            st.warning("Escaneo vacío.")
        else:
            ml_row = find_by_ml(selected_lot_id, code)
            prod_row = find_by_product_code(selected_lot_id, code)

            if ml_row is not None:
                st.session_state["ml_item_id"] = int(ml_row["id"])
                st.session_state["ml_code"] = code
                log_scan(selected_lot_id, int(ml_row["id"]), code, "codigo_ml", "ok", "Código ML escaneado")
                st.success(f"Código ML reconocido: {ml_row['sku']}")
            elif prod_row is not None:
                st.session_state["prod_item_id"] = int(prod_row["id"])
                st.session_state["prod_code"] = code
                log_scan(selected_lot_id, int(prod_row["id"]), code, "sku_ean", "ok", "SKU/EAN/Código Universal escaneado")
                st.success(f"Producto reconocido: {prod_row['sku']}")
            else:
                log_scan(selected_lot_id, None, code, "desconocido", "error", "Código no encontrado en lote ni maestro")
                st.error("Código no encontrado en este lote ni en el maestro SKU/EAN.")

    ml_item_id = st.session_state.get("ml_item_id")
    prod_item_id = st.session_state.get("prod_item_id")

    st.markdown("### Validación actual")
    left, right = st.columns(2)
    with left:
        st.markdown("**1) Etiqueta Mercado Libre / Código ML**")
        if ml_item_id:
            row = get_item_by_id(int(ml_item_id))
            if row:
                st.success(f"OK: {row['codigo_ml']} | SKU {row['sku']}")
                st.write(row["descripcion"])
        else:
            st.warning("Pendiente escanear Código ML.")
    with right:
        st.markdown("**2) Producto físico / SKU-EAN-Código Universal**")
        if prod_item_id:
            row = get_item_by_id(int(prod_item_id))
            if row:
                st.success(f"OK: SKU {row['sku']} | CU/EAN {row['codigo_universal'] or 'sin dato'}")
                st.write(row["descripcion"])
        else:
            st.warning("Pendiente escanear SKU/EAN/Código Universal.")

    verified_id = None
    mismatch = False
    if ml_item_id and prod_item_id:
        if int(ml_item_id) == int(prod_item_id):
            verified_id = int(ml_item_id)
            st.session_state["verified_item_id"] = verified_id
        else:
            mismatch = True
            st.error("No coincide: el Código ML escaneado corresponde a un producto distinto al SKU/EAN escaneado. No se puede agregar cantidad.")

    if ml_item_id and not prod_item_id:
        if st.button("Sin EAN / sin código universal", type="secondary"):
            st.session_state["verified_item_id"] = int(ml_item_id)
            st.session_state["sin_ean"] = True
            log_scan(selected_lot_id, int(ml_item_id), st.session_state.get("ml_code", ""), "sin_ean", "ok", "Validado sin EAN por botón")
            st.rerun()

    verified_id = st.session_state.get("verified_item_id") if not mismatch else None

    if verified_id:
        item = get_item_by_id(int(verified_id))
        if item:
            solicitado = int(item["unidades"])
            acopiado = int(item["acopiado"])
            pendiente = max(solicitado - acopiado, 0)
            st.markdown("### Producto validado")
            st.write(f"**SKU:** {item['sku']}")
            st.write(f"**Descripción:** {item['descripcion']}")
            a, b, c = st.columns(3)
            a.metric("Solicitadas", solicitado)
            b.metric("Ya acopiadas", acopiado)
            c.metric("Pendientes", pendiente)

            if pendiente <= 0:
                st.success("Este producto ya está completo.")
                if st.button("Limpiar validación"):
                    reset_scan_state()
                    st.rerun()
            else:
                qty = st.number_input("Cantidad a agregar", min_value=1, max_value=pendiente, value=1, step=1)
                col_add, col_clear = st.columns([1, 1])
                with col_add:
                    if st.button("Agregar cantidad acopiada", type="primary"):
                        ok, msg = add_qty(int(verified_id), int(qty))
                        log_scan(selected_lot_id, int(verified_id), st.session_state.get("prod_code") or st.session_state.get("ml_code", ""), "acopio", "ok" if ok else "error", msg, int(qty) if ok else 0)
                        if ok:
                            st.success(msg)
                            reset_scan_state()
                            st.rerun()
                        else:
                            st.error(msg)
                with col_clear:
                    if st.button("Limpiar / nuevo producto"):
                        reset_scan_state()
                        st.rerun()

    st.markdown("### Pendientes del lote")
    view = get_items(selected_lot_id)
    cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "acopiado", "pendiente", "estado", "identificacion", "vence"]
    st.dataframe(view[cols], use_container_width=True, hide_index=True)

# -------------------------
# Control
# -------------------------
else:
    st.subheader("Control del lote")
    if selected_lot_id is None:
        st.warning("Primero crea o selecciona un lote.")
        st.stop()

    df = get_items(selected_lot_id)
    if df.empty:
        st.info("El lote no tiene productos.")
        st.stop()

    total_req = int(df["unidades"].sum())
    total_acop = int(df["acopiado"].sum())
    avance = (total_acop / total_req * 100) if total_req else 0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SKUs", len(df))
    c2.metric("Unidades solicitadas", total_req)
    c3.metric("Unidades acopiadas", total_acop)
    c4.metric("Avance", f"{avance:.1f}%")
    st.progress(min(avance / 100, 1.0))

    filtro = st.radio("Filtro", ["Todos", "Pendientes", "Parciales", "Completos"], horizontal=True)
    view = df.copy()
    if filtro == "Pendientes":
        view = view[view["estado"] == "pendiente"]
    elif filtro == "Parciales":
        view = view[view["estado"] == "parcial"]
    elif filtro == "Completos":
        view = view[view["estado"] == "completo"]

    cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "acopiado", "pendiente", "estado", "identificacion", "vence", "dia", "hora"]
    st.dataframe(view[cols], use_container_width=True, hide_index=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df[cols].to_excel(writer, index=False, sheet_name="control_full")
    st.download_button(
        "Descargar control Excel",
        data=output.getvalue(),
        file_name=f"control_full_lote_{selected_lot_id}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    with st.expander("Acciones administrativas", expanded=False):
        if st.button("Cerrar lote"):
            with get_conn() as conn:
                conn.execute("UPDATE lots SET status = 'cerrado' WHERE id = ?", (selected_lot_id,))
                conn.commit()
            st.success("Lote cerrado.")
            st.rerun()
        if st.button("Borrar lote seleccionado", type="secondary"):
            with get_conn() as conn:
                conn.execute("DELETE FROM scans WHERE lot_id = ?", (selected_lot_id,))
                conn.execute("DELETE FROM lot_items WHERE lot_id = ?", (selected_lot_id,))
                conn.execute("DELETE FROM lots WHERE id = ?", (selected_lot_id,))
                conn.commit()
            reset_scan_state()
            st.success("Lote borrado.")
            st.rerun()
