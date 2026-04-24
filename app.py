# app.py
# Aurora FULL - Control de envíos FULL
# App limpia: sin picking, sin sorting, sin packing, sin despacho.

from __future__ import annotations

import io
import re
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

APP_TITLE = "Control FULL Aurora"
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "aurora_full.db"
DATA_DIR.mkdir(exist_ok=True)

# -----------------------------
# Utilidades generales
# -----------------------------

def clean_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip()
    if s.endswith(".0") and s.replace(".0", "", 1).isdigit():
        s = s[:-2]
    return s.strip()


def normalize_key(text: str) -> str:
    text = clean_text(text).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def normalize_code(value) -> str:
    s = clean_text(value).upper()
    s = s.replace(" ", "")
    if s.endswith(".0"):
        s = s[:-2]
    return s


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_int(value, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        if isinstance(value, str):
            value = value.replace(".", "").replace(",", ".").strip()
        return int(float(value))
    except Exception:
        return default

# -----------------------------
# Base SQLite
# -----------------------------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                source_file TEXT,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'abierto'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lot_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lot_id INTEGER NOT NULL,
                sheet TEXT,
                area TEXT,
                nro TEXT,
                codigo_ml TEXT,
                codigo_universal TEXT,
                sku TEXT NOT NULL,
                descripcion TEXT,
                unidades INTEGER NOT NULL DEFAULT 0,
                identificacion TEXT,
                vence TEXT,
                dia TEXT,
                hora TEXT,
                escaneado INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT,
                UNIQUE(lot_id, sku),
                FOREIGN KEY(lot_id) REFERENCES lots(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lot_id INTEGER NOT NULL,
                item_id INTEGER,
                code TEXT NOT NULL,
                matched_by TEXT,
                sku TEXT,
                qty INTEGER NOT NULL DEFAULT 1,
                result TEXT NOT NULL,
                message TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(lot_id) REFERENCES lots(id),
                FOREIGN KEY(item_id) REFERENCES lot_items(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS code_map (
                code TEXT PRIMARY KEY,
                sku TEXT NOT NULL,
                source TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()

# -----------------------------
# Lector Excel FULL
# -----------------------------

COLUMN_ALIASES: Dict[str, List[str]] = {
    "area": ["area", "area_"],
    "nro": ["n", "nro", "numero", "nº", "n°", "no"],
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

REQUIRED_FULL_COLS = ["sku", "unidades"]
OUTPUT_FULL_COLS = [
    "sheet", "area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion",
    "unidades", "identificacion", "vence", "dia", "hora"
]


def find_header_row(raw: pd.DataFrame) -> Optional[int]:
    best_idx = None
    best_score = 0
    alias_flat = {a for values in COLUMN_ALIASES.values() for a in values}
    for idx in range(min(len(raw), 25)):
        row = [normalize_key(x) for x in raw.iloc[idx].tolist()]
        score = sum(1 for cell in row if cell in alias_flat)
        if score > best_score:
            best_idx = idx
            best_score = score
    return best_idx if best_score >= 2 else None


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    current = {col: normalize_key(col) for col in df.columns}
    rename = {}
    for final_col, aliases in COLUMN_ALIASES.items():
        aliases_norm = set(normalize_key(a) for a in aliases)
        for original, norm in current.items():
            if norm in aliases_norm:
                rename[original] = final_col
                break
    return df.rename(columns=rename)


def read_full_excel(uploaded_file) -> pd.DataFrame:
    xls = pd.ExcelFile(uploaded_file)
    frames = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(uploaded_file, sheet_name=sheet, header=None, dtype=object)
        header_row = find_header_row(raw)
        if header_row is None:
            continue

        df = pd.read_excel(uploaded_file, sheet_name=sheet, header=header_row, dtype=object)
        df = df.dropna(how="all")
        df = rename_columns(df)

        if not all(col in df.columns for col in REQUIRED_FULL_COLS):
            continue

        out = pd.DataFrame()
        for col in OUTPUT_FULL_COLS:
            if col == "sheet":
                out[col] = sheet
            elif col in df.columns:
                out[col] = df[col]
            else:
                out[col] = ""

        for col in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "identificacion", "vence", "dia", "hora"]:
            out[col] = out[col].map(clean_text)

        out["codigo_ml"] = out["codigo_ml"].map(normalize_code)
        out["codigo_universal"] = out["codigo_universal"].map(normalize_code)
        out["sku"] = out["sku"].map(normalize_code)
        out["unidades"] = out["unidades"].map(lambda x: to_int(x, 0))

        out = out[(out["sku"] != "") & (out["unidades"] > 0)]
        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=OUTPUT_FULL_COLS)

    full = pd.concat(frames, ignore_index=True)

    # Agrupar por SKU para que un SKU repetido en varias hojas quede como una sola línea operacional.
    grouped = (
        full.groupby("sku", as_index=False)
        .agg({
            "sheet": lambda x: ", ".join(sorted(set(map(str, x))))[:250],
            "area": lambda x: first_non_empty(x),
            "nro": lambda x: first_non_empty(x),
            "codigo_ml": lambda x: first_non_empty(x),
            "codigo_universal": lambda x: first_non_empty(x),
            "descripcion": lambda x: first_non_empty(x),
            "unidades": "sum",
            "identificacion": lambda x: first_non_empty(x),
            "vence": lambda x: first_non_empty(x),
            "dia": lambda x: first_non_empty(x),
            "hora": lambda x: first_non_empty(x),
        })
    )
    return grouped[OUTPUT_FULL_COLS]


def first_non_empty(values) -> str:
    for v in values:
        s = clean_text(v)
        if s:
            return s
    return ""

# -----------------------------
# Maestro SKU / EAN
# -----------------------------

MASTER_ALIASES = {
    "sku": ["sku", "sku_ml", "codigo_sku"],
    "ean": ["ean", "codigo_universal", "cod_universal", "codigo_barra", "codigo_barras", "barcode", "código universal"],
}


def read_master_sku_ean(uploaded_file) -> pd.DataFrame:
    xls = pd.ExcelFile(uploaded_file)
    frames = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(uploaded_file, sheet_name=sheet, dtype=object)
        if df.empty:
            continue
        df = rename_master_columns(df)
        if "sku" not in df.columns:
            continue
        ean_cols = [c for c in df.columns if c == "ean" or str(c).startswith("ean_")]
        if "ean" in df.columns:
            ean_cols = ["ean"] + [c for c in ean_cols if c != "ean"]
        if not ean_cols:
            # Si no hay EAN explícito, igual dejamos SKU como código válido.
            tmp = pd.DataFrame({"sku": df["sku"].map(normalize_code), "ean": df["sku"].map(normalize_code)})
            frames.append(tmp)
            continue
        for ean_col in ean_cols:
            tmp = pd.DataFrame({"sku": df["sku"], "ean": df[ean_col]})
            frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=["code", "sku"])

    out = pd.concat(frames, ignore_index=True)
    out["sku"] = out["sku"].map(normalize_code)
    out["code"] = out["ean"].map(normalize_code)
    out = out[(out["sku"] != "") & (out["code"] != "")]
    out = out[["code", "sku"]].drop_duplicates()
    return out


def rename_master_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_cols = {col: normalize_key(col) for col in df.columns}
    rename = {}
    for final_col, aliases in MASTER_ALIASES.items():
        aliases_norm = set(normalize_key(a) for a in aliases)
        matches = [original for original, norm in normalized_cols.items() if norm in aliases_norm]
        if final_col == "ean":
            for i, original in enumerate(matches):
                rename[original] = "ean" if i == 0 else f"ean_{i+1}"
        elif matches:
            rename[matches[0]] = final_col
    return df.rename(columns=rename)

# -----------------------------
# Operaciones lote y escaneo
# -----------------------------

def create_lot(name: str, source_file: str, df: pd.DataFrame) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO lots(name, source_file, created_at, status) VALUES (?, ?, ?, 'abierto')",
            (name, source_file, now_str()),
        )
        lot_id = int(cur.lastrowid)
        rows = []
        for _, r in df.iterrows():
            rows.append((
                lot_id,
                clean_text(r.get("sheet")),
                clean_text(r.get("area")),
                clean_text(r.get("nro")),
                normalize_code(r.get("codigo_ml")),
                normalize_code(r.get("codigo_universal")),
                normalize_code(r.get("sku")),
                clean_text(r.get("descripcion")),
                to_int(r.get("unidades"), 0),
                clean_text(r.get("identificacion")),
                clean_text(r.get("vence")),
                clean_text(r.get("dia")),
                clean_text(r.get("hora")),
                now_str(),
            ))
        conn.executemany(
            """
            INSERT INTO lot_items(
                lot_id, sheet, area, nro, codigo_ml, codigo_universal, sku, descripcion,
                unidades, identificacion, vence, dia, hora, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        return lot_id


def list_lots() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query("SELECT * FROM lots ORDER BY id DESC", conn)


def load_lot_items(lot_id: int) -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT *,
                   CASE
                     WHEN escaneado >= unidades THEN 'completo'
                     WHEN escaneado > 0 THEN 'parcial'
                     ELSE 'pendiente'
                   END AS estado,
                   MAX(unidades - escaneado, 0) AS pendiente
            FROM lot_items
            WHERE lot_id = ?
            ORDER BY estado DESC, sku
            """,
            conn,
            params=(lot_id,),
        )
    return df


def import_code_map(master_df: pd.DataFrame) -> int:
    if master_df.empty:
        return 0
    rows = [(r["code"], r["sku"], "maestro_sku_ean", now_str()) for _, r in master_df.iterrows()]
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO code_map(code, sku, source, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                sku = excluded.sku,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def build_lot_code_map(lot_id: int) -> Dict[str, Tuple[str, str]]:
    """Retorna code -> (sku, matched_by). Incluye Código ML, Código Universal, SKU y maestro EAN."""
    mapping: Dict[str, Tuple[str, str]] = {}
    items = load_lot_items(lot_id)
    for _, r in items.iterrows():
        sku = normalize_code(r.get("sku"))
        for field, matched_by in [
            ("sku", "SKU"),
            ("codigo_ml", "Código ML"),
            ("codigo_universal", "Código Universal FULL"),
        ]:
            code = normalize_code(r.get(field))
            if code:
                mapping[code] = (sku, matched_by)

    with get_conn() as conn:
        cm = pd.read_sql_query("SELECT code, sku FROM code_map", conn)
    lot_skus = set(items["sku"].map(normalize_code)) if not items.empty else set()
    for _, r in cm.iterrows():
        code = normalize_code(r.get("code"))
        sku = normalize_code(r.get("sku"))
        if code and sku in lot_skus:
            mapping[code] = (sku, "EAN maestro")
    return mapping


def register_scan(lot_id: int, raw_code: str, qty: int = 1) -> Tuple[str, str]:
    code = normalize_code(raw_code)
    if not code:
        return "warning", "Escaneo vacío."
    qty = max(1, int(qty))
    mapping = build_lot_code_map(lot_id)

    if code not in mapping:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO scans(lot_id, code, qty, result, message, created_at) VALUES (?, ?, ?, 'no_encontrado', ?, ?)",
                (lot_id, code, qty, "Código no encontrado en este lote ni en maestro", now_str()),
            )
            conn.commit()
        return "error", f"No encontrado: {code}"

    sku, matched_by = mapping[code]
    with get_conn() as conn:
        item = conn.execute(
            "SELECT * FROM lot_items WHERE lot_id = ? AND sku = ?",
            (lot_id, sku),
        ).fetchone()
        if not item:
            conn.execute(
                "INSERT INTO scans(lot_id, code, matched_by, sku, qty, result, message, created_at) VALUES (?, ?, ?, ?, ?, 'no_en_lote', ?, ?)",
                (lot_id, code, matched_by, sku, qty, "SKU mapeado, pero no está en el lote", now_str()),
            )
            conn.commit()
            return "error", f"El código pertenece al SKU {sku}, pero ese SKU no está en el lote."

        new_scanned = int(item["escaneado"]) + qty
        conn.execute(
            "UPDATE lot_items SET escaneado = ?, updated_at = ? WHERE id = ?",
            (new_scanned, now_str(), int(item["id"])),
        )
        result = "ok" if new_scanned <= int(item["unidades"]) else "exceso"
        msg = f"{matched_by}: {code} → SKU {sku}. Escaneado {new_scanned}/{int(item['unidades'])}."
        conn.execute(
            """
            INSERT INTO scans(lot_id, item_id, code, matched_by, sku, qty, result, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (lot_id, int(item["id"]), code, matched_by, sku, qty, result, msg, now_str()),
        )
        conn.commit()

    if result == "exceso":
        return "warning", "Exceso: " + msg
    return "success", msg


def undo_last_scan(lot_id: int) -> Tuple[str, str]:
    with get_conn() as conn:
        scan = conn.execute(
            "SELECT * FROM scans WHERE lot_id = ? AND item_id IS NOT NULL ORDER BY id DESC LIMIT 1",
            (lot_id,),
        ).fetchone()
        if not scan:
            return "warning", "No hay escaneos para deshacer."
        item = conn.execute("SELECT * FROM lot_items WHERE id = ?", (int(scan["item_id"]),)).fetchone()
        if not item:
            return "error", "No se encontró el ítem del último escaneo."
        new_qty = max(0, int(item["escaneado"]) - int(scan["qty"]))
        conn.execute("UPDATE lot_items SET escaneado = ?, updated_at = ? WHERE id = ?", (new_qty, now_str(), int(item["id"])))
        conn.execute(
            "INSERT INTO scans(lot_id, item_id, code, matched_by, sku, qty, result, message, created_at) VALUES (?, ?, ?, ?, ?, ?, 'deshacer', ?, ?)",
            (lot_id, int(item["id"]), clean_text(scan["code"]), clean_text(scan["matched_by"]), clean_text(scan["sku"]), -int(scan["qty"]), "Deshacer último escaneo", now_str()),
        )
        conn.commit()
    return "success", f"Deshecho último escaneo del SKU {item['sku']}."


def recent_scans(lot_id: int, limit: int = 20) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT created_at, code, matched_by, sku, qty, result, message FROM scans WHERE lot_id = ? ORDER BY id DESC LIMIT ?",
            conn,
            params=(lot_id, limit),
        )


def export_lot_excel(lot_id: int) -> bytes:
    items = load_lot_items(lot_id)
    scans = recent_scans(lot_id, 100000)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        items.to_excel(writer, index=False, sheet_name="control_full")
        scans.to_excel(writer, index=False, sheet_name="escaneos")
    return output.getvalue()

# -----------------------------
# UI Streamlit
# -----------------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")
init_db()

st.title(APP_TITLE)
st.caption("App limpia exclusiva para FULL: escaneo por Código ML, SKU o EAN con maestro SKU/EAN.")

with st.sidebar:
    st.header("Menú")
    page = st.radio("Ir a", ["1. Cargar lote", "2. Escanear", "3. Control"], label_visibility="collapsed")

lots_df = list_lots()
lot_options = []
if not lots_df.empty:
    for _, r in lots_df.iterrows():
        lot_options.append((int(r["id"]), f"#{int(r['id'])} - {r['name']} ({r['status']})"))

# -----------------------------
# Página 1: Cargar lote
# -----------------------------
if page == "1. Cargar lote":
    st.subheader("Cargar nuevo lote FULL")

    col1, col2 = st.columns(2)
    with col1:
        full_file = st.file_uploader("Excel FULL", type=["xlsx"], key="full_file")
    with col2:
        master_file = st.file_uploader("Maestro SKU/EAN opcional", type=["xlsx"], key="master_file")

    if master_file is not None:
        try:
            master_df = read_master_sku_ean(master_file)
            st.success(f"Maestro leído: {len(master_df)} códigos mapeados.")
            st.dataframe(master_df.head(50), use_container_width=True, hide_index=True)
            if st.button("Guardar maestro SKU/EAN en la app"):
                n = import_code_map(master_df)
                st.success(f"Maestro guardado: {n} códigos.")
        except Exception as e:
            st.error(f"No se pudo leer el maestro SKU/EAN: {e}")

    if full_file is not None:
        try:
            df_full = read_full_excel(full_file)
            if df_full.empty:
                st.error("No se encontraron columnas válidas de FULL en el Excel.")
            else:
                st.success(f"Excel FULL leído: {len(df_full)} SKUs agrupados.")
                st.dataframe(df_full.head(50), use_container_width=True, hide_index=True)

                default_name = f"FULL {datetime.now().strftime('%d-%m-%Y %H:%M')}"
                lot_name = st.text_input("Nombre del lote", value=default_name)
                if st.button("Crear lote FULL", type="primary"):
                    lot_id = create_lot(lot_name, getattr(full_file, "name", "archivo_full.xlsx"), df_full)
                    st.success(f"Lote creado correctamente: #{lot_id}")
                    st.info("Ahora ve a 'Escanear' para comenzar el control.")
        except Exception as e:
            st.error(f"Error leyendo Excel FULL: {e}")

# -----------------------------
# Página 2: Escanear
# -----------------------------
elif page == "2. Escanear":
    st.subheader("Escaneo FULL")
    if not lot_options:
        st.warning("Primero debes crear un lote FULL.")
    else:
        selected_label = st.selectbox("Lote", [label for _, label in lot_options])
        lot_id = next(i for i, label in lot_options if label == selected_label)

        items = load_lot_items(lot_id)
        total_req = int(items["unidades"].sum()) if not items.empty else 0
        total_scan = int(items["escaneado"].sum()) if not items.empty else 0
        completed = int((items["escaneado"] >= items["unidades"]).sum()) if not items.empty else 0
        progress = (total_scan / total_req) if total_req else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Unidades requeridas", total_req)
        c2.metric("Unidades escaneadas", total_scan)
        c3.metric("SKUs completos", completed)
        c4.metric("Avance", f"{progress:.1%}")
        st.progress(min(progress, 1.0))

        with st.form("scan_form", clear_on_submit=True):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                scan_code = st.text_input("Escanear Código ML, SKU o EAN")
            with col_b:
                qty = st.number_input("Cantidad", min_value=1, max_value=9999, value=1, step=1)
            submitted = st.form_submit_button("Registrar escaneo", type="primary")

        if submitted:
            level, msg = register_scan(lot_id, scan_code, int(qty))
            getattr(st, level)(msg)
            st.rerun()

        if st.button("Deshacer último escaneo"):
            level, msg = undo_last_scan(lot_id)
            getattr(st, level)(msg)
            st.rerun()

        st.markdown("### Pendientes")
        pending = items[items["escaneado"] < items["unidades"]].copy()
        st.dataframe(
            pending[["sku", "codigo_ml", "codigo_universal", "descripcion", "unidades", "escaneado", "pendiente", "estado"]].head(100),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("### Últimos escaneos")
        st.dataframe(recent_scans(lot_id, 20), use_container_width=True, hide_index=True)

# -----------------------------
# Página 3: Control
# -----------------------------
elif page == "3. Control":
    st.subheader("Panel de control FULL")
    if not lot_options:
        st.warning("No hay lotes creados.")
    else:
        selected_label = st.selectbox("Lote", [label for _, label in lot_options], key="control_lot")
        lot_id = next(i for i, label in lot_options if label == selected_label)
        items = load_lot_items(lot_id)

        total_req = int(items["unidades"].sum()) if not items.empty else 0
        total_scan = int(items["escaneado"].sum()) if not items.empty else 0
        pendientes = int((items["escaneado"] < items["unidades"]).sum()) if not items.empty else 0
        excesos = int((items["escaneado"] > items["unidades"]).sum()) if not items.empty else 0
        progress = (total_scan / total_req) if total_req else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Requerido", total_req)
        c2.metric("Escaneado", total_scan)
        c3.metric("SKUs pendientes", pendientes)
        c4.metric("SKUs con exceso", excesos)
        st.progress(min(progress, 1.0))

        status_filter = st.multiselect(
            "Filtrar estado",
            ["pendiente", "parcial", "completo"],
            default=["pendiente", "parcial", "completo"],
        )
        view = items[items["estado"].isin(status_filter)].copy() if status_filter else items
        st.dataframe(view, use_container_width=True, hide_index=True)

        excel_bytes = export_lot_excel(lot_id)
        st.download_button(
            "Descargar control Excel",
            data=excel_bytes,
            file_name=f"control_full_lote_{lot_id}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        with st.expander("Administración del lote"):
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Cerrar lote"):
                    with get_conn() as conn:
                        conn.execute("UPDATE lots SET status = 'cerrado' WHERE id = ?", (lot_id,))
                        conn.commit()
                    st.success("Lote cerrado.")
                    st.rerun()
            with col2:
                confirm = st.text_input("Escribe BORRAR para eliminar este lote")
                if st.button("Borrar lote definitivamente"):
                    if confirm == "BORRAR":
                        with get_conn() as conn:
                            conn.execute("DELETE FROM scans WHERE lot_id = ?", (lot_id,))
                            conn.execute("DELETE FROM lot_items WHERE lot_id = ?", (lot_id,))
                            conn.execute("DELETE FROM lots WHERE id = ?", (lot_id,))
                            conn.commit()
                        st.success("Lote borrado.")
                        st.rerun()
                    else:
                        st.warning("Debes escribir BORRAR para confirmar.")
