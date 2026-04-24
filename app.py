import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "Control Envíos FULL Aurora"
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "aurora_full.db"

# -----------------------------
# Configuración Streamlit
# -----------------------------
st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")

# -----------------------------
# Utilidades base
# -----------------------------
def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)


def db_conn():
    ensure_data_dir()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_header(value) -> str:
    text = normalize_text(value).lower()
    replacements = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n",
        ".": "", "°": "", "º": "", "#": "", "/": " ", "-": " ", "_": " ",
    }
    for a, b in replacements.items():
        text = text.replace(a, b)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_code(value) -> str:
    """Normaliza SKU, EAN, Código ML o Código Universal sin destruir letras."""
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        # Evita notación científica cuando pandas leyó códigos largos como float.
        return ("%.0f" % value).strip()
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    text = text.replace("\u00a0", " ").strip()
    text = re.sub(r"\.0$", "", text)
    text = re.sub(r"\s+", "", text)
    return text.upper()


def split_possible_codes(value):
    text = normalize_text(value)
    if not text:
        return []
    parts = re.split(r"[,;/|\n\t]+", text)
    out = []
    for p in parts:
        c = normalize_code(p)
        if c:
            out.append(c)
    return list(dict.fromkeys(out))


FULL_ALIASES = {
    "area": ["area", "area"],
    "nro": ["n", "nro", "numero", "n bulto"],
    "codigo_ml": ["codigo ml", "cod ml", "codigo meli", "mlc", "publicacion", "n publicacion"],
    "codigo_universal": ["codigo universal", "cod universal", "ean", "codigo barras", "codigo de barras", "cod barras"],
    "sku": ["sku", "sku ml", "sku meli"],
    "title": ["descripcion", "descripci", "descripción", "title", "producto"],
    "qty_required": ["unidades", "cant", "cantidad", "qty", "qty required"],
    "identificacion": ["identificacion", "identificación", "etiqueta", "etiq", "etiquetar"],
    "instruccion": ["instruccion", "instrucción", "instr", "preparacion", "preparación"],
    "vence": ["vence", "vcto", "vencimiento"],
    "dia": ["dia", "día"],
    "hora": ["hora"],
}

MASTER_ALIASES = {
    "sku": ["sku", "sku ml", "sku meli"],
    "barcode": ["codigo de barras", "codigo barras", "cod barras", "ean", "codigo universal", "cod universal", "barcode"],
    "descripcion": ["descripcion", "descripción", "producto", "title"],
    "familia": ["familia", "family"],
}


def find_col(columns, aliases):
    normalized = {normalize_header(c): c for c in columns}
    for alias in aliases:
        key = normalize_header(alias)
        if key in normalized:
            return normalized[key]
    # Búsqueda parcial cuidadosa
    for alias in aliases:
        key = normalize_header(alias)
        for norm, original in normalized.items():
            if key and key == norm:
                return original
    return None


@st.cache_data(show_spinner=False)
def read_full_excel(file_bytes: bytes):
    xl = pd.ExcelFile(pd.io.common.BytesIO(file_bytes))
    frames = []
    warnings = []

    for sheet in xl.sheet_names:
        raw = pd.read_excel(pd.io.common.BytesIO(file_bytes), sheet_name=sheet, dtype=object)
        if raw.empty:
            continue
        raw = raw.dropna(how="all")
        if raw.empty:
            continue

        colmap = {field: find_col(raw.columns, aliases) for field, aliases in FULL_ALIASES.items()}
        if not colmap.get("sku") or not colmap.get("qty_required"):
            warnings.append(f"Hoja '{sheet}' omitida: no encontré SKU y/o Unidades/CANT.")
            continue

        out = pd.DataFrame()
        out["sheet"] = sheet.strip()
        for field in FULL_ALIASES.keys():
            col = colmap.get(field)
            out[field] = raw[col] if col else ""

        out["sku"] = out["sku"].map(normalize_code)
        out["codigo_ml"] = out["codigo_ml"].map(normalize_code)
        out["codigo_universal"] = out["codigo_universal"].map(normalize_code)
        out["area"] = out["area"].map(normalize_text)
        out["nro"] = out["nro"].map(normalize_text)
        out["title"] = out["title"].map(normalize_text)
        out["identificacion"] = out["identificacion"].map(normalize_text)
        out["instruccion"] = out["instruccion"].map(normalize_text)
        out["vence"] = out["vence"].map(normalize_text)
        out["dia"] = out["dia"].map(normalize_text)
        out["hora"] = out["hora"].map(normalize_text)
        out["qty_required"] = pd.to_numeric(out["qty_required"], errors="coerce").fillna(0).astype(int)
        out = out[(out["sku"] != "") & (out["qty_required"] > 0)]
        if not out.empty:
            frames.append(out)

    if not frames:
        return pd.DataFrame(), warnings

    rows = pd.concat(frames, ignore_index=True)

    def join_unique(series):
        values = [normalize_text(x) for x in series if normalize_text(x)]
        return ", ".join(dict.fromkeys(values))

    grouped = rows.groupby("sku", as_index=False).agg(
        title=("title", lambda s: next((x for x in s if normalize_text(x)), "")),
        qty_required=("qty_required", "sum"),
        codigos_ml=("codigo_ml", join_unique),
        codigos_universales=("codigo_universal", join_unique),
        areas=("area", join_unique),
        nros=("nro", join_unique),
        sheets=("sheet", join_unique),
        identificacion=("identificacion", join_unique),
        instruccion=("instruccion", join_unique),
        vence=("vence", join_unique),
        dia=("dia", join_unique),
        hora=("hora", join_unique),
    )
    grouped["qty_scanned"] = 0
    grouped["pending"] = grouped["qty_required"]
    grouped["status"] = "pendiente"
    return grouped, warnings


@st.cache_data(show_spinner=False)
def read_master_excel(file_bytes: bytes):
    xl = pd.ExcelFile(pd.io.common.BytesIO(file_bytes))
    frames = []
    warnings = []
    for sheet in xl.sheet_names:
        raw = pd.read_excel(pd.io.common.BytesIO(file_bytes), sheet_name=sheet, dtype=object)
        if raw.empty:
            continue
        raw = raw.dropna(how="all")
        colmap = {field: find_col(raw.columns, aliases) for field, aliases in MASTER_ALIASES.items()}
        if not colmap.get("sku") or not colmap.get("barcode"):
            warnings.append(f"Hoja maestro '{sheet}' omitida: no encontré SKU y/o código de barras.")
            continue
        out = pd.DataFrame()
        out["sku"] = raw[colmap["sku"]].map(normalize_code)
        out["barcode_raw"] = raw[colmap["barcode"]].map(normalize_text)
        out["descripcion"] = raw[colmap["descripcion"]].map(normalize_text) if colmap.get("descripcion") else ""
        out["familia"] = raw[colmap["familia"]].map(normalize_text) if colmap.get("familia") else ""
        out = out[out["sku"] != ""]
        frames.append(out)
    if not frames:
        return pd.DataFrame(), warnings
    base = pd.concat(frames, ignore_index=True)
    records = []
    for _, row in base.iterrows():
        for code in split_possible_codes(row["barcode_raw"]):
            records.append({
                "code": code,
                "sku": row["sku"],
                "descripcion": row.get("descripcion", ""),
                "familia": row.get("familia", ""),
            })
    df = pd.DataFrame(records).drop_duplicates() if records else pd.DataFrame(columns=["code", "sku", "descripcion", "familia"])
    return df, warnings


def init_db():
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS batches (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'abierto',
                source_file TEXT
            );
            CREATE TABLE IF NOT EXISTS full_items (
                batch_id TEXT NOT NULL,
                sku TEXT NOT NULL,
                title TEXT,
                qty_required INTEGER NOT NULL DEFAULT 0,
                qty_scanned INTEGER NOT NULL DEFAULT 0,
                codigos_ml TEXT,
                codigos_universales TEXT,
                areas TEXT,
                nros TEXT,
                sheets TEXT,
                identificacion TEXT,
                instruccion TEXT,
                vence TEXT,
                dia TEXT,
                hora TEXT,
                PRIMARY KEY (batch_id, sku)
            );
            CREATE TABLE IF NOT EXISTS scans (
                id TEXT PRIMARY KEY,
                batch_id TEXT NOT NULL,
                scanned_code TEXT NOT NULL,
                resolved_sku TEXT,
                match_type TEXT,
                qty INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                note TEXT
            );
            CREATE TABLE IF NOT EXISTS master_codes (
                code TEXT PRIMARY KEY,
                sku TEXT NOT NULL,
                descripcion TEXT,
                familia TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )


def save_master(master_df: pd.DataFrame):
    if master_df.empty:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    with db_conn() as conn:
        conn.execute("DELETE FROM master_codes")
        rows = [(r.code, r.sku, r.descripcion, r.familia, now) for r in master_df.itertuples(index=False)]
        conn.executemany(
            "INSERT OR REPLACE INTO master_codes (code, sku, descripcion, familia, updated_at) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
    return len(master_df)


def create_batch(name: str, source_file: str, items_df: pd.DataFrame):
    batch_id = str(uuid.uuid4())
    now = datetime.now().isoformat(timespec="seconds")
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO batches (id, name, created_at, status, source_file) VALUES (?, ?, ?, 'abierto', ?)",
            (batch_id, name, now, source_file),
        )
        rows = []
        for r in items_df.itertuples(index=False):
            rows.append((
                batch_id, r.sku, r.title, int(r.qty_required), 0, r.codigos_ml, r.codigos_universales,
                r.areas, r.nros, r.sheets, r.identificacion, r.instruccion, r.vence, r.dia, r.hora,
            ))
        conn.executemany(
            """
            INSERT INTO full_items
            (batch_id, sku, title, qty_required, qty_scanned, codigos_ml, codigos_universales, areas, nros, sheets, identificacion, instruccion, vence, dia, hora)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return batch_id


def list_batches():
    with db_conn() as conn:
        return pd.read_sql_query(
            """
            SELECT b.id, b.name, b.created_at, b.status, b.source_file,
                   COALESCE(SUM(i.qty_required),0) AS required,
                   COALESCE(SUM(i.qty_scanned),0) AS scanned,
                   COUNT(i.sku) AS skus
            FROM batches b
            LEFT JOIN full_items i ON i.batch_id = b.id
            GROUP BY b.id
            ORDER BY b.created_at DESC
            """,
            conn,
        )


def get_items(batch_id: str):
    with db_conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM full_items WHERE batch_id=? ORDER BY sku",
            conn,
            params=(batch_id,),
        )


def resolve_scan(batch_id: str, scanned_code: str):
    code = normalize_code(scanned_code)
    if not code:
        return None, "vacio"
    with db_conn() as conn:
        # 1) SKU directo en lote
        row = conn.execute("SELECT sku FROM full_items WHERE batch_id=? AND sku=?", (batch_id, code)).fetchone()
        if row:
            return row["sku"], "SKU"

        # 2) Código ML de la planilla FULL
        rows = conn.execute("SELECT sku, codigos_ml FROM full_items WHERE batch_id=? AND codigos_ml IS NOT NULL AND codigos_ml!=''", (batch_id,)).fetchall()
        for row in rows:
            if code in [normalize_code(x) for x in str(row["codigos_ml"]).split(",")]:
                return row["sku"], "CODIGO_ML"

        # 3) Código Universal/EAN incluido en FULL
        rows = conn.execute("SELECT sku, codigos_universales FROM full_items WHERE batch_id=? AND codigos_universales IS NOT NULL AND codigos_universales!=''", (batch_id,)).fetchall()
        for row in rows:
            if code in [normalize_code(x) for x in str(row["codigos_universales"]).split(",")]:
                return row["sku"], "CODIGO_UNIVERSAL_FULL"

        # 4) Maestro SKU/EAN
        row = conn.execute("SELECT sku FROM master_codes WHERE code=?", (code,)).fetchone()
        if row:
            sku = row["sku"]
            exists = conn.execute("SELECT sku FROM full_items WHERE batch_id=? AND sku=?", (batch_id, sku)).fetchone()
            if exists:
                return sku, "EAN_MAESTRO"
            return sku, "EAN_MAESTRO_NO_EN_LOTE"

    return None, "NO_ENCONTRADO"


def register_scan(batch_id: str, scanned_code: str, qty: int):
    code = normalize_code(scanned_code)
    sku, match_type = resolve_scan(batch_id, code)
    now = datetime.now().isoformat(timespec="seconds")
    scan_id = str(uuid.uuid4())

    if not sku or match_type in {"NO_ENCONTRADO", "EAN_MAESTRO_NO_EN_LOTE", "vacio"}:
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO scans (id, batch_id, scanned_code, resolved_sku, match_type, qty, created_at, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (scan_id, batch_id, code, sku, match_type, qty, now, "No suma al lote"),
            )
        return False, sku, match_type, "Código no encontrado dentro del lote FULL activo."

    with db_conn() as conn:
        item = conn.execute("SELECT qty_required, qty_scanned FROM full_items WHERE batch_id=? AND sku=?", (batch_id, sku)).fetchone()
        if not item:
            return False, sku, match_type, "SKU resuelto, pero no está en el lote."
        new_qty = int(item["qty_scanned"]) + int(qty)
        conn.execute("UPDATE full_items SET qty_scanned=? WHERE batch_id=? AND sku=?", (new_qty, batch_id, sku))
        conn.execute(
            "INSERT INTO scans (id, batch_id, scanned_code, resolved_sku, match_type, qty, created_at, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (scan_id, batch_id, code, sku, match_type, qty, now, "OK"),
        )
    if new_qty > int(item["qty_required"]):
        return True, sku, match_type, f"Escaneado OK, pero quedó con exceso: {new_qty}/{item['qty_required']}."
    return True, sku, match_type, f"Escaneado OK: {new_qty}/{item['qty_required']}."


def undo_last_scan(batch_id: str):
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM scans WHERE batch_id=? AND resolved_sku IS NOT NULL AND note='OK' ORDER BY created_at DESC LIMIT 1",
            (batch_id,),
        ).fetchone()
        if not row:
            return False, "No hay escaneos OK para deshacer."
        conn.execute(
            "UPDATE full_items SET qty_scanned=MAX(qty_scanned - ?, 0) WHERE batch_id=? AND sku=?",
            (int(row["qty"]), batch_id, row["resolved_sku"]),
        )
        conn.execute("DELETE FROM scans WHERE id=?", (row["id"],))
    return True, f"Se deshizo el último escaneo del SKU {row['resolved_sku']}."


def recent_scans(batch_id: str, limit=30):
    with db_conn() as conn:
        return pd.read_sql_query(
            "SELECT created_at, scanned_code, resolved_sku, match_type, qty, note FROM scans WHERE batch_id=? ORDER BY created_at DESC LIMIT ?",
            conn,
            params=(batch_id, limit),
        )


def export_batch_excel(batch_id: str):
    items = get_items(batch_id)
    items["pendiente"] = (items["qty_required"] - items["qty_scanned"]).clip(lower=0)
    items["exceso"] = (items["qty_scanned"] - items["qty_required"]).clip(lower=0)
    items["estado"] = items.apply(
        lambda r: "EXCESO" if r["exceso"] > 0 else ("COMPLETO" if r["pendiente"] == 0 else "PENDIENTE"), axis=1
    )
    scans = recent_scans(batch_id, 100000)
    out = pd.io.common.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        items.to_excel(writer, index=False, sheet_name="control_full")
        scans.to_excel(writer, index=False, sheet_name="escaneos")
    return out.getvalue()


def delete_batch(batch_id: str):
    with db_conn() as conn:
        conn.execute("DELETE FROM scans WHERE batch_id=?", (batch_id,))
        conn.execute("DELETE FROM full_items WHERE batch_id=?", (batch_id,))
        conn.execute("DELETE FROM batches WHERE id=?", (batch_id,))


# -----------------------------
# UI
# -----------------------------
init_db()
st.title(APP_TITLE)
st.caption("App limpia y separada del WMS: solo carga FULL, maestro SKU/EAN, escaneo y control.")

with st.sidebar:
    st.header("Menú")
    page = st.radio("Vista", ["1. Maestro SKU/EAN", "2. Cargar lote FULL", "3. Escaneo", "4. Control"], label_visibility="collapsed")
    st.divider()
    batches = list_batches()
    batch_options = {f"{r.name} · {r.created_at} · {int(r.scanned)}/{int(r.required)}": r.id for r in batches.itertuples(index=False)}
    selected_label = st.selectbox("Lote activo", list(batch_options.keys()), index=0 if batch_options else None, placeholder="Sin lotes") if batch_options else None
    active_batch_id = batch_options[selected_label] if selected_label else None

if page == "1. Maestro SKU/EAN":
    st.subheader("Maestro SKU/EAN")
    st.write("Sube el maestro para que el scanner pueda resolver códigos de barra hacia SKU.")
    master_file = st.file_uploader("Subir maestro_sku_ean.xlsx", type=["xlsx"], key="master_upload")
    if master_file:
        master_df, warnings = read_master_excel(master_file.getvalue())
        for w in warnings:
            st.warning(w)
        c1, c2, c3 = st.columns(3)
        c1.metric("Códigos EAN leídos", len(master_df))
        c2.metric("SKUs únicos", master_df["sku"].nunique() if not master_df.empty else 0)
        c3.metric("Duplicados", int(master_df.duplicated(["code"]).sum()) if not master_df.empty else 0)
        st.dataframe(master_df.head(50), use_container_width=True, hide_index=True)
        if st.button("Guardar maestro en base local", type="primary"):
            n = save_master(master_df)
            st.success(f"Maestro guardado: {n} códigos disponibles para escaneo.")
    with db_conn() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM master_codes").fetchone()["n"]
        updated = conn.execute("SELECT MAX(updated_at) AS u FROM master_codes").fetchone()["u"]
    st.info(f"Maestro actual en base: {count} códigos. Última actualización: {updated or 'sin datos'}.")

elif page == "2. Cargar lote FULL":
    st.subheader("Cargar lote FULL")
    full_file = st.file_uploader("Subir Excel FULL", type=["xlsx"], key="full_upload")
    if full_file:
        full_df, warnings = read_full_excel(full_file.getvalue())
        for w in warnings:
            st.warning(w)
        if full_df.empty:
            st.error("No pude leer productos válidos desde el Excel FULL.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("SKUs", len(full_df))
            c2.metric("Unidades requeridas", int(full_df["qty_required"].sum()))
            c3.metric("Hojas leídas", full_df["sheets"].nunique())
            st.markdown("#### Vista previa")
            st.dataframe(full_df.head(100), use_container_width=True, hide_index=True)
            default_name = f"FULL {datetime.now().strftime('%d-%m-%Y %H:%M')}"
            batch_name = st.text_input("Nombre del lote", value=default_name)
            if st.button("Crear lote FULL", type="primary"):
                batch_id = create_batch(batch_name, full_file.name, full_df)
                st.success(f"Lote creado correctamente: {batch_name}")
                st.session_state["last_batch_id"] = batch_id

elif page == "3. Escaneo":
    st.subheader("Escaneo FULL")
    if not active_batch_id:
        st.warning("Primero crea o selecciona un lote FULL.")
    else:
        items = get_items(active_batch_id)
        required = int(items["qty_required"].sum()) if not items.empty else 0
        scanned = int(items["qty_scanned"].sum()) if not items.empty else 0
        pending = max(required - scanned, 0)
        pct = (scanned / required * 100) if required else 0
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Requerido", required)
        c2.metric("Escaneado", scanned)
        c3.metric("Pendiente", pending)
        c4.metric("Avance", f"{pct:.1f}%")
        st.progress(min(pct / 100, 1.0))

        with st.form("scan_form", clear_on_submit=True):
            scan_code = st.text_input("Escanear Código ML, SKU o EAN", autofocus=True)
            qty = st.number_input("Cantidad a sumar", min_value=1, max_value=9999, value=1, step=1)
            submitted = st.form_submit_button("Registrar escaneo", type="primary")
        if submitted:
            ok, sku, match_type, msg = register_scan(active_batch_id, scan_code, int(qty))
            if ok:
                st.success(f"{msg} · SKU: {sku} · Tipo: {match_type}")
            else:
                st.error(f"{msg} · Tipo: {match_type}" + (f" · SKU resuelto: {sku}" if sku else ""))

        colu, _ = st.columns([1, 4])
        with colu:
            if st.button("Deshacer último escaneo"):
                ok, msg = undo_last_scan(active_batch_id)
                st.success(msg) if ok else st.warning(msg)

        st.markdown("#### Últimos escaneos")
        st.dataframe(recent_scans(active_batch_id), use_container_width=True, hide_index=True)

elif page == "4. Control":
    st.subheader("Control del lote")
    if not active_batch_id:
        st.warning("No hay lote seleccionado.")
    else:
        items = get_items(active_batch_id)
        if items.empty:
            st.warning("El lote no tiene items.")
        else:
            items["pendiente"] = (items["qty_required"] - items["qty_scanned"]).clip(lower=0)
            items["exceso"] = (items["qty_scanned"] - items["qty_required"]).clip(lower=0)
            items["estado"] = items.apply(
                lambda r: "EXCESO" if r["exceso"] > 0 else ("COMPLETO" if r["pendiente"] == 0 else "PENDIENTE"), axis=1
            )
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("SKUs", len(items))
            c2.metric("Completos", int((items["estado"] == "COMPLETO").sum()))
            c3.metric("Pendientes", int((items["estado"] == "PENDIENTE").sum()))
            c4.metric("Con exceso", int((items["estado"] == "EXCESO").sum()))

            filtro = st.segmented_control("Filtro", ["Todos", "Pendientes", "Completos", "Exceso"], default="Pendientes")
            view = items.copy()
            if filtro == "Pendientes":
                view = view[view["estado"] == "PENDIENTE"]
            elif filtro == "Completos":
                view = view[view["estado"] == "COMPLETO"]
            elif filtro == "Exceso":
                view = view[view["estado"] == "EXCESO"]

            cols = ["estado", "sku", "title", "qty_required", "qty_scanned", "pendiente", "exceso", "codigos_ml", "codigos_universales", "areas", "nros", "identificacion", "instruccion"]
            st.dataframe(view[cols], use_container_width=True, hide_index=True)

            export_bytes = export_batch_excel(active_batch_id)
            st.download_button(
                "Descargar control Excel",
                data=export_bytes,
                file_name=f"control_full_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            with st.expander("Zona peligrosa"):
                st.warning("Borrar el lote elimina sus items y escaneos. No borra el maestro SKU/EAN.")
                if st.button("Borrar lote seleccionado", type="secondary"):
                    delete_batch(active_batch_id)
                    st.success("Lote borrado. Recarga la página si aún aparece en el selector.")
