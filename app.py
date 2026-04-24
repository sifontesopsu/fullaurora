
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
# Limpieza / normalización
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
    rep = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n", "º": "", "°": ""}
    for a, b in rep.items():
        text = text.replace(a, b)
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def norm_code(value) -> str:
    text = clean_text(value).upper()
    rep = {"Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ñ": "N"}
    for a, b in rep.items():
        text = text.replace(a, b)
    # evita notación científica cuando Excel la interpreta así
    if re.fullmatch(r"\d+(\.\d+)?[eE][+-]?\d+", text):
        try:
            text = str(int(float(text)))
        except Exception:
            pass
    return re.sub(r"[^A-Z0-9]+", "", text)


def to_int(value, default=0) -> int:
    try:
        txt = clean_text(value).replace(".", "").replace(",", ".")
        if txt == "":
            return default
        return int(float(txt))
    except Exception:
        return default


def is_supermercado(value) -> bool:
    return "SUPERMERCADO" in norm_code(value)


def looks_like_identificacion(value) -> bool:
    k = norm_code(value)
    return any(x in k for x in ["ETIQUET", "SUPERMERCADO", "SINEAN", "SINETIQUETA"])


# ============================================================
# SQLite
# ============================================================

def conn():
    ensure_dirs()
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS lotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            archivo TEXT,
            created_at TEXT NOT NULL,
            estado TEXT DEFAULT 'abierto'
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lote_id INTEGER NOT NULL,
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
            lote_id INTEGER NOT NULL,
            item_id INTEGER,
            scan_ml TEXT,
            scan_secundario TEXT,
            cantidad INTEGER DEFAULT 0,
            modo TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS maestro (
            code TEXT PRIMARY KEY,
            sku TEXT NOT NULL,
            descripcion TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    c.commit()
    return c


def init_db():
    with conn():
        pass


def list_lotes():
    with conn() as c:
        return pd.read_sql_query(
            """
            SELECT l.id, l.nombre, l.archivo, l.created_at, l.estado,
                   COALESCE(SUM(i.unidades),0) AS unidades,
                   COALESCE(SUM(i.acopiadas),0) AS acopiadas,
                   COUNT(i.id) AS lineas
            FROM lotes l
            LEFT JOIN items i ON i.lote_id = l.id
            GROUP BY l.id
            ORDER BY l.id DESC
            """,
            c,
        )


def get_items(lote_id: int) -> pd.DataFrame:
    with conn() as c:
        return pd.read_sql_query("SELECT * FROM items WHERE lote_id=? ORDER BY id", c, params=(lote_id,))


def get_item(item_id: int):
    with conn() as c:
        return c.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()


def create_lote(nombre: str, archivo: str, df: pd.DataFrame):
    now = datetime.now().isoformat(timespec="seconds")
    with conn() as c:
        cur = c.execute(
            "INSERT INTO lotes (nombre, archivo, created_at, estado) VALUES (?, ?, ?, 'abierto')",
            (nombre, archivo, now),
        )
        lote_id = cur.lastrowid
        rows = []
        for _, r in df.iterrows():
            rows.append((
                lote_id,
                clean_text(r.get("area")),
                clean_text(r.get("nro")),
                clean_text(r.get("codigo_ml")),
                clean_text(r.get("codigo_universal")),
                clean_text(r.get("sku")),
                clean_text(r.get("descripcion")),
                to_int(r.get("unidades")),
                clean_text(r.get("identificacion")),
                clean_text(r.get("vence")),
                clean_text(r.get("dia")),
                clean_text(r.get("hora")),
                0,
                now,
            ))
        c.executemany(
            """
            INSERT INTO items
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades,
             identificacion, vence, dia, hora, acopiadas, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()
        return lote_id


def delete_lote(lote_id: int):
    with conn() as c:
        c.execute("DELETE FROM scans WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM items WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM lotes WHERE id=?", (lote_id,))
        c.commit()


def add_acopio(lote_id: int, item_id: int, qty: int, scan_ml: str, scan_sec: str, modo: str):
    now = datetime.now().isoformat(timespec="seconds")
    with conn() as c:
        item = c.execute("SELECT unidades, acopiadas FROM items WHERE id=? AND lote_id=?", (item_id, lote_id)).fetchone()
        if not item:
            return False, "Producto no encontrado en el lote activo."
        unidades = int(item["unidades"] or 0)
        acopiadas = int(item["acopiadas"] or 0)
        pendiente = max(unidades - acopiadas, 0)
        if qty <= 0:
            return False, "La cantidad debe ser mayor a cero."
        if qty > pendiente:
            return False, f"No puedes agregar {qty}. Pendiente actual: {pendiente}."
        c.execute("UPDATE items SET acopiadas=acopiadas+?, updated_at=? WHERE id=?", (qty, now, item_id))
        c.execute(
            """
            INSERT INTO scans (lote_id, item_id, scan_ml, scan_secundario, cantidad, modo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (lote_id, item_id, clean_text(scan_ml), clean_text(scan_sec), qty, modo, now),
        )
        c.commit()
        return True, "Cantidad agregada correctamente."


def undo_last_scan(lote_id: int):
    with conn() as c:
        row = c.execute(
            "SELECT * FROM scans WHERE lote_id=? AND cantidad>0 ORDER BY id DESC LIMIT 1",
            (lote_id,),
        ).fetchone()
        if not row:
            return False, "No hay escaneos para deshacer."
        c.execute(
            "UPDATE items SET acopiadas=MAX(acopiadas - ?, 0), updated_at=? WHERE id=?",
            (int(row["cantidad"] or 0), datetime.now().isoformat(timespec="seconds"), row["item_id"]),
        )
        c.execute("DELETE FROM scans WHERE id=?", (row["id"],))
        c.commit()
        return True, "Último escaneo deshecho."


# ============================================================
# Lectura Excel FULL
# ============================================================

ALIASES = {
    "area": ["area"],
    "nro": ["nro", "numero", "num", "n"],
    "codigo_ml": ["codigo_ml", "cod_ml", "codigo_meli", "cod_meli"],
    "codigo_universal": ["codigo_universal", "cod_universal", "ean", "codigo_barras", "codigo_barra", "barcode"],
    "sku": ["sku", "sku_ml"],
    "descripcion": ["descripcion", "description", "title", "titulo", "producto"],
    "unidades": ["unidades", "cantidad", "cant", "qty"],
    "identificacion": ["identificacion", "etiqueta", "etiq", "etiquetar"],
    "instruccion": ["instruccion", "instrucciones"],
    "vence": ["vence", "vcto", "vencimiento"],
    "dia": ["dia"],
    "hora": ["hora"],
}


def detect_header_row(raw: pd.DataFrame):
    best_idx = None
    best_score = -1
    targets = set(sum(ALIASES.values(), []))
    for idx in range(min(15, len(raw))):
        vals = [norm_header(v) for v in raw.iloc[idx].tolist()]
        score = 0
        for v in vals:
            if v in targets or any(v == a for a in targets):
                score += 1
        # criterios mínimos: debe aparecer SKU y cantidad/unidades
        joined = " ".join(vals)
        if "sku" in vals and ("unidades" in vals or "cant" in vals or "cantidad" in vals):
            score += 3
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx if best_score >= 3 else 0


def find_col(columns, logical):
    ncols = {norm_header(c): c for c in columns}
    for alias in ALIASES[logical]:
        a = norm_header(alias)
        if a in ncols:
            return ncols[a]
    return None


def safe_series(df, col):
    if col and col in df.columns:
        return df[col].map(clean_text)
    return pd.Series([""] * len(df), index=df.index)


def read_full_excel(file_obj) -> tuple[pd.DataFrame, list[str]]:
    warnings = []
    xls = pd.ExcelFile(file_obj)
    frames = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(file_obj, sheet_name=sheet, header=None, dtype=str)
        if raw.dropna(how="all").empty:
            continue

        header_row = detect_header_row(raw)
        header = [clean_text(x) or f"col_{i}" for i, x in enumerate(raw.iloc[header_row].tolist())]
        data = raw.iloc[header_row + 1:].copy()
        data.columns = header
        data = data.dropna(how="all")

        cols = {k: find_col(data.columns, k) for k in ALIASES.keys()}

        # Formatos antiguos:
        # Full 2403: AREA, blank, NRO, CODIGO ML, SKU, DESCRIPCIÓN, CANT, ETIQ, INSTRUCCIÓN, VENCE, DIA, HORA
        # Full 3103: AREA, blank, n°, COD ML, COD UNIVERSAL, SKU, DESCRIPCION, CANT, ETIQUETA, VCTO
        # Full 2304: Area., Nº, Código ML, Código Universal, SKU, Descripción, Unidades, Identificación, Vence, Dia, Hora
        out = pd.DataFrame(index=data.index)
        out["area"] = safe_series(data, cols["area"])
        out["nro"] = safe_series(data, cols["nro"])
        out["codigo_ml"] = safe_series(data, cols["codigo_ml"])
        out["codigo_universal"] = safe_series(data, cols["codigo_universal"])
        out["sku"] = safe_series(data, cols["sku"])
        out["descripcion"] = safe_series(data, cols["descripcion"])
        out["unidades"] = safe_series(data, cols["unidades"]).map(to_int)
        out["identificacion"] = safe_series(data, cols["identificacion"])
        out["instruccion"] = safe_series(data, cols["instruccion"])
        out["vence"] = safe_series(data, cols["vence"])
        out["dia"] = safe_series(data, cols["dia"])
        out["hora"] = safe_series(data, cols["hora"])

        # Reparación defensiva:
        # si por archivo raro "Etiquetado obligatorio" cae en Vence, devolverlo a Identificación.
        mask_vence_ident = out["vence"].map(looks_like_identificacion)
        out.loc[mask_vence_ident & (out["identificacion"] == ""), "identificacion"] = out.loc[mask_vence_ident, "vence"]
        out.loc[mask_vence_ident, "vence"] = ""

        # si Identificación viene vacía en formatos antiguos pero ETIQ/INSTRUCCIÓN se confundieron,
        # no copiamos instrucción a identificación salvo que tenga texto de identificación real.
        mask_instr_ident = out["instruccion"].map(looks_like_identificacion)
        out.loc[(out["identificacion"] == "") & mask_instr_ident, "identificacion"] = out.loc[(out["identificacion"] == "") & mask_instr_ident, "instruccion"]

        # No arrastrar 'sheet' a la app: el usuario pidió sacarlo.
        out = out[(out["sku"] != "") & (out["unidades"] > 0)]
        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=["area","nro","codigo_ml","codigo_universal","sku","descripcion","unidades","identificacion","vence","dia","hora"]), warnings

    df = pd.concat(frames, ignore_index=True)

    # Mantener filas por etiqueta/código ML, no agrupar, porque FULL puede separar un mismo SKU en varias líneas.
    final_cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]
    df = df[final_cols].copy()

    # Si no hay Código Universal, queda vacío. No se copia Código ML por ningún motivo.
    df["codigo_universal"] = df["codigo_universal"].fillna("").map(clean_text)

    return df, warnings


# ============================================================
# Maestro SKU/EAN
# ============================================================

def read_master_excel(file_obj) -> pd.DataFrame:
    xls = pd.ExcelFile(file_obj)
    frames = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(file_obj, sheet_name=sheet, header=None, dtype=str)
        if raw.dropna(how="all").empty:
            continue
        header_row = detect_master_header(raw)
        header = [clean_text(x) or f"col_{i}" for i, x in enumerate(raw.iloc[header_row].tolist())]
        data = raw.iloc[header_row + 1:].copy()
        data.columns = header
        data = data.dropna(how="all")

        sku_col = find_any_col(data.columns, ["sku", "sku_ml", "codigo_sku"])
        desc_col = find_any_col(data.columns, ["descripcion", "descripción", "title", "titulo", "producto"])

        ean_candidates = []
        for c in data.columns:
            h = norm_header(c)
            if any(x in h for x in ["ean", "barcode", "codigo_barra", "codigo_barras", "cod_barras", "codigo_universal"]):
                ean_candidates.append(c)

        # fallback: si no detecta EAN, buscar columnas con muchos números largos
        if not ean_candidates:
            for c in data.columns:
                vals = data[c].map(norm_code)
                ratio = vals.str.fullmatch(r"\d{7,14}").fillna(False).mean()
                if ratio > 0.25:
                    ean_candidates.append(c)

        if not sku_col:
            continue

        rows = []
        for _, r in data.iterrows():
            sku = norm_code(r.get(sku_col))
            if not sku:
                continue
            desc = clean_text(r.get(desc_col)) if desc_col else ""
            # SKU también sirve como código secundario
            rows.append({"code": sku, "sku": sku, "descripcion": desc})
            for c in ean_candidates:
                code = norm_code(r.get(c))
                if code and code != sku:
                    rows.append({"code": code, "sku": sku, "descripcion": desc})
        if rows:
            frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame(columns=["code", "sku", "descripcion"])

    df = pd.concat(frames, ignore_index=True)
    df = df[df["code"] != ""].drop_duplicates("code", keep="first")
    return df


def detect_master_header(raw):
    best_idx = 0
    best_score = -1
    for idx in range(min(20, len(raw))):
        vals = [norm_header(v) for v in raw.iloc[idx].tolist()]
        joined = " ".join(vals)
        score = 0
        if "sku" in vals or "sku_ml" in vals:
            score += 3
        if any("ean" in v or "barra" in v or "barcode" in v or "universal" in v for v in vals):
            score += 3
        if "descripcion" in vals or "descripción" in vals:
            score += 1
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx


def find_any_col(columns, aliases):
    ncols = {norm_header(c): c for c in columns}
    for alias in aliases:
        a = norm_header(alias)
        if a in ncols:
            return ncols[a]
    return None


def save_maestro(df: pd.DataFrame):
    now = datetime.now().isoformat(timespec="seconds")
    with conn() as c:
        c.execute("DELETE FROM maestro")
        rows = [(r["code"], r["sku"], r.get("descripcion", ""), now) for _, r in df.iterrows()]
        c.executemany("INSERT OR REPLACE INTO maestro (code, sku, descripcion, updated_at) VALUES (?, ?, ?, ?)", rows)
        c.commit()
    return len(df)


def load_maestro_from_repo_if_needed():
    with conn() as c:
        current = c.execute("SELECT COUNT(*) AS n FROM maestro").fetchone()["n"]
    if current > 0:
        return current, "base"
    if MAESTRO_LOCAL_PATH.exists():
        df = read_master_excel(MAESTRO_LOCAL_PATH)
        n = save_maestro(df)
        return n, "repo"
    return 0, "no_encontrado"


# ============================================================
# Matching de escaneo
# ============================================================

def find_by_ml(lote_id: int, code: str):
    key = norm_code(code)
    if not key:
        return []
    items = get_items(lote_id)
    mask = items["codigo_ml"].map(norm_code) == key
    return items[mask].to_dict("records")


def find_by_secondary(lote_id: int, code: str):
    key = norm_code(code)
    if not key:
        return []

    items = get_items(lote_id)
    direct = items[
        (items["sku"].map(norm_code) == key) |
        (items["codigo_universal"].map(norm_code) == key)
    ]
    if not direct.empty:
        return direct.to_dict("records"), "SKU/COD_UNIVERSAL"

    with conn() as c:
        row = c.execute("SELECT sku FROM maestro WHERE code=?", (key,)).fetchone()
    if row:
        sku = row["sku"]
        matched = items[items["sku"].map(norm_code) == norm_code(sku)]
        if not matched.empty:
            return matched.to_dict("records"), "EAN_MAESTRO"

    return [], "NO_ENCONTRADO"


def prefer_pending(rows):
    if not rows:
        return None
    pending = [r for r in rows if int(r.get("acopiadas") or 0) < int(r.get("unidades") or 0)]
    return pending[0] if pending else rows[0]


def item_status_text(item):
    unidades = int(item["unidades"] or 0)
    acopiadas = int(item["acopiadas"] or 0)
    pendiente = max(unidades - acopiadas, 0)
    return unidades, acopiadas, pendiente


def reset_scan_state():
    for k in ["scan_ml", "scan_sec", "scan_item_id", "scan_mode", "scan_msg"]:
        st.session_state.pop(k, None)


# ============================================================
# Exportación
# ============================================================

def export_lote(lote_id: int) -> bytes:
    items = get_items(lote_id)
    if not items.empty:
        items["pendiente"] = (items["unidades"] - items["acopiadas"]).clip(lower=0)
        items["exceso"] = (items["acopiadas"] - items["unidades"]).clip(lower=0)
        items["estado"] = items.apply(
            lambda r: "COMPLETO" if int(r["pendiente"]) == 0 else "PENDIENTE",
            axis=1,
        )

    with conn() as c:
        scans = pd.read_sql_query(
            """
            SELECT created_at, item_id, scan_ml, scan_secundario, cantidad, modo
            FROM scans WHERE lote_id=? ORDER BY id DESC
            """,
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
maestro_count, maestro_source = load_maestro_from_repo_if_needed()

st.title("📦 Control FULL Aurora")
st.caption("App limpia: carga lote FULL, valida por Código ML + SKU/EAN/Código Universal y controla acopio.")

with st.sidebar:
    st.header("Menú")
    page = st.radio("Vista", ["Cargar lote FULL", "Escaneo", "Control", "Maestro SKU/EAN"], label_visibility="collapsed")

    st.divider()
    lotes = list_lotes()
    if not lotes.empty:
        options = {
            f"{r.nombre} · {int(r.acopiadas)}/{int(r.unidades)}": int(r.id)
            for r in lotes.itertuples(index=False)
        }
        active_label = st.selectbox("Lote activo", list(options.keys()))
        active_lote = options[active_label]
    else:
        active_lote = None
        st.info("Sin lotes creados.")

    st.divider()
    if maestro_count:
        msg = "desde repo" if maestro_source == "repo" else "en base local"
        st.success(f"Maestro cargado: {maestro_count} códigos ({msg}).")
    else:
        st.warning("No hay maestro SKU/EAN cargado.")

if page == "Cargar lote FULL":
    st.subheader("Cargar lote FULL")
    full_file = st.file_uploader("Excel FULL", type=["xlsx"])

    if full_file:
        df, warnings = read_full_excel(full_file)
        for w in warnings:
            st.warning(w)

        if df.empty:
            st.error("No pude leer productos válidos desde el Excel.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Líneas", len(df))
            c2.metric("Unidades", int(df["unidades"].sum()))
            c3.metric("SKUs únicos", df["sku"].nunique())

            st.markdown("#### Vista previa")
            preview_cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]
            st.dataframe(df[preview_cols].head(100), use_container_width=True, hide_index=True)

            nombre = st.text_input("Nombre del lote", value=f"FULL {datetime.now().strftime('%d-%m-%Y %H:%M')}")
            if st.button("Crear lote", type="primary"):
                lote_id = create_lote(nombre, full_file.name, df)
                reset_scan_state()
                st.success(f"Lote creado correctamente: {nombre}")
                st.info("Ahora entra a la vista Escaneo para comenzar.")

elif page == "Escaneo":
    st.subheader("Escaneo FULL")
    if not active_lote:
        st.warning("Primero crea un lote FULL.")
    else:
        items = get_items(active_lote)
        total = int(items["unidades"].sum()) if not items.empty else 0
        done = int(items["acopiadas"].sum()) if not items.empty else 0
        st.progress(0 if total == 0 else min(done / total, 1.0))
        m1, m2, m3 = st.columns(3)
        m1.metric("Solicitadas", total)
        m2.metric("Acopiadas", done)
        m3.metric("Pendientes", max(total - done, 0))

        st.divider()

        code = st.text_input("Escanear Código ML, Código Universal, SKU o EAN", key="scan_input")

        if code:
            key = norm_code(code)
            ml_rows = find_by_ml(active_lote, key)
            sec_rows, sec_type = find_by_secondary(active_lote, key)

            if ml_rows and not sec_rows:
                item = prefer_pending(ml_rows)
                if item and is_supermercado(item.get("identificacion")):
                    st.warning("Este producto está marcado como SUPERMERCADO. No debe validarse por Código ML; escanea SKU/EAN/Código Universal.")
                else:
                    st.session_state["scan_ml"] = key
                    st.session_state["scan_item_id"] = int(item["id"])
                    st.session_state["scan_mode"] = "NORMAL"
                    st.session_state["scan_msg"] = "Código ML validado. Falta escanear SKU/EAN/Código Universal."
                    st.success(st.session_state["scan_msg"])

            elif sec_rows and not ml_rows:
                # Supermercado se valida solo por secundario. Producto normal queda pendiente de ML.
                item = prefer_pending(sec_rows)
                st.session_state["scan_sec"] = key
                if item and is_supermercado(item.get("identificacion")):
                    st.session_state["scan_item_id"] = int(item["id"])
                    st.session_state["scan_mode"] = "SUPERMERCADO"
                    st.success("SUPERMERCADO validado por SKU/EAN/Código Universal. Ya puedes agregar cantidad.")
                else:
                    st.session_state["scan_item_id"] = int(item["id"])
                    st.session_state["scan_mode"] = "NORMAL"
                    st.info("SKU/EAN/Código Universal validado. Falta escanear Código ML.")

            elif ml_rows and sec_rows:
                # Puede ocurrir si el código coincide con SKU y ML. Elegimos el rol según estado actual.
                if st.session_state.get("scan_ml") and not st.session_state.get("scan_sec"):
                    item = get_item(st.session_state["scan_item_id"])
                    st.session_state["scan_sec"] = key
                    st.success("Código secundario validado. Ya puedes agregar cantidad.")
                else:
                    item = prefer_pending(ml_rows)
                    st.session_state["scan_ml"] = key
                    st.session_state["scan_item_id"] = int(item["id"])
                    st.session_state["scan_mode"] = "NORMAL"
                    st.info("Código ML validado. Falta escanear SKU/EAN/Código Universal.")
            else:
                st.error("Código no encontrado en el lote ni en el maestro.")

            # Limpia input para próximo escaneo
            st.session_state["scan_input"] = ""

        # Validación cruzada si ya hay ML y secundario
        current_id = st.session_state.get("scan_item_id")
        current_item = get_item(current_id) if current_id else None

        if current_item:
            item_dict = dict(current_item)
            unidades, acopiadas, pendiente = item_status_text(item_dict)

            st.markdown("### Producto detectado")
            st.write(f"**SKU:** {item_dict['sku']}")
            st.write(f"**Descripción:** {item_dict['descripcion']}")
            st.write(f"**Código ML:** {item_dict['codigo_ml'] or '-'}")
            st.write(f"**Código universal:** {item_dict['codigo_universal'] or '-'}")
            st.write(f"**Identificación:** {item_dict['identificacion'] or '-'}")

            a, b, c = st.columns(3)
            a.metric("Solicitadas", unidades)
            b.metric("Ya acopiadas", acopiadas)
            c.metric("Pendientes", pendiente)

            modo = st.session_state.get("scan_mode", "NORMAL")
            is_super = is_supermercado(item_dict.get("identificacion"))

            can_add = False
            reason = ""

            if is_super:
                can_add = bool(st.session_state.get("scan_sec"))
                reason = "SUPERMERCADO: se valida solo por SKU/EAN/Código Universal."
            else:
                # Para productos normales se exige ML + secundario.
                if st.session_state.get("scan_ml") and st.session_state.get("scan_sec"):
                    # Verificar que el secundario corresponda al mismo SKU o universal del item ML
                    sec = st.session_state.get("scan_sec")
                    sec_matches_item = (
                        norm_code(sec) == norm_code(item_dict["sku"]) or
                        norm_code(sec) == norm_code(item_dict["codigo_universal"])
                    )
                    if not sec_matches_item:
                        with conn() as c:
                            row = c.execute("SELECT sku FROM maestro WHERE code=?", (norm_code(sec),)).fetchone()
                        sec_matches_item = bool(row and norm_code(row["sku"]) == norm_code(item_dict["sku"]))
                    if sec_matches_item:
                        can_add = True
                        reason = "Código ML + SKU/EAN/Código Universal validados."
                    else:
                        reason = "El Código ML y el SKU/EAN/Código Universal no corresponden al mismo SKU."
                else:
                    reason = "Falta validar Código ML + SKU/EAN/Código Universal."

            st.info(reason)

            if (not is_super) and st.session_state.get("scan_ml") and not st.session_state.get("scan_sec"):
                if st.button("Sin EAN / sin código universal"):
                    st.session_state["scan_sec"] = "SIN_EAN"
                    can_add = True
                    st.success("Marcado como Sin EAN. Ya puedes agregar cantidad.")
                    st.rerun()

            if can_add and pendiente > 0:
                qty = st.number_input("Cantidad a agregar", min_value=1, max_value=max(pendiente, 1), value=1, step=1)
                if st.button("Agregar cantidad", type="primary"):
                    ok, msg = add_acopio(
                        active_lote,
                        int(item_dict["id"]),
                        int(qty),
                        st.session_state.get("scan_ml", ""),
                        st.session_state.get("scan_sec", ""),
                        "SUPERMERCADO" if is_super else ("SIN_EAN" if st.session_state.get("scan_sec") == "SIN_EAN" else "NORMAL"),
                    )
                    if ok:
                        reset_scan_state()
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
            elif pending := (pendiente <= 0):
                st.success("Este producto ya está completo.")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Limpiar validación actual"):
                reset_scan_state()
                st.rerun()
        with col2:
            if st.button("Deshacer último escaneo"):
                ok, msg = undo_last_scan(active_lote)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.warning(msg)

elif page == "Control":
    st.subheader("Control FULL")
    if not active_lote:
        st.warning("Primero crea o selecciona un lote.")
    else:
        items = get_items(active_lote)
        if items.empty:
            st.warning("El lote no tiene productos.")
        else:
            items["pendiente"] = (items["unidades"] - items["acopiadas"]).clip(lower=0)
            items["estado"] = items["pendiente"].apply(lambda x: "COMPLETO" if x == 0 else "PENDIENTE")

            total = int(items["unidades"].sum())
            done = int(items["acopiadas"].sum())
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Unidades solicitadas", total)
            c2.metric("Unidades acopiadas", done)
            c3.metric("Pendientes", max(total - done, 0))
            c4.metric("Avance", f"{(done / total * 100) if total else 0:.1f}%")

            filtro = st.radio("Filtro", ["Todos", "Pendientes", "Completos"], horizontal=True)
            view = items.copy()
            if filtro == "Pendientes":
                view = view[view["pendiente"] > 0]
            elif filtro == "Completos":
                view = view[view["pendiente"] == 0]

            show_cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "acopiadas", "pendiente", "identificacion", "estado"]
            st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

            st.download_button(
                "Descargar control Excel",
                data=export_lote(active_lote),
                file_name=f"control_full_{active_lote}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            st.divider()
            if st.button("Eliminar lote activo", type="secondary"):
                delete_lote(active_lote)
                reset_scan_state()
                st.success("Lote eliminado.")
                st.rerun()

elif page == "Maestro SKU/EAN":
    st.subheader("Maestro SKU/EAN")
    st.write("La app intenta cargar automáticamente `data/maestro_sku_ean.xlsx` desde el repo. Este uploader queda solo como respaldo o actualización manual.")

    if MAESTRO_LOCAL_PATH.exists():
        st.success(f"Archivo local detectado: {MAESTRO_LOCAL_PATH}")
        if st.button("Recargar maestro desde repo"):
            dfm = read_master_excel(MAESTRO_LOCAL_PATH)
            n = save_maestro(dfm)
            st.success(f"Maestro recargado: {n} códigos.")
            st.rerun()
    else:
        st.warning("No se encontró `data/maestro_sku_ean.xlsx` en el repo.")

    master_file = st.file_uploader("Subir maestro manualmente", type=["xlsx"])
    if master_file:
        dfm = read_master_excel(master_file)
        st.metric("Códigos detectados", len(dfm))
        st.dataframe(dfm.head(100), use_container_width=True, hide_index=True)
        if st.button("Guardar maestro manual", type="primary"):
            n = save_maestro(dfm)
            st.success(f"Maestro guardado: {n} códigos.")

    with conn() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM maestro").fetchone()["n"]
        updated = c.execute("SELECT MAX(updated_at) AS u FROM maestro").fetchone()["u"]
    st.info(f"Maestro actual en base: {n} códigos. Última actualización: {updated or 'sin datos'}.")
