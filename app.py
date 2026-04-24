import io
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "Control FULL Aurora"
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "aurora_full_control.db"
MAESTRO_PATH = DATA_DIR / "maestro_sku_ean.xlsx"

st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")

# ============================================================
# Helpers
# ============================================================

def ensure_dirs():
    DATA_DIR.mkdir(exist_ok=True)


def clean_text(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    if s.lower() in {"nan", "none", "nat"}:
        return ""
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s.strip()


def normalize_header(v) -> str:
    s = clean_text(v).lower()
    for a, b in {"á":"a", "é":"e", "í":"i", "ó":"o", "ú":"u", "ñ":"n", "º":"", "°":""}.items():
        s = s.replace(a, b)
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def norm_code(v) -> str:
    s = clean_text(v).upper()
    for a, b in {"Á":"A", "É":"E", "Í":"I", "Ó":"O", "Ú":"U", "Ñ":"N"}.items():
        s = s.replace(a, b)
    if re.fullmatch(r"\d+(\.\d+)?[eE][+-]?\d+", s):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return re.sub(r"[^A-Z0-9]+", "", s)


def to_int(v, default=0) -> int:
    s = clean_text(v).replace(".", "").replace(",", ".")
    if not s:
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def is_supermercado(v) -> bool:
    return "SUPERMERCADO" in norm_code(v)


def find_col(cols, aliases):
    norm_cols = {normalize_header(c): c for c in cols}
    alias_norm = [normalize_header(a) for a in aliases]
    for a in alias_norm:
        if a in norm_cols:
            return norm_cols[a]
    for a in alias_norm:
        for nc, original in norm_cols.items():
            if a and (a in nc or nc in a):
                return original
    return None

# ============================================================
# SQLite limpio y aislado del WMS
# ============================================================

def db():
    ensure_dirs()
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS full_lotes (
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
        CREATE TABLE IF NOT EXISTS full_items (
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
        CREATE TABLE IF NOT EXISTS full_scans (
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
        CREATE TABLE IF NOT EXISTS full_maestro (
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
    with db():
        pass


def list_lotes():
    with db() as c:
        return pd.read_sql_query(
            """
            SELECT l.id, l.nombre, l.archivo, l.created_at, l.estado,
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


def get_items(lote_id: int) -> pd.DataFrame:
    with db() as c:
        return pd.read_sql_query("SELECT * FROM full_items WHERE lote_id=? ORDER BY id", c, params=(lote_id,))


def get_item(item_id: int):
    with db() as c:
        return c.execute("SELECT * FROM full_items WHERE id=?", (item_id,)).fetchone()


def create_lote(nombre: str, archivo: str, df: pd.DataFrame) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        cur = c.execute(
            "INSERT INTO full_lotes (nombre, archivo, created_at, estado) VALUES (?, ?, ?, 'abierto')",
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
            INSERT INTO full_items
            (lote_id, area, nro, codigo_ml, codigo_universal, sku, descripcion, unidades,
             identificacion, vence, dia, hora, acopiadas, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()
        return int(lote_id)


def delete_lote(lote_id: int):
    with db() as c:
        c.execute("DELETE FROM full_scans WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM full_items WHERE lote_id=?", (lote_id,))
        c.execute("DELETE FROM full_lotes WHERE id=?", (lote_id,))
        c.commit()


def add_acopio(lote_id: int, item_id: int, qty: int, scan_ml: str, scan_sec: str, modo: str):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        item = c.execute("SELECT unidades, acopiadas FROM full_items WHERE id=? AND lote_id=?", (item_id, lote_id)).fetchone()
        if not item:
            return False, "Producto no encontrado en el lote activo."
        unidades = int(item["unidades"] or 0)
        acopiadas = int(item["acopiadas"] or 0)
        pendiente = max(unidades - acopiadas, 0)
        if qty <= 0:
            return False, "La cantidad debe ser mayor a cero."
        if qty > pendiente:
            return False, f"No puedes agregar {qty}. Pendiente actual: {pendiente}."
        c.execute("UPDATE full_items SET acopiadas=acopiadas+?, updated_at=? WHERE id=?", (qty, now, item_id))
        c.execute(
            """
            INSERT INTO full_scans (lote_id, item_id, scan_ml, scan_secundario, cantidad, modo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (lote_id, item_id, clean_text(scan_ml), clean_text(scan_sec), qty, modo, now),
        )
        c.commit()
        return True, "Cantidad agregada correctamente."


def undo_last_scan(lote_id: int):
    with db() as c:
        row = c.execute(
            "SELECT * FROM full_scans WHERE lote_id=? AND cantidad>0 ORDER BY id DESC LIMIT 1",
            (lote_id,),
        ).fetchone()
        if not row:
            return False, "No hay escaneos para deshacer."
        c.execute("UPDATE full_items SET acopiadas=MAX(acopiadas-?, 0), updated_at=? WHERE id=?", (row["cantidad"], datetime.now().isoformat(timespec="seconds"), row["item_id"]))
        c.execute("DELETE FROM full_scans WHERE id=?", (row["id"],))
        c.commit()
        return True, "Último escaneo deshecho."

# ============================================================
# Lectura Excel FULL
# ============================================================

def read_full_excel(file) -> tuple[pd.DataFrame, list[str]]:
    warnings = []
    xls = pd.ExcelFile(file)
    frames = []

    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, dtype=str)
        if raw.empty:
            continue
        raw.columns = [clean_text(c) for c in raw.columns]

        col_map = {
            "area": find_col(raw.columns, ["Area", "Area."]),
            "nro": find_col(raw.columns, ["Nº", "N°", "Nro", "Numero", "Número"]),
            "codigo_ml": find_col(raw.columns, ["Código ML", "Codigo ML", "Cod ML", "COD ML"]),
            "codigo_universal": find_col(raw.columns, ["Código Universal", "Codigo Universal", "Cod Universal", "COD UNIVERSAL", "EAN"]),
            "sku": find_col(raw.columns, ["SKU", "SKU ML", "sku_ml"]),
            "descripcion": find_col(raw.columns, ["Descripción", "Descripcion", "Producto", "Title", "Titulo", "Título"]),
            "unidades": find_col(raw.columns, ["Unidades", "Cantidad", "Cant", "qty_required"]),
            "identificacion": find_col(raw.columns, ["Identificación", "Identificacion", "Etiqueta", "Etiquetar"]),
            "vence": find_col(raw.columns, ["Vence", "Vencimiento"]),
            "dia": find_col(raw.columns, ["Dia", "Día"]),
            "hora": find_col(raw.columns, ["Hora"]),
        }

        if not col_map["sku"] and not col_map["codigo_ml"]:
            continue

        clean = pd.DataFrame()
        for key in ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]:
            col = col_map.get(key)
            clean[key] = raw[col].map(clean_text) if col else ""

        # Protección: si por un Excel raro cae "Etiquetado obligatorio" o "SUPERMERCADO" en vence,
        # se devuelve a identificación. No toca valores "SI" porque pueden ser vencimiento real.
        mask_ident_in_vence = clean["vence"].map(lambda x: any(t in norm_code(x) for t in ["ETIQUET", "SUPERMERCADO", "SINETIQUETA", "SINEAN"]))
        clean.loc[mask_ident_in_vence & (clean["identificacion"] == ""), "identificacion"] = clean.loc[mask_ident_in_vence, "vence"]
        clean.loc[mask_ident_in_vence, "vence"] = ""

        clean["unidades"] = clean["unidades"].map(to_int)
        clean = clean[(clean["sku"].map(norm_code) != "") | (clean["codigo_ml"].map(norm_code) != "") | (clean["codigo_universal"].map(norm_code) != "")]
        clean = clean[clean["unidades"] > 0]
        if not clean.empty:
            frames.append(clean)

    if not frames:
        return pd.DataFrame(columns=["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]), ["No se encontraron filas válidas en el Excel FULL."]

    df = pd.concat(frames, ignore_index=True)
    return df, warnings

# ============================================================
# Maestro SKU/EAN
# ============================================================

def parse_maestro(file_or_path) -> pd.DataFrame:
    df = pd.read_excel(file_or_path, dtype=str)
    df.columns = [clean_text(c) for c in df.columns]
    sku_col = find_col(df.columns, ["SKU", "SKU ML", "sku_ml"])
    desc_col = find_col(df.columns, ["Descripción", "Descripcion", "Producto", "Title", "Titulo"])
    barcode_cols = []
    for c in df.columns:
        nc = normalize_header(c)
        if any(x in nc for x in ["ean", "barra", "barcode", "codigo_universal", "cod_universal", "codigo_de_barras"]):
            barcode_cols.append(c)
    if sku_col and sku_col not in barcode_cols:
        barcode_cols.append(sku_col)
    rows = []
    for _, r in df.iterrows():
        sku = norm_code(r.get(sku_col, "")) if sku_col else ""
        if not sku:
            continue
        desc = clean_text(r.get(desc_col, "")) if desc_col else ""
        codes = {sku}
        for c in barcode_cols:
            val = clean_text(r.get(c, ""))
            for part in re.split(r"[\s,;/|]+", val):
                code = norm_code(part)
                if code:
                    codes.add(code)
        for code in codes:
            rows.append({"code": code, "sku": sku, "descripcion": desc})
    return pd.DataFrame(rows).drop_duplicates(subset=["code"]) if rows else pd.DataFrame(columns=["code", "sku", "descripcion"])


def import_maestro(df: pd.DataFrame):
    now = datetime.now().isoformat(timespec="seconds")
    with db() as c:
        c.execute("DELETE FROM full_maestro")
        c.executemany(
            "INSERT OR REPLACE INTO full_maestro (code, sku, descripcion, updated_at) VALUES (?, ?, ?, ?)",
            [(r.code, r.sku, r.descripcion, now) for r in df.itertuples(index=False)],
        )
        c.commit()


def load_maestro_local_if_needed():
    with db() as c:
        count = c.execute("SELECT COUNT(*) FROM full_maestro").fetchone()[0]
    if count > 0:
        return count, "base local"
    if MAESTRO_PATH.exists():
        try:
            df = parse_maestro(MAESTRO_PATH)
            import_maestro(df)
            return len(df), "repo"
        except Exception as e:
            st.sidebar.warning(f"No pude cargar maestro local: {e}")
    return 0, ""


def maestro_lookup(code: str) -> str:
    code_n = norm_code(code)
    if not code_n:
        return ""
    with db() as c:
        row = c.execute("SELECT sku FROM full_maestro WHERE code=?", (code_n,)).fetchone()
    return clean_text(row["sku"]) if row else ""

# ============================================================
# Matching de escaneo
# ============================================================

def pending_items(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["pendiente"] = (out["unidades"].astype(int) - out["acopiadas"].astype(int)).clip(lower=0)
    return out[out["pendiente"] > 0]


def match_by_ml(items: pd.DataFrame, scan_ml: str) -> pd.DataFrame:
    code = norm_code(scan_ml)
    if not code:
        return items.iloc[0:0]
    p = pending_items(items)
    return p[p["codigo_ml"].map(norm_code) == code]


def match_by_secondary(items: pd.DataFrame, scan_sec: str, only_super: bool | None = None) -> pd.DataFrame:
    code = norm_code(scan_sec)
    if not code:
        return items.iloc[0:0]
    sku_from_maestro = norm_code(maestro_lookup(code))
    p = pending_items(items)
    if only_super is True:
        p = p[p["identificacion"].map(is_supermercado)]
    elif only_super is False:
        p = p[~p["identificacion"].map(is_supermercado)]

    mask = (
        (p["codigo_universal"].map(norm_code) == code)
        | (p["sku"].map(norm_code) == code)
    )
    if sku_from_maestro:
        mask = mask | (p["sku"].map(norm_code) == sku_from_maestro)
    return p[mask]


def reset_scan_state():
    for k in ["scan_ml", "scan_sec", "sin_ean"]:
        st.session_state[k] = "" if k != "sin_ean" else False


def choose_best_match(matches: pd.DataFrame):
    if matches.empty:
        return None
    matches = matches.copy()
    matches["pendiente"] = (matches["unidades"].astype(int) - matches["acopiadas"].astype(int)).clip(lower=0)
    matches = matches.sort_values(["pendiente", "id"], ascending=[False, True])
    return matches.iloc[0]

# ============================================================
# Exportación
# ============================================================

def export_lote(lote_id: int) -> bytes:
    items = get_items(lote_id)
    if not items.empty:
        items["pendiente"] = (items["unidades"].astype(int) - items["acopiadas"].astype(int)).clip(lower=0)
        items["estado"] = items["pendiente"].apply(lambda x: "COMPLETO" if int(x) == 0 else "PENDIENTE")
    with db() as c:
        scans = pd.read_sql_query("SELECT created_at, item_id, scan_ml, scan_secundario, cantidad, modo FROM full_scans WHERE lote_id=? ORDER BY id DESC", c, params=(lote_id,))
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        items.to_excel(writer, sheet_name="control_full", index=False)
        scans.to_excel(writer, sheet_name="escaneos", index=False)
    return out.getvalue()

# ============================================================
# UI
# ============================================================

init_db()
maestro_count, maestro_source = load_maestro_local_if_needed()

st.title("📦 Control FULL Aurora")
st.caption("App limpia para validar FULL por Código ML + SKU/EAN/Código Universal. SUPERMERCADO se valida solo por SKU/EAN/Código Universal.")

with st.sidebar:
    st.header("Menú")
    page = st.radio("Vista", ["Cargar lote FULL", "Escaneo", "Control", "Maestro SKU/EAN"], label_visibility="collapsed")
    st.divider()
    lotes = list_lotes()
    if lotes.empty:
        active_lote = None
        st.info("Sin lotes creados.")
    else:
        options = {f"{r.nombre} · {int(r.acopiadas)}/{int(r.unidades)}": int(r.id) for r in lotes.itertuples(index=False)}
        active_lote = options[st.selectbox("Lote activo", list(options.keys()))]
    st.divider()
    if maestro_count:
        st.success(f"Maestro SKU/EAN: {maestro_count} códigos ({maestro_source}).")
    else:
        st.warning("Sin maestro SKU/EAN. Déjalo en data/maestro_sku_ean.xlsx o súbelo en la vista Maestro.")

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
            c1.metric("Líneas", len(df))
            c2.metric("Unidades", int(df["unidades"].sum()))
            c3.metric("SKUs únicos", df["sku"].nunique())
            st.markdown("#### Vista previa")
            preview_cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "identificacion", "vence", "dia", "hora"]
            st.dataframe(df[preview_cols].head(100), use_container_width=True, hide_index=True)
            nombre = st.text_input("Nombre del lote", value=f"FULL {datetime.now().strftime('%d-%m-%Y %H:%M')}")
            if st.button("Crear lote", type="primary"):
                create_lote(nombre, full_file.name, df)
                reset_scan_state()
                st.success("Lote creado correctamente.")

elif page == "Escaneo":
    st.subheader("Escaneo FULL")
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
            st.markdown("### Validación")
            st.info("Producto normal: escanea Código ML y luego SKU/EAN/Código Universal. SUPERMERCADO: solo SKU/EAN/Código Universal.")

            col1, col2 = st.columns(2)
            with col1:
                scan_ml = st.text_input("1) Código ML", key="scan_ml")
            with col2:
                scan_sec = st.text_input("2) SKU / EAN / Código Universal", key="scan_sec")

            if st.button("Sin EAN / sin código universal", help="Usar solo cuando el producto normal tiene etiqueta ML pero no tiene EAN/código universal."):
                st.session_state["sin_ean"] = True

            scan_ml_v = st.session_state.get("scan_ml", "")
            scan_sec_v = st.session_state.get("scan_sec", "")
            sin_ean = bool(st.session_state.get("sin_ean", False))
            candidate = None
            modo = ""

            if scan_sec_v and not scan_ml_v:
                sm = match_by_secondary(items, scan_sec_v, only_super=True)
                if not sm.empty:
                    candidate = choose_best_match(sm)
                    modo = "SUPERMERCADO"
                else:
                    st.warning("No encontré producto SUPERMERCADO pendiente con ese SKU/EAN/Código Universal. Si es producto normal, falta escanear Código ML.")

            elif scan_ml_v and scan_sec_v:
                m1 = match_by_ml(items, scan_ml_v)
                m2 = match_by_secondary(m1, scan_sec_v, only_super=False)
                if not m1.empty and m2.empty:
                    st.error("El Código ML existe, pero el SKU/EAN/Código Universal no corresponde a ese producto.")
                elif m1.empty:
                    st.error("No encontré ese Código ML pendiente en el lote.")
                else:
                    candidate = choose_best_match(m2)
                    modo = "ML+SECUNDARIO"

            elif scan_ml_v and sin_ean:
                m1 = match_by_ml(items, scan_ml_v)
                m1 = m1[~m1["identificacion"].map(is_supermercado)]
                if m1.empty:
                    st.error("No encontré ese Código ML pendiente para usar Sin EAN.")
                else:
                    candidate = choose_best_match(m1)
                    modo = "SIN_EAN"

            elif scan_ml_v and not scan_sec_v:
                st.warning("Escanea también SKU/EAN/Código Universal, o usa el botón Sin EAN si corresponde.")

            if candidate is not None:
                pendiente = int(candidate["unidades"]) - int(candidate["acopiadas"])
                st.success("Producto validado.")
                st.markdown(f"**{candidate['descripcion']}**")
                x1, x2, x3, x4 = st.columns(4)
                x1.metric("SKU", candidate["sku"])
                x2.metric("Solicitadas", int(candidate["unidades"]))
                x3.metric("Ya acopiadas", int(candidate["acopiadas"]))
                x4.metric("Pendientes", max(pendiente, 0))
                qty = st.number_input("Cantidad a agregar", min_value=1, max_value=max(pendiente, 1), value=1, step=1)
                if st.button("Agregar cantidad", type="primary"):
                    ok, msg = add_acopio(active_lote, int(candidate["id"]), int(qty), scan_ml_v, scan_sec_v, modo)
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
            cols = ["area", "nro", "codigo_ml", "codigo_universal", "sku", "descripcion", "unidades", "acopiadas", "pendiente", "identificacion", "vence", "estado"]
            st.dataframe(show[cols], use_container_width=True, hide_index=True)
            st.download_button("Exportar control Excel", data=export_lote(active_lote), file_name="control_full_aurora.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if st.button("Eliminar lote activo", type="secondary"):
                delete_lote(active_lote)
                st.success("Lote eliminado.")
                st.rerun()

elif page == "Maestro SKU/EAN":
    st.subheader("Maestro SKU/EAN")
    st.write(f"Ruta automática esperada: `{MAESTRO_PATH}`")
    if maestro_count:
        st.success(f"Maestro cargado: {maestro_count} códigos.")
    up = st.file_uploader("Actualizar maestro manualmente", type=["xlsx"])
    if up:
        dfm = parse_maestro(up)
        if dfm.empty:
            st.error("No pude leer códigos válidos desde el maestro.")
        else:
            import_maestro(dfm)
            st.success(f"Maestro actualizado: {len(dfm)} códigos.")
            st.dataframe(dfm.head(50), use_container_width=True, hide_index=True)
