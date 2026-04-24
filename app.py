import io
import re
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
            "area": find_col(raw.columns, ["Area", "Area."]),
            "nro": find_col(raw.columns, ["Nº", "N°", "Nro", "Numero", "Número"]),
            "codigo_ml": find_col(raw.columns, ["Código ML", "Codigo ML", "Cod ML", "COD ML"]),
            "codigo_universal": find_col(raw.columns, ["Código Universal", "Codigo Universal", "Cod Universal", "COD UNIVERSAL", "EAN"]),
            "sku": find_col(raw.columns, ["SKU", "SKU ML"]),
            "descripcion": find_col(raw.columns, ["Descripción", "Descripcion", "Producto", "Title", "Titulo", "Título"]),
            "unidades": find_col(raw.columns, ["Unidades", "Cantidad", "Cant"]),
            "identificacion": find_col(raw.columns, ["Identificación", "Identificacion", "Etiqueta", "Etiquetar"]),
            "vence": find_col(raw.columns, ["Vence", "Vencimiento"]),
            "dia": find_col(raw.columns, ["Dia", "Día"]),
            "hora": find_col(raw.columns, ["Hora"]),
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

st.title("📦 Control FULL Aurora")
st.caption("App limpia para validar FULL. Producto normal: Código ML + SKU/EAN/Código Universal. SUPERMERCADO: solo SKU/EAN/Código Universal.")

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
        st.success(f"Maestro cargado desde {maestro_source}: {maestro_count} códigos.")
    else:
        st.error("No encontré maestro en data/maestro_sku_ean.xlsx")

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

            st.info("Vista previa eliminada. Se cargan las columnas internas sin mostrarlas para evitar confusión visual.")
            nombre = st.text_input("Nombre del lote", value=f"FULL {datetime.now().strftime('%d-%m-%Y %H:%M')}")
            if st.button("Crear lote", type="primary"):
                create_lote(nombre, full_file.name, df)
                reset_scan_state()
                st.success("Lote creado correctamente.")
                st.rerun()

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
            st.info("Normal: escanea Código ML y luego SKU/EAN/Código Universal. SUPERMERCADO: escanea solo SKU/EAN/Código Universal.")

            col1, col2 = st.columns(2)
            with col1:
                st.text_input("1) Código ML", key="scan_ml")
            with col2:
                st.text_input("2) SKU / EAN / Código Universal", key="scan_sec")

            if st.button("Sin EAN / sin código universal"):
                st.session_state["sin_ean"] = True

            scan_ml_v = st.session_state.get("scan_ml", "")
            scan_sec_v = st.session_state.get("scan_sec", "")
            sin_ean = bool(st.session_state.get("sin_ean", False))
            candidate = None
            modo = ""

            if scan_sec_v and not scan_ml_v:
                sm = match_secondary(items, scan_sec_v, only_super=True)
                if not sm.empty:
                    candidate = best_match(sm)
                    modo = "SUPERMERCADO"
                else:
                    st.warning("No encontré producto SUPERMERCADO pendiente con ese SKU/EAN/Código Universal. Si es normal, falta Código ML.")

            elif scan_ml_v and scan_sec_v:
                m1 = match_ml(items, scan_ml_v)
                if m1.empty:
                    st.error("No encontré ese Código ML pendiente en el lote.")
                else:
                    m2 = match_secondary(m1, scan_sec_v, only_super=False)
                    if m2.empty:
                        st.error("El Código ML existe, pero el SKU/EAN/Código Universal no corresponde a ese producto.")
                    else:
                        candidate = best_match(m2)
                        modo = "ML+SECUNDARIO"

            elif scan_ml_v and sin_ean:
                m1 = match_ml(items, scan_ml_v)
                m1 = m1[~m1["identificacion"].map(is_supermercado)]
                if m1.empty:
                    st.error("No encontré ese Código ML pendiente para usar Sin EAN.")
                else:
                    candidate = best_match(m1)
                    modo = "SIN_EAN"

            elif scan_ml_v and not scan_sec_v:
                st.warning("Escanea también SKU/EAN/Código Universal, o usa Sin EAN si corresponde.")

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

            st.divider()
            if st.button("Eliminar lote activo"):
                delete_lote(active_lote)
                st.success("Lote eliminado.")
                st.rerun()

elif page == "Maestro SKU/EAN":
    st.subheader("Maestro SKU/EAN")
    st.write(f"Ruta obligatoria del repo: `{MAESTRO_PATH}`")
    if maestro_count:
        st.success(f"Maestro cargado desde repo: {maestro_count} códigos.")
    else:
        st.error("No se cargó el maestro. Debe existir exactamente como data/maestro_sku_ean.xlsx")
    st.caption("Esta app ya no pide subir maestro. Lo lee desde la repo en cada reinicio.")
