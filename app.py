import streamlit as st
import polars as pl
import sqlite3
import pandas as pd
import io
import os 
import re
import uuid 
from datetime import datetime

# ==============================================================================
# 1. CONFIGURACIÓN Y CONSTANTES
# ==============================================================================

DATE_FORMAT = '%Y-%m-%d'

MONTH_MAPPING = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4,
    'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
    'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12
}

# Blindaje para la columna 'normalizado'
TRUE_VALUES = {'1', 't', 'true', 'si', 's', 'true'} 
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS)

# Mapeo de columnas estándar
CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 'NUMERO_MEDIDOR': 'nro_med', 'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 'NORMALIZADO': 'normalizado', 'FECHA_ALTA': 'fecha_alta'
}

FINAL_SCHEMA = {
    'nro_cli': pl.Int64,
    'nro_med': pl.Int64,
    'usuario': pl.Utf8,
    'domicilio': pl.Utf8,
    'normalizado': pl.Int64, 
    'fecha_alta': pl.Utf8, 
    'fecha_intervencion': pl.Utf8,
    'estado': pl.Utf8
}

# Mapeo especial para archivos ODS con columna "X"
ODS_X_MAPPING = {
    '+': 'cargado',
    '?': 'revisar',
    '': 'pendiente',
    'x': 'otro distrito',
    '-': 'otro distrito'
}

# ==============================================================================
# 2. FUNCIONES DE LÓGICA DE NEGOCIO
# ==============================================================================

def normalizar_fecha(fecha_str):
    if fecha_str is None or str(fecha_str).strip() in ["", "None", "nan"]:
        return datetime.now().strftime(DATE_FORMAT)
    fecha_str = str(fecha_str) 
    try:
        clean_str = fecha_str.lower().replace('.', '').strip()
        match = re.match(r'(\d{1,2})\s*([a-z]+)\s*(\d{4})', clean_str)
        if match:
            day, month_abbr, year = match.groups()
            month_num = MONTH_MAPPING.get(month_abbr)
            if month_num:
                return datetime(int(year), month_num, int(day)).strftime(DATE_FORMAT)
        return datetime.strptime(clean_str, DATE_FORMAT).strftime(DATE_FORMAT)
    except:
        return datetime.now().strftime(DATE_FORMAT)

def cargar_db(uploaded_file):
    try:
        db_bytes = uploaded_file.read()
        temp_file_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_file_path, "wb") as f:
            f.write(db_bytes)
        conn_disk = sqlite3.connect(temp_file_path)
        conn = sqlite3.connect(':memory:')
        conn_disk.backup(conn)
        conn_disk.close()
        
        df = pl.read_database("SELECT * FROM desvinculados", conn)
        df = df.with_columns(pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT)))
        st.session_state.data = df
        st.session_state.db_cargada = True
        st.success(f"Base de datos cargada: {len(df)} registros.")
        os.remove(temp_file_path)
    except Exception as e:
        st.error(f"Error cargando DB: {e}")

def procesar_archivo_inteligente(uploaded_file):
    """Detecta extensión y procesa según lógica de negocio."""
    try:
        filename = uploaded_file.name
        if filename.endswith('.csv'):
            df = pl.read_csv(uploaded_file, infer_schema_length=10000)
        elif filename.endswith('.ods'):
            # Pandas es más robusto para ODS con múltiples tipos
            pdf = pd.read_excel(uploaded_file, engine='odf')
            df = pl.from_pandas(pdf)
        else:
            st.error("Formato no soportatedo.")
            return

        # 1. Renombrar columnas
        df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})

        # 2. Lógica Especial Columna "X" (Solo si existe y es la primera)
        if df.columns[0] == "X":
            df = df.with_columns(
                pl.col("X").fill_null("").cast(pl.Utf8).alias("X_str")
            ).with_columns(
                pl.col("X_str").replace(ODS_X_MAPPING, default="pendiente").alias("estado")
            ).drop(["X", "X_str"])
        else:
            if "estado" not in df.columns:
                df = df.with_columns(pl.lit("pendiente").alias("estado"))

        # 3. Solución al Error de Conversión 'normalizado' (SI/NO -> 1/0)
        if "normalizado" in df.columns:
            df = df.with_columns(
                pl.col("normalizado").cast(pl.Utf8).str.to_lowercase().alias("norm_tmp")
            ).with_columns(
                pl.when(pl.col("norm_tmp").is_in(TRUE_VALUES))
                .then(1)
                .otherwise(0)
                .alias("normalizado")
            ).drop("norm_tmp")

        # 4. Limpieza de Fechas
        df = df.with_columns(
            pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8)
        )
        
        if "fecha_intervencion" not in df.columns:
            df = df.with_columns(pl.lit(datetime.now().strftime(DATE_FORMAT)).alias("fecha_intervencion"))

        # 5. Ajustar al Esquema Final
        df = df.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA.keys() if c in df.columns])

        # 6. Merge con memoria
        if len(st.session_state.data) > 0:
            combined = pl.concat([st.session_state.data, df], how="vertical")
            st.session_state.data = combined.unique(subset=['nro_cli'], keep='first')
        else:
            st.session_state.data = df

        st.success(f"Importación exitosa: {len(df)} registros procesados.")
        st.rerun()

    except Exception as e:
        st.error(f"Error procesando archivo: {e}")

def guardar_db_bytes(df):
    conn = sqlite3.connect(':memory:')
    df.to_pandas().to_sql('desvinculados', conn, if_exists='replace', index=False)
    temp_file_path = f"/tmp/{uuid.uuid4()}.db"
    conn_disk = sqlite3.connect(temp_file_path)
    conn.backup(conn_disk)
    conn_disk.close()
    with open(temp_file_path, "rb") as f:
        db_bytes = f.read()
    os.remove(temp_file_path)
    return db_bytes

# ==============================================================================
# 3. INTERFAZ DE USUARIO
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE", page_icon="⚡")

if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

# --- MENÚ DE NAVEGACIÓN SUPERIOR ---
menu = st.tabs(["📂 Carga y Configuración", "📝 Gestión (ABM)", "📤 Exportar"])

with menu[0]:
    st.header("1. Importación de Datos")
    c1, c2 = st.columns(2)
    with c1:
        db_file = st.file_uploader("Cargar Base de Datos (.db)", type=['db', 'sqlite'])
        if db_file: cargar_db(db_file)
    with c2:
        # BOTÓN INTELIGENTE UNIFICADO
        import_file = st.file_uploader("Importar hoja de cálculo (CSV o ODS)", type=['csv', 'ods'])
        if import_file: procesar_archivo_inteligente(import_file)

with menu[1]:
    if len(st.session_state.data) > 0:
        st.header(f"2. Gestión de registros ({len(st.session_state.data)} en total)")
        
        # Filtros
        filtro_estado = st.selectbox("Filtrar por estado:", options=FILTRO_OPTIONS, index=1) # Default: Pendiente
        
        df_view = st.session_state.data.clone()
        if filtro_estado != "Todos los registros":
            df_view = df_view.filter(pl.col('estado') == filtro_estado)

        # Preparar para edición
        pdf_view = df_view.to_pandas()
        for col in ['fecha_intervencion', 'fecha_alta']:
            pdf_view[col] = pd.to_datetime(pdf_view[col], errors='coerce').dt.date

        edited_pdf = st.data_editor(
            pdf_view,
            column_config={
                "estado": st.column_config.SelectboxColumn("estado", options=list(ESTADOS), required=True),
                "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format="YYYY-MM-DD")
            },
            disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'),
            num_rows='dynamic',
            hide_index=True,
            key="main_editor"
        )

        if st.button("✅ Guardar cambios en memoria"):
            new_data = pl.from_pandas(edited_pdf)
            # Convertir fechas de vuelta a string
            new_data = new_data.with_columns([
                pl.col('fecha_intervencion').cast(pl.Utf8),
                pl.col('fecha_alta').cast(pl.Utf8)
            ])
            
            # Merge lógico (Update de los visibles + Conservar invisibles)
            ids_editados = new_data.get_column("nro_cli")
            df_unaffected = st.session_state.data.filter(~pl.col("nro_cli").is_in(ids_editados))
            st.session_state.data = pl.concat([df_unaffected, new_data], how="vertical")
            st.success("Cambios guardados.")
            st.rerun()
    else:
        st.info("No hay datos cargados.")

with menu[2]:
    if len(st.session_state.data) > 0:
        st.header("3. Finalizar y exportar")
        
        if st.button("🗑️ Limpiar registros 'cargados'", help="Elimina los registros procesados de la lista actual"):
            st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
            st.rerun()

        st.divider()
        col_d1, col_d2 = st.columns(2)
        
        db_out = guardar_db_bytes(st.session_state.data)
        col_d1.download_button("💾 Descargar .DB (SQLite)", data=db_out, file_name="gestion_epe.db")
        
        csv_out = st.session_state.data.write_csv().encode('utf-8')
        col_d2.download_button("⬇️ Descargar .CSV", data=csv_out, file_name="gestion_epe.csv")