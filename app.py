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

ODS_ESTADO_MAP = {
    '+': 'cargado',
    '?': 'revisar',
    'x': 'otro distrito',
    '-': 'otro distrito',
    '': 'pendiente',
    'nan': 'pendiente',
    'none': 'pendiente'
}

TRUE_VALUES = {'1', 't', 'true', 'si', 's', 'true'} 
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS)

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

# ==============================================================================
# 2. FUNCIONES DE LÓGICA (ITERADAS PARA ROBUSTEZ)
# ==============================================================================

def normalizar_fecha(fecha_str):
    if fecha_str is None or str(fecha_str).lower() in ['none', 'nan', '']:
        return datetime.now().strftime(DATE_FORMAT)
    
    fs = str(fecha_str).lower().replace('.', '').strip()
    try:
        # Intento de parseo de formato específico: "12 oct 2023"
        match = re.match(r'(\d{1,2})\s*([a-z]+)\s*(\d{4})', fs)
        if match:
            d, m_abbr, y = match.groups()
            m_num = MONTH_MAPPING.get(m_abbr)
            if m_num:
                return f"{y}-{m_num:02d}-{int(d):02d}"
        return fs 
    except:
        return datetime.now().strftime(DATE_FORMAT)

def procesar_archivo_inteligente(uploaded_file):
    """
    SOLUCIÓN DEFINITIVA AL ERROR: Expected bytes, got an 'int' object.
    Se extrae el contenido binario ANTES de cualquier operación de Pandas/Polars.
    """
    try:
        # EXTRACCIÓN CRÍTICA: Convertimos el archivo de Streamlit en un objeto Bytes puro.
        # Esto mata la referencia al 'fileno' (el entero que causa el error).
        raw_content = uploaded_file.getvalue()
        nombre = uploaded_file.name.lower()
        
        if nombre.endswith('.csv'):
            # Usar BytesIO para que Polars no intente leer del disco
            df_raw = pl.read_csv(io.BytesIO(raw_content), infer_schema_length=10000)
            
            if 'NORMALIZADO' in df_raw.columns:
                df_raw = df_raw.with_columns(
                    pl.col('NORMALIZADO').cast(pl.Utf8).str.to_lowercase()
                    .is_in(TRUE_VALUES).cast(pl.Int64).alias('NORMALIZADO')
                )
            df = df_raw.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_raw.columns})
            
            hoy = datetime.now().strftime(DATE_FORMAT)
            if 'estado' not in df.columns: df = df.with_columns(pl.lit('pendiente').alias('estado'))
            if 'fecha_intervencion' not in df.columns: df = df.with_columns(pl.lit(hoy).alias('fecha_intervencion'))
            
        elif nombre.endswith('.ods'):
            # PASO ITERADO: Forzamos la lectura de bytes pura con el motor 'odf' 
            # asegurándonos de que no haya punteros residuales.
            with io.BytesIO(raw_content) as bio:
                pd_df = pd.read_excel(bio, engine='odf')
                df = pl.from_pandas(pd_df)
            
            # Lógica Columna X
            primera_col = df.columns[0]
            if primera_col.upper() == 'X':
                df = df.with_columns(
                    pl.col(primera_col).cast(pl.Utf8).fill_null('').str.strip_chars().str.to_lowercase()
                    .map_elements(lambda x: ODS_ESTADO_MAP.get(x, 'pendiente'), return_dtype=pl.Utf8)
                    .alias('estado')
                ).drop(primera_col)
                df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})
            else:
                st.error("Encabezado de la columna 1 debe ser 'X'.")
                return

        # Normalización de esquema
        if 'fecha_alta' in df.columns:
            df = df.with_columns(pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8))

        for col, dtype in FINAL_SCHEMA.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

        df = df.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA.keys()])

        # Fusión por nro_cli
        if len(st.session_state.data) > 0:
            st.session_state.data = pl.concat([st.session_state.data, df], how="vertical").unique(subset=['nro_cli'], keep='last')
        else:
            st.session_state.data = df
            
        st.success(f"Procesado: {len(df)} filas.")
        st.rerun()

    except Exception as e:
        st.error(f"Error procesando archivo: {e}")

def cargar_db(uploaded_file):
    try:
        raw_db = uploaded_file.getvalue()
        temp_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_path, "wb") as f: f.write(raw_db)
        
        conn = sqlite3.connect(temp_path)
        df = pl.read_database("SELECT * FROM desvinculados", conn)
        conn.close()
        os.remove(temp_path)
        
        st.session_state.data = df
        st.success("Base de datos cargada.")
    except Exception as e:
        st.error(f"Fallo al cargar DB: {e}")

def exportar_db(df):
    conn = sqlite3.connect(':memory:')
    df.to_pandas().to_sql('desvinculados', conn, if_exists='replace', index=False)
    temp_path = f"/tmp/{uuid.uuid4()}.db"
    disk_conn = sqlite3.connect(temp_path)
    conn.backup(disk_conn)
    disk_conn.close()
    with open(temp_path, "rb") as f: data = f.read()
    os.remove(temp_path)
    return data

# ==============================================================================
# 3. INTERFAZ STREAMLIT
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")

if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

st.title("⚡ EPE - Gestión de Desvinculados")

tab_principal, tab_sys = st.tabs(["📊 Gestión", "⚙️ Sistema"])

with tab_principal:
    c1, c2 = st.columns(2)
    with c1:
        f_db = st.file_uploader("📂 Cargar DB (.db)", type=['db', 'sqlite'], key="db_loader")
        if f_db: cargar_db(f_db)
    with c2:
        f_in = st.file_uploader("📥 Importar hoja de cálculo (CSV o ODS)", type=['csv', 'ods'], key="sheet_loader")
        if f_in: procesar_archivo_inteligente(f_in)

    st.divider()

    if len(st.session_state.data) > 0:
        col_f, col_s = st.columns([2, 1])
        with col_f:
            filtro = st.selectbox("Filtrar vista:", FILTRO_OPTIONS, index=1)
        with col_s:
            st.metric("Total registros", len(st.session_state.data))

        df_view = st.session_state.data.clone()
        if filtro != "Todos los registros":
            df_view = df_view.filter(pl.col('estado') == filtro)

        pdf = df_view.to_pandas()
        for c in ['fecha_intervencion', 'fecha_alta']:
            pdf[c] = pd.to_datetime(pdf[c], errors='coerce').dt.date

        new_pdf = st.data_editor(
            pdf,
            column_config={
                "estado": st.column_config.SelectboxColumn("estado", options=ESTADOS, required=True),
                "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format="YYYY-MM-DD")
            },
            disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'),
            num_rows='dynamic',
            hide_index=True,
            key="main_editor"
        )

        if st.button("💾 Aplicar cambios a la memoria"):
            res_pl = pl.from_pandas(new_pdf)
            res_pl = res_pl.with_columns([pl.col(c).cast(pl.Utf8) for c in ['fecha_intervencion', 'fecha_alta']])
            ids_editados = res_pl['nro_cli'].to_list()
            final_df = pl.concat([
                st.session_state.data.filter(~pl.col('nro_cli').is_in(ids_editados)),
                res_pl
            ], how="vertical").filter(pl.col('nro_cli').is_not_null())
            st.session_state.data = final_df
            st.success("Cambios guardados.")
            st.rerun()

        st.divider()
        st.subheader("3. Exportar resultados")
        bt1, bt2, bt3 = st.columns(3)
        with bt1:
            st.download_button("💾 Descargar DB", data=exportar_db(st.session_state.data), file_name="epe_data.db")
        with bt2:
            st.download_button("📄 Descargar CSV", data=st.session_state.data.write_csv().encode('utf-8'), file_name="reporte_epe.csv")
        with bt3:
            if st.button("🗑️ Limpiar procesados ('cargado')", type="primary"):
                st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
                st.rerun()
    else:
        st.warning("Sin datos. Sube un archivo para comenzar.")

with tab_sys:
    st.info("Sistema listo para operar.")