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
    'may': 5, 'jun : 6, 'jul': 7, 'ago': 8,
    'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12
}

# Mapeo específico para archivos ODS (Columna X)
ODS_ESTADO_MAP = {
    '+': 'cargado',
    '?': 'revisar',
    'x': 'otro distrito',
    '-': 'otro distrito',
    None: 'pendiente',
    '': 'pendiente'
}

TRUE_VALUES = {'1', 't', 'true', 'si', 's'} 
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
# 2. LÓGICA DE PROCESAMIENTO
# ==============================================================================

def normalizar_fecha(fecha_str):
    if fecha_str is None or pl.Series([fecha_str]).is_null().item():
        return datetime.now().strftime(DATE_FORMAT)
    fecha_str = str(fecha_str) 
    try:
        clean_str = fecha_str.lower().replace('.', '').strip()
        match = re.match(r'(\d{1,2})\s*([a-z]+)\s*(\d{4})', clean_str)
        if match:
            day, month_abbr, year = match.groups()
            month_num = MONTH_MAPPING.get(month_abbr, None)
            if month_num:
                return datetime(int(year), month_num, int(day)).strftime(DATE_FORMAT)
        return clean_str 
    except:
        return None 

def cargar_db(uploaded_file):
    try:
        db_bytes = uploaded_file.read()
        temp_file_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_file_path, "wb") as f:
            f.write(db_bytes)
        
        conn_disk = sqlite3.connect(temp_file_path)
        conn = sqlite3.connect(':memory:')
        conn_disk.backup(conn)
        
        df = pl.read_database("SELECT * FROM desvinculados", conn)
        df = df.with_columns(pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT)))
        
        st.session_state.data = df
        st.session_state.db_cargada = True
        st.success(f"Base de datos cargada: {len(df)} registros.")
        conn_disk.close()
        os.remove(temp_file_path)
    except Exception as e:
        st.error(f"Error al cargar DB: {e}")

def procesar_archivo_inteligente(uploaded_file):
    """Detecta si es CSV (Reporte) u ODS (Estados) y procesa en consecuencia."""
    try:
        file_name = uploaded_file.name.lower()
        
        if file_name.endswith('.csv'):
            df = pl.read_csv(uploaded_file)
            # Manejo robusto de 'normalizado' para evitar el error str -> i64
            if 'NORMALIZADO' in df.columns:
                df = df.with_columns(
                    pl.col('NORMALIZADO').cast(pl.Utf8).str.to_lowercase()
                    .is_in(TRUE_VALUES).cast(pl.Int64).alias('NORMALIZADO')
                )
            df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})
            
            # Completar campos faltantes para reporte nuevo
            hoy = datetime.now().strftime(DATE_FORMAT)
            df = df.with_columns([
                pl.lit('pendiente').alias('estado'),
                pl.lit(hoy).alias('fecha_intervencion'),
                pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8)
            ])
            
        elif file_name.endswith('.ods'):
            # Usar pandas con BytesIO para evitar error de tipos de buffer
            pd_df = pd.read_excel(io.BytesIO(uploaded_file.read()), engine='odf')
            df = pl.from_pandas(pd_df)
            
            # Lógica Columna X
            col_1_name = df.columns[0]
            if col_1_name.upper() == 'X':
                df = df.with_columns(
                    pl.col(col_1_name).map_elements(lambda x: ODS_ESTADO_MAP.get(str(x).strip().lower() if x else '', 'pendiente'), return_dtype=pl.Utf8).alias('estado')
                ).drop(col_1_name)
                
                # Mapear columnas restantes si es necesario (asumimos que nro_cli existe)
                df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})
            else:
                st.warning("El archivo ODS no tiene el encabezado 'X' en la primera columna.")
                return

        # Asegurar Esquema Final
        df = df.select([pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items() if col in df.columns])
        
        # Fusión con memoria
        if len(st.session_state.data) > 0:
            existing = st.session_state.data
            st.session_state.data = pl.concat([existing, df], how="vertical").unique(subset=['nro_cli'], keep='last')
        else:
            st.session_state.data = df
            
        st.success(f"Importación exitosa. Total en memoria: {len(st.session_state.data)}")
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

st.set_page_config(layout="wide", page_title="Gestor EPE")

# Inicialización de estado
if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

# MENU SUPERIOR (Tabs)
tab_gestion, tab_config = st.tabs(["📊 Gestión de Datos", "⚙️ Configuración de Sistema"])

with tab_gestion:
    # FILA DE ACCIONES (Importación y carga de DB)
    col_db, col_import = st.columns(2)
    
    with col_db:
        db_file = st.file_uploader("📂 Cargar Base de Datos (.db)", type=['db', 'sqlite'])
        if db_file: cargar_db(db_file)
        
    with col_import:
        # Botón inteligente pedido: Importar hoja de cálculo
        input_file = st.file_uploader("📥 Importar hoja de cálculo (CSV o ODS)", type=['csv', 'ods'])
        if input_file: procesar_archivo_inteligente(input_file)

    st.divider()

    # VISOR Y ABM
    if len(st.session_state.data) > 0:
        st.subheader(f"Registros en memoria: {len(st.session_state.data)}")
        
        # Filtro
        filtro_estado = st.selectbox("Filtrar por estado:", options=FILTRO_OPTIONS, index=1)
        
        df_view = st.session_state.data.clone()
        if filtro_estado != "Todos los registros":
            df_view = df_view.filter(pl.col('estado') == filtro_estado)

        # Editor
        df_pandas = df_view.to_pandas()
        for col in ['fecha_intervencion', 'fecha_alta']:
            df_pandas[col] = pd.to_datetime(df_pandas[col], errors='coerce').dt.date

        edited_pd = st.data_editor(
            df_pandas,
            column_config={
                "estado": st.column_config.SelectboxColumn("estado", options=list(ESTADOS), required=True),
                "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format="YYYY-MM-DD")
            },
            disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'),
            num_rows='dynamic',
            hide_index=True
        )

        # Guardar cambios
        if st.button("💾 Guardar cambios en memoria"):
            new_pl = pl.from_pandas(edited_pd)
            # Convertir fechas de vuelta a string
            new_pl = new_pl.with_columns([
                pl.col('fecha_intervencion').cast(pl.Utf8),
                pl.col('fecha_alta').cast(pl.Utf8)
            ]).filter(pl.col('nro_cli').is_not_null())
            
            # Merge
            unaffected = st.session_state.data.filter(~pl.col('nro_cli').is_in(new_pl['nro_cli']))
            st.session_state.data = pl.concat([unaffected, new_pl], how="vertical")
            st.success("Memoria actualizada.")
            st.rerun()

        # EXPORTACIÓN
        st.divider()
        st.subheader("3. Exportar resultados")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button("💾 Descargar .DB", data=guardar_db_bytes(st.session_state.data), file_name="gestion_epe.db")
        with c2:
            st.download_button("⬇️ Descargar .CSV", data=st.session_state.data.write_csv().encode('utf-8'), file_name="reporte.csv")
        with c3:
            if st.button("🗑️ Limpiar 'cargados'"):
                st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
                st.rerun()
    else:
        st.info("No hay datos cargados. Sube una base de datos o importa un archivo para comenzar.")

with tab_config:
    st.write("Configuración de variables de entorno (SMTP / API Keys)")