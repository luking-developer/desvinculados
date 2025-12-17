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

TRUE_VALUES = {'1', 't', 'true', 'si', 's'} 
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS)

# Mapeo específico para ODS solicitado
ODS_ESTADO_MAP = {
    '+': 'cargado',
    '?': 'revisar',
    '': 'pendiente',
    'x': 'otro distrito'
}

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
# 2. FUNCIONES DE LÓGICA
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
            if month_num is not None:
                return datetime(int(year), month_num, int(day)).strftime(DATE_FORMAT)
        datetime.strptime(clean_str, DATE_FORMAT)
        return clean_str 
    except: return None 

def fusionar_datos(df_nuevo):
    """Lógica central para evitar duplicados al importar."""
    if st.session_state.data is not None and len(st.session_state.data) > 0:
        existing_df = st.session_state.data.select([pl.col(c).cast(t) for c, t in FINAL_SCHEMA.items()])
        df_combined = pl.concat([existing_df, df_nuevo], how="vertical")
        st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='first')
    else:
        st.session_state.data = df_nuevo.unique(subset=['nro_cli'], keep='first')

def procesar_ods(uploaded_ods):
    """Carga ODS, mapea columna X a estado y procesa el resto."""
    try:
        # Nota: requiere 'odfpy' instalado
        df_pd = pd.read_excel(uploaded_ods, engine='odf')
        
        # Validar si es el formato correcto (Columna 1 = "X")
        if df_pd.columns[0].upper() != "X":
            st.error("Archivo ODS inválido: La primera columna debe llamarse 'X'.")
            return

        # Mapeo de estados basado en el primer campo
        # Se limpia el string y se busca en el diccionario, por defecto 'pendiente'
        df_pd['estado'] = df_pd.iloc[:, 0].astype(str).str.strip().str.lower().map(ODS_ESTADO_MAP).fillna('pendiente')
        
        # Convertir a Polars y renombrar columnas restantes
        df_pl = pl.from_pandas(df_pd)
        df_pl = df_pl.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_pl.columns})

        # Normalización estándar
        hoy = datetime.now().strftime(DATE_FORMAT)
        df_pl = df_pl.with_columns([
            pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8),
            pl.lit(hoy).alias('fecha_intervencion')
        ]).select([pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items() if col in df_pl.columns])

        fusionar_datos(df_pl)
        st.success(f"ODS Importado. Total: {len(st.session_state.data)} registros.")
    except Exception as e:
        st.error(f"Error procesando ODS: {e}")

def cargar_db(uploaded_file):
    db_bytes = uploaded_file.read()
    try:
        temp_file_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_file_path, "wb") as f: f.write(db_bytes)
        conn_disk = sqlite3.connect(temp_file_path)
        conn = sqlite3.connect(':memory:')
        conn_disk.backup(conn)
        df = pl.read_database("SELECT * FROM desvinculados", conn)
        st.session_state.data = df.with_columns(pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT)))
        st.session_state.db_cargada = True
        st.success(f"DB cargada: {len(df)} registros.")
    except Exception as e: st.error(f"Error DB: {e}")
    finally:
        if 'conn_disk' in locals(): conn_disk.close()
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path): os.remove(temp_file_path)

def procesar_csv(uploaded_csv):
    try:
        df_csv = pl.read_csv(uploaded_csv)
        df_csv = df_csv.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_csv.columns})
        hoy = datetime.now().strftime(DATE_FORMAT)
        df_csv = df_csv.with_columns([
            pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8),
            pl.lit('pendiente').alias('estado'),
            pl.lit(hoy).alias('fecha_intervencion')
        ]).select([pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items() if col in df_csv.columns])
        fusionar_datos(df_csv)
        st.success("CSV cargado.")
    except Exception as e: st.error(f"Error CSV: {e}")

def guardar_db_bytes(df):
    conn = sqlite3.connect(':memory:')
    df.to_pandas().to_sql('desvinculados', conn, if_exists='replace', index=False)
    temp_file_path = f"/tmp/{uuid.uuid4()}.db"
    conn_disk = sqlite3.connect(temp_file_path)
    conn.backup(conn_disk)
    conn_disk.close()
    with open(temp_file_path, "rb") as f: b = f.read()
    os.remove(temp_file_path)
    return b

# ==============================================================================
# 3. INTERFAZ DE USUARIO (NAVEGACIÓN SUPERIOR)
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")

# Inicialización de estado
if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

# --- MENU DE NAVEGACIÓN SUPERIOR ---
tab_principal, tab_importar = st.tabs(["🏠 Gestión Principal", "📊 Importar hoja de cálculo"])

with tab_importar:
    st.header("Importación de datos externos")
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Carga ODS (Simbólico)")
        st.info("Formato esperado: Columna 1='X' (+:cargado, ?:revisar, x:otro, vacio:pendiente)")
        ods_file = st.file_uploader("Subir archivo .ods", type=['ods'], key="ods_uploader")
        if ods_file:
            procesar_ods(ods_file)

    with col_b:
        st.subheader("Carga CSV (Estándar)")
        csv_file = st.file_uploader("Subir archivo .csv", type=['csv'], key="csv_uploader")
        if csv_file:
            procesar_csv(csv_file)

with tab_principal:
    st.header("Panel de Control")
    
    # Carga de DB inicial solo si está vacía la memoria
    if len(st.session_state.data) == 0:
        db_file = st.file_uploader("Cargar Base de Datos .db para empezar", type=['db', 'sqlite'])
        if db_file: cargar_db(db_file)
    
    # --- Interfaz de ABM existente ---
    if len(st.session_state.data) > 0:
        # (Aquí va toda tu lógica de filtrado, data_editor y guardado que ya tienes resuelta)
        # Por brevedad, mantengo la estructura del ABM:
        
        filtro_estado = st.selectbox("Filtrar registros:", options=FILTRO_OPTIONS, index=FILTRO_OPTIONS.index('pendiente'))
        df_to_edit = st.session_state.data.filter(pl.col('estado') == filtro_estado) if filtro_estado != "Todos los registros" else st.session_state.data
        
        df_edit_pandas = df_to_edit.to_pandas()
        # ... (Conversiones de fecha de tu código base)
        df_edit_pandas['fecha_intervencion'] = pd.to_datetime(df_edit_pandas['fecha_intervencion'], errors='coerce').fillna(datetime.now().date())
        df_edit_pandas['fecha_alta'] = pd.to_datetime(df_edit_pandas['fecha_alta'], errors='coerce').fillna(datetime.now().date())

        edited_df_pandas = st.data_editor(
            df_edit_pandas,
            column_config={
                "estado": st.column_config.SelectboxColumn("estado", options=list(ESTADOS), required=True),
                "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format=DATE_FORMAT)
            },
            disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'),
            num_rows='dynamic', hide_index=True
        )

        if st.button("✅ Guardar cambios"):
            # Lógica de guardado que ya tienes...
            df_comm = pl.from_pandas(edited_df_pandas).filter(pl.col('nro_cli') > 0)
            df_comm = df_comm.with_columns([
                pl.col('fecha_intervencion').dt.strftime(DATE_FORMAT),
                pl.col('fecha_alta').dt.strftime(DATE_FORMAT)
            ])
            nro_cli_comm = df_comm.get_column('nro_cli')
            df_unaff = st.session_state.data.filter(~pl.col('nro_cli').is_in(nro_cli_comm))
            st.session_state.data = pl.concat([df_unaff, df_comm], how="vertical")
            st.success("Cambios guardados.")
            st.rerun()

        st.markdown("---")
        st.header("3. Finalizar y exportar")
        
        # Botón de limpieza de 'cargados'
        if st.button("🗑️ Eliminar registros 'cargado'"):
            st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
            st.rerun()

        c1, c2 = st.columns(2)
        with c1:
            st.download_button("💾 Descargar DB", data=guardar_db_bytes(st.session_state.data), file_name="actualizado.db")
        with c2:
            st.download_button("⬇️ Descargar CSV", data=st.session_state.data.write_csv().encode('utf-8'), file_name="actualizado.csv")
            