import streamlit as st
import polars as pl
import sqlite3
import io
import os 
import uuid 
from datetime import datetime

# ==============================================================================
# 1. CONFIGURACIÓN Y MAPEOS
# ==============================================================================

DATE_FORMAT = '%Y-%m-%d'

# Tu lógica específica de estados
ODS_ESTADO_MAP = {
    '+': 'cargado',
    '?': 'revisar',
    'x': 'otro distrito',
    '-': 'otro distrito',
    '': 'pendiente',
    None: 'pendiente'
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

CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 'NUMERO_MEDIDOR': 'nro_med', 'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 'NORMALIZADO': 'normalizado', 'FECHA_ALTA': 'fecha_alta'
}

# ==============================================================================
# 2. PROCESAMIENTO SIN PANDAS (NUCLEAR)
# ==============================================================================

def procesar_archivo_inteligente(uploaded_file):
    """
    Usa Polars nativo con Calamine. Bypassea totalmente el bug de 'int' de odfpy.
    """
    try:
        nombre = uploaded_file.name.lower()
        # Obtenemos los bytes una sola vez
        file_bytes = uploaded_file.read()
        
        if nombre.endswith('.csv'):
            df = pl.read_csv(io.BytesIO(file_bytes), infer_schema_length=10000)
            # Manejo de normalizado para evitar el error str -> i64
            if 'NORMALIZADO' in df.columns:
                df = df.with_columns(
                    pl.col('NORMALIZADO').cast(pl.Utf8).str.to_lowercase()
                    .is_in(['si', 's', '1', 'true', 't']).cast(pl.Int64).alias('NORMALIZADO')
                )
            estado_default = 'pendiente'
            
        elif nombre.endswith('.ods'):
            # LEER ODS CON POLARS + CALAMINE (Sin pasar por Pandas)
            # Esto es lo que soluciona el error 'Expected bytes, got int'
            df = pl.read_excel(io.BytesIO(file_bytes), engine="calamine")
            
            # Lógica de la Columna X
            col_x = df.columns[0]
            if col_x.upper() != 'X':
                st.error(f"Error: La primera columna debe ser 'X', no '{col_x}'")
                return
            
            # Aplicar tu mapeo de símbolos
            df = df.with_columns(
                pl.col(col_x).cast(pl.Utf8).fill_null("")
                .map_elements(lambda s: ODS_ESTADO_MAP.get(s.strip().lower(), "pendiente"), return_dtype=pl.Utf8)
                .alias("estado")
            ).drop(col_x)
            estado_default = None # Ya lo calculamos arriba
        else:
            st.error("Formato no soportado.")
            return

        # Estandarización de columnas
        df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})
        
        # Añadir campos faltantes
        hoy = datetime.now().strftime(DATE_FORMAT)
        if 'estado' not in df.columns: df = df.with_columns(pl.lit(estado_default).alias('estado'))
        if 'fecha_intervencion' not in df.columns: df = df.with_columns(pl.lit(hoy).alias('fecha_intervencion'))
        
        # Asegurar esquema final y tipos
        for col, dtype in FINAL_SCHEMA.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        df_final = df.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA.keys()])

        # Merge con memoria
        if len(st.session_state.data) > 0:
            st.session_state.data = pl.concat([st.session_state.data, df_final], how="vertical").unique(subset=['nro_cli'], keep='last')
        else:
            st.session_state.data = df_final
            
        st.success(f"Cargados {len(df_final)} registros.")
        st.rerun()

    except Exception as e:
        st.error(f"Fallo crítico: {e}")

# ==============================================================================
# 3. INTERFAZ Y ESTADO
# ==============================================================================

st.set_page_config(layout="wide", page_title="Desvinculados", page_icon="⚡")

if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

st.title("⚡ Gestión de Desvinculados")

# El visor y el botón deben estar en la misma pestaña según tu pedido
tab_unica = st.tabs(["Gestión de datos"])[0]

with tab_unica:
    c1, c2 = st.columns(2)
    with c1:
        # Tu botón de "Importar hoja de cálculo"
        archivo = st.file_uploader("📥 Importar hoja de cálculo (CSV o ODS)", type=['csv', 'ods'])
        if archivo:
            procesar_archivo_inteligente(archivo)
    
    with c2:
        # Carga de DB existente
        f_db = st.file_uploader("📂 Cargar Base de Datos (.db)", type=['db'])
        if f_db:
            tmp = f"/tmp/{uuid.uuid4()}.db"
            with open(tmp, "wb") as f: f.write(f_db.read())
            conn = sqlite3.connect(tmp)
            st.session_state.data = pl.read_database("SELECT * FROM desvinculados", conn)
            conn.close()
            os.remove(tmp)
            st.rerun()

    st.divider()

    if len(st.session_state.data) > 0:
        # Editor y Visor
        df_edit = st.data_editor(
            st.session_state.data.to_pandas(),
            key="editor_principal",
            num_rows="dynamic",
            hide_index=True
        )
        
        if st.button("💾 Guardar cambios"):
            st.session_state.data = pl.from_pandas(df_edit)
            st.success("Datos guardados en memoria.")