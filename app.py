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
DB_COLUMNS = ['nro_cli', 'nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta', 'fecha_intervencion', 'estado']
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')

CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 'NUMERO_MEDIDOR': 'nro_med', 'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 'NORMALICOMERCIAL': 'normalizado', 'FECHA_ALTA': 'fecha_alta'
}

FINAL_SCHEMA = {
    'nro_cli': pl.Int64, 'nro_med': pl.Int64, 'usuario': pl.Utf8, 
    'domicilio': pl.Utf8, 'normalizado': pl.Int64, 'fecha_alta': pl.Utf8, 
    'fecha_intervencion': pl.Utf8, 'estado': pl.Utf8
}

# ==============================================================================
# 2. FUNCIONES DE LÓGICA DE NEGOCIO Y MANEJO DE ARCHIVOS (Solo relevantes)
# ==============================================================================

# ... (normalizar_fecha, cargar_db, procesar_csv se mantienen igual) ...

# Nota: Las funciones normalizar_fecha, cargar_db y procesar_csv deben estar definidas aquí.
# Las dejaremos omitidas por brevedad, asumiendo que tienes la última versión funcional.

def guardar_db_bytes(df):
    """Convierte el Polars DataFrame a un archivo DB binario para descarga."""
    conn = sqlite3.connect(':memory:')
    
    try:
        df.write_database(
            table_name='desvinculados', 
            connection=conn, 
            if_exists='replace',
            database_driver='sqlite' 
        )
    except Exception:
        # Fallback a Pandas si Polars no puede escribir al DB
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
# 3. IMPLEMENTACIÓN DEL ABM
# ==============================================================================

# Función para manejar el Alta (Agregar registro)
def agregar_registro(nro_cli, nro_med, usuario, domicilio):
    
    if nro_cli in st.session_state.data['nro_cli'].to_list():
        st.error(f"Error: El Nro. Cliente {nro_cli} ya existe.")
        return False
        
    hoy_str = datetime.now().strftime(DATE_FORMAT)
    
    nuevo_registro = pl.DataFrame({
        'nro_cli': [nro_cli],
        'nro_med': [nro_med],
        'usuario': [usuario],
        'domicilio': [domicilio],
        'normalizado': [0],
        'fecha_alta': [hoy_str],
        'fecha_intervencion': [hoy_str],
        'estado': ['pendiente']
    }).select(
        [pl.col(col).cast(FINAL_SCHEMA[col]) for col in FINAL_SCHEMA.keys()]
    )

    st.session_state.data = pl.concat([st.session_state.data, nuevo_registro], how="vertical")
    st.success(f"Registro del cliente {nro_cli} añadido con éxito.")
    return True

# ==============================================================================
# 4. INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")
st.title("⚡ Gestor Web de Desvinculados EPE (ABM)")

# ... (Inicialización del estado de sesión y Carga de Datos se mantienen igual) ...
if 'db_cargada' not in st.session_state: st.session_state.db_cargada = False
if 'data' not in st.session_state: st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)
    
# --- Controles de Carga ---
st.header("1. Carga de Datos")
col1, col2 = st.columns(2)
# ... (Contenido de col1 y col2 para cargar DB y CSV se mantiene igual) ...

# --- Interfaz de ABM (Alta y Edición/Baja) ---
if len(st.session_state.data) > 0:
    
    st.header(f"2. Gestión de Registros (ABM)")
    
    # 2.1 Alta (A) de Registros
    st.subheader("2.1 Alta de Nuevo Registro")
    with st.form("form_alta"):
        colA, colB, colC, colD = st.columns(4)
        nro_cli = colA.number_input("Nro. Cliente (Clave)", min_value=1, step=1, key="cli_a")
        nro_med = colB.number_input("Nro. Medidor", min_value=1, step=1, key="med_a")
        usuario = colC.text_input("Usuario", key="user_a")
        domicilio = colD.text_input("Domicilio", key="dom_a")
        
        submitted = st.form_submit_button("➕ Agregar Registro Manual")
        if submitted and nro_cli > 0 and usuario and domicilio:
            agregar_registro(nro_cli, nro_med, usuario, domicilio)
        elif submitted:
            st.warning("Debe completar Nro. Cliente, Usuario y Domicilio.")
            
    st.markdown("---")
    
    # 2.2 Modificación (M) y Baja (B)
    st.subheader(f"2.2 Modificación y Eliminación ({len(st.session_state.data)} registros)")
    
    # Conversión CRÍTICA: Polars a Pandas para st.data_editor
    df_edit_pandas = st.session_state.data.to_pandas()
    
    # CONVERSIÓN A DATETIME DE PANDAS (Necesario para DateColumn)
    try:
        df_edit_pandas['fecha_intervencion'] = pd.to_datetime(df_edit_pandas['fecha_intervencion'], format=DATE_FORMAT, errors='coerce')
        df_edit_pandas['fecha_alta'] = pd.to_datetime(df_edit_pandas['fecha_alta'], format=DATE_FORMAT, errors='coerce')
    except Exception: pass
        
    hoy_datetime = datetime.now().date() 
    df_edit_pandas['fecha_intervencion'] = df_edit_pandas['fecha_intervencion'].fillna(hoy_datetime)
    df_edit_pandas['fecha_alta'] = df_edit_pandas['fecha_alta'].fillna(hoy_datetime)
    
    estado_config = st.column_config.SelectboxColumn("Estado", options=list(ESTADOS), required=True)
    fecha_config = st.column_config.DateColumn("Fecha Intervención", format=DATE_FORMAT, required=True)

    edited_df_pandas = st.data_editor(
        df_edit_pandas,
        column_config={"estado": estado_config, "fecha_intervencion": fecha_config},
        disabled=('nro_cli', 'nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'), 
        hide_index=False, # Habilitar índice para la eliminación (Baja)
        num_rows="dynamic", # Habilitar la adición manual (si fuera compatible, pero es mejor el formulario)
        key="data_editor_abm"
    )

    # 3. Procesar Cambios y Bajas (M y B)
    if edited_df_pandas is not None:
        
        # 3.1 Procesar Bajas (B): Streamlit marca las filas eliminadas.
        edit_result = st.session_state["data_editor_abm"]
        if edit_result.get("deleted_rows"):
            deleted_indices = edit_result["deleted_rows"]
            
            # Polars no soporta eliminación por índice de Pandas, pero sí por fila.
            # Convertimos la lista de índices eliminados a una serie booleana y filtramos.
            mask = [i not in deleted_indices for i in range(len(st.session_state.data))]
            st.session_state.data = st.session_state.data.filter(pl.Series(mask))
            st.success(f"{len(deleted_indices)} registros eliminados con éxito.")

        # 3.2 Procesar Modificaciones (M): Actualizar el estado de sesión
        df_return_polars = pl.from_pandas(edited_df_pandas)
        st.session_state.data = df_return_polars \
            .with_columns(
                pl.col('fecha_intervencion').dt.strftime(DATE_FORMAT).alias('fecha_intervencion'),
                pl.col('fecha_alta').dt.strftime(DATE_FORMAT).alias('fecha_alta') 
            )

    st.header("3. Finalizar y Exportar")
    
    # 3.3 Descarga (Base de Datos o CSV)
    st.download_button(
        label="💾 Descargar Base de Datos Actualizada (.db)",
        data=guardar_db_bytes(st.session_state.data),
        file_name='desvinculados_actualizado.db',
        mime='application/octet-stream',
        help="Guarda la base de datos actualizada con los cambios de ABM."
    )
    
    st.download_button(
        label="⬇️ Descargar CSV (Alternativo)",
        data=st.session_state.data.write_csv(None).encode('utf-8'),
        file_name='desvinculados_actualizado.csv',
        mime='text/csv',
        help="Descarga los datos en formato CSV (más compatible con entornos web)."
    )
    
else:
    st.info("Por favor, cargue una Base de Datos o un CSV para comenzar la gestión de registros.")