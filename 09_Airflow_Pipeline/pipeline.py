from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator  
from datetime import datetime
import os
import pandas as pd

# 🔹 Ruta donde están los archivos TXT con las consultas
SQL_FILES_PATH = "/home/jeisson/Documentos/Query/"

# 🔹 Función para ejecutar consultas SQL y guardar el resultado en XCom
def execute_query_from_file(filename, xcom_key, **kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    file_path = os.path.join(SQL_FILES_PATH, filename)

    with open(file_path, 'r') as file:
        sql_query = file.read()

    df = mysql_hook.get_pandas_df(sql_query)  # 🔹 Ejecuta la consulta y obtiene DataFrame
    print(f"Ejecutado {filename}: {df.head()}")  

    # 📤 Guardar resultado en XCom
    ti = kwargs['ti']
    ti.xcom_push(key=xcom_key, value=df.to_json()) 

# 🔹 Función para validar duplicados en un DataFrame
def validate_duplicates(xcom_key, key_column, **kwargs):
    ti = kwargs['ti']
    
    # 📥 Recuperar los datos de XCom
    query_results_json = ti.xcom_pull(task_ids=f'mysql_{xcom_key}', key=xcom_key)
    if query_results_json is None:
        raise ValueError(f"No se encontraron datos en XCom para {xcom_key}")

    df = pd.read_json(query_results_json)

    # 🔹 Verifica duplicados en la columna específica
    if df[key_column].duplicated().any():
        duplicates_df = df[df[key_column].duplicated()]
        print(f"🚨 Se encontraron duplicados en {xcom_key}:", duplicates_df)

    df_clean = df.drop_duplicates(subset=[key_column])

    # 📤 Guardar el DataFrame limpio en XCom
    ti.xcom_push(key=f'clean_{xcom_key}', value=df_clean.to_json())

# 🔹 Función para validar valores faltantes en todas las columnas
def validate_missing_values(xcom_key, **kwargs):
    ti = kwargs['ti']
    
    # 📥 Recuperar los datos de XCom
    query_results_json = ti.xcom_pull(task_ids=f'mysql_{xcom_key}', key=xcom_key)
    if query_results_json is None:
        raise ValueError(f"No se encontraron datos en XCom para {xcom_key}")

    df = pd.read_json(query_results_json)

    # 🔹 Verifica valores nulos en **todas** las columnas
    missing_values = df.isnull().sum()
    missing_columns = missing_values[missing_values > 0].index.tolist()

    if missing_columns:
        print(f"⚠️ Columnas con valores faltantes en {xcom_key}: {missing_columns}")

    # 📤 Guardar en XCom
    ti.xcom_push(key=f'missing_{xcom_key}', value=missing_columns)

def validate_foreign_keys(xcom_key_main, key_main, xcom_key_ref, key_ref, **kwargs):
    """
    Valida que todas las claves foráneas en xcom_key_main existan en xcom_key_ref.

    :param xcom_key_main: Tabla con la clave foránea (Ej: 'dim_process')
    :param key_main: Columna de la clave foránea en xcom_key_main (Ej: 'status_id' o 'category_id')
    :param xcom_key_ref: Tabla de referencia (Ej: 'dim_status' o 'dim_category')
    :param key_ref: Columna clave primaria en xcom_key_ref (Ej: 'id_status' o 'id_category')
    """
    ti = kwargs['ti']

    # 📥 Obtener datos de XCom
    df_main_json = ti.xcom_pull(task_ids=f'mysql_{xcom_key_main}', key=xcom_key_main)
    df_ref_json = ti.xcom_pull(task_ids=f'mysql_{xcom_key_ref}', key=xcom_key_ref)

    if df_main_json is None or df_ref_json is None:
        raise ValueError(f"No se encontraron datos en XCom para {xcom_key_main} o {xcom_key_ref}")

    df_main = pd.read_json(df_main_json)
    df_ref = pd.read_json(df_ref_json)

    # 🔹 Validar que todas las claves foráneas existan en la tabla de referencia
    claves_ref = set(df_ref[key_ref].unique())
    claves_main = set(df_main[key_main].unique())

    claves_invalidas = claves_main - claves_ref

    if claves_invalidas:
        print(f"🚨 ERROR: Claves inválidas en {xcom_key_main}.{key_main} -> {claves_invalidas}")
    
    # Filtrar solo las filas con claves válidas
    df_validated = df_main[df_main[key_main].isin(claves_ref)]

    # 📤 Guardar en XCom
    ti.xcom_push(key=f'validated_{xcom_key_main}_{key_main}', value=df_validated.to_json())

def populate_table(xcom_key, sql_table, **kwargs):
    ti = kwargs['ti']

    # 📥 Recuperar los datos limpios desde XCom después de validaciones
    clean_data_json = ti.xcom_pull(task_ids=f'validate_duplicates_{xcom_key}', key=f'clean_{xcom_key}')
    if clean_data_json is None:
        raise ValueError(f"No se encontraron datos limpios en XCom para {xcom_key}.")

    df_clean = pd.read_json(clean_data_json)  # Convertir JSON a DataFrame

    # 🔹 Insertar los datos en SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_conn')
    mssql_hook.insert_rows(table=f'dbo.{sql_table}', rows=df_clean.to_records(index=False))

    print(f"✅ Tabla {sql_table} poblada exitosamente en SQL Server.")


# 🔹 Definir el DAG
with DAG(
    'Pipeline_ETL',
    default_args={'owner': 'airflow'},
    description='DAG para ejecutar múltiples consultas SQL y validar duplicados',
    schedule_interval=None,
    start_date=datetime(2025, 2, 1),
    catchup=False,
) as dag:
    
    # 🔹 DummyOperator de inicio
    inicio = DummyOperator(
        task_id='inicio'
    )

    # 🔹 Tarea 1: Leer y guardar datos en XCom para Dim_status
    Tarea_1 = PythonOperator(
        task_id='mysql_dim_status',
        python_callable=execute_query_from_file,  
        op_args=['Dim_status.sql', 'dim_status'],  # Se pasa xcom_key
        provide_context=True  
    )

    # 🔹 Tarea 2: Validar duplicados en Dim_status
    Tarea_2 = PythonOperator(
        task_id='validate_duplicates_dim_status',
        python_callable=validate_duplicates,
        op_args=['dim_status', 'id_status'],  # 🔹 Se pasa xcom_key y key_column
        provide_context=True,  
    )

    # 🔹 Tarea 3: Validar valores faltantes en Dim_status
    Tarea_3 = PythonOperator(
        task_id='validate_missing_dim_status',
        python_callable=validate_missing_values,
        op_args=['dim_status'],  
        provide_context=True,  
    )

    # 🔹 Tarea 4: Leer y guardar datos en XCom para Dim_category
    Tarea_4 = PythonOperator(
        task_id='mysql_dim_category',
        python_callable=execute_query_from_file,  
        op_args=['Dim_Category.sql', 'dim_category'],  # Se pasa xcom_key
        provide_context=True  
    )

    # 🔹 Tarea 5: Validar duplicados en Dim_category
    Tarea_5 = PythonOperator(
        task_id='validate_duplicates_dim_category',
        python_callable=validate_duplicates,
        op_args=['dim_category', 'id_category'],  # 🔹 Se pasa xcom_key y key_column
        provide_context=True,  
    )

    # 🔹 Tarea 6: Validar valores faltantes en Dim_category
    Tarea_6 = PythonOperator(
        task_id='validate_missing_dim_category',
        python_callable=validate_missing_values,
        op_args=['dim_category'],  
        provide_context=True,  
    )
    # 🔹 Tarea 4: Leer y guardar datos en XCom para Dim_process
    Tarea_7 = PythonOperator(
        task_id='mysql_dim_process',
        python_callable=execute_query_from_file,  
        op_args=['Dim_process.sql', 'dim_process'],  # Se pasa xcom_key
        provide_context=True  
    )

    # 🔹 Tarea 5: Validar duplicados en dim_process
    Tarea_8 = PythonOperator(
        task_id='validate_duplicates_dim_process',
        python_callable=validate_duplicates,
        op_args=['dim_process', 'id_process'],  # 🔹 Se pasa xcom_key y key_column
        provide_context=True,  
    )

    # 🔹 Tarea 6: Validar valores faltantes en dim_process
    Tarea_9 = PythonOperator(
        task_id='validate_missing_dim_process',
        python_callable=validate_missing_values,
        op_args=['dim_process'],  
        provide_context=True,  
    )
    Tarea_10 = PythonOperator(
        task_id='validate_fk_status_dim_process',
        python_callable=validate_foreign_keys,
        op_args=['dim_process', 'status_id', 'dim_status', 'id_status'],
        provide_context=True,
    )

    # 🔹 Tarea 11: Validar relación dim_process.category_id con dim_category.id_category
    Tarea_11 = PythonOperator(
        task_id='validate_fk_category_dim_process',
        python_callable=validate_foreign_keys,
        op_args=['dim_process', 'category_id', 'dim_category', 'id_category'],
        provide_context=True,
)

    # 🔹 Tarea 4: Leer y guardar datos en XCom para Dim_cities
    Tarea_12 = PythonOperator(
        task_id='mysql_dim_cities',
        python_callable=execute_query_from_file,  
        op_args=['Dim_Cities.sql', 'dim_cities'],  # Se pasa xcom_key
        provide_context=True  
    )

    # 🔹 Tarea 5: Validar duplicados en dim_cities
    Tarea_13 = PythonOperator(
        task_id='validate_duplicates_dim_cities',
        python_callable=validate_duplicates,
        op_args=['dim_cities', 'id_city'],  # 🔹 Se pasa xcom_key y key_column
        provide_context=True,  
    )

    # 🔹 Tarea 6: Validar valores faltantes en dim_cities
    Tarea_14 = PythonOperator(
        task_id='validate_missing_dim_cities',
        python_callable=validate_missing_values,
        op_args=['dim_cities'],  
        provide_context=True,  
    )

    # 🔹 Tarea 4: Leer y guardar datos en XCom para Dim_cities
    Tarea_15 = PythonOperator(
        task_id='mysql_Dim_departments',
        python_callable=execute_query_from_file,  
        op_args=['Dim_departments.sql', 'Dim_departments'],  # Se pasa xcom_key
        provide_context=True  
    )

    # 🔹 Tarea 5: Validar duplicados en dim_cities
    Tarea_16 = PythonOperator(
        task_id='validate_duplicates_Dim_departments',
        python_callable=validate_duplicates,
        op_args=['Dim_departments', 'id_department'],  # 🔹 Se pasa xcom_key y key_column
        provide_context=True,  
    )

    # 🔹 Tarea 6: Validar valores faltantes en dim_cities
    Tarea_17 = PythonOperator(
        task_id='validate_missing_Dim_departments',
        python_callable=validate_missing_values,
        op_args=['Dim_departments'],  
        provide_context=True,  
    )

    # 🔹 Tarea 4: Leer y guardar datos en XCom para Dim_cities
    Tarea_18 = PythonOperator(
        task_id='mysql_Dim_users',
        python_callable=execute_query_from_file,  
        op_args=['Dim_users.sql', 'Dim_users'],  # Se pasa xcom_key
        provide_context=True  
    )

    # 🔹 Tarea 5: Validar duplicados en dim_cities
    Tarea_19 = PythonOperator(
        task_id='validate_duplicates_Dim_users',
        python_callable=validate_duplicates,
        op_args=['Dim_users', 'id_user'],  # 🔹 Se pasa xcom_key y key_column
        provide_context=True,  
    )

    # 🔹 Tarea 6: Validar valores faltantes en dim_cities
    Tarea_20 = PythonOperator(
        task_id='validate_missing_Dim_users',
        python_callable=validate_missing_values,
        op_args=['Dim_users'],  
        provide_context=True,  
    )

    Tarea_21 = PythonOperator(
        task_id='mysql_Fac_audience',
        python_callable=execute_query_from_file,  
        op_args=['Fac_Audiences.sql', 'Fac_Audience'],  # Se pasa xcom_key
        provide_context=True  
    )

    Tarea_22 = PythonOperator(
        task_id='populate_status_table',
        python_callable=populate_table,
        op_args=['dim_status', 'DimEstado'],  # 🔹 Aquí le pasamos el nombre de la tabla SQL Server
        provide_context=True,  
    )  
    Tarea_23 = PythonOperator(
        task_id='populate_cities_table',
        python_callable=populate_table,
        op_args=['dim_cities', 'DimCiudad'],  # 🔹 Aquí le pasamos el nombre de la tabla SQL Server
        provide_context=True,  
    )  
    Tarea_24 = PythonOperator(
        task_id='populate_Process_Table',
        python_callable=populate_table,
        op_args=['dim_process', 'DimProcess'],  # 🔹 Aquí le pasamos el nombre de la tabla SQL Server
        provide_context=True,  
    )  

    Tarea_25 = PythonOperator(
        task_id='populate_Deparments_Table',
        python_callable=populate_table,
        op_args=['Dim_departments', 'DimDepartamento'],  # 🔹 Aquí le pasamos el nombre de la tabla SQL Server
        provide_context=True,  
    )  

    Tarea_26 = PythonOperator(
        task_id='populate_user_Table',
        python_callable=populate_table,
        op_args=['Dim_users', 'DimUsuario'],  # 🔹 Aquí le pasamos el nombre de la tabla SQL Server
        provide_context=True,  
    ) 

    Tarea_27 = PythonOperator(
        task_id='populate_Category_Table',
        python_callable=populate_table,
        op_args=['dim_category', 'DimCategoria'],  # 🔹 Aquí le pasamos el nombre de la tabla SQL Server
        provide_context=True,  
    ) 

    Tarea_28 = PythonOperator(
        task_id='populate_Audience_Table',
        python_callable=populate_table,
        op_args=['Fac_Audience', 'FactAudiences'],  # 🔹 Aquí le pasamos el nombre de la tabla SQL Server
        provide_context=True,  
    )    

    fin = DummyOperator(
    task_id='fin', 
    dag=dag
)
    # 🔹 Definir la secuencia de ejecución
    inicio >> [Tarea_1, Tarea_4,Tarea_7,Tarea_12,Tarea_15,Tarea_18]  # 🔹 Inicio se divide en dos ramas paralelas
    Tarea_1 >> Tarea_2 >> Tarea_3 >>Tarea_22  # 🔹 Flujo 1: Dim_status
    Tarea_4 >> Tarea_5 >> Tarea_6 >> Tarea_27 # 🔹 Flujo 2: Dim_category
    Tarea_7 >> Tarea_8 >> Tarea_9 # 🔹 Flujo 3: Dim_process
    Tarea_12 >> Tarea_13 >> Tarea_14 >> Tarea_23 # 🔹 Flujo 4: Dim_cities
    Tarea_15 >> Tarea_16 >> Tarea_17 >> Tarea_25  # 🔹 Flujo 5: Dim_departments
    Tarea_18 >> Tarea_19 >> Tarea_20 >> Tarea_26 # 🔹 Flujo 6: Dim_user
    Tarea_9 >> [Tarea_10, Tarea_11] 
    Tarea_10 >>Tarea_24
    Tarea_11   >>Tarea_24 
    [Tarea_3, Tarea_6,Tarea_14, Tarea_17, Tarea_20, Tarea_10, Tarea_11] >> Tarea_21
    Tarea_21 >> Tarea_28
    [Tarea_22,Tarea_23,Tarea_24,Tarea_25,Tarea_26,Tarea_27,Tarea_28]>> fin