# Pipeline ETL con Apache Airflow

## ¿Qué es Apache Airflow?

**Apache Airflow** es una plataforma de código abierto diseñada para ayudar a los usuarios a programar, monitorear y administrar flujos de trabajo (workflows). Airflow permite definir tareas que se ejecutan de manera secuencial o en paralelo y coordina el flujo de ejecución de esas tareas a través de un **DAG (Directed Acyclic Graph)**. 

Airflow es utilizado principalmente para la automatización de tareas de procesamiento de datos en pipelines de **ETL (Extract, Transform, Load)**, donde se extraen datos de diferentes fuentes, se transforman según reglas definidas y finalmente se cargan en un sistema de destino (como bases de datos, almacenamiento en la nube, etc.).

# ¿Qué es un DAG en Apache Airflow?

En **Apache Airflow**, un **DAG** (siglas de **Directed Acyclic Graph**) es un conjunto de tareas organizadas en un gráfico dirigido y acíclico. Los DAGs son la unidad fundamental de trabajo en Airflow y representan un flujo de trabajo completo.

## Características de un DAG

1. **Dirigido (Directed)**: Los DAGs son dirigidos, lo que significa que tienen una dirección explícita entre las tareas. Las tareas dependen de otras tareas para ejecutarse en un orden específico. Este flujo de dependencia se define utilizando operadores de Airflow, como `PythonOperator`, `BashOperator`, etc.

2. **Acíclicos (Acyclic)**: Un DAG es acíclico, lo que quiere decir que no puede haber ciclos, es decir, no se permite que una tarea dependa indirectamente de sí misma. Esto asegura que no haya bucles infinitos en la ejecución del pipeline.

3. **Definición de Tareas**: Cada tarea en un DAG es una operación que se debe ejecutar, como ejecutar un script Python, realizar una consulta SQL o mover archivos. Cada tarea es independiente y puede fallar o completarse con éxito, lo que afectará la ejecución de las tareas siguientes según las dependencias definidas.

4. **Programación**: Los DAGs permiten establecer una **frecuencia de ejecución**, ya sea para ejecutarse de manera periódica (por ejemplo, cada hora, cada día) o de manera manual según se necesite.

5. **Ejemplo de un DAG**: En un DAG de Airflow, puedes tener tareas como:
   - Tarea A: Ejecutar una consulta SQL para extraer datos.
   - Tarea B: Limpiar los datos extraídos.
   - Tarea C: Cargar los datos procesados a un sistema de almacenamiento.

   Las tareas A, B y C están conectadas por dependencias: la tarea B debe ejecutarse después de la tarea A, y la tarea C debe ejecutarse después de la tarea B.

## Componentes de un DAG

Un **DAG** en Airflow está compuesto por varios elementos clave:

- **Definición del DAG**: Es el bloque principal donde se definen las tareas y sus dependencias.
- **Tareas (Tasks)**: Son las unidades de trabajo dentro de un DAG. Cada tarea puede ejecutar cualquier tipo de operación como un script, una consulta SQL, una operación de datos, etc.
- **Dependencias**: Se definen para indicar el orden en el que las tareas deben ejecutarse.
- **Programación (Schedule)**: Puedes definir la periodicidad de ejecución del DAG, especificando un horario o dejando que se ejecute manualmente.

## Ejemplo de un DAG Simple

Aquí tienes un ejemplo de cómo se define un DAG básico en Apache Airflow:

```python
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Definir el DAG
with DAG(
    'mi_dag_simple',
    description='Un DAG simple de ejemplo',
    schedule_interval='@daily',  # Ejecutar todos los días
    start_date=datetime(2025, 2, 1),
    catchup=False
) as dag:

    # Definir las tareas
    tarea_1 = DummyOperator(task_id='inicio')
    
    def mi_funcion():
        print("¡Hola desde Airflow!")
    
    tarea_2 = PythonOperator(
        task_id='ejecutar_funcion',
        python_callable=mi_funcion
    )
    
    tarea_3 = DummyOperator(task_id='fin')

    # Establecer dependencias
    tarea_1 >> tarea_2 >> tarea_3  # Tarea 1 -> Tarea 2 -> Tarea 3
```

## Descripción del Pipeline ETL

Este pipeline ETL está diseñado para extraer, transformar y cargar datos de varias fuentes a una base de datos **SQL Server**. Las fuentes de datos provienen de consultas SQL almacenadas en archivos `.sql` y se procesan usando **Apache Airflow**. Las validaciones incluyen la detección de duplicados, valores faltantes y validación de claves foráneas.

### **Etapas del Pipeline:**

#### 1. **Extracción de Datos:**
   - Se ejecutan consultas SQL almacenadas en archivos `.sql` (como `Dim_status.sql`, `Dim_process.sql`, etc.).
   - Los resultados de estas consultas se almacenan en **XCom**, lo que permite que las tareas posteriores accedan a los datos de manera eficiente.

#### 2. **Transformación de Datos:**
   - **Validación de Duplicados**: Verifica si existen filas duplicadas en las columnas clave de cada conjunto de datos. Si se encuentran duplicados, estos se registran y el DataFrame se limpia de duplicados.
   - **Validación de Valores Faltantes**: Verifica si alguna columna contiene valores nulos o faltantes. Si se encuentran, se registra el nombre de las columnas afectadas.
   - **Validación de Claves Foráneas**: Asegura que las claves foráneas de una tabla existan en la tabla de referencia correspondiente. Si se encuentran claves no válidas, estas se registran y se filtra el DataFrame para eliminar las filas con claves incorrectas.

#### 3. **Carga de Datos:**
   - Una vez que los datos se han transformado y validado, se insertan en las tablas correspondientes en una base de datos **SQL Server** utilizando el hook de **MsSqlHook** de Airflow.

### **Flujo de Ejecución del DAG**

Este pipeline sigue un flujo de trabajo definido por las siguientes tareas:

- **Extracción de Datos**: Se ejecutan consultas SQL desde archivos y se almacenan los resultados en XCom para su posterior procesamiento.
- **Transformación de Datos**: Se validan los duplicados, valores faltantes y claves foráneas. Los datos se limpian y se validan antes de ser cargados.
- **Carga de Datos**: Los datos validados y transformados se insertan en las tablas de SQL Server.

### **Estructura del DAG**

El DAG se compone de tareas que se ejecutan en secuencia, cada una responsable de una parte del proceso. El flujo general es el siguiente:

1. **Inicio (DummyOperator)**: Representa el inicio del pipeline.
2. **Extracción de Datos**: Cada archivo SQL es procesado por una tarea que ejecuta la consulta y guarda el resultado en XCom.
3. **Validaciones**: Se validan duplicados, valores faltantes y claves foráneas en los datos extraídos.
4. **Carga de Datos**: Los datos limpios y validados son insertados en las tablas de **SQL Server**.
5. **Fin (DummyOperator)**: Representa el fin del pipeline.

### **Diagrama de Dependencias**

El flujo de trabajo está organizado de la siguiente manera:

- El pipeline comienza con la tarea `inicio`, que se bifurca en tareas de lectura de archivos SQL.
- Cada conjunto de datos (como `Dim_status`, `Dim_process`, etc.) pasa por validaciones de duplicados y valores faltantes antes de ser insertado en SQL Server.
- Se valida que las claves foráneas entre las tablas estén correctas, antes de insertar los datos en la base de datos.
- Finalmente, todas las tareas de carga de datos se ejecutan y el pipeline termina con la tarea `fin`.

![Diagrama](https://github.com/Jeperezp/DataOps-and-Tools/blob/main/09_Airflow_Pipeline/Pipeline.png)


## Requisitos

Para ejecutar este pipeline, necesitarás tener instalado lo siguiente:

- **Apache Airflow** (preferiblemente en una versión compatible con los operadores y hooks utilizados).
- **Python 3.9** o superiores.
- **Conexiones a bases de datos**: Debes configurar las conexiones para MySQL (`mysql_default`) y SQL Server (`mssql_conn`) en Airflow.
- **Dependencias de Python**: Instala las bibliotecas necesarias como `pandas`, `apache-airflow-providers-mysql`, `apache-airflow-providers-microsoft-mssql`, `pyodbc`.

---

## Configuración de Conexiones

### **Conexión a MySQL**

La conexión a MySQL debe estar configurada en Airflow con el ID `mysql_default`. Asegúrate de tener las credenciales correctas para acceder a la base de datos de MySQL desde Airflow.

### **Conexión a SQL Server**

La conexión a SQL Server debe estar configurada con el ID `mssql_conn`. Asegúrate de que Airflow pueda acceder al servidor SQL Server y de que las credenciales estén correctamente configuradas.

---

## Ejecución del DAG

1. Asegúrate de que Apache Airflow esté en ejecución.
2. Coloca los archivos `.sql` de las consultas en la ruta `/home/jeisson/Documentos/Query/`.
3. Activa el DAG desde la interfaz de usuario de Airflow.
4. El DAG se ejecutará según el flujo de trabajo definido y procesará los datos de acuerdo con las validaciones.

---

## Conclusión

Este pipeline ETL utiliza Apache Airflow para automatizar el proceso de extracción, transformación y carga de datos en un entorno de producción. Gracias a las tareas de validación y carga, el proceso asegura que los datos sean consistentes y estén listos para su uso en análisis y toma de decisiones.

---

