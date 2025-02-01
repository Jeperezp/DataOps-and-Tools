# ¿Qué es un ETL?
**ETL** (Extract, Transform, Load) es un proceso fundamental en la ingeniería de datos que involucra tres pasos clave:

- 1. **Extract** (Extraer): Consiste en obtener datos de diversas fuentes, que pueden ser bases de datos, archivos, APIs, etc.
- 2. **Transform** (Transformar): Los datos extraídos se limpian, convierten o enriquecen según las necesidades del análisis. Este paso puede incluir la eliminación de duplicados, el cambio de formato o la agregación de datos.
- 3. **Load** (Cargar): Finalmente, los datos transformados se cargan en un sistema de almacenamiento como un data warehouse o una base de datos para su posterior análisis.

El proceso ETL es crucial para garantizar que los datos sean accesibles, precisos y adecuados para la toma de decisiones.


# Migración de Datos de OLTP a OLAP

Este proyecto tiene como objetivo realizar una migración de datos de un sistema OLTP (Online Transaction Processing) a un sistema OLAP (Online Analytical Processing). Utiliza Python para realizar la extracción, transformación y carga (ETL) de los datos, conectándose a bases de datos MySQL y SQL Server. A continuación, se describen los pasos y las funciones principales del proyecto.

## Descripción del Proyecto

El flujo de trabajo de este proyecto incluye:

1. **Extracción de datos (ETL):** Los datos se extraen desde un sistema OLTP (bases de datos MySQL).
2. **Transformación de datos:** Se realizan verificaciones de calidad, como la validación de duplicados, la verificación de valores faltantes y la validación de claves foráneas.
3. **Carga de datos:** Los datos transformados se cargan en una base de datos OLAP (SQL Server) para su análisis posterior.

## Requisitos

El proyecto requiere de las siguientes dependencias:

- Python 3.12.2
- `pandas`
- `sqlalchemy`
- `pymysql`
- `pyodbc`
- `logging`

Estas dependencias se pueden instalar utilizando `pip`:

```bash
pip install pandas sqlalchemy pymysql pyodbc
```

## Archivos y Estructura

El proyecto tiene la siguiente estructura de archivos:

- **config.json:**  
  Archivo de configuración que contiene las credenciales para conectar con las bases de datos MySQL y SQL Server. Es necesario proporcionar la información de acceso a ambas bases de datos (usuario, contraseña, host, puerto, etc.) en este archivo para que el proceso ETL funcione correctamente.

- **Mig_Data_ETL.log:**  
  Archivo de registro donde se almacenan los eventos clave del proceso ETL. Este archivo captura la información sobre la carga de datos, inserciones y cualquier error que ocurra durante el proceso. Es útil para el monitoreo y depuración del proceso de migración.

- **Código Python:**  
  El script principal del proyecto que ejecuta el proceso ETL. El código contiene las funciones necesarias para conectarse a las bases de datos, realizar las transformaciones de datos y cargar los resultados en SQL Server. 

  ## Funciones Principales

El script contiene varias funciones clave que realizan operaciones específicas dentro del proceso ETL. A continuación se describe cada una de ellas:

### `connect_db(query: str) -> pd.DataFrame`
Esta función se conecta a la base de datos MySQL, ejecuta una consulta SQL y devuelve los resultados en un `DataFrame` de pandas.

#### Parámetros:
- `query` (str): La consulta SQL que se ejecutará en la base de datos.

#### Retorno:
- `pd.DataFrame`: El resultado de la consulta en formato de `DataFrame`.

#### Ejemplo de uso:
```python
df = connect_db("SELECT * FROM Cities")
```
### `insert_data_to_sql(df: pd.DataFrame, table_name: str, if_exists: str = 'append')`

Esta función inserta los datos de un `DataFrame` en una tabla de SQL Server.
#### Parámetros:
- `df` (pd.DataFrame): El `DataFrame` con los datos a insertar.
- `table_name` (str): El nombre de la tabla en SQL Server.
- `if_exists` (str): Especifica qué hacer si la tabla ya existe. Puede ser `'replace'`, `'append'` o `'fail'`.

#### Ejemplo de uso
```python
insert_data_to_sql(df, 'DimUsuario')
```
### `duplicates_Pk(df: pd.DataFrame, campo: str) -> pd.DataFrame`

Esta función verifica si existen duplicados en la columna especificada de un `DataFrame`. Si se encuentran duplicados, lanza una excepción.

#### Parámetros:
- `df` (pd.DataFrame): El `DataFrame` en el que se verificarán los duplicados.
- `campo` (str): El nombre de la columna en la que se verificarán los duplicados.
#### Retorno:
`pd.DataFrame`: El `DataFrame` original si no se encuentran duplicados.
#### Ejemplo de uso:
```python
df = duplicates_Pk(df, 'id_city')
```

### `Faltantes(df: pd.DataFrame, campos: list) -> pd.DataFrame`
Esta función verifica si existen valores faltantes `(NaN)` en las columnas especificadas de un `DataFrame`. Si se encuentran valores faltantes, lanza una excepción.

#### Parámetros:
- `df` (pd.DataFrame): El `DataFrame` en el que se verificarán los valores faltantes.
- `campos` (list): Lista de nombres de las columnas a verificar.
#### Retorno:
`pd.DataFrame`: El `DataFrame` original si no se encuentran valores faltantes.
#### Ejemplo de uso:
```python
df = Faltantes(df, ['id_city', 'category_name'])
```
### `relaciones(df1: pd.DataFrame, key: str, df2: pd.DataFrame, foreing_key: str) -> pd.DataFrame`

Esta función verifica que los valores en la columna `foreing_key` del `DataFrame df2` existan en la columna `key` del `DataFrame df1`.

#### Parámetros:
- `df1` (pd.DataFrame): Primer `DataFrame` que contiene la clave primaria.
- `key` (str): Nombre de la columna en `df1` que contiene las claves primarias.
- `df2` (pd.DataFrame): Segundo `DataFrame` que contiene la clave foránea.
- `foreing_key` (str): Nombre de la columna en `df2` que contiene las claves foráneas.
#### Retorno:
`pd.DataFrame`: El `DataFrame df2` si todas las claves foráneas son válidas.
#### Ejemplo de uso:
```python
df = relaciones(DimEstado, 'id_status', DimProcess, 'id_status')
```

![Diagrama de la base de datos OLTP](https://github.com/Jeperezp/SqlWorkout/blob/main/Base_de_Datos_OLTP/OLTP/Untitled.svg)


![Diagrama de la base de datos OLTP](https://github.com/Jeperezp/SqlWorkout/blob/main/Base_de_Datos_OLTP/OLAP/Diagrama_OLAP.PNG)