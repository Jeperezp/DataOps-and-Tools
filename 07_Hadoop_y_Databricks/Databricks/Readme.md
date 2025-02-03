![Spark Logo Tiny](https://files.training.databricks.com/images/databricks.png)

## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)Apache Spark Fundamentals

Este repositorio contiene notebooks de Databricks con ejemplos y ejercicios para aprender los fundamentos de Apache Spark. Los notebooks están diseñados para ser ejecutados en la plataforma Databricks, pero también se pueden ejecutar localmente en un entorno de Spark. Los temas cubiertos incluyen la creación y manipulación de DataFrames, operaciones comunes de Spark, consultas SQL, y optimización de rendimiento.


## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)¿Qué es Apache Spark y por qué es útil?

Apache Spark es un motor de procesamiento de datos de código abierto diseñado para el procesamiento de grandes volúmenes de datos en entornos distribuidos. Es ampliamente utilizado debido a su capacidad para procesar datos en paralelo y su alto rendimiento, superando a otros frameworks como MapReduce. Spark es especialmente valioso en análisis de grandes datos, machine learning, y procesamiento de flujo en tiempo real.

Databricks es una plataforma basada en la nube que facilita el uso de Apache Spark. Proporciona un entorno fácil de usar con herramientas integradas para explorar datos, visualizarlos, y desarrollar modelos de machine learning, todo en un único entorno colaborativo. Al usar Databricks, se pueden ejecutar notebooks interactivamente, lo que permite realizar análisis de datos de manera eficiente y eficaz.



## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Notebooks 

Los notebooks incluidos en este repositorio son los siguientes:

1. **Fundamentos Spark**:
   - [01_Fundamentos_Spark.dbc](01_Fundamentos_Spark.dbc)
   
2. **Fundamentos Avanzados**:
   - [02_Fundamentos_Avanzados.dbc](02_Fundamentos_Avanzados.dbc)
   
3. **Caso Práctico: Análisis de Poblaciones en Apache Spark**:
   - [03_Analisis_Poblaciones.dbc](03_Analisis_Poblaciones.dbc)

### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Fundamentos Spark

En este notebook, se cubren los conceptos y técnicas básicas de Apache Spark. Se trata de cómo crear y trabajar con **DataFrames**, que son la estructura principal para manejar datos en Spark. Los temas tratados incluyen:

- **Creación de DataFrames**: Cómo generar DataFrames a partir de diferentes fuentes de datos.
- **Métodos comunes de DataFrames**:
  - `count()`: Para contar el número de filas.
  - `columns`: Para obtener los nombres de las columnas.
  - `dtypes` y `schema`: Para obtener el tipo de datos y el esquema del DataFrame.
  - `printSchema()`: Para visualizar el esquema del DataFrame.
  
- **Operaciones de transformación**:
  - **Select**: Seleccionar columnas específicas de un DataFrame.
  - **Filter**: Filtrar filas de acuerdo a condiciones.
  - **Drop**: Eliminar columnas o filas.
  - **GroupBy y Aggregations**: Agrupar datos por ciertas columnas y aplicar funciones agregadas como `sum()`, `avg()`, etc.
  - **Sort**: Ordenar los datos por una o más columnas.
  
  ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **Fundamentos Avanzados**

En este segundo notebook, se profundiza en funciones avanzadas de Spark para optimizar el manejo de grandes volúmenes de datos. Algunos de los temas cubiertos incluyen:

- **Optimización del almacenamiento de datos**:
  - **Cache** y **Persist**: Cómo guardar los DataFrames en memoria para optimizar su uso en múltiples operaciones.
  - **Unpersist** y **ClearCache**: Cómo liberar la memoria cuando ya no se necesiten los datos en memoria.

- **Manejo de datos nulos**: Métodos para tratar con valores nulos en los DataFrames, tales como:
  - `isNotNull()` y `isNull()`: Para identificar valores nulos.
  - `fillna()` y `dropna()`: Para rellenar o eliminar valores nulos en los DataFrames.

- **Expresiones SQL y UDF**:
  - **SelectExpr**: Permite realizar expresiones más complejas en las columnas durante la selección.
  - **User Defined Functions (UDF)**: Cómo crear y usar funciones personalizadas en Spark.

### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)  **Caso Práctico: Análisis de Poblaciones en Apache Spark**

En este notebook, se utiliza un archivo CSV de poblaciones para realizar un análisis práctico de datos en Apache Spark. Los pasos incluidos son:

- **Listar archivos del DBFS (Databricks File System)**: Se utiliza el comando `dbutils.fs.ls()` para listar los archivos almacenados en el sistema de archivos.
  
- **Lectura de un archivo CSV**: Usamos Spark para leer el archivo CSV y convertirlo en un DataFrame.
  
- **Visualización de datos**: Se utiliza el método `display()` para mostrar los datos de una forma visualmente atractiva en el notebook.
  
- **Creación de una vista temporal**: Se crea una vista temporal a partir del DataFrame para poder consultar los datos utilizando SQL.
  
- **Consultas SQL**: Se ejecutan consultas SQL sobre la vista temporal para obtener información como las medias poblacionales por regiones.
  
- **Gráficos**: Finalmente, se crea un gráfico utilizando Spark para visualizar las tendencias de las poblaciones.


