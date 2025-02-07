# **Predicción de Procesos Problemáticos en Servidores**

Este proyecto tiene como objetivo desarrollar un modelo de clasificación para predecir si un proceso en ejecución en un servidor será problemático o no, utilizando datos históricos de los procesos. El modelo ayudará a detectar procesos potencialmente problemáticos, permitiendo tomar acciones preventivas para asegurar la estabilidad y el rendimiento del servidor.

---

## **Descripción de los Datos**

El conjunto de datos incluye las siguientes características para cada proceso en el servidor:

- **ID_Proceso**: Identificador único del proceso.
- **Uso_CPU**: Porcentaje de CPU utilizado por el proceso.
- **Uso_Memoria**: Porcentaje de memoria utilizada por el proceso.
- **Numero_Hilos**: Número de hilos del proceso.
- **Tiempo_Ejecucion**: Tiempo de ejecución del proceso en horas.
- **Numero_Errores**: Número de errores generados por el proceso en las últimas 24 horas.
- **Tipo_Proceso**: Categoría del proceso (Servicio, Aplicación, Sistema).

La variable objetivo es **Estado**, que indica si el proceso es problemático (1) o no (0), basado en el uso de recursos, número de errores, tipo de proceso, y otros factores relevantes.

---

## **Pasos del Proyecto**

### 1. **Análisis Exploratorio de Datos (EDA)**

Realizamos un análisis preliminar de los datos para entender la distribución y las características de las variables.

```python
import pandas as pd

# Cargar los datos
df_datos_procesos = pd.read_csv('path_to_data.csv')

# Visualizar las primeras filas
df_datos_procesos.head()

# Descripción de las estadísticas de las columnas numéricas
df_datos_procesos.describe()

# Comprobar valores nulos
df_datos_procesos.isnull().sum()

# Analizar la distribución de la variable objetivo
df_datos_procesos['Estado'].value_counts()
``` 

### 2. **Preprocesamiento de Datos**

#### 2.1 **Limpieza de los Datos**

Manejamos valores faltantes y eliminamos posibles duplicados.

``` python
# Eliminar valores nulos
df_datos_procesos = df_datos_procesos.dropna()

# Eliminar duplicados
df_datos_procesos = df_datos_procesos.drop_duplicates()
```

#### 2.2 **Codificación de Variables Categóricas**

Convertimos la variable categórica `Tipo_Proceso` a variables numéricas utilizando **One-Hot Encoding**.

```python
from sklearn.preprocessing import OneHotEncoder

# OneHotEncoding para la columna 'Tipo_Proceso'
encoder = OneHotEncoder(drop='first', sparse=False)
tipo_proceso_encoded = encoder.fit_transform(df_datos_procesos[['Tipo_Proceso']])

# Convertir a DataFrame y agregar al dataset
tipo_proceso_encoded_df = pd.DataFrame(tipo_proceso_encoded, columns=encoder.get_feature_names_out(['Tipo_Proceso']))
df_datos_procesos = df_datos_procesos.join(tipo_proceso_encoded_df)
```

#### 2.3 **Escalado de Características**

Normalizamos las características numéricas como `Uso_CPU`, `Uso_Memoria`, etc., para que tengan el mismo rango de valores.

```python
from sklearn.preprocessing import StandardScaler

# Seleccionar columnas numéricas
features = ['Uso_CPU', 'Uso_Memoria', 'Numero_Hilos', 'Tiempo_Ejecucion', 'Numero_Errores']

# Aplicar StandardScaler
scaler = StandardScaler()
df_datos_procesos[features] = scaler.fit_transform(df_datos_procesos[features])
```

### 3. **Selección y División del Conjunto de Datos**

Dividimos el conjunto de datos en entrenamiento y prueba.

```python
from sklearn.model_selection import train_test_split

# Dividir los datos en entrenamiento y prueba (80% - 20%)
X = df_datos_procesos.drop('Estado', axis=1)
y = df_datos_procesos['Estado']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
```

### 4. **Construcción y Evaluación de Modelos**

Probamos diferentes algoritmos de clasificación, como **Regresión Logística** y **Árboles de Decisión**, y evaluamos el rendimiento de cada uno.

``` python
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import classification_report

# Entrenar el modelo de regresión logística
logreg_model = LogisticRegression(random_state=42)
logreg_model.fit(X_train, y_train)

# Realizar predicciones
y_pred_logreg = logreg_model.predict(X_test)

# Evaluar modelo
print(classification_report(y_test, y_pred_logreg))

# Entrenar el modelo de árbol de decisión
tree_model = DecisionTreeClassifier(random_state=42)
tree_model.fit(X_train, y_train)

# Realizar predicciones
y_pred_tree = tree_model.predict(X_test)

# Evaluar modelo
print(classification_report(y_test, y_pred_tree))
```

### 5. **Interpretación de Resultados y Conclusiones**

Analizamos las métricas de evaluación para entender qué modelo es el más efectivo. Se pueden considerar métricas como la precisión, el recall y la puntuación F1.

- **Precisión**: Qué tan bien el modelo predice los procesos problemáticos sin cometer errores.
- **Recall**: Qué tan bien el modelo detecta todos los procesos problemáticos.
- **Puntuación F1**: La media armónica entre precisión y recall.

Basado en estas métricas, elegimos el mejor modelo para la implementación en producción.

---

## **Reporte**

El informe debe incluir los siguientes puntos:

- Descripción de los datos y las variables.
- Procedimiento de limpieza y preprocesamiento de los datos.
- Evaluación y comparación de los modelos entrenados.
- Conclusiones sobre qué modelo es el más efectivo y cómo puede utilizarse en un entorno de producción para la detección temprana de procesos problemáticos.

---

## **Requisitos**

Este proyecto requiere las siguientes bibliotecas de Python:

- pandas
- scikit-learn
- matplotlib
- seaborn

Para instalar las dependencias, se puede utilizar el siguiente comando:

```bash
pip install pandas scikit-learn matplotlib seaborn
```