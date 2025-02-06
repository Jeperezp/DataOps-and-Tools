# **Configuración de Kafka, Zookeeper, MySQL y Debezium**

## **Descripción de los Servicios y Conceptos**

### **Zookeeper**

- **Zookeeper** es una herramienta de coordinación de servicios distribuida. En este caso, se utiliza para coordinar la comunicación entre los nodos de Kafka. Zookeeper es esencial para el funcionamiento de Kafka, ya que gestiona los metadatos del clúster, la gestión de temas, las particiones y la configuración de los brokers.
  
  **En el archivo `docker-compose.yml`, el contenedor de Zookeeper está configurado para escuchar en el puerto `2181`.**

### **Kafka**

- **Kafka** es un sistema de mensajería distribuido y de alto rendimiento que se utiliza para manejar flujos de datos en tiempo real. Kafka organiza los mensajes en **topics** (temas) y los distribuye entre los productores y consumidores en un clúster de brokers.

  En este archivo, Kafka está configurado para trabajar con Zookeeper. La comunicación se hace a través del puerto `9092`, y Kafka mantiene la configuración del **offset** (administración de los registros consumidos).

---

## **Configuración de Debezium**

### **Debezium**

- **Debezium** es una plataforma de captura de datos en tiempo real (CDC) que se utiliza para replicar cambios de bases de datos hacia Kafka. Debezium se conecta a bases de datos como MySQL, PostgreSQL, etc., y convierte los cambios en un flujo de eventos en Kafka.

  En este archivo, **Debezium** está configurado para conectarse a **Kafka** (en el puerto `9092`) y a **MySQL** (en el puerto `3307`). Debezium capturará los cambios realizados en las tablas de la base de datos de MySQL y los transmitirá a Kafka.

  El contenedor de **Debezium** está configurado con el puerto `8083`, que es donde se encuentra su API REST para la gestión de conectores.

---

# **Docker Compose - Kafka, Zookeeper, MySQL y Debezium**

Este proyecto utiliza Docker Compose para levantar una infraestructura de **Kafka**, **Zookeeper**, **MySQL** y **Debezium**. Es útil para implementaciones de captura de datos en tiempo real, como la integración de bases de datos con Kafka utilizando Debezium.

### **Servicios definidos en el archivo `docker-compose.yml`:**

- **Zookeeper**  
- **Kafka**  
- **MySQL**  
- **Debezium**

---

## **Requisitos Previos**

- Docker y Docker Compose instalados.
- Conexión a Internet (para descargar las imágenes de Docker).

---

## **Instrucciones de Uso**

### 1. **Clonar el repositorio o crear los archivos necesarios.**

   Si aún no tienes los archivos `docker-compose.yml` y `register.json`, crea un directorio en tu máquina y coloca el archivo `docker-compose.yml` proporcionado en dicho directorio.

### 2. **Iniciar los contenedores.**

   Para iniciar los contenedores definidos en el archivo `docker-compose.yml`, ejecuta el siguiente comando:

   ```bash
   docker-compose up
   ```

   Esto descargará las imágenes necesarias (si no están presentes en tu máquina) y pondrá en marcha los contenedores de Zookeeper, Kafka, MySQL y Debezium.

### 3. **Detener los contenedores.**

   Si deseas detener los contenedores, puedes usar:

   ```bash
   docker-compose down
   ```

### 4. **Verificar los servicios.**

   - **Zookeeper** está disponible en el puerto `2181`.
   - **Kafka** está disponible en el puerto `9092`.
   - **MySQL** está disponible en el puerto `3307`.
   - **Debezium** (el servicio de Kafka Connect) está disponible en el puerto `8083`.

---

### 5. **Recomendaciones**

   En el caso de que tengas instalado **MySQL** en tu dispositivo y esté en uso el puerto `3306`, se recomienda cambiar el puerto en el archivo **`docker-compose.yml`** para evitar conflictos. En este caso, se cambió al puerto `3307`.

---

### 6. **Ingreso a la Base de Datos**

   Después de levantar los servicios con Docker, si estás en Windows, debes ejecutar el siguiente comando para registrar el conector:

   ```bash
   Invoke-WebRequest -Uri "http://localhost:8083/connectors/" -Method Post -Headers @{ "Accept" = "application/json"; "Content-Type" = "application/json" } -Body (Get-Content -Raw -Path "register-mysql.json")
   ```

   En este contenedor no existe la base de datos `netflix`. Para este caso, debes ingresar a la base de datos de MySQL y crear la base de datos `netflix` y la tabla `movie`:

   ```bash
   docker exec -it mysql mysql -u root -proot
   ```

   Una vez dentro del motor de MySQL, ejecuta las siguientes consultas:

   ``` sql
   CREATE DATABASE netflix;
   USE netflix;
   
   CREATE TABLE movie (
       movie_id VARCHAR(255) PRIMARY KEY,
       title VARCHAR(255),
       release_date DATE,
       language VARCHAR(50),
       url VARCHAR(255)
   );
   
   -- Creamos el usuario 
   CREATE USER 'debezium'@'%' IDENTIFIED BY 'dbz';
   
   -- Brindamos privilegios al usuario creado
   GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
   FLUSH PRIVILEGES;
   
   GRANT ALL PRIVILEGES ON netflix.* TO 'debezium'@'%';
   FLUSH PRIVILEGES;
   
   exit;
   ```

   Después de crear el usuario y haberle brindado los permisos, ingresa nuevamente a MySQL con el usuario creado:

   ``` bash
   docker exec -it mysql mysql -u debezium -pdbz netflix
   ```

   Luego, crea un nuevo registro en la tabla `movie`:

   ```sql
   INSERT INTO movie (movie_id, title, release_date, language, url)
   VALUES ("80194187", "JonDown II03", "2019-04-11", "English", "https://www.netflix.com/pe-en/title/80194187");
   ```

   En otra ventana de cmd, ejecuta el siguiente comando:

   ``` bash
   docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic dbserver1.inventory.customers --from-beginning
   ```

   En la ventana donde tienes iniciado el motor MySQL, ejecuta las siguientes consultas para realizar las operaciones de actualización y eliminación de registros.

   **Actualización de registro ingresado:**

   ``` sql
   UPDATE movie SET language = 'Spanish' WHERE title = 'JonDown II03';
   ```

   **Eliminación de registro creado:**

   ``` sql
   DELETE FROM movie WHERE title = 'JonDown II03';
   ```

   En la segunda ventana, ejecuta el siguiente comando para ver los cambios realizados a la base de datos:

   ``` bash
   docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic dbserver1.inventory.customers --from-beginning
   ```

   Allí podrás observar los cambios realizados en tu base de datos.

---

## **Conclusión**

Este proyecto demuestra cómo configurar una infraestructura con **Kafka**, **Zookeeper**, **MySQL** y **Debezium** utilizando **Docker Compose** para la captura de datos en tiempo real mediante la replicación de bases de datos hacia Kafka. Esto permite integrar sistemas de bases de datos con Kafka de forma eficiente y en tiempo real, lo que es útil para aplicaciones que requieren un procesamiento rápido y escalable de grandes volúmenes de datos.
