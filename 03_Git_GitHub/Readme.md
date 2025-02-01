# Guía Básica de Git y GitHub

## 1. ¿Qué es Git?

**Git** es un sistema de control de versiones distribuido que permite gestionar el historial de versiones de un proyecto. Es utilizado principalmente para desarrollo de software, pero también puede ser útil en otros tipos de proyectos que requieran un seguimiento detallado de cambios.

Git permite:

- Llevar un registro de los cambios realizados en los archivos de un proyecto.
- Colaborar de forma eficiente con otros desarrolladores.
- Facilitar la reversión de cambios no deseados.

## 2. ¿Qué es GitHub?

**GitHub** es una plataforma basada en la web que ofrece alojamiento para repositorios Git. Permite a los desarrolladores colaborar en proyectos de código abierto o privado, gestionar el código fuente, realizar revisiones de código y mucho más.

## 3. Instalación de Git

### En Windows

1. Descarga el instalador desde [git-scm.com](https://git-scm.com/).
2. Ejecute el instalador y sigue las instrucciones predeterminadas. Asegúrate de seleccionar la opción para agregar Git a la variable de entorno PATH durante la instalación.
   

## 4. Configuración Inicial de Git

Una vez instalado Git, configura tu información de usuario para que los cambios y commits estén asociados correctamente.

1. Configura tu nombre de usuario:
   
```git
   git config --global user.name "Tu Nombre"
```

2. Configura tu correo electrónico:
   
```
   git config --global user.email "tu@correo.com"
```

Puedes verificar la configuración con:

```
   git config --list
```

## 5. Comandos Básicos de Git

### Crear un Repositorio

1. Para crear un nuevo repositorio en Git, navega a la carpeta de tu proyecto y ejecuta:
   
```bash
   git init
```

2. Si deseas clonar un repositorio existente, usa:
   
```bash
   git clone https://github.com/usuario/repositorio.git
```

### Verificar el estado del repositorio

Para ver los archivos modificados, agregados o eliminados, usa:
   
```bash
   git status
```

### Añadir archivos al área de preparación (staging)

Para agregar archivos específicos al área de preparación, usa:
   
```bash
   git add nombre_del_archivo
```
   
Para agregar todos los archivos modificados:
   
```bash
   git add .
```

### Realizar un commit

Una vez que hayas añadido los archivos al área de preparación, puedes hacer un commit:

```bash
   git commit -m "Mensaje de commit"
```
Es una buena práctica escribir mensajes de commit claros, descriptivos y estructurados, ya que esto facilita la comprensión de los cambios que se han realizado, tanto para ti como para otros miembros del equipo. Un buen mensaje de commit debe contener dos partes: un título breve y una descripción detallada.

- **Título**: Un título conciso que resuma el cambio realizado (generalmente, no más de 50 caracteres).
- **Descripción**: Una descripción más extensa que explique el "por qué" detrás del cambio y cualquier detalle relevante, como el impacto o los problemas que resuelve. Esta sección puede extenderse si es necesario.

Ejemplo de cómo realizar un commit con título y descripción:
```bash
git commit -m "Título breve del commit" -m "Descripción detallada que explica el propósito del commit, por qué se hicieron esos cambios y cómo afecta al proyecto."
```
El título debe ir en tiempo presente e indicativo, describiendo de manera precisa el cambio, como si dijeras "Agrega funcionalidad X" o "Arregla bug Y". La descripción debe proporcionar contexto adicional, como qué archivos fueron modificados, qué problemas se resolvieron o qué funcionalidades se añadieron.

Ejemplo
```bash
git commit -m "Corrige error de visualización en el panel de usuario" -m "Se ajustó el CSS para corregir el desbordamiento del contenido en pantallas pequeñas. Se probaron diferentes resoluciones para asegurar que la interfaz se vea correctamente en dispositivos móviles."
```
Siguiendo esta estructura, tus mensajes de commit serán mucho más fáciles de entender y mantendrán un historial claro y útil para todo el equipo de desarrollo.

### Ver el historial de commits

Para ver el historial de commits del repositorio, usa:

```bash
   git log
```

### Crear una nueva rama

Para crear una nueva rama y cambiarte a ella, usa:

```bash
   git checkout -b nombre_de_rama
```

### Fusionar ramas

Para fusionar cambios de una rama a otra, primero cambia a la rama a la que deseas fusionar (por ejemplo, `main`):

```bash
   git checkout main
```

Luego, fusiona la rama que deseas integrar:

```bash
   git merge nombre_de_rama
```

## 6. Comandos Básicos de GitHub

### Subir un Repositorio Local a GitHub

1. Crea un repositorio en GitHub.
2. Para vincular tu repositorio local con el de GitHub, usa:

```bash
   git remote add origin https://github.com/usuario/repositorio.git
```
3. Para subir tus cambios locales a GitHub, usa:

```bash
   git push -u origin main
```

### Clonar un Repositorio de GitHub

Para clonar un repositorio de GitHub, usa:

```bash
   git clone https://github.com/usuario/repositorio.git
```

### Traer los últimos cambios desde GitHub

Para actualizar tu repositorio local con los cambios más recientes de GitHub, usa:

 ```bash
   git pull origin main
```

# Errores Comunes al Hacer Commits en Git y Cómo Revertirlos

Aunque Git es una herramienta poderosa, hay algunos errores comunes que los desarrolladores cometen al hacer commits. Aquí te dejo algunos de esos errores y cómo revertirlos.

## 1. **Hacer un commit con el mensaje incorrecto**

Es común hacer un commit y luego darte cuenta de que el mensaje no es claro o contiene un error. 

### Solución: Cambiar el mensaje del último commit

Si solo has hecho un commit reciente y necesitas cambiar el mensaje, puedes hacerlo con el siguiente comando:

```bash
   git commit --amend -m "Nuevo mensaje de commit"
```
Esto cambiará el mensaje del último commit. **Nota**: Si ya has subido el commit a un repositorio remoto, deberías tener cuidado al usar este comando, ya que puede causar conflictos.

## 2. **Hacer un commit con cambios incorrectos o no deseados**

Es posible que hayas añadido archivos al área de preparación (`git add`) y luego te des cuenta de que cometiste un error al incluir un archivo incorrecto.

### Solución: Eliminar archivos del área de preparación

Para quitar un archivo específico del área de preparación, usa:

```bash
   git reset nombre_del_archivo
```

Si deseas quitar todos los archivos del área de preparación, puedes usar:

```bash
   git reset
```

Esto no elimina los archivos del sistema, solo los quita del área de preparación.


## 3. **Hacer un commit en la rama equivocada**

A veces puedes cometer el error de hacer un commit en la rama incorrecta. Para solucionarlo, puedes cambiar los commits a la rama correcta.

### Solución: Mover el commit a la rama correcta

1. Cambia a la rama correcta donde deseas aplicar el commit:

```bash
   git checkout rama_correcta
```

2. Usa el comando `git cherry-pick` para aplicar el commit desde la otra rama:

```bash
   git cherry-pick id_del_commit
```

Esto aplicará el commit de la otra rama en la rama correcta.


## 4. **Deshacer un commit y mantener los cambios en el área de trabajo**

A veces quieres deshacer un commit pero mantener los cambios en tu área de trabajo para poder corregirlos.

### Solución: Deshacer el commit pero mantener los cambios

Puedes deshacer el commit pero mantener los archivos modificados en tu área de trabajo con:

```bash
   git reset --soft HEAD~1
```

Este comando deshace el último commit y deja los archivos modificados en el área de trabajo (sin cambios).



## 5. **Deshacer un commit y eliminar los cambios del área de trabajo**

Si deseas deshacer un commit y también eliminar los cambios realizados en el área de trabajo, puedes usar:

```bash
   git reset --hard HEAD~1
```

**Advertencia**: Este comando eliminará los cambios no guardados, así que asegúrate de no perder trabajo importante.

# Caso de Uso: Trabajando con Ramas y Conceptos Intermedios de Git

A continuación, te mostraré un caso práctico donde usarás ramas, fusiones (merges) y resolución de conflictos. Este ejemplo cubre tanto aspectos básicos como intermedios de Git.

### Contexto del Caso de Uso

Supón que estás trabajando en un proyecto llamado "MiProyecto" y deseas implementar una nueva funcionalidad en una rama separada. Al mismo tiempo, otro compañero está trabajando en una rama diferente. Más tarde, ambos desean fusionar sus cambios en la rama principal (`main`).

### 1. **Crear una nueva rama para tu funcionalidad**

Primero, creas una rama para trabajar en una nueva funcionalidad, por ejemplo, "nueva-funcionalidad":

```bash
   git checkout -b nueva-funcionalidad
```

Ahora, estás trabajando en esta rama y puedes hacer tus cambios sin afectar la rama `main`.

### 2. **Hacer algunos commits en tu rama**

Realizas varios cambios y haces commits a medida que avanzas:

```bash
   git add .
   git commit -m "funcionalidad" -m "Agregada nueva funcionalidad"
```

### 3. **Ver el historial de commits en tu rama**

Para ver el historial de commits en tu rama actual:

```bash
   git log
```

### 4. **Cambiar a la rama principal (main) y actualizarla**

Antes de fusionar tu rama con la rama `main`, asegúrate de que la rama `main` esté actualizada:

```bash
   git checkout main
   git pull origin main
```

### 5. **Fusionar tu rama con la rama principal (main)**

Ahora, fusionas la rama `nueva-funcionalidad` con la rama `main`:

```bash
   git checkout main
   git merge nueva-funcionalidad
```

Si no hay conflictos, los cambios de tu rama se integrarán automáticamente en `main`.

### 6. **Resolver Conflictos de Merge**

Si hay conflictos (por ejemplo, dos cambios en el mismo archivo), Git te indicará qué archivos están en conflicto. Abre esos archivos y resuelve los conflictos manualmente.

Una vez resueltos, añade los archivos modificados al área de preparación y completa la fusión:

```bash
   git add archivo_conflictivo
   git commit
```

### 7. **Subir los cambios a GitHub**

Finalmente, una vez que todo esté fusionado, subes los cambios a GitHub:

```bash
   git push origin main
```

### Trabaja con ramas

- **Usa ramas para nuevas funcionalidades**: Cada nueva característica o corrección debe desarrollarse en su propia rama. Esto facilita la colaboración, el control de versiones y la organización.
  
- **Fusiona frecuentemente**: Si trabajas con otros colaboradores, es buena idea fusionar tus cambios con la rama `main` o `develop` frecuentemente para evitar grandes conflictos de merge.

### Mensajes de Commit Claros

- **Escribe mensajes de commit significativos**: Un buen mensaje de commit debe ser claro y describir lo que hace ese commit. Esto es útil para ti y para cualquier otra persona que trabaje en el proyecto.


## Recursos adicionales

- [Documentación oficial de Git](https://git-scm.com/doc)
- [Documentación de GitHub](https://docs.github.com/es/github)
