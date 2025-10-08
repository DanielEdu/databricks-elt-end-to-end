# Databricks-elt-end-to-end

Bienvenido/a 👋

Este proyecto end to end está diseñado para evaluar tus habilidades de integración de datos utilizando servicios de Azure y Databricks. Lee detenidamente cada paso y completa cada punto solicitado.

¡Buena suerte! 🚀

---

## 🎯 Objetivo General

Un cliente necesita construir un **registro maestro de personas** unificando datos desde diversas fuentes y cargarlos en **Cosmos DB** con una estructura JSON por cada `id_persona`.

---

## 🧱 Arquitectura Propuesta

El flujo a implementar incluye:

1. **Azure SQL Server** ➝  
2. **Azure Data Factory (ADF)** para ingesta ➝  
3. **Blob Storage (landing)** ➝  
4. **Databricks** para transformación ➝  
5. **Cosmos DB** como destino final

---

## ✅ Actividades a Realizar

### 1. 🏗 Ingestar datos desde Azure SQL a Azure Storage

- Usar **Azure Data Factory**
- Ingestar las tablas:  
  - `datos_residencia`  
  - `datos_personales`
- Guardar en el contenedor `landing` en formato **Parquet** con compresión **Snappy**
- Cada tabla debe estar en su carpeta correspondiente

---

### 2. 🧪 Leer y transformar en Databricks

- Leer con un notebook de Databricks las tablas:  
  - `datos_residencia`  
  - `datos_personales`  
  - `tipos_identificacion`  
  - `generos`  
  - `municipios`
- Realizar los joins necesarios para consolidar un único registro por persona

---

### 3. 📤 Escribir en Cosmos DB

- Escribir el resultado en Cosmos DB desde el mismo notebook
- Usar `md5(id_persona)` para el campo `id`
- Asegurar que no se dupliquen registros si ya existe un `id` en Cosmos

---

### 4. ⏰ Orquestación

- Crear un workflow en Databricks que se ejecute **cada día de la semana**

---

## 🧠 Recomendación

Antes de empezar, revisa el diseño de la estructura JSON esperada por `id_persona`, valida los tipos de datos y realiza pruebas locales en pequeños batches para validar el pipeline completo.

---

## 🎥 Video Tutorial

A continuación puedes ver el video explicativo de este proyecto:

[![Ver en YouTube](https://img.youtube.com/vi/ac1Soqb6puE/0.jpg)](https://www.youtube.com/watch?v=ac1Soqb6puE&t=16s)

---

## 🤝 Conecta conmigo

### Daniel Datashow

- 🌐 [LinkedIn](https://www.linkedin.com/in/danielportugalr/)
- 📹 [YouTube](https://www.youtube.com/@danielportugalr)
- 🎵 [TikTok](https://www.tiktok.com/@danielportugalr)
