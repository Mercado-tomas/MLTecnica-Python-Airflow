# Pipeline en Airflow con Python-PostgreSql

Este proyecto implementa un pipeline de datos automatizado usando **Apache Airflow** para scrapear información de productos de Mercado Libre, 
almacenarla en una base de datos PostgreSQL y enviar alertas por email basadas en criterios específicos. 
Fue desarrollado como parte de un desafío técnico de data engineering encontrado en la web.

## Descripción del desafío
El objetivo fue crear un pipeline que automatice:
1. **Scrapear datos**: Obtener los 50 ítems más relevantes de la categoría "Microondas" (ID: MLA1577) desde la API pública de Mercado Libre.
2. **Almacenar datos**: Guardar los datos (`id`, `site_id`, `title`, `price`, `sold_quantity`, `thumbnail`, `create_date`) en una base de datos PostgreSQL.
3. **Enviar alertas**: Chequear si algún ítem supera los $7.000.000 en ganancias (`price * sold_quantity`) y enviar un email con los resultados.
4. **Automatización**: Orquestar el pipeline con Airflow para que corra diariamente.

## Implementación
El proyecto usa **Docker** para levantar Airflow y PostgreSQL, y está estructurado en tres tareas principales dentro de un DAG:

### Estructura del pipeline
- **DAG**: `mercadolibre_técnica`
- **Tareas**:
  1. **`scrap_to_csv_task`**:
     - Realiza un request a `https://api.mercadolibre.com/sites/MLA/search?category=MLA1577`.
     - Extrae los 50 ítems más relevantes y los guarda en un CSV (`data/items.csv`).
     - Nota: La API no devuelve `sold_quantity` por defecto, por lo que se setea un valor fijo (20) como prueba.
  2. **`csv_to_db_task`**:
     - Lee el CSV y sube los datos a una tabla `mercadolibre_items` en PostgreSQL (schema `public`).
     - Usa `ON CONFLICT` para actualizar ítems existentes.
  3. **`email_alert_task`**:
     - Consulta la DB para encontrar ítems con `price * sold_quantity > 7000000`.
     - Envía un email con los resultados usando SMTP (Gmail) y credenciales seguras almacenadas en `.env`.

### Tecnologías utilizadas
- **Apache Airflow**: Orquestación del pipeline (`schedule_interval="@daily"`).
- **Docker**: Contenedores para Airflow y PostgreSQL (`docker-compose.yml`).
- **PostgreSQL**: Almacenamiento de datos.
- **Python**: Lógica del pipeline con librerías como `requests`, `psycopg2`, `smtplib`, y `python-dotenv`.
- **Mercado Libre API**: Fuente de datos pública.

## Instalación y uso
1. **Requisitos previos**:
   - Docker y Docker Compose instalados.
   - Python 3.11 con entorno virtual(airflow soporte versiones de python3.8 hasta 3.11).
   - Postman para consultar datos de la api
   - Puertos habilitados, para levantar Postgre y Airflow(5432 - 8080)
   - Instalar Airflow en entorno virtual
   - Instalar paquetes de python necesarios(dotenv, request, psycopg2, etc)
  

