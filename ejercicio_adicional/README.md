# Ejercicio 1 - Airflow + MinIO + Trino

## Objetivo

Genera un nuevo DAG utilizando la información (dataset) disponible a tu elección en https://ecobici.cdmx.gob.mx/en/open-data/ 

## Arquitectura

- **Airflow**: orquestación del DAG `etl_engineer_challenge`.
- **MinIO**: almacenamiento tipo S3 para el Data Lake.
- **Trino**: motor SQL para consultar el archivo Parquet.
- **Hive Metastore**: catálogo de metadatos usado por Trino.
- **Polars**: procesamiento, limpieza, transformación y agregaciones.



## PREGUNTAS

1. ¿Qué dataset se seleccionó para tu flujo?
    ```text
        Se selecciono el Dataset station_status del feed abierto GBFS de Ecobici CDMX.Dicho dataset contiene informacion sobre:
            --> Disponibilidad de bicicletas.
            --> Disponibilidad de docks.
            --> Estado operativo de las estaciones.
            --> Timestamps de actualización.
    ```





2. ¿Qué temporalidad se realizará la extracción? Explica por qué se seleccionó este timing.
   ```text
        Se eligió la temporalidad de 5 minutos porque:

            --> El dataset cambia constantemente.
            --> Es información operacional cercana a tiempo real.
            --> Evita sobrecargar el API.
            --> Mantiene información suficientemente fresca para análisis y monitoreo.
            --> Es un balance adecuado entre latencia y costo computacional.

            Además, 5 minutos es una frecuencia común en pipelines near real-time.
    ```

3. ¿Qué limpieza de datos usaste o crees que necesitaba los datos?
    ```text
        --> Limpieza
        --> Eliminación de nulos
        --> Conversión de tipos	Homologar datos
        --> Conversión de timestamps
        --> Eliminación de duplicados
        --> Validación de negativos
        --> Agregado de metadata
     ```

4. ¿Qué propuesta de partición de ruta elegiste para el guardado de tu parquet y crees que esta partición afecta a Trino para su          disponibilización automática de datos?
    ```text
        Utilice los campos de ingestion_date,ingestion_hour (campos que decidi agregar para contorl), esto ayuda positivamente a Trino para que:
            --> Lea menos archivos.
            --> Haga partition pruning.
            --> Mejore performance.
            --> Reduzca I/O.
            --> Acelere consultas por fecha/hora.
     ```
