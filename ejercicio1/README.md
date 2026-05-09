# Ejercicio 1 - Airflow + MinIO + Trino

## Objetivo

Implementar un pipeline ETL con Airflow que ingesta un CSV desde MinIO, limpia y transforma los datos usando Polars, guarda el resultado en Parquet dentro de la capa Bronze y habilita la consulta SQL desde Trino.

## Arquitectura

- **Airflow**: orquestación del DAG `etl_engineer_challenge`.
- **MinIO**: almacenamiento tipo S3 para el Data Lake.
- **Trino**: motor SQL para consultar el archivo Parquet.
- **Hive Metastore**: catálogo de metadatos usado por Trino.
- **Polars**: procesamiento, limpieza, transformación y agregaciones.

## Estructura del Data Lake

```text
bck-landing/
└── data/
    └── data_prueba_tecnica.csv

bck-bronze/
└── master/
    └── data_prueba_tecnica.parquet
```

## PREGUNTAS

1. Para los ids nulos ¿Qué sugieres hacer con ellos ?

Al ser un ID de Usuario creo que la primera accion seria validar con negocio la importancia de dicho ID para la trazabilidad de la informacion que se esta procesando, dependiendo de la respuesta se podrian tomar algunas de las siguientes acciones:

    --Validar si el registro puede recuperarse desde otra fuente.
    --Si el id es obligatorio para trazabilidad o unicidad, mover el registro a una zona de cuarentena/rechazados.



2. Considerando las columnas name y company_id ¿Qué inconsistencias notas y como las mitigas?
   ```text
    name
    -->INCONSISTENCIA
        --Para el campo name se identifican diferencias de formato entre mayusculas y minisculas Ej(john doe - JOHN DOE), lo cual puede generar duplicados logicos durante agrupaciones.
    --SOLUCION
        -- Limpieza de espacios, conversion a lowercase y estandarizacion de formato
    company_id
    -->INCONSISTENCIA
        --Diferencias de capitalizacion Ej(COMP02 - comp02), lo cual puede generar duplicados logicos durante agrupaciones.
    --SOLUCION
        -- Limpieza de espacios, conversion a lowercase y estandarizacion de formato
    ```

3. Para el resto de los campos ¿Encuentras valores atípicos y de ser así cómo procedes? 
    ```text
    created_at
        -->VALORES ATIPICOS
            --invalid-date
        --SOLUCION
            --Conversión segura a tipo datetime
            --Los registros inválidos son enviados a cuarentena o marcados como nulos para revisión posterior


    amount
        -->VALORES ATIPICOS
            --Se detectaron valores negativos, pero dependiendo del negocio esto podria significar algo(Reverso,Devolucion,etc)
        --SOLUCION
            --Validación de reglas de negocio
            --Monitoreo de valores fuera de rango
     ```

4. ¿Qué mejoras propondrías a tu proceso ETL para siguientes versiones?
    ```text
    --EVOLUCION DE LA ARQUITECTURA
      --Implementar arquitectura Medallion completa (RAW,BRONCE,SILVER,GOLD)
      --Particionamiento del parquet por fecha
      --Procesamiento incremental

    --CALIDAD DEL DATO
        --null checks
        --Deteccion de duplicados
        --Validaciones por rango
    
    --TRAZABILIDAD Y PERFORMANCE
        --Registros procesados
        --Registros invalidos
        --Alertas automáticas
        --Tiempos de ejecucion
     ```



6. Guardar una captura de pantalla como imagen, de la query con Trino usando DBeaver 
    ```text
        --Screenshot de evidencia anexo en el proyecto
    ```
