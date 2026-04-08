## Procesamiento Batch de Medicamentos con PySpark
Este proyecto implementa un procesamiento batch utilizando PySpark para analizar un conjunto de datos de medicamentos almacenado en HDFS.  El objetivo es explorar la estructura de los datos, generar estadísticas y detectar medicamentos con precios muy altos

## Fuente de datos
hdfs://localhost:9000/Medicamentos/3t73-n4q9.csv.2

## Tecnologías utilizadas
- Python
- PySpark
- Hadoop HDFS

## Ejecución
- Iniciar Hadoop y Spark
- Ejecutar: python main.py

## Funcionalidades
Carga de datos desde HDFS: Lectura de archivos CSV almacenados en el sistema distribuido HDFS con inferencia automática de esquema.

- 🧩 **Exploración de la estructura de datos**  
  Visualización del esquema del dataset para identificar tipos de datos y columnas disponibles.

- 👀 **Vista previa de datos relevantes**  
  Selección de columnas clave (`principio_activo`, `fabricante`, `precio_por_tableta`) para un análisis más claro.

- 📊 **Análisis estadístico descriptivo**  
  Generación de métricas como promedio, mínimo, máximo y conteo para comprender el comportamiento del dataset.

- 🚨 **Detección de medicamentos con precios elevados**  
  Filtrado de registros donde el precio por tableta supera un umbral definido (5000).

- 📈 **Ordenamiento de datos por precio**  
  Organización de los medicamentos de mayor a menor precio para identificar los más costosos.

- ⚠️ **Validación de columnas**  
  Verificación de la existencia de columnas antes de realizar operaciones para evitar errores en ejecución.

- 🧹 **Optimización de visualización de resultados**  
  Uso de selección de columnas y formato de salida para mejorar la legibilidad de los datos.


## Resultados esperados
- Visualización del esquema del dataset
- Estadísticas descriptivas de los datos
- Listado de medicamentos con precios altos
- Ranking de medicamentos más costosos

## Conclusión
El uso de PySpark permite procesar grandes volúmenes de datos de manera eficiente, facilitando el análisis de información en entornos distribuidos y demostrando su aplicabilidad en problemas reales del mundo empresarial
