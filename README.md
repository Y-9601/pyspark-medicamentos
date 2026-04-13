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
**Carga de datos desde HDFS**  
  El sistema lee el archivo CSV directamente desde HDFS, permitiendo trabajar con datos almacenados de forma distribuida.

**Exploración del dataset**  
  Se analiza la estructura de los datos para entender qué información contiene cada columna y cómo está organizada.

**Vista previa de la información**  
  Se muestran algunos registros clave del dataset para tener una idea clara del contenido antes de analizarlo.

**Análisis estadístico**  
  Se generan estadísticas básicas como promedios, valores mínimos y máximos para comprender mejor los datos.

**Identificación de precios altos**  
  Se filtran los medicamentos cuyo precio por tableta supera los 5000, con el fin de detectar valores elevados.

**Ordenamiento por precio**  
  Los datos se organizan de mayor a menor precio para identificar fácilmente los medicamentos más costosos.

**Validación de datos**  
  Se verifica que las columnas necesarias existan antes de realizar el análisis, evitando errores durante la ejecución.

**Mejora en la visualización**  
  Se seleccionan columnas específicas para presentar los resultados de forma más clara y ordenada.

## Resultados esperados
- Visualización del esquema del dataset
- Estadísticas descriptivas de los datos
- Listado de medicamentos con precios altos
- Ranking de medicamentos más costosos

## Interpretación de resultados

El análisis permitió identificar medicamentos con precios significativamente altos, lo que puede indicar:
- Diferencias entre fabricantes
- Medicamentos especializados
- Posibles oportunidades de optimización de costos
Este tipo de análisis es clave en entornos reales para la toma de decisiones en el sector salud.

## Conclusión
El uso de PySpark permite procesar grandes volúmenes de datos de manera eficiente, facilitando el análisis de información en entornos distribuidos y demostrando su aplicabilidad en problemas reales del mundo empresarial
