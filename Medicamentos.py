from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark 
spark = SparkSession.builder.appName('Medicamentos').getOrCreate()

# Ruta exacta de tu HDFS
file_path = 'hdfs://localhost:9000/Medicamentos/3t73-n4q9.csv.2'

# Lee el archivo CSV
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# 1. Estructura de datos
print("\n--- ESTRUCTURA DE DATOS ---")
df.printSchema()

# 2. Vista previa limpia (seleccionando columnas clave para que no se desordene)
print("\n--- VISTA PREVIA ALINEADA ---")
df.select('principio_activo', 'fabricante', 'precio_por_tableta').show(10, truncate=False)

# 3. Resumen Estadístico (Usa vertical=True para que se vea ordenado)
print("\n--- RESUMEN ESTADÍSTICO ---")
df.summary().show(vertical=True)

# 4. Filtro corregido (Sin la 's' extra)
if 'precio_por_tableta' in df.columns:
    print("\n--- REGISTROS CON PRECIO MAYOR A 5000 ---")
    # Aquí seleccionamos las columnas que queremos ver separadas
    reporte_filtro = df.filter(F.col('precio_por_tableta') > 5000) \
                       .select('principio_activo', 'precio_por_tableta')
    reporte_filtro.show(20, truncate=False)

    print("\n--- ORDENADO POR PRECIO DESCENDENTE ---")
    # Corregido: nombre exacto de la columna y orden descendente
    sorted_df = df.select('principio_activo', 'fabricante', 'precio_por_tableta') \
                  .sort(F.col("precio_por_tableta").desc())
    sorted_df.show(10, truncate=False)
else:
    print("\n[!] Error: No se encontró la columna 'precio_por_tableta'")

spark.stop()