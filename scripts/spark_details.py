from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType, FloatType)

# criando uma sessão com spark 
def spark_session(app_name="SparkApplication"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Estrutura para leitura do arquivo csv
def read_csv(spark, file_path):
    schema = StructType([
        StructField("isbn13", IntegerType()),
        StructField("isbn10", StringType()),
        StructField("title", StringType()),
        StructField("subtitle", StringType()),
        StructField("authors", StringType()),
        StructField("categories", StringType()),
        StructField("thumbnail", StringType()),
        StructField("description", StringType()),
        StructField("published_year", IntegerType()),
        StructField("average_rating", FloatType()),
        StructField("num_pages", IntegerType()),
        StructField("ratings_count", IntegerType())
    ])
    return spark.read.csv(file_path, header=True, schema=schema)
