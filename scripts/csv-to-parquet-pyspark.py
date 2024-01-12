from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from spark_details import spark_session, read_csv


def apply_rows_based_window_function(df):
    
    # Define uma janela baseada em número de linhas
    windowSpec = Window.partitionBy("title").rowsBetween(-15, 15)

    # Conta o número de livro enviadas em cada janela
    return df.withColumn("books_count", count("title").over(windowSpec))


def main():
    spark = spark_session("CSV_to_Parquet_with_Window_Function")

    csv_file_path = "/local/do/arquivo-csv/datasource-books.csv"
    df = read_csv(spark, csv_file_path)

    print("Qtd livros antes de filtrar:", df.count())

    # Utilizando filtro  
    df_filter = df \
        .filter(col("ratings_count").isNotNull()) \
        .filter(col("average_rating") > 4) \
        .filter(col("subtitle").isNotNull()) \
        .filter(col("num_pages") > 300) \
            

    print("Qtd livros depois de filtrar:", df_filter.count())

    # Aplica a função Window com base no tempo
    df_with_window = apply_rows_based_window_function(df_filter)
    
    # Ordena o resultado pelo titulo
    df_with_window = df_with_window.orderBy("title")

    # Mostra o resultado
    df_with_window.select("isbn13","title","subtitle","authors","num_pages","ratings_count","average_rating").show()

    # Caminho para armazenar o arquivo Parquet
    parquet_output_path = "/local/do/arquivo-parquet/book_outputs"

    # Escreve os dados no formato Parquet
    df_with_window.write.mode('overwrite').parquet(parquet_output_path)
    

    # Encerra a sessão Spark
    spark.stop()


if __name__ == "__main__":
    main()
