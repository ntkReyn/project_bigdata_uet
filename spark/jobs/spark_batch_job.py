from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

def run_spark_batch_job():
    print("--- Bắt đầu Spark Batch Job (Reading from MinIO + Insert 3 Tables) ---")

    spark = SparkSession.builder \
        .appName("AirlineSentimentBatchInsert") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    postgres_url = "jdbc:postgresql://postgres:5432/bigdata_db"
    props = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    # Đọc CSV từ MinIO
    df = spark.read.csv("s3a://datalake/airline_sentiment.csv", header=True, inferSchema=True)

    # Chuẩn hóa bảng gốc
    df_airline = df.select(
        col("airline").alias("airline"),
        col("airline_sentiment").alias("sentiment"),
        col("negativereason").alias("negativereason")
    )
    df_airline.write.jdbc(postgres_url, "data_airline", mode="append", properties=props)
    print(">>> Đã ghi bảng data_airline")

    # ====================================================
    # === BẢNG 2: total_sentiments (positive/negative/neutral)
    # ====================================================

    df_total_sentiments = df_airline.groupBy("airline").agg(
        count(when(col("sentiment") == "positive", True)).alias("total_positive"),
        count(when(col("sentiment") == "negative", True)).alias("total_negative"),
        count(when(col("sentiment") == "neutral", True)).alias("total_neutral")
    )

    df_total_sentiments.write.jdbc(
        postgres_url, "total_sentiments", mode="append", properties=props
    )
    print(">>> Đã ghi bảng total_sentiments")

    # ====================================================
    # === BẢNG 3: total_negativereasons
    # ====================================================

    df_total_reasons = df_airline.groupBy("negativereason").agg(
        count("*").alias("total")
    )

    df_total_reasons.write.jdbc(
        postgres_url, "total_negativereasons", mode="append", properties=props
    )
    print(">>> Đã ghi bảng total_negativereasons")

    spark.stop()
    print("--- Hoàn thành toàn bộ Spark Batch Job ---")


if __name__ == "__main__":
    run_spark_batch_job()
