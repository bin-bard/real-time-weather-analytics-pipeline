# vmware_spark_jobs/01_kafka_to_bronze_api.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    KAFKA_BOOTSTRAP_SERVERS = "192.168.64.1:9092" 
    KAFKA_TOPIC = "test" 

    HDFS_NAMENODE = "hdfs://thanhtai-master:9000"
    BASE_HDFS_PATH = f"{HDFS_NAMENODE}/user/thanhtai/delta" # Đảm bảo user 'thanhtai' đúng

    BRONZE_API_PATH = f"{BASE_HDFS_PATH}/bronze/from_api"
    CHECKPOINT_API_PATH = f"{BASE_HDFS_PATH}/_checkpoints/bronze_from_api" # Checkpoint cho stream này
    APP_NAME = "VMWare_KafkaToBronzeAPI_Continuous"

    weather_data_schema_from_producer = StructType([
        StructField("time", StringType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("feelslike", DoubleType(), True),
        StructField("wind", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("gust", DoubleType(), True),
        StructField("cloud", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("weather", StringType(), True),
        StructField("label", StringType(), True)
    ])

    spark = SparkSession.builder.appName(APP_NAME) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    print("SparkSession (VMWare Streaming) đã được khởi tạo.")

    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = raw_kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .withColumn("data", from_json(col("json_string"), weather_data_schema_from_producer)) \
        .select("data.*")
    
    print(f"Đang ghi stream liên tục vào Bronze API Table (HDFS): {BRONZE_API_PATH}")
    streaming_query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_API_PATH) \
        .option("mergeSchema", "true") \
        .start(BRONZE_API_PATH)

    print(f"Streaming query ID: {streaming_query.id} đã bắt đầu và đang chạy liên tục.")
    print("Nhấn Ctrl+C trong terminal chạy spark-submit để dừng job này.")
    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        print("Đang dừng streaming query (API to Bronze) do KeyboardInterrupt...")
    finally:
        # Không stop spark session ở đây nếu bạn muốn query chạy ngầm
        # Spark job sẽ tự dọn dẹp khi bị cancel hoặc kết thúc
        print("Streaming query (API to Bronze) đã được yêu cầu dừng.")


if __name__ == "__main__":
    main()