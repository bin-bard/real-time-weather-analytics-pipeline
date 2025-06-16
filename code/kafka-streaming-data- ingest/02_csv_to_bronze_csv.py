# vmware_spark_jobs/02_csv_to_bronze_csv.py
from pyspark.sql import SparkSession

def main():
    HDFS_NAMENODE = "hdfs://thanhtai-master:9000"
    BASE_HDFS_PATH = f"{HDFS_NAMENODE}/user/thanhtai/delta"

    CSV_INPUT_HDFS_PATH = f"{HDFS_NAMENODE}/user/thanhtai/sample_data/weather_data_from_source.csv"
    BRONZE_CSV_PATH = f"{BASE_HDFS_PATH}/bronze/from_csv"
    APP_NAME = "VMWare_CSVToBronzeCSV_InferSchema_Batch"

    spark = SparkSession.builder.appName(APP_NAME) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    print("SparkSession (VMWare - CSV to Bronze Batch with Inferred Schema) đã được khởi tạo.")

    try:
        print(f"Đọc dữ liệu từ file CSV trên HDFS (sẽ suy luận schema): {CSV_INPUT_HDFS_PATH}")
        df_csv = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(CSV_INPUT_HDFS_PATH)

        if df_csv.count() == 0:
            print(f"Không đọc được dữ liệu hoặc file CSV rỗng: {CSV_INPUT_HDFS_PATH}")
            spark.stop()
            return

        print("Schema của dữ liệu CSV sau khi Spark suy luận:")
        df_csv.printSchema()
        df_csv.show(5, truncate=False)

        print(f"Đang ghi dữ liệu CSV (với schema đã suy luận) vào Bronze CSV Table (HDFS): {BRONZE_CSV_PATH}")
        df_csv.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(BRONZE_CSV_PATH)

        print(f"Ghi dữ liệu CSV vào Bronze (HDFS) thành công. Số dòng: {df_csv.count()}")
        print(f"Kiểm tra dữ liệu đã ghi tại {BRONZE_CSV_PATH}:")
        spark.read.format("delta").load(BRONZE_CSV_PATH).show(5, truncate=False)

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý CSV to Bronze: {e}")
    finally:
        spark.stop()
        print("SparkSession (VMWare - CSV to Bronze Batch with Inferred Schema) đã được dừng.")

if __name__ == "__main__":
    main()