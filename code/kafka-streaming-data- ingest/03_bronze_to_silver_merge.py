# vmware_spark_jobs/03_bronze_to_silver_merge.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, year as spark_year, month as spark_month, dayofmonth, concat_ws, lpad
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import time

def main():
    HDFS_NAMENODE = "hdfs://thanhtai-master:9000"
    BASE_HDFS_PATH = f"{HDFS_NAMENODE}/user/thanhtai/delta"

    BRONZE_API_PATH = f"{BASE_HDFS_PATH}/bronze/from_api"
    BRONZE_CSV_PATH = f"{BASE_HDFS_PATH}/bronze/from_csv"
    SILVER_MERGED_PATH = f"{BASE_HDFS_PATH}/silver/merged_weather"
    APP_NAME = "VMWare_BronzeToSilverMerge_Robust_Batch"

    spark = SparkSession.builder.appName(APP_NAME) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    print("SparkSession (VMWare - Silver Batch - Robust Schema Handling) đã được khởi tạo.")

    try:
        print(f"Đọc dữ liệu từ Bronze API Table: {BRONZE_API_PATH}")
        df_api_raw = spark.read.format("delta").load(BRONZE_API_PATH)

        print(f"Đọc dữ liệu từ Bronze CSV Table (schema được suy luận): {BRONZE_CSV_PATH}")
        df_csv_raw = spark.read.format("delta").load(BRONZE_CSV_PATH)

        print("Schema gốc của df_api_raw (từ Kafka producer):")
        df_api_raw.printSchema()
        time.sleep(3) 

        print("Schema gốc của df_csv_raw (từ CSV, suy luận):")
        df_csv_raw.printSchema()
        time.sleep(3) 

        # --- Thống nhất Schema ---
        # Định nghĩa schema mục tiêu cho lớp Silver.
        # Schema này dựa trên cấu trúc dữ liệu từ producer API,
        target_silver_schema = StructType([
            StructField("time", StringType(), True), StructField("month", IntegerType(), True),
            StructField("year", IntegerType(), True), StructField("temperature", DoubleType(), True),
            StructField("feelslike", DoubleType(), True), StructField("wind", DoubleType(), True),
            StructField("direction", StringType(), True), StructField("gust", DoubleType(), True),
            StructField("cloud", IntegerType(), True), StructField("humidity", IntegerType(), True),
            StructField("precipitation", DoubleType(), True), StructField("pressure", DoubleType(), True),
            StructField("weather", StringType(), True), StructField("label", StringType(), True)
        ])
        target_silver_columns = [field.name for field in target_silver_schema.fields]

        # Hàm trợ giúp để căn chỉnh DataFrame theo schema mục tiêu
        def align_df_to_schema(df, schema_target, df_name="DataFrame"):
            df_aligned = df
            existing_cols_lower = {c.lower(): c for c in df_aligned.columns}

            select_expressions = []

            for field in schema_target.fields:
                target_col_name = field.name
                target_col_type = field.dataType
                
                # Cố gắng tìm cột trong df_aligned, không phân biệt hoa thường
                # Điều này hữu ích nếu header CSV có chữ hoa/thường khác với schema mục tiêu
                source_col_name_found = None
                if target_col_name.lower() in existing_cols_lower:
                    source_col_name_found = existing_cols_lower[target_col_name.lower()]

                if source_col_name_found:
                    # Cột tồn tại, kiểm tra và ép kiểu
                    current_col_type = df_aligned.schema[source_col_name_found].dataType
                    if current_col_type == target_col_type:
                        select_expressions.append(col(source_col_name_found).alias(target_col_name))
                    else:
                        print(f"Thông báo ({df_name}): Ép kiểu cột '{source_col_name_found}' từ {current_col_type} sang {target_col_type} (đổi tên thành '{target_col_name}').")
                        try:
                            # Đối với các kiểu phức tạp hơn hoặc cần làm sạch, bạn có thể cần xử lý cụ thể ở đây
                            select_expressions.append(col(source_col_name_found).cast(target_col_type).alias(target_col_name))
                        except Exception as cast_error:
                            print(f"CẢNH BÁO ({df_name}): Không thể ép kiểu cột '{source_col_name_found}' sang {target_col_type}. Đặt thành NULL. Lỗi: {cast_error}")
                            select_expressions.append(lit(None).cast(target_col_type).alias(target_col_name))
                else:
                    # Cột không tồn tại trong DataFrame nguồn -> thêm cột null
                    print(f"Thông báo ({df_name}): Cột '{target_col_name}' không tìm thấy. Thêm cột này với giá trị NULL.")
                    select_expressions.append(lit(None).cast(target_col_type).alias(target_col_name))
            
            return df_aligned.select(select_expressions)

        # Căn chỉnh df_api_raw theo target_silver_schema
        # df_api_raw thường đã gần khớp, nhưng bước này đảm bảo tính nhất quán tuyệt đối
        print(f"\n--- Căn chỉnh df_api_raw ---")
        df_api_aligned = align_df_to_schema(df_api_raw, target_silver_schema, "df_api_raw")

        # Căn chỉnh df_csv_raw theo target_silver_schema
        print(f"\n--- Căn chỉnh df_csv_raw ---")
        df_csv_aligned = align_df_to_schema(df_csv_raw, target_silver_schema, "df_csv_raw")

        print("\nSchema của df_api_aligned (sau khi chuẩn hóa theo target_silver_schema):")
        df_api_aligned.printSchema()
        print("Schema của df_csv_aligned (sau khi chuẩn hóa theo target_silver_schema):")
        df_csv_aligned.printSchema()

        # df_api_aligned.show(5, truncate=False)
        # df_csv_aligned.show(5, truncate=False)

        # Gộp dữ liệu sau khi đã thống nhất schema
        # unionByName vẫn là lựa chọn an toàn nhất
        df_merged_bronze = df_api_aligned.unionByName(df_csv_aligned, allowMissingColumns=False) # allowMissingColumns=False vì đã align

        print("\nĐang xử lý dữ liệu cho Silver layer (VMWare)...")
        df_silver = df_merged_bronze \
            .withColumn("date_str_for_timestamp",
                        concat_ws("-",
                                  col("year").cast("string"), # Đảm bảo year, month là kiểu có thể cast sang string
                                  lpad(col("month").cast("string"), 2, "0"),
                                  lit("01"))) \
            .withColumn("datetime_str_for_timestamp", concat_ws(" ", col("date_str_for_timestamp"), col("time"))) \
            .withColumn("event_timestamp", to_timestamp(col("datetime_str_for_timestamp"), "yyyy-MM-dd HH:mm")) \
            .withColumn("year_partition", spark_year(col("event_timestamp"))) \
            .withColumn("month_partition", spark_month(col("event_timestamp"))) \
            .withColumn("day_partition", dayofmonth(col("event_timestamp"))) \
            .drop("date_str_for_timestamp", "datetime_str_for_timestamp") \
            .dropDuplicates(["year", "month", "time", "weather", "temperature", "direction"]) # Key loại trùng

        print("\nSchema của Silver DataFrame cuối cùng:")
        df_silver.printSchema()

        print(f"\nĐang ghi dữ liệu vào Silver Merged Table (HDFS): {SILVER_MERGED_PATH}")
        df_silver.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year_partition", "month_partition", "day_partition") \
            .save(SILVER_MERGED_PATH)

        print(f"Ghi dữ liệu vào Silver (HDFS) thành công. Số dòng: {df_silver.count()}")
        print(f"Kiểm tra dữ liệu đã ghi tại {SILVER_MERGED_PATH}:")
        spark.read.format("delta").load(SILVER_MERGED_PATH).show(5, truncate=False)

    except Exception as e:
        print(f"Đã xảy ra lỗi nghiêm trọng trong quá trình xử lý Bronze to Silver: {e}")
        import traceback
        traceback.print_exc() # In đầy đủ stack trace để dễ debug
    finally:
        spark.stop()
        print("SparkSession (VMWare - Silver Batch - Robust Schema Handling) đã được dừng.")

if __name__ == "__main__":
    main()