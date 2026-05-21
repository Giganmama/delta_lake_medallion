from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, col, row_number, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from config import BRONZE_PATH, SILVER_PATH, SPARK_S3_CONF
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("SilverCleaner") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", SPARK_S3_CONF["spark.sql.extensions"]) \
        .config("spark.sql.catalog.spark_catalog", SPARK_S3_CONF["spark.sql.catalog.spark_catalog"]) \
        .config("spark.hadoop.fs.s3a.endpoint", SPARK_S3_CONF["spark.hadoop.fs.s3a.endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", SPARK_S3_CONF["spark.hadoop.fs.s3a.access.key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", SPARK_S3_CONF["spark.hadoop.fs.s3a.secret.key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", SPARK_S3_CONF["spark.hadoop.fs.s3a.path.style.access"]) \
        .getOrCreate()
    return spark

def process_silver(spark: SparkSession, table_name: str = "transactions"):
    logger.info(f"🧹 Processing Silver layer for {table_name}")
    
    bronze_path = f"{BRONZE_PATH}/{table_name}"
    silver_path = f"{SILVER_PATH}/{table_name}"
    
    # 1. Читаем Bronze
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    # 2. Дедупликация (Window Function)
    window_spec = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
    
    deduped_df = bronze_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")
        
    # 3. PII Masking (Hashing sensitive data)
    # Предположим, у нас есть колонка client_id, которую мы хотим захешировать для Silver
    silver_df = deduped_df.withColumn(
        "client_id_hash", 
        sha2(concat_ws("-", col("client_id"), col("transaction_id")), 256)
    ).drop("client_id") # Удаляем оригинал для безопасности
    
    # 4. Запись в Silver с использованием MERGE (Upsert)
    # Если таблица существует -> обновляем/вставляем, если нет -> создаем
    try:
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            silver_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logger.info("✅ Silver Merge complete")
    except Exception:
        # Таблица не существует, пишем новый файл
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .save(silver_path)
        logger.info("✅ Silver table created")

if __name__ == "__main__":
    spark = create_spark_session()
    process_silver(spark)
    spark.stop()
