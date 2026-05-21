from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from config import BRONZE_PATH, SPARK_S3_CONF
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """Инициализация Spark с поддержкой Delta Lake и S3/MinIO"""
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", SPARK_S3_CONF["spark.sql.extensions"]) \
        .config("spark.sql.catalog.spark_catalog", SPARK_S3_CONF["spark.sql.catalog.spark_catalog"]) \
        .config("spark.hadoop.fs.s3a.endpoint", SPARK_S3_CONF["spark.hadoop.fs.s3a.endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", SPARK_S3_CONF["spark.hadoop.fs.s3a.access.key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", SPARK_S3_CONF["spark.hadoop.fs.s3a.secret.key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", SPARK_S3_CONF["spark.hadoop.fs.s3a.path.style.access"]) \
        .getOrCreate()
    return spark

def ingest_to_bronze(spark: SparkSession, source_path: str, table_name: str):
    """Чтение сырых данных и запись в Delta формат (Append-Only)"""
    logger.info(f"📥 Reading raw data from {source_path}")
    
    raw_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(source_path)
    
    # Добавляем метаданные ingestion
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("mock_transactions"))
    
    delta_path = f"{BRONZE_PATH}/{table_name}"
    logger.info(f"💾 Writing to Delta: {delta_path}")
    
    bronze_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingestion_date") \
        .save(delta_path)
    
    logger.info(f"✅ Bronze ingestion complete: {bronze_df.count()} rows")
    return delta_path

if __name__ == "__main__":
    spark = create_spark_session()
    # Пример запуска (замени на реальный путь к CSV/JSON)
    # ingest_to_bronze(spark, "data/mock_transactions.csv", "transactions")
    logger.info("🚀 Bronze layer ready for execution")
    spark.stop()
