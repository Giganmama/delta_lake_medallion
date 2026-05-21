from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col
from config import SILVER_PATH, GOLD_PATH, SPARK_S3_CONF
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("GoldAggregator") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", SPARK_S3_CONF["spark.sql.extensions"]) \
        .config("spark.sql.catalog.spark_catalog", SPARK_S3_CONF["spark.sql.catalog.spark_catalog"]) \
        .config("spark.hadoop.fs.s3a.endpoint", SPARK_S3_CONF["spark.hadoop.fs.s3a.endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", SPARK_S3_CONF["spark.hadoop.fs.s3a.access.key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", SPARK_S3_CONF["spark.hadoop.fs.s3a.secret.key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", SPARK_S3_CONF["spark.hadoop.fs.s3a.path.style.access"]) \
        .getOrCreate()
    return spark

def process_gold(spark: SparkSession, table_name: str = "transactions"):
    logger.info(f"💰 Processing Gold layer for {table_name}")
    
    silver_path = f"{SILVER_PATH}/{table_name}"
    gold_path = f"{GOLD_PATH}/{table_name}_daily"
    
    # 1. Читаем Silver
    silver_df = spark.read.format("delta").load(silver_path)
    
    # 2. Агрегация (Star Schema Fact Table)
    gold_df = silver_df.groupBy("client_id_hash") \
        .agg(
            sum("amount").alias("total_amount"),
            count("transaction_id").alias("tx_count"),
            current_timestamp().alias("last_updated")
        )
        
    # 3. Запись в Gold
    gold_df.write.format("delta") \
        .mode("overwrite") \
        .save(gold_path)
    
    logger.info("✅ Gold aggregation complete")
    
    # 4. Delta Optimization (Физическая оптимизация файлов)
    logger.info("🚀 Running OPTIMIZE and VACUUM...")
    spark.sql(f"OPTIMIZE delta.`{gold_path}`")
    spark.sql(f"VACUUM delta.`{gold_path}` RETAIN 168 HOURS")
    
    logger.info("✅ Maintenance complete")

if __name__ == "__main__":
    spark = create_spark_session()
    process_gold(spark)
    spark.stop()
