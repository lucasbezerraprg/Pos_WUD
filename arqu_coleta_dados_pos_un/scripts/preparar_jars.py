import os, sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.4",
]

spark = (SparkSession.builder.master("local[1]").appName("preparar-jars")
         .config("spark.jars.packages", ",".join(PACKAGES))
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
print("Jars baixados e prontos. Spark", spark.version)
spark.stop()