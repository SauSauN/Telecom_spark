from pyspark.sql import SparkSession
import os
import sys
from src.config import SPARK_APP_NAME, SPARK_MASTER

def get_spark_session():
    """
    Crée ou récupère une SparkSession singleton.
    Configure la mémoire et le master url.
    """
    # Configuration pour windows 
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    return spark