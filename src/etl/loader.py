from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
import os
from src.config import RAW_DIR
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_csv(self, filename: str, schema=None, delimiter=";") -> DataFrame:
        """
        Charge un CSV avec gestion d'erreur et logging.
        """
        file_path = os.path.join(RAW_DIR, filename)
        
        if not os.path.exists(file_path):
            logger.error(f"Fichier introuvable : {file_path}")
            raise FileNotFoundError(f"Le fichier {filename} est manquant dans {RAW_DIR}")

        logger.info(f"Chargement du fichier : {filename}")
        
        try:
            reader = self.spark.read \
                .option("header", "true") \
                .option("delimiter", delimiter) \
                .option("mode", "PERMISSIVE") \
                .option("ignoreLeadingWhiteSpace", "true") \
                .option("ignoreTrailingWhiteSpace", "true")

            if schema:
                df = reader.schema(schema).csv(file_path)
            else:
                logger.warning(f"Aucun schéma fourni pour {filename}, inférence automatique (plus lent).")
                df = reader.option("inferSchema", "true").csv(file_path)

            logger.info(f"Fichier {filename} chargé avec succès. {df.count()} lignes détectées.")
            return df

        except AnalysisException as e:
            logger.error(f"Erreur Spark lors de la lecture de {filename}: {str(e)}")
            raise