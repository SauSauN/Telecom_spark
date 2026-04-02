from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, when, lit
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DataCleaner:
    @staticmethod
    def clean_sites(df: DataFrame) -> DataFrame:
        """Nettoyage spécifique Sites Télécoms"""
        logger.info("Nettoyage des données Sites...")
        
        # Standardisation des codes INSEE
        # Suppression des coordonnées nulles 
        df_clean = df.select(
            col("code_op"),
            col("nom_op"),
            col("num_site"),
            col("x_lambert_93"),
            col("y_lambert_93"),
            col("nom_reg"),
            trim(upper(col("insee_dep"))).alias("code_dep"),
            trim(col("insee_com")).alias("code_insee"),
            col("site_4g"),
            col("site_3g"),
            col("site_2g")
        ).filter(
            col("x_lambert_93").isNotNull() & col("y_lambert_93").isNotNull()
        )
        
        return df_clean

    @staticmethod
    def clean_demography(df: DataFrame, code_col: str, name_col: str) -> DataFrame:
        """Nettoyage générique pour Communes/Dept/Régions"""
        logger.info(f"Nettoyage des données démographiques (clé: {code_col})...")
        
        return df.select(
            trim(col(code_col)).alias("code_insee"),
            trim(col(name_col)).alias("nom_zone"),
            col("PMUN").alias("population_municipale"),
            col("PTOT").alias("population_totale")
        ).dropDuplicates(["code_insee"])