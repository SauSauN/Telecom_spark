from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

def analyze_infrastructure(df_sites):
    """Axe 1 : Statistiques descriptives de l'infra (2G/3G/4G)"""
    logger.info("Calcul des statistiques d'infrastructure...")
    
    stats = df_sites.groupBy("nom_op").agg(
        F.count("num_site").alias("total_sites"),
        F.sum("site_4g").alias("total_4g"),
        F.round(F.sum("site_4g") / F.count("num_site") * 100, 2).alias("taux_4g_pct")
    ).orderBy(F.desc("total_sites"))
    
    return stats