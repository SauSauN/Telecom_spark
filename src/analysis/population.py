from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

def analyze_coverage_density(df_sites, df_communes):
    """Axe 2 : Densité d'antennes par rapport à la population"""
    logger.info("Croisement Sites vs Population...")
    
    # Jointure Sites et Communes
    joined = df_sites.join(df_communes, on="code_insee", how="inner")
    
    # Agrégation par Département
    dept_stats = joined.groupBy("code_dep").agg(
        F.countDistinct("num_site").alias("nb_sites"),
        F.sum("population_municipale").alias("pop_totale") 
    )
    
    # Calcul densité (Nb antennes pour 10.000 habitants)
    res = dept_stats.withColumn(
        "sites_pour_10k_hab", 
        F.round((F.col("nb_sites") / F.col("pop_totale")) * 10000, 2)
    ).orderBy(F.desc("sites_pour_10k_hab"))
    
    return res