from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.utils.logger import get_logger

logger = get_logger(__name__)

def analyze_territorial_competition(df_sites: DataFrame, df_regions: DataFrame):
    """
    Analyse complexe : Parts de marché par région et densité concurrentielle.
    """
    logger.info("Lancement de l'analyse concurrentielle ...")

    # Jointure pour avoir le nom propre de la région (si code disponible) ou usage direct
    
    # Pivot Table : Opérateurs en colonnes, Régions en lignes
    pivot_df = df_sites.groupBy("nom_reg") \
        .pivot("nom_op") \
        .agg(F.count("num_site")) \
        .na.fill(0) \
        .orderBy("nom_reg")
    
    logger.info("Matrice de concurrence générée.")
    pivot_df.show(5)
    
    # Calcul de l'indice HHI (Herfindahl-Hirschman Index) par département
    # HHI = somme des carrés des parts de marché (Indicateur de monopole)
    
    # Total sites par département
    total_sites_dep = df_sites.groupBy("code_dep").agg(F.count("num_site").alias("total_dep"))
    
    # Sites par opérateur par département
    op_sites_dep = df_sites.groupBy("code_dep", "nom_op").agg(F.count("num_site").alias("nb_sites_op"))
    
    # Calcul part de marché
    market_share = op_sites_dep.join(total_sites_dep, "code_dep") \
        .withColumn("market_share", F.col("nb_sites_op") / F.col("total_dep"))
        
    # Somme des carrés
    hhi_index = market_share.groupBy("code_dep") \
        .agg(F.sum(F.pow("market_share", 2)).alias("hhi_index")) \
        .orderBy(F.desc("hhi_index"))
        
    logger.info("Indice de concentration HHI calculé (Haut = Monopole, Bas = Concurrence).")
    hhi_index.show(5)
    
    return pivot_df, hhi_index