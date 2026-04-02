import sys
import os
import time
import shutil

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
HADOOP_HOME = os.path.join(PROJECT_DIR, 'hadoop')
os.environ['HADOOP_HOME'] = HADOOP_HOME
os.environ['PATH'] += os.pathsep + os.path.join(HADOOP_HOME, 'bin')

from src.utils.spark_session import get_spark_session
from src.utils.schemas import SCHEMAS_MAP
from src.utils.logger import get_logger
from src.config import FILES, OUTPUT_DIR
from src.etl.loader import DataLoader
from src.etl.cleaner import DataCleaner
from src.analysis import infrastructure, population, competition

logger = get_logger("MainExecutor")


# Fonction utilitaire pour écrire un CSV avec nom fixe
def save_single_csv(df, output_dir, final_name):
    temp_dir = os.path.join(output_dir, final_name + "_tmp")

    # Écriture Spark (dans dossier temporaire)
    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")

    # Récupération du fichier CSV généré
    files = [f for f in os.listdir(temp_dir) if f.endswith(".csv")]
    if not files:
        raise Exception(f"Aucun fichier CSV généré dans {temp_dir}")

    temp_file = os.path.join(temp_dir, files[0])
    final_path = os.path.join(output_dir, final_name)

    # Déplacement + renommage
    shutil.move(temp_file, final_path)

    # Nettoyage du dossier temporaire
    shutil.rmtree(temp_dir)

    logger.info(f"Fichier sauvegardé : {final_path}")

    return final_path


def main():
    start_time = time.time()
    logger.info(">>> DÉMARRAGE DU PIPELINE DE DONNÉES (MODE LOCAL) <<<")

    spark = get_spark_session()
    loader = DataLoader(spark)
    cleaner = DataCleaner()

    try:
        # Étape 1 : Ingestion
        logger.info("--- Étape 1 : Ingestion des données brutes ---")
        df_sites_raw = loader.load_csv(FILES["sites"], SCHEMAS_MAP["sites"])
        df_com_raw = loader.load_csv(FILES["communes"], SCHEMAS_MAP["communes"])
        df_dept_raw = loader.load_csv(FILES["departements"], SCHEMAS_MAP["departements"])

        # Étape 2 : Nettoyage
        logger.info("--- Étape 2 : Nettoyage et Standardisation ---")
        df_sites = cleaner.clean_sites(df_sites_raw)
        df_communes = cleaner.clean_demography(df_com_raw, "DEPCOM", "COM")
        df_departements = cleaner.clean_demography(df_dept_raw, "CODDEP", "DEP")

        df_sites.cache()
        df_communes.cache()

        logger.info(f"Volumétrie Sites : {df_sites.count()} antennes actives.")

        # Étape 3 : Analyse
        logger.info("--- Étape 3 : Exécution des modèles analytiques ---")
        df_infra_stats = infrastructure.analyze_infrastructure(df_sites)
        df_coverage = population.analyze_coverage_density(df_sites, df_communes)
        df_matrix, df_hhi = competition.analyze_territorial_competition(df_sites, df_departements)

        # Étape 4 : Sauvegarde
        logger.info(f"--- Étape 4 : Sauvegarde des résultats dans {OUTPUT_DIR} ---")

        save_single_csv(df_infra_stats, OUTPUT_DIR, "infra_stats.csv")
        save_single_csv(df_coverage, OUTPUT_DIR, "couverture_habitant.csv")
        save_single_csv(df_matrix, OUTPUT_DIR, "matrice_concurrence.csv")
        save_single_csv(df_hhi, OUTPUT_DIR, "indice_concurrence_hhi.csv")

    except Exception as e:
        logger.critical(f"Le pipeline a échoué : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()
        elapsed = time.time() - start_time
        logger.info(f">>> PIPELINE TERMINÉ EN {elapsed:.2f} SECONDES <<<")


if __name__ == "__main__":
    main()