import os
from pathlib import Path

# Détection automatique de la racine du projet
BASE_DIR = Path(__file__).resolve().parent.parent

# Dossiers
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

# Création automatique des dossiers s'ils n'existent pas
for d in [PROCESSED_DIR, OUTPUT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Noms des fichiers (Mapping complet)
FILES = {
    "sites": "2020-t2-metropole-sites.csv",
    "communes": "Communes.csv",
    "departements": "Departements.csv",
    "regions": "Regions.csv",
    "arrondissements": "Arrondissements.csv",
    "cantons": "Cantons_et_metropoles.csv",
    "fractions": "Fractions_cantonales.csv",
    "outre_mer": "Collectivites_d_outre_mer.csv"
}

# Paramètres Spark
SPARK_APP_NAME = "TelecomInfrastructureAnalysis"
SPARK_MASTER = "local[*]"