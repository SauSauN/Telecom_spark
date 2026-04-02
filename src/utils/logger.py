import logging
import sys
import os
from src.config import LOG_DIR

def get_logger(name):
    """Retourne une instance de logger configurée."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Format du log : [DATE] [NIVEAU] [MODULE] - Message
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')

        # Handler Console (Affiche à l'écran)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler Fichier (Ecrit dans logs/app_execution.log)
        # Gestion compatible Windows (Pathlib ou String)
        log_file_path = LOG_DIR / "app_execution.log"
        
        # Création du dossier logs s'il n'existe pas encore
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger