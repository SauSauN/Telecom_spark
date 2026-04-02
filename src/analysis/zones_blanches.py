from src.utils.logger import get_logger

logger = get_logger(__name__)

def find_white_zones(df_sites, df_communes):
    """Axe 3 : Identification des communes sans aucun site"""
    logger.info("Recherche des zones blanches (Left Anti Join)...")
    
    # Communes QUI NE SONT PAS dans le fichier sites
    zones_blanches = df_communes.join(
        df_sites, 
        on="code_insee", 
        how="left_anti"
    )
    
    return zones_blanches