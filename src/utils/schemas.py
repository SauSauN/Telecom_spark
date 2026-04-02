from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Schéma Sites Télécoms
sites_schema = StructType([
    StructField("code_op", IntegerType(), True),
    StructField("nom_op", StringType(), True),
    StructField("num_site", StringType(), True),
    StructField("x_lambert_93", DoubleType(), True),
    StructField("y_lambert_93", DoubleType(), True),
    StructField("nom_reg", StringType(), True),
    StructField("nom_dep", StringType(), True),
    StructField("insee_dep", StringType(), True),
    StructField("nom_com", StringType(), True),
    StructField("insee_com", StringType(), True),
    StructField("site_2g", IntegerType(), True),
    StructField("site_3g", IntegerType(), True),
    StructField("site_4g", IntegerType(), True),
    StructField("mes_4g_trim", IntegerType(), True),
    StructField("site_ZB", IntegerType(), True),
    StructField("site_DCC", IntegerType(), True)
])

# Schéma Communes (INSEE)
communes_schema = StructType([
    StructField("DEPCOM", StringType(), True), # Code INSEE (clé de jointure)
    StructField("COM", StringType(), True),    # Nom
    StructField("PMUN", LongType(), True),     # Pop Municipale
    StructField("PCAP", LongType(), True),
    StructField("PTOT", LongType(), True)      # Pop Totale
])

# 3. Schéma Départements
departements_schema = StructType([
    StructField("CODDEP", StringType(), True),
    StructField("DEP", StringType(), True),
    StructField("NBARR", IntegerType(), True),
    StructField("NBCAN", IntegerType(), True),
    StructField("NBCOM", IntegerType(), True),
    StructField("PMUN", LongType(), True),
    StructField("PTOT", LongType(), True)
])

# Schéma Régions
regions_schema = StructType([
    StructField("CODREG", StringType(), True),
    StructField("REG", StringType(), True),
    StructField("NBARR", IntegerType(), True),
    StructField("NBCAN", IntegerType(), True),
    StructField("NBCOM", IntegerType(), True),
    StructField("PMUN", LongType(), True),
    StructField("PTOT", LongType(), True)
])

# Schéma Arrondissements
arrondissements_schema = StructType([
    StructField("DEPARR", StringType(), True),
    StructField("ARR", StringType(), True),
    StructField("NBCOM", IntegerType(), True),
    StructField("PMUN", LongType(), True),
    StructField("PTOT", LongType(), True)
])

# Schéma Cantons
cantons_schema = StructType([
    StructField("DEPCAN", StringType(), True),
    StructField("CAN", StringType(), True),
    StructField("NBCOM", IntegerType(), True),
    StructField("PMUN", LongType(), True),
    StructField("PTOT", LongType(), True)
])

# Dictionnaire pour appeler les schémas dynamiquement
SCHEMAS_MAP = {
    "sites": sites_schema,
    "communes": communes_schema,
    "departements": departements_schema,
    "regions": regions_schema,
    "arrondissements": arrondissements_schema,
    "cantons": cantons_schema
}