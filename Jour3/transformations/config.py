"""
Paramètres du pipeline UC-06 (TP Jour 3).

Tous les chemins et réglages sont ici, et nulle part ailleurs : pour rejouer le pipeline sur un autre
environnement, on ne modifie que ce fichier (ou les variables d'environnement ci-dessous).

    TP3_LAKE         racine du Data Lake (défaut : HDFS du Jour 2, hdfs://localhost:9000/retail-lake)
                     repli possible sans HDFS : TP3_LAKE=file:///home/etudiant/retail-lake-local
    TP3_INGEST_DATE  date d'ingestion Bronze (défaut : 2026-11-18, date du TP)
"""
import os
from pathlib import Path

# --------------------------------------------------------------------------- chemins
LAKE = os.environ.get("TP3_LAKE", "hdfs://localhost:9000/retail-lake").rstrip("/")
INGEST_DATE = os.environ.get("TP3_INGEST_DATE", "2026-11-18")

RAW = f"{LAKE}/raw/tpcxbb"            # dépôt du Jour 2 (UC-03), jamais modifié
BRONZE = f"{LAKE}/bronze/tpcxbb"      # copie contrôlée et datée de Raw
SILVER = f"{LAKE}/silver"             # données nettoyées, typées (Parquet)
GOLD = f"{LAKE}/gold"                 # jeux analytiques (Parquet)
QUALITY = f"{LAKE}/quality"           # quarantaine et résultats des contrôles
LOGS = f"{LAKE}/logs/transformation"  # journaux d'exécution

# Dossiers locaux (dans le dépôt) : preuves et journaux lisibles par le formateur
J3 = Path(__file__).resolve().parents[1]
PREUVES = J3 / "UC06_preuves"
JOURNAUX = PREUVES / "journaux"

# --------------------------------------------------------------------------- périmètre
LOTS = ["initial", "refresh"]          # lots du kit : data/ -> initial, data_refresh/ -> refresh

# Tables retenues pour l'UC-06 (choix justifié dans l'inventaire technique) :
#   faits : ventes et retours des deux canaux ; référentiels nécessaires aux questions Q1, Q2, Q3, Q6, Q8
TABLES = ["store_sales", "web_sales", "store_returns", "web_returns", "item", "store", "date_dim", "reason"]

# --------------------------------------------------------------------------- Spark
SPARK_APP = "TP3-Retail-Intelligence"
SPARK_MASTER = os.environ.get("TP3_MASTER", "local[*]")
SPARK_CONF = {
    "spark.sql.shuffle.partitions": "16",        # petit volume : 16 partitions suffisent (défaut 200)
    "spark.sql.session.timeZone": "Europe/Paris",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.driver.memory": "6g",
    "spark.ui.showConsoleProgress": "false",
}
