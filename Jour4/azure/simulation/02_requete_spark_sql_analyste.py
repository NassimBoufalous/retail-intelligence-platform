"""
UC-07, parcours B : requête analytique en Spark SQL sur la zone Gold simulée, avec l'identité « analyste ».

    HADOOP_USER_NAME=analyste ~/venv-tp3/bin/python Jour4/azure/simulation/02_requete_spark_sql_analyste.py

Spark SQL joue ici le rôle de Synapse SQL (ou de Trino) : interroger en SQL des fichiers Parquet du Data Lake.
La même requête est ensuite tentée sur la zone Silver, interdite à l'analyste : l'accès doit être refusé.
Résultat écrit dans Jour4/azure/evidence/spark_sql_analyste.txt.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
os.environ.setdefault("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession  # noqa: E402

CONTENEUR = "hdfs://localhost:9000/adls-simulation/retail-intelligence"   # = abfss://retail-intelligence@<compte>.dfs.core.windows.net
SORTIE = Path(__file__).resolve().parents[1] / "evidence" / "spark_sql_analyste.txt"

spark = (SparkSession.builder.appName("TP4-UC07-SparkSQL-analyste").master("local[2]")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
lignes = [f"# Spark SQL {spark.version} – identité HDFS : {os.environ.get('HADOOP_USER_NAME', '(utilisateur courant)')} – "
          f"{datetime.now():%Y-%m-%d %H:%M:%S}", ""]

spark.read.parquet(f"{CONTENEUR}/gold/analyses/ventes_magasin_annee").createOrReplaceTempView("ventes_magasin_annee")
requete = """
SELECT s_store_name AS magasin, s_state AS etat,
       ROUND(SUM(ca_ht) / 1e6, 2)                      AS ca_ht_millions,
       SUM(nb_tickets_commandes)                        AS tickets,
       ROUND(SUM(ca_ht) / SUM(nb_tickets_commandes), 2) AS panier_moyen
FROM ventes_magasin_annee
WHERE annee = 2005
GROUP BY s_store_name, s_state
ORDER BY ca_ht_millions DESC
LIMIT 5
"""
res = spark.sql(requete)
lignes += ["## 1. Requête sur la zone Gold (autorisée)", requete.strip(), "",
           res._jdf.showString(20, 0, False), ""]

lignes.append("## 2. Même identité, lecture de la zone Silver (interdite par la matrice des droits)")
try:
    spark.read.parquet(f"{CONTENEUR}/silver/magasins/store").count()
    lignes.append("RÉSULTAT : lecture AUTORISÉE (écart par rapport à la matrice)")
except Exception as e:  # AccessControlException renvoyée par HDFS
    msg = next((l for l in str(e).splitlines() if "Permission denied" in l), str(e).splitlines()[0])
    lignes.append("RÉSULTAT : lecture REFUSÉE, conforme à la matrice des droits")
    lignes.append(msg.strip()[:300])

SORTIE.write_text("\n".join(lignes) + "\n", encoding="utf-8")
print("\n".join(lignes))
spark.stop()
