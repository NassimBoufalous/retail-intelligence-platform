"""
Étape 1 – Préparer l'environnement (TP §1.4.1 et §4.4).

Vérifie que Spark fonctionne et relève les versions réellement utilisées (fiche d'environnement) :
    1. création d'une session Spark en mode local ;
    2. petit DataFrame de test (3 magasins) ;
    3. lecture d'un fichier réel du Data Lake (Raw, HDFS) et affichage de son schéma ;
    4. écriture d'un résultat de test en Parquet dans un répertoire autorisé, puis relecture.

Sortie : UC06_preuves/environnement/fiche_environnement.json et test_spark.txt
"""
import os
import platform
import subprocess
import sys

import config as C
import commun as U
from pyspark.sql import functions as F

spark, log = U.session("00_test_environnement")
sortie = C.PREUVES / "environnement"
trace = []

# 1-2. Programme de test du TP (§1.4.1)
donnees = [("Magasin A", 1200.0), ("Magasin B", 850.0), ("Magasin C", 1600.0)]
df = spark.createDataFrame(donnees, ["magasin", "chiffre_affaires"])
trace += ["== Programme de test (TP §1.4.1) ==", f"Version Spark : {spark.version}", U.apercu_texte(df)]

# 3. Lecture d'un fichier réel : le plus petit fichier de ventes du lac (lot refresh), sans en-tête
src = f"{C.RAW}/refresh/store_sales/store_sales.dat"
brut = spark.read.option("sep", "|").option("header", False).option("inferSchema", True).csv(src)
trace += [f"== Lecture d'un fichier réel : {src} ==",
          brut._jdf.schema().treeString(), U.apercu_texte(brut, 3),
          f"Nombre de lignes : {brut.count()}"]

# 4. Écriture de test dans un répertoire autorisé (logs/transformation), puis relecture
test = f"{C.LOGS}/test_ecriture_parquet"
U.ecrire_parquet(df.withColumn("ecrit_le", F.current_timestamp()), test, log)
relu = spark.read.parquet(test)
trace += [f"== Écriture puis relecture Parquet : {test} ==", U.apercu_texte(relu),
          f"Lignes relues : {relu.count()}"]


def cmd(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=30).stdout.strip() \
            or subprocess.run(args, capture_output=True, text=True, timeout=30).stderr.strip()
    except Exception as e:  # noqa: BLE001 - on relève simplement l'erreur dans la fiche
        return f"indisponible ({e})"


jvm = spark._jvm
fiche = {
    "date": U.horodatage(),
    "systeme": f"{platform.system()} {platform.release()}",
    "distribution": cmd("lsb_release", "-ds"),
    "machine": platform.node(),
    "cpu_logiques": os.cpu_count(),
    "python": sys.version.split()[0],
    "python_executable": sys.executable,
    "pyspark": __import__("pyspark").__version__,
    "spark": spark.version,
    "scala": jvm.scala.util.Properties.versionNumberString(),
    "java_spark": jvm.java.lang.System.getProperty("java.version"),
    "java_home": os.environ.get("JAVA_HOME", ""),
    "client_hadoop_de_spark": jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
    "hadoop_hdfs_serveur": cmd("/opt/hadoop/bin/hadoop", "version").splitlines()[0],
    "master": spark.sparkContext.master,
    "parallelisme_par_defaut": spark.sparkContext.defaultParallelism,
    "spark_conf": C.SPARK_CONF,
    "interface": "scripts Python (spark-submit / python) + JupyterLab pour l'exploration",
    "lac": C.LAKE,
    "chemins": {"raw": C.RAW, "bronze": C.BRONZE, "silver": C.SILVER, "gold": C.GOLD,
                "quality": C.QUALITY, "logs": C.LOGS},
    "test_ecriture": test,
}
U.ecrire_json_local(fiche, sortie / "fiche_environnement.json")
(sortie / "test_spark.txt").write_text("\n".join(trace), encoding="utf-8")
log.info("Fiche d'environnement : %s", fiche)
spark.stop()
