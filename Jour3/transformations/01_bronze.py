"""
Raw -> Bronze : copie contrôlée et datée des fichiers retenus (convention du Jour 2, UC-03 §4).

Bronze contient EXACTEMENT les mêmes octets que Raw ; ce qui change, c'est le rangement
(bronze/tpcxbb/<lot>/<table>/ingest_date=AAAA-MM-JJ/) et le manifeste qui accompagne le dépôt.

Contrôles à la copie, pour chaque fichier :
    - somme de contrôle HDFS identique entre Raw et Bronze (contenu identique) ;
    - nombre de lignes ;
    - nombre de champs par ligne constant et égal au schéma (fichiers sans en-tête, séparateur |).

Idempotent : un fichier déjà présent en Bronze avec la même somme de contrôle n'est pas recopié.
Sorties : bronze/tpcxbb/_manifeste_<date>.csv (HDFS) et UC06_preuves/bronze/manifeste_bronze.csv
"""
import config as C
import commun as U
from pyspark.sql import functions as F

spark, log = U.session("01_bronze")
manifeste = []

for lot in C.LOTS:
    for table in C.TABLES:
        src = f"{C.RAW}/{lot}/{table}/{table}.dat"
        if not U.existe(spark, src):
            log.info("%s/%s : absent du lot (normal, le refresh ne contient que 11 tables)", lot, table)
            continue
        cible = f"{U.chemin_bronze(lot, table)}/{table}.dat"
        cs_src = U.checksum(spark, src)
        if U.existe(spark, cible) and U.checksum(spark, cible) == cs_src:
            action = "déjà présent (identique)"
        else:
            U.copier(spark, src, cible)
            action = "copié"
        cs_cible = U.checksum(spark, cible)

        # Contrôle de structure : nombre de champs de chaque ligne
        attendu = len(U.SCHEMA[table])
        lignes = spark.read.text(cible)
        stats = (lignes.select(F.size(F.split("value", r"\|", -1)).alias("n"))
                 .agg(F.count("*").alias("lignes"), F.min("n").alias("min"), F.max("n").alias("max"))
                 .first())
        ok = cs_src == cs_cible and stats["min"] == stats["max"] == attendu
        manifeste.append({
            "lot": lot, "table": table, "source_raw": src, "cible_bronze": cible, "action": action,
            "octets": U.taille(spark, cible), "lignes": stats["lignes"],
            "champs_attendus": attendu, "champs_min": stats["min"], "champs_max": stats["max"],
            "checksum_raw": cs_src, "checksum_bronze": cs_cible,
            "identique": cs_src == cs_cible, "statut": "OK" if ok else "ANOMALIE",
            "ingest_date": C.INGEST_DATE, "horodatage": U.horodatage(),
        })
        log.info("%s/%s : %s, %s lignes, champs %s-%s/%s, %s", lot, table, action, stats["lignes"],
                 stats["min"], stats["max"], attendu, "OK" if ok else "ANOMALIE")

# Manifeste : dans le lac (à côté des données) et dans les preuves locales
U.ecrire_csv_local(manifeste, C.PREUVES / "bronze" / "manifeste_bronze.csv")
(spark.createDataFrame(manifeste).coalesce(1).write.mode("overwrite").option("header", True)
 .option("sep", ";").csv(f"{C.BRONZE}/_manifeste/ingest_date={C.INGEST_DATE}"))
anomalies = [m for m in manifeste if m["statut"] != "OK"]
log.info("Bronze : %d fichiers, %d anomalie(s)", len(manifeste), len(anomalies))
if anomalies:
    raise SystemExit("Anomalie de copie Raw -> Bronze : voir le manifeste")
spark.stop()
