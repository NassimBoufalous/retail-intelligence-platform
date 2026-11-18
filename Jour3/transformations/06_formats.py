"""
Étape 8 – Format Parquet : écriture, relecture et comparaison argumentée CSV / JSON / Parquet.

Jeu de comparaison : les ventes magasin (store_sales, 674 211 lignes, lots initial + refresh), dans trois formats :
    - CSV  : le fichier source Bronze (.dat délimité par |), tel que fourni ;
    - JSON : les mêmes données typées, écrites en JSON lignes (1 objet par ligne) dans un dossier de test
             temporaire, HORS du lac (file:///tmp/tp3_formats), supprimé à la fin ;
    - Parquet : silver/store_sales (Snappy, partitionné par année).

Mesures : taille sur disque, nombre de fichiers, temps de lecture de 2 requêtes (comptage complet ; somme d'UNE
colonne), types obtenus à la relecture, et plans d'exécution montrant la lecture sélective de colonnes et
l'élagage des partitions (Parquet seulement).
Les temps sont indicatifs (machine locale, cache disque du système) : médiane de 3 essais.
"""
import shutil
import statistics
import time

import config as C
import commun as U
from pyspark.sql import functions as F

spark, log = U.session("06_formats")
OUT = C.PREUVES / "parquet"
OUT.mkdir(parents=True, exist_ok=True)
TMP_LOCAL = "/tmp/tp3_formats"
JSON_PATH = f"file://{TMP_LOCAL}/store_sales_json"

silver = spark.read.parquet(f"{C.SILVER}/store_sales")
cols_src = U.SCHEMA["store_sales"]

# JSON de comparaison (mêmes colonnes source, données typées)
shutil.rmtree(TMP_LOCAL, ignore_errors=True)
silver.select(*cols_src).write.mode("overwrite").json(JSON_PATH)


def lecteur(fmt):
    if fmt == "CSV (Bronze .dat)":
        return lambda: (spark.read.option("sep", "|").option("header", False).schema(U.schema_texte("store_sales"))
                        .csv([f"{U.chemin_bronze(l, 'store_sales')}/store_sales.dat" for l in C.LOTS]))
    if fmt == "JSON lignes":
        return lambda: spark.read.json(JSON_PATH)
    return lambda: spark.read.parquet(f"{C.SILVER}/store_sales")


def chrono(f, essais=3):
    t = []
    for _ in range(essais):
        t0 = time.perf_counter()
        f()
        t.append(time.perf_counter() - t0)
    return round(statistics.median(t), 2)


tailles = {"CSV (Bronze .dat)": sum(U.taille(spark, U.chemin_bronze(l, "store_sales")) for l in C.LOTS),
           "JSON lignes": U.taille(spark, JSON_PATH),
           "Parquet (Snappy)": U.taille(spark, f"{C.SILVER}/store_sales")}
fichiers = {"CSV (Bronze .dat)": 2, "JSON lignes": U.nb_fichiers(spark, JSON_PATH, ".json"),
            "Parquet (Snappy)": U.nb_fichiers(spark, f"{C.SILVER}/store_sales")}
# taille d'un Parquet limité aux colonnes source (sans les colonnes ajoutées en Silver), pour comparer à contenu égal
tmp_pq = f"file://{TMP_LOCAL}/store_sales_parquet_colonnes_source"
silver.select(*cols_src).write.mode("overwrite").parquet(tmp_pq)
tailles["Parquet (Snappy), colonnes source seules"] = U.taille(spark, tmp_pq)
fichiers["Parquet (Snappy), colonnes source seules"] = U.nb_fichiers(spark, tmp_pq)

mesures = []
for fmt in ["CSV (Bronze .dat)", "JSON lignes", "Parquet (Snappy)"]:
    lire = lecteur(fmt)
    df = lire()
    t_count = chrono(lambda: lire().count())
    t_sum = chrono(lambda: lire().agg(F.sum(F.col("ss_ext_sales_price").cast("decimal(17,2)"))).first())
    somme = lire().agg(F.sum(F.col("ss_ext_sales_price").cast("decimal(17,2)"))).first()[0]
    types = dict(df.dtypes)
    mesures.append({
        "format": fmt, "octets": tailles[fmt], "mo": round(tailles[fmt] / 1024 ** 2, 1),
        "ratio_vs_csv": round(tailles[fmt] / tailles["CSV (Bronze .dat)"], 3), "fichiers": fichiers[fmt],
        "lignes": df.count(), "temps_count_s": t_count, "temps_somme_1_colonne_s": t_sum,
        "somme_ca_controle": str(somme),
        "type_ss_ext_sales_price": types.get("ss_ext_sales_price"), "type_ss_quantity": types.get("ss_quantity"),
        "type_date": types.get("date_vente", "absent (clé numérique ss_sold_date_sk seulement)"),
        "schema_porte_par_le_fichier": "oui" if fmt.startswith("Parquet") else
        ("non : noms et types fournis par le programme" if fmt.startswith("CSV") else "noms oui, types déduits"),
    })
    log.info("%s : %s", fmt, mesures[-1])
mesures.append({"format": "Parquet (Snappy), colonnes source seules",
                "octets": tailles["Parquet (Snappy), colonnes source seules"],
                "mo": round(tailles["Parquet (Snappy), colonnes source seules"] / 1024 ** 2, 1),
                "ratio_vs_csv": round(tailles["Parquet (Snappy), colonnes source seules"] / tailles["CSV (Bronze .dat)"], 3),
                "fichiers": fichiers["Parquet (Snappy), colonnes source seules"]})

# Plans d'exécution : ce que Spark lit réellement
pq = spark.read.parquet(f"{C.SILVER}/store_sales")
plans = {
    "Parquet : somme d'une colonne, année 2004": pq.filter("annee = 2004").agg(F.sum("ss_ext_sales_price")),
    "CSV : somme d'une colonne": lecteur("CSV (Bronze .dat)")().agg(F.sum("ss_ext_sales_price")),
}
txt = []
for titre, d in plans.items():
    plan = d._jdf.queryExecution().executedPlan().toString()
    lignes_scan = [l.strip() for l in plan.splitlines() if "FileScan" in l or "Scan" in l]
    txt.append(f"== {titre} ==\n" + "\n".join(lignes_scan))
(OUT / "plans_lecture.txt").write_text("\n\n".join(txt), encoding="utf-8")

# Écriture / relecture (TP §4.11) : contrôle sur un jeu Gold
g = f"{C.GOLD}/ventes_mensuelles_canal"
relu = spark.read.parquet(g)
(OUT / "relecture_parquet.txt").write_text(
    f"Relecture de {g}\nFichiers Parquet : {U.nb_fichiers(spark, g)} ; octets : {U.taille(spark, g)}\n"
    f"Lignes relues : {relu.count()}\n\n{relu._jdf.schema().treeString()}\n{U.apercu_texte(relu, 12)}",
    encoding="utf-8")

U.ecrire_csv_local(mesures, OUT / "comparaison_formats.csv")
shutil.rmtree(TMP_LOCAL, ignore_errors=True)   # dossier de test créé par ce script uniquement
log.info("Comparaison des formats terminée")
spark.stop()
