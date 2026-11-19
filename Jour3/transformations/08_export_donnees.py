"""
Export des données produites vers le dépôt (Jour3/data/), pour que le formateur puisse les ouvrir sans HDFS.

    gold/   : les 5 agrégats Gold COMPLETS (Parquet, quelques Ko chacun) ;
              fait_ventes (126 Mo) reste dans HDFS : seul un échantillon est exporté.
    silver/ : échantillons des tables Silver (1 000 premières lignes des faits, référentiels petits complets).

Chaque jeu est exporté en UN fichier Parquet (coalesce(1)), relu et contrôlé (nombre de lignes, schéma).
Sortie : Jour3/data/{silver,gold}/<jeu>.parquet + Jour3/data/contenu_data.csv
"""
import shutil
from pathlib import Path

import config as C
import commun as U

spark, log = U.session("08_export_donnees")
DATA = C.J3 / "data"
TMP = "/tmp/tp3_export"
shutil.rmtree(TMP, ignore_errors=True)

EXPORTS = [  # (zone, jeu, nombre de lignes (None = complet), commentaire)
    ("gold", "ventes_mensuelles_canal", None, "complet"),
    ("gold", "ventes_categorie_annee", None, "complet"),
    ("gold", "ventes_magasin_annee", None, "complet"),
    ("gold", "retours_categorie", None, "complet"),
    ("gold", "retours_motif", None, "complet"),
    ("gold", "fait_ventes", 1000, "échantillon (1 000 lignes) : jeu complet dans HDFS"),
    ("silver", "store_sales", 1000, "échantillon (1 000 lignes)"),
    ("silver", "web_sales", 1000, "échantillon (1 000 lignes)"),
    ("silver", "store_returns", 1000, "échantillon (1 000 lignes)"),
    ("silver", "item", 1000, "échantillon (1 000 lignes)"),
    ("silver", "store", None, "complet"),
    ("silver", "reason", None, "complet"),
]

bilan = []
for zone, jeu, n, commentaire in EXPORTS:
    src = f"{C.SILVER if zone == 'silver' else C.GOLD}/{jeu}"
    df = spark.read.parquet(src)
    total = df.count()
    if n:
        tri = "ligne_id" if "ligne_id" in df.columns else df.columns[0]
        df = df.orderBy(tri).limit(n)          # échantillon déterministe (même résultat à chaque exécution)
    tmp = f"{TMP}/{zone}/{jeu}"
    df.coalesce(1).write.mode("overwrite").parquet(f"file://{tmp}")
    part = next(Path(tmp).glob("part-*.parquet"))
    cible = DATA / zone / f"{jeu}.parquet"
    cible.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(part, cible)
    relu = spark.read.parquet(f"file://{tmp}")
    bilan.append({"zone": zone, "fichier": f"data/{zone}/{jeu}.parquet", "lignes": relu.count(),
                  "lignes_dans_hdfs": total, "colonnes": len(relu.columns), "octets": cible.stat().st_size,
                  "contenu": commentaire, "source_hdfs": src})
    log.info("%s -> %s (%d / %d lignes)", src, cible, relu.count(), total)

U.ecrire_csv_local(bilan, DATA / "contenu_data.csv")
shutil.rmtree(TMP, ignore_errors=True)
spark.stop()
