"""
Fonctions communes aux étapes du pipeline UC-06 : session Spark, journalisation, schémas explicites,
lecture de la zone Bronze, accès au système de fichiers (HDFS ou local) et écriture des preuves.
"""
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import config as C

# Le schéma des colonnes (fichiers sans en-tête) est celui établi et vérifié au Jour 1
sys.path.insert(0, str(C.J3.parent / "Jour1" / "scripts"))
from schema_tpcxbb import SCHEMA  # noqa: E402

from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402


# --------------------------------------------------------------------------- session et journal
def session(nom_etape):
    """Crée (ou récupère) la session Spark et un journal écrit dans UC06_preuves/journaux/<étape>.log."""
    C.JOURNAUX.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(nom_etape)
    log.setLevel(logging.INFO)
    if not log.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
        for h in (logging.FileHandler(C.JOURNAUX / f"{nom_etape}.log", mode="w", encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)):
            h.setFormatter(fmt)
            log.addHandler(h)
    b = SparkSession.builder.appName(f"{C.SPARK_APP}-{nom_etape}").master(C.SPARK_MASTER)
    for k, v in C.SPARK_CONF.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log.info("Session Spark %s (master=%s), lac=%s", spark.version, spark.sparkContext.master, C.LAKE)
    return spark, log


class Chrono:
    """Mesure la durée d'un bloc : with Chrono(log, 'lecture'): ..."""

    def __init__(self, log, libelle):
        self.log, self.libelle = log, libelle

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.duree = time.perf_counter() - self.t0
        self.log.info("%s : %.1f s", self.libelle, self.duree)


# --------------------------------------------------------------------------- types explicites
MONTANT = T.DecimalType(7, 2)   # spécification TPCx-BB v1.7.0, annexe B : DECIMAL(7,2)

MOTS_MONTANT = ("price", "_amt", "cost", "paid", "profit", "_tax", "_fee", "cash", "charge", "credit",
                "loss", "gmt_offset", "precentage", "percentage")


def type_cible(table, col):
    """Type Spark attendu pour une colonne, d'après les règles de type de la spécification TPCx-BB."""
    if col.endswith("_sk") or col in ("d_month_seq", "d_week_seq", "d_quarter_seq", "d_fy_year",
                                     "d_fy_quarter_seq", "d_fy_week_seq", "d_first_dom", "d_last_dom",
                                     "d_same_day_ly", "d_same_day_lq", "ss_ticket_number",
                                     "sr_ticket_number", "ws_order_number", "wr_order_number"):
        return T.LongType()
    if col in ("d_date", "i_rec_start_date", "i_rec_end_date", "s_rec_start_date", "s_rec_end_date"):
        return T.DateType()
    if "quantity" in col or col in ("d_year", "d_dow", "d_moy", "d_dom", "d_qoy", "s_number_employees",
                                    "s_floor_space", "s_market_id", "s_division_id", "s_company_id",
                                    "i_brand_id", "i_class_id", "i_category_id", "i_manufact_id",
                                    "i_manager_id"):
        return T.IntegerType()
    if any(m in col for m in MOTS_MONTANT) and not col.endswith("_id"):
        return MONTANT
    return T.StringType()


def schema_texte(table):
    """Schéma de lecture Bronze : toutes les colonnes en texte. Les conversions sont faites ensuite, une à une,
    pour pouvoir COMPTER les valeurs qui ne se convertissent pas (au lieu de les perdre silencieusement)."""
    return T.StructType([T.StructField(c, T.StringType(), True) for c in SCHEMA[table]])


# --------------------------------------------------------------------------- lecture Bronze
def chemin_bronze(lot, table):
    return f"{C.BRONZE}/{lot}/{table}/ingest_date={C.INGEST_DATE}"


def lire_bronze(spark, table, lots=None):
    """Lit une table Bronze (tous les lots) en texte, avec les colonnes de provenance :
    lot_source, fichier_source, ingest_date."""
    dfs = []
    for lot in lots or C.LOTS:
        p = chemin_bronze(lot, table)
        if not existe(spark, p):
            continue
        df = (spark.read.option("sep", "|").option("header", False).option("quote", "")
              .option("escape", "").option("mode", "PERMISSIVE").schema(schema_texte(table))
              .csv(f"{p}/{table}.dat")
              .withColumn("lot_source", F.lit(lot))
              .withColumn("fichier_source", F.input_file_name())
              .withColumn("ingest_date", F.lit(C.INGEST_DATE).cast("date")))
        dfs.append(df)
    if not dfs:
        raise FileNotFoundError(f"Aucune donnée Bronze pour {table} ({C.BRONZE})")
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d)
    return out


def typer(df, table, log=None):
    """Convertit chaque colonne vers son type cible. Renvoie (df_typé, {colonne: nb_valeurs_non_convertibles}).
    Une valeur vide ("") est traitée comme absente (null) : c'est la convention des fichiers TPCx-BB.
    Le df renvoyé contient une colonne technique _conv_ok (False si au moins une valeur non vide de la ligne
    n'a pas pu être convertie) : la ligne n'est pas perdue, elle pourra être mise en quarantaine."""
    echecs_expr, sel, ok_ligne = [], [], F.lit(True)
    for c in SCHEMA[table]:
        brut = F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
        t = type_cible(table, c)
        # try_cast et non cast : Spark 4 active le mode ANSI par défaut, où cast() fait ÉCHOUER tout le job
        # sur une seule valeur mal formée ; try_cast renvoie null, ce qui permet de mettre la ligne en quarantaine
        conv = brut.try_cast(t) if not isinstance(t, T.StringType) else brut
        sel.append(conv.alias(c))
        if not isinstance(t, T.StringType):
            echec = brut.isNotNull() & conv.isNull()
            echecs_expr.append(F.sum(F.when(echec, 1).otherwise(0)).alias(c))
            ok_ligne = ok_ligne & ~echec
    autres = [c for c in df.columns if c not in SCHEMA[table]]
    echecs = df.agg(*echecs_expr).first().asDict() if echecs_expr else {}
    echecs = {k: int(v or 0) for k, v in echecs.items() if v}
    if log:
        log.info("%s : conversions de type en échec = %s", table, echecs or "aucune")
    return df.select(*sel, *autres, ok_ligne.alias("_conv_ok")), echecs


# --------------------------------------------------------------------------- système de fichiers
def _fs(spark, chemin):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(chemin)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def existe(spark, chemin):
    fs, p = _fs(spark, chemin)
    return fs.exists(p)


def taille(spark, chemin):
    """Taille totale en octets d'un fichier ou d'un répertoire (tous ses fichiers)."""
    fs, p = _fs(spark, chemin)
    return int(fs.getContentSummary(p).getLength())


def nb_fichiers(spark, chemin, suffixe=".parquet"):
    fs, p = _fs(spark, chemin)
    it = fs.listFiles(p, True)
    n = 0
    while it.hasNext():
        if it.next().getPath().getName().endswith(suffixe):
            n += 1
    return n


def copier(spark, source, cible):
    """Copie un fichier dans le même système de fichiers (Raw -> Bronze), sans modifier la source."""
    jvm = spark._jvm
    fs_s, p_s = _fs(spark, source)
    fs_c, p_c = _fs(spark, cible)
    fs_c.mkdirs(p_c.getParent())
    return jvm.org.apache.hadoop.fs.FileUtil.copy(fs_s, p_s, fs_c, p_c, False, True,
                                                  spark._jsc.hadoopConfiguration())


def checksum(spark, chemin):
    """Somme de contrôle HDFS (MD5 des CRC des blocs) : identique si le contenu est identique."""
    fs, p = _fs(spark, chemin)
    cs = fs.getFileChecksum(p)
    return None if cs is None else f"{cs.getAlgorithmName()}:{bytes(cs.getBytes()).hex()}"


# --------------------------------------------------------------------------- écriture des résultats
def ecrire_parquet(df, chemin, log, partition=None):
    """Écrit un jeu en Parquet. mode overwrite : uniquement sur les répertoires de sortie du pipeline
    (silver/, gold/, quality/), jamais sur raw/ ni bronze/ (garde-fou ci-dessous)."""
    if "/raw/" in chemin or "/bronze/" in chemin:
        raise ValueError(f"Écriture interdite dans une zone source : {chemin}")
    w = df.write.mode("overwrite")
    if partition:
        w = w.partitionBy(*partition)
    w.parquet(chemin)
    log.info("Écrit : %s", chemin)


def ecrire_csv_local(lignes, chemin, entetes=None):
    """Écrit une preuve CSV locale (séparateur ;, UTF-8 avec BOM pour Excel)."""
    chemin = Path(chemin)
    chemin.parent.mkdir(parents=True, exist_ok=True)
    lignes = list(lignes)
    if entetes is None:
        entetes = list(lignes[0].keys()) if lignes else []
    with open(chemin, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=entetes, delimiter=";", extrasaction="ignore")
        w.writeheader()
        for l in lignes:
            w.writerow({k: ("" if v is None else v) for k, v in l.items()})


def ecrire_json_local(obj, chemin):
    chemin = Path(chemin)
    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def schema_vers_lignes(df):
    """Schéma d'un DataFrame sous forme de lignes (colonne, type, nullable) pour la documentation."""
    return [{"colonne": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable}
            for f in df.schema.fields]


def apercu_texte(df, n=10):
    """Capture du df.show() en texte (preuve d'exécution)."""
    return df._jdf.showString(n, 40, False)


def horodatage():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
