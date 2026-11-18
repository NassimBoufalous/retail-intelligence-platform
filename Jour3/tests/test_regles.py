"""
Tests des règles de nettoyage Silver sur des lignes VOLONTAIREMENT fausses.

Pourquoi ? Sur les données du kit, la quarantaine est vide (données synthétiques propres). Un résultat
« 0 rejet » ne prouve rien si l'on n'a pas vérifié que les règles savent détecter une anomalie.
Chaque test fabrique une petite table avec une anomalie connue et vérifie le motif de quarantaine obtenu.

    bash Jour3/tests/lancer_tests.sh       (aucun accès HDFS nécessaire : Spark local, données en mémoire)
"""
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "transformations"))
import commun as U  # noqa: E402
import regles as RG  # noqa: E402
from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

spark = (SparkSession.builder.appName("TP3-tests-regles").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
resultats = []


def verifier(nom, condition, detail=""):
    resultats.append((nom, bool(condition), detail))
    print(f"[{'OK ' if condition else 'ÉCHEC'}] {nom} {detail}")


# --------------------------------------------------------------------------- 1. typage (R1, R2)
cols = U.SCHEMA["store_sales"]
base = {c: "1" for c in cols}
base.update(ss_quantity="2", ss_sales_price="10.00", ss_ext_sales_price="20.00")
lignes = [dict(base),                                    # ligne correcte
          dict(base, ss_quantity="deux"),                # texte dans une quantité -> non convertible
          dict(base, ss_promo_sk="   "),                 # espaces seuls -> null (R2), pas une erreur
          dict(base, ss_net_profit="12345678.99")]       # dépasse DECIMAL(7,2) -> non convertible
brut = spark.createDataFrame(lignes, U.schema_texte("store_sales"))
typ, echecs = U.typer(brut, "store_sales")
r = typ.select("ss_quantity", "ss_promo_sk", "_conv_ok").collect()
verifier("R1 conversion valide", r[0]["_conv_ok"] and r[0]["ss_quantity"] == 2)
verifier("R1 texte dans un entier détecté", r[1]["_conv_ok"] is False, f"(échecs={echecs})")
verifier("R2 chaîne vide -> null, ligne conservée", r[2]["ss_promo_sk"] is None and r[2]["_conv_ok"])
verifier("R1 dépassement DECIMAL(7,2) détecté", r[3]["_conv_ok"] is False)
verifier("R1 types cibles appliqués", dict(typ.dtypes)["ss_ext_sales_price"] == "decimal(7,2)"
         and dict(typ.dtypes)["ss_quantity"] == "int" and dict(typ.dtypes)["ss_item_sk"] == "bigint")

# --------------------------------------------------------------------------- 2. règles des ventes (R5-R9)
sch = T.StructType([T.StructField(n, t) for n, t in [
    ("cas", T.StringType()), ("ss_sold_date_sk", T.LongType()), ("ss_item_sk", T.LongType()),
    ("ss_ticket_number", T.LongType()), ("ss_quantity", T.IntegerType()),
    ("ss_sales_price", T.DecimalType(7, 2)), ("ss_ext_sales_price", T.DecimalType(7, 2)),
    ("date_vente", T.DateType()), ("_item_ok", T.LongType()), ("_conv_ok", T.BooleanType())]])
d, P = date(2003, 5, 1), Decimal
cas = [("valide", 1, 10, 100, 2, P("10.00"), P("20.00"), d, 10, True),
       ("arrondi_tolere", 1, 10, 101, 3, P("3.33"), P("9.99"), d, 10, True),
       ("non_convertible", 1, 10, 102, 2, P("10.00"), P("20.00"), d, 10, False),
       ("ticket_nul", 1, 10, None, 2, P("10.00"), P("20.00"), d, 10, True),
       ("quantite_zero", 1, 10, 103, 0, P("10.00"), P("0.00"), d, 10, True),
       ("quantite_negative", 1, 10, 104, -1, P("10.00"), P("-10.00"), d, 10, True),
       ("montant_incoherent", 1, 10, 105, 2, P("10.00"), P("25.00"), d, 10, True),
       ("date_inconnue", 999, 10, 106, 2, P("10.00"), P("20.00"), None, 10, True),
       ("produit_inconnu", 1, 77, 107, 2, P("10.00"), P("20.00"), d, None, True),
       ("plusieurs_anomalies", 1, 77, 108, 0, P("10.00"), P("20.00"), None, None, True)]
df = spark.createDataFrame(cas, sch)
ok, q = RG.separer(df, RG.regles_ventes("ss", "ticket_number"))
motifs = {r["cas"]: r["motif_quarantaine"] for r in q.collect()}
gardes = {r["cas"] for r in ok.collect()}
attendus = {"non_convertible": "R1_type_non_convertible", "ticket_nul": "R5_champ_obligatoire_nul",
            "quantite_zero": "R6_quantite_non_positive", "quantite_negative": "R6_quantite_non_positive",
            "montant_incoherent": "R7_montant_incoherent", "date_inconnue": "R8_date_inconnue",
            "produit_inconnu": "R9_produit_inconnu", "plusieurs_anomalies": "R6_quantite_non_positive"}
for c, m in attendus.items():
    verifier(f"ventes : {c} -> {m}", motifs.get(c) == m, f"(obtenu {motifs.get(c)})")
verifier("ventes : lignes valides conservées", gardes == {"valide", "arrondi_tolere"}, f"{sorted(gardes)}")
verifier("ventes : aucune ligne perdue", ok.count() + q.count() == df.count())

# --------------------------------------------------------------------------- 3. règles des retours
sch_r = T.StructType([T.StructField(n, t) for n, t in [
    ("cas", T.StringType()), ("sr_returned_date_sk", T.LongType()), ("sr_item_sk", T.LongType()),
    ("sr_ticket_number", T.LongType()), ("sr_return_quantity", T.IntegerType()),
    ("sr_return_amt", T.DecimalType(7, 2)), ("date_retour", T.DateType()), ("_item_ok", T.LongType()),
    ("_conv_ok", T.BooleanType())]])
cas_r = [("valide", 1, 10, 100, 1, P("5.00"), d, 10, True),
         ("montant_nul", 1, 10, 100, 1, None, d, 10, True),
         ("quantite_zero", 1, 10, 100, 0, P("0.00"), d, 10, True)]
okr, qr = RG.separer(spark.createDataFrame(cas_r, sch_r), RG.regles_retours("sr", "ticket_number"))
mr = {r["cas"]: r["motif_quarantaine"] for r in qr.collect()}
verifier("retours : montant nul -> R5", mr.get("montant_nul") == "R5_champ_obligatoire_nul")
verifier("retours : quantité 0 -> R6", mr.get("quantite_zero") == "R6_quantite_non_positive")
verifier("retours : ligne valide conservée", [r["cas"] for r in okr.collect()] == ["valide"])

# --------------------------------------------------------------------------- 4. référentiel (R12)
ref = spark.createDataFrame([(1, False), (2, False), (2, False), (None, False)], "i_item_sk long, x boolean")
dup = ref.groupBy("i_item_sk").count().filter("count > 1").select("i_item_sk").withColumn("_dup", F.lit(True))
ref = ref.join(dup, "i_item_sk", "left").withColumn("_conv_ok", F.lit(True))
okf, qf = RG.separer(ref, RG.regles_referentiel("i_item_sk"))
mf = sorted(r["motif_quarantaine"] for r in qf.collect())
verifier("référentiel : clé dupliquée et clé nulle isolées",
         mf == ["R12_cle_dupliquee", "R12_cle_dupliquee", "R12_cle_nulle"], f"{mf}")

# --------------------------------------------------------------------------- 5. clé technique (R4)
k = spark.createDataFrame([("a", 1), ("a", 1), ("a", 2), (None, 1)], "x string, y int")
k = k.withColumn("id", RG.cle_technique(k, ["x", "y"], "magasin")).collect()
verifier("R4 clé déterministe (même contenu -> même clé)", k[0]["id"] == k[1]["id"])
verifier("R4 contenus différents -> clés différentes", len({k[0]["id"], k[2]["id"], k[3]["id"]}) == 3)
k2 = spark.createDataFrame([("a", 1)], "x string, y int")
verifier("R4 le canal fait partie de la clé",
         k2.select(RG.cle_technique(k2, ["x", "y"], "web")).first()[0] != k[0]["id"])

n_ok = sum(1 for _, ok_, _ in resultats if ok_)
print(f"\n{n_ok}/{len(resultats)} tests réussis")
spark.stop()
sys.exit(0 if n_ok == len(resultats) else 1)
