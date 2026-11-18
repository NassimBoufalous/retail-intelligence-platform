"""
Étapes 4 à 6 – Règles de nettoyage et production de la zone Silver (Parquet).

Règles appliquées (détail et justification : UC06_regles_nettoyage) :
    R1  typage explicite (types de la spécification TPCx-BB) ; valeur non convertible -> quarantaine
    R2  texte : espaces de début et de fin retirés, chaîne vide -> null
    R3  provenance : lot_source, fichier_source, ingest_date, silver_traite_le
    R4  clé technique ligne_id (SHA-256 du contenu de la ligne) pour les faits sans clé naturelle
    R5  champs obligatoires des faits non nuls (date, article, ticket/commande, quantité, montant) sinon quarantaine
    R6  quantité vendue ou retournée > 0, sinon quarantaine
    R7  cohérence ext_sales_price = sales_price x quantity (à 0,01 près), sinon quarantaine
    R8  date de vente / de retour : rattachement à date_dim (date réelle, année, mois) ; clé inconnue -> quarantaine
    R9  rattachement au produit (item) ; produit inconnu -> quarantaine
    R10 marges négatives et nulls des colonnes facultatives : CONSERVÉS (synthétique / absence légitime), signalés
    R11 retours : rattachement à la vente d'origine (canal + ticket/commande + article) ; retour sans vente
        -> CONSERVÉ et signalé (vente_trouvee = false), exclu du taux de retour en Gold
    R11b retours cumulés (tous lots) > quantité vendue -> CONSERVÉ et signalé (retour_excedentaire = true)
    R12 référentiels : union des lots (initial + refresh), unicité de la clé contrôlée
    R13 colonnes vides à 100 % (i_rec_end_date, s_rec_end_date) conservées pour garder le schéma source

Sorties HDFS : silver/<table>/ (Parquet), quality/quarantaine/<table>/ (Parquet, avec le motif)
Preuves locales : UC06_preuves/silver/ (bilan des volumes, schémas, aperçus)
"""
import config as C
import commun as U
import regles as RG
from pyspark.sql import Window, functions as F

spark, log = U.session("03_silver")
OUT = C.PREUVES / "silver"
TRAITE_LE = F.lit(U.horodatage()).cast("timestamp")
bilan, schemas, apercus = [], {}, []


def typer_avec_controle(table):
    """R1 + R2 + R3 : lecture Bronze en texte, conversion, et marquage des lignes non convertibles."""
    brut = U.lire_bronze(spark, table)
    n_bronze = brut.count()
    df, echecs = U.typer(brut, table, log)   # _conv_ok = False si une valeur non vide n'a pas été convertie
    df = df.withColumn("silver_traite_le", TRAITE_LE)
    return df, n_bronze, echecs



def enregistrer(table, valides, quarantaine, n_bronze, echecs, partition=None):
    valides = valides.drop("_conv_ok").cache()
    quarantaine = quarantaine.drop("_conv_ok")
    n_ok, n_q = valides.count(), quarantaine.count()
    U.ecrire_parquet(valides, f"{C.SILVER}/{table}", log, partition)
    U.ecrire_parquet(quarantaine, f"{C.QUALITY}/quarantaine/{table}", log)
    motifs = {r["motif_quarantaine"]: r["count"] for r in quarantaine.groupBy("motif_quarantaine").count().collect()}
    relu = spark.read.parquet(f"{C.SILVER}/{table}")
    n_relu = relu.count()
    bilan.append({"table": table, "lignes_bronze": n_bronze, "lignes_silver": n_ok, "lignes_quarantaine": n_q,
                  "bronze_egal_silver_plus_quarantaine": n_bronze == n_ok + n_q, "lignes_relues": n_relu,
                  "motifs_quarantaine": "; ".join(f"{k}={v}" for k, v in sorted(motifs.items())) or "aucun",
                  "conversions_en_echec": sum(echecs.values()), "colonnes": len(relu.columns),
                  "fichiers_parquet": U.nb_fichiers(spark, f"{C.SILVER}/{table}"),
                  "octets_parquet": U.taille(spark, f"{C.SILVER}/{table}"),
                  "partitionnement": ", ".join(partition) if partition else "aucun",
                  "chemin": f"{C.SILVER}/{table}"})
    schemas[table] = U.schema_vers_lignes(relu)
    apercus.append(f"== silver/{table} ({n_relu} lignes) ==\n{U.apercu_texte(relu, 5)}")
    log.info("%s : bronze=%d silver=%d quarantaine=%d %s", table, n_bronze, n_ok, n_q, motifs)
    valides.unpersist()
    return relu


# =========================================================================== référentiels (R12)
def referentiel(table, cle):
    df, n, echecs = typer_avec_controle(table)
    w_cle_dupliquee = (df.groupBy(cle).count().filter("count > 1").select(cle).withColumn("_dup", F.lit(True)))
    df = df.join(w_cle_dupliquee, cle, "left")
    ok, q = RG.separer(df, RG.regles_referentiel(cle))
    return enregistrer(table, ok.drop("_dup"), q.drop("_dup"), n, echecs)


dates = referentiel("date_dim", "d_date_sk")
items = referentiel("item", "i_item_sk")
referentiel("store", "s_store_sk")
referentiel("reason", "r_reason_sk")

cal = F.broadcast(dates.select("d_date_sk", "d_date", "d_year", "d_moy"))
item_keys = F.broadcast(items.select(F.col("i_item_sk").alias("_item_ok")))


# =========================================================================== ventes
def ventes(table, canal, p):
    """p = préfixe des colonnes (ss / ws). Applique R4 à R10."""
    df, n, echecs = typer_avec_controle(table)
    cols = U.SCHEMA[table]
    doc = "ticket_number" if canal == "magasin" else "order_number"
    df = (df.withColumn("ligne_id", RG.cle_technique(df, cols, canal))
            .withColumn("canal", F.lit(canal))
            .join(cal, F.col(f"{p}_sold_date_sk") == F.col("d_date_sk"), "left")
            .withColumnRenamed("d_date", "date_vente").withColumnRenamed("d_year", "annee")
            .withColumnRenamed("d_moy", "mois").drop("d_date_sk")
            .join(item_keys, F.col(f"{p}_item_sk") == F.col("_item_ok"), "left")
            .withColumn("marge_negative", F.col(f"{p}_net_profit") < 0))           # R10 : signalé, conservé
    if canal == "web":  # cohérence spécifique au web : expédition après la vente (P5 : 1 à 120 jours)
        df = df.withColumn("delai_expedition_j", F.col("ws_ship_date_sk") - F.col("ws_sold_date_sk"))
    ok, q = RG.separer(df, RG.regles_ventes(p, doc))
    return enregistrer(table, ok.drop("_item_ok"), q.drop("_item_ok"), n, echecs, partition=["annee"])


ss = ventes("store_sales", "magasin", "ss")
ws = ventes("web_sales", "web", "ws")


# =========================================================================== retours
def retours(table, canal, p, ventes_df, pv, doc_r, doc_v):
    df, n, echecs = typer_avec_controle(table)
    cols = U.SCHEMA[table]
    # R11 : rattachement à la vente (même canal, même ticket/commande, même article)
    v = (ventes_df.groupBy(F.col(f"{pv}_{doc_v}").alias("_doc"), F.col(f"{pv}_item_sk").alias("_it"))
         .agg(F.min("date_vente").alias("date_vente_origine"), F.sum(f"{pv}_quantity").alias("_qte_vendue")))
    date_col = f"{p}_returned_date_sk"
    df = (df.withColumn("ligne_id", RG.cle_technique(df, cols, canal))
            .withColumn("canal", F.lit(canal))
            .join(cal, F.col(date_col) == F.col("d_date_sk"), "left")
            .withColumnRenamed("d_date", "date_retour").withColumnRenamed("d_year", "annee")
            .withColumnRenamed("d_moy", "mois").drop("d_date_sk")
            .join(item_keys, F.col(f"{p}_item_sk") == F.col("_item_ok"), "left")
            .join(v, (F.col(f"{p}_{doc_r}") == F.col("_doc")) & (F.col(f"{p}_item_sk") == F.col("_it")), "left")
            .withColumn("vente_trouvee", F.col("_doc").isNotNull())
            .withColumn("retour_avant_vente", F.col("date_retour") < F.col("date_vente_origine"))
            # R11b : retours cumulés (tous lots) supérieurs à la quantité vendue -> conservé et signalé
            .withColumn("_qte_retournee_cumul", F.sum(f"{p}_return_quantity").over(
                Window.partitionBy(f"{p}_{doc_r}", f"{p}_item_sk")))
            .withColumn("retour_excedentaire", F.col("vente_trouvee")
                        & (F.col("_qte_retournee_cumul") > F.col("_qte_vendue")))
            .drop("_doc", "_it", "_qte_vendue", "_qte_retournee_cumul"))
    ok, q = RG.separer(df, RG.regles_retours(p, doc_r))
    return enregistrer(table, ok.drop("_item_ok"), q.drop("_item_ok"), n, echecs)


retours("store_returns", "magasin", "sr", ss, "ss", "ticket_number", "ticket_number")
retours("web_returns", "web", "wr", ws, "ws", "order_number", "order_number")

# =========================================================================== preuves
U.ecrire_csv_local(bilan, OUT / "bilan_silver.csv")
U.ecrire_csv_local([dict(table=t, **c) for t, cs in schemas.items() for c in cs], OUT / "schemas_silver.csv")
(OUT / "apercus_silver.txt").write_text("\n".join(apercus), encoding="utf-8")
log.info("Silver terminé : %d tables", len(bilan))
spark.stop()
