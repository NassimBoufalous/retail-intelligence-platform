"""
Étape 7 – Production de la zone Gold (Parquet) à partir de Silver.

Jeux produits (catalogue détaillé : UC06_preuves/gold/catalogue_gold.json et UC06_transformation_lakehouse) :
    fait_ventes             1 ligne de vente (magasin + web harmonisés)            -> base des analyses J4
    ventes_mensuelles_canal 1 ligne par année x mois x canal                       -> Q1, Q2, Q7
    ventes_categorie_annee  1 ligne par année x canal x catégorie                  -> Q3
    ventes_magasin_annee    1 ligne par année x magasin                            -> Q6
    retours_categorie       1 ligne par année de vente x canal x catégorie         -> Q8 (taux de retour)
    retours_motif           1 ligne par canal x motif de retour                    -> Q8 (motifs)

Chaque jointure est contrôlée : lignes avant / après, lignes non rapprochées (UC06_preuves/gold/jointures.csv).
"""
import config as C
import commun as U
from pyspark.sql import functions as F

spark, log = U.session("04_gold")
OUT = C.PREUVES / "gold"
CALCULE_LE = F.lit(U.horodatage()).cast("timestamp")
jointures, bilan, schemas, apercus = [], [], {}, []

ss = spark.read.parquet(f"{C.SILVER}/store_sales")
ws = spark.read.parquet(f"{C.SILVER}/web_sales")
sr = spark.read.parquet(f"{C.SILVER}/store_returns")
wr = spark.read.parquet(f"{C.SILVER}/web_returns")
item = spark.read.parquet(f"{C.SILVER}/item")
store = spark.read.parquet(f"{C.SILVER}/store")
reason = spark.read.parquet(f"{C.SILVER}/reason")


def jointure_controlee(nom, gauche, droite, cle_g, cle_d, cardinalite, comment="left"):
    """Jointure gauche avec contrôle : la clé de droite doit être unique (sinon la jointure multiplierait les
    lignes et fausserait les sommes), le nombre de lignes doit être conservé, les non-rapprochés sont comptés."""
    n_droite, n_cles = droite.count(), droite.select(cle_d).distinct().count()
    avant = gauche.count()
    res = gauche.join(droite, gauche[cle_g] == droite[cle_d], comment)
    apres = res.count()
    non_rapproches = res.filter(droite[cle_d].isNull()).count()
    ligne = {"jointure": nom, "cle_gauche": cle_g, "cle_droite": cle_d, "cardinalite_attendue": cardinalite,
             "type": comment, "cle_droite_unique": n_droite == n_cles, "lignes_avant": avant,
             "lignes_apres": apres, "lignes_conservees": avant == apres, "non_rapprochees": non_rapproches}
    jointures.append(ligne)
    log.info("Jointure %s : %s", nom, ligne)
    if not (ligne["cle_droite_unique"] and ligne["lignes_conservees"]):
        raise SystemExit(f"Jointure {nom} non conforme : {ligne}")
    return res


def enregistrer(nom, df, finalite, partition=None):
    df = df.withColumn("gold_calcule_le", CALCULE_LE)
    U.ecrire_parquet(df if partition else df.coalesce(1), f"{C.GOLD}/{nom}", log, partition)
    relu = spark.read.parquet(f"{C.GOLD}/{nom}")
    n = relu.count()
    bilan.append({"jeu": nom, "lignes": n, "colonnes": len(relu.columns), "finalite": finalite,
                  "fichiers_parquet": U.nb_fichiers(spark, f"{C.GOLD}/{nom}"),
                  "octets": U.taille(spark, f"{C.GOLD}/{nom}"),
                  "partitionnement": ", ".join(partition) if partition else "aucun", "chemin": f"{C.GOLD}/{nom}"})
    schemas[nom] = U.schema_vers_lignes(relu)
    apercus.append(f"== gold/{nom} ({n} lignes) ==\n{U.apercu_texte(relu, 10)}")
    return relu


# =========================================================================== fait_ventes (harmonisation)
def harmoniser(df, canal, p, doc, magasin, client):
    return df.select(
        F.lit(canal).alias("canal"), "ligne_id", "date_vente", "annee", "mois",
        F.col(f"{p}_{doc}").alias("document_id"),        # ticket (magasin) ou commande (web)
        F.col(f"{p}_item_sk").alias("item_sk"),
        (F.col(magasin) if magasin else F.lit(None).cast("bigint")).alias("store_sk"),
        F.col(client).alias("client_sk"), F.col(f"{p}_promo_sk").alias("promo_sk"),
        F.col(f"{p}_quantity").alias("quantite"),
        F.col(f"{p}_sales_price").alias("prix_unitaire"),
        F.col(f"{p}_ext_sales_price").alias("montant_ht"),          # CA = K01 du Jour 1
        F.col(f"{p}_ext_discount_amt").alias("remise"),
        F.col(f"{p}_net_paid").alias("montant_net_paye"),
        F.col(f"{p}_ext_wholesale_cost").alias("cout_achat"),
        F.col(f"{p}_net_profit").alias("marge"), "marge_negative", "lot_source")


ventes = (harmoniser(ss, "magasin", "ss", "ticket_number", "ss_store_sk", "ss_customer_sk")
          .unionByName(harmoniser(ws, "web", "ws", "order_number", None, "ws_bill_customer_sk")))
cat = item.select(F.col("i_item_sk"), F.coalesce("i_category", F.lit("Non renseignée")).alias("categorie"),
                  F.col("i_category_id").alias("categorie_id"))
ventes = jointure_controlee("fait_ventes x item (catégorie)", ventes, cat, "item_sk", "i_item_sk", "N:1").drop("i_item_sk")
fait = enregistrer("fait_ventes", ventes, "Base détaillée des analyses (J4)", partition=["annee"])

# =========================================================================== agrégats


def mesures():
    return [F.sum("montant_ht").alias("ca_ht"), F.sum("quantite").alias("quantite"),
            F.count("*").alias("lignes"), F.countDistinct("document_id").alias("nb_tickets_commandes"),
            F.sum("montant_net_paye").alias("montant_net_paye"), F.sum("remise").alias("remise")]


def panier(df):
    return df.withColumn("panier_moyen", F.round(F.col("ca_ht") / F.col("nb_tickets_commandes"), 2))


g1 = panier(fait.groupBy("annee", "mois", "canal").agg(*mesures(), F.sum("marge").alias("marge"))
            ).orderBy("annee", "mois", "canal")
enregistrer("ventes_mensuelles_canal", g1, "Q1 évolution du CA, Q2 répartition par canal, Q7 panier moyen")

g2 = (fait.groupBy("annee", "canal", "categorie_id", "categorie")
      .agg(F.sum("montant_ht").alias("ca_ht"), F.sum("quantite").alias("quantite"), F.count("*").alias("lignes"),
           F.countDistinct("item_sk").alias("nb_produits_vendus"))
      .orderBy("annee", "canal", F.desc("ca_ht")))
enregistrer("ventes_categorie_annee", g2, "Q3 catégories qui génèrent le plus de CA et de quantités")

mag = store.select("s_store_sk", "s_store_id", "s_store_name", "s_city", "s_state")
fm = jointure_controlee("fait_ventes(magasin) x store", fait.filter("canal = 'magasin'"), mag,
                        "store_sk", "s_store_sk", "N:1")
g3 = panier(fm.groupBy("annee", "s_store_sk", "s_store_id", "s_store_name", "s_city", "s_state")
            .agg(*mesures())).orderBy("annee", F.desc("ca_ht"))
enregistrer("ventes_magasin_annee", g3, "Q6 CA et tickets par magasin, magasins en retrait")


# =========================================================================== retours
def retours_harmonises(df, canal, p, doc):
    return df.select(F.lit(canal).alias("canal"), "ligne_id", "date_retour", "date_vente_origine",
                     F.col(f"{p}_{doc}").alias("document_id"), F.col(f"{p}_item_sk").alias("item_sk"),
                     F.col(f"{p}_reason_sk").alias("reason_sk"),
                     F.col(f"{p}_return_quantity").alias("quantite_retournee"),
                     F.col(f"{p}_return_amt").alias("montant_retour"), "vente_trouvee", "lot_source")


ret = (retours_harmonises(sr, "magasin", "sr", "ticket_number")
       .unionByName(retours_harmonises(wr, "web", "wr", "order_number")))
ret = jointure_controlee("retours x item (catégorie)", ret, cat, "item_sk", "i_item_sk", "N:1").drop("i_item_sk")

# Taux de retour : numérateur et dénominateur au MÊME grain (année de la vente d'origine x canal x catégorie) ;
# les retours sans vente retrouvée (R11) sont exclus du taux, mais comptés à part pour rester visibles.
num = (ret.filter("vente_trouvee").withColumn("annee", F.year("date_vente_origine"))
       .groupBy("annee", "canal", "categorie_id", "categorie")
       .agg(F.sum("quantite_retournee").alias("quantite_retournee"),
            F.sum("montant_retour").alias("montant_retours"), F.count("*").alias("lignes_retour")))
den = (fait.groupBy("annee", "canal", "categorie_id", "categorie")
       .agg(F.sum("quantite").alias("quantite_vendue"), F.sum("montant_ht").alias("ca_ht")))
g4 = (den.join(num, ["annee", "canal", "categorie_id", "categorie"], "left").fillna(0, ["quantite_retournee",
                                                                                         "lignes_retour"])
      .withColumn("montant_retours", F.coalesce("montant_retours", F.lit(0).cast("decimal(17,2)")))
      .withColumn("taux_retour_quantite_pct", F.round(100 * F.col("quantite_retournee") / F.col("quantite_vendue"), 3))
      .withColumn("taux_retour_montant_pct", F.round(100 * F.col("montant_retours") / F.col("ca_ht"), 3))
      .orderBy("annee", "canal", F.desc("taux_retour_quantite_pct")))
enregistrer("retours_categorie", g4, "Q8 taux de retour par catégorie (quantité et montant)")

motif = reason.select("r_reason_sk", F.col("r_reason_desc").alias("motif"))
rm = jointure_controlee("retours x reason (motif)", ret, motif, "reason_sk", "r_reason_sk", "N:1")
g5 = (rm.withColumn("motif", F.coalesce("motif", F.lit("Motif non renseigné")))
      .groupBy("canal", "reason_sk", "motif")
      .agg(F.count("*").alias("lignes_retour"), F.sum("quantite_retournee").alias("quantite_retournee"),
           F.sum("montant_retour").alias("montant_retours"),
           F.sum((~F.col("vente_trouvee")).cast("int")).alias("dont_sans_vente_retrouvee"))
      .orderBy("canal", F.desc("montant_retours")))
enregistrer("retours_motif", g5, "Q8 principaux motifs de retour")

# =========================================================================== preuves
U.ecrire_csv_local(jointures, OUT / "jointures.csv")
U.ecrire_csv_local(bilan, OUT / "bilan_gold.csv")
U.ecrire_csv_local([dict(jeu=t, **c) for t, cs in schemas.items() for c in cs], OUT / "schemas_gold.csv")
(OUT / "apercus_gold.txt").write_text("\n".join(apercus), encoding="utf-8")
# Exports CSV des petits agrégats (lecture directe dans les documents et au Jour 4)
for nom in ["ventes_mensuelles_canal", "ventes_categorie_annee", "ventes_magasin_annee", "retours_categorie",
            "retours_motif"]:
    rows = [r.asDict() for r in spark.read.parquet(f"{C.GOLD}/{nom}").drop("gold_calcule_le").collect()]
    U.ecrire_csv_local(rows, OUT / "extraits" / f"{nom}.csv")
log.info("Gold terminé : %s", [b["jeu"] for b in bilan])
spark.stop()
