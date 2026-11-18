"""
Étape 9 – Contrôles qualité des zones Silver et Gold.

Chaque contrôle est défini explicitement (TP §4.12, cours J3) : identifiant, dimension, jeu contrôlé, règle,
méthode, résultat mesuré, seuil, statut, traitement décidé. Dimensions couvertes : complétude, unicité,
validité, cohérence, volume, intégrité référentielle, réconciliation (Gold = Silver = Jour 1) et fraîcheur.

Statuts : OK (règle respectée) / ANOMALIE (règle non respectée, traitée ou documentée) / INFO (mesure sans seuil).
Sorties : UC06_preuves/qualite/controles_qualite.csv (+ quality/controles/ dans le lac), resume_qualite.json
"""
import csv

import config as C
import commun as U
from pyspark.sql import functions as F

spark, log = U.session("05_qualite")
OUT = C.PREUVES / "qualite"
S = {t: spark.read.parquet(f"{C.SILVER}/{t}") for t in C.TABLES}
G = {g: spark.read.parquet(f"{C.GOLD}/{g}") for g in ["fait_ventes", "ventes_mensuelles_canal",
                                                     "ventes_categorie_annee", "ventes_magasin_annee",
                                                     "retours_categorie", "retours_motif"]}
controles = []


def ctrl(id_, dimension, jeu, regle, methode, resultat, ok=None, traitement="", seuil="0"):
    statut = "INFO" if ok is None else ("OK" if ok else "ANOMALIE")
    controles.append({"id": id_, "dimension": dimension, "jeu": jeu, "regle": regle, "methode": methode,
                      "resultat": resultat, "seuil": seuil, "statut": statut, "traitement": traitement})
    log.info("%s %-10s %-28s %s -> %s", id_, dimension, jeu, resultat, statut)


def nb(df, cond):
    return df.filter(cond).count()


VENTES = [("store_sales", "ss", "ticket_number", "magasin"), ("web_sales", "ws", "order_number", "web")]
RETOURS = [("store_returns", "sr", "ticket_number"), ("web_returns", "wr", "order_number")]

# =========================================================================== complétude
for t, p, doc, _ in VENTES:
    oblig = [f"{p}_sold_date_sk", f"{p}_item_sk", f"{p}_{doc}", f"{p}_quantity", f"{p}_ext_sales_price"]
    n = nb(S[t], " OR ".join(f"{c} IS NULL" for c in oblig))
    ctrl("Q01", "Complétude", f"silver/{t}", "Champs obligatoires non nuls (" + ", ".join(oblig) + ")",
         "comptage des lignes avec au moins un null", f"{n} ligne(s)", n == 0, "quarantaine (R5)")
    tot = S[t].count()
    promo = nb(S[t], f"{p}_promo_sk IS NULL")
    ctrl("Q02", "Complétude", f"silver/{t}", "Colonne facultative promo_sk", "taux de null",
         f"{promo} ({100 * promo / tot:.3f} %)", None, "conservé : vente sans promotion (R10)", "–")
for t, cols in [("item", ["i_category", "i_item_id", "i_current_price"]), ("store", ["s_store_name", "s_store_id"]),
                ("reason", ["r_reason_desc"])]:
    for c in cols:
        n = nb(S[t], f"{c} IS NULL")
        ctrl("Q03", "Complétude", f"silver/{t}", f"{c} renseigné", "comptage des null", f"{n}", n == 0,
             "libellé « Non renseignée » en Gold si null" if c == "i_category" else "")
for t, p, _ in RETOURS:
    n = nb(S[t], f"{p}_reason_sk IS NULL")
    ctrl("Q04", "Complétude", f"silver/{t}", "Motif de retour renseigné", "comptage des null", f"{n}", None,
         "conservé ; « Motif non renseigné » dans gold/retours_motif", "–")

# =========================================================================== unicité
for t in ["store_sales", "web_sales", "store_returns", "web_returns"]:
    n, d = S[t].count(), S[t].select("ligne_id").distinct().count()
    ctrl("Q05", "Unicité", f"silver/{t}", "Clé technique ligne_id unique (tous lots confondus)",
         "count vs count distinct", f"{n} lignes, {d} clés distinctes", n == d,
         "aucun doublon exact, y compris entre les lots initial et refresh")
for t, k in [("item", "i_item_sk"), ("store", "s_store_sk"), ("date_dim", "d_date_sk"), ("reason", "r_reason_sk"),
             ("item", "i_item_id")]:
    n, d = S[t].count(), S[t].select(k).distinct().count()
    ctrl("Q06", "Unicité", f"silver/{t}", f"{k} unique", "count vs count distinct", f"{n} / {d}", n == d,
         "quarantaine si doublon (R12)")
for t, p, doc, _ in VENTES:
    n = S[t].count()
    d = S[t].select(f"{p}_{doc}", f"{p}_item_sk").distinct().count()
    ctrl("Q07", "Unicité", f"silver/{t}", f"Grain : ({doc}, item) unique ?", "count vs count distinct",
         f"{n - d} lignes en trop", None,
         "attendu (même article sur 2 lignes d'un ticket) : clé technique ligne_id créée (R4)", "–")

# =========================================================================== validité
for t, df in S.items():
    attendu = {c: U.type_cible(t, c).simpleString() for c in U.SCHEMA[t]}
    obs = dict(df.dtypes)
    ecarts = [c for c, ty in attendu.items() if obs.get(c) != ty]
    ctrl("Q08", "Validité", f"silver/{t}", "Types conformes au schéma cible (spécification TPCx-BB)",
         "comparaison schéma Parquet relu / types cibles", f"{len(ecarts)} colonne(s) en écart {ecarts[:3]}",
         not ecarts, "typage explicite (R1)")
for t, p, _, _ in VENTES:
    n = nb(S[t], f"{p}_quantity <= 0 OR {p}_sales_price < 0 OR {p}_list_price < 0")
    ctrl("Q09", "Validité", f"silver/{t}", "Quantité > 0 et prix >= 0", "comptage", f"{n}", n == 0, "quarantaine (R6)")
    n = nb(S[t], f"{p}_sales_price > {p}_list_price")
    ctrl("Q10", "Validité", f"silver/{t}", "Prix de vente <= prix catalogue", "comptage", f"{n}", n == 0)
    r = S[t].agg(F.min("date_vente"), F.max("date_vente")).first()
    ok = str(r[0]) >= "2001-01-01" and str(r[1]) <= "2005-12-31"
    ctrl("Q11", "Validité", f"silver/{t}", "Dates de vente dans la période du kit (2001-2005)", "min / max",
         f"{r[0]} -> {r[1]}", ok)
n = nb(S["date_dim"], "d_date IS NULL OR d_moy NOT BETWEEN 1 AND 12 OR d_dom NOT BETWEEN 1 AND 31")
ctrl("Q12", "Validité", "silver/date_dim", "Dates et mois valides", "comptage", f"{n}", n == 0)

# =========================================================================== cohérence
for t, p, doc, _ in VENTES:
    n = nb(S[t], f"abs({p}_ext_sales_price - {p}_sales_price * {p}_quantity) > 0.01")
    ctrl("Q13", "Cohérence", f"silver/{t}", "ext_sales_price = sales_price x quantity", "écart > 0,01",
         f"{n}", n == 0, "quarantaine (R7)")
    n = nb(S[t], f"abs({p}_net_paid - ({p}_ext_sales_price - {p}_coupon_amt)) > 0.01")
    ctrl("Q14", "Cohérence", f"silver/{t}", "net_paid = ext_sales_price - coupon_amt", "écart > 0,01", f"{n}", n == 0)
    n = nb(S[t], f"abs({p}_net_profit - ({p}_net_paid - {p}_ext_wholesale_cost)) > 0.01")
    ctrl("Q15", "Cohérence", f"silver/{t}", "net_profit = net_paid - ext_wholesale_cost", "écart > 0,01",
         f"{n}", n == 0)
    m = nb(S[t], "marge_negative")
    ctrl("Q16", "Cohérence", f"silver/{t}", "Lignes à marge négative", "comptage",
         f"{m} ({100 * m / S[t].count():.1f} %)", None,
         "conservées : effet des paramètres du générateur (remise jusqu'à 100 %), marge non interprétable", "–")
    n = (S[t].groupBy(f"{p}_{doc}").agg(F.countDistinct("date_vente").alias("d"),
                                        F.countDistinct(f"{p}_{'customer' if p == 'ss' else 'bill_customer'}_sk")
                                        .alias("c")).filter("d > 1 OR c > 1").count())
    ctrl("Q17", "Cohérence", f"silver/{t}", f"Un {doc} = une seule date et un seul client", "groupBy document",
         f"{n} document(s) incohérent(s)", n == 0)
r = S["web_sales"].agg(F.min("delai_expedition_j"), F.max("delai_expedition_j"),
                       F.sum(F.when(F.col("delai_expedition_j") < 0, 1).otherwise(0))).first()
ctrl("Q18", "Cohérence", "silver/web_sales", "Expédition après la vente (spécification : 1 à 120 jours)",
     "min / max de ship_date - sold_date", f"{r[0]} à {r[1]} jours, {r[2]} négatif(s)",
     (r[2] or 0) == 0 and r[0] is not None and r[0] >= 0)
for t, p, doc in RETOURS:
    n = nb(S[t], "retour_avant_vente")
    ctrl("Q19", "Cohérence", f"silver/{t}", "Retour daté après la vente d'origine", "comptage", f"{n}", n == 0)
for (t, p, doc), (tv, pv, docv, _) in zip(RETOURS, VENTES):
    r_ = S[t].filter("vente_trouvee").groupBy(F.col(f"{p}_{doc}").alias("d"), F.col(f"{p}_item_sk").alias("i")) \
        .agg(F.sum(f"{p}_return_quantity").alias("qr"))
    v_ = S[tv].groupBy(F.col(f"{pv}_{docv}").alias("d"), F.col(f"{pv}_item_sk").alias("i")) \
        .agg(F.sum(f"{pv}_quantity").alias("qv"))
    exc = r_.join(v_, ["d", "i"]).filter("qr > qv")
    n = exc.count()
    detail = "; ".join(f"doc {r['d']} article {r['i']} : {r['qr']} rendus pour {r['qv']} vendus"
                       for r in exc.orderBy("d").limit(5).collect())
    flag = nb(S[t], "retour_excedentaire")
    ctrl("Q20", "Cohérence", f"silver/{t}", "Quantité retournée cumulée <= quantité vendue (document, article)",
         "jointure retours / ventes agrégés", f"{n} couple(s) {('(' + detail + ')') if n else ''}", n == 0,
         f"conservés et signalés (retour_excedentaire = true sur {flag} lignes, R11b)" if n else "")

# =========================================================================== volumes
with open(C.PREUVES / "silver" / "bilan_silver.csv", encoding="utf-8-sig") as f:
    for b in csv.DictReader(f, delimiter=";"):
        ok = b["bronze_egal_silver_plus_quarantaine"] == "True" and b["lignes_relues"] == b["lignes_silver"]
        ctrl("Q21", "Volume", f"silver/{b['table']}", "Bronze = Silver + quarantaine ; relecture = écriture",
             "comptages (bilan_silver.csv)",
             f"{b['lignes_bronze']} = {b['lignes_silver']} + {b['lignes_quarantaine']} ; relu {b['lignes_relues']}", ok)
for t in ["store_sales", "web_sales", "item", "store_returns", "web_returns"]:
    par_lot = {r["lot_source"]: r["count"] for r in S[t].groupBy("lot_source").count().collect()}
    ctrl("Q22", "Volume", f"silver/{t}", "Les deux lots sont intégrés", "comptage par lot_source",
         f"initial={par_lot.get('initial', 0)}, refresh={par_lot.get('refresh', 0)}", len(par_lot) == 2)

# =========================================================================== intégrité référentielle
ref = {"item": S["item"].select(F.col("i_item_sk").alias("k")), "store": S["store"].select(F.col("s_store_sk").alias("k")),
       "date_dim": S["date_dim"].select(F.col("d_date_sk").alias("k")),
       "reason": S["reason"].select(F.col("r_reason_sk").alias("k"))}
for t, col, dim in [("store_sales", "ss_item_sk", "item"), ("store_sales", "ss_store_sk", "store"),
                    ("store_sales", "ss_sold_date_sk", "date_dim"), ("web_sales", "ws_item_sk", "item"),
                    ("web_sales", "ws_sold_date_sk", "date_dim"), ("store_returns", "sr_item_sk", "item"),
                    ("store_returns", "sr_reason_sk", "reason"), ("web_returns", "wr_item_sk", "item"),
                    ("web_returns", "wr_reason_sk", "reason")]:
    n = S[t].filter(F.col(col).isNotNull()).join(ref[dim], F.col(col) == F.col("k"), "left_anti").count()
    ctrl("Q23", "Intégrité", f"silver/{t}", f"{col} -> {dim} : aucune clé orpheline", "left_anti join", f"{n}", n == 0)
for t, p, _ in RETOURS:
    par = {(r["lot_source"], r["vente_trouvee"]): r["count"]
           for r in S[t].groupBy("lot_source", "vente_trouvee").count().collect()}
    sans = sum(v for (l, vt), v in par.items() if not vt)
    detail = ", ".join(f"{l}: {par.get((l, False), 0)}/{par.get((l, True), 0) + par.get((l, False), 0)}"
                       for l in C.LOTS)
    ctrl("Q24", "Intégrité", f"silver/{t}", "Chaque retour est rattaché à une vente (canal, document, article)",
         "jointure retours -> ventes (R11)", f"{sans} retour(s) sans vente ({detail})", sans == 0,
         "conservés et signalés (vente_trouvee = false), exclus du taux de retour")

# =========================================================================== réconciliation
fv = G["fait_ventes"]
for canal, t, p in [("magasin", "store_sales", "ss"), ("web", "web_sales", "ws")]:
    s_ = S[t].agg(F.sum(f"{p}_ext_sales_price")).first()[0]
    g_ = fv.filter(F.col("canal") == canal).agg(F.sum("montant_ht")).first()[0]
    g1 = G["ventes_mensuelles_canal"].filter(F.col("canal") == canal).agg(F.sum("ca_ht")).first()[0]
    g2 = G["ventes_categorie_annee"].filter(F.col("canal") == canal).agg(F.sum("ca_ht")).first()[0]
    ctrl("Q25", "Réconciliation", f"gold ({canal})", "CA identique : Silver = fait_ventes = agrégats mensuels "
         "= agrégats catégorie", "sommes de montant", f"{s_} / {g_} / {g1} / {g2}", s_ == g_ == g1 == g2)
g3 = G["ventes_magasin_annee"].agg(F.sum("ca_ht")).first()[0]
s_ = S["store_sales"].agg(F.sum("ss_ext_sales_price")).first()[0]
ctrl("Q25", "Réconciliation", "gold/ventes_magasin_annee", "CA par magasin = CA magasin Silver", "sommes",
     f"{g3} / {s_}", g3 == s_)

# Comparaison avec les KPI calculés au Jour 1 avec un AUTRE moteur (DuckDB) sur les mêmes fichiers
kpi_j1 = {}
with open(C.J3.parent / "Jour1" / "01_UC01_besoin_exploration" / "UC01_preuves" / "kpi_par_annee.csv",
          encoding="utf-8-sig") as f:
    for r in csv.DictReader(f, delimiter=";"):
        kpi_j1[r["id"]] = {int(a): float(r[str(a)]) for a in range(2001, 2006) if r[str(a)]}
g1 = {(r["annee"], r["canal"]): r for r in G["ventes_mensuelles_canal"].groupBy("annee", "canal")
      .agg(F.sum("ca_ht").alias("ca"), F.sum("nb_tickets_commandes").alias("t"), F.sum("quantite").alias("q"),
           F.sum("lignes").alias("l")).collect()}
g4 = {(r["annee"], r["canal"]): r for r in G["retours_categorie"].groupBy("annee", "canal")
      .agg(F.sum("quantite_retournee").alias("qr"), F.sum("quantite_vendue").alias("qv")).collect()}
comparaison = []
for kid, lib, f_, tol in [
        ("K01", "CA magasin", lambda a: float(g1[(a, "magasin")]["ca"]), 0.5),
        ("K01b", "CA web", lambda a: float(g1[(a, "web")]["ca"]), 0.5),
        ("K02", "Tickets magasin", lambda a: g1[(a, "magasin")]["t"], 0),
        ("K02b", "Commandes web", lambda a: g1[(a, "web")]["t"], 0),
        ("K03", "Panier moyen magasin", lambda a: round(float(g1[(a, "magasin")]["ca"]) / g1[(a, "magasin")]["t"], 2), 0.005),
        ("K04", "Quantités vendues magasin", lambda a: g1[(a, "magasin")]["q"], 0),
        ("K07", "Part du web dans le CA (%)", lambda a: round(100 * float(g1[(a, "web")]["ca"]) /
                                                              float(g1[(a, "web")]["ca"] + g1[(a, "magasin")]["ca"]), 2), 0.005),
        ("K09", "Taux de retour quantité magasin (%)", lambda a: round(100 * g4[(a, "magasin")]["qr"] /
                                                                       g4[(a, "magasin")]["qv"], 2), 0.005),
        ("K12", "Lignes de vente magasin", lambda a: g1[(a, "magasin")]["l"], 0)]:
    for a, v1 in kpi_j1[kid].items():
        v3 = f_(a)
        comparaison.append({"kpi": kid, "libelle": lib, "annee": a, "jour1_duckdb": v1, "jour3_spark_gold": v3,
                            "ecart": round(v3 - v1, 4), "identique": abs(v3 - v1) <= tol})
ecarts = [c for c in comparaison if not c["identique"]]
ctrl("Q26", "Réconciliation", "gold vs Jour 1", "KPI Gold (Spark) = KPI du Jour 1 (DuckDB), même fichiers",
     f"{len(comparaison)} valeurs comparées (K01-K12, 2001-2005), tolérance = arrondi du J1",
     f"{len(comparaison) - len(ecarts)}/{len(comparaison)} identiques"
     + (f" ; écarts : {[(e['kpi'], e['annee'], e['ecart']) for e in ecarts]}" if ecarts else ""), not ecarts)

# =========================================================================== fraîcheur
for t, col in [("store_sales", "date_vente"), ("web_sales", "date_vente"), ("store_returns", "date_retour"),
               ("web_returns", "date_retour")]:
    r = S[t].agg(F.min(col), F.max(col), F.max("ingest_date"), F.max("silver_traite_le")).first()
    ctrl("Q27", "Fraîcheur", f"silver/{t}", "Période couverte et date du dernier chargement", "min / max",
         f"{r[0]} -> {r[1]} ; ingestion {r[2]} ; traitement {r[3]}", None, "", "–")

# =========================================================================== sorties
U.ecrire_csv_local(controles, OUT / "controles_qualite.csv")
U.ecrire_csv_local(comparaison, OUT / "reconciliation_jour1.csv")
(spark.createDataFrame(controles).coalesce(1).write.mode("overwrite").option("header", True).option("sep", ";")
 .csv(f"{C.QUALITY}/controles/run={C.INGEST_DATE}"))
resume = {s: sum(1 for c in controles if c["statut"] == s) for s in ("OK", "ANOMALIE", "INFO")}
resume["total"] = len(controles)
resume["anomalies"] = [f"{c['id']} {c['jeu']} : {c['resultat']}" for c in controles if c["statut"] == "ANOMALIE"]
U.ecrire_json_local(resume, OUT / "resume_qualite.json")
log.info("Qualité : %s", resume)
spark.stop()
