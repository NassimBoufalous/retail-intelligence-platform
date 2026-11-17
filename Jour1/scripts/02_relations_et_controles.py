"""
02 - Vérification des relations et contrôles qualité (UC-01 étapes D et E).

POURQUOI ?
    Le TP interdit de supposer une jointure "parce que les noms se ressemblent".
    Pour chaque relation candidate (fait -> dimension) on MESURE :
        type des deux colonnes, unicité côté dimension, % de nulls côté fait, nb de lignes orphelines.
    Puis on contrôle : grain des faits, cohérence des montants (valide aussi les noms de colonnes déduits),
    cohérence des tickets, lien retours -> ventes, fraîcheur, lot data_refresh, faisabilité des KPI.

Le lot initial (data/) est chargé sous le nom de la table, le lot de rafraîchissement sous <table>_refresh.

SORTIES (UC01_preuves/)
    relations_verifiees.csv, controles_qualite.csv, kpi_faisabilite.csv,
    ventes_par_annee.csv, ventes_par_mois.csv, retours_par_categorie.csv, notes_avis.csv

USAGE
    ./.venv/Scripts/python scripts/02_relations_et_controles.py retail_project
"""
import csv
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schema_tpcxbb import SCHEMA, SPEC_COLONNES_ABSENTES, SPEC_NOT_NULL, read_sql  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
KIT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / "retail_project"
PREUVES = ROOT / "Jour1" / "01_UC01_besoin_exploration" / "UC01_preuves"
PREUVES.mkdir(parents=True, exist_ok=True)
for old in ["ventes_par_mois_2002.csv"]:  # fichier d'une version précédente du script
    (PREUVES / old).unlink(missing_ok=True)

con = duckdb.connect()
for lot, suffix in [("data", ""), ("data_refresh", "_refresh")]:
    for t in SCHEMA:
        p = KIT / lot / t / f"{t}.dat"
        if p.exists():
            con.execute(f"CREATE TABLE {t}{suffix} AS SELECT * FROM {read_sql(p.as_posix(), t)}")
# Tables dérivées : identifiants de transaction distincts (pour tester les liens clics/avis -> vente)
con.execute("CREATE TABLE web_orders AS SELECT DISTINCT ws_order_number FROM web_sales")
con.execute("CREATE TABLE store_tickets AS SELECT DISTINCT ss_ticket_number FROM store_sales")
# Référentiels complets (lot initial + rafraîchissement)
con.execute("CREATE TABLE item_all AS SELECT * FROM item UNION ALL SELECT * FROM item_refresh")
con.execute("CREATE TABLE customer_all AS SELECT * FROM customer UNION ALL SELECT * FROM customer_refresh")
# Faits des deux lots réunis (data + data_refresh) : utilisés pour les valeurs des KPI
FAITS = ["store_sales", "web_sales", "store_returns", "web_returns", "inventory", "web_clickstreams", "product_reviews"]
for t in FAITS:
    con.execute(f"CREATE TABLE {t}_all AS SELECT * FROM {t} UNION ALL SELECT * FROM {t}_refresh")


def one(sql):
    return con.execute(sql).fetchone()


def coltype(table, col):
    return one(f"SELECT data_type FROM information_schema.columns WHERE table_name='{table}' AND column_name='{col}'")[0]


# --------------------------------------------------------------------------
# 1. Relations candidates
# --------------------------------------------------------------------------
RELATIONS = [
    ("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk"),
    ("store_sales", "ss_sold_time_sk", "time_dim", "t_time_sk"),
    ("store_sales", "ss_item_sk", "item", "i_item_sk"),
    ("store_sales", "ss_customer_sk", "customer", "c_customer_sk"),
    ("store_sales", "ss_store_sk", "store", "s_store_sk"),
    ("store_sales", "ss_promo_sk", "promotion", "p_promo_sk"),
    ("store_sales", "ss_cdemo_sk", "customer_demographics", "cd_demo_sk"),
    ("store_sales", "ss_hdemo_sk", "household_demographics", "hd_demo_sk"),
    ("store_sales", "ss_addr_sk", "customer_address", "ca_address_sk"),
    ("web_sales", "ws_sold_date_sk", "date_dim", "d_date_sk"),
    ("web_sales", "ws_ship_date_sk", "date_dim", "d_date_sk"),
    ("web_sales", "ws_item_sk", "item", "i_item_sk"),
    ("web_sales", "ws_bill_customer_sk", "customer", "c_customer_sk"),
    ("web_sales", "ws_web_site_sk", "web_site", "web_site_sk"),
    ("web_sales", "ws_web_page_sk", "web_page", "wp_web_page_sk"),
    ("web_sales", "ws_ship_mode_sk", "ship_mode", "sm_ship_mode_sk"),
    ("web_sales", "ws_warehouse_sk", "warehouse", "w_warehouse_sk"),
    ("web_sales", "ws_promo_sk", "promotion", "p_promo_sk"),
    ("store_returns", "sr_returned_date_sk", "date_dim", "d_date_sk"),
    ("store_returns", "sr_item_sk", "item", "i_item_sk"),
    ("store_returns", "sr_store_sk", "store", "s_store_sk"),
    ("store_returns", "sr_reason_sk", "reason", "r_reason_sk"),
    ("store_returns", "sr_customer_sk", "customer", "c_customer_sk"),
    ("web_returns", "wr_returned_date_sk", "date_dim", "d_date_sk"),
    ("web_returns", "wr_item_sk", "item", "i_item_sk"),
    ("web_returns", "wr_reason_sk", "reason", "r_reason_sk"),
    ("inventory", "inv_date_sk", "date_dim", "d_date_sk"),
    ("inventory", "inv_item_sk", "item", "i_item_sk"),
    ("inventory", "inv_item_sk", "item_all", "i_item_sk"),
    ("inventory", "inv_warehouse_sk", "warehouse", "w_warehouse_sk"),
    ("customer", "c_current_addr_sk", "customer_address", "ca_address_sk"),
    ("customer", "c_current_cdemo_sk", "customer_demographics", "cd_demo_sk"),
    ("customer", "c_current_hdemo_sk", "household_demographics", "hd_demo_sk"),
    ("household_demographics", "hd_income_band_sk", "income_band", "ib_income_band_sk"),
    ("promotion", "p_item_sk", "item", "i_item_sk"),
    # Tables propres à TPCx-BB
    ("web_clickstreams", "wcs_click_date_sk", "date_dim", "d_date_sk"),
    ("web_clickstreams", "wcs_click_time_sk", "time_dim", "t_time_sk"),
    ("web_clickstreams", "wcs_item_sk", "item", "i_item_sk"),
    ("web_clickstreams", "wcs_web_page_sk", "web_page", "wp_web_page_sk"),
    ("web_clickstreams", "wcs_user_sk", "customer", "c_customer_sk"),
    ("web_clickstreams", "wcs_sales_sk", "web_orders", "ws_order_number"),
    ("product_reviews", "pr_item_sk", "item", "i_item_sk"),
    ("product_reviews", "pr_user_sk", "customer", "c_customer_sk"),
    ("product_reviews", "pr_order_sk", "web_orders", "ws_order_number"),
    ("product_reviews", "pr_order_sk", "store_tickets", "ss_ticket_number"),
    ("item_marketprices", "imp_item_sk", "item", "i_item_sk"),
    # Lot de rafraîchissement : les nouvelles ventes pointent-elles vers des référentiels connus ?
    ("store_sales_refresh", "ss_item_sk", "item_all", "i_item_sk"),
    ("store_sales_refresh", "ss_customer_sk", "customer_all", "c_customer_sk"),
    ("store_sales_refresh", "ss_sold_date_sk", "date_dim", "d_date_sk"),
    ("web_sales_refresh", "ws_item_sk", "item_all", "i_item_sk"),
    ("web_sales_refresh", "ws_bill_customer_sk", "customer_all", "c_customer_sk"),
    ("inventory_refresh", "inv_item_sk", "item_all", "i_item_sk"),
]

rel_rows = []
for ft, fc, dt, dc in RELATIONS:
    n, nulls, dist = one(f"SELECT count(*), count(*) - count({fc}), count(DISTINCT {fc}) FROM {ft}")
    d_n, d_dist = one(f"SELECT count(*), count(DISTINCT {dc}) FROM {dt}")
    orphans, orph_vals = one(
        f"SELECT count(*), count(DISTINCT f.{fc}) FROM {ft} f WHERE f.{fc} IS NOT NULL "
        f"AND NOT EXISTS (SELECT 1 FROM {dt} d WHERE d.{dc} = f.{fc})")
    unique_dim = d_n == d_dist
    t1, t2 = coltype(ft, fc), coltype(dt, dc)
    card = "N:1" if unique_dim else "N:N (dimension non unique !)"
    if orphans == 0 and unique_dim and t1 == t2:
        statut = "Vérifié"
    elif orphans > 0 and orphans < n - nulls:
        statut = "À confirmer (orphelins)"
    elif orphans > 0:
        statut = "Rejeté (aucune correspondance)"
    else:
        statut = "À confirmer"
    rel_rows.append({
        "table_fait": ft, "colonne_fait": fc, "type_fait": t1, "dimension": dt, "colonne_dimension": dc,
        "type_dimension": t2, "types_compatibles": "oui" if t1 == t2 else "non",
        "dimension_unique": "oui" if unique_dim else "non", "cardinalite": card,
        "pct_nulls_fait": round(100 * nulls / n, 2), "valeurs_distinctes_fait": dist,
        "lignes_orphelines": orphans, "valeurs_orphelines": orph_vals,
        "pct_orphelins": round(100 * orphans / max(n - nulls, 1), 2), "statut": statut,
    })
    print(f"{ft}.{fc:<20} -> {dt}.{dc:<18} nulls={100*nulls/n:6.2f}%  orphelins={orphans:<9} {statut}")

# --------------------------------------------------------------------------
# 2. Contrôles qualité, grain, cohérence, rafraîchissement
# --------------------------------------------------------------------------
DATE = lambda sk: f"(SELECT d_date FROM date_dim WHERE d_date_sk = {sk})"
CONTROLES = [
    ("G1", "Unicité / grain", "store_sales : doublons sur (ss_ticket_number, ss_item_sk)",
     "SELECT count(*) - count(DISTINCT (ss_ticket_number, ss_item_sk)) FROM store_sales", "0"),
    ("G2", "Unicité / grain", "web_sales : doublons sur (ws_order_number, ws_item_sk)",
     "SELECT count(*) - count(DISTINCT (ws_order_number, ws_item_sk)) FROM web_sales", "0"),
    ("G3", "Unicité / grain", "store_returns : doublons sur (sr_ticket_number, sr_item_sk)",
     "SELECT count(*) - count(DISTINCT (sr_ticket_number, sr_item_sk)) FROM store_returns", "0"),
    ("G4", "Unicité / grain", "web_returns : doublons sur (wr_order_number, wr_item_sk)",
     "SELECT count(*) - count(DISTINCT (wr_order_number, wr_item_sk)) FROM web_returns", "0"),
    ("G5", "Unicité / grain", "inventory : doublons sur (inv_date_sk, inv_item_sk, inv_warehouse_sk)",
     "SELECT count(*) - count(DISTINCT (inv_date_sk, inv_item_sk, inv_warehouse_sk)) FROM inventory", "0"),
    ("G6", "Unicité", "store_sales : lignes entièrement dupliquées",
     "SELECT count(*) - (SELECT count(*) FROM (SELECT DISTINCT * FROM store_sales)) FROM store_sales", "0"),
    ("G7", "Unicité", "web_clickstreams : lignes entièrement dupliquées",
     "SELECT count(*) - (SELECT count(*) FROM (SELECT DISTINCT * FROM web_clickstreams)) FROM web_clickstreams", "info"),
    ("T1", "Cohérence", "store_sales : nb de tickets distincts",
     "SELECT count(DISTINCT ss_ticket_number) FROM store_sales", "info"),
    ("T2", "Cohérence", "store_sales : nb moyen de lignes par ticket",
     "SELECT round(count(*) / count(DISTINCT ss_ticket_number), 2) FROM store_sales", "info"),
    ("T3", "Cohérence", "store_sales : tickets ayant >1 client distinct",
     "SELECT count(*) FROM (SELECT ss_ticket_number FROM store_sales GROUP BY 1 HAVING count(DISTINCT ss_customer_sk) > 1)", "0"),
    ("T4", "Cohérence", "store_sales : tickets ayant >1 magasin distinct",
     "SELECT count(*) FROM (SELECT ss_ticket_number FROM store_sales GROUP BY 1 HAVING count(DISTINCT ss_store_sk) > 1)", "0"),
    ("T5", "Cohérence", "store_sales : tickets ayant >1 date distincte",
     "SELECT count(*) FROM (SELECT ss_ticket_number FROM store_sales GROUP BY 1 HAVING count(DISTINCT ss_sold_date_sk) > 1)", "0"),
    ("T6", "Cohérence", "web_sales : nb de commandes distinctes / lignes par commande",
     "SELECT count(DISTINCT ws_order_number) || ' / ' || round(count(*) / count(DISTINCT ws_order_number), 2) FROM web_sales", "info"),
    ("T7", "Cohérence", "web_sales : commandes ayant >1 client facturé",
     "SELECT count(*) FROM (SELECT ws_order_number FROM web_sales GROUP BY 1 HAVING count(DISTINCT ws_bill_customer_sk) > 1)", "0"),
    ("T8", "Cohérence", "Numéros de ticket magasin également utilisés comme n° de commande web",
     "SELECT count(*) FROM store_tickets s JOIN web_orders w ON s.ss_ticket_number = w.ws_order_number", "info"),
    ("M1", "Exactitude", "store_sales : |ext_sales_price - sales_price*quantity| > 0.01",
     "SELECT count(*) FROM store_sales WHERE abs(ss_ext_sales_price - ss_sales_price * ss_quantity) > 0.01", "0"),
    ("M1b", "Exactitude", "web_sales : |ext_sales_price - sales_price*quantity| > 0.01",
     "SELECT count(*) FROM web_sales WHERE abs(ws_ext_sales_price - ws_sales_price * ws_quantity) > 0.01", "0"),
    ("M2", "Exactitude", "store_sales : |ext_discount_amt - (ext_list_price - ext_sales_price)| > 0.01",
     "SELECT count(*) FROM store_sales WHERE abs(ss_ext_discount_amt - (ss_ext_list_price - ss_ext_sales_price)) > 0.01", "0"),
    ("M3", "Exactitude", "store_sales : |net_paid - (ext_sales_price - coupon_amt)| > 0.01",
     "SELECT count(*) FROM store_sales WHERE abs(ss_net_paid - (ss_ext_sales_price - ss_coupon_amt)) > 0.01", "0"),
    ("M3b", "Exactitude", "store_sales : |net_profit - (net_paid - ext_wholesale_cost)| > 0.01",
     "SELECT count(*) FROM store_sales WHERE abs(ss_net_profit - (ss_net_paid - ss_ext_wholesale_cost)) > 0.01", "0"),
    ("M4", "Validité", "store_sales : quantité <= 0",
     "SELECT count(*) FROM store_sales WHERE ss_quantity <= 0", "0"),
    ("M5", "Validité", "store_sales : lignes à marge nette négative",
     "SELECT count(*) FROM store_sales WHERE ss_net_profit < 0", "info"),
    ("M6", "Validité", "store_sales : prix de vente > prix catalogue",
     "SELECT count(*) FROM store_sales WHERE ss_sales_price > ss_list_price", "0"),
    ("M7", "Validité", "store_sales : lignes avec coupon > 0",
     "SELECT count(*) FROM store_sales WHERE ss_coupon_amt > 0", "info"),
    ("M8", "Validité", "product_reviews : notes hors 1..5",
     "SELECT count(*) FROM product_reviews WHERE pr_review_rating NOT BETWEEN 1 AND 5", "0"),
    ("M9", "Validité", "customer.c_last_review_date : valeurs non convertibles en date",
     "SELECT count(*) FROM customer WHERE c_last_review_date IS NOT NULL AND try_cast(c_last_review_date AS DATE) IS NULL", "0"),
    ("M10", "Validité", "product_reviews : textes contenant des entités HTML (&quot; &amp;…)",
     "SELECT count(*) FROM product_reviews WHERE regexp_matches(pr_review_content, '&[a-z]+;')", "info"),
    ("C1", "Complétude", "store_sales : lignes sans date / article / montant / magasin",
     "SELECT count(*) FROM store_sales WHERE ss_sold_date_sk IS NULL OR ss_item_sk IS NULL OR ss_ext_sales_price IS NULL OR ss_store_sk IS NULL", "0"),
    ("C2", "Complétude", "web_sales : lignes sans date / article / montant",
     "SELECT count(*) FROM web_sales WHERE ws_sold_date_sk IS NULL OR ws_item_sk IS NULL OR ws_ext_sales_price IS NULL", "0"),
    ("C3", "Complétude", "store_sales : lignes sans promotion",
     "SELECT count(*) FROM store_sales WHERE ss_promo_sk IS NULL", "info"),
    ("C4", "Complétude", "web_clickstreams : % clics sans utilisateur (visiteur anonyme)",
     "SELECT round(100 * (count(*) - count(wcs_user_sk)) / count(*), 2) FROM web_clickstreams", "info"),
    ("C5", "Complétude", "web_clickstreams : % clics liés à une vente (wcs_sales_sk renseigné)",
     "SELECT round(100 * count(wcs_sales_sk) / count(*), 2) FROM web_clickstreams", "info"),
    ("C6", "Complétude", "item / store : % de i_rec_end_date / s_rec_end_date renseignés",
     "SELECT round(100 * count(i_rec_end_date) / count(*), 2) || ' / ' || (SELECT round(100 * count(s_rec_end_date) / count(*), 2) FROM store) FROM item", "info"),
    ("C7", "Complétude", "item_marketprices : produits sans prix concurrent",
     "SELECT count(*) FROM item i WHERE NOT EXISTS (SELECT 1 FROM item_marketprices m WHERE m.imp_item_sk = i.i_item_sk)", "info"),
    ("F1", "Fraîcheur", "store_sales : période (min -> max)",
     "SELECT min(d_date)::VARCHAR || ' -> ' || max(d_date)::VARCHAR FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk", "info"),
    ("F2", "Fraîcheur", "web_sales : période (min -> max)",
     "SELECT min(d_date)::VARCHAR || ' -> ' || max(d_date)::VARCHAR FROM web_sales JOIN date_dim ON ws_sold_date_sk = d_date_sk", "info"),
    ("F3", "Fraîcheur", "store_returns : période (min -> max)",
     "SELECT min(d_date)::VARCHAR || ' -> ' || max(d_date)::VARCHAR FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk", "info"),
    ("F4", "Fraîcheur", "web_clickstreams : période (min -> max)",
     "SELECT min(d_date)::VARCHAR || ' -> ' || max(d_date)::VARCHAR FROM web_clickstreams JOIN date_dim ON wcs_click_date_sk = d_date_sk", "info"),
    ("F5", "Fraîcheur", "product_reviews : période (min -> max)",
     "SELECT min(pr_review_date)::VARCHAR || ' -> ' || max(pr_review_date)::VARCHAR FROM product_reviews", "info"),
    ("F6", "Fraîcheur", "inventory : nb de dates de relevé / écart moyen en jours",
     "SELECT count(*) || ' relevés, écart moyen ' || round(avg(ecart),1) || ' j' FROM (SELECT d_date - lag(d_date) OVER (ORDER BY d_date) AS ecart FROM (SELECT DISTINCT d_date FROM inventory JOIN date_dim ON inv_date_sk = d_date_sk))", "info"),
    ("R1", "Intégrité", "store_returns : retours sans vente correspondante (ticket+article)",
     "SELECT count(*) FROM store_returns r WHERE NOT EXISTS (SELECT 1 FROM store_sales s WHERE s.ss_ticket_number = r.sr_ticket_number AND s.ss_item_sk = r.sr_item_sk)", "0"),
    ("R2", "Intégrité", "web_returns : retours sans vente correspondante (commande+article)",
     "SELECT count(*) FROM web_returns r WHERE NOT EXISTS (SELECT 1 FROM web_sales s WHERE s.ws_order_number = r.wr_order_number AND s.ws_item_sk = r.wr_item_sk)", "0"),
    ("R3", "Intégrité", "store_returns : quantité retournée (cumulée) > quantité vendue (cumulée) pour un même ticket+article",
     "SELECT count(*) FROM (SELECT sr_ticket_number t, sr_item_sk i, sum(sr_return_quantity) q FROM store_returns GROUP BY 1, 2) r JOIN (SELECT ss_ticket_number t, ss_item_sk i, sum(ss_quantity) q FROM store_sales GROUP BY 1, 2) s USING (t, i) WHERE r.q > s.q", "0"),
    ("R3b", "Intégrité", "store_returns : retour lié à un ticket+article présent sur plusieurs lignes de vente (rattachement ambigu)",
     "SELECT count(*) FROM store_returns r JOIN (SELECT ss_ticket_number t, ss_item_sk i FROM store_sales GROUP BY 1, 2 HAVING count(*) > 1) d ON d.t = r.sr_ticket_number AND d.i = r.sr_item_sk", "info"),
    ("G1b", "Unicité / grain", "store_sales : couples (ticket, article) présents sur plusieurs lignes",
     "SELECT count(*) FROM (SELECT 1 FROM store_sales GROUP BY ss_ticket_number, ss_item_sk HAVING count(*) > 1)", "info"),
    ("R4", "Intégrité", "store_returns : retour daté AVANT la vente",
     "SELECT count(*) FROM store_returns r JOIN store_sales s ON s.ss_ticket_number = r.sr_ticket_number AND s.ss_item_sk = r.sr_item_sk WHERE r.sr_returned_date_sk < s.ss_sold_date_sk", "0"),
    ("R5", "Intégrité", "store_returns : retour lié dont le client diffère de celui de la vente",
     "SELECT count(*) FROM store_returns r JOIN store_sales s ON s.ss_ticket_number = r.sr_ticket_number AND s.ss_item_sk = r.sr_item_sk WHERE r.sr_customer_sk <> s.ss_customer_sk", "info"),
    ("R6", "Intégrité", "web_clickstreams : clics liés à une vente dont l'article n'est pas dans la commande",
     "SELECT count(*) FROM web_clickstreams c WHERE wcs_sales_sk IS NOT NULL AND NOT EXISTS (SELECT 1 FROM web_sales s WHERE s.ws_order_number = c.wcs_sales_sk AND s.ws_item_sk = c.wcs_item_sk)", "0"),
    ("R7", "Intégrité", "product_reviews : avis dont l'article n'est pas dans la commande web pr_order_sk",
     "SELECT count(*) FROM product_reviews p WHERE NOT EXISTS (SELECT 1 FROM web_sales s WHERE s.ws_order_number = p.pr_order_sk AND s.ws_item_sk = p.pr_item_sk)", "info"),
    ("R7b", "Intégrité", "product_reviews : avis dont l'article est AUSSI dans le ticket magasin de même numéro",
     "SELECT count(*) FROM product_reviews p WHERE EXISTS (SELECT 1 FROM store_sales s WHERE s.ss_ticket_number = p.pr_order_sk AND s.ss_item_sk = p.pr_item_sk)", "info"),
    ("R7c", "Intégrité", "product_reviews : avis dont l'auteur (pr_user_sk) = client facturé de la commande web",
     "SELECT count(*) FROM product_reviews p WHERE EXISTS (SELECT 1 FROM web_sales s WHERE s.ws_order_number = p.pr_order_sk AND s.ws_bill_customer_sk = p.pr_user_sk)", "info"),
    ("R8", "Intégrité", "inventory : produits référencés absents de item (lot data)",
     "SELECT count(DISTINCT inv_item_sk) FROM inventory WHERE inv_item_sk NOT IN (SELECT i_item_sk FROM item)", "0"),
    ("RF1", "Rafraîchissement", "customer_refresh : clients déjà présents dans le lot initial (mises à jour) / nouveaux",
     "SELECT count(*) FILTER (WHERE c_customer_sk IN (SELECT c_customer_sk FROM customer)) || ' / ' || count(*) FILTER (WHERE c_customer_sk NOT IN (SELECT c_customer_sk FROM customer)) FROM customer_refresh", "info"),
    ("RF2", "Rafraîchissement", "item_refresh : produits déjà présents / nouveaux (plage de clés)",
     "SELECT count(*) FILTER (WHERE i_item_sk IN (SELECT i_item_sk FROM item)) || ' / ' || count(*) FILTER (WHERE i_item_sk NOT IN (SELECT i_item_sk FROM item)) || ' (sk ' || min(i_item_sk) || '-' || max(i_item_sk) || ')' FROM item_refresh", "info"),
    ("RF3", "Rafraîchissement", "store_sales_refresh : période des ventes",
     "SELECT min(d_date)::VARCHAR || ' -> ' || max(d_date)::VARCHAR FROM store_sales_refresh JOIN date_dim ON ss_sold_date_sk = d_date_sk", "info"),
    ("RF4", "Rafraîchissement", "store_sales_refresh : lignes déjà présentes dans le lot initial (même ticket+article)",
     "SELECT count(*) FROM store_sales_refresh r JOIN store_sales s ON s.ss_ticket_number = r.ss_ticket_number AND s.ss_item_sk = r.ss_item_sk", "0"),
    ("RF5", "Rafraîchissement", "store_sales_refresh : plage des n° de ticket (vs max du lot initial)",
     "SELECT min(ss_ticket_number) || '-' || max(ss_ticket_number) || ' (max initial ' || (SELECT max(ss_ticket_number) FROM store_sales) || ')' FROM store_sales_refresh", "info"),
    ("RF7", "Rafraîchissement", "store_returns_refresh : retours sans vente correspondante (data + refresh)",
     "SELECT count(*) || ' / ' || (SELECT count(*) FROM store_returns_refresh) FROM store_returns_refresh r WHERE NOT EXISTS (SELECT 1 FROM store_sales_all s WHERE s.ss_ticket_number = r.sr_ticket_number AND s.ss_item_sk = r.sr_item_sk)", "0 / 426"),
    ("RF8", "Rafraîchissement", "web_returns_refresh : retours sans vente correspondante (data + refresh)",
     "SELECT count(*) || ' / ' || (SELECT count(*) FROM web_returns_refresh) FROM web_returns_refresh r WHERE NOT EXISTS (SELECT 1 FROM web_sales_all s WHERE s.ws_order_number = r.wr_order_number AND s.ws_item_sk = r.wr_item_sk)", "0 / 400"),
    ("RF9", "Rafraîchissement", "store_returns_refresh : période des dates de retour",
     "SELECT min(d_date)::VARCHAR || ' -> ' || max(d_date)::VARCHAR FROM store_returns_refresh JOIN date_dim ON sr_returned_date_sk = d_date_sk", "info"),
    ("RF10", "Rafraîchissement", "product_reviews_refresh : période des avis",
     "SELECT min(pr_review_date)::VARCHAR || ' -> ' || max(pr_review_date)::VARCHAR FROM product_reviews_refresh", "info"),
    ("RF6", "Rafraîchissement", "inventory_refresh : dates de relevé",
     "SELECT count(DISTINCT inv_date_sk) || ' date(s) : ' || string_agg(DISTINCT d_date::VARCHAR, ', ') FROM inventory_refresh JOIN date_dim ON inv_date_sk = d_date_sk", "info"),
]

# --------------------------------------------------------------------------
# 2b. Conformité à la spécification TPCx-BB v1.7.0 (Appendix B) - le kit prévaut en cas de conflit
# --------------------------------------------------------------------------
_nn = " UNION ALL ".join(f"SELECT '{t}.{c}' AS col, count(*) - count({c}) AS n FROM {t}"
                         for t, cols in SPEC_NOT_NULL.items() for c in cols)
_abs = ", ".join(f"('{t}', '{c}')" for t, cols in SPEC_COLONNES_ABSENTES.items() for c in cols)
WS_ORDERS = ("(SELECT ws_order_number o, any_value(ws_bill_customer_sk) bill, any_value(ws_ship_customer_sk) ship "
             "FROM web_sales GROUP BY 1)")
CONTROLES += [
    ("S1", "Conformité spec", "Colonnes déclarées NOT NULL par la spec contenant des valeurs nulles",
     f"SELECT coalesce(string_agg(col || ' (' || n || ')', ', '), 'aucune') FROM ({_nn}) WHERE n > 0", "aucune"),
    ("S2", "Conformité spec", "Colonnes décrites par la spec absentes des fichiers du kit",
     f"SELECT coalesce(string_agg(t || '.' || c, ', '), 'aucune') FROM (VALUES {_abs}) v(t, c) "
     "WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = v.t AND column_name = v.c)", "aucune"),
    ("S3", "Conformité spec", "Clés de date égales à la valeur par défaut 9999999 (ventes, retours)",
     "SELECT (SELECT count(*) FROM store_sales WHERE ss_sold_date_sk = 9999999) + (SELECT count(*) FROM web_sales WHERE ws_sold_date_sk = 9999999)"
     " + (SELECT count(*) FROM store_returns WHERE sr_returned_date_sk = 9999999) + (SELECT count(*) FROM web_returns WHERE wr_returned_date_sk = 9999999)", "0"),
    ("S4", "Conformité spec", "date_dim : nb de lignes / première clé (spec B.2 : 73049 / 1)",
     "SELECT count(*) || ' / ' || min(d_date_sk) FROM date_dim", "73049 / 1"),
    ("S5", "Conformité spec", "date_dim : d_dow du lundi 01/01/2001 (spec : 1 = lundi)",
     "SELECT d_dow FROM date_dim WHERE d_date = DATE '2001-01-01'", "1"),
    ("P1", "Paramètre générateur", "Lignes par ticket magasin min-max (spec SS_ITEMS_PER_ORDER : 1-14)",
     "SELECT min(n) || '-' || max(n) FROM (SELECT count(*) n FROM store_sales GROUP BY ss_ticket_number)", "1-14"),
    ("P2", "Paramètre générateur", "Lignes par commande web min-max (spec WS_ITEMS_PER_ORDER : 1-14)",
     "SELECT min(n) || '-' || max(n) FROM (SELECT count(*) n FROM web_sales GROUP BY ws_order_number)", "1-14"),
    ("P3", "Paramètre générateur", "Remise par ligne magasin en % min-max (spec SS_DISCOUNT_MAX : 0-100 %)",
     "SELECT round(100 * min(ss_ext_discount_amt / ss_ext_list_price), 1) || '-' || round(100 * max(ss_ext_discount_amt / ss_ext_list_price), 1) FROM store_sales", "0.0-100.0"),
    ("P3b", "Paramètre générateur", "Remise moyenne par ligne magasin en % (tirage uniforme 0-100 % => 50 %)",
     "SELECT round(100 * avg(ss_ext_discount_amt / ss_ext_list_price), 1) FROM store_sales", "50.0"),
    ("P4", "Paramètre générateur", "Majoration max prix catalogue / coût magasin (spec SS_MARKUP_MAX : 1.00)",
     "SELECT round(max(ss_list_price / ss_wholesale_cost - 1), 2) FROM store_sales", "1.0"),
    ("P4b", "Paramètre générateur", "Majoration max prix catalogue / coût web (spec WS_MARKUP_MAX : 2.00)",
     "SELECT round(max(ws_list_price / ws_wholesale_cost - 1), 2) FROM web_sales", "2.0"),
    ("P5", "Paramètre générateur", "Délai d'expédition web min-max en jours (spec WS_MIN/MAX_SHIP_DELAY : 1-120)",
     "SELECT min(ws_ship_date_sk - ws_sold_date_sk) || '-' || max(ws_ship_date_sk - ws_sold_date_sk) FROM web_sales", "1-120"),
    ("P6", "Paramètre générateur", "% de tickets magasin avec au moins un retour (spec return_store_sale_likelihood : 10 %)",
     "SELECT round(100 * (SELECT count(DISTINCT sr_ticket_number) FROM store_returns) / count(DISTINCT ss_ticket_number), 1) FROM store_sales", "10.0"),
    ("P7", "Paramètre générateur", "% de retours magasin faits par le client de la vente (spec SR_SAME_CUSTOMER : 80 %)",
     "SELECT round(100 * avg((r.sr_customer_sk = s.ss_customer_sk)::INT), 1) FROM store_returns r JOIN (SELECT DISTINCT ss_ticket_number, ss_item_sk, ss_customer_sk FROM store_sales) s ON s.ss_ticket_number = r.sr_ticket_number AND s.ss_item_sk = r.sr_item_sk", "80.0"),
    ("P8", "Paramètre générateur", "% de commandes web offertes : livré ≠ facturé (spec WS_GIFT_PCT : 7 %)",
     f"SELECT round(100 * avg((ship <> bill)::INT), 2) FROM {WS_ORDERS}", "info"),
    ("P9", "Paramètre générateur", "Avis dont l'auteur n'est pas le client facturé : % rédigés par le destinataire",
     f"SELECT round(100 * avg((p.pr_user_sk = w.ship)::INT), 1) || ' % de ' || count(*) || ' avis' FROM product_reviews p JOIN {WS_ORDERS} w ON w.o = p.pr_order_sk WHERE p.pr_user_sk <> w.bill", "info"),
    ("P10", "Paramètre générateur", "% de clics de visiteurs identifiés (spec visitor_known_likelihood : 50 %)",
     "SELECT round(100 * count(wcs_user_sk) / count(*), 1) FROM web_clickstreams", "info"),
]

ctrl_rows = []
print()
for cid, dim, desc, sql, attendu in CONTROLES:
    val = one(sql)[0]
    if attendu == "info":
        res = "Information"
    elif str(val) == attendu:
        res = "OK"
    else:
        res = "ÉCART spec" if cid.startswith(("S", "P")) else "ANOMALIE"
    ctrl_rows.append({"id": cid, "dimension_qualite": dim, "controle": desc, "resultat": val,
                      "attendu": attendu, "statut": res, "requete_sql": sql})
    print(f"[{cid:<4}] {res:<11} {desc:<92} -> {val}")

# --------------------------------------------------------------------------
# 3. Faisabilité des KPI : valeur test sur l'année 2004, lots data + data_refresh réunis
# --------------------------------------------------------------------------
Y = 2004
SS = f"store_sales_all JOIN date_dim ON ss_sold_date_sk=d_date_sk WHERE d_year={Y}"
WS = f"web_sales_all JOIN date_dim ON ws_sold_date_sk=d_date_sk WHERE d_year={Y}"
KPI = [
    ("K01", f"CA magasin {Y} (ss_ext_sales_price)", f"SELECT round(sum(ss_ext_sales_price), 2) FROM {SS}"),
    ("K01b", f"CA web {Y} (ws_ext_sales_price)", f"SELECT round(sum(ws_ext_sales_price), 2) FROM {WS}"),
    ("K02", f"Nb de transactions magasin {Y} (tickets distincts)", f"SELECT count(DISTINCT ss_ticket_number) FROM {SS}"),
    ("K02b", f"Nb de commandes web {Y} (ws_order_number distincts)", f"SELECT count(DISTINCT ws_order_number) FROM {WS}"),
    ("K03", f"Panier moyen magasin {Y} (CA / tickets)", f"SELECT round(sum(ss_ext_sales_price) / count(DISTINCT ss_ticket_number), 2) FROM {SS}"),
    ("K03b", f"Panier moyen web {Y} (CA / commandes)", f"SELECT round(sum(ws_ext_sales_price) / count(DISTINCT ws_order_number), 2) FROM {WS}"),
    ("K04", f"Quantités vendues magasin {Y}", f"SELECT sum(ss_quantity) FROM {SS}"),
    ("K05", f"Taux de remise magasin {Y} en % (ext_discount_amt / ext_list_price)", f"SELECT round(100 * sum(ss_ext_discount_amt) / sum(ss_ext_list_price), 2) FROM {SS}"),
    ("K05b", f"Taux de coupon magasin {Y} en % (coupon_amt / ext_sales_price)", f"SELECT round(100 * sum(ss_coupon_amt) / sum(ss_ext_sales_price), 2) FROM {SS}"),
    ("K06", f"Évolution CA magasin {Y} vs {Y-1} en %",
     f"SELECT round(100 * (sum(ss_ext_sales_price) FILTER (WHERE d_year={Y}) / sum(ss_ext_sales_price) FILTER (WHERE d_year={Y-1}) - 1), 2) FROM store_sales_all JOIN date_dim ON ss_sold_date_sk=d_date_sk"),
    ("K07", f"Part du web dans le CA (magasin+web) {Y} en %",
     f"SELECT round(100 * w / (w + s), 2) FROM (SELECT (SELECT sum(ws_ext_sales_price) FROM {WS}) w, (SELECT sum(ss_ext_sales_price) FROM {SS}) s)"),
    ("K08", f"Montant des retours magasin {Y} (sr_return_amt, date de retour)",
     f"SELECT round(sum(sr_return_amt), 2) FROM store_returns_all JOIN date_dim ON sr_returned_date_sk=d_date_sk WHERE d_year={Y}"),
    ("K09", f"Taux de retour en quantité, ventes magasin {Y} (cohorte de vente)",
     f"SELECT round(100 * sum(sr_return_quantity) / (SELECT sum(ss_quantity) FROM {SS}), 2) FROM store_returns_all r JOIN (SELECT DISTINCT ss_ticket_number, ss_item_sk, ss_sold_date_sk FROM store_sales_all) s ON s.ss_ticket_number=r.sr_ticket_number AND s.ss_item_sk=r.sr_item_sk JOIN date_dim ON s.ss_sold_date_sk=d_date_sk WHERE d_year={Y}"),
    ("K10", f"Stock moyen par relevé (inv_quantity_on_hand), {Y}",
     f"SELECT round(avg(inv_quantity_on_hand), 1) FROM inventory_all JOIN date_dim ON inv_date_sk=d_date_sk WHERE d_year={Y}"),
    ("K11", f"Marge nette magasin {Y} (ss_net_profit)", f"SELECT round(sum(ss_net_profit), 2) FROM {SS}"),
    ("K11b", f"Taux de marge nette magasin {Y} en % (net_profit / net_paid)", f"SELECT round(100 * sum(ss_net_profit) / sum(ss_net_paid), 2) FROM {SS}"),
    ("K12", f"Nb de lignes de vente magasin {Y} (≠ nb de transactions)", f"SELECT count(*) FROM {SS}"),
    ("K13", f"Taux de rupture {Y} en % (relevés stock = 0 / relevés renseignés)",
     f"SELECT round(100 * count(*) FILTER (WHERE inv_quantity_on_hand = 0) / count(inv_quantity_on_hand), 3) FROM inventory_all JOIN date_dim ON inv_date_sk=d_date_sk WHERE d_year={Y}"),
    ("K14", f"Note moyenne des avis {Y} (pr_review_rating, 1 à 5)",
     f"SELECT round(avg(pr_review_rating), 2) FROM product_reviews_all WHERE year(pr_review_date)={Y}"),
    ("K15", f"Part des clics web liés à une vente {Y} en % (≠ taux de conversion par session)",
     f"SELECT round(100 * count(wcs_sales_sk) / count(*), 2) FROM web_clickstreams_all JOIN date_dim ON wcs_click_date_sk=d_date_sk WHERE d_year={Y}"),
    ("K16", "Écart prix courant vs prix concurrent en % (ratio des moyennes / médiane des écarts relatifs)",
     "SELECT round(100 * (avg(i_current_price) / avg(imp_competitor_price) - 1), 2) || ' / ' || round(100 * median(i_current_price / imp_competitor_price - 1), 2) FROM item JOIN item_marketprices ON imp_item_sk=i_item_sk WHERE imp_competitor_price > 0"),
]
kpi_rows = []
print()
for kid, desc, sql in KPI:
    val = one(sql)[0]
    kpi_rows.append({"id": kid, "kpi": desc, "valeur_test": val, "requete_sql": sql})
    print(f"[{kid:<4}] {desc:<88} = {val}")

annees = con.execute("""
    SELECT d_year, 'magasin', count(*), round(sum(ss_ext_sales_price),2), count(DISTINCT ss_ticket_number)
    FROM store_sales JOIN date_dim ON ss_sold_date_sk=d_date_sk GROUP BY 1
    UNION ALL
    SELECT d_year, 'web', count(*), round(sum(ws_ext_sales_price),2), count(DISTINCT ws_order_number)
    FROM web_sales JOIN date_dim ON ws_sold_date_sk=d_date_sk GROUP BY 1
    ORDER BY 1, 2""").fetchall()
mois = con.execute(f"""
    SELECT d_moy, round(sum(ss_ext_sales_price), 2) FROM store_sales JOIN date_dim ON ss_sold_date_sk=d_date_sk
    WHERE d_year={Y} GROUP BY 1 ORDER BY 1""").fetchall()
cat = con.execute("""
    SELECT coalesce(i_category, '(nulle)'), round(100 * sum(sr_return_quantity) / sum(ss_quantity), 2)
    FROM store_sales s JOIN item ON ss_item_sk = i_item_sk
    LEFT JOIN store_returns r ON r.sr_ticket_number = s.ss_ticket_number AND r.sr_item_sk = s.ss_item_sk
    GROUP BY 1 ORDER BY 2""").fetchall()
notes = con.execute("SELECT pr_review_rating, count(*) FROM product_reviews GROUP BY 1 ORDER BY 1").fetchall()
print()
for a in annees:
    print(a)
print(mois, cat, notes, sep="\n")


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter=";")
        w.writeheader()
        w.writerows(rows)


write_csv(PREUVES / "relations_verifiees.csv", rel_rows)
write_csv(PREUVES / "controles_qualite.csv", ctrl_rows)
write_csv(PREUVES / "kpi_faisabilite.csv", kpi_rows)
write_csv(PREUVES / "ventes_par_annee.csv",
          [{"annee": a[0], "canal": a[1], "nb_lignes": a[2], "ca": a[3], "nb_transactions": a[4]} for a in annees])
write_csv(PREUVES / "ventes_par_mois.csv", [{"annee": Y, "mois": m[0], "ca_magasin": m[1]} for m in mois])
write_csv(PREUVES / "retours_par_categorie.csv", [{"categorie": c[0], "taux_retour_pct": c[1]} for c in cat])
write_csv(PREUVES / "notes_avis.csv", [{"note": n[0], "nb_avis": n[1]} for n in notes])

# --------------------------------------------------------------------------
# 4. Valeurs des KPI par année (2001-2005) et par axe d'analyse
#    Lots data + data_refresh réunis (0 recouvrement vérifié par RF4) : 2005 devient complète.
# --------------------------------------------------------------------------
con.execute("""CREATE TABLE annees AS SELECT DISTINCT d_year AS annee FROM store_sales_all
               JOIN date_dim ON ss_sold_date_sk = d_date_sk ORDER BY 1""")
SSY = "(SELECT d_year AS annee, s.* FROM store_sales_all s JOIN date_dim ON ss_sold_date_sk = d_date_sk)"
WSY = "(SELECT d_year AS annee, w.* FROM web_sales_all w JOIN date_dim ON ws_sold_date_sk = d_date_sk)"
PAR_AN = [  # (id, libellé, expression SQL renvoyant (annee, valeur))
    ("K01", "CA magasin", f"SELECT annee, round(sum(ss_ext_sales_price), 0) FROM {SSY} GROUP BY 1"),
    ("K01b", "CA web", f"SELECT annee, round(sum(ws_ext_sales_price), 0) FROM {WSY} GROUP BY 1"),
    ("K02", "Tickets magasin", f"SELECT annee, count(DISTINCT ss_ticket_number) FROM {SSY} GROUP BY 1"),
    ("K02b", "Commandes web", f"SELECT annee, count(DISTINCT ws_order_number) FROM {WSY} GROUP BY 1"),
    ("K03", "Panier moyen magasin", f"SELECT annee, round(sum(ss_ext_sales_price) / count(DISTINCT ss_ticket_number), 2) FROM {SSY} GROUP BY 1"),
    ("K03b", "Panier moyen web", f"SELECT annee, round(sum(ws_ext_sales_price) / count(DISTINCT ws_order_number), 2) FROM {WSY} GROUP BY 1"),
    ("K04", "Quantités vendues magasin", f"SELECT annee, sum(ss_quantity) FROM {SSY} GROUP BY 1"),
    ("K05", "Taux de remise magasin (%)", f"SELECT annee, round(100 * sum(ss_ext_discount_amt) / sum(ss_ext_list_price), 2) FROM {SSY} GROUP BY 1"),
    ("K05b", "Taux de coupon magasin (%)", f"SELECT annee, round(100 * sum(ss_coupon_amt) / sum(ss_ext_sales_price), 2) FROM {SSY} GROUP BY 1"),
    ("K06", "Évolution CA magasin N/N-1 (%)",
     f"SELECT annee, round(100 * (ca / lag(ca) OVER (ORDER BY annee) - 1), 2) FROM (SELECT annee, sum(ss_ext_sales_price) ca FROM {SSY} GROUP BY 1)"),
    ("K07", "Part du web dans le CA (%)",
     f"SELECT s.annee, round(100 * w.ca / (w.ca + s.ca), 2) FROM (SELECT annee, sum(ss_ext_sales_price) ca FROM {SSY} GROUP BY 1) s "
     f"JOIN (SELECT annee, sum(ws_ext_sales_price) ca FROM {WSY} GROUP BY 1) w USING (annee)"),
    ("K08", "Montant des retours magasin (date de retour)",
     "SELECT d_year, round(sum(sr_return_amt), 0) FROM store_returns_all JOIN date_dim ON sr_returned_date_sk = d_date_sk GROUP BY 1"),
    ("K09", "Taux de retour en quantité, magasin (%)",
     f"SELECT v.annee, round(100 * coalesce(r.q, 0) / v.q, 2) FROM (SELECT annee, sum(ss_quantity) q FROM {SSY} GROUP BY 1) v LEFT JOIN "
     f"(SELECT s.annee, sum(r.sr_return_quantity) q FROM store_returns_all r JOIN (SELECT DISTINCT annee, ss_ticket_number, ss_item_sk FROM {SSY}) s "
     "ON s.ss_ticket_number = r.sr_ticket_number AND s.ss_item_sk = r.sr_item_sk GROUP BY 1) r USING (annee)"),
    ("K10", "Stock moyen par relevé (unités)",
     "SELECT d_year, round(avg(inv_quantity_on_hand), 1) FROM inventory_all JOIN date_dim ON inv_date_sk = d_date_sk GROUP BY 1"),
    ("K11", "Marge nette magasin", f"SELECT annee, round(sum(ss_net_profit), 0) FROM {SSY} GROUP BY 1"),
    ("K11b", "Taux de marge nette magasin (%)", f"SELECT annee, round(100 * sum(ss_net_profit) / sum(ss_net_paid), 2) FROM {SSY} GROUP BY 1"),
    ("K12", "Lignes de vente magasin", f"SELECT annee, count(*) FROM {SSY} GROUP BY 1"),
    ("K13", "Taux de rupture (%)",
     "SELECT d_year, round(100 * count(*) FILTER (WHERE inv_quantity_on_hand = 0) / count(inv_quantity_on_hand), 3) FROM inventory_all JOIN date_dim ON inv_date_sk = d_date_sk GROUP BY 1"),
    ("K14", "Note moyenne des avis (/5)", "SELECT year(pr_review_date), round(avg(pr_review_rating), 2) FROM product_reviews_all GROUP BY 1"),
    ("K15", "Part des clics liés à une vente (%)",
     "SELECT d_year, round(100 * count(wcs_sales_sk) / count(*), 2) FROM web_clickstreams_all JOIN date_dim ON wcs_click_date_sk = d_date_sk GROUP BY 1"),
]
ANS = [a[0] for a in con.execute("SELECT annee FROM annees").fetchall()]
kpi_an_rows = []
for kid, lib, sql in PAR_AN:
    vals = dict(con.execute(sql).fetchall())
    kpi_an_rows.append({"id": kid, "kpi": lib, **{str(a): ("" if vals.get(a) is None else vals[a]) for a in ANS}})
write_csv(PREUVES / "kpi_par_annee.csv", kpi_an_rows)

par_cat = con.execute(f"""
    WITH v AS (SELECT i_category c, round(sum(ss_ext_sales_price), 0) ca_mag, sum(ss_quantity) qte_mag
               FROM {SSY} s JOIN item ON ss_item_sk = i_item_sk WHERE annee = {Y} GROUP BY 1),
         w AS (SELECT i_category c, round(sum(ws_ext_sales_price), 0) ca_web FROM {WSY} s JOIN item ON ws_item_sk = i_item_sk WHERE annee = {Y} GROUP BY 1),
         r AS (SELECT i_category c, round(100 * sum(sr_return_quantity) / (SELECT sum(ss_quantity) FROM {SSY} x JOIN item i2 ON x.ss_item_sk = i2.i_item_sk
                                                                           WHERE annee = {Y} AND i2.i_category = item.i_category), 2) taux_retour
               FROM store_returns_all rr JOIN (SELECT DISTINCT ss_ticket_number, ss_item_sk FROM {SSY} WHERE annee = {Y}) s
                 ON s.ss_ticket_number = rr.sr_ticket_number AND s.ss_item_sk = rr.sr_item_sk JOIN item ON rr.sr_item_sk = i_item_sk GROUP BY 1, item.i_category),
         a AS (SELECT i_category c, round(avg(pr_review_rating), 2) note FROM product_reviews_all JOIN item ON pr_item_sk = i_item_sk
               WHERE year(pr_review_date) = {Y} GROUP BY 1)
    SELECT v.c, ca_mag, ca_web, qte_mag, taux_retour, note FROM v JOIN w USING (c) LEFT JOIN r USING (c) LEFT JOIN a USING (c)
    ORDER BY ca_mag + ca_web DESC""").fetchall()
write_csv(PREUVES / "kpi_par_categorie.csv",
          [{"categorie": r[0], "ca_magasin": r[1], "ca_web": r[2], "quantites_magasin": r[3], "taux_retour_pct": r[4], "note_moyenne": r[5]}
           for r in par_cat])

par_mag = con.execute(f"""
    SELECT s_store_name, s_city, s_state, round(sum(ss_ext_sales_price), 0), count(DISTINCT ss_ticket_number),
           round(sum(ss_ext_sales_price) / count(DISTINCT ss_ticket_number), 2), sum(ss_quantity)
    FROM {SSY} s JOIN store ON ss_store_sk = s_store_sk WHERE annee = {Y} GROUP BY 1, 2, 3 ORDER BY 4 DESC""").fetchall()
write_csv(PREUVES / "kpi_par_magasin.csv",
          [{"magasin": r[0], "ville": r[1], "etat": r[2], "ca": r[3], "tickets": r[4], "panier_moyen": r[5], "quantites": r[6]}
           for r in par_mag])
print("\nKPI par année :", ANS)
for r in kpi_an_rows:
    print(r)
print("Par catégorie :", par_cat[:3], "...")
print("Par magasin :", par_mag[:3], "...")
print(f"\nPreuves écrites dans {PREUVES.relative_to(ROOT)}")
