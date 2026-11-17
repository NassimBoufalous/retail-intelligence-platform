"""
04 - Construction des livrables finaux (XLSX + PDF).

    1. UC01_dictionnaire_KPI.xlsx (+ .md -> .pdf)   à partir de la liste KPI ci-dessous
                                                et des valeurs mesurées (UC01_preuves/kpi_faisabilite.csv)
    2. UC01_inventaire_sources.xlsx                 à partir des CSV de UC01_preuves (scripts 01 et 02)
    3. Tous les .md des livrables -> .pdf      (Markdown -> HTML -> PDF via Microsoft Edge headless)

Les .md restent la SOURCE (versionnable, lisible dans Git). Les .pdf sont générés.
"""
import csv
import subprocess
import tempfile
from pathlib import Path

import markdown
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parents[2]
LIV = ROOT / "Jour1"
UC1 = LIV / "01_UC01_besoin_exploration"
PRV = UC1 / "UC01_preuves"
EDGE = Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")


def read_csv(path):
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f, delimiter=";"))


# =============================================================================
# 1. Dictionnaire des KPI
# =============================================================================
V = {r["id"]: r["valeur_test"] for r in read_csv(PRV / "kpi_faisabilite.csv")}
FREQ_M = "Mensuelle (+ cumul annuel)"
ANNEE = 2004  # dernière année complète du kit (ventes du 01/01/2001 au 13/12/2005, refresh jusqu'au 31/12/2005)
KPI = [
    # id, nom, définition métier, formule, unité, périmètre / granularité, fréquence, sources/champs,
    # données manquantes, limites, statut, valeur test
    ("K01", "Chiffre d'affaires (CA)", "Montant des ventes après remise sur prix catalogue, avant coupon, hors taxe",
     "SUM(ss_ext_sales_price) ; web : SUM(ws_ext_sales_price)", "Unité monétaire (devise non documentée)",
     "Ligne de vente ; agrégeable par date, produit, magasin, canal, client", FREQ_M,
     "store_sales.ss_ext_sales_price, web_sales.ws_ext_sales_price, date_dim", "Aucune (0 % de nulls sur date, article, montant : C1, C2)",
     "HT déduit (taxe dans ss_ext_tax). Retours non déduits (K08). 2005 incomplète (s'arrête au 13/12 ; fin d'année dans data_refresh)",
     "Vérifié", f"magasin {V['K01']} / web {V['K01b']}"),
    ("K02", "Nombre de transactions", "Nombre de passages en caisse (tickets) ; web : nombre de commandes",
     "COUNT(DISTINCT ss_ticket_number) ; web : COUNT(DISTINCT ws_order_number)", "Nombre",
     "Ticket / commande", FREQ_M, "store_sales.ss_ticket_number, web_sales.ws_order_number",
     "Aucune (0 % de nulls)", "≠ nombre de lignes (K12). Un ticket = 1 client, 1 magasin, 1 date (T3-T5). Les n° de ticket et de commande web partagent la même plage 0-89 099 (T8) : toujours préciser le canal",
     "Vérifié", f"magasin {V['K02']} / web {V['K02b']}"),
    ("K03", "Panier moyen", "CA moyen par transaction", "K01 / K02 (même canal, même périmètre)", "Unité monétaire / transaction",
     "Ticket / commande ; par magasin, mois, canal", FREQ_M, "store_sales, web_sales", "Aucune : CA et numéro de ticket / commande renseignés sur 100 % des lignes (contrôles C1, C2)",
     "Valeur peu réaliste (environ 14 400 par ticket : 1 à 14 articles de 1 à 100 unités, paramètres de la spécification P1) : effet du générateur synthétique",
     "Vérifié (calcul) / valeur à interpréter avec prudence", f"magasin {V['K03']} / web {V['K03b']}"),
    ("K04", "Quantités vendues", "Nombre d'unités vendues", "SUM(ss_quantity)", "Unités",
     "Ligne de vente ; par produit, catégorie, magasin", FREQ_M, "store_sales.ss_quantity, item",
     "Aucune", "Unités hétérogènes selon les produits (i_units : Box, Each…) : ne pas additionner entre catégories sans précaution",
     "Vérifié", V["K04"]),
    ("K05", "Taux de remise", "Part du prix catalogue abandonnée", "SUM(ss_ext_discount_amt) / SUM(ss_ext_list_price)", "%",
     "Ligne de vente", FREQ_M, "store_sales.ss_ext_discount_amt, ss_ext_list_price",
     "Aucune", "Environ 50 % sur toutes les périodes : la spécification tire la remise entre 0 et 100 % (SS_DISCOUNT_MAX = 1,00 ; contrôles P3, P3b), donc non interprétable. "
     f"Formule validée : ext_discount_amt = ext_list_price - ext_sales_price (M2). Coupon séparé (K05b = {V['K05b']} %)",
     "Vérifié (calcul) / valeur non interprétable", f"{V['K05']} % (coupon {V['K05b']} %)"),
    ("K06", "Évolution du CA (N / N-1)", "Croissance du CA d'une période par rapport à la même période l'année précédente",
     "K01(N) / K01(N-1) - 1", "%", "Année, mois ; par magasin, canal", FREQ_M, "store_sales, date_dim",
     "2005 incomplète", "Comparer uniquement des périodes complètes et de même durée. CA très stable d'une année et d'un mois à l'autre (synthétique)",
     "Vérifié", f"{V['K06']} % ({ANNEE} vs {ANNEE-1})"),
    ("K07", "Répartition du CA par canal", "Part de chaque canal dans le CA total", "K01(canal) / Σ K01(canaux)", "%",
     "Année, mois", FREQ_M, "store_sales, web_sales", "Aucune : montants magasin et web renseignés sur 100 % des lignes (C1, C2)",
     "Les deux canaux n'ont pas le même modèle (web : client facturé / livré, date d'expédition, site, page…)", "Vérifié", f"web {V['K07']} %"),
    ("K08", "Montant des retours", "Montant remboursé ou crédité au titre des articles retournés", "SUM(sr_return_amt) ; web : SUM(wr_return_amt)",
     "Unité monétaire", "Ligne de retour ; par date de retour, produit, motif", FREQ_M, "store_returns, web_returns, reason",
     "0,03 % de motifs nuls", "Date de RETOUR ≠ date de VENTE (retours jusqu'au 14/11/2007) : choisir la date de référence selon la question. Le lot data_refresh apporte 426 retours datés de 2001 à 2007, dont 407 sans vente correspondante (RF7) : comptés ici (date de retour), exclus de K09", "Vérifié", V["K08"]),
    ("K09", "Taux de retour (quantité)", "Part des unités vendues sur une période qui ont été retournées",
     "SUM(sr_return_quantity des retours liés aux ventes de la période) / SUM(ss_quantity de la période)", "%",
     "Cohorte de vente (ticket + article)", "Mensuelle (avec délai de maturité)", "store_returns ⨝ store_sales (ss_ticket_number, ss_item_sk)",
     "Aucune sur les quantités vendues et retournées (0 % de nulls) ; motif de retour nul sur 0,03 % des retours", "Lien vérifié (R1 : 0 orphelin ; R3/R4 cohérents). 470 retours (1,2 %) portent sur un ticket+article présent sur plusieurs lignes (R3b) : rattacher au niveau (ticket, article) agrégé",
     "Vérifié", f"{V['K09']} %"),
    ("K10", "Niveau de stock", "Quantité disponible à la date du relevé", "SUM(inv_quantity_on_hand) à une date ; moyenne des relevés sur une période",
     "Unités", "Produit × entrepôt × semaine (snapshot)", "Hebdomadaire", "inventory, warehouse, item (+ item_refresh)",
     "0,03 % de quantités nulles ; aucun stock par magasin", "Mesure SEMI-ADDITIVE : ne jamais sommer dans le temps. Moyenne environ 500 (uniforme 0-1000) : synthétique",
     "Vérifié", f"moyenne {V['K10']}"),
    ("K11", "Marge nette", "Résultat de la vente après coût d'achat", "SUM(ss_net_profit) = SUM(ss_net_paid - ss_ext_wholesale_cost)", "Unité monétaire",
     "Ligne de vente", FREQ_M, "store_sales.ss_net_profit", "Aucune",
     f"Formule validée (M3b), mais marge globale négative (taux {V['K11b']} %) et 74 % de lignes déficitaires : majoration ≤ 100 % et remise jusqu'à 100 % selon la spécification (P3, P4), non interprétable",
     "Vérifié (calcul) / valeur non interprétable", V["K11"]),
    ("K12", "Nombre de lignes de vente", "Nombre de lignes de ticket, cumulé", "COUNT(*)", "Nombre",
     "Ligne de vente", FREQ_M, "store_sales", "Aucune : 0 ligne sans date, article ou montant (C1)", "Indicateur technique. NE PAS confondre avec K02 (7,5 lignes par ticket)", "Vérifié", V["K12"]),
    ("K13", "Taux de rupture", "Part des relevés produit × entrepôt avec un stock nul", "COUNT(qty = 0) / COUNT(qty renseignée)", "%",
     "Produit × entrepôt × semaine", "Hebdomadaire", "inventory", "Relevés à quantité nulle (0,03 %) exclus : inconnu ≠ rupture",
     "Stock = 0 ≠ demande perdue", "Vérifié", f"{V['K13']} %"),
    ("K14", "Note moyenne des avis", "Satisfaction client exprimée par produit", "AVG(pr_review_rating)", "Note de 1 à 5",
     "Avis ; par produit, catégorie, mois", FREQ_M, "product_reviews (pr_review_rating, pr_item_sk, pr_review_date)",
     "Aucune note nulle ; 5 textes vides", "Distribution très asymétrique (61 % de notes 5) : suivre aussi la part de notes 1-2. Avis liés à une commande web (R7 : 100 %), pas aux ventes magasin",
     "Vérifié", V["K14"]),
    ("K15", "Part des clics liés à une vente", "Part des clics web associés à une commande", "COUNT(wcs_sales_sk) / COUNT(*)", "%",
     "Clic ; par page, produit, mois", "Hebdomadaire", "web_clickstreams (wcs_sales_sk → ws_order_number)",
     "42 % de clics anonymes (wcs_user_sk nul)", "Ce N'EST PAS un taux de conversion : pas d'identifiant de session ; un clic ≠ un ajout au panier ≠ un achat",
     "Vérifié (calcul) / définition à valider", f"{V['K15']} %"),
    ("K16", "Écart de prix vs concurrents", "Positionnement prix de l'enseigne par rapport aux concurrents",
     "AVG(i_current_price) / AVG(imp_competitor_price) - 1 ; médiane des écarts relatifs", "%",
     "Produit × concurrent", "Mensuelle", "item.i_current_price, item_marketprices (imp_item_sk, imp_competitor_price)",
     "0,03 % de prix concurrents nuls", "Une moyenne de ratios est faussée par les très petits prix : utiliser ratio des moyennes ou médiane. Périodes de validité (imp_start/end_date) à respecter",
     "Vérifié", f"{V['K16']} %"),
]
HEAD = ["ID", "Nom", "Définition métier", "Formule", "Unité", "Périmètre / granularité", "Fréquence",
        "Sources / champs", "Données manquantes", "Limites d'interprétation", "Statut", f"Valeur {ANNEE}"]


def save_xlsx(wb, path):
    """Enregistre un classeur ; s'il est ouvert dans Excel (verrouillé), on prévient et on continue."""
    try:
        wb.save(path)
    except PermissionError:
        print(f"  ATTENTION : {path.name} est ouvert dans Excel, il n'a pas été mis à jour (fermer Excel et relancer)")


def write_sheet(ws, header, rows, widths=None):
    ws.append(header)
    for r in rows:
        ws.append(list(r))
    for c in ws[1]:
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="2A78D6")
        c.alignment = Alignment(wrap_text=True, vertical="top")
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.alignment = Alignment(wrap_text=True, vertical="top")
    for i, _ in enumerate(header, 1):
        ws.column_dimensions[get_column_letter(i)].width = (widths or {}).get(i, 18)
    ws.freeze_panes = "B2"
    ws.auto_filter.ref = ws.dimensions


# Valeurs calculées (script 02) : par année, par catégorie, par magasin
AN = read_csv(PRV / "kpi_par_annee.csv")
ANS = [c for c in AN[0] if c.isdigit()]
CAT = read_csv(PRV / "kpi_par_categorie.csv")
MAG = read_csv(PRV / "kpi_par_magasin.csv")


def fr(v, dec=None):
    """Nombre au format français : espace pour les milliers, virgule décimale."""
    if v in ("", None):
        return "–"
    x = float(v)
    if dec is None:
        dec = 0 if abs(x) >= 1000 or x == int(x) else 2
    return f"{x:,.{dec}f}".replace(",", " ").replace(".", ",")


DEC = {"K10": 1, "K13": 3, "K03": 2, "K03b": 2}  # précision d affichage par KPI
num_f = lambda v: float(v) if v not in ("", None) else None
wb = Workbook()
write_sheet(wb.active, HEAD, KPI, {1: 6, 2: 22, 3: 34, 4: 38, 9: 28, 10: 45, 11: 18, 12: 24})
wb.active.title = "KPI"
write_sheet(wb.create_sheet("Valeurs_par_annee"), ["ID", "KPI"] + ANS,
            [[r["id"], r["kpi"]] + [num_f(r[a]) for a in ANS] for r in AN], {2: 44})
write_sheet(wb.create_sheet(f"Par_categorie_{ANNEE}"), ["Catégorie", "CA magasin", "CA web", "Quantités magasin", "Taux de retour (%)", "Note moyenne (/5)"],
            [[r["categorie"], num_f(r["ca_magasin"]), num_f(r["ca_web"]), num_f(r["quantites_magasin"]), num_f(r["taux_retour_pct"]), num_f(r["note_moyenne"])] for r in CAT], {1: 28})
write_sheet(wb.create_sheet(f"Par_magasin_{ANNEE}"), ["Magasin", "Ville", "État", "CA", "Tickets", "Panier moyen", "Quantités"],
            [[r["magasin"], r["ville"], r["etat"], num_f(r["ca"]), num_f(r["tickets"]), num_f(r["panier_moyen"]), num_f(r["quantites"])] for r in MAG], {1: 18, 2: 18})
save_xlsx(wb, UC1 / "UC01_dictionnaire_KPI.xlsx")

# Version Markdown/PDF du dictionnaire
md = ["# UC-01 – Dictionnaire initial des KPI", "",
      "Les valeurs sont calculées par `scripts/02_relations_et_controles.py` sur les lots `data` et `data_refresh` "
      "réunis : l'année 2005 est ainsi complète. Les requêtes SQL sont dans `UC01_preuves/kpi_faisabilite.csv` et "
      "les valeurs dans `UC01_preuves/kpi_par_annee.csv`. Montants en unité monétaire (devise non documentée).", "",
      "## 1. Synthèse", "",
      f"| ID | KPI | Statut | Valeur {ANNEE} |", "|---|---|---|---|"]
md += [f"| {k[0]} | {k[1]} | {k[10]} | {k[11]} |" for k in KPI]
md += ["", "## 2. Valeurs par année", "", "| ID | KPI | " + " | ".join(ANS) + " |", "|---|---|" + "---|" * len(ANS)]
md += [f"| {r['id']} | {r['kpi']} | " + " | ".join(fr(r[a], DEC.get(r["id"])) for a in ANS) + " |" for r in AN]
md += ["", "K06 n'a pas de valeur en 2001 : il n'existe pas d'année précédente dans le kit. K08 est daté à la date du retour, "
       "K09 à la date de la vente d'origine. K08 est plus faible en 2001 car les retours arrivent après les ventes (premier retour le 25/01/2001).",
       "", f"## 3. Résultats par catégorie de produit ({ANNEE})", "",
       "| Catégorie | CA magasin | CA web | Quantités magasin | Taux de retour | Note moyenne |", "|---|---|---|---|---|---|"]
md += [f"| {r['categorie']} | {fr(r['ca_magasin'])} | {fr(r['ca_web'])} | {fr(r['quantites_magasin'])} | {fr(r['taux_retour_pct'])} % | {fr(r['note_moyenne'])} |" for r in CAT]
md += ["", f"## 4. Résultats par magasin ({ANNEE})", "",
       "| Magasin | Ville (État) | CA | Tickets | Panier moyen | Quantités |", "|---|---|---|---|---|---|"]
md += [f"| {r['magasin']} | {r['ville']} ({r['etat']}) | {fr(r['ca'])} | {fr(r['tickets'])} | {fr(r['panier_moyen'], 2)} | {fr(r['quantites'])} |" for r in MAG]
md += ["", "## 5. Fiches des indicateurs"]
by_id = {r["id"]: r for r in AN}
for k in KPI:
    md += ["", f"### {k[0]} – {k[1]}", "", "| Élément | Description |", "|---|---|"]
    md += [f"| {h} | {str(v).replace('|', '/')} |" for h, v in zip(HEAD[2:], k[2:])]
    lignes = [by_id[i] for i in (k[0], k[0] + "b") if i in by_id]
    for r in lignes:
        md += [f"| Valeurs {ANS[0]}–{ANS[-1]} ({r['kpi']}) | " + " ; ".join(f"{a} : {fr(r[a], DEC.get(r['id']))}" for a in ANS) + " |"]
(UC1 / "UC01_dictionnaire_KPI.md").write_text("\n".join(md) + "\n", encoding="utf-8")

# =============================================================================
# 2. Inventaire des sources (classeur multi-onglets)
# =============================================================================
DOMAINES = {
    "store_sales": "Ventes magasin", "web_sales": "Ventes web", "store_returns": "Retours magasin",
    "web_returns": "Retours web", "inventory": "Stocks", "warehouse": "Entrepôts", "item": "Produits",
    "item_marketprices": "Prix concurrents", "promotion": "Promotions", "customer": "Clients",
    "customer_address": "Clients (adresses)", "customer_demographics": "Clients (démographie)",
    "household_demographics": "Clients (foyers)", "income_band": "Clients (tranches de revenus)",
    "store": "Magasins", "date_dim": "Dates", "time_dim": "Heures", "reason": "Motifs de retour",
    "ship_mode": "Modes de livraison", "web_site": "Sites web", "web_page": "Pages web",
    "web_clickstreams": "Navigation web", "product_reviews": "Avis produits",
}
GRAIN = {
    "store_sales": "1 ligne de ticket (ticket+article NON unique)", "web_sales": "1 ligne de commande (commande+article NON unique)",
    "store_returns": "1 ligne de retour × ticket", "web_returns": "1 ligne de retour × commande",
    "inventory": "1 produit × 1 entrepôt × 1 semaine", "item": "1 produit (pas d'historisation)",
    "store": "1 magasin (pas d'historisation)", "date_dim": "1 jour", "time_dim": "1 seconde de la journée",
    "customer": "1 client", "web_clickstreams": "1 clic (sans identifiant de session)",
    "product_reviews": "1 avis", "item_marketprices": "1 prix concurrent × produit × période",
}
TYPE_T = {"sales": "Fait", "returns": "Fait", "inventory": "Fait (snapshot)", "clickstreams": "Fait (événement)",
          "reviews": "Fait (événement + texte)", "marketprices": "Fait (prix)"}
tables = read_csv(PRV / "inventaire_tables.csv")
rows = []
num = lambda v: int(v) if str(v).lstrip("-").isdigit() else v
for t in tables:
    kind = next((v for k, v in TYPE_T.items() if k in t["table"]), "Dimension / référentiel")
    if t["format"].startswith("Annexe"):
        kind = "Métadonnée (audit du générateur)"
    rows.append([t["lot"], t["table"], DOMAINES.get(t["table"], ""), kind, t["fichier"], t["chemin"], t["format"],
                 t["taille"], num(t["taille_octets"]), num(t["nb_lignes"]), num(t["nb_colonnes"]), num(t["champs_par_ligne"]),
                 t["cles_candidates"], GRAIN.get(t["table"], "1 ligne = 1 occurrence du référentiel"),
                 num(t["colonnes_avec_nulls"]), t["statut"]])

wb = Workbook()
ws = wb.active
ws.title = "Tables"
write_sheet(ws, ["Lot", "Table", "Domaine", "Type", "Fichier", "Chemin", "Format", "Taille", "Octets", "Nb lignes",
                 "Nb colonnes", "Champs par ligne (mesuré)", "Clé(s) candidate(s) mesurée(s)", "Grain (déduit)",
                 "Nb colonnes avec nulls", "Statut"], rows,
            {2: 22, 3: 24, 6: 48, 7: 30, 13: 34, 14: 34})
for sheet, fname, w in [("Colonnes", "dictionnaire_colonnes.csv", {4: 26, 9: 30, 10: 30}),
                        ("Relations", "relations_verifiees.csv", {1: 20, 2: 22, 4: 22, 5: 22, 9: 26, 15: 30}),
                        ("Controles_qualite", "controles_qualite.csv", {3: 60, 4: 18, 7: 80}),
                        ("KPI_faisabilite", "kpi_faisabilite.csv", {2: 60, 3: 18, 4: 80}),
                        ("Ventes_par_annee", "ventes_par_annee.csv", {}),
                        ("Ventes_par_mois", "ventes_par_mois.csv", {}),
                        ("Retours_par_categorie", "retours_par_categorie.csv", {1: 28}),
                        ("Notes_avis", "notes_avis.csv", {})]:
    data = read_csv(PRV / fname)
    write_sheet(wb.create_sheet(sheet), list(data[0].keys()), [[num(v) for v in r.values()] for r in data], w)
save_xlsx(wb, UC1 / "UC01_inventaire_sources.xlsx")
print("XLSX OK")

# =============================================================================
# 3. Markdown -> PDF
# =============================================================================
CSS = """
@page { size: A4; margin: 16mm 14mm; }
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 10pt; color: #0b0b0b; line-height: 1.45; }
h1 { font-size: 17pt; color: #0b0b0b; border-bottom: 3px solid #2a78d6; padding-bottom: 4px; }
h2 { font-size: 13pt; color: #1d5fae; margin-top: 18px; border-bottom: 1px solid #e1e0d9; }
h3 { font-size: 11pt; color: #52514e; }
table { border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 8.5pt; page-break-inside: auto; }
tr { page-break-inside: avoid; }
th { background: #2a78d6; color: #fff; text-align: left; padding: 4px 5px; }
td { border: 1px solid #e1e0d9; padding: 3px 5px; vertical-align: top; }
tr:nth-child(even) td { background: #f6f6f3; }
code { background: #efeee9; padding: 0 3px; border-radius: 3px; font-size: 8.5pt; }
pre { background: #f3f3ef; padding: 8px; border-radius: 4px; font-size: 8pt; white-space: pre-wrap; }
blockquote { border-left: 4px solid #eda100; background: #fdf6e3; margin: 8px 0; padding: 6px 10px; }
img { max-width: 100%; margin: 8px 0; border: 1px solid #e1e0d9; }
.footer { color: #898781; font-size: 8pt; margin-top: 20px; }
"""


def md_to_pdf(md_path: Path):
    html_body = markdown.markdown(md_path.read_text(encoding="utf-8"), extensions=["tables", "fenced_code", "sane_lists"])
    html = (f"<!doctype html><html lang='fr'><head><meta charset='utf-8'><title>{md_path.stem.replace('_', ' ')} – Retail Intelligence Platform</title><style>{CSS}</style></head><body>"
            f"{html_body}<p class='footer'>Retail Intelligence Platform – TP J1 – {md_path.name} – "
            f"données : kit TPCx-BB retail_project (data + data_refresh)</p></body></html>")
    # Le HTML est écrit à côté du .md pour que les chemins d'images relatifs fonctionnent
    tmp = md_path.with_suffix(".tmp.html")
    tmp.write_text(html, encoding="utf-8")
    pdf = md_path.with_suffix(".pdf")
    try:
        # Edge headless se bloque parfois : on réessaie (3 tentatives) avec un profil neuf à chaque fois
        for tentative in range(1, 4):
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as prof:
                try:
                    subprocess.run([str(EDGE), "--headless", "--disable-gpu", "--no-pdf-header-footer",
                                    "--no-first-run", "--disable-extensions", f"--user-data-dir={prof}",
                                    f"--print-to-pdf={pdf}", tmp.resolve().as_uri()],
                                   check=True, capture_output=True, timeout=60)
                    break
                except subprocess.TimeoutExpired:
                    print(f"  (Edge bloqué sur {md_path.name}, tentative {tentative}/3)")
        else:
            raise RuntimeError(f"Export PDF impossible : {md_path.name}")
    finally:
        tmp.unlink(missing_ok=True)
    print("  PDF ->", pdf.relative_to(ROOT))


for md_file in sorted(LIV.rglob("*.md")):
    if md_file.name == "README.md":
        continue
    md_to_pdf(md_file)
print("Livrables construits.")
