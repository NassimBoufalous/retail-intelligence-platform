"""
03 - Génération des schémas (PNG) des livrables UC-01 et UC-02.

    UC01_preuves/schema_fonctionnel.png      utilisateurs -> questions -> KPI -> domaines -> tables
    UC01_preuves/ventes_par_annee.png        graphique CA par année et canal (preuve de couverture)
    UC02_preuves/architecture_cible.png      architecture logique en couches
    UC02_preuves/schema_flux.png             flux Bronze -> Silver -> Gold
    UC02_preuves/modele_dimensionnel.png     constellation (faits + dimensions conformes)
    UC02_preuves/local_vs_azure.png          correspondance local / Azure

Les schémas sont dessinés avec matplotlib (aucun outil externe requis).
"""
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

ROOT = Path(__file__).resolve().parents[2]
LIV = ROOT / "Jour1"
P1 = LIV / "01_UC01_besoin_exploration" / "UC01_preuves"
P2 = LIV / "02_UC02_architecture" / "UC02_preuves"
P2.mkdir(parents=True, exist_ok=True)

# Palette (référence dataviz validée) ----------------------------------------
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BLUE, ORANGE, AQUA, YELLOW, VIOLET = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#6250d6"
# Teintes très claires pour les fonds de boîtes (texte en encre primaire, lisible)
FILL = {"blue": "#dcebfb", "orange": "#fde4d8", "aqua": "#d5f3e8", "yellow": "#fcefc9",
        "violet": "#e4e0f8", "grey": "#efeee9"}
EDGE = {"blue": BLUE, "orange": ORANGE, "aqua": AQUA, "yellow": YELLOW, "violet": VIOLET, "grey": MUTED}

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "text.color": INK,
                     "figure.facecolor": SURFACE, "axes.facecolor": SURFACE})


def canvas(w, h, title=None):
    fig, ax = plt.subplots(figsize=(w, h))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")
    if title:
        ax.text(0, 99, title, fontsize=13, fontweight="bold", va="top", color=INK)
    return fig, ax


def box(ax, x, y, w, h, text, tone="blue", dashed=False, size=8.5, bold=False):
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.3,rounding_size=1.2",
                       fc=FILL[tone], ec=EDGE[tone], lw=1.4, ls="--" if dashed else "-")
    ax.add_patch(p)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=size,
            color=INK2 if dashed else INK, fontweight="bold" if bold else "normal", wrap=True)
    return (x, y, w, h)


def arrow(ax, a, b, color=MUTED, dashed=False, lw=1.2):
    ax.add_patch(FancyArrowPatch(a, b, arrowstyle="-|>", mutation_scale=10, color=color,
                                 lw=lw, ls="--" if dashed else "-", shrinkA=2, shrinkB=2))


def right(b):
    return (b[0] + b[2], b[1] + b[3] / 2)


def left(b):
    return (b[0], b[1] + b[3] / 2)


def save(fig, path):
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    print("  ->", path.relative_to(ROOT))


# =============================================================================
# 1. Schéma fonctionnel UC-01
# =============================================================================
fig, ax = canvas(16, 10, "UC-01 – Schéma fonctionnel : des utilisateurs aux fichiers observés")
cols_x = [0, 19, 45, 65, 82]
heads = ["Utilisateurs", "Questions décisionnelles", "Indicateurs (KPI)", "Domaines", "Fichiers observés"]
for x, h in zip(cols_x, heads):
    ax.text(x, 93, h, fontsize=10.5, fontweight="bold", color=INK2)

users = ["Direction générale", "Direction commerciale", "Contrôleur de gestion", "Resp. des stocks"]
U = {u: box(ax, 0, 80 - i * 22, 15, 7, u, "violet") for i, u in enumerate(users)}

questions = [  # (id, libellé, profil, dispo?)
    ("Q1", "Q1 Évolution du CA", "Direction générale", True),
    ("Q2", "Q2 Part du CA par canal", "Direction générale", True),
    ("Q3", "Q3 Top produits / catégories", "Direction commerciale", True),
    ("Q4", "Q4 Effet des promotions", "Direction commerciale", True),
    ("Q6", "Q6 CA & tickets par magasin", "Direction générale", True),
    ("Q7", "Q7 Panier moyen mensuel", "Direction commerciale", True),
    ("Q8", "Q8 Retours par catégorie", "Contrôleur de gestion", True),
    ("Q9", "Q9 Marge par catégorie", "Contrôleur de gestion", True),
    ("Q10", "Q10 Stock produit / entrepôt", "Resp. des stocks", True),
    ("Q12", "Q12 Clics web menant à un achat", "Direction commerciale", True),
    ("Q13", "Q13 Notes des avis produits", "Direction commerciale", True),
    ("Q15", "Q15 Fiabilité des chiffres", "Contrôleur de gestion", True),
]
Q = {}
for i, (qid, lab, prof, ok) in enumerate(questions):
    Q[qid] = box(ax, 19, 87 - i * 7.3, 21, 4.6, lab, "blue" if ok else "grey", dashed=not ok, size=8)
    arrow(ax, right(U[prof]), left(Q[qid]), dashed=not ok)

kpis = [
    ("K01", "CA (K01) / Évolution (K06)", ["Q1", "Q6"], True),
    ("K07", "Répartition par canal (K07)", ["Q2"], True),
    ("K04", "Quantités, CA produit (K04)", ["Q3", "Q4"], True),
    ("K02", "Transactions / Panier (K02-K03)", ["Q6", "Q7"], True),
    ("K08", "Retours / Taux retour (K08-K09)", ["Q8"], True),
    ("K11", "Marge nette (K11) – limite", ["Q9"], True),
    ("K10", "Niveau stock / rupture (K10-K13)", ["Q10"], True),
    ("K15", "Clics liés à une vente (K15)", ["Q12"], True),
    ("K14", "Note moyenne avis (K14)", ["Q13"], True),
    ("KQ", "Contrôles qualité (C*, F*)", ["Q15"], True),
]
K = {}
for i, (kid, lab, qs, ok) in enumerate(kpis):
    K[kid] = box(ax, 45, 85 - i * 8.6, 17, 5, lab, "aqua" if ok else "grey", dashed=not ok, size=7.8)
    for q in qs:
        arrow(ax, right(Q[q]), left(K[kid]), dashed=not ok)

domains = [
    ("ventes", "Ventes magasin & web", ["K01", "K07", "K04", "K02", "K11"], True),
    ("retours", "Retours", ["K08"], True),
    ("stocks", "Stocks", ["K10"], True),
    ("ref", "Référentiels\n(produit, magasin,\nclient, date, promo)", ["K01", "K04", "K02"], True),
    ("nav", "Navigation web", ["K15"], True),
    ("avis", "Avis produits", ["K14"], True),
    ("meta", "Métadonnées qualité", ["KQ"], True),
]
D = {}
for i, (did, lab, ks, ok) in enumerate(domains):
    D[did] = box(ax, 65, 84 - i * 12, 13, 7, lab, "yellow" if ok else "grey", dashed=not ok, size=7.8)
    for k in ks:
        arrow(ax, right(K[k]), left(D[did]), dashed=not ok)

files = [
    ("ventes", "store_sales.dat\nweb_sales.dat", True),
    ("retours", "store_returns.dat\nweb_returns.dat", True),
    ("stocks", "inventory.dat\nwarehouse.dat", True),
    ("ref", "item, store, customer,\ndate_dim, promotion…", True),
    ("nav", "web_clickstreams.dat\n(6,8 M clics)", True),
    ("avis", "product_reviews.dat\n(90 000 avis)", True),
    ("meta", "item_marketprices_audit\n+ scripts 01/02", True),
]
for i, (did, lab, ok) in enumerate(files):
    b = box(ax, 82, 84 - i * 12, 17, 7, lab, "orange" if ok else "grey", dashed=not ok, size=7.8)
    arrow(ax, right(D[did]), left(b), dashed=not ok)

ax.text(0, 1, "Toutes les sources sont VÉRIFIÉES dans le kit retail_project/ (data + data_refresh). Limites : pas d'identifiant de session web ; marge et remise non interprétables.",
        fontsize=8.5, color=INK2)
save(fig, P1 / "schema_fonctionnel.png")

# =============================================================================
# 2. Graphique : CA par année et canal (preuve de couverture temporelle)
# =============================================================================
rows = list(csv.DictReader(open(P1 / "ventes_par_annee.csv", encoding="utf-8-sig"), delimiter=";"))
series = {"web": BLUE, "magasin": ORANGE}
fig, ax = plt.subplots(figsize=(8, 4.2))
for canal, color in series.items():
    pts = [(int(r["annee"]), float(r["ca"]) / 1e6) for r in rows if r["canal"] == canal and int(r["annee"]) <= 2004]
    xs, ys = zip(*pts)
    ax.plot(xs, ys, color=color, lw=2, marker="o", ms=5, mec=SURFACE, mew=1.5, label=canal)
    ax.text(xs[-1] + 0.12, ys[-1], f"{canal}  {ys[-1]:,.0f} M".replace(",", " "), va="center", fontsize=8.5, color=INK2)
ax.set_title("Chiffre d'affaires par année et par canal (M, devise non documentée)", loc="left", fontsize=11, color=INK)
ax.set_xlim(2000.7, 2005.3)
ax.set_ylim(0, 400)
ax.set_xticks(range(2001, 2005))
ax.grid(axis="y", color=GRID, lw=0.8)
for s in ["top", "right", "left"]:
    ax.spines[s].set_visible(False)
ax.spines["bottom"].set_color("#c3c2b7")
ax.tick_params(colors=MUTED, length=0)
ax.legend(frameon=False, loc="lower left", ncol=3, fontsize=8.5)
ax.text(2000.7, -64, "Source : kit TPCx-BB retail_project/data (store_sales, web_sales). 2005 exclu : ventes jusqu'au 13/12 seulement.",
        fontsize=7.5, color=MUTED)
save(fig, P1 / "ventes_par_annee.png")

# =============================================================================
# 3. Architecture logique cible
# =============================================================================
fig, ax = canvas(16, 9, "UC-02 – Architecture logique cible : Retail Intelligence Platform")
layers = [
    (0, "SOURCES", [("Kit TPCx-BB\n(.dat « | »,\nsans en-tête)\ndata + data_refresh", "orange", False),
                    ("Données\ncomplémentaires\n(JSON, logs…)\nidentifiées à part", "grey", True)]),
    (17, "INGESTION", [("Apache NiFi\n(repli : script\nPython 01)", "violet", False),
                       ("Contrôles d'entrée\n(nb lignes, schéma,\nchecksum)", "violet", False)]),
    (34, "STOCKAGE BRUT\nBronze / Raw", [("HDFS\n/datalake/bronze\n.dat d'origine\n+ _provenance", "yellow", False)]),
    (51, "PRÉPARATION\nSilver", [("PySpark\ntypage, nulls,\nclés, dédoublonnage", "blue", False),
                                 ("Parquet partitionné\n/datalake/silver", "yellow", False),
                                 ("Quarantaine\n(lignes rejetées)", "grey", False)]),
    (68, "ANALYTIQUE\nGold", [("Modèle en étoile\nfaits + dimensions\n(Parquet)", "yellow", False),
                              ("Data Marts\nVentes · Retours\nStocks · Web", "aqua", False)]),
    (85, "EXPOSITION", [("KPI & tableaux\nde bord", "aqua", False),
                        ("Requêtes SQL\nanalystes", "aqua", False)]),
]
centers = []
for x, title, items in layers:
    ax.text(x + 7, 90, title, ha="center", va="top", fontsize=9.5, fontweight="bold", color=INK2)
    n = len(items)
    ys = []
    for i, (lab, tone, dashed) in enumerate(items):
        y = 72 - i * 20
        b = box(ax, x, y - 7, 14, 14, lab, tone, dashed, size=8)
        ys.append(b)
    centers.append(ys)
for a, b in zip(centers[:-1], centers[1:]):
    arrow(ax, right(a[0]), left(b[0]), color=INK2, lw=1.6)
arrow(ax, right(centers[0][1]), left(centers[1][0]), dashed=True)
# flux internes aux couches
def down(a, b, **kw):
    arrow(ax, (a[0] + a[2] / 2, a[1]), (b[0] + b[2] / 2, b[1] + b[3]), **kw)
down(centers[1][0], centers[1][1], color=INK2)          # NiFi -> contrôles d'entrée
down(centers[3][0], centers[3][1], color=INK2)          # PySpark -> Parquet Silver
arrow(ax, (centers[3][0][0] + 1, centers[3][0][1]), (centers[3][2][0] + 1, centers[3][2][1] + centers[3][2][3]), color=MUTED, dashed=True)  # rejets
down(centers[4][0], centers[4][1], color=INK2)          # étoile -> data marts
arrow(ax, right(centers[4][1]), left(centers[5][1]), color=INK2, lw=1.6)
# bande gouvernance
gov = FancyBboxPatch((0, 2), 99, 9, boxstyle="round,pad=0.3,rounding_size=1.2", fc=FILL["grey"], ec=MUTED, lw=1.2)
ax.add_patch(gov)
ax.text(49.5, 6.5, "SUPERVISION & GOUVERNANCE (transverse) : catalogue / dictionnaire · lignage (source → règle → contrôle) · journal des traitements\n"
        "résultats qualité conservés · permissions HDFS / RBAC-ACL · secrets hors code · rétention par zone · suivi volumes & coûts",
        ha="center", va="center", fontsize=8.5, color=INK)
save(fig, P2 / "architecture_cible.png")

# =============================================================================
# 4. Schéma des flux
# =============================================================================
fig, ax = canvas(16, 10.5, "UC-02 – Schéma des flux de données (batch)")
for x, t, tone in [(0, "SOURCE", "orange"), (20, "BRONZE", "yellow"), (45, "SILVER", "blue"), (72, "GOLD", "aqua")]:
    ax.text(x + 9, 92, t, ha="center", fontsize=10.5, fontweight="bold", color=EDGE[tone])
flux = [
    ("F1 Ventes", "store_sales\nweb_sales", "ventes_magasin\nventes_web\n(typé, part. année/mois)", "fait_ventes\n(grain : 1 ligne de ticket/cde)"),
    ("F2 Retours", "store_returns\nweb_returns", "retours_magasin\nretours_web", "fait_retours\n+ lien vente d'origine"),
    ("F3 Stocks", "inventory\nwarehouse", "stock_hebdo\n(nulls signalés)", "fait_stock_hebdo\n(snapshot, semi-additif)"),
    ("F4 Référentiels", "item, store, customer,\ndate_dim, promotion…", "référentiels typés\n(+ ajouts data_refresh)", "dim_produit · dim_magasin\ndim_client · dim_date…"),
    ("F5 Navigation", "web_clickstreams", "clics typés\n(anonymes signalés)", "fait_clics\n(grain : 1 clic)"),
    ("F6 Avis & prix", "product_reviews\nitem_marketprices", "avis (texte nettoyé)\nprix concurrents", "fait_avis\nfait_prix_concurrents"),
]
for i, (fid, src, sil, gold) in enumerate(flux):
    y = 78 - i * 14.5
    b0 = box(ax, 0, y, 16, 11, f"{fid}\n{src}", "orange", size=8)
    b1 = box(ax, 20, y, 19, 11, ".dat brut inchangé\n/bronze/tpcxbb/<lot>/<table>/\ningest_date=AAAA-MM-JJ", "yellow", size=7.5)
    b2 = box(ax, 45, y, 21, 11, sil, "blue", size=8)
    b3 = box(ax, 72, y, 24, 11, gold, "aqua", size=8)
    arrow(ax, right(b0), left(b1), INK2)
    arrow(ax, right(b1), left(b2), INK2)
    arrow(ax, right(b2), left(b3), INK2)
ax.text(0, 2, "Contrôles : Bronze = nb lignes & schéma vs source · Silver = types, nulls, unicité du grain, orphelins → quarantaine · "
        "Gold = totaux Silver = totaux Gold, KPI de référence", fontsize=8.5, color=INK2)
save(fig, P2 / "schema_flux.png")

# =============================================================================
# 5. Modèle dimensionnel (constellation)
# =============================================================================
fig, ax = canvas(14, 9, "UC-02 – Modèle dimensionnel initial (constellation à dimensions conformes)")
facts = {
    "fv": box(ax, 34, 60, 30, 14, "FAIT_VENTES\ngrain : 1 ligne de ticket (magasin)\nou de commande (web) · clé technique\nquantité, CA, remise, coupon, marge", "orange", size=8, bold=False),
    "fr": box(ax, 4, 32, 26, 12, "FAIT_RETOURS\ngrain : 1 article retourné × ticket/cde\nquantité, montant, motif", "orange", size=8),
    "fs": box(ax, 68, 32, 28, 12, "FAIT_STOCK_HEBDO\ngrain : produit × entrepôt × semaine\nquantité en stock (semi-additive)", "orange", size=8),
    "fc": box(ax, 36, 8, 26, 11, "FAIT_CLICS · FAIT_AVIS\n1 clic web / 1 avis produit\nlien commande web vérifié", "orange", size=8),
}
dims = {
    "date": box(ax, 38, 88, 22, 7, "DIM_DATE (date_dim)", "blue", size=8.5),
    "prod": box(ax, 40, 34, 20, 9, "DIM_PRODUIT (item)\n17 820 + 180 (refresh)\n1 version / produit", "blue", size=8),
    "mag": box(ax, 4, 66, 22, 9, "DIM_MAGASIN (store)\n12 magasins\n1 version / magasin", "blue", size=8),
    "cli": box(ax, 72, 66, 22, 9, "DIM_CLIENT\n(customer + address\n+ demographics)", "blue", size=8),
    "promo": box(ax, 4, 86, 20, 7, "DIM_PROMOTION", "blue", size=8.5),
    "canal": box(ax, 76, 86, 20, 7, "DIM_CANAL\n(magasin / web)", "blue", size=8),
    "ent": box(ax, 74, 10, 20, 7, "DIM_ENTREPOT\n(warehouse)", "blue", size=8),
    "motif": box(ax, 8, 10, 20, 7, "DIM_MOTIF (reason)", "blue", size=8.5),
}
c = lambda b: (b[0] + b[2] / 2, b[1] + b[3] / 2)
links = [("fv", "date"), ("fv", "prod"), ("fv", "mag"), ("fv", "cli"), ("fv", "promo"), ("fv", "canal"),
         ("fr", "date"), ("fr", "prod"), ("fr", "mag"), ("fr", "motif"),
         ("fs", "date"), ("fs", "prod"), ("fs", "ent"),
         ("fc", "prod"), ("fc", "cli")]
for f, d in links:
    ax.plot(*zip(c(facts[f]), c(dims[d])), color=MUTED, lw=0.9, zorder=0)
ax.text(0, 1, "Dimensions conformes (date, produit, magasin) partagées entre faits → comparaisons ventes / retours / stocks possibles. "
        "Clés *_sk vérifiées : 0 orphelin (en incluant data_refresh).", fontsize=8.3, color=INK2)
save(fig, P2 / "modele_dimensionnel.png")

# =============================================================================
# 6. Local vs Azure
# =============================================================================
fig, ax = canvas(14, 7.5, "UC-02 – Déclinaison locale (parcours) et cible Azure (conception, non déployée)")
ax.text(18, 90, "ENVIRONNEMENT LOCAL (Ubuntu)", ha="center", fontsize=10, fontweight="bold", color=INK2)
ax.text(80, 90, "CIBLE AZURE (Jour 4)", ha="center", fontsize=10, fontweight="bold", color=INK2)
pairs = [
    ("Apache NiFi (ingestion)", "Azure Data Factory (pipelines, triggers)"),
    ("HDFS /datalake/{bronze,silver,gold}", "ADLS Gen2 (HNS activé) : conteneurs bronze / silver / gold"),
    ("Spark / PySpark local (YARN)", "Synapse Spark Pools"),
    ("Parquet (+ Delta Lake facultatif)", "Parquet / Delta sur ADLS Gen2"),
    ("SQL (DuckDB / Spark SQL)", "Synapse SQL serverless sur Gold"),
    ("Permissions HDFS (user/group/ACL)", "Entra ID + RBAC + ACL POSIX ADLS"),
    ("Fichier de config hors Git", "Azure Key Vault + identités managées"),
]
for i, (l, r) in enumerate(pairs):
    y = 76 - i * 11
    bl = box(ax, 2, y, 32, 7, l, "yellow", size=8.5)
    br = box(ax, 56, y, 42, 7, r, "blue", size=8.5)
    arrow(ax, right(bl), left(br), INK2)
save(fig, P2 / "local_vs_azure.png")
print("Schémas générés.")
