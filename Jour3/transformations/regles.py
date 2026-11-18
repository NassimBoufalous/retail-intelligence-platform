"""
Règles de nettoyage de la zone Silver, isolées du reste du pipeline pour pouvoir être TESTÉES
(tests/test_regles.py) sur des lignes volontairement fausses.

Chaque règle est un couple (motif, condition_de_validité). Une ligne qui ne respecte pas une règle
n'est jamais supprimée : elle part en quarantaine avec le motif de la première règle violée.
Le catalogue complet (anomalie, règle, action, justification) est dans UC06_regles_nettoyage.
"""
from functools import reduce

from pyspark.sql import functions as F

TOLERANCE_MONTANT = 0.01   # R7 : écart d'arrondi accepté entre montant de ligne et prix x quantité


def tous_non_nuls(cols):
    return reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in cols])


def cle_technique(df, cols, canal):
    """R4 : empreinte SHA-256 du contenu de la ligne (colonnes sources), préfixée du canal.
    Déterministe : la même ligne source donne la même clé à chaque exécution (rejouable, comparable),
    contrairement à un numéro attribué au hasard (monotonically_increasing_id)."""
    parts = [F.lit(canal)] + [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]
    return F.sha2(F.concat_ws("|", *parts), 256)


def separer(df, regles):
    """Applique les règles dans l'ordre. Renvoie (valides, quarantaine) ; la quarantaine porte une colonne
    motif_quarantaine. Une condition indéterminée (null) est considérée comme NON respectée."""
    motif = F.lit(None).cast("string")
    for nom, valide in reversed(regles):
        motif = F.when(~F.coalesce(valide, F.lit(False)), F.lit(nom)).otherwise(motif)
    df = df.withColumn("motif_quarantaine", motif)
    return (df.filter(F.col("motif_quarantaine").isNull()).drop("motif_quarantaine"),
            df.filter(F.col("motif_quarantaine").isNotNull()))


def r1():
    """R1 : la colonne _conv_ok est produite par commun.typer()."""
    return ("R1_type_non_convertible", F.col("_conv_ok"))


def regles_referentiel(cle):
    """R1 + R12 (clé non nulle et unique). Suppose une colonne _dup (True si la clé apparaît plusieurs fois)."""
    return [r1(),
            ("R12_cle_nulle", F.col(cle).isNotNull()),
            ("R12_cle_dupliquee", F.col("_dup").isNull())]


def regles_ventes(p, doc):
    """p : préfixe (ss / ws) ; doc : ticket_number / order_number.
    Suppose les colonnes date_vente (jointure date_dim) et _item_ok (jointure item)."""
    ecart = F.abs(F.col(f"{p}_ext_sales_price") - F.col(f"{p}_sales_price") * F.col(f"{p}_quantity"))
    return [
        r1(),
        ("R5_champ_obligatoire_nul", tous_non_nuls(
            [f"{p}_sold_date_sk", f"{p}_item_sk", f"{p}_{doc}", f"{p}_quantity", f"{p}_ext_sales_price"])),
        ("R6_quantite_non_positive", F.col(f"{p}_quantity") > 0),
        ("R7_montant_incoherent", ecart <= TOLERANCE_MONTANT),
        ("R8_date_inconnue", F.col("date_vente").isNotNull()),
        ("R9_produit_inconnu", F.col("_item_ok").isNotNull()),
    ]


def regles_retours(p, doc):
    """p : préfixe (sr / wr). Suppose date_retour (jointure date_dim) et _item_ok (jointure item)."""
    return [
        r1(),
        ("R5_champ_obligatoire_nul", tous_non_nuls(
            [f"{p}_returned_date_sk", f"{p}_item_sk", f"{p}_{doc}", f"{p}_return_quantity", f"{p}_return_amt"])),
        ("R6_quantite_non_positive", F.col(f"{p}_return_quantity") > 0),
        ("R8_date_inconnue", F.col("date_retour").isNotNull()),
        ("R9_produit_inconnu", F.col("_item_ok").isNotNull()),
    ]
