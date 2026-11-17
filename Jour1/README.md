# J1 – Retail Intelligence Platform : UC-01 et UC-02

TP Jour 1, IPSSI, Mastère Dev, Data & IA. Besoin métier, exploration des sources et choix d'architecture.

> **Données utilisées** : kit TPCx-BB fourni par le formateur (`retail_project.zip`, extrait dans
> `retail_project/`), avec le lot initial `data/` et le lot de rafraîchissement `data_refresh/`.
> Les fichiers `.dat` n'ont pas d'en-tête : les noms de colonnes viennent du schéma de référence
> TPCx-BB (`scripts/schema_tpcxbb.py`, statut « Déduit », structure vérifiée).

## Contenu

| Dossier | Livrable | Fichier |
|---|---|---|
| `01_UC01_besoin_exploration/` | 1. Fiche de cadrage métier | `UC01_cadrage_metier.pdf` |
| | 2. Matrice utilisateurs / questions décisionnelles | `UC01_questions_decisionnelles.pdf` |
| | 3. Dictionnaire initial des KPI | `UC01_dictionnaire_KPI.pdf` |
| | 4. Inventaire réel des sources | `UC01_inventaire_sources.xlsx` (onglets Tables, Colonnes, Relations, Contrôles, KPI, Ventes, Retours, Avis) |
| | 5. Qualification des données et relations | `UC01_qualification_donnees.pdf` |
| | 6. Limites et hypothèses | `UC01_limites_hypotheses.pdf` |
| | Preuves | `UC01_preuves/` : arborescence, aperçus des fichiers, CSV de profilage et de contrôles, schéma fonctionnel, graphique |
| `02_UC02_architecture/` | 7. Comparaison DW / Lake / Lakehouse | `UC02_comparaison_architectures.pdf` |
| | 8. Architecture logique cible (+ modèle dimensionnel) | `UC02_architecture_cible.pdf` |
| | 9. Schéma des flux | `UC02_schema_flux.pdf` |
| | 10. Positionnement des composants | `UC02_positionnement_composants.pdf` |
| | 11. Gouvernance | `UC02_gouvernance.pdf` |
| | 12. Local vers Azure | `UC02_local_vers_azure.pdf` |
| | Preuves | `UC02_preuves/` : schémas PNG |
| `scripts/` | Code Python : inventaire, contrôles, schémas, génération des documents | `schema_tpcxbb.py`, `01_` à `04_*.py` |

Chaque document est fourni en un seul format, le PDF en priorité. Seul l'inventaire des sources est un classeur Excel, car il n'existe que sous ce format. Les PDF sont générés par `scripts/04_construire_livrables.py`.

## Reproduire

Prérequis : Python 3.13 avec les bibliothèques de `requirements.txt` (à la racine du dépôt), Microsoft Edge pour
l'export PDF. Le kit doit être extrait dans `retail_project/`, à la racine du dépôt. Les commandes se lancent depuis
cette racine.

```bash
# 1. inventaire et profilage des 36 fichiers (data + data_refresh)
./.venv/Scripts/python Jour1/scripts/01_inventaire_sources.py retail_project
# 2. relations, contrôles qualité, faisabilité des KPI
./.venv/Scripts/python Jour1/scripts/02_relations_et_controles.py retail_project
# 3. schémas et graphique
./.venv/Scripts/python Jour1/scripts/03_diagrammes.py
# 4. XLSX + PDF
./.venv/Scripts/python Jour1/scripts/04_construire_livrables.py
```

(`scripts/00_generer_donnees_substitut.py` a servi avant la réception du kit ; il n'est plus utilisé.)

## Résultats clés

- 34 fichiers `.dat` (23 tables en `data`, 11 en `data_refresh`) + 2 fichiers d'audit ; 34,2 M lignes ; 986 Mo.
  Nombre de champs constant sur chaque ligne et conforme au schéma.
- Ventes du 01/01/2001 au 13/12/2005 (lot initial), complétées jusqu'au 31/12/2005 par `data_refresh`.
- 52 relations testées : 51 vérifiées (0 orphelin). Exception : `inventory` référence 180 produits
  livrés seulement dans `data_refresh/item`, ce qui impose l'ordre de chargement.
- **Pas de clé naturelle** sur les lignes de vente : (ticket, article) en double dans 4 038 cas, d'où une clé technique en Silver.
- Tableau « Tables principales » du formateur : clés de 17 tables confirmées ; `ws_sk` et `wcs_click_sk` absents des fichiers.
- Spécification TPCx-BB v1.7.0 (annexe B) : kit en SF = 1 ; 17 contrôles de conformité : 10 conformes, 3 cohérents (informatifs), 4 écarts documentés ; en cas de conflit, la spécification indique que le kit prévaut.
- Liens TPCx-BB vérifiés : clic → commande web (R6) et avis → commande web (R7), à 100 %.
- Qualité technique bonne (aucune vente sans date, montant ou magasin ; formules de montants vérifiées) ;
  anomalies : `c_last_review_date` invalide, entités HTML dans 16,5 % des avis, 42 % de clics anonymes.
- Valeurs synthétiques non interprétables : remise d'environ 50 %, marge de -48 %, stock moyen d'environ 500, CA plat.
- `data_refresh` = **ajouts seuls** (nouveaux clients, produits, tickets ; 0 recouvrement), mais 407 retours magasin sur 426 et 371 retours web sur 400 sans vente correspondante.
- Architecture retenue : Data Lake Bronze / Silver / Gold (HDFS, NiFi, PySpark, Parquet) avec un Gold
  dimensionnel ; évolution possible vers Delta Lake (facultatif) ; cible ADLS Gen2 conçue, non déployée.

## Réalisé ou proposé

| Réalisé au J1 | Proposé (non déployé) |
|---|---|
| Inventaire, profilage, contrôles et calcul test des KPI sur le kit officiel ; mesure Parquet ; schémas ; documents | HDFS, NiFi, Spark, Parquet en production, Delta Lake, Azure (ADLS Gen2, ADF, Synapse) |
