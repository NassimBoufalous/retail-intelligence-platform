# Jour 3 – UC-06 : traitement et transformation des données avec Python / PySpark

Retail Intelligence Platform – IPSSI, Mastère Dev, Data & IA – TP du 18/11/2026 (partie obligatoire).

Pipeline PySpark **Raw → Bronze → Silver → Gold** sur les données TPCx-BB du Data Lake HDFS construit au Jour 2,
avec stockage en **Parquet** et **85 contrôles qualité**. Tout a été réellement exécuté (Ubuntu 24.04 sous WSL 2,
Spark 4.3.0 en mode `local[*]`, Hadoop 3.5.0).

## Où trouver chaque livrable

| Livrable attendu | Où |
|---|---|
| 1. Fiche d'environnement technique | [`UC06_transformation_lakehouse.pdf`](UC06_transformation_lakehouse.pdf) §1 ; [`UC06_preuves/environnement/`](UC06_preuves/environnement/) |
| 2. Inventaire technique des sources (8 sources) | PDF §2 ; [`UC06_preuves/exploration/inventaire_sources.csv`](UC06_preuves/exploration/inventaire_sources.csv) |
| 3. Notebook d'exploration (exécuté) | [`notebooks/01_exploration_bronze.ipynb`](notebooks/01_exploration_bronze.ipynb) ; synthèse PDF §3 |
| 4. Règles de nettoyage (13 règles) | PDF §4 ; code [`transformations/regles.py`](transformations/regles.py) ; tests [`tests/`](tests/) |
| 5. Scripts de transformation | [`transformations/`](transformations/) (étapes 00 à 08, lancées par `lancer.sh`) |
| 6. Jeux Silver (8 tables) | PDF §6 ; échantillons Parquet [`data/silver/`](data/silver/) ; tables complètes dans HDFS `/retail-lake/silver/` |
| 7. Jeux Gold (6 jeux, questions Q1, Q2, Q3, Q6, Q7, Q8 du Jour 1) | PDF §7 ; Parquet [`data/gold/`](data/gold/) |
| 8. Résultat Parquet relu + comparaison CSV / JSON / Parquet | PDF §8 ; [`UC06_preuves/parquet/`](UC06_preuves/parquet/) |
| 9. Rapport de contrôles qualité | [`UC06_rapport_qualite.pdf`](UC06_rapport_qualite.pdf) |
| 10. README de reproduction, preuves | [`README_Jour3.pdf`](README_Jour3.pdf) (procédure complète) ; [`UC06_preuves/`](UC06_preuves/) (chaque preuve expliquée dans [`UC06_preuves/README.md`](UC06_preuves/README.md)) |
| Questions de recul | [`J3_questions_de_recul.pdf`](J3_questions_de_recul.pdf) |

## Données (`data/`)

Les données complètes sont dans HDFS (`/retail-lake/silver`, `/retail-lake/gold`, environ 315 Mo) et ne sont pas
versionnées. Le dossier `data/` contient de **vrais fichiers Parquet produits par le pipeline**, exportés par
`transformations/08_export_donnees.py` et lisibles sans HDFS (pandas, DuckDB, Spark) :

- `data/gold/` : les 5 agrégats Gold **complets** + un échantillon de 1 000 lignes de `fait_ventes` ;
- `data/silver/` : échantillons de 1 000 lignes des tables de faits et de `item`, tables `store` et `reason` complètes.

Le détail (lignes exportées / lignes dans HDFS, colonnes, taille) est dans [`data/contenu_data.csv`](data/contenu_data.csv).
Le format Parquet est celui de toutes les zones Silver et Gold : il n'y a donc pas de dossier `data/parquet/` séparé.

```python
import pandas as pd
pd.read_parquet("Jour3/data/gold/ventes_mensuelles_canal.parquet").head()
```

## Reproduire

```bash
# dans Ubuntu (WSL), HDFS démarré : voir README_Jour3.pdf pour l'installation
cd ~/retail-intelligence-platform
bash Jour3/transformations/lancer.sh     # pipeline complet (environ 10 minutes)
bash Jour3/tests/lancer_tests.sh         # 22 tests des règles de nettoyage
```

## Résultats clés

- **Silver** : 1 553 977 lignes, 0 ligne perdue (Bronze = Silver + quarantaine pour chaque table).
- **Gold** : CA identique au centime à Silver ; 45 KPI identiques à ceux du Jour 1 (calculés avec DuckDB), aux
  arrondis du Jour 1 près.
- **Qualité** : 85 contrôles, 70 OK, 12 mesures informatives, 3 anomalies documentées (retours sans vente et retours
  excédentaires du lot de rafraîchissement).
- **Parquet** : environ 2,5 fois plus petit que le CSV à contenu égal ; seul format qui conserve les types
  (`decimal(7,2)`, `date`).
