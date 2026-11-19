# Jour 4 – UC-08 : analyse et visualisation ; UC-07 : transposition sur Azure (ADLS Gen2)

Retail Intelligence Platform – IPSSI, Mastère Dev, Data & IA – TP du 19/11/2026 (partie obligatoire).

- **UC-08** : KPI, statistiques descriptives, 4 analyses métier et 5 graphiques, calculés avec **PySpark** sur les
  données **complètes** Silver / Gold du Data Lake HDFS du Jour 3 (Pandas et Matplotlib ne reçoivent que des
  résultats agrégés).
- **UC-07** : **parcours B (local / open source)**. L'architecture cible Azure est proposée, pas déployée (aucune
  ressource Azure créée : connexion réussie mais aucun abonnement, offre gratuite déjà utilisée). L'organisation ADLS Gen2 et la matrice des droits ont été **réellement reproduites dans HDFS**
  (ACL) et testées.

## Où trouver chaque livrable

| Code | Livrable | Où |
|---|---|---|
| J4-01 | Inventaire analytique | [`UC08_analyse_donnees.pdf`](UC08_analyse_donnees.pdf) §2 ; [`analytics/outputs/inventaire_jeux.csv`](analytics/outputs/inventaire_jeux.csv) |
| J4-02 | Dictionnaire des KPI | PDF UC-08 §3 ; [`analytics/outputs/kpi.csv`](analytics/outputs/kpi.csv) |
| J4-03 | Statistiques descriptives | PDF UC-08 §4 ; [`analytics/outputs/statistiques.csv`](analytics/outputs/statistiques.csv) |
| J4-04 | Analyses métier | PDF UC-08 §5 ; `analytics/outputs/analyse1_*.csv` à `analyse4_*.csv` |
| J4-05 | Visualisations | [`analytics/visualizations/`](analytics/visualizations/) (5 PNG) |
| J4-06 | Synthèse décisionnelle | PDF UC-08 §7 |
| J4-07 | Architecture Azure | [`UC07_architecture_azure.pdf`](UC07_architecture_azure.pdf) §1 ; [`azure/architecture/`](azure/architecture/) |
| J4-08 | Organisation ADLS Gen2 et HNS | PDF UC-07 §2 |
| J4-09 | Sécurité (matrice des droits) | PDF UC-07 §3 ; [`azure/evidence/tests_droits.csv`](azure/evidence/tests_droits.csv) |
| J4-10 | Preuves et simulation | PDF UC-07 §4 ; [`azure/evidence/`](azure/evidence/), [`azure/simulation/`](azure/simulation/), [`azure/configuration/`](azure/configuration/) |
| J4-11 | README de reproduction | [`README_Jour4.pdf`](README_Jour4.pdf) |
| – | Notebook d'analyse (exécuté) | [`analytics/notebooks/TP4_Retail_Intelligence.ipynb`](analytics/notebooks/TP4_Retail_Intelligence.ipynb) |
| – | Questions de recul (TP) et de vérification (cours 4) | [`J4_questions_de_recul.pdf`](J4_questions_de_recul.pdf) |
| – | Captures d'écran | [`captures/`](captures/) |

## Contenu des dossiers

```
Jour4/
├── analytics/
│   ├── notebooks/        TP4_Retail_Intelligence.ipynb (exécuté, sorties incluses)
│   ├── outputs/          tableaux CSV produits par le notebook (KPI, statistiques, analyses, contrôles)
│   └── visualizations/   5 graphiques PNG
├── azure/
│   ├── architecture/     schémas : cible Azure (proposée) et équivalent local (réalisé)
│   ├── configuration/    script Azure CLI de création ADLS Gen2 (simulation, NON exécuté)
│   ├── simulation/       scripts exécutés : zones + ACL dans HDFS, requête Spark SQL « analyste »
│   └── evidence/         preuves : arborescence, ACL, 16 tests de droits, résultat Spark SQL
├── captures/             10 captures d'écran : notebook exécuté, interface web HDFS
└── requirements_jour4.txt
```

## Reproduire

```bash
# dans Ubuntu (WSL), HDFS démarré et Gold du Jour 3 présent (voir README_Jour4.pdf)
cd ~/retail-intelligence-platform/Jour4/analytics/notebooks
~/venv-tp3/bin/jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.kernel_name=retail-tp4 TP4_Retail_Intelligence.ipynb
cd ~/retail-intelligence-platform
bash Jour4/azure/simulation/01_demo_zones_acl_hdfs.sh
HADOOP_USER_NAME=analyste ~/venv-tp3/bin/python Jour4/azure/simulation/02_requete_spark_sql_analyste.py
```

## Résultats clés

- **KPI 2001-2005** : CA HT de 3 012 M (web 57,2 %), 90 000 transactions par canal, 7,5 lignes par transaction,
  panier moyen de 14 334 (magasin) et 19 131 (web), taux de retour de 2,85 % et 2,91 %. **45 KPI sur 45 identiques**
  à ceux du Jour 1.
- **Constats** : CA stable, sans saisonnalité ; écart de 5 % seulement entre les 12 magasins ; Music + Books =
  72 % du CA, à cause de la largeur de l'assortiment ; retours homogènes (Toys & Games et Tools un peu plus élevés sur le web).
- **Sécurité et HNS** : matrice des droits appliquée en ACL HDFS, **16 tests de droits conformes sur 16** ; dépôt vérifié par MD5 ; publication par renommage atomique d'un répertoire.
- Les données TPCx-BB sont **synthétiques** : les constats valident la démarche, pas une stratégie commerciale.
