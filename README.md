# Retail Intelligence Platform

Projet fil rouge du module Data Warehouse / Data Lake (IPSSI, Mastère Dev, Data & IA) : conception et
réalisation d'une plateforme Data Lake / Lakehouse pour l'analyse de données retail (kit TPCx-BB).

## Sommaire

| Jour | Use cases | Contenu | État |
|---|---|---|---|
| [**Jour 1**](Jour1/) | UC-01, UC-02 | Besoin métier, exploration des sources, choix d'architecture | Livré |
| [**Jour 2**](Jour2/) | UC-03, UC-04 | Hadoop / HDFS (Data Lake en zones), ingestion et orchestration avec NiFi (partie obligatoire) | Livré |
| [**Jour 3**](Jour3/) | UC-06 | Transformations PySpark Bronze / Silver / Gold en Parquet, contrôles qualité (partie obligatoire) | Livré |
| [**Jour 4**](Jour4/) | UC-08, UC-07 | KPI, statistiques, analyses métier et visualisations (PySpark, Matplotlib) ; transposition Azure / ADLS Gen2 (parcours local, zones et ACL démontrées sur HDFS) | Livré |

Chaque dossier `JourN/` contient les livrables du jour, leurs preuves et les scripts utilisés, ainsi qu'un
`README.md` qui en détaille le contenu.

## Organisation du dépôt

```
.
├── README.md                     ce fichier
├── requirements.txt              bibliothèques Python du projet
├── Jour1/
│   ├── README.md                 détail des livrables et résultats clés du Jour 1
│   ├── 01_UC01_besoin_exploration/   UC-01 : 6 livrables (PDF, Excel) + UC01_preuves/
│   ├── 02_UC02_architecture/         UC-02 : 6 livrables (PDF) + UC02_preuves/
│   └── scripts/                  code Python du Jour 1
├── Jour2/
│   ├── UC03_Hadoop_HDFS.pdf      UC-03 : livrables 1 à 6
│   ├── UC03_preuves/             environnement/, hdfs/, data_lake/
│   ├── UC04_ingestion_NiFi.pdf   UC-04 : livrables 7 à 11
│   ├── UC04_preuves/             configuration/, execution/, erreurs/, provenance/
│   ├── README_Jour2.pdf          livrable 13 : reproduction (versions, commandes, chemins)
│   └── J2_questions_de_recul.pdf
├── Jour3/
│   ├── UC06_transformation_lakehouse.pdf   UC-06 : livrables 1 à 8
│   ├── UC06_rapport_qualite.pdf            livrable 9 : 85 contrôles qualité
│   ├── README_Jour3.pdf                    livrable 10 : reproduction
│   ├── J3_questions_de_recul.pdf
│   ├── notebooks/                notebook d'exploration PySpark (exécuté)
│   ├── transformations/          scripts PySpark du pipeline Raw -> Bronze -> Silver -> Gold
│   ├── tests/                    tests des règles de nettoyage
│   ├── README.md                 sommaire du Jour 3
│   ├── data/                     fichiers Parquet Gold complets et échantillons Silver
│   ├── requirements_jour3.txt    bibliothèques Python de l'environnement Spark (WSL)
│   └── UC06_preuves/             preuves d'exécution (README.md : explication de chaque preuve)
└── Jour4/
    ├── UC08_analyse_donnees.pdf            UC-08 : livrables J4-01 à J4-06
    ├── UC07_architecture_azure.pdf         UC-07 : livrables J4-07 à J4-10
    ├── README_Jour4.pdf                    livrable J4-11 : reproduction et inventaire
    ├── J4_questions_de_recul.pdf
    ├── README.md                 sommaire du Jour 4
    ├── analytics/                notebook TP4 (exécuté), tableaux CSV, graphiques PNG
    ├── azure/                    schémas, script Azure CLI (simulation), scripts et preuves de la démo HDFS
    └── requirements_jour4.txt    bibliothèques Python de l'environnement (WSL)
```

Chaque document n'est présent qu'en un seul format, le PDF en priorité. Les données du kit
(`retail_project/`, environ 1 Go) ne sont pas versionnées : elles sont fournies par le formateur.

## Installation

```bash
python -m venv .venv
./.venv/Scripts/pip install -r requirements.txt     # Linux/macOS : .venv/bin/pip
```

Les commandes de chaque jour sont décrites dans le `README.md` du dossier correspondant.
