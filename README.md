# Retail Intelligence Platform

Projet fil rouge du module Data Warehouse / Data Lake (IPSSI, Mastère Dev, Data & IA) : conception et
réalisation d'une plateforme Data Lake / Lakehouse pour l'analyse de données retail (kit TPCx-BB).

## Sommaire

| Jour | Use cases | Contenu | État |
|---|---|---|---|
| [**Jour 1**](Jour1/) | UC-01, UC-02 | Besoin métier, exploration des sources, choix d'architecture | Livré |
| Jour 2 | UC-03, UC-04 | Hadoop / HDFS, ingestion avec NiFi | À venir |
| Jour 3 | UC-05 (facultatif), UC-06 | NoSQL / HBase, transformations Silver / Gold | À venir |
| Jour 4 | UC-08, UC-07 | Analyses, indicateurs, déclinaison Azure | À venir |

Chaque dossier `JourN/` contient les livrables du jour, leurs preuves et les scripts utilisés, ainsi qu'un
`README.md` qui en détaille le contenu.

## Organisation du dépôt

```
.
├── README.md                     ce fichier
├── requirements.txt              bibliothèques Python du projet
└── Jour1/
    ├── README.md                 détail des livrables et résultats clés du Jour 1
    ├── 01_UC01_besoin_exploration/   UC-01 : 6 livrables (PDF, Excel) + UC01_preuves/
    ├── 02_UC02_architecture/         UC-02 : 6 livrables (PDF) + UC02_preuves/
    └── scripts/                  code Python du Jour 1
```

Chaque document n'est présent qu'en un seul format, le PDF en priorité. Les données du kit
(`retail_project/`, environ 1 Go) ne sont pas versionnées : elles sont fournies par le formateur.

## Installation

```bash
python -m venv .venv
./.venv/Scripts/pip install -r requirements.txt     # Linux/macOS : .venv/bin/pip
```

Les commandes de chaque jour sont décrites dans le `README.md` du dossier correspondant.
