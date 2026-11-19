# Preuves d'exécution – Jour 3 (UC-06)

Toutes les preuves ont été produites par les scripts de `Jour3/transformations/` lors de l'exécution complète du
pipeline (18/11/2026, Ubuntu 24.04 sous WSL 2, Spark 4.3.0, HDFS `/retail-lake`). Aucune n'a été écrite à la main.
Fichiers CSV : séparateur `;`, UTF-8 (ouvrables dans Excel).

## `environnement/` – versions et test de Spark (livrable 1)
| Fichier | Ce qu'il prouve |
|---|---|
| `fiche_environnement.json` | versions réellement utilisées : Python 3.12.3, PySpark / Spark 4.3.0, Java 17, client et serveur Hadoop 3.5.0, mode `local[*]`, chemins des zones |
| `test_spark.txt` | le programme de test du TP fonctionne : session Spark créée, fichier lu dans HDFS (schéma et 6 632 lignes), Parquet écrit puis relu |

## `bronze/` – copie Raw → Bronze
| Fichier | Ce qu'il prouve |
|---|---|
| `manifeste_bronze.csv` | les 13 fichiers copiés sont identiques à Raw (somme de contrôle HDFS) et ont le bon nombre de champs sur chaque ligne |

## `exploration/` – inventaire et premiers contrôles (livrables 2 et 3)
| Fichier | Ce qu'il prouve |
|---|---|
| `inventaire_sources.csv` | pour les 8 sources : emplacement, format, taille, lignes (par lot), colonnes, clés candidates, doublons exacts, granularité |
| `profil_colonnes.csv` | pour les 183 colonnes : type proposé par `inferSchema`, type retenu, valeurs nulles (nombre et %), distinctes, min, max |
| `grain_faits.csv` | la clé (ticket, article) n'est pas unique : il faut une clé technique en Silver |
| `schemas_infer.txt` | les schémas « devinés » par Spark, comparés au schéma attendu |

## `silver/` – production Silver (livrable 6)
| Fichier | Ce qu'il prouve |
|---|---|
| `bilan_silver.csv` | pour chaque table : lignes Bronze = lignes Silver + quarantaine, lignes relues, fichiers et taille Parquet, partitionnement |
| `schemas_silver.csv` | schéma relu de chaque table Silver (types explicites) |
| `apercus_silver.txt` | 5 premières lignes de chaque table Silver (`show()`) |

## `gold/` – production Gold et agrégations métier (livrable 7)
| Fichier | Ce qu'il prouve |
|---|---|
| `bilan_gold.csv` | les 6 jeux Gold produits : lignes, colonnes, fichiers, taille, question métier |
| `jointures.csv` | chaque jointure vérifiée : clé de droite unique, lignes avant = lignes après, lignes non rapprochées |
| `schemas_gold.csv`, `apercus_gold.txt` | schémas et premières lignes des jeux Gold |
| `extraits/*.csv` | le contenu complet des 5 agrégats (CA par mois et canal, par catégorie, par magasin, taux et motifs de retour) |

## `parquet/` – écriture, relecture, comparaison des formats (livrable 8)
| Fichier | Ce qu'il prouve |
|---|---|
| `relecture_parquet.txt` | un jeu Gold relu depuis Parquet : nombre de lignes et schéma conservés |
| `comparaison_formats.csv` | mêmes ventes en CSV, JSON et Parquet : taille, temps de lecture, types relus |
| `plans_lecture.txt` | plans d'exécution Spark : Parquet ne lit qu'une colonne et une seule partition, le CSV lit tout |

## `qualite/` – contrôles qualité (livrable 9)
| Fichier | Ce qu'il prouve |
|---|---|
| `controles_qualite.csv` | les 85 contrôles : dimension, règle, méthode, résultat, statut, traitement décidé |
| `reconciliation_jour1.csv` | 45 KPI recalculés par Spark comparés à ceux du Jour 1 (DuckDB) |
| `resume_qualite.json` | bilan : 70 OK, 3 anomalies, 12 mesures informatives, et la liste des anomalies |

## `tests/` – tests des règles de nettoyage
| Fichier | Ce qu'il prouve |
|---|---|
| `resultats_tests_regles.txt` | 22/22 tests réussis : chaque règle détecte bien l'anomalie injectée volontairement |

## `hdfs/` – contenu du Data Lake
| Fichier | Ce qu'il prouve |
|---|---|
| `zones_volumes.txt` | volume de chaque zone (raw, bronze, silver, gold, quality) et de chaque table |
| `listing_silver_gold.txt`, `partitions_store_sales.txt` | les fichiers Parquet réellement écrits dans HDFS, et le partitionnement par année |
| `fsck_retail_lake.txt` | santé de HDFS : 0 bloc corrompu, manquant ou sous-répliqué |
| `correction_replication_D5.txt` | difficulté D5 : constat (réplication 3) puis correction (réplication 1) |
| `explorateur_*.png` | captures de l'explorateur web HDFS montrant les zones Silver et Gold |

## `journaux/` – journaux d'exécution
| Fichier | Ce qu'il prouve |
|---|---|
| `<étape>.log` | journal horodaté de chaque étape (volumes, jointures, contrôles, écritures) |
| `pipeline_complet.log` | exécution complète du pipeline, étape par étape, avec leur durée |
| `notebook_exploration.log` | exécution du notebook d'exploration |

## `schemas/`
| Fichier | Ce qu'il prouve |
|---|---|
| `chaine_transformation.png` | schéma de la chaîne de traitement Raw → Bronze → Silver → Gold et des contrôles |
