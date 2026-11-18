#!/usr/bin/env bash
# Lance une ou plusieurs étapes du pipeline UC-06 dans l'environnement validé (WSL Ubuntu 24.04).
#
#   bash Jour3/transformations/lancer.sh               # pipeline complet, dans l'ordre
#   bash Jour3/transformations/lancer.sh 03_silver.py  # une seule étape
#
# Prérequis : HDFS démarré (Jour2/scripts/03_demarrer_hdfs.sh) et environnement ~/venv-tp3 (README_Jour3).
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64     # Java 17 : compatible Spark 4.x et Hadoop 3.5
export PATH="$JAVA_HOME/bin:/opt/hadoop/bin:$PATH"
# Configuration du cluster HDFS du Jour 2 : sans elle, Spark écrit avec la réplication par défaut (3) alors que
# le cluster n'a qu'un DataNode (réplication 1) -> blocs « sous-répliqués » (difficulté D5 du README_Jour3).
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PYSPARK_PYTHON="$HOME/venv-tp3/bin/python"
PY="$HOME/venv-tp3/bin/python"
cd "$(dirname "$0")"

ETAPES=("$@")
if [ ${#ETAPES[@]} -eq 0 ]; then
  ETAPES=(00_test_environnement.py 01_bronze.py 02_inventaire.py 03_silver.py 04_gold.py
          05_qualite.py 06_formats.py 07_preuves_hdfs.sh)
fi

for e in "${ETAPES[@]}"; do
  echo "==================== $e ===================="
  debut=$(date +%s)
  case "$e" in
    *.sh) bash "$e" ;;
    *)    "$PY" "$e" ;;
  esac
  echo "---- $e terminé en $(( $(date +%s) - debut )) s"
done
