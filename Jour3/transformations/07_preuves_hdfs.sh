#!/usr/bin/env bash
# Preuves du contenu des zones du Data Lake après exécution du pipeline (listing, volumes, fichiers Parquet).
set -euo pipefail
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="/opt/hadoop/bin:$PATH"
OUT="$(dirname "$0")/../UC06_preuves/hdfs"
mkdir -p "$OUT"
L=/retail-lake
{
  echo "# Volumes par zone ($(date '+%Y-%m-%d %H:%M:%S'))"
  hdfs dfs -du -s -h $L/raw $L/bronze $L/silver $L/gold $L/quality $L/logs
  echo; echo "# Bronze : fichiers copiés depuis Raw"
  hdfs dfs -ls -R $L/bronze | grep -v '^d'
  echo; echo "# Silver : tables Parquet (volume par table)"
  hdfs dfs -du -h $L/silver
  echo; echo "# Gold : jeux Parquet (volume par jeu)"
  hdfs dfs -du -h $L/gold
  echo; echo "# Quality : quarantaine et résultats des contrôles"
  hdfs dfs -du -h $L/quality/quarantaine $L/quality/controles
} > "$OUT/zones_volumes.txt" 2>&1
hdfs dfs -ls -R $L/silver $L/gold > "$OUT/listing_silver_gold.txt" 2>&1
hdfs dfs -ls -R $L/silver/store_sales | head -20 > "$OUT/partitions_store_sales.txt" 2>&1
# Santé du système de fichiers : blocs sous-répliqués, corrompus, manquants (difficulté D5)
hdfs fsck $L 2>/dev/null | grep -E "Status|Total size|Total files|Total blocks|replicated|Corrupt|Missing|Default replication" \
  > "$OUT/fsck_retail_lake.txt"
echo "Preuves HDFS écrites dans $OUT"
