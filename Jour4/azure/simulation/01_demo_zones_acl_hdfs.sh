#!/usr/bin/env bash
# UC-07, parcours B : reproduction locale (HDFS) de l'organisation ADLS Gen2 proposée et de la matrice des droits.
#
#   bash Jour4/azure/simulation/01_demo_zones_acl_hdfs.sh
#
# Ce que fait le script (réellement exécuté, sur le HDFS du Jour 2) :
#   1. crée /adls-simulation/retail-intelligence, qui joue le rôle du conteneur ADLS Gen2 « retail-intelligence »
#      (HDFS a, comme ADLS Gen2 avec Hierarchical Namespace, de vrais répertoires et des ACL POSIX) ;
#   2. crée les zones bronze / silver / gold / quality / documentation et y dépose des fichiers de démonstration
#      issus du projet (copie, rien n'est déplacé ni modifié dans /retail-lake) ;
#   3. applique la matrice des droits avec des ACL (entrées nommées + ACL par défaut héritées) ;
#   4. teste chaque droit en se présentant comme chaque profil (HADOOP_USER_NAME) et enregistre les résultats.
#
# Le script recrée tout à chaque exécution (il ne touche qu'à /adls-simulation).
set -uo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PATH="$JAVA_HOME/bin:/opt/hadoop/bin:$PATH"
J4="$(cd "$(dirname "$0")/../.." && pwd)"
EV="$J4/azure/evidence"
mkdir -p "$EV"
C=/adls-simulation/retail-intelligence        # = abfss://retail-intelligence@<compte>.dfs.core.windows.net/
ADMIN=$(whoami)                                # propriétaire, super-utilisateur HDFS (rôle « administrateur »)

# Arrêt immédiat si HDFS ne répond pas (sinon les preuves seraient écrasées par des résultats vides)
hdfs dfs -test -d /retail-lake/gold 2>/dev/null || {
  echo "HDFS injoignable ou /retail-lake/gold absent : lancer Jour2/scripts/03_demarrer_hdfs.sh"; exit 1; }

echo "== 1. Conteneur et zones"
hdfs dfs -rm -r -f -skipTrash /adls-simulation >/dev/null 2>&1
hdfs dfs -mkdir -p \
  $C/bronze/tpcxbb/initial $C/bronze/tpcxbb/refresh \
  $C/silver/ventes $C/silver/produits $C/silver/magasins \
  $C/gold/indicateurs $C/gold/analyses \
  $C/quality/controles $C/documentation

echo "== 2. Fichiers de démonstration (copies d'échantillons maîtrisés du projet)"
hdfs dfs -mkdir -p $C/bronze/tpcxbb/initial/store/ingest_date=2026-11-18
hdfs dfs -cp /retail-lake/bronze/tpcxbb/initial/store/ingest_date=2026-11-18/store.dat \
  $C/bronze/tpcxbb/initial/store/ingest_date=2026-11-18/
hdfs dfs -cp /retail-lake/silver/store $C/silver/magasins/store
hdfs dfs -cp /retail-lake/gold/ventes_magasin_annee $C/gold/analyses/ventes_magasin_annee
hdfs dfs -put "$J4/analytics/outputs/kpi_periode.csv" $C/gold/indicateurs/
hdfs dfs -put "$J4/analytics/outputs/kpi.csv" $C/gold/indicateurs/
hdfs dfs -put "$J4/analytics/outputs/controles_prealables.csv" $C/quality/controles/
hdfs dfs -put "$J4/analytics/outputs/inventaire_jeux.csv" $C/documentation/

echo "== 2b. Intégrité du dépôt : MD5 du fichier local = MD5 du fichier relu dans HDFS"
{
  echo "# Vérification d'intégrité du dépôt ($(date '+%Y-%m-%d %H:%M:%S'))"
  for f in kpi_periode.csv kpi.csv; do
    loc=$(md5sum "$J4/analytics/outputs/$f" | cut -d' ' -f1)
    dist=$(hdfs dfs -cat $C/gold/indicateurs/$f | md5sum | cut -d' ' -f1)
    echo "$f ; MD5 local = $loc ; MD5 relu dans HDFS = $dist ; $([ "$loc" = "$dist" ] && echo IDENTIQUE || echo DIFFERENT)"
  done
} > "$EV/integrite_depot_md5.txt"
cat "$EV/integrite_depot_md5.txt"

echo "== 2c. Hierarchical Namespace : publication d'un jeu par renommage ATOMIQUE d'un répertoire"
# Un traitement écrit d'abord dans un répertoire temporaire, puis publie TOUS les fichiers d'un coup en renommant
# le répertoire (une seule opération sur les métadonnées du NameNode, comme avec le HNS d'ADLS Gen2).
TMP=$C/gold/analyses/_tmp_publication_retours_categorie
hdfs dfs -cp /retail-lake/gold/retours_categorie $TMP
{
  echo "# Renommage atomique d'un répertoire ($(date '+%Y-%m-%d %H:%M:%S'))"
  echo "## Avant : jeu écrit dans un répertoire temporaire (invisible pour les lecteurs du jeu final)"
  hdfs dfs -count $TMP | awk '{print "répertoires="$1", fichiers="$2", octets="$3"  "$4}'
  hdfs dfs -ls $TMP | awk 'NR>1 {print "  "$NF}' | sed "s|$C/||"
  debut=$(date +%s%N)
  hdfs dfs -mv $TMP $C/gold/analyses/retours_categorie
  fin=$(date +%s%N)
  echo "## Opération : hdfs dfs -mv <tmp> gold/analyses/retours_categorie   (une seule opération, $(( (fin - debut) / 1000000 )) ms client compris)"
  echo "## Après : le répertoire temporaire n'existe plus, tous les fichiers sont publiés ensemble"
  hdfs dfs -test -e $TMP && echo "  temporaire encore présent : ÉCART" || echo "  temporaire absent : OK"
  hdfs dfs -count $C/gold/analyses/retours_categorie | awk '{print "répertoires="$1", fichiers="$2", octets="$3"  "$4}' | sed "s|$C/||"
  echo "## Sans HNS (stockage objet plat), le même « renommage » = copie puis suppression de chaque objet : non atomique."
} > "$EV/renommage_atomique_hns.txt"
cat "$EV/renommage_atomique_hns.txt"

echo "== 3. Matrice des droits (ACL)"
# Base : rien pour « les autres » ; seuls le propriétaire (administrateur) et les ACL nommées donnent accès.
hdfs dfs -chmod -R 750 /adls-simulation
hdfs dfs -chmod -R o-rwx,g-w /adls-simulation
# Traversée (--x) des répertoires parents : permet d'atteindre sa zone sans pouvoir lister les autres
for u in svc-ingestion data-engineer analyste; do
  hdfs dfs -setfacl -m user:$u:--x /adls-simulation
  hdfs dfs -setfacl -m user:$u:--x $C
done
acl() {   # acl <zone> <profil> <droits> : ACL d'accès (récursive) + ACL par défaut héritée par les nouveaux éléments
  hdfs dfs -setfacl -R -m user:$2:$3 $C/$1
  hdfs dfs -ls -R $C/$1 | awk '$1 ~ /^d/ {print $NF}' | while read -r d; do
    hdfs dfs -setfacl -m default:user:$2:$3 "$d"
  done
  hdfs dfs -setfacl -m default:user:$2:$3 $C/$1
}
acl bronze        svc-ingestion rwx    # ingestion : dépose dans Bronze
acl bronze        data-engineer r-x    # data engineering : lit Bronze
acl silver        data-engineer rwx    # data engineering : écrit Silver
acl gold          data-engineer rwx    # data engineering : publie Gold (écriture contrôlée)
acl gold          analyste      r-x    # analyste métier : lit Gold seulement
acl quality       data-engineer rwx
acl quality       analyste      r-x    # l'analyste consulte les résultats des contrôles
acl documentation svc-ingestion r-x
acl documentation data-engineer rwx
acl documentation analyste      r-x

echo "== 4. Preuves : arborescence et ACL"
{
  echo "# hdfs dfs -ls -R /adls-simulation   ($(date '+%Y-%m-%d %H:%M:%S'))"
  hdfs dfs -ls -R /adls-simulation | sed 's/  */ /g'
} > "$EV/arborescence_adls_simulee.txt"
{
  echo "# hdfs dfs -getfacl (répertoires des zones)   ($(date '+%Y-%m-%d %H:%M:%S'))"
  for d in /adls-simulation $C $C/bronze $C/silver $C/gold $C/quality $C/documentation; do
    hdfs dfs -getfacl "$d"; echo
  done
} > "$EV/acl_zones.txt"

echo "== 5. Tests des droits (chaque profil, chaque zone)"
OUT="$EV/tests_droits.csv"
echo "profil;operation;chemin;attendu;obtenu;resultat;message" > "$OUT"
essai() {  # essai <profil> <attendu AUTORISE|REFUSE> <libellé> <commande hdfs dfs ...>
  local profil=$1 attendu=$2 lib=$3; shift 3
  local msg
  msg=$(HADOOP_USER_NAME=$profil hdfs dfs "$@" 2>&1 >/dev/null)
  local code=$?
  local obtenu=$([ $code -eq 0 ] && echo AUTORISE || echo REFUSE)
  local res=$([ "$obtenu" = "$attendu" ] && echo OK || echo ECART)
  msg=$(echo "$msg" | grep -o "Permission denied.*" | head -1 | cut -c1-140 | tr ';' ',')
  echo "$profil;$lib;${@: -1};$attendu;$obtenu;$res;$msg" >> "$OUT"
  printf '%-14s %-38s %-9s -> %-9s %s\n' "$profil" "$lib" "$attendu" "$obtenu" "$res"
}
echo "démonstration" > /tmp/tp4_depot.txt
essai svc-ingestion AUTORISE "écrire un fichier dans Bronze"   -put -f /tmp/tp4_depot.txt $C/bronze/tpcxbb/refresh/depot_ingestion.txt
essai svc-ingestion REFUSE   "lister Silver"                   -ls $C/silver
essai svc-ingestion REFUSE   "lister Gold"                     -ls $C/gold
essai data-engineer AUTORISE "lire un fichier Bronze"          -cat $C/bronze/tpcxbb/initial/store/ingest_date=2026-11-18/store.dat
essai data-engineer REFUSE   "écrire dans Bronze"              -put -f /tmp/tp4_depot.txt $C/bronze/tpcxbb/initial/depot_dataeng.txt
essai data-engineer AUTORISE "écrire dans Silver"              -put -f /tmp/tp4_depot.txt $C/silver/ventes/depot_dataeng.txt
essai data-engineer AUTORISE "écrire dans Gold"                -put -f /tmp/tp4_depot.txt $C/gold/analyses/depot_dataeng.txt
essai analyste      AUTORISE "lire un indicateur Gold"         -cat $C/gold/indicateurs/kpi_periode.csv
essai analyste      AUTORISE "lister Gold (analyses)"          -ls $C/gold/analyses
essai analyste      REFUSE   "écrire dans Gold"                -put -f /tmp/tp4_depot.txt $C/gold/indicateurs/depot_analyste.txt
essai analyste      REFUSE   "supprimer un fichier Gold"       -rm -skipTrash $C/gold/indicateurs/kpi.csv
essai analyste      REFUSE   "lister Bronze"                   -ls $C/bronze
essai analyste      REFUSE   "lister Silver"                   -ls $C/silver
essai analyste      REFUSE   "lister la racine du conteneur"   -ls $C
essai analyste      AUTORISE "lire les contrôles qualité"      -ls $C/quality/controles
essai inconnu       REFUSE   "lire un indicateur Gold"         -cat $C/gold/indicateurs/kpi_periode.csv
rm -f /tmp/tp4_depot.txt
# ACL héritée : le fichier déposé par l'ingestion a reçu automatiquement les droits par défaut de son répertoire
hdfs dfs -getfacl $C/bronze/tpcxbb/refresh/depot_ingestion.txt > "$EV/acl_heritee_fichier_depose.txt"
echo "-> $(grep -c ';OK;' "$OUT") tests conformes sur $(($(wc -l < "$OUT") - 1))"
