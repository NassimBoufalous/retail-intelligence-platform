#!/usr/bin/env bash
# =====================================================================================================================
# UC-07 – SIMULATION DOCUMENTAIRE : création de l'espace ADLS Gen2 de la Retail Intelligence Platform (Azure CLI)
#
#   >>> CE SCRIPT N'A PAS ÉTÉ EXÉCUTÉ. <<<
#   Parcours B : pas d'abonnement Azure autorisé pour le TP, pas d'Azure CLI installée, et aucune ressource
#   payante ne doit être créée sans validation du formateur. Le script décrit les opérations qui SERAIENT
#   réalisées ; leur équivalent local a été exécuté sur HDFS (Jour4/azure/simulation/01_demo_zones_acl_hdfs.sh).
#
#   Les commandes suivent la syntaxe documentée de l'Azure CLI (az group, az storage account, az storage fs,
#   az role assignment) ; elles devraient être revérifiées (az --version, az <commande> --help) avant usage.
#   Aucun secret n'apparaît : authentification Microsoft Entra ID (az login, --auth-mode login), pas de clé de compte.
# =====================================================================================================================
set -euo pipefail
[ "${CONFIRMATION_FORMATEUR:-non}" = "oui" ] || { echo "Simulation : exécution bloquée (CONFIRMATION_FORMATEUR != oui)"; exit 1; }

RG=rg-retail-intelligence-dev          # groupe de ressources : tout le projet, supprimable d'un coup
LOC=francecentral                      # région à confirmer selon les quotas de l'abonnement
SA=stretaildev                         # compte de stockage : 3-24 caractères, minuscules et chiffres, unique dans Azure
FS=retail-intelligence                 # conteneur (« file system » en vocabulaire ADLS Gen2)
# Identifiants d'objets Microsoft Entra ID des groupes (à relever avec : az ad group show --group <nom> --query id)
G_INGESTION="<object-id du groupe g-retail-ingestion>"
G_DATAENG="<object-id du groupe g-retail-dataeng>"
G_ANALYSTES="<object-id du groupe g-retail-analystes>"

# --- 0. Contexte : identité et abonnement (vérifier qu'il s'agit bien de l'abonnement autorisé par le formateur)
az login
az account show --query "{abonnement:name, id:id, utilisateur:user.name}" -o table

# --- 1. Groupe de ressources (conteneur logique de gestion et de facturation)
az group create --name "$RG" --location "$LOC" --tags projet=retail-intelligence env=dev proprietaire=etudiant

# --- 2. Compte de stockage ADLS Gen2 : StorageV2 + Hierarchical Namespace (--hns true, choix fait À LA CRÉATION)
#        LRS (moins cher, suffisant pour un TP), TLS 1.2 minimum, aucun accès public, pas d'accès par clé partagée
az storage account create --name "$SA" --resource-group "$RG" --location "$LOC" \
  --kind StorageV2 --sku Standard_LRS --hns true --access-tier Hot \
  --min-tls-version TLS1_2 --allow-blob-public-access false --allow-shared-key-access false
az storage account show --name "$SA" --resource-group "$RG" --query "{nom:name, hns:isHnsEnabled, kind:kind, sku:sku.name}" -o table

# --- 3. Conteneur (file system) et arborescence des zones
az storage fs create --name "$FS" --account-name "$SA" --auth-mode login
for d in bronze/tpcxbb/initial bronze/tpcxbb/refresh silver/ventes silver/produits silver/magasins \
         gold/indicateurs gold/analyses quality/controles documentation; do
  az storage fs directory create --name "$d" --file-system "$FS" --account-name "$SA" --auth-mode login
done

# --- 4. RBAC (plan de contrôle et accès large) : le MINIMUM
#   - Pour les données, les ACL ci-dessous suffisent. Attention : un rôle RBAC de données (Storage Blob Data Reader /
#     Contributor) attribué sur le compte donne accès à TOUTES les zones et les ACL ne sont alors plus évaluées.
#     On ne l'attribue donc qu'aux identités de traitement, au niveau du conteneur.
SCOPE_FS="$(az storage account show -n "$SA" -g "$RG" --query id -o tsv)/blobServices/default/containers/$FS"
# Identité managée du workspace Synapse (traitements Spark) : lecture/écriture sur le conteneur du projet
# az role assignment create --assignee-object-id "<object-id identité managée Synapse>" --assignee-principal-type ServicePrincipal \
#   --role "Storage Blob Data Contributor" --scope "$SCOPE_FS"
# Analystes : rôle « Reader » sur le groupe de ressources (voir les ressources dans le portail, pas les données)
az role assignment create --assignee-object-id "$G_ANALYSTES" --assignee-principal-type Group --role "Reader" \
  --scope "$(az group show -n "$RG" --query id -o tsv)"

# --- 5. ACL POSIX par zone (moindre privilège) : accès + ACL par défaut héritées par les nouveaux fichiers
acl() {  # acl <chemin> <entrées ACL>
  az storage fs access update-recursive --path "$1" --acl "$2" --file-system "$FS" --account-name "$SA" --auth-mode login
}
# Racine : traversée seulement (--x) pour les 3 groupes (« set » remplace l'ACL entière : une seule commande)
az storage fs access set --path "/" \
  --acl "user::rwx,group::r-x,other::---,mask::r-x,group:$G_INGESTION:--x,group:$G_DATAENG:--x,group:$G_ANALYSTES:--x" \
  --file-system "$FS" --account-name "$SA" --auth-mode login
acl bronze        "group:$G_INGESTION:rwx,default:group:$G_INGESTION:rwx,group:$G_DATAENG:r-x,default:group:$G_DATAENG:r-x"
acl silver        "group:$G_DATAENG:rwx,default:group:$G_DATAENG:rwx"
acl gold          "group:$G_DATAENG:rwx,default:group:$G_DATAENG:rwx,group:$G_ANALYSTES:r-x,default:group:$G_ANALYSTES:r-x"
acl quality       "group:$G_DATAENG:rwx,default:group:$G_DATAENG:rwx,group:$G_ANALYSTES:r-x,default:group:$G_ANALYSTES:r-x"
acl documentation "group:$G_DATAENG:rwx,group:$G_INGESTION:r-x,group:$G_ANALYSTES:r-x"

# --- 6. Dépôt d'un fichier de démonstration autorisé (petit échantillon maîtrisé, non sensible) et vérification
az storage fs file upload --source Jour4/analytics/outputs/kpi_periode.csv --path gold/indicateurs/kpi_periode.csv \
  --file-system "$FS" --account-name "$SA" --auth-mode login
az storage fs file list --path gold --file-system "$FS" --account-name "$SA" --auth-mode login -o table
az storage fs access show --path gold --file-system "$FS" --account-name "$SA" --auth-mode login

# --- 7. Coûts : budget et alertes à créer dans Cost Management (portail) AVANT tout traitement ; puis, en fin de TP :
# az group delete --name "$RG" --yes      # supprime TOUTES les ressources du projet (stockage compris)
