"""
00 - Génération d'un jeu de données de SUBSTITUTION (TPC-DS via DuckDB).

POURQUOI CE SCRIPT ?
    Le TP J1 demande d'explorer les fichiers TPCx-BB fournis par le formateur.
    Au moment du TP, aucun kit TPCx-BB n'était disponible sur le poste.
    TPCx-BB (BigBench) réutilise le modèle de données "retail" de TPC-DS
    (store_sales, web_sales, item, customer, date_dim, ...).
    On génère donc TPC-DS avec l'extension officielle `tpcds` de DuckDB (dsdgen)
    pour pouvoir réaliser la démarche d'inspection sur des fichiers réels.

    ⚠️ Statut : SUBSTITUT - "À valider" par le formateur.
    Les tables propres à TPCx-BB (product_reviews, web_clickstreams,
    item_marketprices) ne sont PAS produites par TPC-DS.

SORTIE
    data/raw/tpcds_sf<SF>/<table>.csv   (séparateur '|', avec en-tête, UTF-8)
    data/raw/tpcds_sf<SF>/_provenance.json

USAGE
    ./.venv/Scripts/python scripts/00_generer_donnees_substitut.py [scale_factor]
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

SF = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "raw" / f"tpcds_sf{SF:g}"
OUT.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("INSTALL tpcds; LOAD tpcds;")

t0 = time.time()
print(f"Génération TPC-DS scale factor = {SF:g} ...")
con.execute(f"CALL dsdgen(sf={SF:g});")
tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

for t in tables:
    target = OUT / f"{t}.csv"
    con.execute(
        f"COPY {t} TO '{target.as_posix()}' (HEADER, DELIMITER '|')"
    )
    print(f"  - {t:<25} -> {target.name}")

provenance = {
    "nom_kit": f"tpcds_sf{SF:g} (substitut local, non fourni par le formateur)",
    "generateur": "DuckDB extension 'tpcds' (dsdgen intégré)",
    "version_duckdb": duckdb.__version__,
    "scale_factor": SF,
    "date_generation": datetime.now().isoformat(timespec="seconds"),
    "format": "CSV, séparateur '|', en-tête, UTF-8",
    "nb_tables": len(tables),
    "statut": "À valider - substitut de TPCx-BB",
    "tables_tpcxbb_absentes": ["product_reviews", "web_clickstreams", "item_marketprices"],
}
(OUT / "_provenance.json").write_text(json.dumps(provenance, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Terminé en {time.time() - t0:.1f} s -> {OUT}")
