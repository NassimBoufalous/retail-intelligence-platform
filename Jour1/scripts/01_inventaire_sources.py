"""
01 - Inventaire et profilage des sources (UC-01, étape C "Examiner les données").

Entrée : le kit TPCx-BB fourni par le formateur, organisé en lots :
    <kit>/data/<table>/<table>.dat           (lot initial)
    <kit>/data_refresh/<table>/<table>.dat   (lot de rafraîchissement)
    + éventuels fichiers annexes (ex. item_marketprices_audit.csv)

Pour CHAQUE fichier on mesure (sans rien supposer) :
    lot, nom exact, chemin, format, taille, nb de lignes, nb de champs par ligne (constant ?),
    noms/types des colonnes, valeurs nulles, nb de valeurs distinctes, min/max, clés candidates.

Les .dat n'ont PAS d'en-tête : les noms de colonnes viennent de scripts/schema_tpcxbb.py (statut "Déduit").

SORTIES (Jour1/01_UC01_besoin_exploration/UC01_preuves/)
    arborescence_donnees.txt, inventaire_tables.csv, dictionnaire_colonnes.csv, apercus/<lot>__<table>.txt

USAGE
    ./.venv/Scripts/python scripts/01_inventaire_sources.py retail_project
"""
import csv
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schema_tpcxbb import SCHEMA, read_sql  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
KIT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / "retail_project"
PREUVES = ROOT / "Jour1" / "01_UC01_besoin_exploration" / "UC01_preuves"
APERCUS = PREUVES / "apercus"
APERCUS.mkdir(parents=True, exist_ok=True)
for old in APERCUS.glob("*.txt"):  # aperçus d'une exécution précédente
    old.unlink()

con = duckdb.connect()


def human(n: float) -> str:
    for unit in ["o", "Ko", "Mo", "Go"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} To"


def rel(p: Path) -> str:
    return p.relative_to(ROOT).as_posix()


# --- 1. Arborescence (preuve) -----------------------------------------------
all_files = sorted(p for p in KIT.rglob("*") if p.is_file())
with open(PREUVES / "arborescence_donnees.txt", "w", encoding="utf-8") as f:
    f.write(f"{rel(KIT)}/\n")
    for p in all_files:
        f.write(f"|-- {p.relative_to(KIT).as_posix():<62} {human(p.stat().st_size):>10}\n")
    for lot in ["data", "data_refresh"]:
        lf = [p for p in all_files if p.relative_to(KIT).parts[0] == lot]
        f.write(f"\n{lot}: {len(lf)} fichiers, {human(sum(p.stat().st_size for p in lf))}")
    f.write(f"\nTOTAL : {len(all_files)} fichiers, {human(sum(p.stat().st_size for p in all_files))}\n")

# --- 2. Profilage fichier par fichier ---------------------------------------
tables_rows, col_rows = [], []
for p in all_files:
    lot = p.relative_to(KIT).parts[0]
    table = p.parent.name
    size = p.stat().st_size

    # Aperçu brut (preuve du format réel) + comptage des champs par ligne
    with open(p, encoding="utf-8", errors="replace") as fh:
        head = [next(fh, "") for _ in range(5)]
    (APERCUS / f"{lot}__{p.name}.txt").write_text("".join(l[:400] + ("…\n" if len(l) > 400 else "") for l in head),
                                                    encoding="utf-8")

    if p.suffix != ".dat" or table not in SCHEMA:
        # Fichier annexe : on l'inventorie sans le profiler comme une table
        nrows = sum(1 for _ in open(p, encoding="utf-8", errors="replace"))
        tables_rows.append({"lot": lot, "table": table, "fichier": p.name, "chemin": rel(p),
                            "format": f"Annexe ({p.suffix[1:].upper()}, avec en-tête)", "taille_octets": size,
                            "taille": human(size), "nb_lignes": nrows, "nb_colonnes": "", "champs_par_ligne": "",
                            "cles_candidates": "", "colonnes_avec_nulls": "", "statut": "Vérifié (métadonnée du générateur)"})
        print(f"[{lot}] {p.name:<34} annexe, {nrows} lignes")
        continue

    src = read_sql(p.as_posix(), table)
    nf = con.execute(  # nb de champs par ligne : 1 seule valeur attendue
        f"SELECT string_agg(DISTINCT (len(string_split(l, '|')))::VARCHAR, ',') FROM "
        f"read_csv('{p.as_posix()}', columns={{'l':'VARCHAR'}}, delim='\\x01', header=false, quote='', escape='')"
    ).fetchone()[0]
    cols = [(s[0], s[1]) for s in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    nrows = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]

    exprs = []
    for c, _ in cols:
        exprs += [f"count(*) - count({c})", f"count(DISTINCT {c})", f"min({c})::VARCHAR", f"max({c})::VARCHAR"]
    stats = con.execute(f"SELECT {', '.join(exprs)} FROM {src}").fetchone()

    candidates, n_null_cols = [], 0
    for i, (c, typ) in enumerate(cols):
        nulls, distinct, vmin, vmax = stats[4 * i: 4 * i + 4]
        is_key = nrows > 0 and nulls == 0 and distinct == nrows
        candidates += [c] if is_key else []
        n_null_cols += nulls > 0
        col_rows.append({"lot": lot, "table": table, "position": i + 1, "colonne": c, "type_observe": typ,
                         "nb_nulls": nulls, "pct_nulls": round(100 * nulls / nrows, 2) if nrows else 0,
                         "nb_distincts": distinct, "min": (vmin or "")[:40], "max": (vmax or "")[:40],
                         "cle_candidate": "oui" if is_key else "", "nom_colonne": "Déduit (DDL TPCx-BB)"})

    tables_rows.append({"lot": lot, "table": table, "fichier": p.name, "chemin": rel(p),
                        "format": "Texte délimité « | », sans en-tête, fins de ligne CRLF", "taille_octets": size,
                        "taille": human(size), "nb_lignes": nrows, "nb_colonnes": len(cols), "champs_par_ligne": nf,
                        "cles_candidates": ", ".join(candidates) or "aucune clé simple",
                        "colonnes_avec_nulls": n_null_cols,
                        "statut": "Vérifié (mesuré)" if str(nf) == str(len(cols)) else "ANOMALIE nb de champs"})
    print(f"[{lot}] {table:<24} {nrows:>10,} lignes  {len(cols):>3} col. (champs/ligne: {nf})  clé(s): {', '.join(candidates) or '-'}")


def write_csv(path, rows):
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=keys, delimiter=";")
        w.writeheader()
        w.writerows(rows)


write_csv(PREUVES / "inventaire_tables.csv", tables_rows)
write_csv(PREUVES / "dictionnaire_colonnes.csv", col_rows)
print(f"\nPreuves écrites dans {rel(PREUVES)}")
