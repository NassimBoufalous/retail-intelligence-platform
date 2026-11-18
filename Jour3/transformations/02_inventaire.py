"""
Étapes 2 et 3 – Inventaire technique et exploration des sources Bronze avec PySpark.

Pour chaque table retenue (lots initial + refresh) :
    - inventaire : emplacement, format, taille, lignes, colonnes, clé candidate, doublons exacts ;
    - profil de chaque colonne : type proposé par inferSchema, type cible, valeurs nulles (et %),
      valeurs distinctes, min, max, valeurs non convertibles vers le type cible ;
    - contrôles de grain sur les faits : la clé « métier » supposée est-elle unique ?

Sorties (UC06_preuves/exploration/) : inventaire_sources.csv, profil_colonnes.csv, grain_faits.csv,
schemas_infer.txt (schémas proposés par Spark, pour les comparer au schéma attendu).
"""
import config as C
import commun as U
from pyspark.sql import functions as F

spark, log = U.session("02_inventaire")
OUT = C.PREUVES / "exploration"

GRAIN = {  # clé métier supposée -> à contrôler (jamais supposée vraie)
    "store_sales": ["ss_ticket_number", "ss_item_sk"],
    "web_sales": ["ws_order_number", "ws_item_sk"],
    "store_returns": ["sr_ticket_number", "sr_item_sk"],
    "web_returns": ["wr_order_number", "wr_item_sk"],
    "item": ["i_item_sk"], "store": ["s_store_sk"], "date_dim": ["d_date_sk"], "reason": ["r_reason_sk"],
}
GRANULARITE = {
    "store_sales": "1 ligne de ticket de caisse (article vendu en magasin)",
    "web_sales": "1 ligne de commande web (article commandé)",
    "store_returns": "1 ligne de retour en magasin",
    "web_returns": "1 ligne de retour web",
    "item": "1 produit", "store": "1 magasin", "date_dim": "1 jour calendaire", "reason": "1 motif de retour",
}

inventaire, profil, grain, schemas = [], [], [], []
for table in C.TABLES:
    df_txt = U.lire_bronze(spark, table)
    lots = [r["lot_source"] for r in df_txt.select("lot_source").distinct().collect()]
    chemins = [U.chemin_bronze(l, table) for l in sorted(lots)]
    octets = sum(U.taille(spark, p) for p in chemins)

    # 1) Ce que Spark « devine » (inferSchema) : utile pour une 1re exploration, pas une validation
    infer = (spark.read.option("sep", "|").option("header", False).option("inferSchema", True)
             .csv([f"{p}/{table}.dat" for p in chemins]).toDF(*U.SCHEMA[table]))
    types_infer = {f.name: f.dataType.simpleString() for f in infer.schema.fields}
    schemas.append(f"== {table} (inferSchema) ==\n{infer._jdf.schema().treeString()}")

    # 2) Conversion explicite vers les types cibles, en comptant les échecs
    df, echecs = U.typer(df_txt, table, log)
    df.cache()
    n = df.count()
    cols = U.SCHEMA[table]

    agg = []
    for c in cols:
        agg += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__nulls"),
                F.countDistinct(c).alias(f"{c}__distinct"),
                F.min(c).cast("string").alias(f"{c}__min"), F.max(c).cast("string").alias(f"{c}__max")]
    r = df.agg(*agg).first().asDict()
    cles = []
    for c in cols:
        nulls, dist = int(r[f"{c}__nulls"]), int(r[f"{c}__distinct"])
        if nulls == 0 and dist == n:
            cles.append(c)
        profil.append({"table": table, "colonne": c, "type_infer": types_infer[c],
                       "type_cible": U.type_cible(table, c).simpleString(), "lignes": n, "nulls": nulls,
                       "pct_nulls": round(100 * nulls / n, 3) if n else 0, "distinct": dist,
                       "min": r[f"{c}__min"], "max": r[f"{c}__max"],
                       "non_convertibles": echecs.get(c, 0)})

    doublons_exacts = n - df.select(*cols).distinct().count()
    k = GRAIN[table]
    k_dist = df.select(*k).distinct().count()
    k_nulls = df.filter(" OR ".join(f"{c} IS NULL" for c in k)).count()
    grain.append({"table": table, "cle_supposee": "+".join(k), "lignes": n, "valeurs_distinctes": k_dist,
                  "lignes_en_trop": n - k_dist, "lignes_cle_nulle": k_nulls,
                  "cle_unique": n == k_dist and k_nulls == 0})

    par_lot = {r_["lot_source"]: r_["count"] for r_ in df.groupBy("lot_source").count().collect()}
    inventaire.append({
        "table": table, "fichiers": " + ".join(f"{p}/{table}.dat" for p in chemins),
        "format": "texte délimité (CSV) séparateur |, sans en-tête, UTF-8, fins de ligne CRLF",
        "octets": octets, "lignes": n, "lignes_initial": par_lot.get("initial", 0),
        "lignes_refresh": par_lot.get("refresh", 0), "colonnes": len(cols),
        "colonnes_principales": ", ".join(cols[:6]) + (" …" if len(cols) > 6 else ""),
        "cles_candidates": ", ".join(cles) or "aucune colonne seule",
        "cle_metier_controlee": f"{'+'.join(k)} : {'unique' if n == k_dist else f'{n - k_dist} lignes en trop'}",
        "doublons_exacts": doublons_exacts, "granularite": GRANULARITE[table],
        "colonnes_avec_nulls": sum(1 for p in profil if p["table"] == table and p["nulls"] > 0),
        "valeurs_non_convertibles": sum(echecs.values()),
    })
    log.info("%s : %d lignes, %d col., clés candidates=%s, doublons exacts=%d, grain %s=%s",
             table, n, len(cols), cles, doublons_exacts, "+".join(k), "unique" if n == k_dist else "NON unique")
    df.unpersist()

U.ecrire_csv_local(inventaire, OUT / "inventaire_sources.csv")
U.ecrire_csv_local(profil, OUT / "profil_colonnes.csv")
U.ecrire_csv_local(grain, OUT / "grain_faits.csv")
(OUT / "schemas_infer.txt").write_text("\n".join(schemas), encoding="utf-8")
spark.stop()
