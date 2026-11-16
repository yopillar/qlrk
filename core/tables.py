"""Calculer une table Qlik avec sa requete du dossier sql/.

run_sql("ventes__01_ventes.sql", table="Ventes")   une table Qlik

Les fichiers lus par la requete (vues in_<entree>) sont charges tout seuls
depuis conf/io.yml : rien a declarer dans le script. Lire et ecrire a la
main (read_input, write_output) sont dans datasets.py.
"""

from __future__ import annotations

import re
import time

from core import config, context, lineage
from core.datasets import read_input
from core.session import active_session

# Vue d'une entree de conf/io.yml dans les requetes : in_<nom de l'entree>,
# et in_<nom>__file pour FileTime().
INPUT_VIEW = re.compile(r"\bin_(\w+)")
# Commentaires et chaines d'une requete, ou un "in_" ne compte pas.
NOT_CODE = re.compile(
    r"--[^\n]*|/\*.*?\*/|'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"", re.S
)


def read_needed_inputs(sql: str) -> None:
    """Charge les entrees de conf/io.yml que la requete lit et qui ne sont
    pas encore la (vue in_ventes -> entree ventes).

    Args:
        sql: le texte de la requete.
    """
    state = context.state()
    inputs = config.section("input")
    for m in INPUT_VIEW.finditer(NOT_CODE.sub(" ", sql)):
        name = m.group(1).removesuffix("__file")
        view = f"in_{name}"
        if name in inputs and view not in state.tables:
            read_input(name, table=view)


def run_sql(sql_file: str, table: str) -> None:
    """Calcule une table Qlik avec sa requete du dossier sql/.

    Les entrees que la requete lit sont d'abord chargees depuis
    conf/io.yml. Le resultat devient la table `table`, que les requetes
    suivantes peuvent lire. Il est aussi ecrit dans une table du catalogue,
    pour que la transformation remonte dans OpenLineage (sauf avec
    --temp-views).

    Args:
        sql_file: le fichier de la requete, dans le dossier sql/ du projet.
        table: le nom de la table Qlik.
    """
    spark = active_session()
    state = context.state()
    sql = (state.sql_dir / sql_file).read_text(encoding="utf-8")
    read_needed_inputs(sql)
    context.doing(f"sql/{sql_file} (table {table})")
    t0 = time.perf_counter()
    df = spark.sql(sql)
    where = "vue temporaire"
    if state.database:
        if not state.database_ready:
            lineage.ensure_database(spark, state.database)
            state.database_ready = True
        name = lineage.table_name(state.database, state.script, table)
        df = lineage.materialize(spark, df, name)
        where = name
    df.createOrReplaceTempView(table)
    state.tables.append(table)
    context.log(
        f"sql {sql_file} -> {table} ({where}) en {time.perf_counter() - t0:.0f}s"
    )
