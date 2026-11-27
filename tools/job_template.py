"""Gabarits du code genere pour un projet : le job et l'etape 010.

Ce code est lu par des developpeurs Qlik qui ne connaissent pas PySpark. Il
ne contient donc que des appels aux fonctions du socle spark_foundation :
    spark_foundation            pipeline (runner.py) : le job
    spark_foundation.runner     script : un script Qlik, vise par --from
    spark_foundation.datasets   write_output : ecrire une sortie
    spark_foundation.tables     run_sql : calculer une table Qlik
Tout ce qui est technique (session, io.yml, lecture des entrees, tables
pour OpenLineage, reprise, logs) est dans les autres fichiers du socle, et
les transformations dans pyspark_commons.

    src/job_<projet>.py                       le job, le seul a lancer
    src/scripts/__init__.py
    src/scripts/<projet>_step_010_run_sql.py  les scripts Qlik (genere)
    src/scripts/<projet>_step_020_<nom>.py    vos etapes (ecrites a la main)
    src/sql/                                  une requete par table Qlik

Le job n'est cree que s'il n'existe pas : on y ajoute ses propres etapes.
L'etape 010 est reecrite a chaque migration.
"""

from __future__ import annotations

import json
import keyword
import os
import re
from pathlib import Path

# Numero et nom de l'etape generee. Les numeros vont de 10 en 10 pour
# pouvoir inserer une etape plus tard (step_005, step_015...).
STEP = "step_010_run_sql"


def project_name() -> str:
    """Nom du projet : la variable QLIK_PROJECT si elle existe, sinon le nom
    du dossier courant, ramene a un identifiant Python (mon-projet devient
    mon_projet).

    Returns:
        Le nom, qui sert dans les noms de fichiers et de job.
    """
    name = os.environ.get("QLIK_PROJECT") or Path.cwd().name
    name = re.sub(r"\W", "_", name.strip().lower(), flags=re.ASCII)
    # un identifiant ne peut pas commencer par un chiffre
    return name if name and not name[0].isdigit() else f"p_{name}"


def project_dir() -> Path:
    """Dossier du code du projet.

    Returns:
        src/, qui contient le job, scripts/ et sql/.
    """
    return Path("src")


def job_path() -> Path:
    """Le job du projet, le seul fichier a lancer.

    Returns:
        src/job_<projet>.py.
    """
    return project_dir() / f"job_{project_name()}.py"


def step_module() -> str:
    """Nom du module de l'etape generee.

    Returns:
        <projet>_step_010_run_sql, dans src/scripts/.
    """
    return f"{project_name()}_{STEP}"


# Noms deja pris dans le fichier de l'etape : un script Qlik qui s'appelle
# ainsi prend le suffixe _qlik.
RESERVED = {"run", "script", "run_sql", "write_output", "run_tables"}


def function_name(script_stem: str) -> str:
    """Nom de la fonction d'un script Qlik dans l'etape 010, celui qu'on
    passe a --from et --only.

    Args:
        script_stem: le nom du script sans extension (ventes).

    Returns:
        Un identifiant Python : ventes, mon_script, s_2024...
    """
    name = re.sub(r"\W", "_", script_stem.lower(), flags=re.ASCII)
    if not name or name[0].isdigit():
        name = f"s_{name}"
    if keyword.iskeyword(name) or name in RESERVED:
        name += "_qlik"
    return name


JOB = '''"""Job du projet {project}.

Cree une seule fois, jamais reecrit : c'est ici qu'on ajoute ses propres
etapes, dans l'ordre (scripts/{project}_step_020_<nom>.py...). L'etape 010
rejoue les scripts Qlik ; ses requetes sont dans sql/. Le job s'arrete a la
premiere table en echec, et le dit dans les logs.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from spark_foundation import pipeline

from scripts import {step}


@pipeline("{project}")
def run(spark: SparkSession, args: list[str]) -> None:
    """Fait tourner les etapes du projet, dans l'ordre.

    Args:
        spark: la session Spark, ouverte par @pipeline.
        args: les arguments de la ligne de commande, deja lus par
            @pipeline.
    """
    {step}.run()


if __name__ == "__main__":
    raise SystemExit(run())
'''

SCRIPTS_INIT = '"""Etapes du projet {project}, une par fichier, dans l\'ordre."""\n'

STEP_HEADER = '''"""Etape 010 du projet {project} : les scripts Qlik, traduits en Spark SQL.

Fichier genere : ne pas le modifier a la main, il est reecrit a chaque
migration. Vos etapes vont dans d'autres fichiers
({project}_step_020_<nom>.py...).

Une liste par script Qlik : ses tables dans l'ordre, chacune avec sa
requete du dossier sql/ et, si le script Qlik la stockait (STORE), sa
sortie dans conf/io.yml. Les fichiers que lisent les requetes sont trouves
dans conf/io.yml : il n'y a rien a declarer ici.
"""

from __future__ import annotations

from spark_foundation.datasets import write_output
from spark_foundation.tables import run_sql
'''

STEP_RUN = '''

def run() -> None:
    """Fait tourner les scripts Qlik du projet, dans l'ordre : chaque table
    est calculee, puis ecrite si le script Qlik la stockait (STORE)."""
    for sql_file, table, output in {tables}:
        run_sql(sql_file, table=table)
        if output:
            write_output(output, table=table)
'''


def render_job(project: str) -> str:
    """Code de src/job_<projet>.py.

    Args:
        project: le nom du projet.

    Returns:
        Le code du job.
    """
    return JOB.format(project=project, step=f"{project}_{STEP}")


def render_scripts_init(project: str) -> str:
    """Code de src/scripts/__init__.py.

    Args:
        project: le nom du projet.

    Returns:
        Une simple docstring : le dossier devient un package.
    """
    return SCRIPTS_INIT.format(project=project)


def _literal(value: str | None) -> str:
    """Valeur Python ecrite dans le code genere : "texte" ou None."""
    return "None" if value is None else json.dumps(value)


def _table_line(sql_file: str, table: str, output: str | None) -> str:
    """Une ligne d'une liste de tables, mise en forme comme ruff format.

    Args:
        sql_file: la requete.
        table: le nom de la table.
        output: la sortie, ou None.

    Returns:
        La ligne, ou le tuple sur plusieurs lignes s'il depasse 88 caracteres.
    """
    values = [_literal(sql_file), _literal(table), _literal(output)]
    line = f"    ({', '.join(values)}),"
    if len(line) <= 88:
        return line
    inner = "".join(f"        {v},\n" for v in values)
    return f"    (\n{inner}    ),"


def render_step(
    project: str, scripts: list[tuple[str, str, list[list], list[str]]]
) -> str:
    """Code de src/scripts/<projet>_step_010_run_sql.py.

    Une liste de tables par script Qlik, puis run() et sa boucle unique,
    qui les enchaine dans l'ordre.

    Args:
        project: le nom du projet.
        scripts: pour chaque script, dans l'ordre ou ils tournent : (nom de
            la liste, fichier du script Qlik, ses tables [fichier sql,
            table, sortie ou None], scripts dont il depend).

    Returns:
        Le code du module.
    """
    parts = [STEP_HEADER.format(project=project)]
    for func, qlik_file, steps, deps in scripts:
        lines = "".join(
            _table_line(sql_file, table, output or None) + "\n"
            # *_ : les anciens assembly.json ont une 4e valeur, ignoree
            for sql_file, table, output, *_ in steps
        )
        after = f", apres {', '.join(deps)} dont il lit les sorties" if deps else ""
        parts.append(
            f"\n# Script Qlik {qlik_file}{after}\n"
            f"{func.upper()}: list[tuple[str, str, str | None]] = ["
            + (f"\n{lines}" if lines else "")
            + "]\n"
        )
    names = " + ".join(func.upper() for func, *_ in scripts)
    if len(f"    for sql_file, table, output in {names}:") > 88:
        # une liste par ligne, comme ruff format
        names = (
            "(\n        "
            + "\n        + ".join(func.upper() for func, *_ in scripts)
            + "\n    )"
        )
    if not scripts:
        return parts[0] + '\n\ndef run() -> None:\n    """Aucun script Qlik."""\n'
    return "".join(parts) + STEP_RUN.format(tables=names)
