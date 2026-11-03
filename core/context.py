"""Etat du job en cours : dossier des requetes, base des tables
intermediaires, script courant et ce qu'il a lu."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from core.session import active_session

logger = logging.getLogger("core")


@dataclass
class JobState:
    """Ce que les fonctions de core/ ont besoin de savoir pendant le job."""

    job_name: str = ""  # nom donne a @pipeline
    sql_dir: Path = Path("sql")  # requetes du projet, une par table Qlik
    database: str | None = None  # base des tables intermediaires, ou None
    step: str = ""  # etape en cours (step_010_run_sql)
    script: str = ""  # script Qlik en cours
    action: str = ""  # ce qui est en train de tourner, pour les erreurs
    tables: list[str] = field(default_factory=list)  # vues du script en cours
    read_locations: set[str] = field(default_factory=set)  # fichiers lus
    database_ready: bool = False  # base deja creee
    start: str | None = None  # --from
    only: str | None = None  # --only
    started: bool = False  # --from : le script de depart est passe
    seen: list[str] = field(default_factory=list)  # scripts appeles


_STATE = JobState()


def state() -> JobState:
    """Etat du job en cours.

    Returns:
        L'etat partage par toutes les fonctions de core/.
    """
    return _STATE


def start_job(
    job_name: str,
    sql_dir: Path = Path("sql"),
    database: str | None = None,
    start: str | None = None,
    only: str | None = None,
) -> None:
    """Remet l'etat a zero au lancement d'un job.

    Args:
        job_name: le nom du job.
        sql_dir: le dossier des requetes du projet.
        database: la base des tables intermediaires, ou None (vues
            temporaires seulement).
        start: --from, le script ou reprendre.
        only: --only, le seul script a lancer.
    """
    global _STATE
    _STATE = JobState(
        job_name=job_name, sql_dir=sql_dir, database=database, start=start, only=only
    )


def label() -> str:
    """Debut de chaque ligne de log : [projet] etape > script.

    Returns:
        Par exemple "[demo] step_010_run_sql > ventes".
    """
    text = f"[{_STATE.job_name}]" if _STATE.job_name else ""
    if _STATE.step:
        text += f" {_STATE.step}"
    if _STATE.script:
        text += f" > {_STATE.script}"
    return text.strip()


def log(message: str) -> None:
    """Ecrit une ligne de log, avec le projet, l'etape et le script.

    Args:
        message: ce qui se passe (lecture, sql, ecriture...).
    """
    logger.info("%s : %s", label(), message)


def doing(action: str) -> None:
    """Note ce qui commence, pour le citer si ca echoue.

    Args:
        action: par exemple "sql/ventes__01_ventes.sql (table Ventes)".
    """
    _STATE.action = action


@contextmanager
def script_scope(name: str, step: str = "") -> Iterator[None]:
    """Le temps d'un script : son nom, puis le menage de ses vues.

    Les vues d'un script sont supprimees a la fin, pour que le script
    suivant ne tombe pas sur une table du meme nom.

    Args:
        name: le nom du script.
        step: l'etape qui le contient (step_010_run_sql).

    Yields:
        Rien : le script tourne dans le bloc.
    """
    _STATE.script = name
    _STATE.step = step
    _STATE.action = ""
    _STATE.tables = []
    _STATE.read_locations = set()
    try:
        yield
    finally:
        spark = active_session()
        for table in _STATE.tables:
            spark.catalog.dropTempView(table)
        _STATE.script = ""
        _STATE.step = ""
