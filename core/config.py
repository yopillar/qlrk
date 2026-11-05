"""Lecture de conf/io.yml et des schemas.

io.yml a deux sections, inputs et outputs. Chaque entree a une location
(avec des variables ${entrees}, ${sorties}...), un format, et pour une
entree un schema_file et des options de lecture. Une entree qui relit une
sortie du job s'ecrit simplement "from_output: <nom de la sortie>".
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

IO_FILE = Path("conf/io.yml")
# Valeurs des variables de location quand l'environnement ne les donne pas.
DEFAULT_VARIABLES = {"entrees": "data/input", "sorties": "data/output"}


def load_io() -> dict[str, Any]:
    """Contenu de conf/io.yml.

    Returns:
        Le fichier lu, ou un dictionnaire vide s'il est vide.
    """
    data = yaml.safe_load(IO_FILE.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def section(kind: str) -> dict[str, dict[str, Any]]:
    """Entrees ou sorties de io.yml.

    Args:
        kind: "input" ou "output" (la section inputs ou outputs).

    Returns:
        Les entrees de la section, par nom.
    """
    for key, value in load_io().items():
        if kind in str(key).lower() and isinstance(value, dict):
            return value
    return {}


def entry(kind: str, name: str) -> dict[str, Any]:
    """Une entree ou une sortie de io.yml.

    Args:
        kind: "input" ou "output".
        name: son nom dans io.yml.

    Returns:
        Sa description (location, format, schema_file, options).

    Raises:
        KeyError: elle n'est pas declaree.
    """
    entries = section(kind)
    if name not in entries:
        raise KeyError(f"{name} absent de la section {kind}s de {IO_FILE}")
    description = entries[name]
    target = description.get("from_output") if kind == "input" else None
    if target:
        # entree qui relit une sortie du job : meme location, en Parquet
        output = entry("output", str(target))
        return {"format": "parquet", **output}
    return description


def location(description: dict[str, Any]) -> str:
    """Location d'une entree, variables remplacees.

    Une variable ${x} prend la valeur de la variable d'environnement x, sinon
    celle de DEFAULT_VARIABLES ; inconnue, elle reste telle quelle.

    Args:
        description: l'entree de io.yml.

    Returns:
        Le chemin a lire ou a ecrire.
    """

    def value(match: re.Match[str]) -> str:
        """Valeur d'une variable ${x}."""
        name = match.group(1)
        return os.environ.get(name, DEFAULT_VARIABLES.get(name, match.group(0)))

    return re.sub(r"\$\{(\w+)\}", value, str(description["location"]))


def schema_columns(description: dict[str, Any]) -> dict[str, str]:
    """Colonnes d'une entree et leur type, d'apres son schema_file.

    Args:
        description: l'entree de io.yml.

    Returns:
        {colonne: type} dans l'ordre du schema, vide sans schema.
    """
    if not description.get("schema_file"):
        return {}
    path = IO_FILE.parent / description["schema_file"]
    columns = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("columns")
    return {str(c): str(t) for c, t in (columns or {}).items()}


def schema_ddl(description: dict[str, Any]) -> str | None:
    """Schema d'une entree, au format DDL de Spark.

    Args:
        description: l'entree de io.yml, avec son schema_file.

    Returns:
        "`col` TYPE, ..." dans l'ordre du schema, ou None sans schema.
    """
    columns = schema_columns(description)
    if not columns:
        return None
    return ", ".join(f"`{name}` {typ}" for name, typ in columns.items())
