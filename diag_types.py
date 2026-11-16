"""Diagnostic des types d'un projet migre.

Pour chaque entree de conf/io.yml : sa forme (from_output ou sa propre
location), les types de son schema, et les types reellement stockes dans le
Parquet. Puis, pour chaque script, les types que le controle utilise pour
ses entrees. Rien n'est modifie.

    python diag_types.py sorties=data/output entrees=data/input

A lancer a la racine du projet, la ou sont tools/ et conf/. Les variables
sont celles des locations de conf/io.yml.
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

sys.path.insert(0, "tools")
from conf import IoConf, IoEntry, resolve_location  # noqa: E402


def stored_types(location: str, variables: dict[str, str]) -> dict[str, str] | str:
    """Types reellement stockes dans un Parquet.

    Args:
        location: la location de l'entree, avec ses variables.
        variables: la valeur des variables (sorties=data/output...).

    Returns:
        {colonne: type pyarrow}, ou la raison pour laquelle on ne sait pas.
    """
    path = Path(resolve_location(location, variables))
    files = (
        [str(path)]
        if path.is_file()
        else sorted(glob.glob(f"{path}/**/*.parquet", recursive=True))
    )
    files = [f for f in files if f.endswith(".parquet")]
    if not files:
        return f"pas de Parquet a {path}"
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return "pyarrow absent"
    return {f.name: str(f.type) for f in pq.read_schema(files[0])}


def show_entry(entry: IoEntry, variables: dict[str, str]) -> None:
    """Affiche une entree : sa forme, son schema et le fichier.

    Args:
        entry: l'entree de conf/io.yml.
        variables: la valeur des variables des locations.
    """
    form = (
        f"from_output: {entry.from_output}"
        if entry.from_output
        else f"location: {entry.location}"
    )
    print(
        f"\n=== entree {entry.name} ({entry.format}) | {form} | "
        f"schema: {entry.schema_file}"
    )
    real = (
        stored_types(entry.location, variables)
        if entry.format == "parquet"
        else "pas un Parquet"
    )
    if isinstance(real, str):
        print(f"    fichier : {real}")
    for col, typ in (entry.types or {}).items():
        found = real.get(col, "-") if isinstance(real, dict) else "-"
        print(f"    schema {col:25} {typ:12} fichier: {found}")
    if not entry.types and isinstance(real, dict):
        print(f"    pas de schema ; fichier : {real}")


def show_checks() -> None:
    """Affiche, pour chaque script, les types que le controle utilise pour
    ses entrees (work/<script>/manifest.json)."""
    seen = set()
    for manifest in sorted(glob.glob("work/*/manifest.json")):
        data = json.loads(Path(manifest).read_text(encoding="utf-8"))
        for sec in data["sections"]:
            for view, props in sec.get("views", {}).items():
                key = (data["script"], view)
                if "path" not in props or key in seen:
                    continue
                seen.add(key)
                print(
                    f"\n[{data['script']}] vue {view} (entree {props.get('io')}) "
                    f"types du controle : {props.get('types')}"
                )


def main(argv: list[str] | None = None) -> int:
    """Point d'entree en ligne de commande.

    Args:
        argv: les variables NOM=VALEUR ; par defaut la ligne de commande.

    Returns:
        Le code retour du programme.
    """
    args = sys.argv[1:] if argv is None else argv
    variables = dict(a.split("=", 1) for a in args if "=" in a)
    conf = IoConf()
    for problem in conf.problems:
        print(f"[!] {problem}")
    for entry in conf.inputs:
        show_entry(entry, variables)
    show_checks()
    return 0


if __name__ == "__main__":
    sys.exit(main())
