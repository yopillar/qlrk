"""Genere le code du projet dans src/ :

    src/job_<projet>.py                       le job (cree une seule fois)
    src/scripts/__init__.py                   (cree une seule fois)
    src/scripts/<projet>_step_010_run_sql.py  les scripts Qlik (reecrit)

L'etape 010 enchaine tous les scripts assembles, dans l'ordre de leurs
dependances. L'ordre est calcule a partir de work/stores.json (rempli par
prepare.py --collect), sans passer par le modele : si le script A ecrit un
fichier que B lit, A passe avant B. C'est ce que faisait l'enchainement des
taches de rechargement cote Qlik. Les entrees et les etapes de chaque script
sont lues dans work/<script>/assembly.json, ecrit par assemble.py.

Le job n'est jamais reecrit : on y ajoute ses propres etapes. S'il
existe deja sans appeler l'etape 010, on le signale.

Usage :
    python tools/orchestrate.py          # ecrit dans src/
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

STORES = Path("work/stores.json")
# Marque des fichiers que ce programme ecrivait dans l'ancienne
# arborescence (src/<projet>/job.py...).
GENERATED = "Fichier genere par tools/orchestrate.py"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_template import (  # noqa: E402
    function_name,
    job_path,
    project_dir,
    project_name,
    render_job,
    render_scripts_init,
    render_step,
    step_module,
)


def order_scripts(io: dict) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """Trie les scripts pour que chacun passe apres ceux qu'il lit."""

    # Meme rapprochement que dans prepare.py : meme chemin, ou meme nom de
    # fichier sans tenir compte de l'extension ni de la casse.
    def stem(path: str) -> str:
        """Nom de fichier sans dossier, extension ni casse."""
        return re.sub(
            r"\.(qvd|txt|csv|parquet|xlsx?)$",
            "",
            re.split(r"[\\/]", path)[-1],
            flags=re.I,
        ).lower()

    writers: dict[str, set[str]] = {}
    for script, d in io.items():
        for w in d["writes"]:
            writers.setdefault(w, set()).add(script)
            writers.setdefault(stem(w), set()).add(script)
    deps = {
        s: sorted(
            {
                w
                for r in d["reads"]
                for key in (r, stem(r))
                for w in writers.get(key, ())
                if w != s
            }
        )
        for s, d in io.items()
    }
    order: list[tuple[str, list[str]]] = []
    done: set[str] = set()
    cycles: list[str] = []
    while len(order) < len(deps):
        ready = sorted(s for s in deps if s not in done and set(deps[s]) <= done)
        if not ready:  # cycle : on en sort par ordre alphabetique
            blocked = sorted(s for s in deps if s not in done)
            cycles.append(blocked[0])
            ready = blocked[:1]
        for s in ready:
            order.append((s, deps[s]))
            done.add(s)
    return order, cycles


def main(argv: list[str] | None = None) -> int:
    """Point d'entree en ligne de commande.

    Args:
        argv: les arguments ; par defaut ceux de la ligne de commande.

    Returns:
        Le code retour du programme.
    """
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args(argv)
    if not STORES.exists():
        print(
            f"{STORES} absent : lance d'abord migrate.py (ou "
            "prepare.py --collect script_qvd/)."
        )
        return 1
    io = json.loads(STORES.read_text(encoding="utf-8"))
    order, cycles = order_scripts(io)
    stem = {s: Path(s).stem for s in io}
    assembled = {s: Path("work") / stem[s] / "assembly.json" for s in io}
    ready = [(s, d) for s, d in order if assembled[s].exists()]
    missing = [s for s, _ in order if not assembled[s].exists()]

    project = project_name()
    scripts_dir = project_dir() / "scripts"
    step_path = scripts_dir / f"{step_module()}.py"
    func = {s: function_name(stem[s]) for s in io}
    entries = []
    for s, d in ready:
        a = json.loads(assembled[s].read_text(encoding="utf-8"))
        entries.append((func[s], Path(s).name, a["steps"], [func[x] for x in d]))
    code = render_step(project, entries)
    # on s'assure que tout compile avant d'ecrire quoi que ce soit
    compile(code, str(step_path), "exec")
    scripts_dir.mkdir(parents=True, exist_ok=True)
    init = scripts_dir / "__init__.py"
    if not init.exists():
        init.write_text(render_scripts_init(project), encoding="utf-8")
    step_path.write_text(code, encoding="utf-8")
    job = job_path()
    if not job.exists():
        job.write_text(render_job(project), encoding="utf-8")
        print(f"{job} cree.")
    elif step_module() not in job.read_text(encoding="utf-8"):
        print(
            f"[!] {job} n'appelle pas l'etape {step_module()} : a ajouter "
            f"a la main (from scripts import {step_module()}, puis "
            f"{step_module()}.run())."
        )
    old = project_dir() / project
    if (old / "job.py").exists() and GENERATED in (old / "job.py").read_text(
        encoding="utf-8"
    ):
        print(
            f"[!] {old}/ : ancienne arborescence, remplacee par {job} et "
            f"{scripts_dir}/. A supprimer une fois la nouvelle verifiee."
        )

    print(f"{step_path} : {len(ready)} script(s) Qlik dans l'ordre :")
    for s, d in ready:
        print(
            f"  {stem[s]}"
            + (f"   (apres {', '.join(stem[x] for x in d)})" if d else "")
        )
    blocked = [stem[s] for s, d in ready if any(x in missing for x in d)]
    if missing:
        print(f"Pas encore assembles : {', '.join(stem[s] for s in missing)}")
    if blocked:
        print(
            f"[!] Dependent d'un script non assemble (liront des donnees "
            f"absentes ou anciennes) : {', '.join(blocked)}"
        )
    if cycles:
        print(
            f"[!] Dependance circulaire autour de : "
            f"{', '.join(stem[s] for s in cycles)} (ordre force)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
