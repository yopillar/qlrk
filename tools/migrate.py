"""Lance toute la migration d'un script, ou d'un dossier de scripts.

    python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # la 1re fois
    python tools/migrate.py script_qvd/ventes.qvs             # ensuite
    python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier

Avant tout, fill_conf.py ajoute dans conf/io.yml et dans les schemas ce
qui manque pour les scripts du dossier (jamais de modification de
l'existant, --no-conf pour s'en passer). Puis, pour chaque script :
  1. prepare.py    decoupe le script et traduit en SQL tout ce qu'on sait
                   traduire sans le modele (le reste est marque TODO(model))
  2. translate.py  fait completer les TODO par le modele, une table par
                   appel, avec un controle et une relance si ca ne passe pas
  3. assemble.py   copie les requetes dans src/sql/ quand tout est valide,
                   puis orchestrate.py regenere l'etape 010
                   (src/scripts/<projet>_step_010_run_sql.py)
  4. verify.py     compare avec Qlik, s'il y a des sorties dans
                   data/qlik_outputs/ : une fois tous les scripts assembles,
                   le job est lance une seule fois, en entier (comme
                   spark-submit), puis chaque script est compare

Au bout, l'etape 010 contient tous les scripts assembles, dans l'ordre de
leurs dependances, et src/job_<projet>.py la lance.

On peut relancer autant de fois qu'on veut : ce qui est deja traduit et n'a
pas bouge n'est pas refait.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))
from conf import WORK, find_scripts  # noqa: E402
from job_template import job_path  # noqa: E402
from orchestrate import STORES, order_scripts  # noqa: E402


def compare_all(
    assembled: list[tuple[int, Path]],
    summary: list[tuple[str, str, list[str]]],
    variables: list[str],
) -> None:
    """Lance le job une seule fois, en entier comme spark-submit, puis
    compare chaque script assemble avec les exports Qlik.

    Le job n'est pas lance script par script avec --only : la facon dont le
    socle nomme les scripts n'entre pas en jeu.

    Args:
        assembled: (rang dans le resume, work/<script>) des scripts
            assembles.
        summary: le resume, complete avec le verdict de chaque script.
        variables: les --var de la ligne de commande.
    """
    print(f"\nExecution du job {job_path()} pour la comparaison avec Qlik ...")
    started = time.time()
    failed = subprocess.run([sys.executable, str(job_path())]).returncode != 0
    if failed:
        print(
            "[!] Une ou plusieurs etapes du job ont echoue (voir la trace "
            "ci-dessus) : seules les sorties qu'il a reecrites sont comparees."
        )
    for rank, workdir in assembled:
        rc = run(
            "verify.py",
            str(workdir),
            "--no-run",
            f"--started={started}",
            *[f"--var={v}" for v in variables],
        )
        verdict = {
            0: "donnees identiques a Qlik",
            1: "ecarts avec Qlik : voir comparaison.md",
            3: "pas d'export Qlik dans data/qlik_outputs/ pour comparer",
        }.get(rc, "comparaison impossible (voir ci-dessus)")
        if failed:
            verdict += " (le job a echoue en route)"
        name, state, to_fix = summary[rank]
        summary[rank] = (name, f"{state}, {verdict}", to_fix)


def run(script: str, *args: str) -> int:
    """Lance un outil du kit et renvoie son code retour."""
    return subprocess.run([sys.executable, str(TOOLS / script), *args]).returncode


def main(argv: list[str] | None = None) -> int:
    """Point d'entree en ligne de commande.

    Args:
        argv: les arguments ; par defaut ceux de la ligne de commande.

    Returns:
        Le code retour du programme.
    """
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("path", type=Path, help="un script Qlik ou le dossier script_qvd/")
    p.add_argument("--pilot", type=int, help="nombre de tables, puis arret")
    p.add_argument("--jobs", type=int, default=1, help="tables traduites en parallele")
    p.add_argument("--model", help="provider/model")
    p.add_argument("--opencode-bin", default="opencode")
    p.add_argument(
        "--opencode-in-box",
        action="store_true",
        help="opencode est dans la distrobox : pas de host-spawn",
    )
    p.add_argument(
        "--skip-selftest",
        action="store_true",
        help="ne pas tester le modele avant de traduire",
    )
    p.add_argument(
        "--no-conf",
        action="store_true",
        help="ne pas completer conf/io.yml ni les schemas",
    )
    p.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="NOM=VALEUR",
        help="variable des locations de conf/io.yml, pour la "
        "comparaison (ex. --var sorties=data/output)",
    )
    args = p.parse_args(argv)

    # avant tout : scripts en minuscules, # remplace par _ dans les fichiers
    if run("normalize.py"):
        return 1
    if not args.path.exists():
        # le script vient peut-etre d'etre renomme en minuscules
        args.path = args.path.with_name(args.path.name.lower())
    scripts = find_scripts(args.path)
    if not scripts:
        print(
            f"Aucun script Qlik dans {args.path} : on prend les fichiers "
            ".qvs, .txt ou sans extension."
        )
        return 1
    qlik_dir = Path("data") / "qlik_outputs"
    compare = qlik_dir.is_dir() and any(qlik_dir.iterdir())

    # On recense tous les scripts du dossier, meme si on n'en migre qu'un :
    # quand un script lit un fichier ecrit par un autre, il doit le lire dans
    # data/output.
    folder = args.path if args.path.is_dir() else args.path.parent
    run("prepare.py", "--collect", *map(str, find_scripts(folder)))
    # On traite les scripts dans l'ordre de leurs dependances, comme le job.
    if STORES.exists():
        rank = {
            s: i
            for i, (s, _) in enumerate(
                order_scripts(json.loads(STORES.read_text(encoding="utf-8")))[0]
            )
        }
        scripts.sort(key=lambda s: rank.get(s.name, len(rank)))
    # On complete conf/io.yml et les schemas avant de traduire, puisque la
    # traduction s'en sert (colonnes des LOAD *, types des dates). Seulement
    # des ajouts : ce qui est deja declare n'est jamais modifie.
    conf_state = "non touchee (--no-conf)"
    if not args.no_conf:
        if not Path("conf").is_dir():
            # Tous les chemins du kit partent du dossier courant : lance
            # ailleurs qu'a la racine du projet, il ecrirait un conf/ et un
            # src/ a cote des vrais.
            print(
                f"[!] pas de dossier conf/ dans {Path.cwd()} : migrate.py se "
                "lance depuis la racine du projet. Un conf/io.yml va etre "
                "cree ici."
            )
        rc = run("fill_conf.py", str(folder), *[f"--var={v}" for v in args.var])
        conf_state = (
            f"completee ({Path('conf/io.yml').resolve()})"
            if rc == 0
            else "PAS completee, voir le message de fill_conf.py plus haut"
        )

    summary: list[tuple[str, str, list[str]]] = []
    assembled: list[tuple[int, Path]] = []  # scripts a comparer avec Qlik
    stopped = False
    for script in scripts:
        print(f"\n{'#' * 70}\n# {script.name}\n{'#' * 70}")
        if run("prepare.py", str(script)):
            summary.append((script.name, "echec de la preparation", []))
            continue
        workdir = WORK / script.stem
        extra = ["--opencode-bin", args.opencode_bin, "--jobs", str(args.jobs)]
        if args.skip_selftest:
            extra.append("--skip-selftest")
        if args.opencode_in_box:
            extra.append("--opencode-in-box")
        if args.model:
            extra += ["--model", args.model]
        if args.pilot:
            extra += ["--limit", str(args.pilot)]
        if run("translate.py", str(workdir), *extra):
            # Le modele ne repond pas, pas la peine d'enchainer sur les scripts
            # suivants. On regenere quand meme le job en sortant de la boucle.
            summary.append(
                (
                    script.name,
                    "traduction arretee : le modele ne "
                    "repond pas (voir le test ci-dessus)",
                    [],
                )
            )
            stopped = True
            break

        # Spark parse toutes les requetes avant l'assemblage. Une traduction
        # bancale se voit ici, et pas au premier lancement du job.
        run("check.py", str(workdir), "--parse", "--mark")
        manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
        retry = [s["id"] for s in manifest["sections"] if s.get("auto_retry")]
        if retry:
            # Tables completees par le modele que le controle refuse : une
            # nouvelle traduction, une seule fois, avec l'erreur de Spark.
            print(
                f"\n{len(retry)} table(s) refusee(s) par le controle, "
                f"retraduite(s) une fois : {', '.join(retry)}"
            )
            run("translate.py", str(workdir), "--auto-retry", *extra)
            run("check.py", str(workdir), "--parse", "--mark")
            manifest = json.loads(
                (workdir / "manifest.json").read_text(encoding="utf-8")
            )
        todo = sum(s["status"] == "todo" for s in manifest["sections"])
        to_fix = [
            s["id"]
            for s in manifest["sections"]
            if s["status"] in ("escalade", "erreur_modele")
        ]
        if todo or to_fix:
            # Pas assemble : une ancienne version du script ne doit pas
            # rester dans le job sans qu'on le sache.
            was_in_job = (workdir / "assembly.json").exists()
            (workdir / "assembly.json").unlink(missing_ok=True)
            summary.append(
                (
                    script.name,
                    f"{todo} a traduire, {len(to_fix)} a corriger a la main"
                    + (", retire du job jusqu'a correction" if was_in_job else ""),
                    to_fix,
                )
            )
        elif run("assemble.py", str(workdir)):
            summary.append(
                (
                    script.name,
                    "non assemble : entrees ou sorties "
                    "absentes de conf/io.yml (voir ci-dessus)",
                    [],
                )
            )
        else:
            run("orchestrate.py")  # ajoute ce script au job
            assembled.append((len(summary), workdir))
            summary.append((script.name, "assemble dans src/", []))
        if args.pilot:
            break

    print()
    run("orchestrate.py")  # regenere l'etape 010 (et cree le job)
    if compare and assembled:
        compare_all(assembled, summary, args.var)

    print(f"\n{'=' * 70}\nRESUME")
    print(f"Conf : {conf_state}")
    for name, state, to_fix in summary:
        print(f"- {name} : {state}")
        for sid in to_fix[:15]:
            print(f"    a corriger : {WORK / Path(name).stem / sid}/query.sql")
    ready = sum("assemble" in state for _, state, _ in summary)
    print(
        f"\nJob du projet : {job_path().resolve()} "
        f"({'contient les scripts assembles' if ready else 'aucun script assemble pour le moment'})"
    )
    print(f"Journal : {WORK / 'journal.csv'}")
    if not compare:
        print("Pas de dossier data/qlik_outputs/ : comparaison des donnees non faite.")
    if args.pilot:
        print(
            "Pilote termine : relis les query.sql, puis relance sans --pilot "
            "(le script n'est assemble, et ajoute au job, qu'une fois "
            "toutes ses tables validees)."
        )
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
