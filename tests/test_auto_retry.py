"""Retraduction automatique d'une table que le controle refuse."""

import json
import sys
from pathlib import Path

import pytest

from conf import query_sha
from conftest import Project
from prepare import fill_holes

pytestmark = pytest.mark.spark

SCRIPT = (
    "Src:\nLOAD * INLINE [\nCode, Liste\na1, 1\n];\n"
    "T:\nNoConcatenate LOAD Code RESIDENT Src WHERE PurgeChar(Code, Liste) = 'a';\n"
    "STORE T INTO [lib://out/t.qvd] (qvd);\n"
)
# la reponse acceptee autrefois sans controle : la fonction n'existe pas
WRONG = "purgechar(Code, Liste) = 'a'"
RIGHT = "translate(Code, Liste, '') = 'a'"


def fake_opencode(tmp_path: Path) -> Path:
    """Un faux opencode qui repond toujours la bonne expression pour h1."""
    answer = json.dumps(
        {"type": "text", "part": {"type": "text", "text": f"h1: {RIGHT}"}}
    )
    path = tmp_path / "opencode"
    path.write_text(
        f"#!{sys.executable}\nprint({answer!r})\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def accepted_without_check(project: Project) -> tuple[Path, dict]:
    """Prepare le script, puis fait comme si la reponse fausse du modele
    avait ete acceptee sans controle Spark, comme hors distrobox.

    Returns:
        Le manifeste et la table T.
    """
    (project / "script_qvd/codes.qvs").write_text(SCRIPT, encoding="utf-8")
    project.run("prepare.py", "script_qvd/codes.qvs")
    mpath = project / "work/codes/manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    sec = next(s for s in manifest["sections"] if s["table"] == "T")
    d = project / "work/codes" / sec["id"]
    draft = (d / "draft.sql").read_text(encoding="utf-8")
    (d / "query.sql").write_text(fill_holes(draft, {"h1": WRONG}), encoding="utf-8")
    key = "condition||" + "PurgeChar(Code, Liste) = 'a'"
    sec.update(
        status="ok",
        query_sha=query_sha(d / "query.sql"),
        hole_answers={key: WRONG},
    )
    mpath.write_text(json.dumps(manifest), encoding="utf-8")
    return mpath, sec


def table(mpath: Path, sid: str) -> dict:
    """La table sid du manifeste."""
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    return next(s for s in manifest["sections"] if s["id"] == sid)


def test_refused_table_is_retranslated_once(project: Project, tmp_path: Path) -> None:
    """Refusee par le controle, la table repart de son brouillon et le
    modele la complete de nouveau ; elle repasse ok."""
    mpath, sec = accepted_without_check(project)
    project.run("check.py", "work/codes", "--parse", "--mark", check=False)
    refused = table(mpath, sec["id"])
    assert refused["status"] == "escalade" and refused["auto_retry"]

    project.run(
        "translate.py",
        "work/codes",
        "--auto-retry",
        "--skip-selftest",
        "--opencode-bin",
        str(fake_opencode(tmp_path)),
    )
    done = table(mpath, sec["id"])
    # reussie : un refus plus tard aura de nouveau droit a son tour
    assert done["status"] == "ok"
    assert "auto_retry" not in done and "auto_retried" not in done
    query = (project / "work/codes" / sec["id"] / "query.sql").read_text(
        encoding="utf-8"
    )
    assert RIGHT in query and WRONG not in query


def test_hand_edited_query_is_never_retranslated(project: Project) -> None:
    """Une requete retouchee a la main reste en escalade : on ne l'ecrase
    pas."""
    mpath, sec = accepted_without_check(project)
    query = project / "work/codes" / sec["id"] / "query.sql"
    query.write_text(query.read_text(encoding="utf-8") + "\n-- corrige\n")
    project.run("check.py", "work/codes", "--parse", "--mark", check=False)
    refused = table(mpath, sec["id"])
    assert refused["status"] == "escalade" and "auto_retry" not in refused
