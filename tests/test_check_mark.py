"""Controle avant assemblage : colonnes sures et cache des reponses."""

import json
from pathlib import Path

import pytest

import check


def project(tmp_path: Path, sure: bool, hole_answers: dict) -> Path:
    """Un script d'une table qui lit une colonne absente de son entree.

    Args:
        tmp_path: le dossier du test.
        sure: si les colonnes de l'entree sont sures.
        hole_answers: les reponses du modele gardees par la table.

    Returns:
        Le dossier de travail du script (work/s).
    """
    workdir = tmp_path / "work" / "s"
    (workdir / "t1").mkdir(parents=True)
    (workdir / "t1" / "query.sql").write_text(
        "-- l.1 : T\nSELECT absente, _qlik_order FROM in_a\n", encoding="utf-8"
    )
    section = {
        "id": "t1",
        "table": "T",
        "status": "ok",
        "statement_lines": [1],
        "views": {"in_a": {"columns": ["x"], "columns_sure": sure}},
        "hole_answers": hole_answers,
    }
    (workdir / "manifest.json").write_text(
        json.dumps({"script": "s", "sections": [section]}), encoding="utf-8"
    )
    return workdir


def status(workdir: Path) -> str:
    """Statut de la table apres le controle."""
    manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
    return str(manifest["sections"][0]["status"])


def test_columns_sure_needs_every_view() -> None:
    """Une seule vue deduite d'un autre script suffit a ne plus etre sur."""
    sure = {"columns": ["x"], "columns_sure": True}
    guessed = {"table": "T0", "columns": ["x"]}
    assert check.columns_sure({"views": {"in_a": sure}})
    assert not check.columns_sure({"views": {"in_a": sure, "t0": guessed}})
    assert not check.columns_sure({"views": {}})


def test_only_unverified_answers_are_dropped(tmp_path: Path) -> None:
    """Une reponse confirmee par une comparaison avec Qlik reste en cache."""
    cache = tmp_path / "expressions.json"
    cache.write_text(
        json.dumps(
            {
                "where||a": {"sql": "faux", "verified": False},
                "value||b": {"sql": "juste", "verified": True},
                "value||c": {"sql": "autre table", "verified": False},
            }
        ),
        encoding="utf-8",
    )
    section = {"hole_answers": {"where||a": "faux", "value||b": "juste"}}
    assert check.cached_answers_dropped(cache, section) == 1
    assert set(json.loads(cache.read_text(encoding="utf-8"))) == {
        "value||b",
        "value||c",
    }


@pytest.mark.spark
def test_missing_column_blocks_when_columns_are_sure(tmp_path: Path) -> None:
    """Colonnes sures : la colonne introuvable passe la table en escalade,
    et sa reponse fausse quitte le cache."""
    workdir = project(tmp_path, sure=True, hole_answers={"where||a": "faux"})
    cache = tmp_path / "work" / "expressions.json"
    cache.write_text(
        json.dumps({"where||a": {"sql": "faux", "verified": False}}),
        encoding="utf-8",
    )
    check.main([str(workdir), "--parse", "--mark"])
    assert status(workdir) == "escalade"
    assert json.loads(cache.read_text(encoding="utf-8")) == {}


@pytest.mark.spark
def test_missing_column_only_warns_when_guessed(tmp_path: Path) -> None:
    """Colonnes deduites du script : on signale sans bloquer."""
    workdir = project(tmp_path, sure=False, hole_answers={})
    check.main([str(workdir), "--parse", "--mark"])
    assert status(workdir) == "ok"
