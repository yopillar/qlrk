"""Coherence entre les outils : ce que l'un ecrit, l'autre le retrouve."""

import json
import os
import time
from pathlib import Path

import pytest

import assemble
import check
from conftest import Project
from verify import written_since


def test_spark_message_skips_blank_first_line() -> None:
    """Le message de Spark commence parfois par une ligne vide."""
    error = Exception("\n\n[PARSE_SYNTAX_ERROR] near '>'\n== SQL ==")
    assert check.spark_message(error) == "[PARSE_SYNTAX_ERROR] near '>'"
    assert check.spark_message(Exception("")) == ""


@pytest.mark.spark
def test_unanalysed_table_is_reported(tmp_path: Path) -> None:
    """Sans les colonnes d'une vue, Spark n'analyse pas : on le dit, au lieu
    de laisser croire que la table est controlee."""
    query = tmp_path / "query.sql"
    query.write_text("-- l.1 : T\nSELECT x, _qlik_order FROM in_a\n", encoding="utf-8")
    section = {"views": {"in_a": {"columns": None}}, "statement_lines": [1]}
    errors, warnings = check.check_section(section, query, check.get_parser())
    assert not errors
    assert any(w.startswith("non analysee par Spark") and "in_a" in w for w in warnings)


def test_prepare_keeps_spark_types_of_an_unchanged_query(project: Project) -> None:
    """Les types calcules par Spark survivent a un nouveau passage de
    prepare.py tant que la requete ne change pas : le script qui relit la
    sortie ne repasse pas en STRING."""
    project.run("prepare.py", "script_qvd/ventes.qvs")
    mpath = project / "work/ventes/manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    sec = next(s for s in manifest["sections"] if s["status"] == "deterministic")
    sec["output_types"], sec["output_types_sure"] = {"Id": "BIGINT"}, True
    mpath.write_text(json.dumps(manifest), encoding="utf-8")

    project.run("prepare.py", "script_qvd/ventes.qvs")
    again = json.loads(mpath.read_text(encoding="utf-8"))
    kept = next(s for s in again["sections"] if s["id"] == sec["id"])
    assert kept["output_types"] == {"Id": "BIGINT"} and kept["output_types_sure"]


def test_stale_output_is_not_compared(tmp_path: Path) -> None:
    """Une sortie que le job n'a pas reecrite date d'un lancement precedent."""
    out = tmp_path / "sortie"
    out.mkdir()
    part = out / "part-0.parquet"
    part.write_text("x", encoding="utf-8")
    old = time.time() - 3600
    os.utime(part, (old, old))
    assert not written_since(out, time.time() - 60)
    part.write_text("y", encoding="utf-8")
    assert written_since(out, time.time() - 60)


def test_unvalidated_script_leaves_the_job(tmp_path: Path) -> None:
    """Un script qui a des tables non validees sort du job : son ancien
    assembly.json ne doit pas y garder une version perimee."""
    workdir = tmp_path / "work" / "s"
    workdir.mkdir(parents=True)
    (workdir / "manifest.json").write_text(
        json.dumps(
            {"script": "s.qvs", "sections": [{"id": "t1", "status": "escalade"}]}
        ),
        encoding="utf-8",
    )
    (workdir / "assembly.json").write_text("{}", encoding="utf-8")
    assert assemble.main([str(workdir)]) == 1
    assert not (workdir / "assembly.json").exists()


@pytest.mark.spark
def test_missing_column_same_rule_everywhere(tmp_path: Path) -> None:
    """Colonne introuvable : avertissement si les colonnes amont sont
    deduites, erreur si elles sont sures. check_section le decide, pour la
    traduction comme pour le controle de migrate.py."""
    query = tmp_path / "query.sql"
    query.write_text(
        "-- l.1 : T\nSELECT absente, _qlik_order FROM in_a\n", encoding="utf-8"
    )
    parser = check.get_parser()
    for sure in (False, True):
        section = {
            "views": {"in_a": {"columns": ["x"], "columns_sure": sure}},
            "statement_lines": [1],
        }
        errors, warnings = check.check_section(section, query, parser)
        if sure:
            assert any("UNRESOLVED_COLUMN" in e for e in errors)
        else:
            assert not errors
            assert any("non bloquant" in w for w in warnings)
