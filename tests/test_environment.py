"""Environnement : opencode sur l'hote depuis la distrobox, et arret net
quand pyspark ou pyarrow manquent."""

import argparse
import shutil
from pathlib import Path

import pytest

import check
import fill_conf
import translate


def options(**kwargs: object) -> argparse.Namespace:
    """Options de translate.py utiles a la commande opencode."""
    base: dict[str, object] = {"opencode_bin": "opencode", "opencode_in_box": False}
    return argparse.Namespace(**{**base, **kwargs})


def test_opencode_runs_on_host_from_distrobox(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dans une distrobox, opencode passe par host-spawn, sauf si on dit
    qu'il est installe dans la box."""
    monkeypatch.setenv("CONTAINER_ID", "box")
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")
    assert translate.opencode_command(options()) == ["host-spawn", "opencode"]
    assert translate.opencode_command(options(opencode_in_box=True)) == ["opencode"]


def test_opencode_direct_outside_distrobox(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hors distrobox, la commande ne change pas."""
    monkeypatch.delenv("CONTAINER_ID", raising=False)
    monkeypatch.setattr(translate, "in_distrobox", lambda: False)
    assert translate.opencode_command(options(opencode_bin="oc")) == ["oc"]


def hide(monkeypatch: pytest.MonkeyPatch, *modules: str) -> None:
    """Fait comme si ces modules n'etaient pas installes."""
    for name in modules:
        monkeypatch.setitem(__import__("sys").modules, name, None)


def test_no_spark_stops_the_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sans pyspark, le controle s'arrete au lieu de laisser les types en
    STRING."""
    hide(monkeypatch, "pyspark", "pyspark.sql")
    with pytest.raises(SystemExit, match="pyspark absent.*distrobox"):
        check.get_parser()


def test_no_library_stops_parquet_types(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Sans pyarrow ni pyspark, les types d'un Parquet ne sont pas devines."""
    hide(monkeypatch, "pyarrow", "pyarrow.parquet", "pyspark", "pyspark.sql")
    with pytest.raises(SystemExit, match="pyarrow et pyspark absent"):
        fill_conf.parquet_types(tmp_path / "t.parquet")
