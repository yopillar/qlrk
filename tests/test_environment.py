"""Environnement : opencode sur l'hote depuis la distrobox, et arret net
quand pyspark ou pyarrow manquent."""

import argparse
import shutil
import sys
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
    monkeypatch.setattr(translate, "host_env_names", lambda: ["ORNITH_API_KEY"])
    assert translate.opencode_command(options()) == [
        "host-spawn",
        "-env=TERM,ORNITH_API_KEY",
        "opencode",
    ]
    assert translate.opencode_command(options(opencode_in_box=True)) == ["opencode"]


def test_host_env_names(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """On transmet les variables lues par la conf d'opencode et celles d'un
    modele, si elles sont definies ; jamais les autres."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "opencode.json").write_text(
        '{"provider": {"o": {"options": {"baseURL": "{env:ORNITH_URL}"}}}}',
        encoding="utf-8",
    )
    monkeypatch.setenv("ORNITH_URL", "http://serveur")
    monkeypatch.setenv("MISTRAL_API_KEY", "x")
    monkeypatch.setenv("SANS_RAPPORT", "y")
    monkeypatch.delenv("ABSENTE", raising=False)
    names = translate.host_env_names()
    assert "ORNITH_URL" in names and "MISTRAL_API_KEY" in names
    assert "SANS_RAPPORT" not in names


def test_opencode_direct_outside_distrobox(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hors distrobox, la commande ne change pas."""
    monkeypatch.delenv("CONTAINER_ID", raising=False)
    monkeypatch.setattr(translate, "in_distrobox", lambda: False)
    assert translate.opencode_command(options(opencode_bin="oc")) == ["oc"]


def hide(monkeypatch: pytest.MonkeyPatch, *modules: str) -> None:
    """Fait comme si ces modules n'etaient pas installes."""
    for name in modules:
        monkeypatch.setitem(sys.modules, name, None)


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
