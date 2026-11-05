"""Completion de conf/io.yml et des schemas par fill_conf.py."""

import datetime as dt
import decimal
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from check import spark_type
from conftest import Project
from fill_conf import csv_types, parquet_types

SCRIPTS = ("script_qvd/calendrier", "script_qvd/synthese.txt", "script_qvd/ventes.qvs")


def fill(project: Project) -> subprocess.CompletedProcess[str]:
    """Recense les scripts de la demo et complete conf/."""
    project.run("normalize.py")
    project.run("prepare.py", "--collect", *SCRIPTS)
    return project.run("fill_conf.py", "script_qvd/")


def test_fill_conf_adds_without_touching_existing(project: Project) -> None:
    """fill_conf ajoute ce qui manque sans modifier l'existant, et relancer n'ajoute rien."""
    before = (project / "conf/io.yml").read_text(encoding="utf-8")
    fill(project)
    after = (project / "conf/io.yml").read_text(encoding="utf-8")

    # l'existant est garde tel quel, commentaires compris
    assert after.startswith(before.split("outputs:")[0].rstrip("\n"))
    assert (project / "conf/io.yml.bak").read_text(encoding="utf-8") == before
    conf = yaml.safe_load(after)
    # sections au pluriel, comme le socle les attend
    assert set(conf) == {"inputs", "outputs"}
    # ajouts a la meme indentation (3 espaces) que le fichier
    assert "\n   ref_sites_2024:" in after

    # relancer n'ajoute rien
    fill(project)
    assert (project / "conf/io.yml").read_text(encoding="utf-8") == after


def test_hash_becomes_underscore(project: Project) -> None:
    """Le # des noms de fichiers devient _ ; aucune sortie ne garde de #."""
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # [ref]-sites#2024.csv : renomme (ref)-sites_2024.csv, lu en csv sous ce nom
    sites = conf["inputs"]["ref_sites_2024"]
    assert sites["location"] == "${entrees}/(ref)-sites_2024.csv"
    assert sites["format"] == "csv"
    # ventes_enrichies#v2.qvd : sortie ventes_enrichies_v2
    assert (
        conf["outputs"]["ventes_enrichies_v2"]["location"]
        == "${sorties}/ventes_enrichies_v2"
    )
    assert not any("#" in k for s in conf.values() for k in s)
    assert not any("#" in v["location"] for v in conf["outputs"].values())


def test_csv_options_declared(project: Project) -> None:
    """Les options CSV de la specification Qlik vont dans la cle options."""
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # options du lecteur CSV de Spark, d'apres la specification Qlik
    assert conf["inputs"]["ventes"]["format"] == "csv"
    sites = conf["inputs"]["ref_sites_2024"]
    assert sites["options"] == {
        "header": True,
        "mode": "PERMISSIVE",
        "encoding": "UTF-8",
        "sep": ",",
    }


NO_LABELS = (
    "Codes:\n"
    "LOAD @1 as Code, Upper(@3) as Libelle\n"
    "FROM [lib://data/codes.csv] (txt, no labels, delimiter is ';');\n"
    "STORE Codes INTO [lib://out/codes_sortie.qvd] (qvd);\n"
)


def test_csv_without_header_file_absent(project: Project) -> None:
    """Sans le fichier, un CSV sans en-tete a toutes ses colonnes _c0... dans l'ordre."""
    # sans le fichier : colonnes _c0 a _c2, toutes, dans l'ordre (Spark les
    # affecte par position), meme si le script ne lit pas @2
    (project / "script_qvd/codes.qvs").write_text(NO_LABELS)
    project.run("prepare.py", "--collect", "script_qvd/codes.qvs")
    project.run("fill_conf.py", "script_qvd/codes.qvs")
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    assert conf["inputs"]["codes"]["options"] == {
        "header": False,
        "mode": "PERMISSIVE",
        "sep": ";",
    }
    schema = yaml.safe_load(
        (project / "conf/schemas/codes.yml").read_text(encoding="utf-8")
    )
    assert list(schema["columns"]) == ["_c0", "_c1", "_c2"]


def test_csv_without_header_file_present(project: Project) -> None:
    """Avec le fichier, un CSV sans en-tete prend toutes ses colonnes et leurs types."""
    # avec le fichier : toutes ses colonnes, la 1re ligne est une donnee
    (project / "script_qvd/codes.qvs").write_text(NO_LABELS)
    (project / "data/input/codes.csv").write_text("1;x;abc;9\n2;y;def;8\n")
    project.run("prepare.py", "--collect", "script_qvd/codes.qvs")
    project.run("fill_conf.py", "script_qvd/codes.qvs")
    schema = yaml.safe_load(
        (project / "conf/schemas/codes.yml").read_text(encoding="utf-8")
    )
    assert schema["columns"] == {
        "_c0": "BIGINT",
        "_c1": "STRING",
        "_c2": "STRING",
        "_c3": "BIGINT",
    }


def test_model_tables_become_outputs(project: Project) -> None:
    """Les tables encore la a la fin du script deviennent des sorties."""
    # Dates reste en memoire a la fin du calendrier, sans STORE : c'est le
    # modele de donnees de l'app, il doit etre ecrit
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # cle en minuscules, location au nom de la table dans le script Qlik
    assert conf["outputs"]["dates"]["location"] == "${sorties}/Dates"
    # pas les tables supprimees, ni celles qui ont un STORE
    assert "bornes" not in conf["outputs"]
    assert "ventes" not in conf["outputs"]
    # toutes les cles des sorties sont en minuscules
    assert all(k == k.lower() for k in conf["outputs"])


def test_model_table_name_taken_by_another_script(project: Project) -> None:
    """Deux scripts avec une table du meme nom gardent chacun leur sortie."""
    # deux scripts laissent une table Dates : chacune garde sa sortie
    (project / "script_qvd/autre.qvs").write_text(
        "Dates:\nLOAD Jour FROM [lib://src/jours.qvd] (qvd);\n"
    )
    project.run("normalize.py")
    project.run("prepare.py", "--collect", *SCRIPTS, "script_qvd/autre.qvs")
    project.run("fill_conf.py", "script_qvd/")
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    assert {"calendrier_dates", "autre_dates"} <= set(conf["outputs"])
    assert "dates" not in conf["outputs"]


def test_reread_output_declared_with_from_output(project: Project) -> None:
    """Un fichier ecrit par un script et relu par un autre est une sortie,
    et en entree un simple from_output, sous un autre nom."""
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # synthese relit ce que ventes ecrit
    assert conf["outputs"]["ventes_enrichies_v2"] == {
        "location": "${sorties}/ventes_enrichies_v2"
    }
    assert conf["inputs"]["ventes_enrichies_v2_relue"] == {
        "from_output": "ventes_enrichies_v2"
    }
    # pas d'entree du meme nom que la sortie, et pas de schema a ecrire
    assert "ventes_enrichies_v2" not in conf["inputs"]
    assert not (project / "conf/schemas/ventes_enrichies_v2.yml").exists()


@pytest.mark.spark
def test_reread_output_gets_writer_types(project: Project) -> None:
    """La requete qui lit une sortie est controlee avec les types de la
    requete Spark qui l'ecrit."""
    fill(project)
    project.run("prepare.py", "script_qvd/ventes.qvs")
    project.run("check.py", "work/ventes", "--parse")
    project.run("prepare.py", "script_qvd/synthese.txt")
    manifest = yaml.safe_load(
        (project / "work/synthese/manifest.json").read_text(encoding="utf-8")
    )
    view = next(
        v
        for sec in manifest["sections"]
        for name, v in sec.get("views", {}).items()
        if name == "in_ventes_enrichies_v2_relue"
    )
    assert view["io"] == "ventes_enrichies_v2_relue"
    assert view["types"]["MontantTTC"] == "DOUBLE"


def test_excel_input_declared_with_its_sheets(project: Project) -> None:
    """Un fichier Excel garde son nom et recoit l'option sheets."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    pd.DataFrame({"Code": [1, 2], "Libelle": ["a", "b"]}).to_excel(
        project / "data/input/tarifs.xlsx", sheet_name="Prix", index=False
    )
    (project / "script_qvd/tarifs.qvs").write_text(
        "Tarifs:\nLOAD Code, Libelle\n"
        "FROM [lib://in/tarifs.xlsx] (ooxml, embedded labels, table is Prix);\n"
        "Tout:\nLOAD Code FROM [lib://in/tarifs.xlsx] (ooxml, embedded labels);\n"
        "STORE Tarifs INTO [lib://out/tarifs_sortie.qvd] (qvd);\n"
    )
    project.run("prepare.py", "--collect", "script_qvd/tarifs.qvs")
    project.run("fill_conf.py", "script_qvd/tarifs.qvs")
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    entry = conf["inputs"]["tarifs"]
    assert entry["format"] == "xlsx"
    assert entry["location"] == "${entrees}/tarifs.xlsx"
    assert entry["options"] == {"sheets": "Prix"}
    schema = yaml.safe_load(
        (project / "conf/schemas/tarifs.yml").read_text(encoding="utf-8")
    )
    assert list(schema["columns"]) == ["Code", "Libelle"]


def test_types_from_script_comparisons_and_shared_fields(project: Project) -> None:
    """Les types se devinent des comparaisons du script et des champs du meme nom."""
    (project / "script_qvd/c.qvs").write_text(
        "C:\nLOAD Statut, Qte, DateCde, Site FROM [lib://src/c.qvd] (qvd)\n"
        "WHERE Statut = 'OK' and Qte > 0 and DateCde >= '01/01/2024';\n"
    )
    project.run("prepare.py", "--collect", *SCRIPTS, "script_qvd/c.qvs")
    project.run("fill_conf.py", "script_qvd/")
    text = (project / "conf/schemas/c.yml").read_text(encoding="utf-8")
    assert "Statut: STRING" in text and "compare a un texte" in text
    assert "Qte: DOUBLE" in text and "DateCde: DATE" in text
    # Site n'a aucun indice ici, mais c'est le meme champ que dans ventes
    assert "Site: STRING" in text and "meme champ que dans" in text


def test_types_from_csv_values(project: Project) -> None:
    """Les types d'un CSV se deduisent de ses valeurs."""
    fill(project)
    text = (project / "conf/schemas/ref_sites_2024.yml").read_text(encoding="utf-8")
    assert "Site: STRING" in text and "deduit des 3 ligne(s)" in text


def test_types_from_parquet_metadata(project: Project) -> None:
    """Les types d'un Parquet se lisent dans ses metadonnees."""
    import pandas as pd

    (project / "script_qvd/produits.qvs").write_text(
        "P:\nLOAD * FROM [lib://src/produits.qvd] (qvd);\n"
        "STORE P INTO [lib://qvd/produits.qvd] (qvd);\n"
    )
    (project / "data/input/produits").mkdir()
    pd.DataFrame(
        {
            "Code": ["a"],
            "Prix": [1.5],
            "Stock": [3],
            "Jour": pd.to_datetime(["2024-12-29"]).date,
        }
    ).to_parquet(project / "data/input/produits/part-0.parquet")
    project.run("prepare.py", "--collect", *SCRIPTS, "script_qvd/produits.qvs")
    project.run("fill_conf.py", "script_qvd/")
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # un QVD est lu en Parquet, sous le meme nom, depuis sa source (et pas
    # depuis la sortie du meme nom ecrite par ce script)
    assert conf["inputs"]["produits"] == {
        "format": "parquet",
        "schema_file": "schemas/produits.yml",
        "location": "${entrees}/produits",
    }
    schema = yaml.safe_load(
        (project / "conf/schemas/produits.yml").read_text(encoding="utf-8")
    )
    assert schema["columns"] == {
        "Code": "STRING",
        "Prix": "DOUBLE",
        "Stock": "BIGINT",
        "Jour": "DATE",
    }


def test_unknown_location_variable_is_reported(project: Project) -> None:
    """Une variable de location inconnue est signalee."""
    (project / "conf/io.yml").write_text(
        "inputs:\n  dummy:\n    location: ${lac}/dummy\n", encoding="utf-8"
    )
    (project / "script_qvd/p.qvs").write_text(
        "P:\nLOAD a FROM [lib://src/p.qvd] (qvd);\n"
    )
    project.run("prepare.py", "--collect", "script_qvd/p.qvs")
    res = project.run("fill_conf.py", "script_qvd/p.qvs")
    assert "${lac} inconnue(s)" in res.stdout


def test_no_function_name_taken_for_a_column() -> None:
    """Un nom de fonction n'est jamais pris pour une colonne."""
    from fill_conf import type_hints

    hints = type_hints(
        "LOAD Sum(Montant) / Count(Ligne) as M, x * PurgeChar(Ref, 'a') as P FROM t;"
    )
    assert "Count" not in hints and "PurgeChar" not in hints
    assert "x" in hints


def test_csv_types_read_the_whole_file(project: Project) -> None:
    """Une seule valeur texte, meme loin dans le fichier, met la colonne en string."""
    # une seule valeur texte, loin dans le fichier, met la colonne en string
    lines = "".join(f"{k}\n" for k in range(6000)) + "N/A\n"
    (project / "data/input/gros.csv").write_text("Num\n" + lines)
    (project / "script_qvd/gros.qvs").write_text(
        "G: LOAD Num FROM [lib://data/gros.csv] (txt, embedded labels);\n"
    )
    project.run("prepare.py", "--collect", "script_qvd/gros.qvs")
    project.run("fill_conf.py", "script_qvd/gros.qvs")
    schema = yaml.safe_load(
        (project / "conf/schemas/gros.yml").read_text(encoding="utf-8")
    )
    assert schema["columns"] == {"Num": "STRING"}


@pytest.mark.spark
def test_parquet_types_without_pyarrow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sans pyarrow, les types du Parquet sont lus par Spark, pas devines."""
    pd = pytest.importorskip("pandas")
    path = tmp_path / "stock.parquet"
    pd.DataFrame({"Site": ["A"], "Qte": [3]}).to_parquet(path)
    # un module a None dans sys.modules fait echouer son import
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", None)
    assert parquet_types(path) == {"Site": "string", "Qte": "bigint"}


def test_parquet_types_keep_precision(tmp_path: Path) -> None:
    """Les types Parquet sont repris exactement : un decimal garde sa
    precision (sinon Spark le lit en decimal(10,0), sans les decimales)."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table(
        {
            "montant": pa.array([decimal.Decimal("12.34")], pa.decimal128(18, 2)),
            "qte": pa.array([1], pa.int32()),
            "id": pa.array([1], pa.uint32()),
            "taux": pa.array([1.5], pa.float16()),
            "code": pa.array([1]).dictionary_encode(),
            "libelle": pa.array(["x"]).dictionary_encode(),
            "blob": pa.array([b"x"]),
            "heure": pa.array([dt.time(1, 2)], pa.time64("us")),
        }
    )
    path = tmp_path / "t.parquet"
    pq.write_table(table, path)
    assert parquet_types(path) == {
        "montant": "decimal(18,2)",
        "qte": "int",
        "id": "bigint",
        "taux": "double",
        "code": "bigint",
        "libelle": "string",
        "blob": "binary",
        "heure": "string",
    }


def test_csv_values_give_dates_and_booleans(tmp_path: Path) -> None:
    """Dates et horodatages ISO et booleens d'un CSV sont types, pas les
    dates a la francaise ni les fausses dates."""
    path = tmp_path / "t.csv"
    path.write_text(
        "jour;moment;actif;fr;faux;mixte\n"
        "2024-01-31;2024-01-31 10:00:00;true;29/12/2024;2024-13-45;2024-01-31\n"
        "2024-02-01;2024-02-01T11:30:15;False;30/12/2024;2024-01-01;"
        "2024-02-01 08:00\n",
        encoding="utf-8",
    )
    _, types, _ = csv_types(path)
    assert types == {
        "jour": "date",
        "moment": "timestamp",
        "actif": "boolean",
        "fr": "string",
        "faux": "string",
        "mixte": "timestamp",
    }


def test_check_keeps_decimal_precision() -> None:
    """Le controle declare les vues avec la precision des decimaux."""
    assert spark_type("DECIMAL(18,2)") == "DECIMAL(18,2)"
    assert spark_type("decimal") == "DECIMAL(38,10)"
    assert spark_type("binary") == "BINARY"
