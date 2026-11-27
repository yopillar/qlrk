"""Cas releves en traduisant de vrais scripts Qlik, plus ou moins bien ecrits."""

import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

from conf import IoConf
from conftest import Project
from fill_conf import columns_read
from prepare import analyze_script, extract_fields, is_output
from qlik2sql import ExprCtx, expr


def insts_of(text: str) -> list[Any]:
    """Les LOAD d'un script, apres analyse."""
    path = Path(tempfile.mkdtemp()) / "s.qvs"
    path.write_text(text, encoding="utf-8")
    return [i for i in analyze_script(path)[2] if i.kind == "load"]


def test_loop_with_same_label_appends_rows() -> None:
    """Une boucle FOR qui recharge "Stocks:" avec les memes champs ajoute ses
    lignes a Stocks ; elle ne la remplace pas."""
    loads = insts_of(
        "FOR EACH v IN 'a','b'\nStocks:\nLOAD x, y FROM [lib://d/$(v).csv] (txt);\n"
        "NEXT v\nSTORE Stocks INTO [lib://o/s.qvd] (qvd);\n"
    )
    assert "concatenate" in loads[1].constructs and loads[1].target == "Stocks"


def test_same_label_other_fields_is_a_new_table() -> None:
    """Meme etiquette, autres champs : Qlik cree A-1, A reste la premiere."""
    loads = insts_of(
        "A:\nLOAD x, y FROM [lib://d/a.csv] (txt);\n"
        "A:\nLOAD x, z FROM [lib://d/b.csv] (txt);\n"
    )
    assert loads[1].target == "A-1" and "concatenate" not in loads[1].constructs


def test_label_after_prefix_is_kept() -> None:
    """ "LEFT KEEP (E) P: LOAD" : l'etiquette n'est pas perdue (la table se
    fondait dans E), et l'ecriture est signalee."""
    loads = insts_of(
        "E:\nLOAD m, s FROM [lib://d/e.csv] (txt);\n"
        "LEFT KEEP (E)\nP:\nLOAD m, p FROM [lib://d/p.csv] (txt);\n"
    )
    assert loads[1].target == "P" and loads[1].keep_other == "E"
    assert any("apres le prefixe" in w for w in loads[1].warnings)


def test_applymap_before_its_mapping_is_reported() -> None:
    """Qlik s'arrete sur un mapping utilise avant d'etre charge."""
    loads = insts_of(
        "T:\nLOAD k, ApplyMap('M', k, 0) AS v FROM [lib://d/t.csv] (txt);\n"
        "M:\nMAPPING LOAD k, v FROM [lib://d/m.csv] (txt);\n"
    )
    assert any("avant le chargement de ce mapping" in w for w in loads[0].warnings)


def test_stacked_load_star_takes_fields_from_below() -> None:
    """ "LOAD *, x; LOAD a, b FROM" : les champs sont connus (cle de jointure,
    colonnes ecrites, types). L'en-tete d'un INLINE est sa premiere ligne."""
    assert extract_fields(
        "LOAD *, Year(d) AS An;\nLOAD a, d FROM [lib://x/c.csv];"
    ) == [
        "a",
        "d",
        "An",
    ]
    raw = "T:\nLOAD * INLINE [\nId, Nom\n1, a\n2, b\n];"
    assert extract_fields(" ".join(raw.split()), raw) == ["Id", "Nom"]


def test_raw_csv_is_not_the_output_of_the_same_name() -> None:
    """commandes.csv, la source brute, n'est pas commandes.qvd, ecrit par un
    autre script ; un QVD du meme nom a un autre chemin l'est."""
    stored = {"paths": {"lib://out/commandes.qvd"}, "names": {"commandes": "a.qvs"}}
    assert not is_output("lib://data/commandes.csv", stored)
    assert is_output("lib://qvd/commandes.qvd", stored)


def test_find_prefers_the_reread_for_a_produced_file(tmp_path: Path) -> None:
    """Deux entrees repondent a "commandes" : la relecture de la sortie pour
    le fichier produit, le CSV brut pour la source."""
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "io.yml").write_text(
        "inputs:\n"
        "  commandes:\n    format: csv\n    location: ${entrees}/commandes.csv\n"
        "  commandes_relue:\n    from_output: commandes\n"
        "outputs:\n  commandes:\n    location: ${sorties}/commandes\n",
        encoding="utf-8",
    )
    io = IoConf(conf / "io.yml")
    produced = io.find("lib://out/commandes.qvd", "input", produced=True)
    raw = io.find("lib://data/commandes.csv", "input", produced=False)
    assert produced is not None and produced.name == "commandes_relue"
    assert raw is not None and raw.name == "commandes"


def test_exists_field_is_not_read_from_the_file() -> None:
    """Exists(champ, expr) : champ est deja charge, pas lu dans le fichier."""
    cols, _ = columns_read(
        "LOAD [Code] AS C, P FROM [lib://d/p.csv] (txt) WHERE Exists(C, [Code]);"
    )
    assert cols == ["Code", "P"]


def test_fill_conf_rereads_an_output_named_like_an_input(project: Project) -> None:
    """Un script lit commandes.csv et ecrit commandes.qvd, un autre relit
    commandes.qvd : la relecture passe par from_output, pas par le CSV."""
    (project / "data/input/commandes.csv").write_text("Id;M\n1;2\n", encoding="utf-8")
    (project / "script_qvd/a.qvs").write_text(
        "C:\nLOAD Id, M FROM [lib://data/commandes.csv] "
        "(txt, utf8, embedded labels, delimiter is ';');\n"
        "STORE C INTO [lib://out/commandes.qvd] (qvd);\n",
        encoding="utf-8",
    )
    (project / "script_qvd/b.qvs").write_text(
        "R:\nLOAD Id, M FROM [lib://out/commandes.qvd] (qvd);\n"
        "STORE R INTO [lib://out/relu.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("fill_conf.py", "script_qvd/")
    text = (project / "conf/io.yml").read_text(encoding="utf-8")
    assert "commandes_relue:" in text and "from_output: commandes" in text


def test_text_numbers_use_the_script_separators() -> None:
    """Avec DecimalSep=',', un texte "1 200,50" utilise dans un calcul est
    lu comme un nombre ; une copie ou une comparaison a un texte ne change
    pas, ni un script aux separateurs par defaut."""
    ctx = ExprCtx("", decimal_sep=",", thousand_sep=" ")
    ctx.column_types = {"M": "STRING", "S": "STRING"}
    assert "try_cast(" in expr("M * 1.2", ctx)
    assert "try_cast(" in expr("M >= 200", ctx)
    assert "try_cast(" in expr("Sum(M)", ctx)
    assert expr("M", ctx) == "M"
    assert "try_cast(" not in expr("S <> 'test'", ctx)
    plain = ExprCtx("", column_types={"M": "STRING"})
    assert "try_cast(" not in expr("M * 1.2", plain)


def run_sections(workdir: Path) -> dict[str, list[tuple]]:
    """Execute les requetes d'un script dans l'ordre, chaque table declaree
    en vue comme le fait le job.

    Returns:
        Les lignes de chaque table livree, sans _qlik_order, triees.
    """
    from check import get_parser

    spark = get_parser().spark
    manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
    out = {}
    for sec in manifest["sections"]:
        if sec["status"] == "skip":
            continue
        df = spark.sql((workdir / sec["id"] / "query.sql").read_text(encoding="utf-8"))
        df.createOrReplaceTempView(sec["view"])
        rows = df.drop("_qlik_order").collect()
        out[sec["table"]] = sorted((tuple(r) for r in rows), key=str)
    return out


@pytest.mark.spark
def test_read_then_modified_table_keeps_its_state(project: Project) -> None:
    """Un agregat lu avant un CONCATENATE, un JOIN et un DROP FIELD de sa
    source les ignore, comme dans Qlik ; la source finale les a tous."""
    (project / "script_qvd/ordre.qvs").write_text(
        "V:\nLOAD * INLINE [\nId, M, Tag\n1, 10, x\n2, 20, y\n];\n"
        "Tot:\nLOAD Sum(M) AS S, Count(Tag) AS NbTag RESIDENT V;\n"
        "CONCATENATE (V) LOAD * INLINE [\nId, M, Tag\n3, 1000, z\n];\n"
        "LEFT JOIN (V) LOAD * INLINE [\nId, Lib\n1, a\n1, b\n];\n"
        "DROP FIELD Tag FROM V;\n"
        "STORE Tot INTO [lib://out/tot.qvd] (qvd);\n"
        "STORE V INTO [lib://out/v.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/ordre.qvs")
    tables = run_sections(project / "work/ordre")
    assert tables["Tot"] == [(30, 2)]
    final = next(rows for name, rows in tables.items() if name.startswith("V__"))
    assert sorted(r[0] for r in final) == [1, 1, 2, 3]


@pytest.mark.spark
def test_crosstable_and_text_functions(project: Project) -> None:
    """CROSSTABLE depivote les colonnes ; RENAME FIELD renomme ;
    TextBetween, SubStringCount et Class sont traduits sans modele."""
    (project / "script_qvd/divers.qvs").write_text(
        "Obj:\nCROSSTABLE (Trimestre, Objectif)\n"
        "LOAD * INLINE [\nService, T1, T2\nRH, 10, 12\n];\n"
        "Emp:\nLOAD Nom, TextBetween(Nom, '[', ']') AS Tag, "
        "SubStringCount(Nom, 'o') AS NbO, Class(Sal, 1000) AS Tranche "
        "INLINE [\nNom, Sal\nBob [IT] foo, 4100\n];\n"
        "RENAME FIELD Tranche TO Classe;\n"
        "STORE Obj INTO [lib://out/obj.qvd] (qvd);\n"
        "STORE Emp INTO [lib://out/emp.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/divers.qvs")
    manifest = json.loads(
        (project / "work/divers/manifest.json").read_text(encoding="utf-8")
    )
    assert {s["status"] for s in manifest["sections"]} <= {"skip", "deterministic"}
    emp = next(s for s in manifest["sections"] if s["table"] == "Emp")
    assert emp["columns"] == ["Nom", "Tag", "NbO", "Classe"]
    tables = run_sections(project / "work/divers")
    assert tables["Obj"] == [("RH", "T1", 10), ("RH", "T2", 12)]
    assert tables["Emp"] == [("Bob [IT] foo", "IT", 3, "4000 <= x < 5000")]


def test_apostrophe_in_comment_keeps_variables_expanded() -> None:
    """Une apostrophe dans un commentaire ("today's") ne fait plus croire
    que la suite est une chaine : $(vToday) est remplace partout."""
    loads = insts_of(
        "LET vToday = Today();\nT:\nLOAD d,\n"
        "  // same day as today's date\n"
        "  If(d = $(vToday), 1, 0) AS IsToday\nFROM [lib://d/t.csv] (txt);\n"
    )
    assert "$(vToday)" not in loads[0].text and "(Today())" in loads[0].text


def test_alias_in_single_quotes() -> None:
    """AS 'Nom' : Qlik nomme la colonne Nom, sans les apostrophes."""
    loads = insts_of("T:\nLOAD a AS 'Nom', b AS [Autre] FROM [lib://d/t.csv] (txt);\n")
    assert loads[0].fields == ["Nom", "Autre"]


def test_csv_code_with_leading_zero_stays_text() -> None:
    """001 dans un CSV est un code : il reste du texte, comme dans un INLINE."""
    from fill_conf import value_kind

    assert [value_kind(v) for v in ("001", "0", "12", "007.5", "0.5")] == [
        "string",
        "bigint",
        "bigint",
        "string",
        "double",
    ]


def test_fill_conf_alone_names_shared_model_tables(project: Project) -> None:
    """Deux scripts ont une table Airports sans STORE : fill_conf.py, meme
    lance seul, leur donne deux sorties (<script>_airports), comme
    prepare.py ; et airports.csv et airports.xlsx, deux entrees."""
    (project / "data/input/airports.csv").write_text("Id,Nom\n1,a\n", encoding="utf-8")
    (project / "script_qvd/a.qvs").write_text(
        "Airports:\nLOAD Id, Nom FROM [lib://data/airports.csv] "
        "(txt, utf8, embedded labels, delimiter is ',');\n",
        encoding="utf-8",
    )
    (project / "script_qvd/b.qvs").write_text(
        "Airports:\nLOAD * FROM [lib://data/airports.xlsx] "
        "(ooxml, embedded labels, table is Airports);\n",
        encoding="utf-8",
    )
    project.run("fill_conf.py", "script_qvd/")
    text = (project / "conf/io.yml").read_text(encoding="utf-8")
    for name in ("a_airports:", "b_airports:", "airports_csv:", "airports_xlsx:"):
        assert name in text


@pytest.mark.spark
def test_formatted_values_and_calendar_functions() -> None:
    """Num(x, '00') et Date(d, fmt) donnent leur texte dans une
    concatenation ou un Dual, et restent des valeurs dans un calcul ;
    Even, Odd, DayNumberOfYear et QuarterStart sont traduits."""
    from check import get_parser

    spark = get_parser().spark
    ctx = ExprCtx(column_types={"d": "DATE"})
    cases = {
        "Year(d) & '-W' & Num(Week(d), '00')": "2024-W05",
        "Dual(Date(MonthStart(d), 'YYYY-MM'), 1)": "2024-01",
        "Num(Week(d), '00') + 1": 6,
        "If(Even(Day(d)), 'pair', 'impair')": "pair",
        "Odd(3)": -1,
        "DayNumberOfYear(d)": 30,
        "QuarterStart(d) = MakeDate(2024, 1, 1)": -1,
    }
    for qlik, expected in cases.items():
        sql = expr(qlik, ctx)
        got = spark.sql(f"SELECT {sql} FROM VALUES (DATE'2024-01-30') AS t(d)")
        assert got.collect()[0][0] == expected, qlik


@pytest.mark.spark
def test_calendar_bounds_read_with_peek_in_while(project: Project) -> None:
    """Le calendrier classique : bornes lues par Peek('Min', 0, 'Bornes')
    dans un AUTOGENERATE ... WHILE, la table des bornes supprimee avant."""
    (project / "script_qvd/cal.qvs").write_text(
        "F:\nLOAD * INLINE [\nd\n2024-01-30\n2024-02-02\n];\n"
        "Bornes:\nLOAD Min(Date#(d, 'YYYY-MM-DD')) AS Mn, "
        "Max(Date#(d, 'YYYY-MM-DD')) AS Mx RESIDENT F;\n"
        "LET vMin = Peek('Mn', 0, 'Bornes');\nLET vMax = Peek('Mx', 0, 'Bornes');\n"
        "DROP TABLE Bornes;\n"
        "Cal:\nLOAD Date($(vMin) + IterNo() - 1) AS Jour\n"
        "AUTOGENERATE 1 WHILE $(vMin) + IterNo() - 1 <= $(vMax);\n"
        "STORE Cal INTO [lib://out/cal.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/cal.qvs")
    tables = run_sections(project / "work/cal")
    assert len(tables["Cal"]) == 4


def test_drop_field_on_several_read_tables(project: Project) -> None:
    """DROP FIELD sans FROM sur deux tables lues avant : chaque nouvelle
    version recoit l'instruction (une section restait vide et prepare.py
    plantait sur ses lignes)."""
    (project / "script_qvd/deux.qvs").write_text(
        "A:\nLOAD * INLINE [\nk, x, Tmp\n1, 2, a\n];\n"
        "B:\nLOAD * INLINE [\nk, y, Tmp\n1, 3, b\n];\n"
        "C:\nLOAD k, Tmp AS t1 RESIDENT A;\n"
        "D:\nLOAD k, Tmp AS t2 RESIDENT B;\n"
        "DROP FIELD Tmp;\n"
        "STORE C INTO [lib://o/c.qvd] (qvd);\n"
        "STORE D INTO [lib://o/d.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/deux.qvs")
    manifest = json.loads(
        (project / "work/deux/manifest.json").read_text(encoding="utf-8")
    )
    by_table = {s["table"]: s for s in manifest["sections"]}
    for name in ("A__2", "B__2"):
        assert by_table[name]["status"] == "deterministic"
        assert "Tmp" not in by_table[name]["columns"]


@pytest.mark.spark
def test_index_with_computed_arguments() -> None:
    """Index(texte, motif, n) avec un motif ou un rang calcules : traduit
    sans instr a 3 arguments (absent de Spark 3.5)."""
    from check import get_parser

    spark = get_parser().spark
    sql = expr("Index(s, m, n)", ExprCtx())
    assert "instr" not in sql
    rows = spark.sql(
        f"SELECT {sql} FROM VALUES ('a-b-c', '-', 2), ('a-b-c', '-', -1), "
        "('a-b-c', '-', 5), ('a-b-c', '-', 0) AS t(s, m, n)"
    ).collect()
    assert [r[0] for r in rows] == [4, 4, 0, 0]


def test_select_star_except_is_refused(tmp_path: Path) -> None:
    """SELECT * EXCEPT : message clair pour le modele, Spark 3.5 ne le
    connait pas."""
    from check import check_section

    query = tmp_path / "query.sql"
    query.write_text("-- l.1 : T\nSELECT * EXCEPT (a) FROM in_t\n", encoding="utf-8")
    errors, _ = check_section({"views": {"in_t": {}}, "statement_lines": [1]}, query)
    assert any("EXCEPT" in e and "lister les colonnes" in e for e in errors)


def test_new_version_keeps_translated_columns(project: Project) -> None:
    """LOAD * d'un fichier decrit par conf/schema, lu puis DROP FIELD : la
    nouvelle version reprend les colonnes que la traduction connait (le
    graphe, lui, ne les connait pas) et reste traduite sans modele."""
    (project / "script_qvd/v.qvs").write_text(
        "V:\nLOAD * FROM [lib://data/ventes.csv] "
        "(txt, utf8, embedded labels, delimiter is ',');\n"
        "A:\nLOAD Site, Count(Produit) AS Nb RESIDENT V GROUP BY Site;\n"
        "DROP FIELD Montant FROM V;\n"
        "STORE V INTO [lib://out/v.qvd] (qvd);\n"
        "STORE A INTO [lib://out/a.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/v.qvs")
    manifest = json.loads(
        (project / "work/v/manifest.json").read_text(encoding="utf-8")
    )
    v2 = next(s for s in manifest["sections"] if s["table"] == "V__2")
    assert v2["status"] == "deterministic"
    assert v2["columns"] == ["Site", "Produit"]


def test_drop_field_ignores_dropped_tables(project: Project) -> None:
    """DROP FIELDS apres un DROP TABLE : la table supprimee n'est pas
    touchee (elle gardait sa colonne pour la table qui la lit, et ne gagne
    plus de version fantome ecrite en sortie)."""
    (project / "script_qvd/fin.qvs").write_text(
        "T:\nLOAD * INLINE [\nk, Garde, Col\n1, a, x\n];\n"
        "STORE T INTO [lib://o/t.qvd] (qvd);\n"
        "U:\nLOAD k, Col AS ColU RESIDENT T;\n"
        "STORE U INTO [lib://o/u.qvd] (qvd);\n"
        "DROP TABLE T;\nDROP TABLE U;\n"
        "Autre:\nLOAD * INLINE [\nk, Col\n2, y\n];\n"
        "DROP FIELDS Col;\n"
        "STORE Autre INTO [lib://o/autre.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/fin.qvs")
    manifest = json.loads(
        (project / "work/fin/manifest.json").read_text(encoding="utf-8")
    )
    columns = {s["table"]: s["columns"] for s in manifest["sections"]}
    assert set(columns) == {"T", "U", "Autre"}
    assert columns["T"] == ["k", "Garde", "Col"] and columns["Autre"] == ["k"]


@pytest.mark.spark
def test_mapping_with_where_and_aggregate(project: Project) -> None:
    """Mapping filtre par un WHERE (une condition, pas le -1/0 de Qlik que
    Spark refuse en filtre) et mapping avec Max et GROUP BY."""
    (project / "script_qvd/map.qvs").write_text(
        "Src:\nLOAD * INLINE [\nc1, c2, m\na, ok, 10\na, ok, 30\nb, verif, 5\n];\n"
        "MapF:\nMAPPING LOAD c1, c2 RESIDENT Src WHERE c2 <> 'verif';\n"
        "MapMax:\nMAPPING LOAD c1, Max(m) RESIDENT Src GROUP BY c1;\n"
        "T:\nLOAD DISTINCT c1, ApplyMap('MapF', c1, 'absent') AS Etat, "
        "ApplyMap('MapMax', c1, 0) AS Mx RESIDENT Src;\n"
        "STORE T INTO [lib://o/t.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/map.qvs")
    tables = run_sections(project / "work/map")
    assert tables["T"] == [("a", "ok", 30), ("b", "absent", 5)]


@pytest.mark.spark
def test_two_argument_if_from_the_model_is_completed() -> None:
    """Le modele recopie If(cond, x) de Qlik ; le if de Spark exige trois
    arguments : on complete par NULL, le resultat de Qlik."""
    from check import get_parser
    from translate import fix_spark_if, parse_holes

    assert (
        fix_spark_if("if(a > 1, if(b, 'x,y'))") == "if(a > 1, if(b, 'x,y', NULL), NULL)"
    )
    assert fix_spark_if("if(a, 1, 2)") == "if(a, 1, 2)"
    answer = parse_holes("```sql\nh1: if(n > 1, 'grand')\n```")["h1"]
    spark = get_parser().spark
    rows = spark.sql(f"SELECT {answer} FROM VALUES (2), (0) AS t(n)").collect()
    assert [r[0] for r in rows] == ["grand", None]


@pytest.mark.spark
def test_date_of_floor_keeps_the_day() -> None:
    """Date(Floor(d)) garde le jour de d, Date(Ceil(d)) le jour suivant s'il
    y a une heure : floor() de Spark refuse une date ou un horodatage, quel
    que soit le type de d (connu ou non)."""
    from check import get_parser

    spark = get_parser().spark
    values = {
        "DATE": ("DATE'2024-01-30'", "2024-01-30"),
        "TIMESTAMP": ("TIMESTAMP'2024-01-30 15:45:00'", "2024-01-31"),
        "STRING": ("'30/01/2024 15:45:00'", "2024-01-31"),
        "text": ("'30/01/2024'", "2024-01-30"),
        None: ("TIMESTAMP'2024-01-30 23:59:00'", "2024-01-31"),
    }
    for typ, (value, ceil) in values.items():
        ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
        known = "STRING" if typ == "text" else typ
        ctx.column_types = {"d": known} if known else {}
        for text, day in (
            ("Date(Floor(d))", "2024-01-30"),
            ("Date(Ceil(d), 'DD/MM/YYYY')", ceil),
        ):
            sql = expr(text, ctx)
            assert "floor(" not in sql.lower() and "ceil(" not in sql.lower()
            rows = spark.sql(f"SELECT {sql} FROM VALUES ({value}) AS t(d)").collect()
            assert str(rows[0][0]) == day, (typ, text, sql)
    # m numerique : un numero de serie Qlik, des jours depuis le 30/12/1899
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    ctx.column_types = {"m": "DOUBLE"}
    sql = expr("Date(Floor(m))", ctx)
    assert sql.lower().count("floor(") == 1
    rows = spark.sql(f"SELECT {sql} FROM VALUES (45321.7D) AS t(m)").collect()
    assert str(rows[0][0]) == "2024-01-30"


@pytest.mark.spark
def test_qlik_day_rounding_of_timestamps() -> None:
    """Floor, Ceil, Round et Frac d'un horodatage disent ce que Qlik fait :
    une date y est un nombre de jours, l'heure sa partie decimale."""
    from check import get_parser

    spark = get_parser().spark
    expected = {
        "Floor(d)": "2024-01-30",
        "Round(d)": "2024-01-31",
        "Floor(d, 1/24)": "2024-01-30 15:00:00",
        "Ceil(d, 1/24)": "2024-01-30 16:00:00",
        "Round(d, 1/96)": "2024-01-30 15:45:00",
        "Floor(d, 7)": "2024-01-27",
        "DayStart(d)": "2024-01-30 00:00:00",
        "Frac(d) * 24": "15.75",
        "Floor(d) - Floor(e)": "2",
        "Frac(m)": "0.6",
    }
    for typ in ("TIMESTAMP", None):
        ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
        ctx.column_types = {"d": typ, "e": typ, "m": "DOUBLE"} if typ else {}
        for text, value in expected.items():
            if typ is None and text not in ("DayStart(d)", "Frac(m)"):
                continue
            sql = expr(text, ctx)
            rows = spark.sql(
                f"SELECT {sql} FROM VALUES (TIMESTAMP'2024-01-30 15:45:00', "
                "TIMESTAMP'2024-01-28 08:00:00', -1.4) AS t(d, e, m)"
            ).collect()
            assert str(rows[0][0]).rstrip("0").rstrip(".") == value.rstrip("0").rstrip(
                "."
            ), (typ, text, sql)
    # type inconnu, mais compare a une date : d est une date
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    sql = expr("If(Floor(d) >= MakeDate(2024, 1, 30), 1, 0)", ctx)
    rows = spark.sql(
        f"SELECT {sql} FROM VALUES (TIMESTAMP'2024-01-30 15:45:00') AS t(d)"
    ).collect()
    assert rows[0][0] == 1, sql


def test_spark_types_reach_the_next_tables(project: Project) -> None:
    """Le type qu'a calcule Spark pour une table est connu des tables qui la
    relisent au passage suivant : Floor d'un horodatage n'y est plus floor()
    d'un nombre."""
    (project / "script_qvd/jours.qvs").write_text(
        "Src:\nLOAD * INLINE [\nd\n2024-01-30 15:45:00\n];\n"
        "STORE Src INTO [lib://o/src.qvd] (qvd);\n"
        "T:\nNoConcatenate LOAD Floor(d) AS Jour RESIDENT Src;\n"
        "STORE T INTO [lib://o/t.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/jours.qvs")
    mpath = project / "work/jours/manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    t_id = next(s["id"] for s in manifest["sections"] if s["table"] == "T")
    query = project / "work/jours" / t_id / "query.sql"
    assert "floor(d)" in query.read_text(encoding="utf-8")  # type encore inconnu
    src = next(s for s in manifest["sections"] if s["table"] == "Src")
    src["output_types"], src["output_types_sure"] = {"d": "TIMESTAMP"}, True
    mpath.write_text(json.dumps(manifest), encoding="utf-8")

    project.run("prepare.py", "script_qvd/jours.qvs")
    again = json.loads(mpath.read_text(encoding="utf-8"))
    sec = next(s for s in again["sections"] if s["table"] == "T")
    draft = (project / "work/jours" / sec["id"] / "query.sql").read_text(
        encoding="utf-8"
    )
    assert "CAST(d AS DATE)" in draft and "floor(" not in draft


DATES_SCRIPT = """SET DateFormat='DD/MM/YYYY';
SET TimestampFormat='DD/MM/YYYY hh:mm:ss';
Src:
LOAD * INLINE [
Id, Client, TxtDebut, TxtFin
1, a, 28/01/2024 08:00:00, 30/01/2024 15:45:00
2, a, 30/03/2024 12:00:00, 31/03/2024 12:00:00
3, b, 01/02/2024 00:00:00, 01/02/2024 06:00:00
];
MapDerniere:
MAPPING LOAD Client, Max(Timestamp#(TxtFin, 'DD/MM/YYYY hh:mm:ss'))
RESIDENT Src GROUP BY Client;
T1:
NoConcatenate LOAD Id, Client,
  Timestamp#(TxtDebut, 'DD/MM/YYYY hh:mm:ss') AS Debut,
  Timestamp#(TxtFin, 'DD/MM/YYYY hh:mm:ss') AS Fin
RESIDENT Src;
T2:
NoConcatenate LOAD Id, Client, Debut, Fin,
  Fin - Debut AS Duree,
  Floor(Fin) - Floor(Debut) AS NbJours,
  Round((Fin - Debut) * 24, 0.01) AS Heures,
  Date(Floor(Fin)) AS JourFin,
  If(Fin > Debut, Fin, Debut) AS Dmax,
  ApplyMap('MapDerniere', Client) AS Derniere
RESIDENT T1;
DROP TABLE T1;
T3:
NoConcatenate LOAD Id, Duree, NbJours, Heures,
  Floor(Dmax - Debut) AS Ecart,
  Date(JourFin + 7) AS Semaine,
  Floor(Derniere) - Floor(Debut) AS DepuisDerniere,
  Timestamp(Debut + 1/24) AS DebutPlus1h
RESIDENT T2;
STORE T3 INTO [lib://o/t3.qvd] (qvd);
Agg:
NoConcatenate LOAD Client, Max(Fin) - Min(Debut) AS Etendue,
  Date(Max(Floor(Fin))) AS DernierJour
RESIDENT T2 GROUP BY Client;
STORE Agg INTO [lib://o/agg.qvd] (qvd);
"""


@pytest.mark.spark
def test_date_arithmetic_through_resident_chain(project: Project) -> None:
    """Calculs de dates Qlik sur plusieurs tables : date - date en jours (pas
    un intervalle Spark), Floor d'un horodatage, date + 1/24, If et ApplyMap
    de dates ; les types passent d'une table a l'autre. La 2e ligne passe a
    l'heure d'ete : 24 heures quand meme, comme Qlik."""
    (project / "script_qvd/dates.qvs").write_text(DATES_SCRIPT, encoding="utf-8")
    project.run("prepare.py", "script_qvd/dates.qvs")
    tables = run_sections(project / "work/dates")
    rows = {r[0]: r for r in tables["T3"]}
    # Id, Duree, NbJours, Heures, Ecart, Semaine, DepuisDerniere, DebutPlus1h
    one = rows[1]
    assert round(float(one[1]), 9) == round(2 + 7.75 / 24, 9)
    assert (one[2], float(one[3]), one[4], one[6]) == (2, 55.75, 2, 63)
    assert str(one[5]) == "2024-02-06"
    assert str(one[7]) == "2024-01-28 09:00:00"
    dst = rows[2]
    assert (float(dst[1]), dst[2], float(dst[3])) == (1.0, 1, 24.0)
    agg = {r[0]: r for r in tables["Agg"]}
    assert round(float(agg["a"][1]), 6) == round(63 + 4 / 24, 6)
    assert str(agg["a"][2]) == "2024-03-31"


# Exemples de calculs de dates, valeurs attendues calculees a la main avec
# les regles de Qlik : une date est un nombre de jours depuis le 30/12/1899,
# l'heure en est la partie decimale. Colonnes DateDebut et DateFin pour trois
# jeux de valeurs : des dates, des horodatages, et un jour de passage a
# l'heure d'ete (24 heures quand meme pour Qlik).
DATE_VALUES = {
    "date": ("DATE'2024-01-28'", "DATE'2024-01-30'"),
    "timestamp": ("TIMESTAMP'2024-01-28 08:00:00'", "TIMESTAMP'2024-01-30 15:45:00'"),
    "dst": ("TIMESTAMP'2024-03-30 12:00:00'", "TIMESTAMP'2024-03-31 12:00:00'"),
}
DATE_CASES = [
    # expression, attendu pour date, timestamp, dst
    ("DateFin - DateDebut", ("2", "2.322917", "1")),
    ("Floor(DateFin - DateDebut)", ("2", "2", "1")),
    ("Floor(DateFin) - Floor(DateDebut)", ("2", "2", "1")),
    ("Round((DateFin - DateDebut) * 24, 0.01)", ("48", "55.75", "24")),
    ("Ceil(DateFin - DateDebut)", ("2", "3", "1")),
    ("DateFin + 1", ("2024-01-31", "2024-01-31 15:45:00", "2024-04-01 12:00:00")),
    ("Date(DateFin + n)", ("2024-02-02", "2024-02-02", "2024-04-03")),
    (
        "DateFin + x",
        ("2024-01-31 12:00:00", "2024-02-01 03:45:00", "2024-04-02 00:00:00"),
    ),
    (
        "Timestamp(DateFin - 1/24)",
        ("2024-01-29 23:00:00", "2024-01-30 14:45:00", "2024-03-31 11:00:00"),
    ),
    ("If(DateFin > DateDebut, DateFin, DateDebut) - DateDebut", ("2", "2.322917", "1")),
    ("Alt(DateFin, DateDebut) - DateDebut", ("2", "2.322917", "1")),
    ("RangeMax(DateFin, DateDebut) - DateDebut", ("2", "2.322917", "1")),
    ("Age(DateFin, MakeDate(1990, 1, 31))", ("33", "33", "34")),
    ("DateFin >= DateDebut + 2", ("-1", "-1", "0")),
    ("Date(Floor(DateFin, 7))", ("2024-01-27", "2024-01-27", "2024-03-30")),
]


def spark_text(spark: Any, sql: str, select: str) -> str:
    """Valeur d'une expression Spark en texte : nombre arrondi, date,
    horodatage ; un intervalle est signale (Qlik donne des jours)."""
    row = spark.sql(
        f"SELECT typeof(v), CAST(v AS STRING) FROM (SELECT {sql} AS v FROM "
        f"(SELECT {select}))"
    ).collect()[0]
    if row[0].startswith("interval"):
        return "intervalle " + row[1]
    if row[0] in ("date", "timestamp", "string"):
        return row[1]
    return f"{float(row[1]):.6f}".rstrip("0").rstrip(".")


@pytest.mark.spark
@pytest.mark.parametrize("known", [True, False], ids=["type_connu", "type_inconnu"])
def test_qlik_date_arithmetic(known: bool) -> None:
    """date - date donne des jours (Spark : un intervalle), floor() ne recoit
    jamais une date, date + n garde l'heure ; le type des colonnes connu
    (conf) ou non (nom de date), et au passage a l'heure d'ete."""
    from check import get_parser

    spark = get_parser().spark
    zone = str(spark.conf.get("spark.sql.session.timeZone"))
    spark.conf.set("spark.sql.session.timeZone", "Europe/Paris")
    try:
        for k, (scenario, (debut, fin)) in enumerate(DATE_VALUES.items()):
            types = {"n": "int", "x": "double"}
            if known:
                kind = "date" if scenario == "date" else "timestamp"
                types.update(DateDebut=kind, DateFin=kind)
            select = f"{debut} AS DateDebut, {fin} AS DateFin, 3 AS n, 1.5D AS x"
            for text, expected in DATE_CASES:
                ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
                ctx.column_types = types
                sql = expr(text, ctx)
                got = spark_text(spark, sql, select)
                assert got == expected[k], (scenario, text, sql)
    finally:
        spark.conf.set("spark.sql.session.timeZone", zone)


@pytest.mark.spark
def test_text_dates_never_crash_spark() -> None:
    """Spark 3.5 plante (INCONSISTENT_BEHAVIOR_CROSS_VERSION) sur un texte
    que l'ancien analyseur aurait lu : "1/2/2024", une heure en trop, une
    annee sur 2 chiffres. Lu comme Qlik, ou NULL, jamais d'erreur."""
    from check import get_parser

    spark = get_parser().spark
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    ctx.column_types = {"s": "string"}
    expected = {
        "30/01/2024 15:45:00": ("2024-01-30", "2024-01-30 15:45:00"),
        "1/2/2024": ("2024-02-01", None),  # pas d heure : pas un horodatage
        "30/01/24": (None, None),
        "abc": (None, None),
    }
    day, stamp = expr("Date#(s)", ctx), expr("Timestamp#(s)", ctx)
    for value, (want_day, want_stamp) in expected.items():
        select = f"'{value}' AS s"
        row = spark.sql(
            f"SELECT CAST({day} AS STRING), "
            f"CAST({stamp} AS STRING) FROM (SELECT {select})"
        ).collect()[0]
        assert (row[0], row[1]) == (want_day, want_stamp), value


@pytest.mark.spark
def test_date_compared_to_a_number() -> None:
    """ColDate > 0 dit "date renseignee" en Qlik (le nombre d'une date
    compte les jours depuis le 30/12/1899) : IS NOT NULL. Spark refuse de
    comparer une date a un nombre ; un autre nombre devient la date de ce
    rang."""
    from check import get_parser

    spark = get_parser().spark
    values = [
        ("date", "DATE'2024-01-30'", "CAST(NULL AS DATE)"),
        ("timestamp", "TIMESTAMP'2024-01-30 15:45:00'", "CAST(NULL AS TIMESTAMP)"),
        (None, "DATE'2024-01-30'", "CAST(NULL AS DATE)"),
        ("string", "'30/01/2024'", "''"),
    ]
    cases = {
        "ColDate > 0": [-1, 0],
        "0 < ColDate": [-1, 0],
        "ColDate >= 45321": [-1, 0],
    }
    for typ, filled, empty in values:
        ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
        ctx.column_types = {"ColDate": typ} if typ else {}
        for text, want in cases.items():
            sql = expr(text, ctx)
            got = [
                spark.sql(f"SELECT {sql} FROM (SELECT {v} AS ColDate)").collect()[0][0]
                for v in (filled, empty)
            ]
            assert got == want, (typ, text, sql)
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    ctx.column_types = {"ColDate": "date"}
    assert "IS NOT NULL" in expr("ColDate > 0", ctx)


@pytest.mark.spark
def test_exists_as_in_and_mapping_ctes(project: Project) -> None:
    """Exists devient un IN, NOT Exists garde une cle NULL comme Qlik (une
    valeur NULL n'existe jamais) ; chaque mapping est une CTE map_<nom>,
    lue par un LEFT JOIN."""
    (project / "script_qvd/exist.qvs").write_text(
        "Clients:\nLOAD * INLINE [\nCodeClient, Pays\na, FR\nb, BE\n];\n"
        "STORE Clients INTO [lib://o/clients.qvd] (qvd);\n"
        "MapPays:\nMAPPING LOAD CodeClient, Pays RESIDENT Clients;\n"
        "Src:\nLOAD * INLINE [\nCode, M\na, 1\nc, 2\nx, 3\n];\n"
        "Ventes:\nNoConcatenate LOAD If(Code = 'x', Null(), Code) AS Code, M "
        "RESIDENT Src;\n"
        "Connus:\nNoConcatenate LOAD Code, M, ApplyMap('MapPays', Code, '?') AS Pays "
        "RESIDENT Ventes WHERE Exists(CodeClient, Code);\n"
        "STORE Connus INTO [lib://o/connus.qvd] (qvd);\n"
        "Inconnus:\nNoConcatenate LOAD Code, M RESIDENT Ventes "
        "WHERE NOT Exists(CodeClient, Code);\n"
        "STORE Inconnus INTO [lib://o/inconnus.qvd] (qvd);\n",
        encoding="utf-8",
    )
    project.run("prepare.py", "script_qvd/exist.qvs")
    work = project / "work/exist"
    queries = {
        q.parent.name: q.read_text(encoding="utf-8") for q in work.glob("*/query.sql")
    }
    connus = next(
        v for k, v in queries.items() if k.endswith("connus") and "in" not in k
    )
    assert "Code IN (SELECT CodeClient FROM" in connus and "EXISTS" not in connus
    assert "map_mappays AS (" in connus and "LEFT JOIN map_mappays m1" in connus
    tables = run_sections(work)
    assert tables["Connus"] == [("a", 1, "FR")]
    assert tables["Inconnus"] == sorted([("c", 2), (None, 3)], key=str)


@pytest.mark.spark
def test_readable_not_same_result() -> None:
    """NOT s'ecrit sous sa forme lisible (x IS NOT NULL, a <> b, NOT LIKE,
    NOT NOT c = c) et donne le meme resultat que NOT (...), NULL compris."""
    from check import get_parser
    from qlik2sql import cond

    spark = get_parser().spark
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    ctx.column_types = {"a": "int", "b": "int", "x": "string", "d": "date"}
    rows = (
        "VALUES (1, 2, 'Abc', DATE'2024-01-30'), (2, 2, 'zz', CAST(NULL AS DATE)),"
        " (NULL, 2, NULL, DATE'2024-01-30') AS t(a, b, x, d)"
    )
    readable = {
        "NOT IsNull(x)": "(x IS NOT NULL)",
        "NOT a = b": "(a <> b)",
        "NOT a > b": "(a <= b)",
        "NOT x LIKE 'A*'": "(x NOT ILIKE 'A%')",
        "NOT NOT a = b": "(a = b)",
        "NOT d > 0": "(d IS NULL)",
    }

    def values(where: str) -> list:
        """Vrai, faux ou NULL pour chaque ligne."""
        return [
            r[0]
            for r in spark.sql(
                f"SELECT CASE WHEN {where} THEN 1 WHEN NOT ({where}) THEN 0 END "
                f"FROM {rows}"
            ).collect()
        ]

    for text, want in readable.items():
        sql = cond(text, ctx)
        assert sql == want, text
        assert values(sql) == values(f"NOT ({cond(text[4:], ctx)})"), text


@pytest.mark.spark
def test_date_of_unknown_plus_days() -> None:
    """Date(x + 1), x au type inconnu (une date relue par RESIDENT) : un
    CAST AS DATE, pas une conversion de numero de serie Qlik, que Spark
    refusait (int et date). Avec un entier connu, c'est un numero Qlik."""
    from check import get_parser

    spark = get_parser().spark
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    ctx.column_types = {"n": "int"}
    for text, value, want in [
        ("Date(x + 1)", "DATE'2024-01-30'", "2024-01-31"),
        ("Date(x - 1)", "DATE'2024-01-30'", "2024-01-29"),
        ("Date(n + 1)", "45321", "2024-01-31"),
        ("Date(45321)", "0", "2024-01-30"),
    ]:
        sql = expr(text, ctx)
        rows = spark.sql(
            f"SELECT {sql} FROM (SELECT {value} AS x, {value} AS n)"
        ).collect()
        assert str(rows[0][0]) == want, (text, sql)


TIDY_CASES = [
    # (avant, apres) : meme resultat dans Spark, moins de parentheses
    ("(a + (b * 2))", "a + b * 2"),
    ("((a + b) * 2)", "(a + b) * 2"),
    ("((a - b) - 1)", "a - b - 1"),
    ("(a - (b - 1))", "a - (b - 1)"),
    ("((a > 0) AND (b > 0))", "a > 0 AND b > 0"),
    ("((a > 0) AND ((b > 0) OR (m > 1)))", "a > 0 AND (b > 0 OR m > 1)"),
    ("(((a > 0) AND (b > 0)) OR (m > 1))", "(a > 0 AND b > 0) OR m > 1"),
    ("(NOT ((a > 0) AND (b > 0)))", "NOT (a > 0 AND b > 0)"),
    ("((a = b) IS NULL)", "(a = b) IS NULL"),
    (
        "CASE WHEN (a > 0) THEN (a + 1) ELSE -1 END",
        "CASE WHEN a > 0 THEN a + 1 ELSE -1 END",
    ),
    ("CAST((a + b) AS STRING)", "CAST(a + b AS STRING)"),
    ("((a IS NOT NULL) AND (a IN (1, 2)))", "a IS NOT NULL AND a IN (1, 2)"),
    ("(a BETWEEN (b - 1) AND (b + 1))", "a BETWEEN b - 1 AND b + 1"),
    ("((/*TODO(model):h1*/ NULL) * 2)", "(/*TODO(model):h1*/ NULL) * 2"),
]


@pytest.mark.spark
def test_tidy_parens_same_result() -> None:
    """Les parentheses que la priorite rend inutiles partent, les autres
    restent (un trou du modele garde les siennes) ; le resultat dans Spark
    ne change pas, NULL compris."""
    from check import get_parser
    from qlik2sql import tidy_parens

    spark = get_parser().spark
    rows = "VALUES (1, 2, 3.5), (NULL, 2, -1.0), (5, 0, 0.0) AS t(a, b, m)"
    for before, after in TIDY_CASES:
        assert tidy_parens(before) == after, before
        if "TODO" in before:
            continue
        got = [
            spark.sql(f"SELECT {sql} FROM {rows}").collect() for sql in (before, after)
        ]
        assert got[0] == got[1], before


@pytest.mark.spark
def test_friendly_translations() -> None:
    """If imbriques en un seul CASE, Round au pas decimal en round(x, n),
    concatenation sans CAST, Match en condition en = ou IN (NOT garde une
    valeur NULL, comme Qlik)."""
    from check import get_parser
    from qlik2sql import cond, tidy_parens

    spark = get_parser().spark
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    ctx.column_types = {"a": "int", "m": "double", "Code": "string"}
    rows = (
        "VALUES (1, 1.005D, 'A'), (NULL, 2.675D, NULL), (2, 3.0D, 'bz')"
        " AS t(a, m, Code)"
    )
    values = {
        "If(a = 1, 'x', If(a = 2, 'y', 'z'))": (
            "CASE WHEN a = 1 THEN 'x' WHEN a = 2 THEN 'y' ELSE 'z' END",
            ["x", "z", "y"],
        ),
        "Round(m, 0.01)": ("round(m, 2)", [1.01, 2.68, 3.0]),
        "Code & '-' & a": ("concat_ws('', Code, '-', a)", ["A-1", "-", "bz-2"]),
    }
    for text, (want, result) in values.items():
        sql = tidy_parens(expr(text, ctx))
        assert sql == want, text
        assert [
            r[0] for r in spark.sql(f"SELECT {sql} FROM {rows}").collect()
        ] == result
    conditions = {
        "Match(Code, 'A', 'B')": ("Code IN ('A', 'B')", [1, 0, 0]),
        "not Match(Code, 'A')": ("Code IS NULL OR Code <> 'A'", [0, 1, 1]),
        "not WildMatch(Code, 'b*')": (
            "Code IS NULL OR Code NOT ILIKE 'b%'",
            [1, 1, 0],
        ),
    }
    for text, (want, result) in conditions.items():
        sql = tidy_parens(cond(text, ctx))
        assert sql == want, text
        got = spark.sql(
            f"SELECT CASE WHEN {sql} THEN 1 ELSE 0 END FROM {rows}"
        ).collect()
        assert [r[0] for r in got] == result, text


@pytest.mark.spark
def test_alt_skips_text_that_is_not_a_number() -> None:
    """Alt rend le premier argument qui a une valeur numerique, sinon le
    dernier : un texte 'abc' ou '' est saute, pas seulement NULL. Un nombre
    ou une date garde un simple COALESCE."""
    from check import get_parser
    from qlik2sql import tidy_parens

    spark = get_parser().spark
    ctx = ExprCtx("", None, "DD/MM/YYYY", "DD/MM/YYYY hh:mm:ss")
    ctx.column_types = {"s": "string", "n": "int"}
    rows = "VALUES ('12', 5), ('abc', NULL), ('', 7), (NULL, NULL) AS t(s, n)"
    cases = {
        "Alt(s, 0)": ("COALESCE(try_cast(s AS DOUBLE), 0)", [12.0, 0.0, 0.0, 0.0]),
        "Alt(s, 'N/A')": (None, ["12", "N/A", "N/A", "N/A"]),
        "Alt(s, n, -1)": (None, [12.0, -1.0, 7.0, -1.0]),
        "Alt(n, 0)": ("COALESCE(n, 0)", [5, 0, 7, 0]),
    }
    for text, (want, result) in cases.items():
        sql = tidy_parens(expr(text, ctx))
        if want:
            assert sql == want, text
        got = [r[0] for r in spark.sql(f"SELECT {sql} FROM {rows}").collect()]
        assert got == result, (text, sql)


def test_calendar_and_peek_in_spark_style(project: Project) -> None:
    """Le calendrier de Qlik (AUTOGENERATE fin - debut + 1, debut + RecNo()
    - 1) devient sequence(debut, fin), et chaque Peek une CTE d'une ligne,
    ecrite une fois ; la requete finale est un SELECT * quand les colonnes
    sont deja les bonnes."""
    project.run("prepare.py", "script_qvd/calendrier")
    work = project / "work/calendrier"
    sql = next(work.glob("*_calendrier/query.sql")).read_text(encoding="utf-8")
    assert "posexplode(sequence((SELECT MinDate FROM peek_mindate)" in sql
    assert sql.count("ORDER BY _qlik_order\n  LIMIT 1") == 2
    assert "RecNo" not in sql and "__jour AS Jour" in sql
    assert sql.rstrip().endswith("SELECT *\nFROM calendrier_1")
