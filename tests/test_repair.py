"""Reparations en Python des reponses du modele, avant le controle."""

import pytest

from translate import backtick_names, expand_star_except, fix_spark_if, repair


def test_names_with_hash_get_backticks() -> None:
    """Un nom avec # n'est jamais valide nu en Spark SQL ; ni une chaine, ni
    un commentaire, ni un nom deja entre backticks ne sont touches."""
    sql = (
        "SELECT s.TST#TEST, `TI#ID`, 'a#b' AS x -- TST#TEST\nFROM t WHERE TST#TEST = 1"
    )
    assert backtick_names(sql, set()) == (
        "SELECT s.`TST#TEST`, `TI#ID`, 'a#b' AS x -- TST#TEST\n"
        "FROM t WHERE `TST#TEST` = 1"
    )


def test_known_names_with_spaces_get_backticks() -> None:
    """Une colonne connue avec un espace, ecrite nue, prend ses backticks."""
    sql = "SELECT Code Article AS c FROM t"
    assert backtick_names(sql, {"Code Article", "Lib"}) == (
        "SELECT `Code Article` AS c FROM t"
    )


def test_if_completed_and_left_alone() -> None:
    """if a deux arguments complete par NULL, if a trois inchange."""
    assert fix_spark_if("if(a, if(b, 1))") == "if(a, if(b, 1, NULL), NULL)"
    assert fix_spark_if("if(a, 1, 2) + diff(x, y)") == "if(a, 1, 2) + diff(x, y)"


def test_star_except_on_a_view() -> None:
    """SELECT * EXCEPT (a) FROM une vue : la liste de ses autres colonnes."""
    section = {"views": {"in_t": {"columns": ["k", "a", "TST#B"]}}}
    sql = "WITH c AS (\n  SELECT * EXCEPT (a) FROM in_t\n)\nSELECT * FROM c"
    assert expand_star_except(sql, section, None) == (
        "WITH c AS (\n  SELECT k, `TST#B`, _qlik_order FROM in_t\n)\nSELECT * FROM c"
    )


@pytest.mark.spark
def test_star_except_on_a_cte() -> None:
    """SELECT * EXCEPT sur une CTE : ses colonnes viennent de Spark, et la
    requete reparee passe."""
    from check import get_parser

    parser = get_parser()
    section = {
        "views": {"in_t": {"columns": ["k", "a"]}},
        "columns": ["k", "b"],
        "statement_lines": [],
    }
    sql = (
        "WITH c1 AS (\n  SELECT k, a, a * 2 AS b, _qlik_order FROM in_t\n),\n"
        "c2 AS (\n  SELECT * EXCEPT (a) FROM c1\n)\nSELECT k, b, _qlik_order FROM c2"
    )
    fixed = repair(sql, section, parser)
    assert "EXCEPT" not in fixed and "SELECT k, b, _qlik_order FROM c1" in fixed
    assert parser.columns_of(section, fixed) == ["k", "b", "_qlik_order"]
