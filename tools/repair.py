"""Reparations sures des reponses du modele, faites en Python avant le
controle.

Le modele recopie souvent des ecritures valides en Qlik, ou en Databricks,
mais pas en Spark 3.5.8. Quand la bonne ecriture se deduit sans risque, on la
corrige ici plutot que de lui redemander (il peut se tromper de nouveau) :
    - un nom avec # ou un autre caractere special, laisse sans backticks ;
    - SELECT * EXCEPT (a, b), remplace par la liste des colonnes gardees ;
    - if(cond, x) a deux arguments, complete par NULL comme Qlik.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

sys.path.insert(0, str(Path(__file__).resolve().parent))
from qlik2sql import ident  # noqa: E402

if TYPE_CHECKING:
    from check import SparkChecker

# Un nom avec # : jamais valide sans backticks en Spark SQL.
HASH_NAME = re.compile(r"(?<![\w`#])([A-Za-z_0-9][\w]*#[\w#]*)(?![\w`#(])")
# * EXCEPT (a, b) apres un SELECT
STAR_EXCEPT = re.compile(r"\bSELECT\s+(DISTINCT\s+)?\*\s*EXCEPT\s*\(([^()]*)\)", re.I)


def skip_quoted(sql: str, i: int) -> int:
    """Position juste apres une chaine '...', un nom `...` ou un
    commentaire -- qui commence en i ; i lui-meme sinon.

    Args:
        sql: le texte SQL.
        i: la position a regarder.

    Returns:
        La position apres l'element, ou i.
    """
    if sql.startswith("--", i):
        end = sql.find("\n", i)
        return len(sql) if end == -1 else end
    if sql[i] in "'\"`":
        quote, j = sql[i], i + 1
        while j < len(sql) and sql[j] != quote:
            j += 2 if sql[j] == "\\" else 1
        return j + 1
    return i


def masked(sql: str) -> str:
    """Le texte avec ses chaines, noms entre backticks et commentaires
    remplaces par des espaces : les positions restent les memes.

    Args:
        sql: le texte SQL.

    Returns:
        Le texte masque, de meme longueur.
    """
    out, i = list(sql), 0
    while i < len(sql):
        j = skip_quoted(sql, i)
        if j != i:
            for k in range(i, min(j, len(sql))):
                if out[k] != "\n":
                    out[k] = " "
            i = j
        else:
            i += 1
    return "".join(out)


def fix_spark_if(sql: str) -> str:
    """Complete les if(cond, valeur) a deux arguments par NULL.

    Qlik accepte If(cond, valeur) (NULL sinon) ; le if de Spark exige trois
    arguments. Ajouter NULL donne exactement le resultat de Qlik.

    Args:
        sql: une expression ou une requete.

    Returns:
        La meme, avec ses if a deux arguments completes.
    """
    hidden = masked(sql)
    starts = [m.end() - 1 for m in re.finditer(r"(?<![\w.`])if\s*\(", hidden, re.I)]
    # de la fin vers le debut : les if imbriques d'abord, sans decaler les
    # positions de ceux d'avant
    for open_pos in reversed(starts):
        hidden = masked(sql)
        depth, commas, k = 0, 0, open_pos
        while k < len(hidden):
            ch = hidden[k]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    break
            elif ch == "," and depth == 1:
                commas += 1
            k += 1
        if k < len(sql) and commas == 1:
            sql = sql[:k] + ", NULL" + sql[k:]
    return sql


def backtick_names(sql: str, names: set[str]) -> str:
    """Met entre backticks les noms que Spark ne lit pas sans eux : ceux
    avec #, et les colonnes connues qui ont un caractere special (espace,
    point...).

    Args:
        sql: une expression ou une requete.
        names: les colonnes connues de la table.

    Returns:
        La meme, avec ces noms entre backticks.
    """
    special = sorted(
        (n for n in names if not re.fullmatch(r"[A-Za-z_]\w*", n) and "`" not in n),
        key=len,
        reverse=True,
    )
    edits: list[tuple[int, int, str]] = []
    hidden = masked(sql)
    for m in HASH_NAME.finditer(hidden):
        edits.append((m.start(1), m.end(1), f"`{m.group(1)}`"))
    for name in special:
        for m in re.finditer(r"(?<![\w`])" + re.escape(name) + r"(?![\w`(])", hidden):
            edits.append((m.start(), m.end(), f"`{name}`"))
    done = 0
    for start, end, text in sorted(edits, reverse=True):
        if done and end > done:
            continue  # chevauche une correction deja faite
        sql = sql[:start] + text + sql[end:]
        done = start
    return sql


def _close_paren(hidden: str, start: int) -> int:
    """Position de la parenthese qui ferme celle de start, ou -1."""
    depth = 0
    for k in range(start, len(hidden)):
        if hidden[k] == "(":
            depth += 1
        elif hidden[k] == ")":
            depth -= 1
            if depth == 0:
                return k
    return -1


def expand_star_except(sql: str, section: dict, parser: SparkChecker | None) -> str:
    """Remplace SELECT * EXCEPT (a, b) FROM x, absent de Spark 3.5, par la
    liste des colonnes de x sans a ni b.

    Les colonnes d'une vue viennent de la section ; celles d'une CTE de la
    requete, de Spark, qui analyse la requete jusqu'a elle.

    Args:
        sql: la requete.
        section: la table, avec ses vues.
        parser: le verificateur Spark, ou None.

    Returns:
        La requete, sans * EXCEPT quand les colonnes sont connues.
    """
    views = {k.lower(): v for k, v in section.get("views", {}).items()}
    pos = 0  # apres les EXCEPT qu'on n'a pas su reparer
    for _ in range(50):
        hidden = masked(sql)
        m = STAR_EXCEPT.search(hidden, pos)
        if not m:
            return sql
        excluded = {
            c.strip().strip("`").lower()
            for c in sql[m.start(2) : m.end(2)].split(",")
            if c.strip()
        }
        frm = re.compile(r"\bFROM\s+(`[^`]+`|[\w.]+)", re.I).search(hidden, m.end())
        rel = sql[frm.start(1) : frm.end(1)].strip("`") if frm else ""
        cols: list[str] | None = None
        if rel.lower() in views:
            cols = list(views[rel.lower()].get("columns") or []) or None
            if cols and "_qlik_order" not in cols:
                cols.append("_qlik_order")
        elif rel and parser is not None:
            # CTE de la requete : sa definition, puis SELECT * dessus
            d = re.search(r"\b" + re.escape(rel) + r"\s+AS\s*\(", hidden, re.I)
            end = _close_paren(hidden, d.end() - 1) if d else -1
            if d and end != -1 and end < m.start():
                probe = sql[: end + 1] + f"\nSELECT * FROM {ident(rel)}"
                cols = parser.columns_of(section, probe)
        if not cols:
            # colonnes inconnues : on laisse, le controle le signale
            pos = m.end()
            continue
        keep = [ident(c) for c in cols if c.lower() not in excluded]
        distinct = m.group(1) or ""
        sql = sql[: m.start()] + f"SELECT {distinct}{', '.join(keep)}" + sql[m.end() :]
    return sql


def repair(sql: str, section: dict, parser: SparkChecker | None = None) -> str:
    """Toutes les reparations sures d'une reponse du modele.

    Args:
        sql: l'expression ou la requete.
        section: la table, avec ses vues, ses colonnes et ses trous.
        parser: le verificateur Spark, pour les colonnes d'une CTE.

    Returns:
        La reponse reparee.
    """
    names = set(section.get("columns") or [])
    for v in section.get("views", {}).values():
        names |= set(v.get("columns") or [])
    for h in section.get("holes", []):
        names |= set(h.get("columns") or [])
    sql = fix_spark_if(backtick_names(sql, names))
    if STAR_EXCEPT.search(masked(sql)):
        sql = expand_star_except(sql, section, parser)
    return sql
