"""Lecture et ecriture des fichiers declares dans conf/io.yml."""

from __future__ import annotations

import io
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commons import LOAD_ORDER, cast_void_to_string, drop_columns
from core import config


def read_config(spark: SparkSession, name: str) -> DataFrame:
    """Lit une entree de io.yml.

    Un CSV est lu avec son schema (colonnes dans l'ordre du fichier) et ses
    options (header, sep, encoding, mode...), passees telles quelles au
    lecteur CSV de Spark. En-tete par defaut. Un fichier Excel (xlsx, xls)
    est lu feuille par feuille (option sheets). Les autres formats sont lus
    par leur nom (parquet...).

    Args:
        spark: la session.
        name: le nom de l'entree dans io.yml.

    Returns:
        La table lue.
    """
    description = config.entry("input", name)
    path = config.location(description)
    fmt = str(description.get("format", "parquet")).lower()
    if fmt in ("xlsx", "xls"):
        sheets = (description.get("options") or {}).get("sheets", "*")
        return read_excel(spark, path, sheets, config.schema_columns(description))
    if fmt != "csv":
        return spark.read.format(fmt).load(path)
    reader = spark.read
    options: dict[str, Any] = {"header": True, **(description.get("options") or {})}
    for key, value in options.items():
        reader = reader.option(
            key, str(value).lower() if isinstance(value, bool) else value
        )
    schema = config.schema_ddl(description)
    reader = reader.schema(schema) if schema else reader.option("inferSchema", "false")
    return reader.csv(path)


def read_excel(
    spark: SparkSession, path: str, sheets: str | list[str], columns: dict[str, str]
) -> DataFrame:
    """Lit un fichier Excel : une feuille, plusieurs, ou toutes ("*"), mises
    bout a bout (elles doivent avoir les memes colonnes).

    Le fichier est lu en binaire par Spark, ce qui marche aussi sur HDFS,
    puis ouvert par pandas sur le driver : c'est fait pour des fichiers de
    taille raisonnable, comme ceux que Qlik chargeait. Toutes les cellules
    sont lues en texte, puis converties aux types du schema s'il y en a un.

    Args:
        spark: la session.
        path: la location du fichier.
        sheets: "*", le nom d'une feuille ou une liste de noms.
        columns: {colonne: type} du schema, vide sans schema.

    Returns:
        La table lue.

    Raises:
        FileNotFoundError: le fichier n'existe pas.
    """
    import pandas as pd

    row = spark.read.format("binaryFile").load(path).select("content").head()
    if row is None:
        raise FileNotFoundError(path)
    wanted = None if sheets == "*" else sheets
    data = pd.read_excel(io.BytesIO(row[0]), sheet_name=wanted, dtype=str)
    frames = list(data.values()) if isinstance(data, dict) else [data]
    table = pd.concat(frames, ignore_index=True)
    table = table.astype(object).where(table.notna(), None)
    as_text = ", ".join(f"`{c}` STRING" for c in table.columns)
    df = spark.createDataFrame(table, schema=as_text)
    if not columns:
        return df
    return df.select(
        *[
            F.col(f"`{c}`").cast(t).alias(c)
            for c, t in columns.items()
            if c in df.columns
        ]
    )


def output_location(name: str) -> str:
    """Location d'une sortie de io.yml.

    Args:
        name: le nom de la sortie.

    Returns:
        Le chemin ou elle est ecrite.
    """
    return config.location(config.entry("output", name))


def input_location(name: str) -> str:
    """Location d'une entree de io.yml.

    Args:
        name: le nom de l'entree.

    Returns:
        Le chemin ou elle est lue.
    """
    return config.location(config.entry("input", name))


def write_config(df: DataFrame, name: str) -> None:
    """Ecrit une table dans une sortie de io.yml, en Parquet, en remplacant
    ce qui y etait.

    La colonne d'ordre de chargement ne sert qu'a l'interieur du job : elle
    n'est pas ecrite. Une colonne void est passee en string.

    Args:
        df: la table.
        name: le nom de la sortie dans io.yml.
    """
    ready = cast_void_to_string(drop_columns(df, LOAD_ORDER))
    ready.write.mode("overwrite").parquet(output_location(name))
