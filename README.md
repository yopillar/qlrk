# Migration Qlik -> Spark SQL

Ce kit traduit des scripts de chargement Qlik Sense en Spark SQL, une
requete par table, et produit un job PySpark qui les enchaine. Le job est
ecrit pour etre lu par un developpeur Qlik : il ne contient aucun code
Spark, seulement des appels au socle `spark_foundation`
(`spark-foundation-core`).

L'essentiel de la traduction se fait en Python, sans modele de langage. Le
modele n'intervient que sur les morceaux que le traducteur ne sait pas
traduire a coup sur : ils sont marques `TODO(model)` dans une requete par
ailleurs deja ecrite.

## Demarrer

Tout se lance depuis la racine du projet : les chemins (`conf/`, `src/`,
`work/`, `data/`) partent du dossier courant.

```bash
python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # la 1re fois : 3 tables, puis on relit
python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier
python src/job_<projet>.py                                # lancer le resultat
```

Le kit se lance dans la distrobox, celle qui a pyspark et pyarrow : sans
eux, il s'arrete au lieu de mettre toutes les colonnes en `string`.
opencode, lui, reste installe sur l'hote : depuis la distrobox,
`translate.py` le lance par `host-spawn`, sans rien a faire
(`--opencode-in-box` s'il est installe dans la box). host-spawn ne reprend
pas l'environnement du terminal : opencode est donc lance par le shell de
l'hote, qui charge le profil (`bash -lic`), comme dans un terminal. Le projet doit donc
etre dans un dossier que la box partage avec l'hote, le home en general.

`migrate.py` est la seule commande a connaitre. Pour chaque script, il
traduit ce qu'il peut sans modele, fait completer le reste par le modele,
range les requetes dans `src/`, puis lance le job et compare avec Qlik s'il
trouve des exports dans `data/qlik_outputs/`. On peut le relancer autant de
fois qu'on veut : ce qui est fait et n'a pas change n'est pas refait.

Pour savoir ou on en est, `work/<script>/rapport.md` dit table par table ce
qui a ete traduit, ce qui reste a faire et pourquoi.

## Le projet

```
mon-projet/
|-- script_qvd/                   scripts Qlik (.qvs, .txt ou sans extension)   a fournir
|-- conf/io.yml                   entrees et sorties : format, location, schema a fournir
|-- conf/schema/*.yml             colonnes et types des entrees                 a fournir
|-- data/input/                   donnees lues (si absentes de conf/io.yml)     a fournir
|-- data/qlik_outputs/            exports Qlik pour comparer (facultatif)       a fournir
|-- data/output/                  donnees ecrites par le job
|-- src/
|   |-- job_<projet>.py           le job, le seul fichier a lancer              cree une fois
|   |-- scripts/
|   |   |-- __init__.py                                                         cree une fois
|   |   |-- <projet>_step_010_run_sql.py   les scripts Qlik                     regenere
|   |   `-- <projet>_step_020_<nom>.py     vos etapes                           a la main
|   `-- sql/                      <script>__01_<table>.sql, ...                 regenere
|-- work/                         travail intermediaire, journal.csv
|-- .opencode/                    l'agent traducteur et ses 4 skills SQL
|-- AGENTS.md                     les consignes du modele
`-- tools/
```

`<projet>` est le nom du dossier, ou la variable `QLIK_PROJECT` si elle est
definie, ramene a un nom Python (`mon-projet` devient `mon_projet`). On le
retrouve dans les noms de fichiers, dans le nom passe a `@pipeline` et dans
chaque ligne de log : c'est ce qui permet de s'y retrouver dans YARN.

Quelques regles :
- **Le job est a vous.** Il est cree une seule fois, puis jamais reecrit :
  c'est la qu'on ajoute ses propres etapes (`step_020`, `step_030`...).
- **L'etape 010 est au kit.** Elle est reecrite a chaque migration, on ne la
  modifie pas a la main.
- **Les etapes vont de 10 en 10**, pour pouvoir en inserer une plus tard
  (`step_015`).
- **D'autres jobs peuvent venir a cote** (`src/job_<traitement>_<projet>.py`,
  pour exporter un data product, transformer ou nettoyer en PySpark). Ils
  partagent `scripts/` et `sql/`.
- **`scripts/` a un `__init__.py`** : c'est un package, et
  `from scripts import ...` se resout de la meme facon pour Python et pour
  les outils de CI.
- **Les requetes de tous les scripts sont dans le meme `sql/`**, d'ou le
  prefixe `<script>__` : deux scripts peuvent avoir une table du meme nom.
  Le numero suit l'ordre des tables dans le script Qlik.

## Le code genere

Le job tient en quelques lignes :

```python
from scripts import demo_step_010_run_sql
from spark_foundation import pipeline


@pipeline("demo")
def run(spark: SparkSession, args: list[str]) -> None:
    demo_step_010_run_sql.run()
```

L'etape 010 contient une liste de tables par script Qlik, une seule boucle,
et `run()` qui appelle les scripts dans l'ordre. Avec 50 tables, on a 50
lignes dans une liste, pas 140 appels de fonction :

```python
VENTES: list[tuple[str, str, str | None]] = [
    ("ventes__01_ventes.sql", "Ventes", "ventes_enrichies_v2"),
    ("ventes__02_clients.sql", "Clients", None),  # None : pas de STORE
]


@script
def ventes() -> None:
    """Script Qlik ventes.qvs."""
    run_tables(VENTES)


def run() -> None:
    """Fait tourner les scripts Qlik du projet, dans l'ordre."""
    calendrier()
    ventes()
    synthese()  # apres ventes, dont il lit les sorties
```

Chaque ligne de `VENTES` donne la requete, le nom de la table Qlik et, si
le script la stockait (`STORE`), sa sortie dans `conf/io.yml`. Les fichiers
lus n'apparaissent nulle part : `run_sql` charge tout seul les entrees dont
sa requete a besoin (la vue `in_ventes` est l'entree `ventes` de
`conf/io.yml`). `conf/io.yml` reste la seule source pour les fichiers.

Tout le code genere a des docstrings et des annotations de type, et le test
de bout en bout le passe a `ruff` et `mypy`. Son texte est dans
`tools/job_template.py` : pour changer sa forme, on corrige ce fichier et
on relance `migrate.py` (ou `orchestrate.py`).

## Lancer le job

```bash
python src/job_<projet>.py                  # tous les scripts
python src/job_<projet>.py --from ventes    # reprendre a un script apres un echec
python src/job_<projet>.py --only ventes    # un seul script
```

Sur le cluster, depuis la racine du projet (la commande est rappelee en tete
du job) :

```bash
spark-submit --name <projet> --deploy-mode client \
    --py-files <spark-foundation-core et pyspark-commons, s'ils ne sont pas installes> \
    src/job_<projet>.py [--from <script> | --only <script>]
```

En mode client, le driver lit `src/scripts/` et `src/sql/` sur place. Le
mode cluster demanderait d'envoyer aussi ces dossiers et `conf/` : ce n'est
pas prevu pour l'instant. Les reglages SQL dont dependent les requetes
(sensibilite a la casse, ANSI desactive, `timeParserPolicy=LEGACY`,
`analyzer.maxIterations=10000`) sont poses par le socle, il n'y a pas de
`--conf` a passer.

### Les logs

Chaque ligne dit le projet, l'etape et le script, puis ce qui se passe :

```
[demo] step_010_run_sql > ventes : lecture ventes <- data/input/ventes.csv (table in_ventes)
[demo] step_010_run_sql > ventes : sql ventes__01_ventes.sql -> Ventes (demo_qlik.ventes__ventes) en 4s
[demo] step_010_run_sql > ventes : ecriture Ventes -> ventes_enrichies_v2 (data/output/ventes_enrichies_v2)
```

En cas d'erreur, le message dit quoi regarder et comment reprendre :

```
[demo] step_010_run_sql > ventes : ECHEC pendant sql/ventes__01_ventes.sql (table Ventes)
  -> erreur : [UNRESOLVED_COLUMN.WITH_SUGGESTION] ... `s`.`Montantt` ...
  -> une fois corrige, relancer le job avec --from ventes
```

Si l'application a deja configure ses logs, le socle n'y touche pas. Sinon
il les affiche au niveau INFO, sur la sortie d'erreur, que YARN recupere.

### Comment le job se comporte

- **Les scripts tournent dans l'ordre des appels de `run()`.** Cet ordre
  compte : un script qui lit un fichier ecrit par un autre doit passer apres
  lui, comme dans l'enchainement des taches de rechargement Qlik.
  `orchestrate.py` le calcule a partir de `work/stores.json` (ce que chaque
  script lit et ecrit) et le note en commentaire.
- **Le job s'arrete au premier script en echec**, comme un rechargement
  Qlik : les suivants liraient des donnees absentes ou d'hier. On corrige,
  puis on relance avec `--from <script>`. Toutes les ecritures sont en
  overwrite, relancer ne cree donc pas de doublon. Un nom inconnu passe a
  `--from` ou `--only` est signale.
- **`@script` marche aussi sur vos etapes** : posez-le sur leur fonction
  pour pouvoir les viser avec `--from` et `--only`.
- **Chaque script a ses propres vues**, supprimees a la fin, pour que deux
  scripts puissent avoir une table du meme nom.
- **Chaque table Qlik est ecrite dans le catalogue**, sous
  `<base>.<script>__<table>` (base `<projet>_qlik` par defaut, `--database`
  pour en changer), en overwrite, puis relue de la. C'est pour OpenLineage :
  une vue temporaire n'est pas un jeu de donnees, et la lignee irait des
  fichiers lus aux sorties sans montrer les tables entre les deux. Avec
  `--temp-views`, le job garde des vues temporaires : plus rapide, mais rien
  dans la lignee. Une table dont deux colonnes ne different que par la
  casse reste en vue temporaire, car le catalogue ne les distingue pas.
- **En local**, sans metastore Hive, le socle en cree un dans `metastore_db/`
  et ecrit les tables dans `spark-warehouse/` (deux dossiers ignores par
  git). Sans lui, une table ne pourrait pas etre remplacee d'un lancement a
  l'autre.

### Ce que deviennent les STORE

- **Chaque `STORE` devient une ecriture.** Un `STORE` qu'on n'arrive a
  rattacher a aucune table est signale dans le rapport et a l'assemblage. La
  boucle classique qui stocke toutes les tables
  (`FOR i = NoOfTables()-1 TO 0`, `LET vTable = TableName($(i))`) est
  deroulee sur les tables presentes a ce moment.
- **Une sortie porte le nom du fichier du `STORE`** (sans extension, casse
  gardee), et sa cle dans `conf/io.yml` est ce nom en minuscules :
  `STORE T INTO [lib://qvd/Clients.qvd]` donne la cle `clients`, de location
  `${sorties}/Clients`. Un autre script qui relit ce fichier a la meme cle en
  entree.
- **Les tables encore en memoire a la fin du script**, sans `STORE`, forment
  le modele de donnees de l'app : c'est le resultat d'une app de
  restitution. Elles sont ecrites aussi, sous le nom de la table. Si deux
  scripts ont une table du meme nom, chacun prefixe la sienne par son nom de
  script (`<script>_<table>`).
- **Seuls les scripts entierement valides entrent dans le job.**
  `orchestrate.py` liste les autres, et previent si un script du job depend
  de l'un d'eux.

## Les noms de fichiers

Avant tout le reste, `migrate.py` lance `tools/normalize.py` :
- les scripts de `script_qvd/` passent en minuscules ;
- dans `data/input/` et `data/qlik_outputs/`, les noms perdent les
  caracteres que Hadoop ne sait pas lire. Il prend le `#` pour un fragment
  d'URI et `[ ] { } * ?` pour un motif, et ne retrouve pas le fichier. Les
  crochets et accolades deviennent des parentheses, `# * ?` des `_` :
  `[anon]-TT#TEST.CSV` devient `(anon)-TT_TEST.CSV`.

Le kit fait la meme chose sur les chemins lus dans les scripts Qlik, les
deux restent donc d'accord. Jamais de `_` en tete de nom : Spark ignore sans
rien dire les fichiers qui commencent par `_` ou `.` (comme `_SUCCESS`), et
`normalize.py` signale ceux qui sont deja dans ce cas. Si deux fichiers
tombent sur le meme nom, rien n'est renomme. Les fichiers du cluster ne sont
pas touches : ceux qui ont ces caracteres sont a renommer de la meme facon.

```bash
python tools/normalize.py --dry-run     # montre ce qui changerait
```

## Les entrees et sorties : `conf/io.yml`

Chaque fichier lu ou ecrit par un script est rattache a l'entree (ou la
sortie) de `conf/io.yml` qui porte le meme nom, ou dont la `location` porte
ce nom. Le `schema_file` donne les colonnes et leurs types, ce qui permet de
traduire les `LOAD *` sans avoir les donnees.

```yaml
inputs:
  sites:
    format: csv
    schema_file: schemas/sites.yml
    location: ${entrees}/sites.csv
outputs:
  disponibilite_equipement:
    location: ${sorties}/disponibilite_equipement
```

```bash
python tools/conf.py      # ce qui a ete compris de conf/io.yml
```

Le socle ne lit et n'ecrit que par nom : tout fichier lu ou ecrit doit
etre declare. Une lecture se cherche dans `inputs`, une ecriture dans
`outputs`.

Un fichier ecrit par un script et relu par un autre est une sortie, et en
entree un simple renvoi vers elle, sous un autre nom :

```yaml
inputs:
  ventes_enrichies_v2_relue:
    from_output: ventes_enrichies_v2
outputs:
  ventes_enrichies_v2:
    location: ${sorties}/ventes_enrichies_v2
```

L'entree reprend la location de la sortie et se lit en Parquet (le job
ecrit toujours ses sorties en Parquet, qui porte ses types : pas de schema
a ecrire). Elle ne peut pas porter le meme nom que la sortie : Spark refuse
une entree et une sortie du meme nom. Le controle des requetes qui la lisent
prend les types de la requete Spark qui ecrit la sortie.

Un fichier lu n'est rattache a une sortie que par son chemin, ou par son nom
avec la meme extension : `commandes.csv`, la source brute, et
`commandes.qvd`, ecrit par un autre script, restent deux entrees
differentes (`commandes` et `commandes_relue`). Deux fichiers du meme nom
mais d'extensions differentes (`airports.csv`, `airports.xlsx`) donnent
`airports_csv` et `airports_xlsx`. Une table sans STORE presente dans
plusieurs scripts prend une sortie par script (`<script>_<table>`), que
`fill_conf.py` soit lance seul ou par `migrate.py`. Dans un CSV, une valeur a
zero en tete (`001`) reste du texte, comme dans un `INLINE`.

`fill_conf.py` ne modifie jamais une entree existante : une ancienne entree
qui relit une sortie avec sa propre location (et souvent le meme nom que la
sortie) est a supprimer a la main, il ecrira alors le `from_output`.

Le rapport liste ce qui manque, et `assemble.py` refuse d'assembler un script tant qu'il en
manque. Pour la traduction seule, un fichier non declare est cherche dans
`data/input/<nom>` (ou `data/output/<nom>` s'il est ecrit par un `STORE`),
ce qui permet d'avancer en attendant de completer la conf.

La cle d'une entree ne contient que des lettres, des chiffres et des `_`
(c'est ce que `fill_conf.py` ecrit) : sa vue dans les requetes est
`in_<cle>`. `assemble.py` refuse un script dont une entree ne suit pas la
regle.

### `fill_conf.py` complete la conf

Avant de traduire, `migrate.py` lance `tools/fill_conf.py`, qui ajoute ce
qui manque pour les scripts du dossier. Il ajoute, il ne modifie jamais :
- **Une entree ou une sortie absente** est ajoutee a la fin de sa section,
  en texte, avec l'indentation du fichier. Les commentaires et la mise en
  forme sont conserves, et chaque ajout porte un commentaire date qui dit
  pourquoi il est la.
- **Un schema absent** est cree dans la forme des schemas deja presents
  (par defaut celle du socle : `columns:` puis `NomCol: TYPE`). Les noms de
  colonnes gardent la casse du script Qlik ou du fichier, puisque Qlik y est
  sensible. Un schema existant n'est jamais retouche, meme s'il lui manque
  des colonnes : c'est signale.
- **Une copie de `io.yml`** est gardee dans `conf/io.yml.bak`. Si le
  resultat n'est plus du YAML lisible, l'original est conserve.
- **Relancer n'ajoute rien de plus.**

```bash
python tools/migrate.py script_qvd/ --no-conf          # sans toucher a conf/
python tools/fill_conf.py script_qvd/                  # seulement completer conf/
python tools/fill_conf.py script_qvd/ --draft          # brouillon dans conf/draft/
```

D'ou viennent les informations :
- **Les entrees et sorties** viennent des `FROM` et des `STORE`, nommees
  d'apres les fichiers Qlik, avec le prefixe de location deja utilise dans
  `io.yml` (`${entrees}/...`).
- **Le format** est celui que Qlik lit : `(txt, ...)` ou un `.csv` donne
  `csv`, `(ooxml, ...)` ou un `.xlsx` donne `xlsx` (`xls` pour un ancien
  Excel), avec le nom complet du fichier dans la location ; un QVD est
  suppose converti en Parquet sous le meme nom.
- **Un fichier Excel** recoit une cle `options` avec la feuille lue : celle
  du `table is ...` de Qlik, sinon `"*"`, toutes les feuilles mises bout a
  bout (elles doivent avoir les memes colonnes). Le socle le lit en
  binaire, puis l'ouvre avec pandas sur le driver (`openpyxl` requis), et
  le convertit aux types du schema.

  ```yaml
  tarifs:
     format: xlsx
     schema_file: schemas/tarifs.yml
     location: ${entrees}/tarifs.xlsx
     options:
        sheets: "*"
  ```
- **Un CSV** recoit une cle `options` avec les noms des options du lecteur
  CSV de Spark, d'apres la specification Qlik : `header` (toujours ecrit),
  `sep`, `encoding`, `quote`.

  ```yaml
  codes:
     format: csv
     schema_file: schemas/codes.yml
     location: ${entrees}/codes.csv
     options: {header: false, sep: ";"}
  ```
- **Un CSV sans en-tete** (`no labels`) : Qlik nomme ses colonnes `@1`,
  `@2`... Le kit les appelle `_c0`, `_c1`..., les noms que Spark leur donne
  lui-meme : `@1 as Code` devient `_c0 AS Code`. Le schema liste toutes les
  colonnes dans l'ordre, puisque Spark les affecte par position.
- **Les colonnes** sont celles que les scripts lisent. Si le fichier de
  donnees est la (dans `data/input/` ou a sa location), on prend toutes ses
  colonnes et on signale celles que les scripts lisent mais qui n'y sont
  pas. Pour un fichier ecrit par un autre script, on prend toutes les
  colonnes de son `STORE`, dans l'ordre.
- **Les types**, du plus sur au moins sur :
  1. les metadonnees du Parquet, dans `data/input/` ou a sa location
     (variables passees par `--var`) ;
  2. les types que pandas lit dans un fichier Excel ;
  3. les valeurs d'un CSV (tout le fichier) : entier, decimal a point,
     date et horodatage ISO (`2024-01-31`, `2024-01-31 10:00:00`), booleen
     `true`/`false`, sinon texte. Une date a la francaise (`29/12/2024`)
     ou un decimal a virgule restent en texte : Spark ne les lirait pas
     sans options ;
  4. le script Qlik : la fonction qui recoit la colonne (`Year(col)` donne
     `date`, `Sum(col)` donne `double`, `Trim(col)` donne `string`), la
     constante a laquelle elle est comparee, ou son nom (`date_...`) ;
  5. le meme champ ailleurs : dans Qlik, un nom de champ designe la meme
     donnee dans toutes les tables.

  Chaque type porte en commentaire d'ou il vient, et "a verifier" quand il
  est devine (4 et 5). Sans aucun indice, la colonne est en `string`, avec
  le commentaire "type inconnu".

  Les metadonnees du Parquet se lisent avec `pyarrow`, ou a defaut avec
  Spark (plus lent a demarrer). Les types sont repris exactement : un
  decimal garde sa precision (`decimal(18,2)`, et non `decimal`, que Spark
  lirait en `decimal(10,0)`), une colonne categorielle prend le type de ses
  valeurs. Sans les deux, `fill_conf.py` le signale et
  les types sont devines. Un schema deja ecrit n'est jamais corrige : un
  schema cree avec des types devines (fichier absent, pyarrow manquant) est
  a supprimer pour etre recree avec les vrais types.

Les variables des locations (`${entrees}`, `${sorties}`...) sont resolues
par le socle quand le job tourne. Le kit en a besoin pour aller lire les
sorties au moment de la comparaison : on les lui donne avec `--var` (ou par
des variables d'environnement du meme nom).

```bash
python tools/migrate.py script_qvd/ --var sorties=data/output
python tools/verify.py work/ventes --no-run --var sorties=data/output
```

L'export Qlik de `data/qlik_outputs/` peut porter le nom de la sortie dans
`io.yml` ou celui du fichier de sa location. Dans un export CSV, Qlik ecrit
les dates au `DateFormat` du script (`29/12/2024`) : la comparaison les
relit avec ce format.

## Ce que le traducteur sait faire sans modele

- **Les expressions** : `If`, `&`, arithmetique, comparaisons, `like`, texte
  (`Index` avec n-ieme occurrence, `SubField`, `KeepChar`...), dates
  (`Date#` avec conversion du format, `WeekYear`, `MakeDate`, `AddMonths`
  avec ses deux modes...), nombres, `IsNum`, `Match`, `WildMatch`, `Pick`,
  `RangeSum`, agregations.
- **Les booleens facon Qlik** : vrai vaut -1, faux vaut 0, et dans une
  condition tout nombre non nul est vrai.
- **Les clauses** : `WHERE`, `GROUP BY`, `ORDER BY`, `DISTINCT`, `FIRST n`
  et les LOAD empiles.
- **Les sources** : fichier, `RESIDENT`, `INLINE`, `SQL SELECT` simple et
  `AUTOGENERATE` (voir plus bas).
- **Les colonnes d'un `INLINE`** sont typees comme Qlik les lit : que des
  entiers donnent `BIGINT`, des nombres a point `DOUBLE`. Une valeur a zero
  en tete (`007`, `01000`) ou une colonne mixte reste du texte. Laissees en
  texte, elles faisaient passer en `string` la colonne a laquelle on les
  ajoutait (`CONCATENATE`) et tout `ApplyMap` qui s'en servait.
- **Tout ce qui depend de l'ordre de chargement** : `RowNo`, `RecNo`,
  `Previous`, `Peek` (champ simple, ou `Peek('f', n, 'T')` sur une autre
  table), `FirstValue`, `LastValue`, `NoOfRows`, et les mappings, qui gardent
  la premiere valeur chargee comme Qlik.
- **`Exists(champ[, expr])` et `NOT Exists(...)`**, y compris le
  dedoublonnage quand le LOAD charge lui-meme le champ.
- **`FileTime()`** dans un `LOAD ... FROM fichier` (ou `FileTime('fichier')`
  sur ce meme fichier) : la date de modification du fichier lu. Pour un
  autre fichier, c'est laisse au modele, car il n'est peut-etre pas declare
  dans `conf/io.yml`.
- **Les variables `LET` calculees** (`Peek`, `NoOfRows`, `Today()`...),
  remplacees par leur expression, y compris dans
  `AUTOGENERATE vMax - vMin + 1`.
- **Les dates a la Qlik** : `date - date` donne un nombre de jours, `Floor`
  et `Ceil` gardent le jour. `Min`, `Max`, `FirstValue`, `LastValue` et
  `Peek` d'une date restent des dates, ce qui permet de traduire le
  calendrier classique. Une colonne declaree texte mais au `DateFormat` du
  script (`DD/MM/YYYY`...) est lue comme une date dans les calculs, comme
  Qlik le fait au chargement.
- **La casse des noms de champs** est respectee, comme dans Qlik.
- **`Dual(texte, nombre)`** : on garde le texte, Parquet n'ayant qu'un type
  par colonne (choix a valider). Pareil pour `Class(x, pas)`, qui donne
  `3000 <= x < 4000`.
- **Les nombres ecrits en texte** : avec `SET DecimalSep=','` (et
  `ThousandSep`), une colonne texte comme `1 200,50` utilisee dans un calcul,
  une comparaison a un nombre, `Sum`, `Avg`, `Round`, `RangeSum`... est lue
  comme Qlik la lit. Sans cela, Spark donnait NULL. Une simple copie garde le
  texte d'origine.
- **Les valeurs formatees** : `Num(x, '00')` ou `Date(d, 'YYYY-MM')` donnent
  leur texte affiche dans une concatenation (`'W' & Num(Week(d), '00')`
  donne `W05`) ou comme texte d'un `Dual`, et restent un nombre ou une date
  dans un calcul.
- **`Even`**, **`Odd`**, **`DayNumberOfYear`**, **`QuarterStart`**, et un
  alias entre apostrophes (`AS 'Nom'`), que Qlik accepte.
- **`TextBetween`** (premiere occurrence), **`SubStringCount`**,
  **`CROSSTABLE (attribut, donnee[, n])`** quand les colonnes sont connues, et
  **`RENAME FIELD a TO b`** (pas `USING`, qui depend d'un mapping).
- **Une table lue par une autre puis modifiee** (`JOIN`, `CONCATENATE`,
  `DROP FIELD`, `RENAME FIELD`) : la lecture voit la table d'avant, comme
  dans Qlik. Un agregat calcule avant un `CONCATENATE` ne compte pas les
  lignes ajoutees ensuite.
- **Les etiquettes repetees** : une boucle `FOR` qui recharge `Stocks:` avec
  les memes champs ajoute ses lignes a `Stocks` ; avec d'autres champs, Qlik
  cree `Stocks-1`. Un mapping utilise avant d'etre charge, ou une etiquette
  ecrite apres le prefixe (`LEFT KEEP (A) T: LOAD`), est signale dans le
  rapport.
- **`JOIN`** (cles implicites, ordre des colonnes de Qlik), **`KEEP`**,
  **`CONCATENATE`**, l'auto-concatenation et **`DROP FIELD`**.
- **`ApplyMap`**, traduit en `LEFT JOIN` sur la source du mapping, avec un
  `COALESCE` pour la valeur par defaut.
- **`AutoNumber`** et `AutoNumberHash128/256`, traduits en `xxhash64`. La
  cle est stable d'une table a l'autre, mais les nombres ne sont pas ceux de
  Qlik : ces colonnes sont exclues de la comparaison.
- **Le controle du script** : variables, `CALL` de SUB, boucles `FOR` sur
  une liste fixe et `IF` qu'on peut evaluer.
- **Les commentaires** (`//`, `/* */`, `REM`) sont retires, et tout ce qui
  suit `EXIT SCRIPT;` est ignore.
- **Les apostrophes** : `'l''ete'` devient `"l'ete"`. En Spark SQL, `''`
  colle deux chaines et on obtiendrait "lete" : `check.py` refuse cette
  ecriture.

### `AUTOGENERATE`, toujours traduit sans modele

| Forme Qlik | Spark SQL genere |
|---|---|
| `AUTOGENERATE 10` | `range(1, 11)` |
| `AUTOGENERATE vMax - vMin + 1` (meme avec des `LET` calcules par `Peek`) | `explode(sequence(1, <expression>))` |
| `AUTOGENERATE 1 WHILE <condition sur IterNo()>` (calendriers) | une ligne par iteration jusqu'a la premiere fausse, comme Qlik (100 000 au plus) |
| `AUTOGENERATE FieldValueCount('F')` avec `FieldValue('F', RecNo())` | les valeurs distinctes de `F` dans les tables deja chargees |

`RecNo()`, `RowNo()` et `IterNo()` y sont exacts.

### L'ordre des lignes

Qlik traite les lignes dans l'ordre ou elles ont ete chargees ; Spark n'a
pas d'ordre. Chaque table porte donc une colonne technique `_qlik_order`
(`array<bigint>`) :
- **a la lecture**, on la reprend si elle est deja dans le fichier, sinon on
  numerote les lignes dans l'ordre de lecture, avant toute autre operation ;
- **un `INLINE`** porte son numero de ligne en dur dans le `VALUES`. Les
  requetes n'utilisent jamais `monotonically_increasing_id()` : Spark le
  refuse dans un agregat ou une jointure, et il renumerote les lignes s'il
  est recalcule ;
- **ensuite**, elle suit les regles de Qlik : `ORDER BY` (a egalite, l'ordre
  d'avant), `CONCATENATE` (les lignes ajoutees passent apres), `JOIN`
  (l'ordre de la table de gauche, puis les lignes sans correspondance),
  `GROUP BY` et `DISTINCT` (premiere occurrence) ;
- **elle n'est pas ecrite dans les sorties.** Un script qui relit la sortie
  d'un autre renumerote donc ses lignes dans l'ordre de lecture du fichier.

Pour des donnees converties depuis des QVD, le plus sur est d'y ajouter le
numero de ligne d'origine dans une colonne `_qlik_order`. A defaut, l'ordre
de lecture d'un fichier Parquet unique convient.

Avant toute ecriture Parquet, une colonne toujours nulle (`Null()` dans
Qlik, de type `void` que Parquet refuse) est passee en `string`.

## Le modele

### Il ne traduit que de petits morceaux

Quand le traducteur bloque, c'est presque toujours sur une seule expression
d'un LOAD (`ConvertToLocalTime`, un `Peek` recursif, une fonction
inconnue). Il ecrit quand meme toute l'instruction et laisse un trou a la
place de l'expression. Ensuite, pour chaque table :
1. **Les expressions isolees** (`holes_prompt.md`) : le modele recoit
   l'expression, l'instruction Qlik qui la contient, les colonnes
   disponibles avec leur type et l'ordre de chargement, et repond
   `h1: <expression>`. S'il repond `h1: ?`, on le relance avec plus de
   contexte.
2. **Les instructions entieres** (`prompt.md`), seulement pour ce qui bloque
   une instruction complete (colonnes inconnues, `KEEP` entre tables...), ou
   en dernier recours. Le modele ne renvoie que les CTE a completer, qu'on
   greffe dans le brouillon.

Aucune valeur de donnee n'est envoyee au modele : seulement des noms et des
types de colonnes, et les messages d'erreur de Spark. Sur une table de plus
de 30 colonnes, les prompts ne citent que les colonnes que le code a
traduire utilise : une table de 300 colonnes repetee partout faisait passer
un prompt de 2 000 a 45 000 tokens.

### Le controle avant l'assemblage

Avant d'assembler un script, `migrate.py` fait analyser toutes ses requetes
par Spark (`check.py --parse --mark`), sans donnees, sur des vues vides qui
ont les bonnes colonnes. Une erreur certaine (syntaxe, `GROUP BY`,
reference ambigue...) bloque la table avant le job.

Une colonne introuvable bloque aussi, quand toutes les colonnes que lit la
table sont sures : celles d'un schema de `conf/schema` ou d'un vrai fichier
Parquet. A la retraduction, le modele recoit la liste des vraies colonnes
que donne Spark et peut corriger le nom. Quand une table amont a ses
colonnes deduites d'un autre script (`RESIDENT`, `STORE`), elles peuvent
etre incompletes : la colonne introuvable est seulement signalee
("non bloquant"). La regle est la meme pendant la traduction : une
reponse du modele n'y est pas refusee pour une colonne que le controle
accepterait. `fill_conf.py`, lance avec les vrais fichiers dans `data/`,
remplit les schemas et rend ces colonnes sures.

Si les colonnes d'une table lue sont inconnues (ni schema, ni fichier, ni
script qui l'ecrit), Spark ne peut pas analyser la requete. Le controle le
dit ("non analysee par Spark"), au lieu de laisser croire que la table est
validee : ses erreurs n'apparaitront qu'au lancement du job, et ses types
de sortie ne sont pas calcules.

Un script dont une table n'est pas validee sort du job jusqu'a sa
correction : le job ne garde pas une ancienne version de ses requetes. Le
resume de `migrate.py` le signale ("retire du job").

Une table bloquee ici perd ses reponses du modele non verifiees dans
`work/expressions.json` : sans cela, la retraduction les lui redonnerait
comme piste, et il risquerait de recopier sa reponse fausse.

Si le modele l'avait completee et que personne n'a retouche sa requete
depuis, `migrate.py` la fait retraduire tout de suite, une seule fois, en
repartant du brouillon (`draft.sql`, avec ses trous) : le modele recoit
l'erreur de Spark et peut se corriger. Si elle est encore refusee, elle
reste en escalade et le resume la donne a corriger. Une table traduite
sans modele, ou dont la requete a ete corrigee a la main (le kit garde
l'empreinte de chaque requete qu'il ecrit), n'est jamais relancee.

Le controle refuse aussi :
- un prefixe `x.colonne` qui ne designe ni une CTE, ni une vue, ni un alias
  (le modele a deja ecrit `pos._qlik_order` pour un INLINE aliase `v(...)`) ;
- `monotonically_increasing_id()` ;
- les fonctions de date inventees (`add_month`, `dateadd`...) ;
- `FROM t WHERE (x)` : hors mode ANSI, Spark lit `WHERE` comme un alias de
  table et le filtre disparait sans erreur.

Les erreurs deja presentes dans le brouillon viennent du traducteur : on ne
les renvoie jamais au modele, sinon il cherche a corriger ce qui n'est pas
de son fait.

### Le cache d'expressions : `work/expressions.json`

Chaque expression traduite par le modele y est enregistree comme non
verifiee. Elle passe en verifiee quand la comparaison de sa table avec Qlik
est identique : elle est alors reutilisee telle quelle, sans modele, dans
les scripts suivants. Une sortie que le job n'a pas reecrite (etape en
echec) n'est pas comparee : elle date d'un lancement precedent. Une
requete retouchee a la main depuis la traduction ne valide pas les
reponses du modele. Avant cela, elle est juste proposee au modele comme
piste. Le fichier se corrige a la main ; attention a ne pas le supprimer en
vidant `work/`.

### Les reponses du modele reparees en Python

Avant de controler une reponse du modele, le kit corrige ce qui se deduit
sans risque, plutot que de lui redemander (dans `translate.py`) : un nom avec
`#` (ou une colonne connue avec un espace) laisse sans backticks,
`SELECT * EXCEPT (a, b)` (absent de Spark 3.5) remplace par la liste des
colonnes gardees, et `if(cond, x)` a deux arguments complete par `NULL`,
comme Qlik. Le modele n'est relance que pour le reste.

### Verifier que le modele repond

```bash
python tools/translate.py --selftest
```

Ca envoie une question simple a l'agent et affiche le code retour, les
outils que le modele a essaye d'utiliser, le texte recu et le SQL extrait.
`migrate.py` fait ce test avant de traduire, et s'arrete s'il echoue plutot
que d'enchainer les echecs. `translate.py` lit la sortie
`opencode run --format json` evenement par evenement, ce qui distingue un
modele qui a repondu, qui a tente un outil au lieu de repondre, ou qui n'a
produit que du raisonnement.

### Voir ce que le modele a fait

Dans `work/<script>/<table>/` :

| Fichier | Contenu |
|---|---|
| `prompt.md` | le prompt de la table |
| `model_1.log`, `model_2.log` | par tentative : la commande, le message envoye, la duree et la reponse brute |
| `query.attempt1.sql`, `query.attempt2.sql` | la requete extraite de chaque reponse |

La premiere ligne de chaque log, `# Note`, resume ce qui s'est passe ("a
tente d'utiliser un outil", "reponse VIDE", "aucune requete SQL
trouvee"...). La tentative est alors relancee une fois. Si rien n'aboutit,
la table passe en `erreur_modele` et sera reprise au prochain `migrate.py`.

## Corriger une table a la main

1. Corriger `work/<script>/<table>/query.sql` (jamais un
   `query.attemptN.sql`). Le `prompt.md` d'a cote contient le code Qlik et
   ce qu'on sait de la table.
2. Valider : `python tools/check.py work/<script> --only <table> --parse`.
3. Relancer `migrate.py`, qui assemble et compare.
4. Noter la cause dans la colonne `resolution` de `work/journal.csv`.

En dehors du tour automatique ci-dessus, une table en `escalade` n'est
jamais retraduite, pour ne pas ecraser une correction manuelle.
`translate.py --retry-escalated` la refait expres, en repartant du
brouillon : la correction manuelle de sa `query.sql` est alors perdue.

`check.py`, `translate.py`, `assemble.py` et `verify.py` prennent le
dossier de travail d'un script (`work/ventes`), mais retrouvent aussi ce
dossier si on leur passe le script (`script_qvd/ventes.qvs`) ou son nom
(`ventes`).

## Le socle

Le code genere n'importe que ceci :

| Import | Fichier du socle | Role |
|---|---|---|
| `from spark_foundation import pipeline` | `runner.py` | `@pipeline("<projet>")` ouvre la session, configure les logs, lit les options du job et trouve `sql/` a cote du job |
| `from spark_foundation.runner import script` | `runner.py` | `@script` sur un script : `--from` / `--only`, vues du script, logs, arret au premier echec |
| `from spark_foundation.tables import run_sql` | `tables.py` | calcule une table Qlik avec sa requete, apres avoir lu les entrees dont elle a besoin |
| `from spark_foundation.datasets import write_output` | `datasets.py` | ecrit une table dans une sortie de `conf/io.yml` |

Pour vos etapes en PySpark, `read_input("ventes")` (dans `datasets.py`)
renvoie l'entree en DataFrame. Avec `table="in_ventes"`, elle devient aussi
une table pour les requetes SQL, avec l'ordre de lecture Qlik, et
`in_ventes__file` donne la date du fichier (c'est ce que lit `FileTime()`).

Le reste du socle est technique et reserve aux data engineers : la session
et ses reglages (`session.py`), la lecture de `conf/io.yml` (`config.py`),
l'etat du job (`context.py`), les tables du catalogue pour OpenLineage
(`lineage.py`), la lecture et l'ecriture des fichiers (`access.py`). Les
transformations de DataFrame sont dans `pyspark_commons`.

`core/` et `commons/` reprennent, dans ce depot, les fichiers et les noms du
socle ; seul le nom des packages differe :

| Ici | Dans le socle | Contenu |
|---|---|---|
| `core/runner.py` | `runner.py` | `Pipeline`, `pipeline`, `script`, `parse_options` |
| `core/datasets.py` | `datasets.py` | `read_input`, `write_output`, `file_times` |
| `core/tables.py` | `tables.py` | `run_sql` |
| `core/session.py` | `session.py` | `get_session`, `active_session` |
| `core/config.py` | `config.py` | `load_io`, `section`, `entry` (resout `from_output`), `location`, `schema_columns`, `schema_ddl` |
| `core/context.py` | `context.py` | `JobState`, `state`, `start_job`, `script_scope`, `log` |
| `core/lineage.py` | `lineage.py` | `ensure_database`, `table_name`, `materialize` |
| `core/access.py` | `access.py` | `read_config`, `read_excel`, `write_config`, `input_location`, `output_location` |
| `commons/` | `pyspark_commons` | `add_load_order`, `cast_void_to_string`, `drop_columns` |

Les tests lancent le code genere sur `tests/fakelib/spark_foundation/`, qui
expose `core/` sous le nom du socle. Si l'API du socle change, on reporte le
changement dans `core/`, dans cette doublure et dans
`tools/job_template.py`.

## Travailler sur le kit

Avant chaque commit :

```bash
ruff format . && ruff check . && mypy     # configuration dans pyproject.toml
python -m pytest                          # tout doit passer
```

Pas d'accent dans les fichiers, commentaires compris. Toute fonction a une
docstring et des annotations de type : `ruff` (regles `D1` et `ANN`) et
`mypy` le verifient.

```bash
pip install pyspark pyyaml pandas pyarrow openpyxl pytest
python -m pytest -m "not spark"     # rapide, sans Spark
python -m pytest                    # tout, Spark et Java requis
```

`tests/demo/` est un petit projet complet : deux scripts chaines
(`synthese.txt` lit la sortie de `ventes.qvs`), un calendrier sans extension
(`Min`/`Max` de dates, `LET` avec `Peek`, `AUTOGENERATE`, `WeekYear`), un
`io.yml` a 3 espaces avec des commentaires, et les exports Qlik attendus.
Chaque test le copie dans un dossier temporaire. Les tests marques `spark`
lancent `migrate.py` de bout en bout, puis le job genere, et verifient ses
logs, ses tables dans le catalogue et la reprise apres un echec. Sans
pyspark, ils sont sautes.

| Outil | Role |
|---|---|
| `migrate.py` | la seule commande a lancer |
| `normalize.py` | noms des scripts et des fichiers de donnees |
| `fill_conf.py` | complete `conf/io.yml` et les schemas |
| `prepare.py` | analyse du script, sections, chemins, prompts, rapport |
| `qlik2sql.py` | traduction Qlik -> Spark SQL sans modele |
| `translate.py` | completion des `TODO(model)` par le modele |
| `check.py` | controles, et analyse Spark avec `--parse` |
| `assemble.py` | copie des requetes d'un script dans `src/sql/` |
| `orchestrate.py` | ordonne les scripts, ecrit l'etape 010 (et le job s'il manque) |
| `job_template.py` | texte du job et de l'etape 010 |
| `verify.py` | lance le job en entier (sans `--only`) et compare avec `data/qlik_outputs/` ; `migrate.py` lance le job une seule fois puis compare chaque script |
| `compare.py` | comparaison de deux tables |
| `conf.py` | lecture de `conf/io.yml` et des schemas |
| `journal.py` | tenue de `work/journal.csv` |

## Limites connues

- **Ce qui reste au modele** : `Peek` recursif (cumuls), `FirstSortedValue`,
  `ConvertToLocalTime` sans fuseau explicite, `RIGHT`/`INNER KEEP` vers une
  table d'une autre section, boucles sur `FileList`, `SQL SELECT` avec
  jointure ou `WHERE` (le dialecte depend de la base).
- **`QUALIFY`, `NullAsValue`, `SET NullInterpret/NullValue`** : signales, a
  completer a la main.
- **L'ordre des lignes apres un `JOIN`** : Qlik ne le documente pas. La
  regle "la gauche, puis les lignes sans correspondance" reste a confirmer
  sur de vrais exports, avec `verify.py --strict-order`. Comme
  `_qlik_order` n'est pas ecrite dans les sorties, la comparaison ne
  verifie pas l'ordre des lignes.
- **`FieldValue('F', RecNo())`** numerote les valeurs dans l'ordre trie,
  alors que Qlik suit l'ordre de chargement.
- **`Date(x)` sur un nombre** (date Qlik en jours) n'est pas converti.
- **Une valeur vide dans un CSV** est une chaine vide pour Qlik, NULL pour
  Spark : `IsText`, `Len(x) = 0` ou `x = ''` peuvent donner un autre
  resultat. Ecrire `IsNull(x) or Len(Trim(x)) = 0`, ou le traiter dans la
  conf de lecture.
- **Le typage des dates repose sur `conf/schema`.** Une colonne de dates
  sans type declare n'est pas reconnue comme date par `Min`, `Max` ou
  `Peek`, et c'est le job qui plantera sur `date - date`.
- **L'ordre entre scripts** ne tient compte que des fichiers, pas des tables
  en base de donnees.
- **Les tables du catalogue sont ecrasees a chaque lancement** : on n'a que
  la derniere version. Un overwrite Parquet n'est pas atomique, un echec au
  milieu laisse la table incomplete jusqu'au lancement suivant (Delta ou
  Iceberg le seraient).
- **Teste en local** avec PySpark 3.5.8 (la cible) et 4.0.1, Java 21, sur la
  doublure du socle : pas encore sur le cluster, ni avec le vrai
  `spark_foundation`, ni avec un vrai opencode.
