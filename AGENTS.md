# Qlik Sense -> Spark SQL 3.5.8 migration

Each request contains everything needed. No need to read documentation;
never write files. Think in English; SQL comments in French. Answer with one
```sql block, in the exact format the request asks, and nothing else.

## Qlik in short
- A script runs top to bottom and builds in-memory tables. `NAME:` names the table.
- `LOAD ... FROM file` reads a file; `LOAD ... RESIDENT T` re-reads table T.
- Stacked LOADs (a LOAD without FROM above another) read BOTTOM-UP.
- A LOAD with exactly the fields of an existing table is APPENDED to it,
  unless prefixed by `NoConcatenate` (the prompt flags it as AUTO-CONCATENATION).
- `JOIN` never names its key: Qlik joins on ALL same-named fields.
  `JOIN` alone = OUTER. KEEP = join used as a filter, tables stay separate.
- `CONCATENATE (T)` = union into T, missing columns become null.
- `Mapping LOAD` + `ApplyMap('M', x, default)` = lookup. Without a default,
  ApplyMap returns x itself. First value wins for duplicate keys.
- `ORDER BY` only matters for `Peek`, `Previous`, `RowNo`, `Above`.
- Booleans are -1 (true) and 0 (false); any non-zero number is true.
- `'l''ete'` in Qlik is `"l'ete"` in Spark SQL (never `''` in Spark).
- `STORE` writes, `DROP TABLE` frees memory (not translated).
- `Date(x, 'fmt')`, `Num(x, 'fmt')` are display formats only: keep the type.
- `AddMonths(d, n)` is `add_months(d, n)` (plural). `add_month`, `month_add`
  and `dateadd` do not exist in Spark. `AddYears(d, n)` is
  `add_months(d, 12 * n)`.
- A Qlik date is a number of days, the time is its decimal part. Spark
  `floor()` refuses a date or timestamp: `Floor(d)` / `Date(Floor(d))` is
  `CAST(d AS DATE)`, `DayStart(d)` is `CAST(CAST(d AS DATE) AS TIMESTAMP)`,
  `Floor(d, 1/24)` keeps the hour, `Frac(d)` is the time as a day fraction.
- `d1 - d2` in Qlik is a number of days. In Spark it is an INTERVAL, which
  `floor()`, `round()` and `* 24` refuse: write `datediff(d1, d2)` for dates,
  and for timestamps
  `datediff(CAST(t1 AS DATE), CAST(t2 AS DATE)) + (secs(t1) - secs(t2)) / 86400D`
  with secs = hour * 3600 + minute * 60 + second (never unix_timestamp: a
  daylight saving day lasts 23 hours).
- `d + n` days: `d + n` only for a DATE and an integer n. A TIMESTAMP + 1 is
  refused: `t + INTERVAL 1 DAYS`; a fraction (1/24) needs a timestamp:
  `CAST(d AS TIMESTAMP) + INTERVAL 3600 SECONDS`.
- `Date(n)` of a number is `date_add(DATE'1899-12-30', n)`, never
  `CAST(n AS DATE)`.
- An empty string `''` is not null in Qlik.

## Spark SQL 3.5.8 (not Databricks, not Spark 4)
- No `SELECT * EXCEPT (col)`: list the columns you keep.
- `instr(str, substr)` has 2 arguments only. Nth occurrence:
  `length(substring_index(str, substr, n)) + 1`.
- No `regexp_last`, no `>>`/`<<` operators (`shiftright`, `shiftleft`).
- Qlik `If(cond, x)` (2 arguments) is `CASE WHEN cond THEN x END`: Spark
  `if` needs 3 arguments.
- Qlik functions do not exist in Spark: `IsNum(x)` is
  `try_cast(x AS DOUBLE) IS NOT NULL`, `RangeMax(a, b)` is `greatest(a, b)`.
- A column name with a special character (#, space, dot) stays in
  backticks: `TST#TEST`.
