# cost accounting benchmark (Java)

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * iceaxe

## How to build

```bash
./gradlew distTar
ls build/distributions/
```

### How to deploy

```bash
tar xf cost-accounting-benchmark.tar.gz
cd cost-accounting-benchmark/bin
chmod +x *.sh
```

## How to generate source files

Some source files are generated programmatically.
If the table definition ([table.xlsx](src/main/resources/table.xlsx)) is changed, generate the source again.

- PostgresqlDdlGenarator
- OracleDdlGenerator
- TsurugiDdlGenerator
  - Read table.xlsx and create a ddl file (create table).
  - However, when using `initdata.sh`, the table is created within it, so there is no need to create the table using the ddl file.
- EntityGenerator
  - Read table.xlsx and generate Entity class source file.

