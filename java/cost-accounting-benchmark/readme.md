# 原価計算ベンチマーク（Java版）

## 開発環境構築

Eclipseのプロジェクトは以下のコマンドで作成する。

````bash
./gradlew cleanEclipse eclipse
````



## ソースの生成

一部のソースファイルは、プログラムで生成する。

テーブル定義（table.xlsx）が変更になった場合は、ソースを生成し直す。

- PostgresqlDdlGenarator
- OracleDdlGenerator
- TsurugiDdlGenerator
  - table.xlsxを読み込み、ddlファイル（create table）を作成する。
  - ただし、initdata.shを使用する場合はその中でテーブルを作成するので、ddlファイルを使ってテーブルを作成する必要は無い。
- EntityGenerator
  - table.xlsxを読み込み、Entityクラスのソースファイルを生成する。



## 実行用アーカイブファイル生成方法

```bash
./gradlew distTar
ls build/distributions/
```

実行環境で解凍する。

```bash
tar xf cost-accounting-benchmark.tar.tar
cd cost-accounting-benchmark.tar/bin
chmod +x *.sh
```

binには実行用シェルとプロパティーファイルがある。

シェルやプロパティーファイルの説明は、bin/readme.mdを参照。

