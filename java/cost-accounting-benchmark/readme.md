# 原価計算ベンチマーク（Java版）

## 開発環境構築

````bash
./gradlew cleanEclipse eclipse
````

Eclipse上で、Doma2のDAOのsqlファイルが見つからなくてコンパイルエラーになるかもしれない。
その場合、src/main/resourcesのビルドパス（出力先）を修正する。（bin/mainをbin/defaultにする）



## ソースの生成

一部のソースファイルは、プログラムで生成する。

テーブル定義（table.xlsx）が変更になった場合は、ソースを生成し直す。

- PostgresqlDdlGenarator
- OracleDdlGenerator
  - table.xlsxを読み込み、ddlファイル（create table）を作成する。
- EntityGenerator
  - table.xlsxを読み込み、Doma2のEntityクラスのソースファイルを生成する。



## 実行用アーカイブファイル生成方法

```bash
./gradlew distTar
ls build/distributions/
```

実行環境で解凍する。

```bash
tar xf example-nedo.tar
cd example-nedo/bin
chmod +x *.sh
```

binには実行用シェルとプロパティーファイルがある。

シェルやプロパティーファイルの説明は、bin/readme.mdを参照。