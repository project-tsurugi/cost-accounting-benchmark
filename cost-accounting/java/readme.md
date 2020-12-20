# 原価計算ベンチマーク（Java版）

## 開発環境構築

````bash
./gradlew cleanEclipse eclipse
````

Eclipse上で、Doma2のDAOのsqlファイルが見つからなくてコンパイルエラーになるかもしれない。
その場合、src/main/resourcesのビルドパス（出力先）を修正する。



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
実行用シェルの引数としてプロパティーファイルを指定する。
プロパティーファイル内には主にJDBC接続のための情報が書かれているので、環境に応じて用意する。



## 実行方法

以下のクラスを実行する。

### 準備用

- PostgresqlDdlGenarator
- OracleDdlGenerator
  - table.xlsxを読み込み、ddlファイル（create table）を作成する。
- EntityGenerator
  - table.xlsxを読み込み、Doma2のEntityクラスのソースファイルを生成する。



### 初期データ作成

1. InitialData01MeasurementMaster
   - measurement.xlsxを読み込み、度量衡マスターにinsertする。
2. InitialData02FactoryMaster
   - 工場マスターのデータを生成する。
3. InitialData03ItemMaster
   - 品目マスター・品目構成マスターのデータを生成する。
4. InitialData04ItemManufacturingMaster
   - 製造品目マスターのデータを生成する。
5. InitialData05CostMaster
   - 原価マスターのデータを生成する。
6. InitialData06Increase
   - データを増幅する（件数を増やす）。

アーカイブの中に実行用シェルがある。

```bash
./initdata.sh プロパティーファイル
```



### バッチ処理

- BenchBatch
  - 第1引数はバッチ基準日（初期データ作成時に指定したものと同じ）
  - 第2引数は処理対象の工場ID。カンマ区切りで複数指定可能。省略時または「all」は全工場が対象。
  - 第3引数はコミット率。100で常にコミット、0で常にロールバック。省略時は100。

アーカイブの中に実行用シェルがある。

```bash
./exec-batch.sh プロパティーファイル
```



### オンライン処理

- BenchOnline