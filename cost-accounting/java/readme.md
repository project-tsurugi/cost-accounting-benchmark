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



### バッチ処理

- BenchBatch



### オンライン処理

- BenchOnline