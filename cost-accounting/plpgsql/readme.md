# 原価計算ベンチマーク（PL/pgSQL版）

## 実行環境構築

PostgreSQLで以下のsqlファイル（create procedure等）を実行する。

- bb_measurement.sql
- bb_measurement_value.sql
- bench_batch.sql



## 実行方法

psqlから以下のようにして実行する。

```sql
\timing on
call bench_batch('2020-09-15', '1,2,3');
```

第1引数はバッチ基準日（初期データ作成時に指定したものと同じ）。

第2引数は処理対象の工場ID。カンマ区切りで複数指定可能。省略時は全工場が対象。

第3引数はコミット率。100で常にコミット、0で常にロールバック。省略時は100。

