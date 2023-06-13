# 原価計算ベンチマーク実行方法

各処理を実行する為のシェルが用意されている。
シェルには引数としてプロパティーファイルを指定する。

プロパティーファイル内にはDB接続の情報（や各処理で必要とする値）等が書かれているので、環境に応じて用意する。



## 初期データ作成処理

バッチ処理やオンライン処理で使用する初期データを作成する。

```bash
./run.sh プロパティーファイル createTable
./run.sh プロパティーファイル createTestData
```

生成するデータ量等はプロパティーファイル内で指定する。

何度でも実行可能。（既存データをtruncateして新しいデータをinsertする）

これらのシェルは以下のプログラムを実行する。

1. InitialData00CreateTable
   - テーブルを作成する。
2. InitialData01MeasurementMaster
   - measurement.xlsxを読み込み、度量衡マスターにinsertする。
3. InitialData02FactoryMaster
   - 工場マスターのデータを生成する。
4. InitialData03ItemMaster
   - 品目マスター・品目構成マスターのデータを生成する。
5. InitialData04ItemManufacturingMaster
   - 製造品目マスターのデータを生成する。
6. InitialData05CostMaster
   - 原価マスターのデータを生成する。
7. InitialData06StockHistory
   - 在庫履歴をクリアする。
8. InitialData07ResultTable
   - 結果テーブルをクリアする。



## バッチ処理

バッチ処理を実行する。

```bash
./run.sh プロパティーファイル executeBatch [引数]
```

バッチ処理は、指定された工場の処理が終わると終了する。

このシェルは以下のプログラムを実行する。

- CostAccountingBatch
  - 第1引数はバッチ基準日（初期データ作成時に指定した日付と同じものを指定する）
    - 省略時はプロパティーファイルのinit.batch.date（初期データ作成の日付）。
  - 第2引数は処理対象の工場ID。カンマ区切りで複数指定可能。「1-3」のようにハイフンで範囲を指定可能。
    - 省略時または「all」は全工場が対象。
  - 第3引数はコミット率。トランザクションに対し、ランダムでコミットするかロールバックする。
    100で常にコミット、0で常にロールバック。
    - 省略時は100。



## オンライン処理

オンライン処理を実行する。

```bash
./run.sh プロパティーファイル executeOnline [引数]
```

オンライン処理は無限に動き続けるので、停止させる場合はCtrl+Cを入力する。（Ctrl+Cが入力されると、各スレッドを正常に終了させ、ログファイルを出力している場合はそれをクローズして終了する）

このシェルは以下のプログラムを実行する。

- CostAccountingOnline
  - 第1引数は実行基準日（バッチの基準日と同じものを指定する）
    - 省略時はプロパティーファイルのinit.batch.date（初期データ作成の日付）。



# プロパティーの説明

実行時に各処理で使用する値（JDBCの接続先など）をプロパティーとして指定する。

プロパティーは、プロパティーファイルに記述する他に、javaコマンドの引数で指定できる。

- プロパティーファイルで指定する場合、javaコマンドに `-Dproperty="プロパティーファイルのパス"` を指定する。
- 個別に指定する場合、javaコマンドの-Dオプションで指定する。（例 `-Donline.dbmanager.type=1` ）



## 共通

初期データ作成処理・バッチ処理・オンライン処理で使用するパラメーター。

- jdbc.url
  - JDBCの接続URL
- jdbc.user, jdbc.password
  - DBに接続するユーザー・パスワード
- tsurugi.endpoint
  - Tsurugiに接続する場合のエンドポイント
- tsurugi.user, tsurugi.password
  - Tsurugiに接続する場合のユーザー・パスワード
- *.dbmanager.type
  - SQL実行に使用するライブラリー
    - `JDBC`
    - `iceaxe`
    - `tsubakuro`
- decimal.scale
  - BigDecimalの除算時の小数点以下の桁数



## init

初期データ作成処理で使用するパラメーター。

- doc.dir
  - ドキュメント（テーブル定義（table.xlsx）や度量衡マスターのデータ（measurement.xlsx））が置いてある場所。
  - これが指定されていない場合、クラスパス内のxlsxファイルが使われる。
- init.batch.date
  - データを作成する基準日
    - バッチ処理（およびオンライン処理）を実行する際の日付を指定する。その日付の初期データが作成される。
- init.factory.size
  - 工場数（factory_masterに生成するレコード数）
    - バッチ処理は工場毎に分散して処理する想定。
- init.item.product.size
  - 品目マスターの「製品」の数（item_masterに生成するproductのレコード数）
    - 製品は、オンライン処理の新商品追加業務で増えていく。
- init.item.work.size
  - 品目マスターの「中間材」の数（item_masterに生成するwork_in_processのレコード数）
- init.item.material.size
  - 品目マスターの「原料」の数（item_masterに生成するraw_materialのレコード数）
- init.item.manufacturing.size
  - 製造品目数（item_manufacturing_masterに生成するレコード数）
- init.cost.factory.per.material
  - 原価マスターの原料毎の対象工場数（cost_masterに生成する原料毎のレコード数）
  - デフォルトは工場数の50％
- init.dbmanager.multi.session
  - スレッド毎にTsurugiのセッションを作るかどうか。デフォルトはtrue（セッションを作る）
- init.parallelism
  - 初期データ生成処理の並列実行数（最大スレッド数）
    - 省略時はCPU数
    - Tsurugiが対象で、init.dbmanager.multi.sessionがtrueの場合、DBサーバーのセッション数の上限を超えない値にする必要がある。



※品目構成マスター（item_construction_master）は、品目マスターからランダムに生成するので、レコード数は指定しない。
（品目構成マスターは、オンライン処理の原材料変更業務で増減していく）



## batch

バッチ処理で使用するパラメーター。

- batch.execute.type
  - バッチ処理の実行方式
    - sequential-single-tx: 全工場を直列に実行する（全工場を1トランザクションで処理）
    - sequential-factory-tx: 全工場を直列に実行する（工場毎にコミット）
    - parallel-single-tx: 工場毎に並列で実行する（全工場を1トランザクションで処理）
    - parallel-factory-tx: 工場毎に並列で実行する（工場毎にコミット）（工場全体で1セッション）
    - parallel-factory-session: 工場毎に並列で実行する（工場毎にコミット）（工場毎にセッション）
- batch.factory.order
  - 工場の処理順
    - `none`: 順序なし（工場ID順）（デフォルト）
    - `count-desc`: 処理対象件数の降順
    - `count-asc`: 処理対象件数の昇順
- batch.thread.size
  - 並列で実行する場合のスレッド数
    - 省略時や0以下の場合、工場数
- batch.jdbc.isolation.level
  - JDBCのトランザクション分離レベル
    - `READ_COMMITTED`
    - `SERIALIZABLE`
- batch.tsurugi.tx.option
  - Tsurugiのトランザクションオプション
    - OCC
    - LTX
    - LTX[n]：工場IDを指定する。指定された工場はLTX、それ以外の工場はOCC



## online

オンライン処理で使用するパラメーター。

- online.console.type
  - オンライン処理の結果を画面表示する先
    - `NULL`
      - 出力しない
    - `STDOUT`（デフォルト）
      - 標準出力に出力する
- online.dbmanager.multi.session
  - Tsurugiでスレッド毎にセッションを生成するかどうか
  - デフォルトはtrue（生成する）
- online.jdbc.isolation.level
  - JDBCのトランザクション分離レベル
    - `READ_COMMITTED`
    - `SERIALIZABLE`
- online.tsugugi.tx.option
  - Tsurugiのトランザクションオプション
    - `OCC`
    - `LTX`
      - 照会タスクの場合はRTXとして扱う
    - `MIX`
      - OCCで開始し、リトライ時はLTX
- online.cover.rate
  - オンライン処理でランダムにIDを決定する元となる一覧のカバー率（デフォルトは100）
- online.type
  - オンライン処理の実行形式
    - `random`
      - スレッド毎にランダムにタスクを実行する
    - `schedule`
      - スレッド毎にタスクを固定する（一定時間内の実行回数を指定する）

### online.typeが`random`の場合

- online.log.file
  - オンライン処理が出力するログファイルのパス
    - オンライン処理はマルチスレッドで処理するので、ログファイルは各スレッドが出力する。このため、ファイル名には `%d` を含める必要がある。（`%d` の部分がスレッド番号に置換される）
- online.random.thread.size
  - スレッド数
- online.random.task.ratio.タスク名
  - タスク（業務）を実行する割合
    - 百分率ではなく、online.task.ratio.*の全合計に対する値（比率）を指定する。
    - 0にすると、そのタスクは実行されない。
- online.random.task.sleep.タスク名
  - タスク実行後に一時停止（スリープ）する時間[秒]

### online.typeが`schedule`の場合

- online.schedule.thread.size.タスク名
  - オンライン処理のタスクのスレッド数
- online.schedule.execute.per.minute.タスク名
  - オンライン処理の一定時間内の実行回数[タスク数/分]
    - -1の場合、間隔を空けずに次のタスクを実行する
- periodic.schedule.thread.size.タスク名
  - 定期実行バッチのスレッド数
- periodic.schedule.interval.タスク名
  - 定期実行バッチの実行間隔[秒]
- periodic.schedule.update-stock.split.size
  - 在庫の追加処理のスレッド数



# その他の機能

## バッチ処理一括実行

条件を変更しつつバッチ処理を実行し、記録（実行時間等）をファイルに出力する。

```bash
./run.sh プロパティーファイル executeBatchCB
```

一括実行用に、プロパティーファイルに以下の設定を行う。

- batch-command.label
  - ラベル
- batch-command.execute.type
  - 実行方式（カンマ区切りで複数指定）
    - `sequential-single-tx` … 全工場を直列実行（全体で1トランザクション）
    - `sequential-factory-tx` … 全工場を直列実行（工場毎にトランザクション）
    - `parallel-single-tx` … 工場毎に並列実行（全体で1トランザクション）
    - `parallel-factory-tx` … 工場毎に並列実行（工場毎にトランザクション）
    - `parallel-factory-session` … 工場毎に並列実行（工場毎に別セッション（1セッション1トランザクション））
- batch-command.factory.list
  - 処理対象工場
    - `1-8` … 工場IDが1～8
    - `all` … 全工場
- batch-command.factory.order
  - 工場の処理順
    - `none`: 順序なし（工場ID順）（デフォルト）
    - `count-desc`: 処理対象件数の降順
    - `count-asc`: 処理対象件数の昇順
- batch-command.thread.size
  - 並列で実行する場合のスレッド数（カンマ区切りで複数指定）
    - 0以下の場合は工場数
- batch-command.isolation.level
  - トランザクション分離レベル（カンマ区切りで複数指定）
    - `READ_COMMITTED`
    - `SERIALIZABLE` （TsurugiはSERIALIZABLEのみ）
- batch-command.tx.option
  - トランザクションオプション（カンマ区切りで複数指定）（JDBCでは無視）
    - `OCC`
    - `LTX`
- batch-command.execute.times
  - 上記の組み合わせのそれぞれを実行する回数。デフォルトは1
- batch-command.diff.dir
  - result_tableのダンプを出力するディレクトリー。バッチ処理を複数回実行する場合、最初にダンプしたファイルとの差異が無いかどうかをチェックする
  - このプロパティーを指定しない場合、ダンプファイルを出力せず、比較も行わない
- batch-command.result.file
  - 実行結果（処理時間やリトライ回数等）を出力するcsvファイルのパス
- batch-command.with.initdata
  - バッチ処理の実行前に初期データ作成処理（データの初期化・再作成）を行うかどうか
  - デフォルトはfalse（初期化しない）
- batch-command.with.online
  - バッチ処理の実行と同時にオンライン処理を実行するかどうか
  - デフォルトはfalse（オンライン処理を実行しない）
  - 同時に実行するオンライン処理のパラメーターは、通常のオンライン処理のものと同じ
- batch-command.online.cover.rate
  - オンライン処理でランダムにIDを決定する元となる一覧のカバー率（カンマ区切りで複数指定）
- batch-command.online.report
  - オンライン処理の実行結果（処理時間やリトライ回数等）を出力するmdファイルのパス
- batch-command.online.compare.base
  - 他で実行したオンラインの実行結果ファイルのパス
    - 実行結果ファイルに、このファイルに出力されている実行時間と今回の実行時間の比較を出力する

#### バッチ一括実行用プロパティーの例

```properties
## batch-command
batch-command.execute.type=parallel-factory-session
batch-command.tx.option=OCC,LTX
batch-command.execute.times=1
batch-command.factory.list=all
batch-command.factory.order=none
batch-command.thread.size=-1
batch-command.diff.dir=/tmp/cost-accounting-benchmark/diff
batch-command.label=medium.online_var
batch-command.result.file=/tmp/cost-accounting-benchmark/batch-command/tsurugi.medium.online_var.csv
batch-command.with.initdata=true
batch-command.with.prebatch=true
batch-command.with.online=true
batch-command.online.cover.rate=100,25
batch-command.online.report=/tmp/cost-accounting-benchmark/batch-command/tsurugi.medium.online_var.online-app.md
batch-command.online.compare.base=/tmp/cost-accounting-benchmark/online-command/tsurugi.medium.online-app.md
```



## オンライン処理一括実行

条件を変更しつつオンライン処理を実行し、記録（実行時間等）をファイルに出力する。

```bash
./run.sh プロパティーファイル executeOnlineCB
```

一括実行用に、プロパティーファイルに以下の設定を行う。

- online-command.label
  - ラベル
- online-command.isolation.level
  - トランザクション分離レベル（カンマ区切りで複数指定）
    - `READ_COMMITTED`
    - `SERIALIZABLE` （TsurugiはSERIALIZABLEのみ）
- online-command.tx.option
  - トランザクションオプション（カンマ区切りで複数指定）（JDBCでは無視）
    - `OCC`
      - 常にOCC（リトライもOCC）
    - `LTX`
      - 常にLTX（リトライもLTX）
    - `MIX3-1`
      - OCCで3回実行した後、LTXで1回実行する
    - `MIX3-1:LTX`
      - オンライン処理はOCCで3回実行した後、LTXで1回実行する。定期実行バッチは常にLTX
- online-command.cover.rate
  - オンライン処理でランダムにIDを決定する元となる一覧のカバー率（カンマ区切りで複数指定）
- online-command.execute.times
  - 上記の組み合わせのそれぞれを実行する回数。デフォルトは1
- online-command.with.initdata
  - オンライン処理の実行前に初期データ作成処理（データの初期化・再作成）を行うかどうか
  - デフォルトはfalse（初期化しない）
- online-command.with.prebatch
  - オンライン処理の実行前にデータ準備としてバッチ処理を実行するかどうか
  - デフォルトはfalse（バッチ処理を実行しない）
- online-command.execute.time
  - オンライン処理を実行する時間（秒）
    - バッチ処理を実行する代わりにこの時間スリープし、その間に別スレッドで各オンライン処理を実行する
- online-command.result.file
  - 実行結果（処理時間やリトライ回数等）を出力するcsvファイルのパス
- online-command.online.report
  - オンライン処理の実行結果（処理時間やリトライ回数等）を出力するmdファイルのパス

#### オンライン一括実行用プロパティーの例

```properties
## online-command
online-command.label=medium
online-command.tx.option=OCC,OCC:LTX,LTX,MIX3-1,MIX3-1:LTX
online-command.cover.rate=100,75,50,25
online-command.execute.times=1
online-command.with.initdata=true
online-command.with.prebatch=true
online-command.execute.time=100
online-command.result.file=/tmp/cost-accounting-benchmark/online-command/tsurugi.medium.csv
online-command.online.report=/tmp/cost-accounting-benchmark/online-command/tsurugi.medium.online-app.md
```



## SQL実行時間計測処理

原価計算ベンチマークで使われるSQLの一部の実行時間を計測し、結果ファイルに出力する。

```bash
./run.sh プロパティーファイル createTable # 事前にテーブルを作っておく必要がある
./run.sh プロパティーファイル time
```

実行時間計測用の値をプロパティーファイルで指定する。

- time-command.dbmanager.type
  - `JDBC`
  - `iceaxe`
  - `tsubakuro`
- time-command.isolation.level
  - トランザクション分離レベル（カンマ区切りで複数指定）
    - `READ_COMMITTED`
    - `SERIALIZABLE` （TsurugiはSERIALIZABLEのみ）
- time-command.tx.option
  - トランザクションオプション（カンマ区切りで複数指定）（JDBCでは無視）
    - `OCC`
    - `LTX`
- time-command.<テーブル名>.size
  - テーブルにinsertする件数（品目ID（i_id）の数）
- time-command.<テーブル名>.size.adjust.start, time-command.<テーブル名>.size.adjust.end
  - item_masterの場合、item_masterにinsertする日付の範囲
    - nの場合、基本となる日付のnヶ月後のレコードまで作成する
      - 「-1, 2」の場合、i_id毎に、1ヶ月前～2ヶ月後の4レコード作成される
    - デフォルトは0
- time-command.<テーブル名>
  - item_masterの計測を実行するかどうか。デフォルトはtrue
  - 特定のSQLだけ実行したい場合はfalseにする
- time-command.<テーブル名>.＜SQL名＞
  - trueにすると、そのSQLを実行する。デフォルトはtime-command.<テーブル名>の設定値
- time-command.result.file
  - 実行結果（処理時間等）を出力するファイルのパス

#### SQL実行時間計測用プロパティーの例

```properties
## time-command
time-command.dbmanager.type=2
time-command.isolation.level=SERIALIZABLE
time-command.tx.option=OCC, LTX
time-command.item_master.size=10_000
time-command.item_master.size.adjust.start=-1
time-command.item_master.size.adjust.end=1
time-command.cost_master.size=30_000
time-command.result_table.size=10_000
time-command.result.file=/tmp/cost-accounting-benchmark.time.tsurugi.csv
```

### 結果ファイルの内容

#### 例

```
dbmsType, option, table, size, sql, elapsed[ms], begin[ms], main[ms], commit[ms], tryCount
ICEAXE,LTX,item_master,10000[-1:1],insert,4610,0,4515,95,1
ICEAXE,LTX,item_master,10000[-1:1],selectAll,277,1,266,10,1
ICEAXE,LTX,item_master,10000[-1:1],selectAll,229,0,219,10,1
ICEAXE,LTX,item_master,10000[-1:1],selectAll,253,1,242,10,1
ICEAXE,LTX,item_master,10000[-1:1],select(point),1919,0,1914,5,1
ICEAXE,LTX,item_master,10000[-1:1],select(point),1828,1,1822,5,1
ICEAXE,LTX,item_master,10000[-1:1],select(point),1964,1,1959,4,1
ICEAXE,LTX,item_master,10000[-1:1],select(range-scan),2309,0,2247,62,1
ICEAXE,LTX,item_master,10000[-1:1],select(range-scan),2535,1,2472,62,1
ICEAXE,LTX,item_master,10000[-1:1],select(range-scan),2602,0,2542,60,1
ICEAXE,LTX,item_master,10000[-1:1],deleteAll,333,0,265,68,1
```

- elapsed
  - 全体の実行時間
  - begin + main + commit
- begin
  - 実行を開始してから最初のSQL実行を行うまでの時間（主にトランザクションを開始する時間）
    - JDBCの場合は0（コネクション作成はmainに含まれる）
    - Tsubakuroの場合はトランザクション開始のFutureResponseをawaitするので、その時間
    - Iceaxeの場合はトランザクション開始のFutureResponseを作る時間（FutureResponse.getはmainで行う）
- main
  - SQL実行本体（commit手前まで）の時間
- commit
  - コミット処理の時間
- tryCount
  - 試行回数（リトライした場合は2以上になる）

