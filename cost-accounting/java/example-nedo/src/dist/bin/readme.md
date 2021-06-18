# プロパティーの説明

実行時に各処理で使用する値（JDBCの接続先など）をプロパティーとして指定する。

プロパティーは、プロパティーファイルに記述する他に、javaコマンドの引数で指定できる。

- プロパティーファイルで指定する場合、javaコマンドに `-Dproperty="プロパティーファイルのパス"` を指定する。
- 個別に指定する場合、javaコマンドの-Dオプションで指定する。（例 `-Donline.jdbc.type=3` ）



## 共通

初期データ作成処理・バッチ処理・オンライン処理で使用するパラメーター。

- jdbc.url
  - JDBCの接続URL
- jdbc.user, jdbc.password
  - DBに接続するユーザー・パスワード
- *.jdbc.type
  - JDBC実行に使用するライブラリー
    - 1: Doma2
    - 2: 生JDBC
    - 3: コネクション（トランザクション管理）はDoma2を使用し、SQLの実行は生JDBCを使う方式
- decimal.scale
  - BigDecimalの除算時の小数点以下の桁数



## init

初期データ作成処理で使用するパラメーター。

- doc.dir
  - ドキュメント（度量衡マスターのデータ（measurement.xlsx））が置いてある場所
- init.batch.date
  - データを作成する基準日
    - バッチ処理（およびオンライン処理）を実行する際の日付を指定する。その日付の初期データが作成される。
- init.factory.size
  - 工場数（factory_masterに生成するレコード数）
    - バッチ処理やオンライン処理は工場毎に分散して処理する想定。
- init.item.product.size
  - 品目マスターの「製品」の数（item_masterに生成するproductのレコード数）
    - 製品は、オンライン処理の新商品追加業務で増えていく。
- init.item.work.size
  - 品目マスターの「中間材」の数（item_masterに生成するwork_in_processのレコード数）
- init.item.material.size
  - 品目マスターの「原料」の数（item_masterに生成するraw_materialのレコード数）
- init.item.manufacturing.size
  - 製造品目数（item_manufacturing_masterに生成するレコード数）
- init.parallelism
  - 初期データ生成処理の並列実行数（最大スレッド数）
    - 省略時はCPU数



※品目構成マスター（item_construction_master）は、品目マスターからランダムに生成するので、レコード数は指定しない。
（品目構成マスターは、オンライン処理の原材料変更業務で増減していく）



## batch

バッチ処理で使用するパラメーター。

- batch.execute.type
  - バッチ処理の実行方式
    - 1: 全工場を直列に実行する（シングルスレッド）
    - 2: 工場毎に並列で実行する（マルチスレッド）



## online

オンライン処理で使用するパラメーター。

- online.log.file
  - オンライン処理が出力するログファイルのパス
    - オンライン処理では、処理したトランザクション数をカウントするのに使う目的で、ログファイルを出力する。
    - オンライン処理はマルチスレッドで処理するので、ログファイルは各スレッドが出力する。このため、ファイル名には `%d` を含める必要がある。（`%d` の部分がスレッド番号に置換される）
- online.task.ratio.タスク名
  - タスクを実行する割合
    - 百分率ではなく、online.task.ratio.*の全合計に対する値（比率）を指定する。
    - 0にすると、そのタスクは実行されない。
- online.task.sleep.タスク名
  - タスク実行後に一時停止（スリープ）する時間[秒]

