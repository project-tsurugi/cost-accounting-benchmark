# 原価計算ベンチマーク（Java版）データベースアクセスポリシー

Java版の原価計算ベンチマークのデータベースアクセス関連クラスの実装方針を説明する。



## DBアクセスの為の主要なクラス

Java版の原価計算ベンチマークでは、既存のRDBMSとTsurugiの両方に対応する為に、既存RDBMS用のJDBCとTsurugi用のIceaxe（TsurugiでSQLを実行する為のライブラリー）を抽象化したクラスを設ける。

| クラス             | 説明                                                         | 備考                                 |
| ------------------ | ------------------------------------------------------------ | ------------------------------------ |
| CostBenchDbManager | DB接続とトランザクションを管理するクラス                     | JDBCとIceaxeで個別の具象クラスがある |
| DAO                | DBのテーブル毎のクラスで、SQLを実行する為のメソッドをSQL毎に用意する | JDBCとIceaxeで個別の具象クラスがある |
| Entity             | テーブルの1行分のデータを保持するクラス                      | JDBCとIceaxeで共通                   |



## CostBenchDbManager

CostBenchDbManagerは、DB接続を管理し、トランザクションを実行するクラス。
具象クラスはDBMSの種類（JDBCかIceaxeか）によって異なる。

JDBCの場合はConnection、Iceaxeの場合はTsurugiSession（DB接続）を保持する。

- JDBC版は、スレッド毎にConnectionを生成する。
- Iceaxe版は、全スレッドでひとつのTsurugiSessionを共有する方法と、スレッド毎にTsurugiSessionを生成する方法が選択できるようにする。（ただし、性能面の問題から、基本的に後者で使用する）

また、CostBenchDbManagerでDAOのインスタンスも保持する。
（DAOもDBMSの種類に応じて具象クラスが異なるので、CostBenchDbManagerの具象クラスのDBMSに対応するDAOインスタンスを生成・保持する）

### CostBenchDbManagerインスタンス生成

CostBenchDbManagerのインスタンスは、CostBenchDbManagerのcreateInstanceメソッドで生成する。

| createInstanceの引数 | 説明                                                         |
| -------------------- | ------------------------------------------------------------ |
| DbManagerType        | JDBCか、Iceaxeかを指定する                                   |
| IsolationLevel       | トランザクション分離レベル（READ_COMMITTED・SERIALIZABLE）を指定する。JDBCの場合のみ有効 |
| isMultiSession       | 全スレッドでTsurugiSessionを共有するか、スレッド毎に生成するかを指定する。Iceaxeの場合のみ有効 |

> **Note**
>
> 原価計算ベンチマークのアプリケーションでは、プロパティーファイルの `dbmanager.type` によってDbManagerTypeを切り替える。
> DBの接続情報（JDBCのURLや、Tsurugiのエンドポイント）もプロパティーファイルから取得する。

CostBenchDbManagerはCloseableを継承し、使用後はクローズする必要がある。
（内部でConnectionやTsurugiSessionのクローズを行う）

### トランザクションの実行

CostBenchDbManagerのexecuteメソッドを使って、トランザクションを実行する。

executeメソッドは渡された関数を実行して、正常に終了すればコミットし、例外が発生したらロールバックする。シリアライゼーションエラー（リトライ可能なアボート）が発生した場合はこの関数を再度実行する。

```java
try (var dbManager = CostBenchDbManager.createInstance(～)) {
    var setting = TgTmSetting.of(～); // @see Iceaxe
    dbManager.execute(setting, () -> {
        // DAOを使ってSQLを実行する関数
    });
}
```

executeメソッドの引数TgTmSettingは、IceaxeのTsurugiTransactionManagerで使用する設定。

- Iceaxe版CostBenchDbManagerではTsurugiTransactionManagerを使うので、この設定に従ってシリアライゼーションエラー発生時のトランザクション再実行が行われる。
- JDBC版CostBenchDbManagerではこの設定は無視する。（シリアライゼーションエラーが発生したら常に再実行する）

executeメソッドはスレッドセーフであり、内部ではスレッド毎に個別のトランザクションを開始する。
（JDBC版はスレッド毎にConnectionを生成し、Iceaxe版はスレッド毎にTsurugiTransactionを生成する）

### カウンター

CostBenchDbManagerで、処理の成功件数・再実行件数等をカウントする。

件数のカウントはBenchDbCounterクラスで行う。
CostBenchDbManagerのstaticフィールドで保持し、CostBenchDbManagerのgetCounterメソッドで取得できる。

カウンターはstaticフィールドなので、同一JavaVM内で異なるアプリケーションを連続して実行する場合は、CostBenchDbManagerのinitCounterメソッドを呼び出して、カウンターをクリアする必要がある。



## DAO

DAOは、SQLを実行するメソッドを宣言するインターフェース。
テーブル毎にDAOインターフェースがあり、一つのSQLにつき一つのメソッドを用意する。

```java
// 工場マスターのDAO
public interface FactoryMasterDao {

    public static final String TABLE_NAME = "factory_master";
～
    /**
     * <pre>
     * delete from factory_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into factory_master
     * values(:entity)
     * </pre>
     */
    int insert(FactoryMaster entity);
～
}
```

メソッドにはthrows宣言が無い為、JDBCのSQLExceptionやIceaxeのTsurugiTransactionExceptionは非チェック例外に変換する必要がある。

### SQLの実行方法

DAOのインスタンスをCostBenchDbManagerから取得し、そのメソッドを呼び出すことでSQLを実行する。

```java
dbManager.execute(setting, () -> {
    FactoryMasterDao dao = dbManager.getFactoryMasterDao();
    dao.deleteAll();
});
```

DAOインスタンスはシングルトンで、スレッドセーフである。
（内部ではCostBenchDbManagerからスレッド毎のトランザクションを取得してSQLを実行する）



### DAOの具象クラス

SQLを実行する実装はJDBCとIceaxeで異なるので、DAOの具象クラスは別々になる。

DAOの具象クラスは、JDBCやIceaxe毎の共通クラスを継承し、テーブル毎のインターフェースを実装する。

```java
// 工場マスターDAOの具象クラス（JDBC版）
public class FactoryMasterDaoJdbc extends JdbcDao<FactoryMaster> implements FactoryMasterDao {
    ～
}

// 工場マスターDAOの具象クラス（Iceaxe版）
public class FactoryMasterDaoIceaxe extends IceaxeDao<FactoryMaster> implements FactoryMasterDao {
    ～
}
```

JdbcDaoやIceaxeDaoといった共通クラスに、各テーブルで共通の機能を用意する。
共通機能には以下のようなものがある。

- insertや全件削除のdeleteといった定型的なSQLのSQL文を生成する。
- SQL文からPreparedStatementを作成・キャッシュする。
- PreparedStatementを実行する。

----

SQLは基本的にキャッシュする。
（値の個数が不定のin句を使ったSQLはキャッシュしない）

- JDBC版の場合、CostBenchDbManagerJdbcの中でConnection毎にPreparedStatementを作り、キャッシュする。
- Iceaxe版の場合、キャッシュ用のクラス（CachePreparedQueryやCachePreparedStatement等）を用意し、それを使ってIceaxeDaoの中でTsurugiSession毎にキャッシュする。

----

SQLを実行する実装を共通クラスに用意し、具象クラスのメソッドから呼び出して使用する。

| 共通機能                          | JDBC版メソッド     | Iceaxe版メソッド                |
| --------------------------------- | ------------------ | ------------------------------- |
| truncate文を生成して実行する      | doTruncate         | doTruncate（実体はdoDeleteAll） |
| delete文を生成して実行する        | doDeleteAll        | doDeleteAll                     |
| insert文を生成して実行する        | doInsert           | doInsert                        |
| select文を生成して実行する        | doSelectAll        | doSelectAll                     |
| select文を生成して1件ずつ処理する | doForEach          | doForEach                       |
| update文を生成して実行する        | doUpdate           | doUpdate                        |
| selectを実行して1件返す           | executeQuery1      | executeAndGetRecord             |
| selectを実行してListを返す        | executeQueryList   | executeAndGetList               |
| selectを実行してStreamを返す      | executeQueryStream | executeAndGetStream             |
| 更新系SQLを実行する               | executeUpdate      | executeAndGetCount              |

上記のSQL文を生成する為に、テーブル名やカラム情報をDAOに保持する。



### SQLメソッドの実装例（JDBC版）

JDBC版のDAOで `select * from factory_master where f_id = パラメーター` を実装する例。

```java
public class FactoryMasterDaoJdbc extends JdbcDao<FactoryMaster> implements FactoryMasterDao {
～
    @Override
    public FactoryMaster selectById(int factoryId) {
        String sql = "select * from " + TABLE_NAME + " where f_id = ?";
        return executeQuery1(sql, ps -> {
            int i = 1;
            setInt(ps, i++, factoryId);
        }, this::newEntity);
    }

    private FactoryMaster newEntity(ResultSet rs) throws SQLException {
        FactoryMaster entity = new FactoryMaster();
        fillEntity(entity, rs);
        return entity;
    }
～
}
```

- executeQuery1は、select文を実行して1レコードだけ取得する共通メソッド。
  - 第1引数でSQL文を渡す。
    - executeQeury1内部では、PreparedStatementを作りキャッシュする。
  - 第2引数は、PreparedStatementにSQLのパラメーターをセットする処理。
  - 第3引数は、ResultSetをEntityに変換する処理。
    - fillEntityメソッドは、ResultSetからEntityに値をコピーする共通メソッド。

### SQLメソッドの実装例（Iceaxe版）

Iceaxe版のDAOで `select * from factory_master where f_id = パラメーター` を実装する例。

```java
public class FactoryMasterDaoIceaxe extends IceaxeDao<FactoryMaster> implements FactoryMasterDao {
～
    private final TgBindVariable<Integer> vFactoryId = BenchVariable.ofInt("factoryId");

    @Override
    public FactoryMaster selectById(int factoryId) {
        var ps = selectByIdCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId));
        return executeAndGetRecord(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, FactoryMaster> selectByIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where f_id = " + vFactoryId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId);
            this.resultMapping = getEntityResultMapping();
        }
    };
～
}
```

- CachePreparedQueryは、select文をキャッシュするクラス。
  - initializeメソッドはTsurugiSession毎に1回だけ呼ばれる。この中で、SQL文や、Iceaxeのバインド変数を定義するTgParameterMapping、select文の結果をEntityに変換するTgResultMappingを準備する。
    - getSelectEntitySqlメソッドは、テーブルの全カラム名を列挙したselect文を返す共通メソッド。
    - getEntityResultMappingメソッドは、select結果をEntityに変換するTgResultMappingを返す共通メソッド。

- executeAndGetRecordは、select文を実行して1レコードだけ取得する共通メソッド。
  - 第1引数に、キャッシュから取得したSQLを渡す。
  - 第2引数に、SQLのパラメーターを渡す。

> **Note**
>
> JDBC版の場合、SQL文のみがキャッシュのキーとなるので、共通メソッドの内部でキャッシュしている。
> しかしIceaxe版の場合はSQL文と共にTgParameterMappingやTgResultMappingを準備する必要があるので、一度だけ初期化を行うキャッシュクラスを用意した。



## Entity

Entityは、テーブルの1行分のデータを保持するクラス。DBMSの種類に依らず共通。

カラム毎の単純なsetter/getterメソッドの他に、cloneメソッドを実装する。

また、数値カラムの小数部の桁数を定数として保持する。
（現在のTsurugiではテーブル定義の小数部の桁数を超えた数値はinsert/updateできない為、クライアント側で小数部を丸める必要がある。その際に、Entityで保持している桁数を使用する）

> **Note**
>
> 小数部の桁数をDAOでなくEntityで保持する理由は、Entityのソースコードはテーブル定義から生成するので、テーブル定義が変更された時に追随しやすい為。

品目マスターの品目種類・度量衡マスターの単位種類は列挙型で表し、Entityクラス内でも列挙型で保持する。
テーブル定義上は文字列型なので、DBアクセスの際にEntityとテーブル間で変換する必要がある。

### Entityのソースコードの生成

Entityクラスのソースファイルは、EntityGeneratorクラスを実行して生成する。

EntityGeneratorは、src/main/resourcesに置かれているテーブル定義のExcelファイルを読み込む。
Entityクラスのソースファイルの生成場所はEntityGeneratorのmainメソッドの第1引数で指定する。



## DDL

初期データ作成処理（initdata.sh、実体はInitialData00CreateTableクラス）を実行することで、原価計算ベンチマークで使用するテーブルを作成する。

### DDLファイルの生成

（初期データ作成処理でテーブルが作成されるのでcreate文が書かれたDDLファイルは必要ないのだが、）DBMSの種類毎に存在するDdlGeneratorクラスを実行すると、原価計算ベンチマークで使用するテーブルのcreate文が書かれたDDLファイルを生成することが出来る。

DdlGeneratorは、src/main/resourcesに置かれているテーブル定義のExcelファイルを読み込む。
DDLファイルが出力される場所は、プロパティーファイルのdoc.dirで指定されているディレクトリー。

