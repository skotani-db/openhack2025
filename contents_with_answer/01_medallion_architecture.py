# Databricks notebook source
# MAGIC %md
# MAGIC ## 01. メダリオンアーキテクチャに基づいたデータエンジニアリング概要 (標準時間：60分)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 本ノートブックの目的：Databricksにおけるデータ処理の基礎と[メダリオンアーキテクチャ](https://www.databricks.com/jp/glossary/medallion-architecture)について理解を深める

# COMMAND ----------

# MAGIC %md
# MAGIC ![メダリオンアーキテクチャ](https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_01__introduction/delta-lake-medallion-architecture-2.jpeg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### メダリオンアーキテクチャとは(標準時間：5分)
# MAGIC
# MAGIC データを、Bronze、Silver、Goldの３層の論理レイヤーで管理する手法です。Databricks では、すべてのレイヤーを Delta Lake 形式で保持することが推奨されています。
# MAGIC
# MAGIC | #    | データレイヤー | 概要                                                   | 類義語             |
# MAGIC | ---- | -------------- | ------------------------------------------------------ | ------------------ |
# MAGIC | 1    | Bronze         | 未加工データを保持するレイヤー                             | Raw     |
# MAGIC | 2    | Silver         | クレンジング・適合済みデータデータを保持するレイヤー | Enriched      |
# MAGIC | 3    | Gold           | ビジネスレベルのキュレート済みデータを保持するレイヤー   | Curated |
# MAGIC
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [Medallion Architecture | Databricks](https://databricks.com/jp/glossary/medallion-architecture)
# MAGIC - [What's Data Lake ?](https://docs.google.com/presentation/d/1pViTuBmK4nDWg4n8_yGKbN4gOPbbFUTw/edit?usp=sharing&ouid=110902353658379996895&rtpof=true&sd=true)
# MAGIC
# MAGIC
# MAGIC 次のメリットがあります。
# MAGIC
# MAGIC - データレイヤーごとの役割分担が可能となること
# MAGIC - データレイクにてデータ品質が担保できるようなること
# MAGIC - ローデータから再度テーブルの再作成が容易となること
# MAGIC
# MAGIC
# MAGIC **Bronzeの特徴について**
# MAGIC - 取り込んだローデータのコピーを、スキーマ展開を許可するなど、そのまま保持。
# MAGIC - ロード日時などの監査列（システム列）を必要に応じて付加。
# MAGIC - データ型を文字型として保持するなどの対応によりデータ損失の発生を低減。
# MAGIC - データを削除する場合には、物理削除ではなく、論理削除が推奨。
# MAGIC
# MAGIC **Silverの特徴について**
# MAGIC - Bronze のデータに処理を行い、クレンジング・適合済みデータを保持。
# MAGIC - スキーマを適用し、dropDuplicates関数を利用した重複排除などによるデータ品質チェック処理を実施。
# MAGIC - 最小限、あるいは「適度な」変換およびデータクレンジングルールのみを適用。
# MAGIC - Bronze との関係性が、「1 対多」方式となることもある。
# MAGIC
# MAGIC **Goldの特徴について**
# MAGIC - 企業や部門のデータプロジェクトにおいてビジネス上の課題を解決するように編成・集計したデータを保持。
# MAGIC - アクセス制御リスト（ACL）や行レベルセキュリティ等のデータアクセス制御を考慮することが多い。
# MAGIC
# MAGIC **参考:データソースの種類について**
# MAGIC - [Unity Catalogにおける外部ロケーション](https://learn.microsoft.com/ja-jp/azure/databricks/spark/latest/spark-sql/language-manual/sql-ref-external-locations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# 既存のウィジェットを削除
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 環境のセットアップ
# MAGIC 本ノートブックで作業するために以下の処理を行います。
# MAGIC - スキーマ作成
# MAGIC - ボリューム作成
# MAGIC - ボリュームへのデータコピー

# COMMAND ----------

# 本ノートブックで利用するスキーマを作成
schema_name = f"01_medallion_architecture_for_{user_name}"
print(f"schema_name: `{schema_name}`")
spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
    """
)

# COMMAND ----------

# 本ノートブックで利用する Volume を作成
volume_name = "src_file_volume_01"
print(f"volume_name: `{volume_name}`")
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
    """
)

checkpoint_volume_name = "checkpoint_volume_01"
print(f"checkpoint_volume_name: `{checkpoint_volume_name}`")
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{checkpoint_volume_name}
    """
)

# COMMAND ----------

# 本ノートブックで利用するソースファイルを Volume に移動
file_dir = f"/Volumes/{catalog_name}/{src_schema_name}/{src_volume_name}/{src_folder_name}"
volume_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
checkpoint_volume_dir = f"/Volumes/{catalog_name}/{schema_name}/{checkpoint_volume_name}"

file_dir
volume_dir
checkpoint_volume_dir
dbutils.fs.cp(file_dir, volume_dir, recurse=True)
file_list = [file.name for file in dbutils.fs.ls(volume_dir)]
display(dbutils.fs.ls(volume_dir))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 変数の管理とウィジェット

# COMMAND ----------

# MAGIC %md
# MAGIC 本ノートブックで利用する変数を記載(整理)しておきます。  
# MAGIC
# MAGIC | 変数名       | 用途                                     |
# MAGIC |--------------|-----------------------------------------|
# MAGIC | volume_dir   | ソースデータが格納されているVolumeのパス   |  
# MAGIC | src_file     | ソースデータファイル                      | 
# MAGIC | catalog_name | カタログ名                               | 
# MAGIC | schema_name  | スキーマ名                               | 
# MAGIC
# MAGIC
# MAGIC #### 変数の定義と受け渡し
# MAGIC 変数はコード上で定義することが多いと思いますが、Databricks では Widget を使って変数を管理することができます。  
# MAGIC 参考：[Databricks ウィジェット](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/widgets)
# MAGIC
# MAGIC #### Widgetのメリット
# MAGIC 1. Notebook上で変数を簡単に管理できます
# MAGIC     - dbutils.widgets.text() などで変数を作成し、どのSQLセルやPythonセルからも利用可能です。
# MAGIC 1. 異なる言語 ( %sql でも %python でも) 同じ値を参照できる
# MAGIC     - SQLセルとPythonセルのどちらからも dbutils.widgets.get("my_var") で取得できるため、統一的に扱えます。
# MAGIC 1. SQLの中で ${} 記法を使って簡単に展開できる(現在は非推奨となりました)
# MAGIC     - 例：
# MAGIC         ```SELECT * FROM ${my_table};```
# MAGIC 1. UIから値を変更できる
# MAGIC     - DatabricksのNotebookでは、ウィジェットがUI要素として表示されるため、手入力や選択肢から簡単に値をセットできます。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgetの作成例

# COMMAND ----------

# 環境セットアップで定義した変数 volume_dir をウィジェットで定義
dbutils.widgets.text("volume_dir", volume_dir, "1.データファイルの保存先")

# COMMAND ----------

# ドロップダウンウィジェットを作成し、ファイルリストからデータファイルを選択
dbutils.widgets.dropdown("src_file", file_list[0], file_list, "2.取り込むデータファイルを選択")

# 利用するカタログ名とスキーマ名をウィジェットで管理
dbutils.widgets.text("catalog_name", catalog_name, "3.カタログ名")
dbutils.widgets.text("schema_name", schema_name, "4.スキーマ名")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ヒント：SQLを使った、Databricksのデータパイプライン作成
# MAGIC
# MAGIC 環境のセットアップや変数の整理ができたので、ソースデータからデータを読み込みましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC #### ファイルに対するクエリ
# MAGIC Databricksはファイルに対してSQLを使ったクエリを実行することができます。  
# MAGIC 以下のセルでは、csv ファイルに対してクエリを実行します。(Unity Catalogボリューム内のファイルを操作する例です)
# MAGIC
# MAGIC csvだけではなく、parquet、JSON など、多くのデータファイルタイプでクエリを実行可能です。
# MAGIC
# MAGIC データ基盤(データレイクハウス)へのデータ取り込みのワークフローでは、クラウドストレージなどからデータアクセスするケースがありますが、SQLの構文でデータ取り込みを行うことができます。  
# MAGIC ヘッダ行を無視したい場合などの指定も可能です。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM csv.`/Volumes/trainer_catalog/default/src_data/sample_data_01/Product2.csv` -- 読み込み対象のcsvパスを指定。パスはバッククオート(`)で囲む
# MAGIC WITH (
# MAGIC   header = "true",        -- 最初の行をヘッダー (列名) として使用
# MAGIC   delimiter = ",",        -- カンマ区切りのファイル
# MAGIC   inferSchema = "true",   -- 自動的にデータ型を推測
# MAGIC   mode = "FAILFAST"       -- 読み込みエラー時に即時失敗
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC また、Databricks では SQL ユーザーが CSV ファイルを読み取る場合は、read_files テーブル値関数の利用を推奨しています。  
# MAGIC 参考：
# MAGIC  [CSVファイルの読み取り](https://docs.databricks.com/ja/query/formats/csv.html)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- read_filesテーブル値関数の利用例
# MAGIC SELECT * FROM read_files(:volume_dir||'/'||'Product2.csv',
# MAGIC   format => 'csv',
# MAGIC   delimiter => ',',
# MAGIC   header => true,
# MAGIC   mode => 'FAILFAST')

# COMMAND ----------

# MAGIC %md
# MAGIC #### (参考)`read_files()` テーブル値関数について
# MAGIC
# MAGIC `read_files()`テーブル値関数（TVF）は、さまざまなファイル形式を読み取ることができます。詳細は[こちら](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/read_files)を参照してください。最初のパラメータはデータのパスです。
# MAGIC
# MAGIC 使用しているオプションは次の通りです：
# MAGIC
# MAGIC 1. `format => "csv"` -- データファイルは `CSV` 形式です
# MAGIC 1. `sep => "|"` -- データフィールドは |（パイプ）文字で区切られています
# MAGIC 1. `header => true` -- 最初の行はカラム名として使用されます
# MAGIC 1. `mode => "FAILFAST"` -- 異常データがある場合、ステートメントはエラーをスローします
# MAGIC
# MAGIC この場合、既存の `CSV` データを移動していますが、異なるオプションを使用することで他のデータタイプも簡単に使用できます。
# MAGIC
# MAGIC スキーマに一致しないデータを救出するための `_rescued_data` カラムがデフォルトで提供されます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Bronzeテーブルのパイプラインを作成してください。(標準時間：20分)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Bronzeテーブルを作成し、Volumeにあるソースデータを取り込む処理を作成します。  
# MAGIC 以下の図はこのノートブックで作成するパイプラインです。  
# MAGIC Bronzeテーブルのパイプラインは赤点線で囲った部分の処理となります。
# MAGIC
# MAGIC <img src="/Volumes/trainer_catalog/default/src_data/images/01_medallion_bronze.png" width="1300" height="500">
# MAGIC
# MAGIC 取り込み対象のデータについては、下記のオブジェクトと同等のものとなっております。
# MAGIC
# MAGIC - [Product2 | Salesforce プラットフォームのオブジェクトリファレンス | Salesforce Developers](https://developer.salesforce.com/docs/atlas.ja-jp.object_reference.meta/object_reference/sforce_api_objects_product2.htm)
# MAGIC - [PricebookEntry | Salesforce プラットフォームのオブジェクトリファレンス | Salesforce Developers](https://developer.salesforce.com/docs/atlas.ja-jp.object_reference.meta/object_reference/sforce_api_objects_pricebookentry.htm)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 手順
# MAGIC 1. 利用するカタログとスキーマの指定
# MAGIC 1. データソースとターゲットテーブルの定義
# MAGIC 1. テーブル定義と作成
# MAGIC 1. ソースデータの読み込み
# MAGIC 1. bronze テーブルへデータを書き込み

# COMMAND ----------

# DBTITLE 1,利用するカタログとスキーマの指定
# MAGIC %sql
# MAGIC -- currentのカタログとスキーマを指定
# MAGIC use catalog ${catalog_name};
# MAGIC use schema ${schema_name};
# MAGIC
# MAGIC select current_schema();

# COMMAND ----------

# DBTITLE 1,データソースとターゲットテーブルの定義
# 変数の定義

# 取り込み対象は Product2.csv です。
# ウィジェットの 2.取り込むデータファイルを選択(src_file) から Product2.csv を選択してください。

src_file = dbutils.widgets.get("src_file")
src_file_path__1_1_1 = f"{volume_dir}/{src_file}"
tgt_table_name__1_1_1 = f"{catalog_name}.{schema_name}.product2__bronze"

# COMMAND ----------

# 参考：CSV の中身をチェック

# CSV の中身をチェックしたい場合、dbutilsを使って参照することができます。
# 以下の例では指定したパスのファイルの先頭700byteを表示しています。

data = dbutils.fs.head(src_file_path__1_1_1, 700)
print(data)

# COMMAND ----------

# DBTITLE 1,product2__bronze テーブルを作成
# Databricks SQLのDDL文では、変数化した値をテーブル名などに直接利用することはできません。Pythonでクエリ文字列を生成してSQLを実行します。
# 定義したSQLは spark.sql(SQL文)で実行可能です。

create_tbl_ddl = f"""
CREATE OR REPLACE TABLE {tgt_table_name__1_1_1}
(
    `Id` STRING,
    `Name` STRING,
    `ProductCode` STRING,
    `Description` STRING,
    `IsActive` STRING,
    `CreatedDate` STRING,
    `CreatedById` STRING,
    `LastModifiedDate` STRING,
    `LastModifiedById` STRING,
    `SystemModstamp` STRING,
    `Family` STRING,
    `ExternalDataSourceId` STRING,
    `ExternalId` STRING,
    `DisplayUrl` STRING,
    `QuantityUnitOfMeasure` STRING,
    `IsDeleted` STRING,
    `IsArchived` STRING,
    `LastViewedDate` STRING,
    `LastReferencedDate` STRING,
    `StockKeepingUnit` STRING,
    _rescued_data STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta;
"""
spark.sql(create_tbl_ddl)

# COMMAND ----------

# DBTITLE 1,ソースデータの読み込みと一時view化
# MAGIC %sql
# MAGIC -- TEMP VIEWの作成もDDLとなるため、ファイルパスのリテラル値で渡す必要があります。(:volume_dirなどは使えません)
# MAGIC -- ここではSQLを使ってリテラル値を指定していますが、Pythonを使ってビュー定義クエリを組み立て、リテラルに展開してから実行する方法 spark.sql(query) もあります。
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_data AS
# MAGIC SELECT 
# MAGIC   t.* ,
# MAGIC   -- 監査列として`_datasource`列と`_ingest_timestamp`列を追加
# MAGIC   _metadata.file_path AS _datasource,
# MAGIC   current_timestamp() AS _ingest_timestamp
# MAGIC FROM read_files('/Volumes/trainer_catalog/01_medallion_architecture_for_nssol/src_file_volume_01/Product2.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   delimiter => ',',
# MAGIC   mode => 'FAILFAST') as t

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPORARY VIEW の中身を確認
# MAGIC select * from bronze_data
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- (参考)テーブル情報の確認
# MAGIC describe extended bronze_data
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,product2__bronze テーブルへデータを書き込み
query = f"""
MERGE INTO {tgt_table_name__1_1_1} AS tgt
    USING bronze_data AS src
    ON tgt.Id = src.Id
    WHEN MATCHED 
        AND tgt._ingest_timestamp < src._ingest_timestamp THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### (参考) 冪等性の考慮とMERGE処理について
# MAGIC Databricksのデータパイプラインでは、再実行しても結果が変わらない「冪等性」を考慮することが重要です。
# MAGIC パイプラインの処理が失敗した場合に再実行しても、データの重複や不整合が発生しないように設計する必要があります。
# MAGIC
# MAGIC MERGE文は、以下の理由から冪等性を担保する実装に適しています：
# MAGIC
# MAGIC 1. **アトミックな操作**:
# MAGIC    - MERGEは、INSERT、UPDATE、DELETEの複数の操作を1つのトランザクション内で実行します。
# MAGIC    - そのため、途中でエラーが発生した場合でも、すべての操作が一括してロールバックされ、整合性が保たれます。
# MAGIC
# MAGIC 2. **条件に基づくデータ同期**:
# MAGIC    - ソースデータとターゲットテーブルを、キーに基づいて一致させ、必要に応じて更新や挿入、削除を行うため、
# MAGIC      同じMERGE文を再実行しても、既に正しい状態にあるレコードは更新されず、不要な重複が生じません。
# MAGIC
# MAGIC 3. **再実行時の安全性**:
# MAGIC    - MERGE文は、既に適用済みの変更に対して無駄な操作を行わないため、同じ操作を複数回実行しても
# MAGIC      テーブルの状態が変わらず、パイプラインの冪等性が確保されます。
# MAGIC
# MAGIC MERGE文を使用することで、ターゲットテーブルへの挿入・更新・削除を一貫性のある方法で管理し、データパイプラインの再実行時にも問題が発生しないように実装できます。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 取り込んだBronzeデータの確認
# MAGIC SELECT * FROM product2__bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo `pricebook_entry__bronze`のパイプラインを作成してください。

# COMMAND ----------

# 2.取り込むデータファイルを選択のウィジェットから、PricebookEntry.csv を選択してください。
print(src_file)

# COMMAND ----------

# ソースデータファイルとターゲットテーブルのパスを変数に格納

src_file_path__1_2_1 = f"{volume_dir}/{src_file}"
tgt_table_name__1_2_1 = f"{catalog_name}.{schema_name}.pricebook_entry__bronze"

# COMMAND ----------

# ToDo CSV の中身をチェック
# 例1
data = dbutils.fs.head(src_file_path__1_2_1, 700)
display(data)

# 例2
# display(spark.read.format("csv").option("header", "true").load(src_file_path__1_2_1))

# COMMAND ----------

# `pricebook_entry__bronze`テーブルを作成
create_tbl_ddl = f"""
CREATE OR REPLACE TABLE {tgt_table_name__1_2_1}
(
    `Id` STRING,
    `Name` STRING,
    `Pricebook2Id` STRING,
    `Product2Id` STRING,
    `UnitPrice` STRING,
    `IsActive` STRING,
    `UseStandardPrice` STRING,
    `CreatedDate` STRING,
    `CreatedById` STRING,
    `LastModifiedDate` STRING,
    `LastModifiedById` STRING,
    `SystemModstamp` STRING,
    `ProductCode` STRING,
    `IsDeleted` STRING,
    `IsArchived` STRING,
    _rescued_data STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
spark.sql(create_tbl_ddl)

# COMMAND ----------

# ソースデータのパスを確認(TEMP VIEW作成時のパス確認用)
src_file_path__1_2_1

# COMMAND ----------

# DBTITLE 1,csvを読み込み、TEMP VIEWに格納
# MAGIC %sql
# MAGIC -- ToDo データソースのcsvを読み込み、TEMP VIEWに格納する処理を記述してください
# MAGIC -- csvの読み込みの際に、監査用に取り込み元のfile_path と 取り込み時のtimestamp の列を追加してください
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_data AS
# MAGIC SELECT 
# MAGIC   t.*,
# MAGIC   _metadata.file_path as _datasource,
# MAGIC   current_timestamp() as _ingest_timestamp
# MAGIC FROM read_files('/Volumes/trainer_catalog/01_medallion_architecture_for_nssol/src_file_volume_01/PricebookEntry.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   delimiter => ',',
# MAGIC   mode => 'FAILEFAST'
# MAGIC ) as t
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_data limit 10;

# COMMAND ----------

# ToDo `pricebook_entry__bronze`テーブルへ書き込みを実施してください。

query = f"""
MERGE INTO {tgt_table_name__1_2_1} as tgt
    USING bronze_data AS src
    ON tgt.Id = src.Id
    WHEN MATCHED 
        AND tgt._ingest_timestamp < src._ingest_timestamp THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
"""

# クエリの実行
spark.sql(query)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- データが書き込まれたことを確認
# MAGIC select * from pricebook_entry__bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Silver テーブルのパイプラインを作成してください(標準時間：20分)

# COMMAND ----------

# MAGIC %md
# MAGIC 続いて、Silver テーブルを作成します。  
# MAGIC Silver テーブルは、Bronze に取り込まれたデータからノイズや不整合を取り除き、統合・最適化された高品質なデータセットを生成する処理を行います。  
# MAGIC
# MAGIC データエンジニアリングにおけるデータパイプラインの中核的な役割を担います。  
# MAGIC 代表的な処理は、データクレンジング、データ統合、データ変換、重複除去、データ品質チェックなどです。
# MAGIC
# MAGIC ここでは、データの重複チェックと、Bronze に取り込まれたデータに対するスキーマ適用を行います。  
# MAGIC <br>
# MAGIC
# MAGIC <img src="/Volumes/trainer_catalog/default/src_data/images/01_medallion_silver.png" width="1300" height="500">

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例

# COMMAND ----------

# ソースデータとターゲットテーブルのパスを変数に格納

src_table_name__2_1_1 = f"{catalog_name}.{schema_name}.product2__bronze"
tgt_table_name__2_1_1 = f"{catalog_name}.{schema_name}.product2__silver"

# COMMAND ----------

# `product2__silver`テーブルを作成
query = f"""
CREATE OR REPLACE TABLE {tgt_table_name__2_1_1}
(
    `Id` STRING,
    `Name` STRING,
    `ProductCode` STRING,
    `Description` STRING,
    `IsActive` BOOLEAN,
    `CreatedDate` TIMESTAMP,
    `CreatedById` STRING,
    `LastModifiedDate` TIMESTAMP,
    `LastModifiedById` STRING,
    `SystemModstamp` TIMESTAMP,
    `Family` STRING,
    `ExternalDataSourceId` STRING,
    `ExternalId` STRING,
    `DisplayUrl` STRING,
    `QuantityUnitOfMeasure` STRING,
    `IsDeleted` BOOLEAN,
    `IsArchived` BOOLEAN,
    `LastViewedDate` TIMESTAMP,
    `LastReferencedDate` TIMESTAMP,
    `StockKeepingUnit` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
spark.sql(query)

# COMMAND ----------

# DBTITLE 1,シルバーデータの作成
# 下記の処理を実行したTEMPORARY VIEWを作成する
## 1. product2__bronzeテーブルから主キー（Id）ごとに_ingest_timestamp列の最大日を抽出したサブセットを作成
## 2. 主キー＋_ingest_timestamp列の条件で、1のサブセットとproduct2__bronzeテーブルを結合
## 3. product2__bronzeテーブルのデータ型をシルバーテーブルと同一のデータ型に変換

query = f"""
WITH slv_records AS (
  -- 各Idごとに最新の_ingest_timestampを求める
  SELECT
    Id,
    MAX(_ingest_timestamp) AS max_ingest_timestamp
  FROM {src_table_name__2_1_1}
  GROUP BY Id
),
joined_data AS (
  -- 最新タイムスタンプのレコードのみを抽出する
  SELECT
    brz.Id,
    brz.Name,
    brz.ProductCode,
    brz.Description,
    CAST(brz.IsActive AS BOOLEAN) AS IsActive,
    CAST(brz.CreatedDate AS TIMESTAMP) AS CreatedDate,
    brz.CreatedById,
    CAST(brz.LastModifiedDate AS TIMESTAMP) AS LastModifiedDate,
    brz.LastModifiedById,
    CAST(brz.SystemModstamp AS TIMESTAMP) AS SystemModstamp,
    brz.Family,
    brz.ExternalDataSourceId,
    brz.ExternalId,
    brz.DisplayUrl,
    brz.QuantityUnitOfMeasure,
    CAST(brz.IsDeleted AS BOOLEAN) AS IsDeleted,
    CAST(brz.IsArchived AS BOOLEAN) AS IsArchived,
    CAST(brz.LastViewedDate AS TIMESTAMP) AS LastViewedDate,
    CAST(brz.LastReferencedDate AS TIMESTAMP) AS LastReferencedDate,
    brz.StockKeepingUnit,
    brz._datasource,
    CAST(brz._ingest_timestamp AS TIMESTAMP) AS _ingest_timestamp
  FROM {src_table_name__2_1_1} AS brz
  INNER JOIN slv_records AS slv
    ON brz.Id = slv.Id
    AND brz._ingest_timestamp = slv.max_ingest_timestamp
),
deduped AS (
  -- 万が一、最新タイムスタンプのレコードが複数存在する場合、1件だけ選ぶ（Id単位で重複排除）
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY Id ORDER BY _ingest_timestamp DESC) AS rn
  FROM joined_data
)
-- rn=1 のレコードのみを最終結果として返す
SELECT
  Id,
  Name,
  ProductCode,
  Description,
  IsActive,
  CreatedDate,
  CreatedById,
  LastModifiedDate,
  LastModifiedById,
  SystemModstamp,
  Family,
  ExternalDataSourceId,
  ExternalId,
  DisplayUrl,
  QuantityUnitOfMeasure,
  IsDeleted,
  IsArchived,
  LastViewedDate,
  LastReferencedDate,
  StockKeepingUnit,
  _datasource,
  _ingest_timestamp
FROM deduped
WHERE rn = 1
"""

spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW silver_data AS {query}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_data limit 10;

# COMMAND ----------

# 一時ビューから`product2__silver`に対して、MERGE文によりアップサート処理を実施。


query = spark.sql(f'''
MERGE INTO {tgt_table_name__2_1_1} AS tgt
  USING silver_data AS src
  ON tgt.Id = src.Id 
  WHEN MATCHED
    AND tgt._ingest_timestamp < src._ingest_timestamp THEN 
    UPDATE SET *
  WHEN NOT MATCHED THEN 
    INSERT *
''')


# COMMAND ----------

# DBTITLE 1,データが書き込まれたことを確認
# MAGIC %sql
# MAGIC select * from product2__silver limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo `pricebook_entry__silver`のパイプラインを作成してください。
# MAGIC
# MAGIC `pricebook_entry__silver`テーブルにおける主キーは`Id`列です。

# COMMAND ----------

# ソースデータとターゲットテーブルのパスを変数に格納

src_table_name__2_2_1 = f"{catalog_name}.{schema_name}.pricebook_entry__bronze"
tgt_table_name__2_2_1 = f"{catalog_name}.{schema_name}.pricebook_entry__silver"

# COMMAND ----------

# Silver テーブルを作成
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_2_1}
    (
        `Id` STRING,
        `Name` STRING,
        `Pricebook2Id` STRING,
        `Product2Id` STRING,
        `UnitPrice` DECIMAL(16, 0),
        `IsActive` BOOLEAN,
        `UseStandardPrice` BOOLEAN,
        `CreatedDate` TIMESTAMP,
        `CreatedById` STRING,
        `LastModifiedDate` TIMESTAMP,
        `LastModifiedById` STRING,
        `SystemModstamp` TIMESTAMP,
        `ProductCode` STRING,
        `IsDeleted` BOOLEAN,
        `IsArchived` BOOLEAN,
        _datasource STRING,
        _ingest_timestamp timestamp
    )
    USING delta
    """
)

# COMMAND ----------

# ToDo 下記の処理を実行したデータフレーム（df）を作成してください。
## 1. `pricebook_entry__bronze`テーブルから主キー（`Id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットと`pricebook_entry__bronze`テーブルを結合
## 3. `pricebook_entry__bronze`テーブルのデータ型をシルバーテーブルと同一のデータ型に変換

query = f"""
WITH slv_records AS (
  -- 各Idごとに最新の_ingest_timestampを求める
  SELECT
    Id,
    MAX(_ingest_timestamp) AS max_ingest_timestamp
  FROM {src_table_name__2_2_1}
  GROUP BY Id
),
joined_data AS (
  -- 最新タイムスタンプのレコードのみを抽出する
  SELECT
    brz.`Id`,
    brz.`Name`,
    brz.`Pricebook2Id`,
    brz.`Product2Id`,
    brz.`UnitPrice`::DECIMAL(16, 0),
    brz.`IsActive`::BOOLEAN,
    brz.`UseStandardPrice`::BOOLEAN,
    brz.`CreatedDate`::TIMESTAMP,
    brz.`CreatedById`,
    brz.`LastModifiedDate`::TIMESTAMP,
    brz.`LastModifiedById`,
    brz.`SystemModstamp`::TIMESTAMP,
    brz.`ProductCode`,
    brz.`IsDeleted`::BOOLEAN,
    brz.`IsArchived`::BOOLEAN,
    brz._datasource,
    brz._ingest_timestamp::timestamp
  FROM {src_table_name__2_2_1} AS brz
  INNER JOIN slv_records AS slv
    ON brz.Id = slv.Id
    AND brz._ingest_timestamp = slv.max_ingest_timestamp
),
deduped AS (
  -- 万が一、最新タイムスタンプのレコードが複数存在する場合、1件だけ選ぶ（Id単位で重複排除）
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY Id ORDER BY _ingest_timestamp DESC) AS rn
  FROM joined_data
)
-- rn=1 のレコードのみを最終結果として返す
SELECT
    `Id`,
    `Name`,
    `Pricebook2Id`,
    `Product2Id`,
    `UnitPrice`,
    `IsActive`,
    `UseStandardPrice`,
    `CreatedDate`,
    `CreatedById`,
    `LastModifiedDate`,
    `LastModifiedById`,
    `SystemModstamp`,
    `ProductCode`,
    `IsDeleted`,
    `IsArchived`,
    _datasource,
    _ingest_timestamp
FROM deduped
WHERE rn = 1
"""

spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW silver_data AS {query}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 結果を確認
# MAGIC select * from silver_data limit
# MAGIC  10;

# COMMAND ----------

# ToDo 一時ビューから`pricebook_entry__silver`テーブルに対して、MERGE文によりアップサート処理を実施してください。
query = f"""
MERGE INTO {tgt_table_name__2_2_1} AS tgt
  USING silver_data AS src
  ON tgt.id = src.id
  WHEN MATCHED AND tgt._ingest_timestamp < src._ingest_timestamp THEN 
    UPDATE SET *
  WHEN NOT MATCHED THEN
    INSERT *
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,データが書き込まれたことを確認
# MAGIC %sql
# MAGIC select * from pricebook_entry__silver limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Gold Tableのパイプラインを作成してください(標準時間：15分)

# COMMAND ----------

# MAGIC %md
# MAGIC Goldテーブルを作成し、業務で利用可能な、信頼できるデータを提供します。  
# MAGIC
# MAGIC Goldレイヤは最終的に意思決定やビジネスインサイトを得るためのデータセットが用意される場所です。Silverレイヤで整備されたデータに対して、ビジネスロジックや集計、結合などが適用され、実際の業務や分析に即した「信頼性の高い」データを生成します。
# MAGIC
# MAGIC また、Goldテーブルは、レポーティングやダッシュボード、機械学習モデルなどの活用を前提として設計されているため、クエリ性能やデータアクセスの最適化が図られています。
# MAGIC
# MAGIC ここでは、  
# MAGIC 「製品ファミリごとの製品数をカウントしたテーブル」、  
# MAGIC 「Product2をベースに、pricebook_entryにあるUnitPriceを追加したデータのテーブル」  
# MAGIC を作成します。
# MAGIC
# MAGIC <br>
# MAGIC <img src="/Volumes/trainer_catalog/default/src_data/images/01_medallion_gold.png" width="1300" height="500">
# MAGIC
# MAGIC 取り込み対象のデータについては、下記のオブジェクトと同等のものとなっております。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例
# MAGIC Pythonを活用してゴールドテーブルを作成します

# COMMAND ----------

# ソースデータとターゲットテーブルのパスを変数に格納

src_table_name__3_1_1 = f"{catalog_name}.{schema_name}.product2__silver"
tgt_table_name__3_1_1 = f"{catalog_name}.{schema_name}.product_count_by_family"

# COMMAND ----------

# 作成対象のテーブルが既に存在する場合、Drop
spark.sql(
    f"""
    DROP TABLE IF EXISTS {tgt_table_name__3_1_1}
    """
)

# COMMAND ----------

# Goldテーブルに書き込むためのデータフレームを作成
query = f"""
SELECT
  Family,
  COUNT(*) AS product_count
  FROM
    {src_table_name__3_1_1}
  GROUP BY
    ALL
"""
df = spark.sql(query)

# COMMAND ----------

# 処理後の結果を確認
df.display()

# COMMAND ----------

# `product_count_by_family`テーブルへ書き込み
df.write.mode("overwrite").saveAsTable(tgt_table_name__3_1_1)

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__3_1_1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo `d_product`パイプラインを作成してください。
# MAGIC
# MAGIC 以下のサンプルを参考に、`Product2`をベースに、`pricebook_entry`にある`UnitPrice`を追加したデータのテーブルを作成してください。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   prd.*
# MAGIC     EXCEPT (
# MAGIC       _datasource,
# MAGIC       _ingest_timestamp
# MAGIC     ),
# MAGIC   pbk.UnitPrice
# MAGIC   FROM
# MAGIC     {src_table_name__3_2_1} prd
# MAGIC   INNER JOIN 
# MAGIC     {src_table_name__3_2_2} pbk
# MAGIC   on 
# MAGIC     prd.id = pbk.Product2Id
# MAGIC ```

# COMMAND ----------

# データソースとターゲットテーブルのパスを変数に格納

src_table_name__3_2_1 = f"{catalog_name}.{schema_name}.product2__silver"
src_table_name__3_2_2 = f"{catalog_name}.{schema_name}.pricebook_entry__silver"
tgt_table_name__3_2_1 = f"{catalog_name}.{schema_name}.d_product"

# COMMAND ----------

# テーブルが存在する場合に Drop
spark.sql(
    f"""
    DROP TABLE IF EXISTS {tgt_table_name__3_2_1}
    """
)

# COMMAND ----------

# ToDo Goldテーブルに書き込むためのデータフレームを作成してください。
df = spark.sql(f"""
SELECT
  prd.*
    EXCEPT (
      _datasource,
      _ingest_timestamp
    ),
  pbk.UnitPrice
  FROM
    {src_table_name__3_2_1} prd
  INNER JOIN 
    {src_table_name__3_2_2} pbk
  on 
    prd.id = pbk.Product2Id
""")

# COMMAND ----------

# 処理後の結果を確認
df.display()

# COMMAND ----------

# ToDo `d_product`テーブルへ書き込みを実施してください。
df.write.mode("append").saveAsTable(tgt_table_name__3_2_1)

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__3_2_1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge1. フェデレーションクエリを使ったデータの取り込み

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge2. Databricks Auto Loader によりデータ取り込みを実施してください。
# MAGIC
# MAGIC こちらは Challenge のコンテンツであり、実施は任意です。
# MAGIC
# MAGIC Databricks Auto Loader（自動ローダー）について詳しく知りたい方は、以下のドキュメントをご参照ください。
# MAGIC
# MAGIC > 自動ローダーでは、追加の設定を行わなくても、クラウド ストレージに到着した新しいデータ ファイルが段階的かつ効率的に処理されます。
# MAGIC
# MAGIC 引用元：[自動ローダー - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
# MAGIC
# MAGIC
# MAGIC 取り込み対象のデータについては、下記のオブジェクトと同等のものとなっております。
# MAGIC
# MAGIC - [Campaign | Salesforce プラットフォームのオブジェクトリファレンス | Salesforce Developers](https://developer.salesforce.com/docs/atlas.ja-jp.object_reference.meta/object_reference/sforce_api_objects_campaign.htm)
# MAGIC - [Account | Salesforce プラットフォームのオブジェクトリファレンス | Salesforce Developers](https://developer.salesforce.com/docs/atlas.ja-jp.object_reference.meta/object_reference/sforce_api_objects_account.htm)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
