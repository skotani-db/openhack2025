# Databricks notebook source
# MAGIC %md
# MAGIC # メダリオンアーキテクチャに基づいたデータエンジニアリング概要 (標準時間：60分)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 本ノートブックの目的：Databricksにおけるデータ処理の基礎と[メダリオンアーキテクチャ](https://www.databricks.com/jp/glossary/medallion-architecture)について理解を深める
# MAGIC
# MAGIC Q1. Bronze テーブルを作成してください<br>
# MAGIC Q2. Silver テーブルを作成してください<br>
# MAGIC Q3. Gold テーブルを作成してください<br>
# MAGIC Challenge1. フェデレーションクエリを使ったデータ取り込み<br>
# MAGIC Challenge2. Auto Loader によるデータの取り込み

# COMMAND ----------

# MAGIC %md
# MAGIC ![メダリオンアーキテクチャ](https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_01__introduction/delta-lake-medallion-architecture-2.jpeg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## メダリオンアーキテクチャとは(標準時間：5分)
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
# MAGIC ## 事前準備(標準時間：10分)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC 本ノートブックで作業するために以下の処理を行います。
# MAGIC - スキーマ作成
# MAGIC
# MAGIC Unity Catalogで指定されたスキーマを作成するためのGUIを使用した手順は以下の通りです:
# MAGIC
# MAGIC ### スキーマの作成
# MAGIC
# MAGIC 1. Databricksワークスペースにログインします。
# MAGIC
# MAGIC 2. 左側のナビゲーションペインで「カタログ」をクリックします。
# MAGIC
# MAGIC 3. カタログペインで、スキーマを作成したいカタログを選択します。
# MAGIC
# MAGIC 4. 画面右上の「スキーマを作成」ボタンをクリックします。
# MAGIC
# MAGIC 5. スキーマ名として 上のセルに表示された`01_medallion_architecture_for_{username}` を入力します。
# MAGIC
# MAGIC 6. オプションでスキーマの目的を説明するコメントを追加します。AIによるコメントの付与も可能です。
# MAGIC
# MAGIC 7. 「作成」ボタンをクリックしてスキーマを作成します。
# MAGIC
# MAGIC 8. 必要に応じて、スキーマに対する権限が付与されていることを確認します。

# COMMAND ----------

# 本ノートブックで利用するスキーマ
schema_name = f"01_medallion_architecture_for_{user_name}"
print(f"schema_name: `{schema_name}`")

# COMMAND ----------

# MAGIC %md
# MAGIC 本ノートブックで利用する変数を記載(整理)しておきます。  
# MAGIC
# MAGIC | 変数名       | 用途                                     |
# MAGIC |--------------|-----------------------------------------|
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

# 既存のウィジェットを削除
dbutils.widgets.removeAll()

# COMMAND ----------

# 利用するカタログ名とスキーマ名をウィジェットで管理
dbutils.widgets.text("catalog_name", catalog_name, "3.カタログ名")
dbutils.widgets.text("schema_name", schema_name, "4.スキーマ名")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog_name};
# MAGIC USE SCHEMA ${schema_name};

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Q1: Bronze テーブルを作成してください(標準時間：15分)
# MAGIC
# MAGIC <img src="/Volumes/trainer_catalog/default/src_data/images/01_medallion_bronze.png" width="1300" height="500">
# MAGIC
# MAGIC 取り込み対象のデータについては、下記のオブジェクトと同等のものとなっております。
# MAGIC
# MAGIC - [Product2 | Salesforce プラットフォームのオブジェクトリファレンス | Salesforce Developers](https://developer.salesforce.com/docs/atlas.ja-jp.object_reference.meta/object_reference/sforce_api_objects_product2.htm)
# MAGIC - [PricebookEntry | Salesforce プラットフォームのオブジェクトリファレンス | Salesforce Developers](https://developer.salesforce.com/docs/atlas.ja-jp.object_reference.meta/object_reference/sforce_api_objects_pricebookentry.htm)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###ToDo `product2__bronze`, `pricebook_entry__bronze`をUIから作成する
# MAGIC DatabricksのUnity CatalogでVolumeに格納したCSVデータからGUIでテーブルを作成する方法を以下にステップバイステップで説明します。
# MAGIC
# MAGIC
# MAGIC ### 1. Volumeへのアクセス
# MAGIC
# MAGIC 1. Databricksワークスペースにログインします。
# MAGIC 2. 左側のナビゲーションペインで「カタログ」をクリックします。
# MAGIC 3. csvをアップロードしたカタログ、スキーマ、そしてボリュームを選択します。 \
# MAGIC `x_catalog`カタログ, `default`スキーマ, `src_data`ボリューム。
# MAGIC
# MAGIC ### 2. CSVファイルの確認
# MAGIC
# MAGIC 1. 選択したボリュームでCSVファイルを見つけます。 \
# MAGIC `Product2.csv`と`PricebookEntry.csv`が必要です。
# MAGIC 2. ファイル名をクリックしてプレビューを表示し、データの構造を確認します。
# MAGIC
# MAGIC ### 3. テーブル作成ウィザードの起動
# MAGIC
# MAGIC 1. CSVファイルの右側にある「・・・」（その他のオプション）をクリックします。
# MAGIC 2. ドロップダウンメニューから「Create Table」を選択します。
# MAGIC
# MAGIC ### 4. テーブル情報の設定
# MAGIC
# MAGIC 1. 「Table name」フィールドに新しいテーブルの名前を入力します。 \
# MAGIC `Product2.csv`は`product2__bronze`, `PricebookEntry.csv`は`pricebook_entry__bronze`がテーブル名となります。
# MAGIC 2. 「Database」ドロップダウンからテーブルを作成するスキーマを選択します。 \
# MAGIC 上で作成した`01_medallion_architecture_for_{username}`スキーマを指定しましょう。
# MAGIC
# MAGIC ### 5. ファイルフォーマットの確認
# MAGIC
# MAGIC 1. 「詳細な属性」をクリックし、「File type」が「CSV」に設定されていることを確認します。
# MAGIC 2. 必要に応じて「区切り文字」、「エスケープ文字」、「ヘッダー」などのオプションを調整します。
# MAGIC
# MAGIC ### 7. テーブルの作成
# MAGIC
# MAGIC 1. すべての設定を確認したら、画面下部の「テーブルを作成」ボタンをクリックします。
# MAGIC 2. テーブル作成プロセスが完了するまで待ちます。
# MAGIC 3. テーブル作成が完了したら、「サンプルデータ」タブをクリックしてテーブルの内容を確認します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データが書き込まれたことを確認
# MAGIC SELECT * FROM product2__bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データが書き込まれたことを確認
# MAGIC SELECT * FROM pricebook_entry__bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Silver テーブルを作成してください(標準時間：15分)
# MAGIC
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

## `product2__silver`テーブルを作成
# Id についての重複を削除するため、CreatedDateが最新のレコードを残す
query = f"""
CREATE OR REPLACE TABLE {tgt_table_name__2_1_1} AS
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY Id ORDER BY CreatedDate DESC) as row_num 
    FROM {src_table_name__2_1_1}
) tmp 
WHERE row_num = 1
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM product2__silver

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

## ToDo: `pricebook_entry__silver`テーブルを作成
# Id についての重複を削除するため、CreatedDateが最新のレコードを残す
query = <ToDo>

spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pricebook_entry__silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Gold テーブルを作成してください(標準時間：15分)
# MAGIC
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
CREATE OR REPLACE TABLE {tgt_table_name__3_1_1} AS 
SELECT
  Family,
  COUNT(*) AS product_count
  FROM
    {src_table_name__3_1_1}
  GROUP BY
    ALL
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM product_count_by_family

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
# Product2をベースに、pricebook_entryにあるUnitPrice列を追加したデータのテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_2_1} AS
SELECT
<ToDo>
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM d_product;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge. フェデレーションクエリを使ったデータの取り込み(任意)
# MAGIC
# MAGIC こちらは Challenge のコンテンツであり、実施は任意です。
# MAGIC
# MAGIC Azure SQL ServerをDatabricks Unity Catalogの外部テーブルとして登録し、Federation Queryを発行する方法を以下にステップバイステップで示します。
# MAGIC 1, 2, 3 のステップはカタログ作成権限が参加者にないため、4から実施してください
# MAGIC
# MAGIC ## 1. 外部接続の作成
# MAGIC
# MAGIC 1. Databricksワークスペースにログインします。
# MAGIC 2. SQL Editorを開きます。
# MAGIC 3. 以下のSQLコマンドを実行して、SQL Serverへの接続を作成します:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CONNECTION sql_server_connection
# MAGIC TYPE SQLSERVER
# MAGIC OPTIONS (
# MAGIC   host '<your-sql-server-host>',
# MAGIC   port '1433',
# MAGIC   user '<your-username>',
# MAGIC   password '<your-password>',
# MAGIC   database '<your-database-name>'
# MAGIC );
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC ## 2. 外部カタログの登録
# MAGIC
# MAGIC 1. 作成した接続を使用して、外部カタログを登録します:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FOREIGN CATALOG sql_server_catalog
# MAGIC USING CONNECTION sql_server_connection;
# MAGIC ```
# MAGIC
# MAGIC ## 3. アクセス権の付与
# MAGIC
# MAGIC 1. 必要なユーザーまたはグループに外部カタログへのアクセス権を付与します:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG sql_server_catalog TO `user@example.com`;
# MAGIC ```
# MAGIC
# MAGIC ## 4. Federation Queryの発行
# MAGIC
# MAGIC 1. 登録した外部カタログを使用して、Federation Queryを発行します。例:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM sql_server_catalog.schema_name.table_name
# MAGIC WHERE condition;
# MAGIC ```
# MAGIC
# MAGIC 2. ローカルのDatabricksテーブルと結合することも可能です:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT t1.*, t2.column_name
# MAGIC FROM default.local_table t1
# MAGIC JOIN sql_server_catalog.schema_name.table_name t2
# MAGIC ON t1.id = t2.id;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Challenge: sqlserver_catalog カタログの、ringo スキーマにあるorderテーブルにクエリ(SELECT)を発行し、 SQL Server からデータを取得してください
# MAGIC SELECT * FROM sqlserver_catalog.ringo.opportunity

# COMMAND ----------

# MAGIC %md
# MAGIC ## TIPS. Auto Loader によるデータの取り込み
# MAGIC
# MAGIC 今回はUIからBronzeテーブルを作成しましたが、本番用途ではオブジェクトストレージで新しいデータファイルを増分的に取り込むためのAuto Loaderという機能でデータを取り込むことが推奨されます。
# MAGIC
# MAGIC Databricks Auto Loaderについて詳しく知りたい方は、以下のドキュメントをご参照ください。
# MAGIC
# MAGIC > Auto Loader では、追加の設定を行わなくても、クラウド ストレージに到着した新しいデータ ファイルが段階的かつ効率的に処理されます。
# MAGIC
# MAGIC 引用元：[Auto Loader - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
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
