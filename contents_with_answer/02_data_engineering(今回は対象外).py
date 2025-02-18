# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Live Tables によるデータエンジニアリング (標準時間：60分)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 本ノートブックの目的：Delta Live Tablesによってパイプライン構築がモダナイズされることを実感する
# MAGIC
# MAGIC Q1. Delta Live Tablesのセッティングを行なってください<br>
# MAGIC Q2. パイプラインを実行する（Run a Pipeline）<br>
# MAGIC Q3. DAGを調べる（Exploring the DAG）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# 本ノートブックで利用するスキーマを作成
schema_name = f"02_data_engineering_for_{user_name}"
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

# COMMAND ----------

# 本ノートブックで利用するソースファイルを Volume に移動
file_dir = f"/Volumes/{catalog_name}/{src_schema_name}/{src_volume_name}/{src_folder_name}"
volume_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

dbutils.fs.cp(file_dir, volume_dir, recurse=True)
display(dbutils.fs.ls(volume_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Q1. Delta Live Tablesのセッティングを行なってください(標準時間：15分)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC このセクションでは、コースウェアに付属しているノートブックを使って、今までのセクションで構築したメダリオンアーキテクチャのパイプラインを構築します。 　<br>
# MAGIC 次のレッスンでは、ノートブックの内容について見ていきます。　<br>
# MAGIC 　<br>
# MAGIC 1. サイドバーの**Workflows**ボタンをクリックします。
# MAGIC 1. **Delta Live Tables**タブを選択します。
# MAGIC 1. **Create Pipeline**をクリックします。
# MAGIC 1. **General**における**Pipeline name**を入力します。これらの名前は一意である必要があるため、下記のセルに記載されている`pipeline_name`を使用することをおすすめします。
# MAGIC 1. **General**にて**Serveless**をチェックします。
# MAGIC 1. **General**における**Pipeline mode**では、**Trigger**を選択します。本手順ではファイルの取り込みを1回のみ行うため、`Trigger`を選択しています。
# MAGIC    * このフィールドでは、パイプラインの実行方法を指定します。
# MAGIC    * **Trigger**パイプラインは一度だけ実行され、次の手動またはスケジュールされた更新まではシャットダウンします。
# MAGIC    * **Continuous**パイプラインは継続的に実行され、新しいデータが到着するとそのデータを取り込みます。 レイテンシとコスト要件に基づいてモードを選択してください。
# MAGIC 1. **Source code**における**Paths**では、ナビゲーターを使って**02_delta_live_table_conf**という付録のノートブックを探して選択します。
# MAGIC    * このドキュメントは標準のDatabricksノートブックですが、SQL構文はDLTテーブル宣言に特化しています。
# MAGIC    * 次のエクササイズでは、構文について見ていきます。
# MAGIC 1. **Destination**における**Storage options**にて**Unity Catalog**をチェックし、下記のセルで`Catalog`と`Target schema`の隣に表示されているカタログ名とスキーマ名を指定します。
# MAGIC    * このフィールドは任意です。指定しなかった場合、テーブルはメタストアに登録されませんが、引き続きDBFSでは使用できます。 このオプションに関して詳しく知りたい場合は、こちらの<a href="https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/publish-data" target="_blank">ドキュメント</a>を参考にしてください。
# MAGIC 1. **Notifications**にて**Add notification**を選択して下記の 3 つを設定します。
# MAGIC    * **Email(s) (comma-separated)**にメールアドレスを入力
# MAGIC    * **On update**をすべてチェック
# MAGIC    * **On flow**をすべてチェック
# MAGIC 1. **Advanced**における**Configuratione**にて**Add configuration**を選択して下記の 2 つを設定します。
# MAGIC    * **Key**に**mypipeline.catalog_name**を、Value にその key に相当する下記セルの値を設定
# MAGIC    * **Key**に**mypipeline.schema_name**を、Value にその key に相当する下記セルの値を設定
# MAGIC 1. **Create**をクリックします。
# MAGIC
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [パイプラインを作成する](https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/tutorial-pipelines#--create-a-pipeline)
# MAGIC - [Delta Live Tables のパイプライン設定を構成する](https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/settings)
# MAGIC - [サーバーレス コンピューティングで Delta Live Tables を使用してフル マネージド パイプラインを作成する](https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/serverless-dlt)

# COMMAND ----------

print("-- Pipeline name")
pipeline_name = f"dlt_pipe_by_{user_name}"
print(f"pipeline_name: {pipeline_name}")
print("")
print("-- Destination")
print(f"Catalog: {catalog_name}")
print(f"Target schema: {schema_name}")
print("")
print("-- Configuration")
print(f"mypipeline.catalog_name: {catalog_name}")
print(f"mypipeline.schema_name: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. パイプラインを実行する（Run a Pipeline）(標準時間：10分)
# MAGIC
# MAGIC パイプラインを構築したら、そのパイプラインを実行します。
# MAGIC
# MAGIC 1. **Development**を選択し、Developmentモードでパイプラインを実行します。
# MAGIC 2. **Start**をクリックします。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Q3. DAGを調べる（Exploring the DAG）(標準時間：5分)
# MAGIC
# MAGIC パイプラインが完了すると、実行フローがグラフ化されます。
# MAGIC
# MAGIC テーブルを選択すると詳細を確認できます。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Q4. パイプラインの修正 (標準時間：30分)
# MAGIC
# MAGIC `02_delta_live_table_conf`ノートブックに記載されている ToDo を実施してパイプラインを修正してください。修正後、下記の手順で DAG が変更されます。
# MAGIC
# MAGIC 1. **Development**を選択し、Developmentモードでパイプラインを実行します。
# MAGIC 2. **Start**、あるいは、**Full refresh all**をクリックします。
# MAGIC 3. 修正内容に応じて DAG が変更されることを確認します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
