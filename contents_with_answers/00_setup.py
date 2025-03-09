# Databricks notebook source
# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %md
# MAGIC **※本ノートブックは、各チーム（各カタログ）につき1名のみが実施してください。**

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog の確認
# MAGIC 各チームのカタログ(TeamAの場合は、a_catalog を利用)が存在するかを確認してください。  
# MAGIC カタログが存在しない場合は、各グループのコーチに確認して下さい。(環境構築担当者にて、カタログの作成と権限付与を行います。)
# MAGIC
# MAGIC
# MAGIC 参考リンク：  
# MAGIC [カタログを作成する - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/catalogs/create-catalog#catalogexplorer)  
# MAGIC [Unity Catalog の特権の管理 - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/data-governance/unity-catalog/manage-privileges/#grant-permissions-on-objects-in-a-unity-catalog-metastore)  
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## (参考)Databricksで新規カタログを作成するSQL実行例
# MAGIC Databricksで新規カタログを作成するSQL実行例です。  
# MAGIC **※本日の環境では、皆さんにカタログの作成権限がないため、以下の処理を実行時してもエラーとなります。**

# COMMAND ----------

# {catalog_name} という名前のカタログをDatabricks上に作成するSQL文を生成
create_catalog_ddl = f""" 
CREATE CATALOG IF NOT EXISTS {catalog_name}
""".strip()

# 生成されたDDLを表示（デバッグや確認用）
print(create_catalog_ddl) 

# Spark SQLを使用してカタログ作成SQLを実行
# spark.sql(create_catalog_ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## サンプルデータ配置用の Volume を作成
# MAGIC
# MAGIC ※本手順は、チーム内（同一カタログ）で一度だけ実施してください。(複数名が実施しても結果は同じです)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. 下記セルを実行して Volume を作成
# MAGIC

# COMMAND ----------

print(f"volume_name: `{src_volume_name}`")

# COMMAND ----------

# 本ノートブックで利用するソースファイルを Volume に移動
print(f"volume_name: `{src_volume_name}`")
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.{src_schema_name}.{src_volume_name}
    """
)

# COMMAND ----------

# ソースファイルを配置するフォルダを作成
dbutils.fs.mkdirs(f"/Volumes/{catalog_name}/{src_schema_name}/{src_volume_name}/{src_folder_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ソースデータファイルのダウンロードとボリュームへのアップロード
# MAGIC 1. 現在のノートブックの左型タブにある`Workspace (Ctrl + Alt + E)`を選択し、現在のディレクトリにある`data`フォルダに移動
# MAGIC 1. `sample_data_01`フォルダのハンバーガーメニュー（`︙`）を選択して、 `ダウンロード形式：` -> `ソースファイル` をクリックしてデータファイルをダウンロード
# MAGIC 1. ダウンロードした Zip ファイルを解凍
# MAGIC 1. 現在のノートブックの左型タブにある`Catalog (Ctrl + Alt + C)`を選択後、`default`スキーマ下にある`src_data` Volume の`sample_data_01`フォルダにてハンバーガーメニュー（`︙`）を選択し、`このボリュームにアップロード`を選択
# MAGIC 1. 表示されたウィンドウに解凍した CSV ファイルをすべて配置して、`アップロード`を選択
# MAGIC 1. 下記のセルを実行し、ファイルが配置されていることを確認

# COMMAND ----------

print(f"/Volumes/{catalog_name}/{src_schema_name}/{src_volume_name}/{src_folder_name}")

# COMMAND ----------

# ファイルが配置されていることを確認
src_file_dir = f"/Volumes/{catalog_name}/{src_schema_name}/{src_volume_name}/{src_folder_name}"
file_list = dbutils.fs.ls(src_file_dir)
if file_list == []:
    raise Exception("ファイルがディレクトリにありません。ファイルを配置してください。")
display(dbutils.fs.ls(src_file_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (参考) dbutils を使ったコピー

# COMMAND ----------

# (参考) ソースデータファイルのダウンロードとアップロードをGUIで行いましたが、dbutils を使ってコピーすることも可能です。

# ソースデータのパスを設定
source_path = "/Volumes/trainer_catalog/default/src_data/sample_data_01"

# 宛先パスを設定
destination_path = f"/Volumes/{catalog_name}/{src_schema_name}/{src_volume_name}/{src_folder_name}"

# dbutils.fs.lsの結果からファイル名を取り出し、各ファイルを Volume にコピーする
files = dbutils.fs.ls(source_path)

for file in files:
    source_file_path = file.path
    destination_file_path = f"{destination_path}/{file.name}"
    dbutils.fs.cp(source_file_path, destination_file_path)
    print(f"Copied {source_file_path} to {destination_file_path}")

# COMMAND ----------

# 一括コピーも可能
dbutils.fs.cp(source_path, destination_path, recurse=True)

display(dbutils.fs.ls(destination_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 参考リンク：  
# MAGIC [Databricks Utilities (dbutils) リファレンス](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
