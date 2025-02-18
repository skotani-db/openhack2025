# Databricks notebook source
# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %md
# MAGIC ※本ノートブックは、各チーム（各カタログ）につき1名のみが実施してください。

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog を作成
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC コンテンツを実行するためのカタログが存在するかを確認してください。カタログが存在しない場合は、以下のセルを使用してカタログを作成してください。カタログを作成した場合、チームメンバーに対してALL PRIVILEGES権限を付与してください。
# MAGIC
# MAGIC - [Unity Catalog の特権の管理 - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/data-governance/unity-catalog/manage-privileges/#grant-permissions-on-objects-in-a-unity-catalog-metastore)
# MAGIC
# MAGIC カタログ作成処理が下記エラーのようになった場合には、 下記リンクを参考に Cataglog Explorer にて手動でカタログを作成してください。
# MAGIC
# MAGIC > Metastore storage root URL does not exist. Please provide a storage location for the catalog (for example 'CREATE CATALOG myCatalog MANAGED LOCATION '<location-path>'). Alternatively set up a metastore root storage location to provide a storage location for all catalogs in the metastore.
# MAGIC
# MAGIC 参考リンク： [カタログを作成する - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/catalogs/create-catalog#catalogexplorer)
# MAGIC
# MAGIC カタログ作成時に下記エラーのようになった場合には、 Unity Catalog のメタストア管理者に問い合わせを実施してください。
# MAGIC
# MAGIC > PERMISSION DENIED: User does not have CREATE CATALOG on Metastore '{catalog_name}'.
# MAGIC

# COMMAND ----------

# カタログが存在しない場合には下記のコメントアウトを解除して実行
# create_catalog_ddl = f"""
# CREATE CATALOG IF NOT EXISTS {catalog_name}
# """.strip()
# print(create_catalog_ddl)
# spark.sql(create_catalog_ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## サンプルデータ配置用の Volume を作成
# MAGIC
# MAGIC ※本手順は、チーム内（同一カタログ）で一度だけ実施すれば十分です。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. 下記セルを実行して Volume を作成
# MAGIC

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
# MAGIC 1. 現在のノートブックの左型タブにある`Workspace (Ctrl + Alt + E)`を選択し、現在のディレクトリにある`data`フォルダに移動
# MAGIC 1. `sample_data_01`フォルダのハンバーガーメニュー（`︙`）を選択して、 `export` -> `Source File` をクリックしてデータファイルをダウンロード
# MAGIC 1. ダウンロードした Zip ファイルを解凍
# MAGIC 1. 現在のノートブックの左型タブにある`Catalog (Ctrl + Alt + C)`を選択後、`src_data` Volumne の`sample_data_01`フォルダにてハンバーガーメニュー（`︙`）を選択し、`Upload to volume`を選択
# MAGIC 1. 表示されたウィンドウに解凍した CSV ファイルをすべて配置して、`Upload`を選択
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
# MAGIC ## End
