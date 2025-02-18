# Databricks notebook source
# チーム名、及び、ユーザー名を設定
team_name = "trainer_catalog"  ## team_name はカタログ名となる
user_name = "nssol"  ## user_name はスキーマ名の一部となる
print(f"user_name: `{user_name}`")

# 利用するカタログ名を設定
catalog_name = team_name
print(f"catalog_name: `{catalog_name}`")

# COMMAND ----------

# ソースファイルを配置する Volume 名を設定
src_volume_name = "src_data"
src_schema_name = "default"
src_folder_name = "sample_data_01"
