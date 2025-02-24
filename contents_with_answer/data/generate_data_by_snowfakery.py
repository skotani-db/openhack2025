# Databricks notebook source
# MAGIC %md
# MAGIC ## ライブラリのインストール

# COMMAND ----------

# MAGIC %pip install cumulusci -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sh snowfakery --version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowfakery にて利用する YAML ファイル作成の事前準備
# MAGIC
# MAGIC 下記のオブジェクトのデータを生成する。 Product2 のみ製品名や価格を指定するため、ループ処理にて YAML ファイルの生成を実施する。
# MAGIC
# MAGIC - Lead
# MAGIC - Opportunity
# MAGIC - Order
# MAGIC - Case
# MAGIC - User
# MAGIC - Account
# MAGIC - Contract
# MAGIC - Campaign
# MAGIC - Product2
# MAGIC - Price Book Entry
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### YAML ファイルの確認

# COMMAND ----------

# Snowfakery 実行時の YAML ファイルの一覧を表示
import os

current_dir = os.getcwd()
display(dbutils.fs.ls(f"file:{current_dir}/data_config"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product のデータを生成する YAML ファイルを生成

# COMMAND ----------

import yaml

# COMMAND ----------

products = [
    {"family": "Low-End", "name": "Low-End ARM PC", "unit_price": 100000},
    {"family": "Middle-End", "name": "Middle-End ARM PC", "unit_price": 150000},
    {"family": "High-End", "name": "High-End ARM PC", "unit_price": 250000},
    {"family": "Low-End", "name": "Low-End x86 PC", "unit_price": 80000},
    {"family": "Middle-End", "name": "Middle-End x86 PC", "unit_price": 130000},
    {"family": "High-End", "name": "High-End x86 PC", "unit_price": 230000},
    {"family": "Low-End", "name": "Low-End ARM Tablet", "unit_price": 60000},
    {"family": "Middle-End", "name": "Middle-End ARM Tablet", "unit_price": 100000},
    {"family": "High-End", "name": "High-End ARM Tablet", "unit_price": 150000},
    {"family": "Low-End", "name": "Low-End ARM Smartphone", "unit_price": 50000},
    {"family": "Middle-End", "name": "Middle-End ARM Smartphone", "unit_price": 80000},
    {"family": "High-End", "name": "High-End ARM Smartphone", "unit_price": 130000},
]

# COMMAND ----------

base_yml_path = 'data_config/Product2_PricebookEntry_files/_Product2_PricebookEntry.recipe.yml'
tgt_dir = 'data_config/Product2_PricebookEntry_files'

# COMMAND ----------

# base_yml_path のファイル内容を読み込み、 products の項目で置換
import yaml

with open(base_yml_path, 'r') as file:
    yaml_content = yaml.safe_load(file)

new_yaml_content = yaml_content.copy()
yml_names = []
for prd in products:
    tgt_file_name = f"{prd['name'].replace(' ', '_')}.yml"
    tgt_file_path = f"{tgt_dir}/{tgt_file_name}"
    for key,val in prd.items():
        for item in new_yaml_content:
            if item.get('var') == key:
                item['value'] = val
    with open(tgt_file_path, 'w') as file:
        yaml.dump(new_yaml_content, file, allow_unicode=True)
    yml_names.append(tgt_file_name)

# COMMAND ----------

# data_config/Product2_PricebookEntry.yml ディレクトリに出力結果をコピー＆ペースを実施
base_str = "- include_file: Product2_PricebookEntry_files"
for y_name in yml_names:
    print(f"{base_str}/{y_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowfakery にてデータを生成

# COMMAND ----------

# ディレクトリを初期化
import os

dir_name = "sample_data_01"

current_dir = os.getcwd()
dbutils.fs.rm(f"file:{current_dir}/{dir_name}", True)
dbutils.fs.mkdirs(f"file:{current_dir}/{dir_name}")

# COMMAND ----------

# MAGIC %sh
# MAGIC snowfakery data_config/main.yml --output-format CSV --output-folder sample_data_01

# COMMAND ----------

# 生成したデータを表示
import os

dir_name = "sample_data_01"

current_dir = os.getcwd()
file_list = dbutils.fs.ls(f"file:{current_dir}/{dir_name}")

files_dir = []
for f_dir in file_list:
    files_dir.append(f_dir.path)

for f_dir in files_dir:
    file_name = f_dir.split("/")[-1]
    if file_name == "csvw_metadata.json":
        continue
    print(f"-- {file_name}")
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .csv(f_dir)
    )
    print(df.count())
    df.limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
