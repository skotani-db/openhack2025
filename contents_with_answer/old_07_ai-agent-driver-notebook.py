# Databricks notebook source
# MAGIC %md
# MAGIC # "AI Agent" driver notebook
# MAGIC
# MAGIC This notebook demonstrates how to log, test, and deploy a simple "AI Agent" application using Mosaic AI Agent Framework. It covers the following steps:
# MAGIC
# MAGIC 1. Install Mosaic AI Agent Framework
# MAGIC 2. Import required modules
# MAGIC 3. Define path for the notebook
# MAGIC 4. Log the chain to MLflow and test it locally
# MAGIC 5. Register the chain as a model in Unity Catalog
# MAGIC 6. Deploy the chain
# MAGIC 7. View the deployed chain in the Agent Framework UI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install package

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow unitycatalog-ai[databricks] databricks-langchain databricks-agents

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import os
import mlflow
from databricks import agents

# Use the Unity Catalog model registry
mlflow.set_registry_uri('databricks-uc')

# COMMAND ----------

# # Databricksの認証情報を設定
# os.environ["DATABRICKS_HOST"] = "https://adb-4424204643698881.1.azuredatabricks.net"
# os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get(scope="openhack", key="token")

# # サービング環境で文字化け対策として、ロケール・エンコーディングを明示的に指定
# os.environ["LC_ALL"] = "en_US.UTF-8"
# os.environ["PYTHONIOENCODING"] = "utf-8"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the Unity Catalog location where the chain will be logged

# COMMAND ----------

# Create widgets 
dbutils.widgets.text("uc_catalog", "", "catalog")
dbutils.widgets.text("uc_schema", "", "schema")
dbutils.widgets.text("model_name", "ai_agent_openhack", "Model name")

# Retrieve the values from the widgets
uc_catalog = dbutils.widgets.get("uc_catalog")
uc_schema = dbutils.widgets.get("uc_schema")
model_name = dbutils.widgets.get("model_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the chain to MLflow
# MAGIC
# MAGIC Log the chain and the resulting model to the notebook's MLflow experiment.

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLflow logging input parameters

# COMMAND ----------

# Provide an example of the input schema that is used to set the MLflow model's signature
input_example = {
   "messages": [
       {
           "role": "user",
           "content": "What is Retrieval-augmented Generation?",
       }
   ]
}

# Specify the full path to the chain notebook
chain_notebook_file = "06_ai-agent"
chain_notebook_path = os.path.join(os.getcwd(), chain_notebook_file)

print(f"Chain notebook path: {chain_notebook_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the chain

# COMMAND ----------

with mlflow.start_run():
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=chain_notebook_path,
        pip_requirements=[
            "mlflow",
            "databricks-langchain",
            "unitycatalog-ai[databricks]", 
        ],
        artifact_path="chain",
        input_example=input_example,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the chain
# MAGIC Typically at this point, you would evaluate the chain. The following cell shows a simplified approach for demonstration. In a real-world setting, you would use `mlflow.evaluate()` to evaluate the chain before deploying it. For more information, see ([AWS](https://docs.databricks.com/en/generative-ai/agent-evaluation/index.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-evaluation/)).

# COMMAND ----------

############
# Now, test the models locally
# In the actual usage, you would use mlflow.evaluate(...) to run LLM judge and evaluate each chain's quality, cost, and latency.
############

# model_input = {
#     "messages": [
#         {
#             "role": "user",
#             "content": "What is Databricks?",
#         }
#     ]
# }
from langchain.schema import HumanMessage

model_input = [HumanMessage(content="What is Databricks?")]

loaded_model = mlflow.langchain.load_model(logged_chain_info.model_uri)
print(loaded_model.invoke(model_input))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy chain
# MAGIC
# MAGIC To deploy the model, first register the chain from the MLflow Run as a Unity Catalog model.

# COMMAND ----------

# Unity Catalog location
uc_model_name = f"{uc_catalog}.{uc_schema}.{model_name}"

# Register the model to the Unity Catalog
uc_registered_model_info = mlflow.register_model(model_uri=logged_chain_info.model_uri, name=uc_model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC When you deploy the chain by using [`agents.deploy`](https://api-docs.databricks.com/python/databricks-agents/latest/databricks_agent_framework.html#databricks.agents.deploy), the chain is deployed to the following:
# MAGIC - to a REST API endpoint, for querying
# MAGIC - to the review app, which lets you chat with the chain and provide feedback using a web UI.
# MAGIC

# COMMAND ----------

if not uc_catalog or not uc_schema or not model_name:
    raise ValueError("One or more required parameters (uc_catalog, uc_schema, model_name) are missing!")

uc_model_name = f"{uc_catalog}.{uc_schema}.{model_name}"
print(f"Registering model as: {uc_model_name}")  # 確認用ログ


# COMMAND ----------

deployment_info = agents.deploy(uc_model_name, uc_registered_model_info.version)

# COMMAND ----------

# Use this URL to query the app.
deployment_info.query_endpoint

# COMMAND ----------

# Copy this URL to a browser to interact with your application.
deployment_info.review_app_url

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## View deployments
# MAGIC
# MAGIC If you have lost the deployment information captured above, you can find it using [`list_deployments()`](https://api-docs.databricks.com/python/databricks-agents/latest/databricks_agent_framework.html#databricks.agents.list_deployments).
# MAGIC

# COMMAND ----------

deployments = agents.list_deployments()
deployments

