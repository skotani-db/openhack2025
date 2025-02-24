# Databricks notebook source
# MAGIC %md
# MAGIC # LangGraph custom schema driver notebook
# MAGIC
# MAGIC This notebook shows you how to log, register, and deploy a LangGraph AI agent compatible with Mosaic AI Agent Framework that accepts custom inputs and returns custom outputs. 
# MAGIC
# MAGIC To ensure compatibility, the agent must conform to Mosaic AI Agent Framework schema requirements, see ([AWS](https://docs.databricks.com/en/generative-ai/agent-framework/agent-schema.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-framework/agent-schema)).
# MAGIC
# MAGIC ### Model-as-code notebook
# MAGIC
# MAGIC Mosaic AI Agent Framework uses MLflows **Models-as-code** development workflow, which requires two notebooks: 
# MAGIC
# MAGIC - A driver notebook that logs, registers, and deploys the agent (this notebook)
# MAGIC - An agent notebook that defines the agent's logic
# MAGIC   - You can find the agent notebook for this driver, **custom-langgraph-schema-agent**, on Databricks documentation ([AWS](https://docs.databricks.com/en/generative-ai/agent-framework/agent-schema.html#langgraph-custom-schema-driver-notebook) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-framework/agent-schema#langgraph-custom-schema-driver-notebook))
# MAGIC
# MAGIC For more information on Model-as-code, see MLflow's [Models as code guide](https://mlflow.org/docs/latest/model/models-from-code.html).
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC This notebook requires a Unity Catalog enabled workspace.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses LangGraph, but Mosaic AI Agent Framework is compatible with other agent authoring frameworks, like LlamaIndex.

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow databricks-agents langchain langgraph-checkpoint  langchain_core langgraph pydantic databricks-langchain
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [agent]($./agent) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Log the model to MLflow
import os
import mlflow
from mlflow.models.signature import ModelSignature
from mlflow.types.llm import CHAT_MODEL_INPUT_SCHEMA, CHAT_MODEL_OUTPUT_SCHEMA

agent_signature = ModelSignature(
    CHAT_MODEL_INPUT_SCHEMA,
    CHAT_MODEL_OUTPUT_SCHEMA,
)

with mlflow.start_run():
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=os.path.join(
            os.getcwd(),
            "08_ai-agent",
        ),
        pip_requirements=[
            "langchain",
            "langgraph",
            "pydantic",
            "databricks-langchain",
            "databricks-connect==15.1.0",
            # "langgraph-checkpoint==1.0.12",
            # "langchain_core",
            # "databricks-openai",
        ],
        artifact_path="agent",
        # input_example=input_example,
        signature=agent_signature

    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = "trainer_catalog"
schema = "05_vector_search_index_for_nssol"
model_name = "case-support-agent"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents

# Deploy the model to the review app and a model serving endpoint
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version)
