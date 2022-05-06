# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Using the silver delta table(s) that were setup by your ETL module train and validate your token recommendation engine. Split, Fit, Score, Save
# MAGIC - Log all experiments using mlflow
# MAGIC - capture model parameters, signature, training/test metrics and artifacts
# MAGIC - Tune hyperparameters using an appropriate scaling mechanism for spark.  [Hyperopt/Spark Trials ](https://docs.databricks.com/_static/notebooks/hyperopt-spark-ml.html)
# MAGIC - Register your best model from the training run at **Staging**.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Your Code starts here...

# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline

# COMMAND ----------

# import libraries
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# load the silver delta tables
token_balance_df = spark.sql("select * from g09_db.silver_token_balance")
token_df = spark.sql("select * from g09_db.silver_token_table")
user_df = spark.sql("select * from g09_db.silver_user_table")

# COMMAND ----------

# cache
token_balance_df.cache()
token_balance_df.printSchema()
token_balance_df.show(5)
 
token_df.cache()
token_df.printSchema()
token_df.show(5)
 
user_df.cache()
user_df.printSchema()
user_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preprocessing

# COMMAND ----------

# Normalize
#max_b, min_b = token_balance_df.select(max("balance"), min("balance")).first()
mean_b, std_b = token_balance_df.select(mean("balance"), stddev("balance")).first()
#token_balance_df_s = token_balance_df.withColumn("balance_s", ((2000*(col("balance")-min_b))/(max_b-min_b)-1000))
token_balance_df_s = token_balance_df.withColumn("balance", (col("balance") - mean_b) / std_b)
display(token_balance_df_s)

# COMMAND ----------

# test_balance = token_balance_df_s.head(10000)
# test_balance_df = spark.createDataFrame(test_balance)
# display(test_balance_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Model

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from mlflow.types.schema import Schema, ColSpec
from mlflow.models.signature import ModelSignature
import numpy as np

# COMMAND ----------

# 60% for training, 20% for validation, and 20% for testing
seed = 29
(split60_df, split20a_df, split20b_df) = token_balance_df_s.randomSplit([0.6, 0.2, 0.2], seed = seed)
 
# cache the training, validation, and test data
training_df = split60_df.cache()
validation_df = split20a_df.cache()
test_df = split20b_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Parameter Initialization

# COMMAND ----------

# Initiate ALS learner
als = ALS()
 
# Set the parameters for the method
als.setSeed(seed).setItemCol("token_id").setRatingCol("balance").setUserCol("user_id").setColdStartStrategy("drop")
 
# Evaluation metric for the test dataset
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="balance", metricName="rmse")
 
# Set up grid search parameters and values to find out the best hyperparameters
grid = ParamGridBuilder().addGrid(als.maxIter, [5, 10]).addGrid(als.regParam, [0.25, 0.5, 0.75]).addGrid(als.rank, [4, 8, 12, 16]).build()
 
# Cross validator: created using the reg_eval and grid defined above
cv = CrossValidator(estimator=als, evaluator=reg_eval, estimatorParamMaps=grid, numFolds=3)

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Training

# COMMAND ----------

# setup the schema for the model
input_schema = Schema([
    ColSpec("long", "token_id"), 
    ColSpec("long", "user_id"),])
output_schema = Schema([ColSpec("float")])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)
    
with mlflow.start_run(run_name="ALS-token-run") as run:
    mlflow.set_tags({"group": 'G09', "class": "DSCC202-402"})
    # mlflow.log_params({"user_rating_training_data_version": self.training_data_version,"minimum_play_count":self.minPlayCount})
 
    # Run the cross validation on the training dataset. The cv.fit() call returns the best model it found.
    cvModel = cv.fit(training_df)
    best_model = cvModel.bestModel
    # print(cvModel.getEstimatorParamMaps()[np.argmax(cvModel.avgMetrics)])
    mlflow.log_param("Rank", best_model._java_obj.parent().getRank())
    mlflow.log_param("MaxIter", best_model._java_obj.parent().getMaxIter())
    mlflow.log_param("RegParam", best_model._java_obj.parent().getRegParam())
    
    # validation_metric = reg_eval.evaluate(cvModel.transform(validation_df))
    # Run the model to create a prediction. Predict against the validation_df.
    predict_df = cvModel.transform(validation_df)
    
    # Remove NaN values from prediction
    predicted_tokens_df = predict_df.filter(predict_df.prediction != float('nan'))
    predicted_tokens_df = predicted_tokens_df.withColumn("prediction", F.abs(predicted_tokens_df["prediction"]))
    
    # Evaluate the best model's performance on the validation dataset and log the result.
    validation_metric = reg_eval.evaluate(predicted_tokens_df)
    mlflow.log_metric('test_' + reg_eval.getMetricName(), validation_metric) 
    
    # Log the best model.
    mlflow.spark.log_model(spark_model=cvModel.bestModel, signature = signature,artifact_path='als-model', registered_model_name="G09_test")

# COMMAND ----------

# MAGIC %md
# MAGIC # staging the model

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
import builtins as p
client = MlflowClient()
model_version_infos = client.search_model_versions(f"name = 'G09_test'")
new_model_version = [model_version_info.version for model_version_info in model_version_infos]
max_version = p.max(new_model_version)
client.transition_model_version_stage(name="G09_test", version=max_version, stage="Staging",)

# COMMAND ----------

# MAGIC %md
# MAGIC # Remove the trained model path

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/dscc202-datasets/misc/G09/token_model", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # save model to G09 path

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/mnt/dscc202-datasets/misc/G09/token_model")

# COMMAND ----------

mlflow.spark.save_model(best_model, "/dbfs/mnt/dscc202-datasets/misc/G09/token_model")

# COMMAND ----------

ALS_model_loaded = mlflow.spark.load_model("dbfs:/mnt/dscc202-datasets/misc/G09/token_model")

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
