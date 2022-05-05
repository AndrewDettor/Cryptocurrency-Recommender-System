# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Implement a routine to "promote" your model at **Staging** in the registry to **Production** based on a boolean flag that you set in the code.
# MAGIC - Using wallet addresses from your **Staging** and **Production** model test data, compare the recommendations of the two models.

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
# MAGIC ## Your Code Starts Here...

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # find the latest version

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
import builtins as p
client = MlflowClient()
model_version_infos = client.search_model_versions(f"name = 'G09_test'")
new_model_version = [model_version_info.version for model_version_info in model_version_infos]
max_version = p.max(new_model_version)

# COMMAND ----------

# MAGIC %md 
# MAGIC # push the latest model to production if there is no production model

# COMMAND ----------

info_production_model = client.get_latest_versions("G09_test", stages=["Production"])
if (len(info_production_model) == 0):
    client.transition_model_version_stage(name="G09_test", version=max_version, stage="Production",)

# COMMAND ----------

# MAGIC %md
# MAGIC # load model

# COMMAND ----------

model_path = f"models:/G09_test/production"
ALS_model_production = mlflow.spark.load_model(model_path)
ALS_model_staging = mlflow.spark.load_model(f"models:/G09_test/{max_version}")

# COMMAND ----------

token_balance_df = spark.sql("select * from g09_db.silver_token_balance")
token_df = spark.sql("select * from g09_db.silver_token_table")
user_df = spark.sql("select * from g09_db.silver_user_table")

# COMMAND ----------

# MAGIC %md
# MAGIC compare the average value of predicted values to see which model has more similar tokens recommended.

# COMMAND ----------

# set of random users
user = [15398787,60160501,60160502,1435267]
flag_promote = False
count = 0

for UserID in user:
    used_tokens = token_balance_df.filter(token_balance_df.user_id == UserID).join(token_df, token_df.id == token_balance_df.token_id).select('user_id', 'token_id','token_address', 'name', 'image')

    used_tokens_list = []
    for token in used_tokens.collect():
        used_tokens_list.append(token['token_id'])
    # generate dataframe of transfered tokens
    untransfered_tokens = token_balance_df.filter(~token_balance_df['token_id'].isin(used_tokens_list)).select('token_id').withColumn('user_id',F.lit(UserID)).distinct()
    
    # production Model
    predicted_likes = ALS_model_production.transform(untransfered_tokens)
    # remove NaNs
    predicted_likes = predicted_likes.filter(predicted_likes['prediction'] != float('nan'))

    final_result = predicted_likes.join(token_df, token_df.id == predicted_likes.token_id).select(predicted_likes.user_id,'token_id', 'token_address', 'name','image','links','prediction').distinct().orderBy('prediction', ascending = False)
    final_result = final_result.head(10)
    if(len(final_result) == 0):
        production_likes_avg = 0
    else:
        final_result_df = spark.createDataFrame(final_result)
        production_likes_avg = final_result_df.select(sum("prediction")).first()[0]
    #-------------------------------------------------------------
    # Staging Model
    predicted_likes_1 = ALS_model_staging.transform(untransfered_tokens)
    # remove NaNs
    predicted_likes_1 = predicted_likes_1.filter(predicted_likes_1['prediction'] != float('nan'))

    final_result_1 = predicted_likes_1.join(token_df, token_df.id == predicted_likes_1.token_id).select(predicted_likes_1.user_id,'token_id', 'token_address', 'name','image','links','prediction').distinct().orderBy('prediction', ascending = False)
    final_result_1 = final_result_1.head(10)
    if (len(final_result_1) == 0):
        staging_likes_avg = 0
    else:
        final_result_df_1 = spark.createDataFrame(final_result_1)
        staging_likes_avg = final_result_df_1.select(sum("prediction")).first()[0]
    # staging model is better than production model 
    if (production_likes_avg < staging_likes_avg):
        count = count+1

# COMMAND ----------

if (count >= 2):
    flag_promote = True

info = client.get_latest_versions("G09_test", stages=["Production"])
production_version = int(info[0].version)
info_staging = client.get_latest_versions("G09_test", stages=["Staging"])
staging_version = int(info_staging[0].version)

if (flag_promote):
    client.transition_model_version_stage(name="G09_test", version=production_version, stage="Archive",)
    client.transition_model_version_stage(name="G09_test", version=staging_version, stage="Production",)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
