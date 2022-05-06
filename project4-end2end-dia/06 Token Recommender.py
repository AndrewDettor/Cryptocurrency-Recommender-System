# Databricks notebook source
# MAGIC %md
# MAGIC ## Token Recommendation
# MAGIC <table border=0>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png'></td>
# MAGIC     <td>Your application should allow a specific wallet address to be entered via a widget in your application notebook.  Each time a new wallet address is entered, a new recommendation of the top tokens for consideration should be made. <br> **Bonus** (3 points): include links to the Token contract on the blockchain or etherscan.io for further investigation.</td></tr>
# MAGIC   </table>

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
# MAGIC ## Your code starts here...

# COMMAND ----------

# MAGIC %md
# MAGIC ### find the top 10 most popular erc20 token based on transfer counts(used when wallet address does not have other similar users)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 10 popular tokens based on # of transactions: used as default token recommendations
# MAGIC CREATE table IF NOT EXISTS g09_db.popular10_tokens as
# MAGIC (select image, links, name from (((select g09_db.silver_token_table.token_address, count(*) as transaction_num from g09_db.silver_token_table inner join g09_db.erc20_token_transfers on g09_db.silver_token_table.token_address = g09_db.erc20_token_transfers.token_address group by g09_db.silver_token_table.token_address order by transaction_num desc limit(10)) as top10) inner join g09_db.silver_token_table on top10.token_address=g09_db.silver_token_table.token_address));

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load prediction model

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
from pyspark.sql import functions as F
import builtins as p
 
#load tables
user_df = spark.sql("select * from g09_db.silver_user_table")
token_balance_df = spark.sql("select * from g09_db.silver_token_balance")
token_df = spark.sql("select * from g09_db.silver_token_table")
pop10_df = spark.sql("select * from g09_db.popular10_tokens")
 
#load model
# - get the latest version number
client = MlflowClient()
model_version_infos = client.search_model_versions(f"name = 'G09_test'")
new_model_version = [model_version_info.version for model_version_info in model_version_infos]
max_version = p.max(new_model_version)
# - load the latest model
model_path = f"models:/G09_test/{max_version}"
latest_model = mlflow.spark.load_model(model_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate HTML

# COMMAND ----------

UserID_query = user_df.filter(col("users")==wallet_address)
 
#set up the html head
htmlCode =  """
    <h1>Recommend Tokens for user address:</h1>
    """
htmlCode += "<p>"+ wallet_address + "</p>\n"
 
if(UserID_query.count()==0): #handle the case where no such user id in the database
    htmlCode += """
    <style>
            td{
                padding: 20px;
            }
            table{
                border-collapse: collapse;
            }
            a{
                text-decoration: none;
            }
    </style>
    <p>No such user in the database, please check your input.</p>
    <p>10 popular tokens:</p>  
    <table>
    <tr><td><b>Logo</b></td><td><b>Name</b></td><td><b>Link</b></td></tr>
    """
    for row in pop10_df.collect():  
            logo = row["image"]
            name = row["name"]
            link = row["links"]
            html_row = "<tr><td><img src='" + logo + "' alt='token logo'></td><td>" + name + "</td><td>" + "<a href='" + link + "'><img src='https://www.clipartkey.com/mpngs/m/84-840903_transparent-grass-tuft-clipart-weaknesses-icon.png' alt='link image' width='35px' height='35px'></a></td></tr>\n"
            htmlCode+=html_row
    displayHTML(htmlCode)
else: #make predictions by using the model
    UserID = UserID_query.collect()[0][1]
    
    used_tokens = token_balance_df.filter(token_balance_df.user_id == UserID).join(token_df, token_df.id == token_balance_df.token_id).select('user_id', 'token_id', 'token_address', 'name', 'image')
    used_tokens_list = []
    for token in used_tokens.collect():
        used_tokens_list.append(token['token_id'])
 
    # generate dataframe of transfered tokens
    untransfered_tokens = token_balance_df.filter(~token_balance_df['token_id'].isin(used_tokens_list)).select('token_id').withColumn('user_id', F.lit(UserID)).distinct()
 
    predicted_likes = latest_model.transform(untransfered_tokens)
    # remove NaNs
    predicted_likes = predicted_likes.filter(predicted_likes['prediction'] != float('nan'))
 
    final_result = predicted_likes.join(token_df, token_df.id == predicted_likes.token_id).select(predicted_likes.user_id,'token_id', 'token_address', 'name','image','links','prediction').distinct().orderBy('prediction', ascending = False)
    
    htmlCode += """
        <style>
            td{
                padding: 20px;
            }
            table{
                border-collapse: collapse;
            }
            a{
                text-decoration: none;
            }
        </style>
        <table>
        <tr><td><b>Logo</b></td><td><b>Name</b></td><td><b>Link</b></td></tr>
        """
    if(final_result.count() == 0): #if no similar users detected, display 10 popular tokens
        for row in pop10_df.collect():  
            logo = row["image"]
            name = row["name"]
            link = row["links"]
            html_row = "<tr><td><img src='" + logo + "' alt='token logo'></td><td>" + name + "</td><td>" + "<a href='" + link + "'><img src='https://www.clipartkey.com/mpngs/m/84-840903_transparent-grass-tuft-clipart-weaknesses-icon.png' alt='link image' width='35px' height='35px'></a></td></tr>\n"
            htmlCode+=html_row
        displayHTML(htmlCode)
    else:
        #recommend the 10 most similar tokens based on the model
        final_result = final_result.head(10)
        final_result_df = spark.createDataFrame(final_result)
        for row in final_result_df.collect():  
            logo = row["image"]
            name = row["name"]
            link = row["links"]
            html_row = "<tr><td><img src='" + logo + "' alt='token logo'></td><td>" + name + "</td><td>" + "<a href='" + link + "'><img src='https://www.clipartkey.com/mpngs/m/84-840903_transparent-grass-tuft-clipart-weaknesses-icon.png' alt='link image' width='35px' height='35px'></a></td></tr>\n"
            htmlCode+=html_row
        displayHTML(htmlCode)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
