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

# FIXME!!!!!!!!!! Need to change "token_df" to the actual recommendation result df
token_df = spark.sql("select * from g09_db.silver_token_table limit(3)")
display(token_df)

# COMMAND ----------

html_code = """
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
<h1>Recommend Tokens for user address:</h1>
"""
html_code += "<p>"+ wallet_address + "</p>\n"
html_code += """
<table>
<tr><td><b>Logo</b></td><td><b>Name</b></td><td><b>Link</b></td></tr>
"""
 
for row in token_df.collect():  
    logo = row["image"]
    name = row["name"]
    link = row["links"]
    html_row = "<tr><td><img src='" + logo + "' alt='token logo'></td><td>" + name + "</td><td>" + "<a href='" + link + "'><img src='https://www.clipartkey.com/mpngs/m/84-840903_transparent-grass-tuft-clipart-weaknesses-icon.png' alt='link image' width='35px' height='35px'></a></td></tr>\n"
    html_code+=html_row
 
displayHTML(html_code)


# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
