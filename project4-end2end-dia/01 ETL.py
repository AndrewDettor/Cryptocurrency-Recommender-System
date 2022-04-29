# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 1905)

# COMMAND ----------

# MAGIC %sql
# MAGIC use ethereumetl;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table IF NOT EXISTS g09_db.erc20_token_transfers as
# MAGIC (select * from ethereumetl.silver_contracts join ethereumetl.token_transfers on ethereumetl.silver_contracts.address = ethereumetl.token_transfers.token_address where ethereumetl.silver_contracts.is_erc20 = True);

# COMMAND ----------

blocks = spark.sql("select * from blocks")
contracts = spark.sql("select * from contracts")
logs = spark.sql("select * from logs")
receipts = spark.sql("select * from receipts")
token_transfers = spark.sql("select * from token_transfers")
tokens = spark.sql("select * from tokens")
token_prices_usd = spark.sql("select * from token_prices_usd")
transactions = spark.sql("select * from transactions")
silver_contracts = spark.sql("select * from silver_contracts")
erc20_token_transfers = spark.sql("select * from g09_db.erc20_token_transfers")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tokens Table

# COMMAND ----------

Tokens_Table = (erc20_token_transfers.select("token_address")
                                    .distinct()
               )

Tokens_Table = (Tokens_Table.join(token_prices_usd, Tokens_Table.token_address == token_prices_usd.contract_address, "inner")
                            .select("token_address", "name", "links", "image", "price_usd")
                                    
               )
display(Tokens_Table)

# COMMAND ----------

Tokens_Table = Tokens_Table.coalesce(1).withColumn("id", monotonically_increasing_id())
display(Tokens_Table)

# COMMAND ----------

Tokens_Table.write.mode("overwrite").saveAsTable("g09_db.silver_token_table");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Users Table
# MAGIC - int index
# MAGIC - from token_transfers
# MAGIC   - set(all to_address) - set(all token_address)

# COMMAND ----------

Users_Table = (erc20_token_transfers.select(explode(array(col("from_address"), col("to_address"))).alias("users"))
               .distinct()
               )
display(Users_Table)

# COMMAND ----------

Users_Table = Users_Table.coalesce(1).withColumn("id", monotonically_increasing_id())
display(Users_Table)

# COMMAND ----------

Users_Table.write.mode("overwrite").saveAsTable("g09_db.silver_user_table");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wallet Table
# MAGIC - rows =  users
# MAGIC - cols = tokens
# MAGIC - values = how much of that token (in USD) the user has in their wallet at Start_Date
# MAGIC   - get all the token transfers a user has done
# MAGIC   - convert token_amount into USD
# MAGIC   - add the amount to the wallet if the user is in the to_address
# MAGIC   - remove the amount from the wallet if the user is in the from_address

# COMMAND ----------

timestampDF = blocks.withColumn("timestamp", to_date(col("timestamp").cast("timestamp")))
display(timestampDF)

# COMMAND ----------

# MAGIC %md
# MAGIC filter out all the erc20 token transfers before the start date

# COMMAND ----------

erc20_date_filtered_df = erc20_token_transfers.join(timestampDF,timestampDF.number == erc20_token_transfers.block_number,"inner").filter(col("timestamp") < start_date).select(col("from_address"), col("to_address"), col("token_address"), col("value"),col("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC for each token, find the net value (to=+tokens, from=-tokens)

# COMMAND ----------

erc20_from_df = erc20_date_filtered_df.groupby(col("from_address"),col("token_address")).agg(sum((-1)*col("value")).alias("from_value"))
display(erc20_from_df)

# COMMAND ----------

erc20_to_df = erc20_date_filtered_df.groupby(col("to_address"),col("token_address")).agg(sum(col("value")).alias("to_value"))
display(erc20_to_df)

# COMMAND ----------

temp = erc20_to_df.union(erc20_from_df)
display(temp)

# COMMAND ----------

wallet_token_value = temp.groupby(col("to_address"),col("token_address")).agg(sum(col("to_value")).alias("balance_value"))

# COMMAND ----------

wallet_token_value.write.mode("overwrite").saveAsTable("g09_db.silver_wallet_table_value");

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
