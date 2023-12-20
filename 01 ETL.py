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

# MAGIC %md
# MAGIC # Pipeline: Bronze to Silver
# MAGIC ## 1. Bronze Data
# MAGIC    - Source: ethereumetl (in SQL Metastore)
# MAGIC        - save as DataFrames using spark.sql
# MAGIC    - Tables:
# MAGIC        - blocks (DataFrame)
# MAGIC             - number:long
# MAGIC             - hash:string
# MAGIC             - parent_hash:string
# MAGIC             - nonce:string
# MAGIC             - sha3_uncles:string
# MAGIC             - logs_bloom:string
# MAGIC             - transactions_root:string
# MAGIC             - state_root:string
# MAGIC             - receipts_root:string
# MAGIC             - miner:string
# MAGIC             - difficulty:decimal(38,0)
# MAGIC             - total_difficulty:decimal(38,0)
# MAGIC             - size:long
# MAGIC             - extra_data:string
# MAGIC             - gas_limit:long
# MAGIC             - gas_used:long
# MAGIC             - timestamp:long
# MAGIC             - transaction_count:long
# MAGIC             - start_block:long
# MAGIC             - end_block:long
# MAGIC        - token_transfers (DataFrame)
# MAGIC             - token_address:string
# MAGIC             - from_address:string
# MAGIC             - to_address:string
# MAGIC             - value:decimal(38,0)
# MAGIC             - transaction_hash:string
# MAGIC             - log_index:long
# MAGIC             - block_number:long
# MAGIC             - start_block:long
# MAGIC             - end_block:long
# MAGIC        - token_prices_usd (DataFrame)
# MAGIC             - id:string
# MAGIC             - symbol:string
# MAGIC             - name:string
# MAGIC             - asset_platform_id:string
# MAGIC             - description:string
# MAGIC             - links:string
# MAGIC             - image:string
# MAGIC             - contract_address:string
# MAGIC             - sentiment_votes_up_percentage:double
# MAGIC             - sentiment_votes_down_percentage:double
# MAGIC             - market_cap_rank:double
# MAGIC             - coingecko_rank:double
# MAGIC             - coingecko_score:double
# MAGIC             - developer_score:double
# MAGIC             - community_score:double
# MAGIC             - liquidity_score:double
# MAGIC             - public_interest_score:double
# MAGIC             - price_usd:double
# MAGIC        - silver_contracts (DataFrame)
# MAGIC             - address:string
# MAGIC             - bytecode:string
# MAGIC             - is_erc20:string
# MAGIC             - is_erc721:string

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Create erc20_token_transfers (in SQL Metastore)
# MAGIC    - use SQL to join silver_contracts with token_transfers on address where is_erc20 = True
# MAGIC    - Schema:
# MAGIC        - token_address:string
# MAGIC        - from_address:string
# MAGIC        - to_address:string
# MAGIC        - value:decimal(38,0)
# MAGIC        - transaction_hash:string
# MAGIC        - log_index:long
# MAGIC        - block_number:long
# MAGIC        - start_block:long
# MAGIC        - end_block:long
# MAGIC        - address:string
# MAGIC        - bytecode:string
# MAGIC        - is_erc20:string
# MAGIC        - is_erc721:string

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create erc20_date_filtered_df (DataFrame)
# MAGIC    - join erc20_token_transfers with blocks to filter transfers from before Start_Date
# MAGIC    - Schema:
# MAGIC        - from_address:string
# MAGIC        - to_address:string
# MAGIC        - token_address:string
# MAGIC        - value:decimal(38,0)
# MAGIC        - timestamp:date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Tokens_Table (DataFrame)
# MAGIC    - get distinct token addresses from erc20_date_filtered_df
# MAGIC    - join with token_prices_usd to keep track of all the information we need for each token
# MAGIC    - create a unique integer id for each token
# MAGIC    - save to SQL Metastore as silver_token_table
# MAGIC    - Schema:
# MAGIC        - token_address:string
# MAGIC        - name:string
# MAGIC        - links:string
# MAGIC        - image:string
# MAGIC        - price_usd:double
# MAGIC        - id:long

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Users_Table (DataFrame)
# MAGIC    - combine from_address and to_address from erc20_date_filtered_df into one column and remove duplicate addresses
# MAGIC    - create a unique integer id for each user
# MAGIC    - save to SQL Metastore as silver_user_table
# MAGIC    - Schema:
# MAGIC        - users:string
# MAGIC        - id:long

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create silver_token_balance (DataFrame)
# MAGIC    - for each user for each token, find how much they've sent and how much they've received
# MAGIC    - change value into USD by joining with Tokens_Table
# MAGIC    - change token_address into its integer id by joining with Tokens_Table
# MAGIC    - change user_address into its integer id by joining with Users_Table
# MAGIC    - save to SQL Metastore as silver_token_balance
# MAGIC    - Schema:
# MAGIC        - user_id:long
# MAGIC        - token_id:long
# MAGIC        - balance:double

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Widget Values

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC # Partitioning

# COMMAND ----------

# MAGIC %python
# MAGIC # spark.conf.set("spark.sql.shuffle.partitions", 1905)

# COMMAND ----------

# MAGIC %md
# MAGIC # Show Available Data Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from ethereumetl

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables from g09_db

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest Bronze Data

# COMMAND ----------

# MAGIC %sql
# MAGIC use ethereumetl;

# COMMAND ----------

blocks = spark.sql("select * from blocks")
token_transfers = spark.sql("select * from token_transfers")
token_prices_usd = spark.sql("select * from token_prices_usd")
silver_contracts = spark.sql("select * from silver_contracts")

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Validation

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

# COMMAND ----------

blocks_schema = "number long, hash string, parent_hash string, nonce string, sha3_uncles string, logs_bloom string, transactions_root string, state_root string, receipts_root string, miner string, difficulty decimal(38,0), total_difficulty decimal(38,0), size long, extra_data string, gas_limit long, gas_used long, timestamp long, transaction_count long, start_block long, end_block long"
 
assert blocks.schema == _parse_datatype_string(blocks_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

token_transfers_schema = "token_address string, from_address string, to_address string, value decimal(38,0), transaction_hash string, log_index long, block_number long, start_block long, end_block long"

assert token_transfers.schema == _parse_datatype_string(token_transfers_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

token_prices_usd_schema = "id string, symbol string, name string, asset_platform_id string, description string, links string, image string, contract_address string, sentiment_votes_up_percentage double, sentiment_votes_down_percentage double, market_cap_rank double, coingecko_rank double, coingecko_score double, developer_score double, community_score double, liquidity_score double, public_interest_score double, price_usd double"

assert token_prices_usd.schema == _parse_datatype_string(token_prices_usd_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

silver_contracts_schema = "address string, bytecode string, is_erc20 string, is_erc721 string"

assert silver_contracts.schema == _parse_datatype_string(silver_contracts_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaned Token Transfers Table

# COMMAND ----------

# MAGIC %md
# MAGIC Filter token transfers based on silver contracts to make sure we only consider ERC20 token transfers from the ethereum blockchain.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table IF NOT EXISTS g09_db.erc20_token_transfers as
# MAGIC (select * from ethereumetl.silver_contracts join ethereumetl.token_transfers on ethereumetl.silver_contracts.address = ethereumetl.token_transfers.token_address where ethereumetl.silver_contracts.is_erc20 = True);

# COMMAND ----------

erc20_token_transfers = spark.sql("select * from g09_db.erc20_token_transfers")

# COMMAND ----------

# MAGIC %md 
# MAGIC Change blocks' timestamp column to non-epoch time so it can be compatible with the Start_Date widget.

# COMMAND ----------

timestampDF = blocks.withColumn("timestamp", to_date(col("timestamp").cast("timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC Filter out all ERC20 token transfers from before the start date.

# COMMAND ----------

erc20_date_filtered_df = (erc20_token_transfers.join(timestampDF, timestampDF.number == erc20_token_transfers.block_number, "inner")
                                             .filter(col("timestamp") < start_date)
                                             .select(col("from_address"), col("to_address"), col("token_address"), col("value"), col("timestamp"))
                        )

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Validation

# COMMAND ----------

erc20_date_filtered_df_schema = "from_address string, to_address string, token_address string, value decimal(38,0), timestamp date"

assert erc20_date_filtered_df.schema == _parse_datatype_string(erc20_date_filtered_df_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Tokens Table

# COMMAND ----------

# MAGIC %md
# MAGIC Get all the unique tokens from the cleaned token transfers table.

# COMMAND ----------

Tokens_Table = (erc20_date_filtered_df.select("token_address")
                                    .distinct()
               )

# COMMAND ----------

# MAGIC %md
# MAGIC Join with token_prices_usd to keep track of more information about the token for each transfer.

# COMMAND ----------

Tokens_Table = (Tokens_Table.join(token_prices_usd, Tokens_Table.token_address == token_prices_usd.contract_address, "inner")
                            .select("token_address", "name", "links", "image", "price_usd").distinct()
                                    
               )

# COMMAND ----------

# MAGIC %md
# MAGIC Create an ID for each token (to be used in ALS modelling later).

# COMMAND ----------

Tokens_Table = Tokens_Table.coalesce(1).withColumn("id", monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Validation

# COMMAND ----------

Tokens_Table_schema = "token_address string, name string, links string, image string, price_usd double, id long not null"

assert Tokens_Table.schema == _parse_datatype_string(Tokens_Table_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Save to our group's database in the metastore.

# COMMAND ----------

Tokens_Table.repartition("token_address").write.mode("overwrite").saveAsTable("g09_db.silver_token_table")
display(spark.sql("OPTIMIZE g09_db.silver_token_table ZORDER BY (price_usd)"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Users Table

# COMMAND ----------

# MAGIC %md
# MAGIC Get all unique users from all transactions by combining the from_address and to_address columns and removing duplicates.

# COMMAND ----------

Users_Table = (erc20_date_filtered_df.select(explode(array(col("from_address"), col("to_address"))).alias("users"))
                                      .distinct()
               )

# COMMAND ----------

# MAGIC %md
# MAGIC Create an ID for each user (to be used in ALS modelling later).

# COMMAND ----------

Users_Table = (Users_Table.coalesce(1)
                          .withColumn("id", monotonically_increasing_id())
              )

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Validation

# COMMAND ----------

Users_Table_schema = "users string, id long not null"

assert Users_Table.schema == _parse_datatype_string(Users_Table_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md 
# MAGIC Save to our group's database in the metastore.

# COMMAND ----------

Users_Table.repartition("users").write.mode("overwrite").saveAsTable("g09_db.silver_user_table")
display(spark.sql("OPTIMIZE g09_db.silver_user_table ZORDER BY (id)"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Wallet Table

# COMMAND ----------

# MAGIC %md
# MAGIC For each token for each user, find the net balance in their wallet.

# COMMAND ----------

erc20_from_df = (erc20_date_filtered_df.groupby(col("from_address"),col("token_address"))
                                       .agg(sum((-1)*col("value")).alias("from_value"))
                )

# COMMAND ----------

erc20_to_df = (erc20_date_filtered_df.groupby(col("to_address"),col("token_address"))
                                     .agg(sum(col("value")).alias("to_value"))
              )

# COMMAND ----------

# MAGIC %md
# MAGIC Net balance = tokens user has gotten - tokens user has given away

# COMMAND ----------

temp = erc20_to_df.union(erc20_from_df)

# COMMAND ----------

wallet_token_value = (temp.groupby(col("to_address"),col("token_address"))
                          .agg(sum(col("to_value")).alias("balance_value"))
                          .select(col("to_address"), col("token_address").alias("token_address_wallet_df"), col("balance_value")) # renaming token address because AnalysisException: Column token_address#140 are ambiguous
                     )

# COMMAND ----------

# MAGIC %md
# MAGIC Change the value for each token to USD, so they are all comparable.

# COMMAND ----------

wallet_token_price_usd = (wallet_token_value.join(Tokens_Table, wallet_token_value.token_address_wallet_df == Tokens_Table.token_address, "inner")
                                             .withColumn("balance_usd", col("price_usd")*col("balance_value"))
                                             .select(col("to_address").alias("User_address"), col("token_address"), col("balance_usd").alias("balance"))
                         )

# COMMAND ----------

# MAGIC %md
# MAGIC Convert token and wallet address to corresponding id's in Tokens_Table and Users_Table

# COMMAND ----------

silver_wallet_token_price_usd = (wallet_token_price_usd.join(Tokens_Table, wallet_token_price_usd.token_address ==  Tokens_Table.token_address, "left")
                                                       .select(col("User_address"), col("id").alias("token_id"), col("balance"))
                                )

# COMMAND ----------

silver_token_balance = (silver_wallet_token_price_usd.join(Users_Table, silver_wallet_token_price_usd.User_address ==  Users_Table.users, "left")
                                                     .select(col("id").alias("user_id"), col("token_id"), col("balance"))
                       )

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Validation

# COMMAND ----------

silver_token_balance_schema = "user_id long, token_id long, balance double"

assert silver_token_balance.schema == _parse_datatype_string(silver_token_balance_schema), "File not present in Silver Path"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Save to our group's database in the metastore.

# COMMAND ----------

silver_token_balance.repartition("token_id").write.mode("overwrite").saveAsTable("g09_db.silver_token_balance")
display(spark.sql("OPTIMIZE g09_db.silver_token_balance ZORDER BY (user_id)"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Exit Notebook

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
