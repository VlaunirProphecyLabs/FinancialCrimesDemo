from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from simple_pipeline.config.ConfigStore import *
from simple_pipeline.udfs.UDFs import *

def reformatted_transactions_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("transaction_id"), 
        col("full_name"), 
        col("from_bank"), 
        col("from_account_number"), 
        col("to_bank"), 
        col("to_account_number"), 
        col("originating_country"), 
        col("destination_country"), 
        col("transaction_amount"), 
        col("person_reason"), 
        col("country_issue"), 
        when(~ col("person_reason").isNull(), lit(1)).otherwise(lit(0)).alias("person_reason_flag"), 
        when(~ col("country_issue").isNull(), lit(1)).otherwise(lit(0)).alias("country_reason_flag"), 
        when(((col("transaction_amount") >= lit(5000)) & (col("transaction_amount") <= lit(10000))), lit(1))\
          .otherwise(lit(0))\
          .alias("transaction_amount_flag")
    )
