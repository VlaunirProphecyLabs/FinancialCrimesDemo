from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from pipe_financial_crimes.udfs.UDFs import *

def risk_flags(spark: SparkSession, in0: DataFrame) -> DataFrame:
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
        col("source_country_issue"), 
        col("target_country_issue"), 
        when(col("person_reason").isNull(), lit(0)).otherwise(lit(1)).alias("person_flag"), 
        when(col("source_country_issue").isNull(), lit(0)).otherwise(lit(1)).alias("src_country_flag"), 
        when(col("target_country_issue").isNull(), lit(0)).otherwise(lit(1)).alias("tar_country_flag"), 
        when(((col("transaction_amount") >= lit(5000)) & (col("transaction_amount") < lit(10000))), lit(1))\
          .otherwise(lit(0))\
          .alias("transaction_flag")
    )
