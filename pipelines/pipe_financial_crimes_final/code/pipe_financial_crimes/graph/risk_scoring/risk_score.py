from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from pipe_financial_crimes.udfs.UDFs import *

def risk_score(spark: SparkSession, in0: DataFrame) -> DataFrame:
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
        col("person_flag"), 
        col("src_country_flag"), 
        col("tar_country_flag"), 
        col("transaction_flag"), 
        (((col("person_flag") + col("src_country_flag")) + col("tar_country_flag")) + col("transaction_flag")).alias(
          "total_risk"
        )
    )
