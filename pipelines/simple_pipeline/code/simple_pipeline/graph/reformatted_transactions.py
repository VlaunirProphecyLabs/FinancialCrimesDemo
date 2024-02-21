from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from simple_pipeline.config.ConfigStore import *
from simple_pipeline.udfs.UDFs import *

def reformatted_transactions(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("transaction_id"), 
        concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"), 
        col("from_bank"), 
        col("from_account_number"), 
        col("to_bank"), 
        col("to_account_number"), 
        col("originating_country"), 
        col("destination_country"), 
        col("transaction_amount")
    )
