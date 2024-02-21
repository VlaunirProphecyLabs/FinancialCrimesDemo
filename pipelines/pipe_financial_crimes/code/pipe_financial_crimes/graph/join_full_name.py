from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *

def join_full_name(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.full_name") == col("in1.full_name")), "left_outer")\
        .select(col("in0.transaction_id").alias("transaction_id"), col("in0.full_name").alias("full_name"), col("in0.from_bank").alias("from_bank"), col("in0.from_account_number").alias("from_account_number"), col("in0.to_bank").alias("to_bank"), col("in0.to_account_number").alias("to_account_number"), col("in0.originating_country").alias("originating_country"), col("in0.destination_country").alias("destination_country"), col("in0.transaction_amount").alias("transaction_amount"), col("in1.reason").alias("person_reason"))
