from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo.config.ConfigStore import *
from demo.udfs.UDFs import *

def all_scored_transx(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("transaction_id", LongType(), True), StructField("full_name", StringType(), True), StructField("from_bank", StringType(), True), StructField("from_account_number", StringType(), True), StructField("to_bank", StringType(), True), StructField("to_account_number", StringType(), True), StructField("originating_country", StringType(), True), StructField("destination_country", StringType(), True), StructField("transaction_amount", DoubleType(), True), StructField("person_reason", StringType(), True), StructField("source_country_issue", StringType(), True), StructField("target_country_issue", StringType(), True), StructField("person_flag", IntegerType(), False), StructField("src_country_flag", IntegerType(), False), StructField("tar_country_flag", IntegerType(), False), StructField("transaction_flag", IntegerType(), False), StructField("total_risk", IntegerType(), False)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Users/vlaunir@prophecy.io/financial_crimes/outputs/flagged_trans.csv/all_scored_transx.csv")
