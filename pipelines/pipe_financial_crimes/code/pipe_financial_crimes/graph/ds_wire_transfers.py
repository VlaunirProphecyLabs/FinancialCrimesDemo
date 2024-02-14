from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *

def ds_wire_transfers(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("transaction_id", StringType(), True), StructField("first_name", StringType(), True), StructField("last_name", StringType(), True), StructField("from_bank", StringType(), True), StructField("from_account_number", StringType(), True), StructField("to_bank", StringType(), True), StructField("to_account_number", StringType(), True), StructField("originating_country", StringType(), True), StructField("destination_country", StringType(), True), StructField("transaction_amount", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/bobwelshmer/financial_crimes/wire_transfer.csv")
