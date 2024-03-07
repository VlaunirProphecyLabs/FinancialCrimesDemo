from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *

def ds_tar_country_watchlist_1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("country", StringType(), True), StructField("issue", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/bobwelshmer/financial_crimes/country_watchlist.csv")
