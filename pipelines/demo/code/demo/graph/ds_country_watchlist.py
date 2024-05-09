from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo.config.ConfigStore import *
from demo.udfs.UDFs import *

def ds_country_watchlist(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("country", StringType(), True), StructField("issue", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/bobwelshmer/financial_crimes/country_watchlist.csv")
