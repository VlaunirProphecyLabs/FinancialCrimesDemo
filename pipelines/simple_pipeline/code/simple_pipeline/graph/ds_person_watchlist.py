from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from simple_pipeline.config.ConfigStore import *
from simple_pipeline.udfs.UDFs import *

def ds_person_watchlist(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("full_name", StringType(), True), StructField("reason", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/bobwelshmer/financial_crimes/individual_watchlist.csv")
