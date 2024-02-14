from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, *inDFs: DataFrame) -> DataFrame:
    pass
