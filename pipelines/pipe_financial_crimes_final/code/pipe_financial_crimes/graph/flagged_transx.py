from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *

def flagged_transx(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("total_risk") > lit(1)))
