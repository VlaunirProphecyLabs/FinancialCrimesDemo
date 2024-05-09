from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *

def table_all_transx(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`vlaunir_demos`.`financial_crimes`.`all_transx_scored`")
