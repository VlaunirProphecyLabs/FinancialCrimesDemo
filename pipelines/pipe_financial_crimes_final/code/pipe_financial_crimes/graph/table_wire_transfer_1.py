from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *

def table_wire_transfer_1(spark: SparkSession) -> DataFrame:
    return spark.read.table("`vlaunir_demos`.`financial_crimes`.`wire_transfer`")
