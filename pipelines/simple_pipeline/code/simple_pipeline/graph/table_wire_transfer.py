from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from simple_pipeline.config.ConfigStore import *
from simple_pipeline.udfs.UDFs import *

def table_wire_transfer(spark: SparkSession) -> DataFrame:
    return spark.read.table("`bobwelshmer`.`financial_crimes`.`wire_transfer`")
