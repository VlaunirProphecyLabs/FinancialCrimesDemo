from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo.config.ConfigStore import *
from demo.udfs.UDFs import *

def limit_10(spark: SparkSession, group_by_origin_dest: DataFrame) -> DataFrame:
    return group_by_origin_dest.limit(10)
