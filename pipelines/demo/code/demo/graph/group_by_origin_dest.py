from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo.config.ConfigStore import *
from demo.udfs.UDFs import *

def group_by_origin_dest(spark: SparkSession, transaction_details_1: DataFrame) -> DataFrame:
    return transaction_details_1.distinct()
