from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo.config.ConfigStore import *
from demo.udfs.UDFs import *

def transaction_details_1(spark: SparkSession, transaction_details: DataFrame) -> DataFrame:
    return transaction_details.select(col("ORIGINATING_COUNTRY"), col("DESTINATION_COUNTRY"))
