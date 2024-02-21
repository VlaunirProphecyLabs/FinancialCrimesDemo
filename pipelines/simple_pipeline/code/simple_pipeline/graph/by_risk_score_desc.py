from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from simple_pipeline.config.ConfigStore import *
from simple_pipeline.udfs.UDFs import *

def by_risk_score_desc(spark: SparkSession, reformatted_transaction_data: DataFrame) -> DataFrame:
    return reformatted_transaction_data.orderBy(col("risk_score").desc())
