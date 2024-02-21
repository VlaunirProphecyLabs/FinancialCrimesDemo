from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from simple_pipeline.config.ConfigStore import *
from simple_pipeline.udfs.UDFs import *

def filter_by_risk_score(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("risk_score") > lit(1)))
