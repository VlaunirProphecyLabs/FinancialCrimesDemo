from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from pipe_financial_crimes.udfs.UDFs import *

def sort_risk_score(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("total_risk").desc())
