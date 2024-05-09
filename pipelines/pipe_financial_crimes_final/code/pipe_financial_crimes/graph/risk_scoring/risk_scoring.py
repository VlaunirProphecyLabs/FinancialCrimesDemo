from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from pipe_financial_crimes.udfs.UDFs import *
from . import *
from .config import *

def risk_scoring(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> (DataFrame, DataFrame):
    Config.update(subgraph_config)
    df_risk_flags = risk_flags(spark, in0)
    df_risk_score = risk_score(spark, df_risk_flags)
    df_sort_risk_score = sort_risk_score(spark, df_risk_score)
    subgraph_config.update(Config)

    return df_sort_risk_score, df_sort_risk_score
