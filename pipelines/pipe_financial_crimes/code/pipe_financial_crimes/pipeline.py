from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *
from prophecy.utils import *
from pipe_financial_crimes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_wire_transfers = ds_wire_transfers(spark)
    df_Reformat_1 = Reformat_1(spark, df_ds_wire_transfers)
    df_Join_1 = Join_1(spark, df_Reformat_1)
    df_ds_person_watchlist = ds_person_watchlist(spark)
    df_ds_country_watchlist = ds_country_watchlist(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipe_financial_crimes")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pipe_financial_crimes", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pipe_financial_crimes")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
