from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from demo.config.ConfigStore import *
from demo.udfs.UDFs import *
from prophecy.utils import *
from demo.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_country_watchlist = ds_country_watchlist(spark)
    df_all_scored_transx = all_scored_transx(spark)
    df_ds_person_watchlist = ds_person_watchlist(spark)
    df_transaction_details = transaction_details(
        spark, 
        df_all_scored_transx, 
        df_ds_country_watchlist, 
        df_ds_person_watchlist
    )
    df_transaction_details_1 = transaction_details_1(spark, df_transaction_details)
    df_Reformat_1 = Reformat_1(spark, df_transaction_details)
    df_group_by_origin_dest = group_by_origin_dest(spark, df_transaction_details_1)
    df_limit_10 = limit_10(spark, df_group_by_origin_dest)
    df_risk_scoring_out, df_risk_scoring_out0 = risk_scoring(spark, Config.risk_scoring, df_transaction_details)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/demo")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/demo", config = Config)(pipeline)

if __name__ == "__main__":
    main()
