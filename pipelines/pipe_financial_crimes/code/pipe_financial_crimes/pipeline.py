from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *
from prophecy.utils import *
from pipe_financial_crimes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_person_watchlist = ds_person_watchlist(spark)
    df_ds_wire_transfers = ds_wire_transfers(spark)
    df_reformatted_transactions = reformatted_transactions(spark, df_ds_wire_transfers)
    df_transaction_full_name_reason = transaction_full_name_reason(
        spark, 
        df_reformatted_transactions, 
        df_ds_person_watchlist
    )
    df_ds_country_watchlist_src = ds_country_watchlist_src(spark)
    df_transaction_details_with_country_info = transaction_details_with_country_info(
        spark, 
        df_transaction_full_name_reason, 
        df_ds_country_watchlist_src
    )
    df_ds_country_watchlist_tar = ds_country_watchlist_tar(spark)
    df_international_transactions_join = international_transactions_join(
        spark, 
        df_transaction_details_with_country_info, 
        df_ds_country_watchlist_tar
    )
    df_risk_flags = risk_flags(spark, df_international_transactions_join)
    df_risk_score = risk_score(spark, df_risk_flags)
    df_sort_risk_score = sort_risk_score(spark, df_risk_score)

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
