from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipe_financial_crimes.config.ConfigStore import *
from pipe_financial_crimes.udfs.UDFs import *
from prophecy.utils import *
from pipe_financial_crimes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_table_wire_transfer_1 = table_wire_transfer_1(spark)
    df_concat_full_name = concat_full_name(spark, df_table_wire_transfer_1)
    df_ds_person_watchlist = ds_person_watchlist(spark)
    df_ds_src_country_watchlist = ds_src_country_watchlist(spark)
    df_ds_tar_country_watchlist_1 = ds_tar_country_watchlist_1(spark)
    df_join_multiple_dataframes = join_multiple_dataframes(
        spark, 
        df_concat_full_name, 
        df_ds_person_watchlist, 
        df_ds_src_country_watchlist, 
        df_ds_tar_country_watchlist_1
    )
    df_risk_scoring_out, df_risk_scoring_out0 = risk_scoring(spark, Config.risk_scoring, df_join_multiple_dataframes)
    df_flagged_transx = flagged_transx(spark, df_risk_scoring_out)
    table_all_transx(spark, df_risk_scoring_out0)
    t_flagged_transx(spark, df_flagged_transx)
    csv_all_transx(spark, df_risk_scoring_out0)

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
