from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from simple_pipeline.config.ConfigStore import *
from simple_pipeline.udfs.UDFs import *
from prophecy.utils import *
from simple_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_table_wire_transfer = table_wire_transfer(spark)
    df_reformatted_transactions = reformatted_transactions(spark, df_table_wire_transfer)
    df_ds_person_watchlist = ds_person_watchlist(spark)
    df_left_join_by_full_name = left_join_by_full_name(spark, df_reformatted_transactions, df_ds_person_watchlist)
    df_ds_country_watchlist = ds_country_watchlist(spark)
    df_left_outer_join_by_destination_country = left_outer_join_by_destination_country(
        spark, 
        df_left_join_by_full_name, 
        df_ds_country_watchlist
    )
    df_reformatted_transactions_1 = reformatted_transactions_1(spark, df_left_outer_join_by_destination_country)
    df_reformatted_transaction_data = reformatted_transaction_data(spark, df_reformatted_transactions_1)
    df_by_risk_score_desc = by_risk_score_desc(spark, df_reformatted_transaction_data)
    All(spark, df_by_risk_score_desc)
    df_filter_by_risk_score = filter_by_risk_score(spark, df_by_risk_score_desc)
    flagged(spark, df_filter_by_risk_score)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/simple_pipeline")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/simple_pipeline", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/simple_pipeline")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
