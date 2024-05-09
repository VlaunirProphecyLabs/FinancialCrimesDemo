from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo.config.ConfigStore import *
from demo.udfs.UDFs import *

def transaction_details(
        spark: SparkSession,
        all_scored_transx: DataFrame,
        ds_country_watchlist: DataFrame,
        ds_person_watchlist: DataFrame
) -> DataFrame:
    return all_scored_transx\
        .alias("all_scored_transx")\
        .join(
          ds_country_watchlist.alias("ds_country_watchlist"),
          (col("all_scored_transx.originating_country") == col("ds_country_watchlist.country")),
          "inner"
        )\
        .join(
          ds_person_watchlist.alias("ds_person_watchlist"),
          (col("all_scored_transx.full_name") == col("ds_person_watchlist.full_name")),
          "inner"
        )\
        .select(col("all_scored_transx.transaction_id").alias("TRANSACTION_ID"), col("all_scored_transx.full_name").alias("FULL_NAME"), col("all_scored_transx.from_bank").alias("FROM_BANK"), col("all_scored_transx.from_account_number").alias("FROM_ACCOUNT_NUMBER"), col("all_scored_transx.to_bank").alias("TO_BANK"), col("all_scored_transx.to_account_number").alias("TO_ACCOUNT_NUMBER"), col("all_scored_transx.originating_country").alias("ORIGINATING_COUNTRY"), col("all_scored_transx.destination_country").alias("DESTINATION_COUNTRY"), col("all_scored_transx.transaction_amount").alias("TRANSACTION_AMOUNT"), col("all_scored_transx.person_reason").alias("PERSON_REASON"), col("all_scored_transx.source_country_issue").alias("SOURCE_COUNTRY_ISSUE"), col("all_scored_transx.target_country_issue").alias("TARGET_COUNTRY_ISSUE"), col("all_scored_transx.person_flag").alias("PERSON_FLAG"), col("all_scored_transx.src_country_flag").alias("SRC_COUNTRY_FLAG"), col("all_scored_transx.tar_country_flag").alias("TAR_COUNTRY_FLAG"), col("all_scored_transx.transaction_flag").alias("TRANSACTION_FLAG"), col("all_scored_transx.total_risk").alias("TOTAL_RISK"))
