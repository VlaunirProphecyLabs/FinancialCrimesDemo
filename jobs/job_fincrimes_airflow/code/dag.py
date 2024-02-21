import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from z8pmqqavaeqprviq_ek4cq_.tasks import pipe_financial_crimes
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "Z8PMqqaVaeQPRVIq_EK4cQ_", 
    schedule_interval = None, 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "P1Mfmhdo"}, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2024, 3, 12, tz = "UTC"), 
    catchup = True
) as dag:
    pipe_financial_crimes_op = pipe_financial_crimes()
