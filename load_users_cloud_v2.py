import datetime

from airflow import DAG
from utils.dbtFunctions import *

default_args = {
  'start_date': datetime.datetime(2021,1,1)
}

with DAG('Our_medium_project_v2',
  schedule_interval="@daily",
  default_args=default_args,
  catchup=False) as dag:
  
  task_1 = triggerJobOperator('task_1', 32159)
  task_2 = waitJobRunOperator('task_2', 32159)
  task_3 = notifyErrorIfOneFailedOperator('task_3', 'Error running DBT job')
  task_1 >> task_2 >> task_3