from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

import datetime, json

default_args = {
  'start_date': datetime.datetime(2021,1,1)
}

dbt_header = {
  'Content-Type': 'application/json',
  'Authorization': 'Token f54d8c20e3440d8fbeaa13ba7c185935fa3bf4b6'
}

def getDbtMessage(message):
  return {'cause': message}

def getDbtApiLink(jobId, accountId):
  return 'accounts/{0}/jobs/{1}/run/'.format(accountId, jobId)

def getDbtApiOperator(task_id, jobId, message='Triggered by Airflow', accountId=25183):
  return SimpleHttpOperator(
    task_id=task_id,
    method='POST',
    data=json.dumps(getDbtMessage(message)),
    http_conn_id='dbt_api',
    endpoint=getDbtApiLink(jobId, accountId),
    headers=dbt_header
  )

with DAG('Our_medium_project',
  schedule_interval="@daily",
  default_args=default_args,
  catchup=False) as dag:
  load_user_cloud = getDbtApiOperator('load_users', 32159)