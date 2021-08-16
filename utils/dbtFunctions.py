import requests, json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator

DBT_API = BaseHook.get_connection('dbt_api').host
ACCOUNT_ID = Variable.get('dbt_account_id')
SNS_CONN = BaseHook.get_connection('aws_conn').conn_id
SNS_ARN = Variable.get('sns_arn')

dbt_header = {
  'Content-Type': 'application/json',
  'Authorization': 'Token {0}'.format(Variable.get('SECRET_DBT_TOKEN'))
}

def getDbtMessage(message):
  return {'cause': message}

def _triggerJob(job_id, account_id, message, **context):
    response = requests.post(
        f'{DBT_API}/accounts/{account_id}/jobs/{job_id}/run/', 
        headers=dbt_header, 
        data=json.dumps(getDbtMessage(message))
    ).json()
    status_code = response['status']['code']
    job_run_id = response['data']['id']
    jobRunData = json.dumps({"status": status_code, "job_run_id": job_run_id}) 
    context['ti'].xcom_push(key=f'job-{job_id}', value=jobRunData)

def triggerJobOperator(task_id, job_id, account_id=ACCOUNT_ID, message='Triggered by Airflow', **context):
    return PythonOperator(
        task_id=task_id,
        python_callable=_triggerJob,
        op_kwargs={'job_id': job_id, 'account_id': account_id, 'message': message}
    )

def _waitJobRun(job_id, account_id, **context):
    jobRunData = json.loads(context['ti'].xcom_pull(key=f'job-{job_id}'))
    jobRunId = jobRunData['job_run_id']
    response = requests.get(f'{DBT_API}/accounts/{account_id}/runs/{jobRunId}/', headers=dbt_header).json()
    status = response['data']['status']
    if (status == 10):
        return True
    elif (status == 20):
        raise AirflowFailException('Error on DBT job run!')
    elif (status == 30):
        raise AirflowFailException('DBT job run cancelled!')
    # 1-Queued / 3-Running / 10-Success / 20-Error / 30-Cancelled

def waitJobRunOperator(task_id, job_id, interval=30, retries=20, account_id=ACCOUNT_ID):
    return PythonSensor(
        task_id=task_id,
        poke_interval=interval,
        timeout=interval*retries,
        python_callable=_waitJobRun,
        op_kwargs={'job_id': job_id, 'account_id': account_id}
    )

def notifyErrorIfOneFailedOperator(task_id, message='Error on DAG!'):
    return SnsPublishOperator(
        task_id=task_id,
        target_arn=SNS_ARN,
        message=message,
        aws_conn_id=SNS_CONN,
        trigger_rule='one_failed'
    )