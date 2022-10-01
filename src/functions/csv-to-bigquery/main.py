import os
import time
from google.cloud import bigquery
from google.cloud.pubsub import PublisherClient

project_id = os.getenv('GCP_PROJECT_ID')
dataset = os.getenv('BIGQUERY_DATASET')


def handler(data, context):
    try:
        csv_to_bigquery(data)
    except Exception as e:
        publish_error_report(str(e))
        print(e)


def csv_to_bigquery(data):
    bucket = data['bucket']
    file_path: str = data["name"]
    gcs_uri = f'gs://{bucket}/{file_path}'

    table_name = get_table_name(file_path)
    table_id = f'{dataset}.{table_name}'

    client = bigquery.Client(project_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = 'WRITE_APPEND'

    load_job = client.load_table_from_uri(
        gcs_uri,
        client.get_table(table_id),
        job_config=job_config
    )
    load_job.result()


def get_table_name(file_path: str):
    # file_path: <ticker>/<period>.csv
    # table_name: <ticker>_<period>

    split_slash = file_path.split('/')
    ticker = split_slash[0]
    period = split_slash[1].split('.')[0]

    return f'{ticker}_{period}'


# エラー通知用topicへpublishする
def publish_error_report(error: str):
    publisher = PublisherClient()
    function_name = os.getenv('FUNCTION_TARGET')
    error_report_topic = os.getenv('ERROR_REPORT_TOPIC')
    topic_name = f'projects/{project_id}/topics/{error_report_topic}'

    try:
        publisher.publish(
            topic_name,
            data=error.encode('utf-8'),
            projectId=project_id,
            functionName=function_name,
            eventTime=str(int(time.time()))
        )
    except Exception as e:
        # エラー時はリトライしない
        print(e)
