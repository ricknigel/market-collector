# 複数のgoogle cloud functionsで使用する関数を定義する
import os
import time
from google.cloud.pubsub import PublisherClient

project_id = os.environ['GCP_PROJECT_ID']
dataset = os.environ['BIGQUERY_DATASET']


# エラー通知用topicへpublishする
def publish_error_report(error: str):
    publisher = PublisherClient()
    function_name = os.environ['FUNCTION_TARGET']
    error_report_topic = os.environ['ERROR_REPORT_TOPIC']
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
