import os
import time
import requests
from google.cloud.pubsub import PublisherClient
from google.oauth2.id_token import fetch_id_token
from google.auth.transport.requests import Request


# GCPのプロジェクトID
project_id = os.getenv("GCP_PROJECT_ID")
# crypto-collectorのエンドポイント
crypto_collector_endpoint = os.getenv("CRYPTO_COLLECTOR_ENDPOINT")


def handler(event, context):
    """
    エンドポイント
    """
    try:
        market_collector()
    except Exception as e:
        publish_error_report(str(e))
        raise e


def market_collector():
    """
    金融データを収集する
    """

    # 暗号資産データを収集するAPIを実行する
    request_crypto_collector()


def request_crypto_collector():
    """
    暗号資産データ収集APIを実行する
    """
    response = request_google_functions(crypto_collector_endpoint)

    if response.status_code != 200:
        error_msg = f"crypto-collector Error \
            [HTTP STATUS: {response.status_code}], [RESULT: {response.text}]"
        raise Exception(error_msg)


def request_google_functions(url):
    """
    Google Cloud Functionsの関数をHTTPリクエストする
    """
    auth_req = Request()
    id_token = fetch_id_token(auth_req, url)
    headers = {
        "Authorization": f"Bearer {id_token}"
    }
    return requests.post(url, headers=headers)


def publish_error_report(error: str):
    """
    エラー通知用topicへpublishする
    """
    publisher = PublisherClient()
    error_report_topic = os.getenv("ERROR_REPORT_TOPIC")
    topic_name = f"projects/{project_id}/topics/{error_report_topic}"

    publisher.publish(
        topic_name,
        data=error.encode("utf-8"),
        projectId=project_id,
        functionName="market-collector",
        eventTime=str(int(time.time()))
    )
