import asyncio
import os
import time
import requests
from google.cloud.pubsub import PublisherClient
from google.oauth2.id_token import fetch_id_token
from google.auth.transport.requests import Request
from google.cloud.bigquery import Client as BqClient, QueryJobConfig


# GCPのプロジェクトID
project_id = os.getenv("GCP_PROJECT_ID")
# BigQueryのデータセット名
dataset = os.getenv("BIGQUERY_DATASET")
# 最新unixtime管理テーブル名
recently_unixtime_table = os.getenv("BIGQUERY_UNIXTIME_TABLE")
# crypto-collectorのエンドポイント
crypto_collector_endpoint = os.getenv("CRYPTO_COLLECTOR_ENDPOINT")
# stock-collectorのエンドポイント
stock_collector_endpoint = os.getenv("STOCK_COLLECTOR_ENDPOINT")
# fx-collectorのエンドポイント
fx_collector_endpoint = os.getenv("FX_COLLECTOR_ENDPOINT")
# commodity-collectorのエンドポイント
commodity_collector_endpoint = os.getenv("COMMODITY_COLLECTOR_ENDPOINT")


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

    loop = asyncio.get_event_loop()

    # 以下のデータを収集するAPIを実行する
    # ・暗号資産データ
    # ・株指標データ
    # ・為替通貨データ
    # ・コモディデータ
    tasks = asyncio.gather(
        request_crypto_collector(),
        request_stock_collector(),
        request_fx_collector(),
        request_commodity_collector()
    )
    results = loop.run_until_complete(tasks)
    print(results)

    # unixtime管理テーブルの重複データを削除する
    duplicate_unixtime()


def request_crypto_collector():
    """
    暗号資産データ収集APIを実行する
    """
    response = request_google_functions(crypto_collector_endpoint)

    if response.status_code != 200:
        error_msg = f"crypto-collector Error \
            [HTTP STATUS: {response.status_code}], [RESULT: {response.reason}]"
        raise Exception(error_msg)


def request_stock_collector():
    """
    株データ収集APIを実行する
    """
    response = request_google_functions(stock_collector_endpoint)

    if response.status_code != 200:
        error_msg = f"stock-collector Error \
            [HTTP STATUS: {response.status_code}], [RESULT: {response.reason}]"
        raise Exception(error_msg)


def request_fx_collector():
    """
    為替通貨データ収集APIを実行する
    """
    response = request_google_functions(fx_collector_endpoint)

    if response.status_code != 200:
        error_msg = f"fx-collector Error \
            [HTTP STATUS: {response.status_code}], [RESULT: {response.reason}]"
        raise Exception(error_msg)


def request_commodity_collector():
    """
    コモディティデータ収集APIを実行する
    """
    response = request_google_functions(commodity_collector_endpoint)

    if response.status_code != 200:
        error_msg = f"commodity-collector Error \
            [HTTP STATUS: {response.status_code}], [RESULT: {response.reason}]"
        raise Exception(error_msg)


async def request_google_functions(url):
    """
    Google Cloud Functionsの関数をHTTPリクエストする
    """
    loop = asyncio.get_event_loop()
    auth_req = Request()
    id_token = fetch_id_token(auth_req, url)
    headers = {
        "Authorization": f"Bearer {id_token}"
    }
    return await loop.run_in_executor(
        None, requests.post, url, headers=headers)


def duplicate_unixtime():
    """
    unixtime管理テーブルでTABLE_NAMEカラムが重複してるデータを削除する
    """

    client = BqClient(project_id)
    table_id = f"{project_id}.{dataset}.{recently_unixtime_table}"

    duplicate_query = f"""
        SELECT
            * EXCEPT(rowNumber)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY TABLE_NAME ORDER BY UNIX_TIME DESC
                ) as rowNumber
            FROM
                {table_id}
        )
        WHERE
            rowNumber = 1;
    """

    job_config = QueryJobConfig()
    job_config.destination = table_id
    job_config.write_disposition = "WRITE_TRUNCATE"
    job = client.query(duplicate_query, job_config=job_config)
    job.result()


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
