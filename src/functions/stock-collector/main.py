import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from google.cloud.bigquery import Client as BqClient, QueryJobConfig
from google.cloud.storage import Client as StorageClient
from google.cloud.pubsub import PublisherClient


# GCPのプロジェクトID
project_id = os.getenv("GCP_PROJECT_ID")
# BigQueryのデータセット名
dataset = os.getenv("BIGQUERY_DATASET")
# 金融データ(csv)のアップロード先バケット名
bucket_name = os.getenv("MARKET_DATA_BUCKET")
# 最新unixtime管理テーブル名
recently_unixtime_table = os.getenv("BIGQUERY_UNIXTIME_TABLE")

stocks = [
    {"symbol": "ダウ工業株30種"},
    {"symbol": "NASDAQ100"},
    {"symbol": "S&P500"},
    {"symbol": "日経225"},
    {"symbol": "香港ハンセン指数"},
    {"symbol": "上海総合"},
    {"symbol": "インドSENSEX"},
    {"symbol": "ユーロストック50指数"},
    {"symbol": "FTSE100指数"},
]

periods = [
    {"name": "1D", "time": "1day"}
]

float_columns = [
    "OPEN_PRICE",
    "HIGH_PRICE",
    "LOW_PRICE",
    "CLOSE_PRICE",
    "VOLUME",
    "QUOTE_VOLUME"
]

columns = ["UNIX_TIME"] + float_columns


def handler(request):
    try:
        stock_collector()
    except Exception as e:
        publish_error_report(e)
        raise e

    return "ok"


def stock_collector():
    bigquery_client = BqClient(project_id)

    # 最新unixTimeを取得する
    df_unixtime = load_recently_unixtime(bigquery_client)

    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    execTime = now.strftime("%Y%m%d_%Hh")

    for stock in stocks:
        for period in periods:

            table_name = f"{stock['symbol']}_{period['name']}"

            df_target_unixtime = df_unixtime.query(
                f'TABLE_NAME == "{table_name}"'
            )

            target_unixtime = 0
            if not df_target_unixtime.empty:
                target_unixtime = df_target_unixtime["UNIX_TIME"].values[0]

            # response_data = request_investpy(
            #     stock["symbol"], period["time"], target_unixtime
            # )


def load_recently_unixtime(client: BqClient):

    table_name = f"{project_id}.{dataset}.{recently_unixtime_table}"
    query = f"SELECT TABLE_NAME, UNIX_TIME FROM `{table_name}`;"

    unixtime_df = client.query(query).to_dataframe()

    return unixtime_df


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
        functionName="crypto-collector",
        eventTime=str(int(time.time()))
    )
