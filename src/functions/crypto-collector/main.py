import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import requests
from google.cloud.bigquery import Client as BqClient
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
# 暗号資産データ取得先API(kraken API)
crypto_api_url = "https://api.kraken.com/0/public/OHLC"

tickers = [
    {"table": "BTCUSD", "ticker": "XBTUSD", "res_ticker": "XXBTZUSD"},
    {"table": "ETHBTC", "ticker": "ETHBTC", "res_ticker": "XETHXXBT"}
]

periods = [
    {"name": "1M", "time": "1"},
    # {"name": "3M", "time": "180"},
    {"name": "5M", "time": "5"},
    {"name": "15M", "time": "15"},
    {"name": "30M", "time": "30"},
    {"name": "1H", "time": "60"},
    # {"name": "2H", "time": "7200"},
    {"name": "4H", "time": "240"},
    # {"name": "6H", "time": "21600"},
    # {"name": "12H", "time": "43200"},
    {"name": "1D", "time": "1440"},
    # {"name": "3D", "time": "259200"},
    {"name": "1W", "time": "10080"}
]

api_columns = [
    "UNIX_TIME",
    "OPEN_PRICE",
    "HIGH_PRICE",
    "LOW_PRICE",
    "CLOSE_PRICE",
    "VWAP",
    "VOLUME",
    "COUNT"
]

float_columns = [
    "OPEN_PRICE",
    "HIGH_PRICE",
    "LOW_PRICE",
    "CLOSE_PRICE",
    "VOLUME",
]


def handler(request):
    try:
        crypto_collector()
    except Exception as e:
        publish_error_report(str(e))
        raise e

    return "ok"


def crypto_collector():
    bigquery_client = BqClient(project_id)

    # 最新unixTimeを取得する
    df_unixtime = load_recently_unixtime(bigquery_client)

    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    execTime = now.strftime("%Y%m%d_%Hh")

    for ticker in tickers:
        for period in periods:

            table_name = f"{ticker['table']}_{period['name']}"

            df_target_unixtime = df_unixtime.query(
                f'TABLE_NAME == "{table_name}"'
            )

            target_unixtime = 0
            if not df_target_unixtime.empty:
                target_unixtime = df_target_unixtime["UNIX_TIME"].values[0]

            response_data = request_crypto_watch_api(
                ticker["res_ticker"],
                ticker["ticker"],
                period["time"],
                target_unixtime
            )

            # レスポンスデータの長さが2未満の場合、データが無いかもしくは、未来のデータしかないため、処理を中断する
            if len(response_data) < 2:
                continue

            # レスポンスデータの末尾には未来日の価格に実行時点の最新値が入るが、この値は実行日時によって変化してしまうため、不確実のデータとなる。
            # よって未来日の項目となる配列の末尾を削除する。
            response_data = response_data[:-1]

            # api取得分のデータ(list in list)をDataFrameに変換
            df_api = pd.DataFrame(response_data, columns=api_columns)

            # 不要カラム削除
            df_api = df_api.drop(columns=['VWAP', 'COUNT'])

            # カラム追加
            df_api['QUOTE_VOLUME'] = 0

            # int → floatへ変換(cryptowatchから小数点無しで来る場合がある)
            df_api[float_columns] = df_api[float_columns].astype("float")
            # unixtime → datetimeへ変換
            df_api["CLOSE_TIME"] = pd.to_datetime(
                df_api["UNIX_TIME"],
                unit="s",
                utc=True
            )

            # api取得分のdfをcsv形式でgcsへアップロードする
            upload_df_to_gcs(
                ticker["table"],
                execTime,
                period["name"],
                df_api
            )

            # api取得データから最新のunixtimeを取得する
            max_unixtime = df_api["UNIX_TIME"].max()

            if df_target_unixtime.empty:
                # unixtimeデータフレームが空の場合、追加する
                df_unixtime = pd.concat([
                        df_unixtime,
                        {"TABLE_NAME": table_name, "UNIX_TIME": max_unixtime}
                    ],
                    ignore_index=True
                )
            else:
                # unixtimeデータフレームが空ではない場合、該当するunixtimeを更新する
                df_unixtime.loc[
                    df_unixtime["TABLE_NAME"] == table_name, "UNIX_TIME"
                ] = max_unixtime

    # unixtime重複削除
    update_recently_unixtime(bigquery_client, df_unixtime)


def load_recently_unixtime(client: BqClient):

    table_name = f"{project_id}.{dataset}.{recently_unixtime_table}"
    query = f"""
        SELECT
            TABLE_NAME,
            UNIX_TIME
        FROM
            `{table_name}`
        ORDER BY
            UNIX_TIME DESC;
    """

    unixtime_df = client.query(query).to_dataframe()

    return unixtime_df


def request_crypto_watch_api(res_ticker, ticker, period, unixtime):
    """
    暗号資産データを取得するためにkraken APIへリクエストする
    """

    # unixtimeに1を加えた値を設定することで、前回取得分以降のデータを取得する
    params = {"pair": ticker, "interval": period, "since": unixtime + 1}

    response = requests.get(crypto_api_url, params=params).json()

    return response["result"][res_ticker]


def upload_df_to_gcs(ticker, execTime, period, df_api):
    """
    APIデータ（データフレーム）をcsv形式でGCSへアップロードする
    """
    client = StorageClient(project_id)
    bucket = client.get_bucket(bucket_name)

    gcs_path = f"{dataset}/{ticker}/{execTime}/{period}.csv"

    blob = bucket.blob(gcs_path)
    blob.upload_from_string(
        df_api.to_csv(index=False, header=True, sep=","),
        content_type="text/csv"
    )


def update_recently_unixtime(client: BqClient, df_unixtime):
    """
    最新UnixTime管理テーブルのunixtimeを更新する
    """
    table_id = f"{project_id}.{dataset}.{recently_unixtime_table}"

    # unixtimeデータフレームをunixtime管理テーブルへinsert
    client.insert_rows_from_dataframe(client.get_table(table_id), df_unixtime)


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
