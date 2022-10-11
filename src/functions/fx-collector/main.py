import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from google.cloud.bigquery import Client as BqClient, QueryJobConfig
from google.cloud.storage import Client as StorageClient
from google.cloud.pubsub import PublisherClient
import yfinance as yf

# GCPのプロジェクトID
project_id = os.getenv("GCP_PROJECT_ID")
# BigQueryのデータセット名
dataset = os.getenv("BIGQUERY_DATASET")
# 金融データ(csv)のアップロード先バケット名
bucket_name = os.getenv("MARKET_DATA_BUCKET")
# 最新unixtime管理テーブル名
recently_unixtime_table = os.getenv("BIGQUERY_UNIXTIME_TABLE")

fxs = [
    {"ticker": "EUR=X", "name": "USDEUR"},
    {"ticker": "JPY=X", "name": "USDJPY"},
    {"ticker": "GBP=X", "name": "USDGBP"},
    {"ticker": "CHF=X", "name": "USDCHF"},
    {"ticker": "AUD=X", "name": "USDAUD"},
    {"ticker": "CAD=X", "name": "USDCAD"},
    {"ticker": "HKD=X", "name": "USDHKD"},
    {"ticker": "KRW=X", "name": "USDKRW"},
    {"ticker": "EURJPY=X", "name": "EURJPY"},
    {"ticker": "EURGBP=X", "name": "EURGBP"},
    {"ticker": "EURCHF=X", "name": "EURCHF"},
]

periods = [
    {"name": "1M", "time": "1m"},
    {"name": "1D", "time": "1d"}
]

OPEN = "OPEN_PRICE"
HIGH = "HIGH_PRICE"
LOW = "LOW_PRICE"
CLOSE = "CLOSE_PRICE"
VOLUME = "VOLUME"
QUOTE_VOLUME = "QUOTE_VOLUME"
float_columns = [OPEN, HIGH, LOW, CLOSE, VOLUME, QUOTE_VOLUME]
columns = ["UNIX_TIME"] + float_columns


def handler(request):
    try:
        fx_collector()
    except Exception as e:
        publish_error_report(str(e))
        raise e

    return "ok"


def fx_collector():
    bigquery_client = BqClient(project_id)

    # 最新unixTimeを取得する
    df_unixtime = load_recently_unixtime(bigquery_client)

    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    execTime = now.strftime("%Y%m%d_%Hh")

    for fx in fxs:
        for period in periods:

            table_name = f"{fx['name']}_{period['name']}"

            df_target_unixtime = df_unixtime.query(
                f'TABLE_NAME == "{table_name}"'
            )

            target_unixtime = 0
            if not df_target_unixtime.empty:
                target_unixtime = df_target_unixtime["UNIX_TIME"].values[0]

            # yfinanceよりデータを取得する
            response = request_yfinance(
                fx["ticker"], period["time"], target_unixtime
            )
            # 整理・正規化
            df = cleansing_df(response, target_unixtime)

            if df.empty:
                continue

            # データをcsv形式でgcsへアップロードする
            upload_df_to_gcs(
                fx['name'],
                execTime,
                period["name"],
                df
            )

            # yfinance取得データから最新のunixtimeを取得する
            max_unixtime = df["UNIX_TIME"].max()

            if df_target_unixtime.empty:
                # unixtimeデータフレームが空の場合、追加する
                df_add = pd.DataFrame({
                    "TABLE_NAME": [table_name],
                    "UNIX_TIME": [max_unixtime]
                })
                df_unixtime = pd.concat(
                    [df_unixtime, df_add],
                    ignore_index=True
                )
            else:
                # unixtimeデータフレームが空ではない場合、該当するunixtimeを更新する
                df_unixtime.loc[
                    df_unixtime["TABLE_NAME"] == table_name, "UNIX_TIME"
                ] = max_unixtime

    # unixtime重複削除
    update_recently_unixtime(bigquery_client, df_unixtime)


def request_yfinance(ticker, interval, unixtime):

    period = "max" if unixtime == 0 else "5d"
    response = yf.download(
        tickers=ticker,
        interval=interval,
        period=period,
        auto_adjust=True
    )
    return response


def cleansing_df(df: pd.DataFrame, target_unixtime):
    # レスポンスデータの長さが2未満の場合、データが無いかもしくは、未来のデータしかないため、処理を中断する
    if len(df) < 2:
        return pd.DataFrame()

    # datetimeindexを使って日付の昇順にソート
    df = df.sort_index(ascending=True)

    # レスポンスデータの末尾には未来日の価格に実行時点の最新値が入るが、この値は実行日時によって変化してしまうため、不確実のデータとなる。
    # よって未来日の項目となる配列の末尾を削除する。
    df = df[:-1]

    # datetimeindexをCLOSE_TIMEカラムへ移動する
    index_name = df.index.name
    df.reset_index(inplace=True)
    df = df.rename(columns={index_name: "CLOSE_TIME"})

    # CLOSE_TIMEからunixtimeを生成する
    df["UNIX_TIME"] = df["CLOSE_TIME"].apply(lambda x: int(x.timestamp()))
    # CLOSE_TIMEをUTCに変換する
    df["CLOSE_TIME"] = pd.to_datetime(df["UNIX_TIME"], unit="s", utc=True)
    # 列名を変換する
    df = df.rename(columns={
        "Open": OPEN,
        "High": HIGH,
        "Low": LOW,
        "Close": CLOSE,
        "Volume": VOLUME
    })
    # QUOTE_VOLUMEカラムを設定する
    df[QUOTE_VOLUME] = 0
    # int → floatへ変換(cryptowatchから小数点無しで来る場合がある)
    df[float_columns] = df[float_columns].astype("float")

    # unixtimeより上のデータを抽出する
    if target_unixtime != 0:
        df = df.query(f'UNIX_TIME > {target_unixtime}')

    return df


def upload_df_to_gcs(ticker, execTime, period, df):
    """
    yfinanceデータ（データフレーム）をcsv形式でGCSへアップロードする
    """
    client = StorageClient(project_id)
    bucket = client.get_bucket(bucket_name)

    gcs_path = f"{dataset}/{ticker}/{execTime}/{period}.csv"

    blob = bucket.blob(gcs_path)
    blob.upload_from_string(
        df.to_csv(index=False, header=True, sep=","),
        content_type="text/csv"
    )


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


def update_recently_unixtime(client: BqClient, df_unixtime):
    """
    最新UnixTime管理テーブルのunixtimeを更新する
    """
    table_id = f"{project_id}.{dataset}.{recently_unixtime_table}"

    # unixtimeデータフレームをunixtime管理テーブルへinsert
    client.insert_rows_from_dataframe(client.get_table(table_id), df_unixtime)

    # insert処理が完了してから、重複削除処理をしたいので、5秒間待機する
    # time.sleep(5)

    # unixtime管理テーブルでTABLE_NAMEカラムが重複してるデータを削除
    # duplicate_query = f"""
    #     SELECT
    #         * EXCEPT(rowNumber)
    #     FROM (
    #         SELECT
    #             *,
    #             ROW_NUMBER() OVER (
    #                 PARTITION BY TABLE_NAME ORDER BY UNIX_TIME DESC
    #             ) as rowNumber
    #         FROM
    #             {table_id}
    #     )
    #     WHERE
    #         rowNumber = 1;
    # """

    # job_config = QueryJobConfig()
    # job_config.destination = table_id
    # job_config.write_disposition = "WRITE_TRUNCATE"
    # job = client.query(duplicate_query, job_config=job_config)
    # job.result()


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
        functionName="fx-collector",
        eventTime=str(int(time.time()))
    )
