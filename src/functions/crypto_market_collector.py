from io import BytesIO
import os
from pandas.core.frame import DataFrame
import requests
from google.cloud import bigquery, storage
import pandas as pd
from shared_module import project_id, dataset

exchanges = [
    {'exchange': 'coinbase-pro', 'ticker': 'BTCUSD'},
    {'exchange': 'coinbase-pro', 'ticker': 'ETHBTC'}
]

periods = [
    {'name': "1M", 'time': "60"},
    {'name': "3M", 'time': "180"},
    {'name': "5M", 'time': "300"},
    {'name': "15M", 'time': "900"},
    {'name': "30M", 'time': "1800"},
    {'name': "1H", 'time': "3600"},
    {'name': "2H", 'time': "7200"},
    {'name': "4H", 'time': "14400"},
    {'name': "6H", 'time': "21600"},
    {'name': "12H", 'time': "43200"},
    {'name': "1D", 'time': "86400"},
    {'name': "3D", 'time': "259200"},
    {'name': "1W", 'time': "604800"}
]

columns = [
    'UNIX_TIME',
    'OPEN_PRICE',
    'HIGH_PRICE',
    'LOW_PRICE',
    'CLOSE_PRICE',
    'VOLUME',
    'QUOTE_VOLUME'
]

crypto_watch_url = "https://api.cryptowat.ch/markets/{exchange}/{ticker}/ohlc"
recently_unixtime_table = 'RECENTLY_UNIXTIME'
bucket_name = os.environ['GCS_BUCKET']


def crypto_market_collector_main():

    bigquery_client = bigquery.Client(project_id)

    # 最新unixTime取得
    df_unixtime = load_recently_unixtime(bigquery_client)

    for exchange in exchanges:
        for period in periods:

            table_name = f"{exchange['ticker']}_{period['name']}"

            df_target_unixtime = df_unixtime.query(
                f'TABLE_NAME == "{table_name}"'
            )

            target_unixtime = 0
            if len(df_target_unixtime) == 1:
                target_unixtime = df_target_unixtime['UNIX_TIME'].values[0]

            response_data = request_crypto_watch_api(
                exchange['exchange'],
                exchange['ticker'],
                period['time'],
                target_unixtime
            )

            # レスポンスデータの長さが2未満の場合、データが無いかもしくは、未来のデータしかないため、処理を中断する
            if len(response_data) < 2:
                continue

            # レスポンスデータの末尾には未来日の価格に実行時点の最新値が入るが、この値は実行日時によって変化してしまうため、不確実のデータとなる。
            # よって未来日の項目となる配列の末尾を削除する。
            response_data = response_data[:-1]

            # api取得分のデータ(list in list)をDataFrameに変換
            df_api = pd.DataFrame(response_data, columns=columns)
            df_api['CLOSE_TIME'] = pd.to_datetime(
                df_api['UNIX_TIME'],
                unit='s'
            )

            # api取得分のdfをcsv形式でgcsへアップロードする
            upload_df_to_gcs(
                exchange['ticker'],
                period['name'],
                df_api
            )

            # api取得データから最新のunixtimeを取得する
            max_unixtime = df_api['UNIX_TIME'].max()

            if df_target_unixtime.empty:
                # unixtimeデータフレームが空の場合、追加する
                df_unixtime = df_unixtime.append(
                    {'TABLE_NAME': table_name, 'UNIX_TIME': max_unixtime},
                    ignore_index=True)
            else:
                # unixtimeデータフレームが空ではない場合、該当するunixtimeを更新する
                df_unixtime.loc[
                    df_unixtime['TABLE_NAME'] == table_name, 'UNIX_TIME'
                ] = max_unixtime

    # unixtime重複削除
    update_recently_unixtime(bigquery_client, df_unixtime)


def load_recently_unixtime(client: bigquery.Client):

    query = f"""
        SELECT
            TABLE_NAME,
            UNIX_TIME
        FROM
            `{dataset}.{recently_unixtime_table}`;
    """

    unixtime_df = client.query(query).to_dataframe()

    return unixtime_df


def request_crypto_watch_api(exchange, ticker, period, unixtime):
    endpoint = crypto_watch_url.format(
        exchange=exchange,
        ticker=ticker
    )

    # unixtimeに1を加えた値を設定することで、前回取得分以降のデータを取得する
    params = {'periods': period, 'after': unixtime + 1}

    response = requests.get(endpoint, params=params).json()

    return response['result'][period]


def upload_df_to_gcs(ticker, period, df_api):
    client = storage.Client(project_id)
    bucket = client.get_bucket(bucket_name)

    gcs_path = f'{ticker}/{period}.csv'
    blob = bucket.blob(gcs_path)

    if blob.exists():
        # 特定pathにファイルがある場合、元ファイルのdfにapi取得分のdfを結合する
        df_master: DataFrame = pd.read_csv(BytesIO(blob.download_as_string()))
        df_api = df_master.append(df_api, ignore_index=True, sort=False)

    # 特定pathにファイルがない場合、api取得分のdfを直接gcsへアップロードする

    # アップロード用のblobを取得
    upload_blob = bucket.blob(gcs_path)
    upload_blob.upload_from_string(
        df_api.to_csv(index=False, header=True, sep=','),
        content_type='text/csv'
    )


def update_recently_unixtime(client: bigquery.Client, df_unixtime):

    table_id = f'{project_id}.{dataset}.{recently_unixtime_table}'
    # unixtimeデータフレームをunixtime管理テーブルへinsert
    client.insert_rows_from_dataframe(client.get_table(table_id), df_unixtime)

    # unixtime管理テーブルでTABLE_NAMEカラムが重複してるデータを削除
    duplicate_query = f"""
        SELECT
            * EXCEPT(rowNumber)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        TABLE_NAME
                    ORDER BY
                        UNIX_TIME DESC
                ) as rowNumber
            FROM
                {table_id}
        )
        WHERE
            rowNumber = 1;
    """

    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_id
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job = client.query(duplicate_query, job_config=job_config)
    job.result()
