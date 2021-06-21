from crypto_market_collector import crypto_market_collector_main
from insert_to_bigquery import insert_to_bigquery_main
from shared_module import publish_error_report


def crypto_market_collector(event, context):
    try:
        crypto_market_collector_main()
    except Exception as e:
        publish_error_report(str(e))
        print(e)


def insert_to_bigquery(data, context):
    try:
        insert_to_bigquery_main(data)
    except Exception as e:
        publish_error_report(str(e))
        print(e)
