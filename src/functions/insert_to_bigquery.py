from google.cloud import bigquery
from shared_module import project_id, dataset


def insert_to_bigquery_main(data):
    bucket = data['bucket']
    file_path: str = data["name"]
    gcs_uri = f'gs://{bucket}/{file_path}'

    table_name = get_table_name(file_path)
    table_id = f'{dataset}.{table_name}'

    client = bigquery.Client(project_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = 'WRITE_TRUNCATE'

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
