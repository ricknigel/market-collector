gcloud functions deploy insert_to_bigquery \
  --runtime=python39 \
  --trigger-resource=market_data_accumlation \
  --trigger-event=google.storage.object.finalize \
  --region=asia-northeast1 \
  --env-vars-file=.env.yml \
  --timeout=300
