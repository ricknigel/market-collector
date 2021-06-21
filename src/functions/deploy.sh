gcloud functions deploy crypto_market_collector \
  --runtime=python39 \
  --trigger-topic=collect-schedule-topic \
  --region=asia-northeast1 \
  --env-vars-file=.env.yml \
  --timeout=300 \
