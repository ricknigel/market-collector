#!/usr/bin/bash

topicId="collect-schedule-topic"

# スケジュール作成
gcloud scheduler jobs create pubsub "market-collect-scheduler" \
  --schedule="0 */8 * * *" \
  --topic=${topicId} \
  --message-body="none" \
  --time-zone="Asia/Tokyo"
