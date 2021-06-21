#!/usr/bin/bash

dataset_name="MARKET_DATA"
table_name="RECENTLY_UNIXTIME"

echo "start create Tables in BigQuery"

bq mk -t "${dataset_name}.${table_name}" ../schema/RECENTLY_UNIXTIME_schema.json
