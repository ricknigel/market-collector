dataset_name="MARKET_DATA"

target_tickers=(BTCUSD ETHBTC)

period_array=(1M 3M 5M 15M 30M 1H 2H 4H 6H 12H 1D 3D 1W)

for exchange in ${target_tickers[@]}; do
  for period in ${period_array[@]}; do
    table_name="${exchange}_${period}"
    echo "delete ${table_name} table"
    bq rm -f -t "${dataset_name}.${table_name}"
  done
done
