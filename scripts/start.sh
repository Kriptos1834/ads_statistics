#!/bin/bash

scriptPath=$(dirname "$(readlink -f "$0")")
printenv | sed 's/^\(.*\)$/export \1/g' > ${scriptPath}/.env.sh
chmod +x ${scriptPath}/.env.sh

python /ads_stat/get_stat/google_adwords/get_stat.py -df 2021-01-01 &&
python /ads_stat/get_stat/facebook_ads/get_stat.py -df 2021-01-01

cron -f