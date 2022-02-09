#!/bin/bash
scriptPath=$(dirname "$(readlink -f "$0")")
source "${scriptPath}/.env.sh"

date
python /ads_stat/get_stat/facebook_ads/get_stat.py -df $(date -d "2 days ago" +"%Y"-"%m"-"%d") &&
python /ads_stat/get_stat/google_adwords/get_stat.py -df $(date -d "2 days ago" +"%Y"-"%m"-"%d")