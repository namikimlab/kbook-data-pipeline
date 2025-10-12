#!/usr/bin/env bash
set -euo pipefail
cd /home/ec2-user/kbook-data-pipeline
source venv/bin/activate
# log a timestamp so you know it ran
echo "---- $(date -u) (UTC) START ----" >> ingest.log
python scripts/fetch_pages.py >> ingest.log 2>&1
echo "---- $(date -u) (UTC) END ----"   >> ingest.log