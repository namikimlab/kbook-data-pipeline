#!/usr/bin/env bash
set -euo pipefail
cd /home/ec2-user/kbook-data-pipeline
echo "---- $(date -u) START ----" >> ingest.log
/home/ec2-user/venv/bin/python scripts/fetch_pages.py >> ingest.log 2>&1
echo "---- $(date -u) END ----"   >> ingest.log