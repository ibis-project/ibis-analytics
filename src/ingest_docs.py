import os
import time
import json
import requests

import pandas as pd
import logging as log
import gzip

from pathlib import Path
from dotenv import load_dotenv

# configure logger
log.basicConfig(level=log.INFO)

# load .env file
load_dotenv()

# setup for goatcounter
api_token = os.getenv("GOAT_TOKEN")
api_url = "https://ibis.goatcounter.com/api/v0"
headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json",
}


def get_last_cursor():
    path = Path("data/docs")
    files = path.glob("goatcounter-export-*.csv.gz")
    if not files:
        return None
    last_file = max(files, key=lambda p: p.stat().st_mtime)
    # extract the cursor from the filename
    return str(last_file.stem.split("-")[-2])


def start_export(cursor=None):
    data = {}
    if cursor is not None:
        data = {"start_from_hit_id": cursor}
    r = requests.post(f"{api_url}/export", headers=headers, data=data)
    breakpoint()
    if r.status_code >= 200 and r.status_code < 300:
        return r.json()["id"]
    else:
        log.error(f"Error starting export: {r.status_code} {r.text}")
        return None


def check_export(id):
    r = requests.get(f"{api_url}/export/{id}", headers=headers)
    return r.json()["finished_at"]


def download_export(id):
    r = requests.get(f"{api_url}/export/{id}/download", headers=headers)
    filename = f"data/docs/goatcounter-export-{id}.csv.gz"
    with open(filename, "wb") as f:
        f.write(r.content)


cursor = get_last_cursor()
export_id = start_export(cursor)

while True:
    time.sleep(1)
    if check_export(export_id):
        download_export(export_id)
        break
