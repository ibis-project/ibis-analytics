# imports
import re
import os
import sys
import toml
import ibis
import openai
import marvin
import requests

import ibis.selectors as s
import logging as log
import plotly.io as pio
import plotly.express as px

from enum import Enum

from rich import print
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

from azure.identity import DefaultAzureCredential as DAC
from azure.storage.blob import BlobServiceClient as BSC
from azure.storage.filedatalake import DataLakeServiceClient as DLSC
from azure.storage.filedatalake import DataLakeFileClient as DLFC

## local imports
import dag.functions as f

from dag.assets import extract, load, transform

# configuration
## logger
log.basicConfig(level=log.INFO)

## config.toml
config = toml.load("config.toml")["eda"]

## load .env file
load_dotenv()

## ibis config
ibis.options.interactive = True

## ai config
model = "azure_openai/gpt-4"
marvin.settings.llm_model = model

## cloud config
account_name = "notonedrive"
account_url = f"https://{account_name}.blob.core.windows.net/"
storage_token = os.getenv("AZURE_STORAGE_KEY")
credential = {"account_name": account_name, "account_key": storage_token}

bsc = BSC(account_url=account_url, credential=credential)
dlsc = DLSC(account_url=account_url, credential=credential)
bs = bsc.get_blob_client("eda", "cache.ddb")
dlfc = dlsc.get_file_system_client("eda")
dldc = dlfc.get_directory_client("eda")

# variables
NOW = datetime.now()
NOW_7 = NOW - timedelta(days=7)
NOW_30 = NOW - timedelta(days=30)
NOW_90 = NOW - timedelta(days=90)
NOW_180 = NOW - timedelta(days=180)
NOW_365 = NOW - timedelta(days=365)
NOW_10 = NOW - timedelta(days=3650)

# connect to database
database = config["database"]
log.info(f"database: {database}")
con = ibis.connect(f"duckdb://{database}")
