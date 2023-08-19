# imports
import re
import os
import sys
import toml
import ibis
import openai
import requests

import ibis.selectors as s
import logging as log
import plotly.io as pio
import plotly.express as px

from rich import print
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

from azure.identity import DefaultAzureCredential as DAC
from azure.storage.blob import BlobServiceClient as BSC
from azure.storage.filedatalake import DataLakeServiceClient as DLSC
from azure.storage.filedatalake import DataLakeFileClient as DLFC

## local imports
from functions import *

# configuration
## logger
log.basicConfig(level=log.INFO)

## config.toml
config = toml.load("config.toml")["eda"]

## load .env file
load_dotenv()

## ibis config
ibis.options.interactive = True

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

## ai config
openai.api_type = "azure"
openai.api_base = "https://birdbrain.openai.azure.com/"
openai.api_version = "2023-03-15-preview"
openai.api_key = os.getenv("AZURE_OPENAI_API_KEY")
model = "birdbrain-35"
# model = "birdbrain-4"

system = f"""
You are to pick the correct string. You will receive an input like:

"string_0 or string_1?"
""".strip()

messages = []
messages.append({"role": "system", "content": system})

## plotly config
pio.templates.default = "plotly_dark"

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
# TODO: fix
if "_share" in database:
    con = ibis.connect(f"duckdb://")
    con.raw_sql(f"ATTACH '{database}' as eda;")
else:
    con = ibis.connect(f"duckdb://{database}")

# load tables
docs = con.table("docs")
downloads = con.table("downloads")
stars = con.table("stars")
issues = con.table("issues")
pulls = con.table("pulls")
forks = con.table("forks")
watchers = con.table("watchers")
commits = con.table("commits")


# @ibis.udf.scalar.python
# def llm(user_message: str = "") -> str:
#    log.info("llming")
#    messages.append({"role": "user", "content": user_message})
#    full_response = ""
#    for response in openai.ChatCompletion.create(
#        engine=model,
#        messages=messages,
#        stream=True,
#        temperature=0.7,
#        max_tokens=150,
#        top_p=0.95,
#        frequency_penalty=0.5,
#        presence_penalty=0.0,
#        stop=None,
#    ):
#        print(response.choices[0].delta.get("content", ""), end="")
#        full_response += response.choices[0].delta.get("content", "")
#    messages.append({"role": "system", "content": full_response})
#    return full_response

# orgs = stars.group_by("company").agg()
# t = orgs.join(orgs, how="cross", lname="a", rname="b")
# t = t.mutate(t.a.levenshtein(t.b).name("c"))
# t = t.mutate(
#    (((t.a.length() + t.b.length()) - t.c) / (t.a.length() + t.b.length())).name(
#        "ratio"
#    )
# )
# temp = t.filter(t.ratio > 0.7)
# a = temp[4:5].a.to_pandas()[0]
# b = temp[4:5].b.to_pandas()[0]
