# imports
import re
import os
import sys
import toml
import ibis
import requests

import ibis.selectors as s
import logging as log
import plotly.io as pio
import plotly.express as px

from enum import Enum

from rich import print
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

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
