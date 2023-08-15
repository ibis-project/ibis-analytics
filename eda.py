# imports
import os
import sys
import toml
import ibis
import ibis.selectors as s
import logging as log
import plotly.io as pio
import plotly.express as px

from dotenv import load_dotenv
from datetime import datetime, timedelta, date

# configuration
## logger
log.basicConfig(level=log.INFO)

## config.toml
config = toml.load("config.toml")["eda"]

## plotly config
pio.templates.default = "plotly_dark"

## load .env file
load_dotenv()

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
ibis.options.interactive = True

# load tables
docs = con.table("docs")
downloads = con.table("downloads")
stars = con.table("stars")
issues = con.table("issues")
pulls = con.table("pulls")
forks = con.table("forks")
watchers = con.table("watchers")
commits = con.table("commits")
