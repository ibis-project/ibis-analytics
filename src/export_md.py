# imports
import os
import time
import ibis
import duckdb

from dotenv import load_dotenv

# load env vars
load_dotenv()
#MD_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
#MD_TOKEN = "dummy_token"

# connect to motherduck
con = duckdb.connect(f"md:")
#con = ibis.connect(f"duckdb://md:")

# export metrics
con.sql("DROP DATABASE IF EXISTS metrics CASCADE")
time.sleep(30)
con.sql("CREATE DATABASE metrics FROM 'metrics.ddb'")
