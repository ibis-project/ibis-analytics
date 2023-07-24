# imports
import os
import time
import ibis
import duckdb

from dotenv import load_dotenv

# load env vars
load_dotenv()

# connect to motherduck
con = duckdb.connect(f"md:")

# export metrics
con.sql("DROP DATABASE IF EXISTS metrics CASCADE")
con.sql("CREATE DATABASE metrics FROM 'cache.ddb'")
