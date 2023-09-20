# imports
import toml
import ibis

from dotenv import load_dotenv

# options
## load .env
load_dotenv()

## config.toml
config = toml.load("config.toml")["app"]

## ibis config
con = ibis.connect(f"duckdb://{config['database']}", read_only=True)

# drop tables
cur = con.raw_sql("drop database if exists staging_metrics cascade;")
cur.close()
con.raw_sql("drop database if exists prod_metrics cascade;")
cur.close()
