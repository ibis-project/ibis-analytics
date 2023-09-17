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
con.sql("drop database staging_metrics cascade")
con.sql("drop database prod_metrics cascade")
