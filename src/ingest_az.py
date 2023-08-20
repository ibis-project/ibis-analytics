# imports
import os
import toml

import logging as log

from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient as BSC

# configure logger
log.basicConfig(level=log.INFO)

# config.toml
config = toml.load("config.toml")["azure"]
transform_config = toml.load("config.toml")["transform"]

account_name = config.get("account_name")
directory = config.get("directory")
blob_name = config.get("blob_name")

# load env vars
load_dotenv()
storage_token = os.getenv("AZURE_STORAGE_SECRET")

# cloud config
account_url = f"https://{account_name}.blob.core.windows.net/"
credential = {"account_name": account_name, "account_key": storage_token}

bsc = BSC(account_url=account_url, credential=credential)
bs = bsc.get_blob_client(directory, "cache.ddb")

with open(transform_config.get("database"), "wb") as backup:
    log.info("Downloading database from Azure...")
    data = bs.download_blob()
    data.readinto(backup)
    log.info("Download complete.")
