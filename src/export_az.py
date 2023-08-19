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
storage_token = os.getenv("AZURE_STORAGE_KEY")

# cloud config
account_url = f"https://{account_name}.blob.core.windows.net/"
credential = {"account_name": account_name, "account_key": storage_token}

bsc = BSC(account_url=account_url, credential=credential)
bs = bsc.get_blob_client(directory, "cache.ddb")

# check directory exists
if not any(c.get("name") == directory for c in bsc.list_containers()):
    log.info("Creating container")
    bsc.create_container(directory)
    log.info("Container created")

with open(transform_config.get("database"), "rb") as data:
    log.info("Uploading database to Azure Storage")
    bs.upload_blob(data, overwrite=True)
    log.info("Database uploaded to Azure Storage")
