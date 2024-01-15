# imports
import os
import ibis
import toml
import json
import zulip
import inspect
import requests

import logging as log

from ibis import _
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

from graphql_queries import (
    issues_query,
    pulls_query,
    forks_query,
    commits_query,
    stargazers_query,
    watchers_query,
)


# main function
def main():
    # load environment variables
    load_dotenv()

    # ingest data
    ingest_zulip()
    ingest_pypi()
    ingest_gh()
    # ingest_ci() # TODO: fix permissions, add assets


# helper functions
def write_json(data, filename):
    # write the data to a file
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)


# ingest functions
def ingest_gh():
    """
    Ingest the GitHub data.
    """
    # configure logger
    log.basicConfig(level=log.INFO)

    # constants
    GRAPH_URL = "https://api.github.com/graphql"

    # load environment variables
    GH_TOKEN = os.getenv("GITHUB_TOKEN")

    # load config
    config = toml.load("config.toml")["ingest"]["github"]
    log.info(f"Using repos: {config['repos']}")

    # construct header
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
    }

    # map queries
    queries = {
        "issues": issues_query,
        "pullRequests": pulls_query,
        "commits": commits_query,
        "forks": forks_query,
        "stargazers": stargazers_query,
        "watchers": watchers_query,
    }

    # define helper functions
    def get_filename(query_name, page):
        # return the filename
        return f"{query_name}.{page:06}.json"

    def get_next_link(link_header):
        # if there is no link header, return None
        if link_header is None:
            return None

        # split the link header into links
        links = link_header.split(", ")
        for link in links:
            # split the link into segments
            segments = link.split("; ")

            # if there are two segments and the second segment is rel="next"
            if len(segments) == 2 and segments[1] == 'rel="next"':
                # remove the < and > around the link
                return segments[0].strip("<>")

        # if there is no next link, return None
        return None

    def fetch_data(client, owner, repo, query_name, query, output_dir, num_items=100):
        # initialize variables
        variables = {
            "owner": owner,
            "repo": repo,
            "num_items": num_items,
            "before": "null",
        }

        # initialize page number
        page = 1

        # while True
        while True:
            # request data
            try:
                log.info(f"\t\tFetching page {page}...")
                resp = requests.post(
                    GRAPH_URL,
                    headers=headers,
                    json={"query": query, "variables": variables},
                )
                json_data = resp.json()

                log.info(f"\t\t\tStatus code: {resp.status_code}")
                # log.info(f"\t\t\tResponse: {resp.text}")
                # log.info(f"\t\t\tJSON: {json_data}")

                if resp.status_code != 200:
                    log.error(
                        f"\t\tFailed to fetch data for {owner}/{repo}; url={GRAPH_URL}\n\n {resp.status_code}\n {resp.text}"
                    )
                    return

                # extract data
                if query_name == "commits":
                    data = json_data["data"]["repository"]["defaultBranchRef"][
                        "target"
                    ]["history"]["edges"]
                    # get the next link
                    cursor = json_data["data"]["repository"]["defaultBranchRef"][
                        "target"
                    ]["history"]["pageInfo"]["endCursor"]
                    has_next_page = json_data["data"]["repository"]["defaultBranchRef"][
                        "target"
                    ]["history"]["pageInfo"]["hasNextPage"]

                else:
                    data = json_data["data"]["repository"][query_name]["edges"]
                    cursor = json_data["data"]["repository"][query_name]["pageInfo"][
                        "endCursor"
                    ]
                    has_next_page = json_data["data"]["repository"][query_name][
                        "pageInfo"
                    ]["hasNextPage"]

                # save json to a file
                filename = get_filename(query_name, page)
                output_path = os.path.join(output_dir, filename)
                log.info(f"\t\tWriting data to {output_path}")
                write_json(data, output_path)

                variables["cursor"] = f"{cursor}"
                print(f"has_next_page={has_next_page}")
                print(f"cursor={cursor}")
                if not has_next_page:
                    break

                # increment page number
                page += 1
            except:
                # print error if response
                log.error(f"\t\tFailed to fetch data for {owner}/{repo}")

                try:
                    log.error(f"\t\t\tResponse: {resp.text}")
                except:
                    pass

                break

    # create a requests session
    with requests.Session() as client:
        for repo in config["repos"]:
            log.info(f"Fetching data for {repo}...")
            for query in queries:
                owner, repo_name = repo.split("/")
                output_dir = os.path.join(
                    "data",
                    "ingest",
                    "github",
                    owner,
                    repo_name,
                )
                os.makedirs(output_dir, exist_ok=True)
                log.info(f"\tFetching data for {owner}/{repo_name} {query}...")
                fetch_data(client, owner, repo_name, query, queries[query], output_dir)


def ingest_pypi():
    """
    Ingest the PyPI data.
    """
    # constants
    # set DEFAULT_BACKFILL to the number of days
    # since July 19th, 2015 until today
    DEFAULT_BACKFILL = (datetime.now() - datetime(2015, 7, 19)).days
    BIGQUERY_DATASET = "bigquery-public-data.pypi.file_downloads"

    # configure logger
    log.basicConfig(level=log.INFO)

    # load environment variables
    project_id = os.getenv("BQ_PROJECT_ID")
    log.info(f"Project ID: {project_id}")

    # load config
    config = toml.load("config.toml")["ingest"]["pypi"]
    log.info(f"Packages: {config['packages']}")

    # configure lookback window
    backfill = config["backfill"] if "backfill" in config else DEFAULT_BACKFILL
    log.info(f"Backfill: {backfill}")

    # for each package
    for package in config["packages"]:
        log.info(f"Package: {package}")
        # create output directory
        output_dir = os.path.join("data", "ingest", "pypi", package)
        os.makedirs(output_dir, exist_ok=True)

        # construct query
        query = f"""
        SELECT *
        FROM `{BIGQUERY_DATASET}`
        WHERE file.project = '{package}'
        AND DATE(timestamp)
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {backfill} DAY)
        AND CURRENT_DATE()
        """.strip()
        query = inspect.cleandoc(query)

        # connect to bigquery and execute query
        con = ibis.connect(f"bigquery://{project_id}")
        log.info(f"Executing query:\n{query}")
        t = con.sql(query)

        # write to parquet
        filename = f"file_downloads.parquet"
        output_path = os.path.join(output_dir, filename)
        log.info(f"Writing to: {output_path}")
        t.to_parquet(output_path)


def ingest_ci():
    """
    Ingest the CI data.
    """
    # constants
    # set DEFAULT_BACKFILL to the number of days
    # since July 19th, 2015 until today
    DEFAULT_BACKFILL = (datetime.now() - datetime(2015, 7, 19)).days

    # configure logger
    log.basicConfig(level=log.INFO)

    # load environment variables
    project_id = os.getenv("BQ_PROJECT_ID")
    log.info(f"Project ID: {project_id}")

    # load config
    config = toml.load("config.toml")["ingest"]["ci"]

    # configure lookback window
    backfill = config["backfill"] if "backfill" in config else DEFAULT_BACKFILL
    log.info(f"Backfill: {backfill}")

    # make sure the data directory exists
    os.makedirs("data/ingest/ci/ibis", exist_ok=True)

    # connect to databases
    con = ibis.connect("duckdb://data/ingest/ci/ibis/raw.ddb")
    bq_con = ibis.connect(f"bigquery://{project_id}/workflows")

    # copy over tables
    for table in bq_con.list_tables():
        log.info(f"Writing table: {table}")
        con.create_table(table, bq_con.table(table).to_pyarrow(), overwrite=True)


def ingest_zulip():
    """Ingest the Zulip data."""
    # constants
    email = "cody@dkdc.dev"

    # load config
    config = toml.load("config.toml")["ingest"]["zulip"]
    log.info(f"Using url: {config['url']}")

    # configure logger
    log.basicConfig(level=log.INFO)

    # load environment variables
    zulip_key = os.getenv("ZULIP_KEY")

    # create the client
    client = zulip.Client(email=email, site=config["url"], api_key=zulip_key)

    # get the users
    r = client.get_members()
    if r["result"] != "success":
        log.error(f"Failed to get users: {r}")
    else:
        members = r["members"]
        # make sure the directory exists
        os.makedirs("data/ingest/zulip", exist_ok=True)

        # write the users to a file
        filename = "members.json"
        output_path = os.path.join("data", "ingest", "zulip", filename)
        log.info(f"Writing members to: {output_path}")
        write_json(members, output_path)

    # get the messages
    all_messages = []
    r = client.get_messages(
        {"anchor": "newest", "num_before": 100, "num_after": 0, "type": "stream"}
    )
    if r["result"] != "success":
        log.error(f"Failed to get messages: {r}")
    else:
        messages = r["messages"]
        all_messages.extend(messages)
        while len(messages) > 1:
            r = client.get_messages(
                {
                    "anchor": messages[0]["id"],
                    "num_before": 100,
                    "num_after": 0,
                    "type": "stream",
                }
            )
            if r["result"] != "success":
                log.error(f"Failed to get messages: {r}")
                break
            else:
                messages = r["messages"]
                all_messages.extend(messages)

        # make sure the directory exists
        os.makedirs("data/ingest/zulip", exist_ok=True)

        # write the messages to a file
        filename = "messages.json"
        output_path = os.path.join("data", "ingest", "zulip", filename)
        log.info(f"Writing messages to: {output_path}")
        write_json(all_messages, output_path)


if __name__ == "__main__":
    main()
