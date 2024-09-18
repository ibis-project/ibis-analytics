# imports
import os
import json
import time
import httpx
import zulip
import typer
import requests

import logging as log

from dotenv import load_dotenv

from ibis_analytics.config import (
    GH_REPOS,
    ZULIP_URL,
    DOCS_URL,
    DATA_DIR,
    RAW_DATA_DIR,
    RAW_DATA_GH_DIR,
    RAW_DATA_DOCS_DIR,
    RAW_DATA_ZULIP_DIR,
)
from ibis_analytics.ingest.graphql_queries import (
    issues_query,
    pulls_query,
    forks_query,
    commits_query,
    stargazers_query,
    watchers_query,
)

# configure logger
log.basicConfig(level=log.INFO)


# main function
def main(gh: bool, zulip: bool, docs: bool):
    """
    Ingest data.
    """
    # load environment variables
    load_dotenv()

    # ingest data
    if gh:
        typer.echo("Ingesting GitHub data...")
        ingest_gh(gh_repos=GH_REPOS)
    if zulip:
        typer.echo("Ingesting Zulip data...")
        ingest_zulip(zulip_url=ZULIP_URL)
    if docs:
        typer.echo("Ingesting docs data...")
        ingest_docs(docs_url=DOCS_URL)


# helper functions
def write_json(data, filename):
    # write the data to a file
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)


# ingest functions
def ingest_gh(gh_repos):
    """
    Ingest GitHub data.
    """

    # constants
    GRAPH_URL = "https://api.github.com/graphql"

    # load environment variables
    GH_TOKEN = os.getenv("GITHUB_TOKEN")

    assert GH_TOKEN is not None and GH_TOKEN != "", "GITHUB_TOKEN is not set"

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
            # wait
            time.sleep(0.1)
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
            except Exception as e:
                # print error if response
                log.error(f"\t\tFailed to fetch data for {owner}/{repo}: {e}")

                try:
                    log.error(f"\t\t\tResponse: {resp.text}")
                except Exception as e:
                    log.error(f"\t\t\tFailed to print response: {e}")
                break

    # create a requests session
    with requests.Session() as client:
        for repo in gh_repos:
            log.info(f"Fetching data for {repo}...")
            for query in queries:
                owner, repo_name = repo.split("/")
                output_dir = os.path.join(
                    DATA_DIR,
                    RAW_DATA_DIR,
                    RAW_DATA_GH_DIR,
                    f"repo_name={repo_name}",
                )
                os.makedirs(output_dir, exist_ok=True)
                log.info(f"\tFetching data for {owner}/{repo_name} {query}...")
                fetch_data(client, owner, repo_name, query, queries[query], output_dir)


def ingest_zulip(zulip_url, zulip_email: str = "cody@dkdc.dev"):
    """Ingest the Zulip data."""
    # load config
    # configure logger
    log.basicConfig(level=log.INFO)

    # load environment variables
    zulip_key = os.getenv("ZULIP_KEY")

    # create the client
    client = zulip.Client(email=zulip_email, site=zulip_url, api_key=zulip_key)

    # get the users
    r = client.get_members()
    if r["result"] != "success":
        log.error(f"Failed to get users: {r}")
    else:
        members = r["members"]
        # write the users to a file
        filename = "members.json"
        output_dir = os.path.join(DATA_DIR, RAW_DATA_DIR, RAW_DATA_ZULIP_DIR)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
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

        # write the messages to a file
        filename = "messages.json"
        output_dir = os.path.join(DATA_DIR, RAW_DATA_DIR, RAW_DATA_ZULIP_DIR)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
        log.info(f"Writing messages to: {output_path}")
        write_json(all_messages, output_path)


def ingest_docs(docs_url):
    """Ingest the docs data."""

    # configure logger
    log.basicConfig(level=log.INFO)

    url = docs_url
    endpoint = "/api/v0/export"

    # load environment variables
    goat_token = os.getenv("GOAT_TOKEN")

    # create the headers
    headers = {
        "Authorization": f"Bearer {goat_token}",
        "Content-Type": "application/json",
    }

    # start the .csv.gz export
    log.info(f"Starting export...")
    r = httpx.post(url + endpoint, headers=headers)
    log.info(f"Status code: {r.status_code}")
    log.info(f"Response: {r.text}")

    # get the export id
    try:
        export_id = r.json()["id"]
        log.info(f"Export ID: {export_id}")
    except:
        log.error(f"Failed to get export id: {r}")
        log.error(f"Response: {r.text}")
        return

    # wait a few seconds
    time.sleep(5)

    # wait for the export to finish
    while True:
        log.info(f"Checking export status...")
        r = httpx.get(url + endpoint + f"/{export_id}", headers=headers)
        log.info(f"Status code: {r.status_code}")
        log.info(f"Response: {r.text}")

        # check the status
        try:
            status = r.json()["finished_at"]
            log.info(f"Status: {status}")
            if status is not None:
                break
        except:
            log.error(f"Failed to get status: {r}")
            log.error(f"Response: {r.text}")
            return

        # sleep for 10 seconds
        time.sleep(10)

    # download the export
    log.info(f"Downloading export...")
    r = httpx.get(url + endpoint + f"/{export_id}/download", headers=headers)
    log.info(f"Status code: {r.status_code}")

    # write the export to a file
    try:
        output_dir = os.path.join(DATA_DIR, RAW_DATA_DIR, RAW_DATA_DOCS_DIR)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "goatcounter.csv.gz")
        with open(output_path, "wb") as f:
            f.write(r.content)
    except:
        log.error(f"Failed to write export to file: {r}")
        return


# def ingest_pypi(pypi_package):
#     """
#     Ingest PyPI data.
#     """
#     log.info(f"Fetching data for {pypi_package}...")
#
#     # define external source
#     host = "clickpy-clickhouse.clickhouse.com"
#     port = 443
#     user = "play"
#     database = "pypi"
#
#     ch_con = ibis.clickhouse.connect(
#         host=host,
#         port=port,
#         user=user,
#         database=database,
#     )
#
#     # create output directory
#     output_dir = os.path.join(DATA_DIR, RAW_DATA_DIR, "pypi")
#     os.makedirs(output_dir, exist_ok=True)
#
#     # get table and metadata
#     t = ch_con.table(
#         "pypi_downloads_per_day_by_version_by_installer_by_type_by_country"
#     ).filter(ibis._["project"] == pypi_package)
#     min_date = t["date"].min().to_pyarrow().as_py()
#     max_date = t["date"].max().to_pyarrow().as_py()
#
#     # write data to parquet files
#     date = min_date
#     while date <= max_date:
#         old_date = date
#         date += timedelta(days=7)
#         a = t.filter(t["date"] >= old_date, t["date"] < date)
#
#         filename = f"start_date={old_date.strftime('%Y-%m-%d')}.parquet"
#         log.info(f"\tWriting data to {os.path.join(output_dir, filename)}...")
#         if a.count().to_pyarrow().as_py() > 0:
#             a.to_parquet(os.path.join(output_dir, filename))
#         log.info(f"\tData written to {os.path.join(output_dir, filename)}...")
