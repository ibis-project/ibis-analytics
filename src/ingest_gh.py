# imports
import os
import toml
import json
import requests

import logging as log

from dotenv import load_dotenv
from datetime import datetime

## local imports
from graphql_queries import *

# configure logger
log.basicConfig(level=log.INFO)

# constants
GRAPH_URL = "https://api.github.com/graphql"

# load environment variables
load_dotenv()
GH_TOKEN = os.getenv("GITHUB_TOKEN")

# load config
config = toml.load("config.toml")["github"]
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
        log.info(f"\t\tFetching page {page}...")
        resp = requests.post(
            GRAPH_URL,
            headers=headers,
            json={"query": query, "variables": variables},
        )
        json_data = resp.json()

        if resp.status_code != 200:
            log.error(
                f"\t\tFailed to fetch data for {owner}/{repo}; url={GRAPH_URL}\n\n {resp.status_code}\n {resp.text}"
            )
            return

        # extract data
        try:
            if query_name == "commits":
                data = json_data["data"]["repository"]["defaultBranchRef"]["target"][
                    "history"
                ]["edges"]
                # get the next link
                cursor = json_data["data"]["repository"]["defaultBranchRef"]["target"][
                    "history"
                ]["pageInfo"]["endCursor"]
                has_next_page = json_data["data"]["repository"]["defaultBranchRef"][
                    "target"
                ]["history"]["pageInfo"]["hasNextPage"]

            else:
                data = json_data["data"]["repository"][query_name]["edges"]
                cursor = json_data["data"]["repository"][query_name]["pageInfo"][
                    "endCursor"
                ]
                has_next_page = json_data["data"]["repository"][query_name]["pageInfo"][
                    "hasNextPage"
                ]
        except:
            if not os.getenv("CI"):
                breakpoint()
            else:
                pass

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


def get_filename(query_name, page):
    # return the filename
    return f"{query_name}.{page:06}.json"


def write_json(data, filename):
    # write the data to a file
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)


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


# create a requests session
with requests.Session() as client:
    for repo in config["repos"]:
        log.info(f"Fetching data for {repo}...")
        for query in queries:
            owner, repo_name = repo.split("/")
            output_dir = os.path.join(
                "data",
                "github",
                owner,
                repo_name,
            )
            os.makedirs(output_dir, exist_ok=True)
            log.info(f"\tFetching data for {owner}/{repo_name} {query}...")
            fetch_data(client, owner, repo_name, query, queries[query], output_dir)
