# imports
import re
import os
import sys
import toml
import ibis
import openai
import requests

import ibis.selectors as s
import logging as log
import plotly.io as pio
import plotly.express as px

from rich import print
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

from langchain.chat_models import ChatOpenAI
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)
from langchain.chains import LLMChain
from langchain.schema import BaseOutputParser

# configuration
## logger
log.basicConfig(level=log.INFO)

## config.toml
config = toml.load("config.toml")["eda"]

## load .env file
load_dotenv()

## ibis config
ibis.options.interactive = True

## ai config
openai.api_type = "azure"
openai.api_base = "https://birdbrain.openai.azure.com/"
openai.api_version = "2023-03-15-preview"
openai.api_key = os.getenv("AZURE_OPENAI_API_KEY")
#model = "birdbrain-35"
model = "birdbrain-4"

system = f"""
You are to pick the correct string. You will receive an input like:

"string_0 or string_1?"
""".strip()

messages = []
messages.append({"role": "system", "content": system})

## plotly config
pio.templates.default = "plotly_dark"

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
# TODO: fix
if "_share" in database:
    con = ibis.connect(f"duckdb://")
    con.raw_sql(f"ATTACH '{database}' as eda;")
else:
    con = ibis.connect(f"duckdb://{database}")

# load tables
docs = con.table("docs")
downloads = con.table("downloads")
stars = con.table("stars")
issues = con.table("issues")
pulls = con.table("pulls")
forks = con.table("forks")
watchers = con.table("watchers")
commits = con.table("commits")


# scratch
chat = ChatOpenAI(openai_api_key=openai.api_key)

#@ibis.udf.scalar.python
#def llm(user_message: str = "") -> str:
#    log.info("llming")
#    messages.append({"role": "user", "content": user_message})
#    full_response = ""
#    for response in openai.ChatCompletion.create(
#        engine=model,
#        messages=messages,
#        stream=True,
#        temperature=0.7,
#        max_tokens=150,
#        top_p=0.95,
#        frequency_penalty=0.5,
#        presence_penalty=0.0,
#        stop=None,
#    ):
#        print(response.choices[0].delta.get("content", ""), end="")
#        full_response += response.choices[0].delta.get("content", "")
#    messages.append({"role": "system", "content": full_response})
#    return full_response

orgs = stars.group_by("company").agg()
t = orgs.join(orgs, how="cross", lname="a", rname="b")
t = t.mutate(t.a.levenshtein(t.b).name("c"))
t = t.mutate(
    (((t.a.length() + t.b.length()) - t.c) / (t.a.length() + t.b.length())).name(
        "ratio"
    )
)
temp = t.filter(t.ratio > 0.7)
test = f"{temp[4:5].a.to_pandas()[0]} or {temp[4:5].b.to_pandas()[0]}?"

template = "Which is a better name?"
system_message_prompt = SystemMessagePromptTemplate.from_template(template)
user_template = "{string_a} or {string_b}?"
user_message_prompt = HumanMessagePromptTemplate.from_template(user_template)

chat_prompt = ChatPromptTemplate.from_messages([system_message_prompt, user_message_prompt])
messages = chat_prompt.format_messages(string_a=temp[4:5].a.to_pandas()[0], string_b=temp[4:5].b.to_pandas()[0])

class CommaSeparatedListOutputParser(BaseOutputParser):
    """Parse the output of an LLM call to a comma-separated list."""


    def parse(self, text: str):
        """Parse the output of an LLM call."""
        return text.strip().split(", ")

template = """You are a helpful assistant who picks the better string of two options.
A user will pass two strings and ask you which is best.
ONLY return a single, and nothing more."""
system_message_prompt = SystemMessagePromptTemplate.from_template(template)
user_template = "{string_a} or {string_b}?"
user_message_prompt = HumanMessagePromptTemplate.from_template(user_template)

chat_prompt = ChatPromptTemplate.from_messages([system_message_prompt, user_message_prompt])
chain = LLMChain(
    llm=ChatOpenAI(engine=model),
    prompt=chat_prompt,
    output_parser=CommaSeparatedListOutputParser()
)
a = temp[4:5].a.to_pandas()[0]
b = temp[4:5].b.to_pandas()[0]


@ibis.udf.scalar.python
def llm_dedup(string_a: str = "", string_b: str = "") -> str:
    log.info("llming")
    response = chain.run({"string_a": string_a, "string_b": string_b})
    return response[0]
