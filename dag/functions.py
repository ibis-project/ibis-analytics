# imports
import re
import ibis
import datetime

import ibis.selectors as s


# udfs
@ibis.udf.scalar.python
def clean_version(version: str, patch: bool = True) -> str:
    pattern = r"(\d+\.\d+\.\d+)" if patch else r"(\d+\.\d+)"
    match = re.search(pattern, version)
    if match:
        return match.group(1)
    else:
        return version


# functions
def now():
    return datetime.datetime.now()


def today():
    return now().date()


def clean_data(t):
    t = t.rename("snake_case")
    # t = t.mutate(s.across(s.of_type("timestamp"), lambda x: x.cast("timestamp('')")))
    return t


def add_ingested_at(t, ingested_at=now()):
    t = t.mutate(ingested_at=ingested_at).relocate("ingested_at")
    return t
