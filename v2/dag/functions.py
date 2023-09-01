# imports
import re
import datetime
import ibis

import ibis.selectors as s


# functions
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
    t = t.relabel("snake_case")
    t = t.mutate(s.across(s.of_type("timestamp"), lambda x: x.cast("timestamp('UTC')")))
    return t


def agg_downloads(downloads):
    downloads = downloads.mutate(
        major_minor_patch=clean_version(downloads["version"], patch=True),
        major_minor=clean_version(downloads["version"], patch=False).cast("float"),
    )
    downloads = downloads.relabel(
        {
            "version": "version_raw",
            "major_minor_patch": "version",
        }
    )
    downloads = (
        downloads.group_by(
            [
                ibis._.timestamp.truncate("D").name("timestamp"),
                ibis._.country_code,
                ibis._.version,
                ibis._.python,
                ibis._.system["name"].name("system"),
            ]
        )
        .agg(
            ibis._.count().name("downloads"),
        )
        .order_by(ibis._.timestamp.desc())
        .mutate(
            ibis._.downloads.sum()
            .over(
                rows=(0, None),
                group_by=["country_code", "version", "python", "system"],
                order_by=ibis._.timestamp.desc(),
            )
            .name("total_downloads")
        )
        .order_by(ibis._.timestamp.desc())
    )
    downloads = downloads.mutate(ibis._["python"].fillna("").name("python_full"))
    downloads = downloads.mutate(
        clean_version(downloads["python_full"], patch=False).name("python")
    )
    return downloads
