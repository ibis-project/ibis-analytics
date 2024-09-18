# imports
import ibis


# define metrics
def get_categories(t: ibis.Table, column: str) -> list:
    return t.select(column).distinct()[column].to_pyarrow().to_pylist()


def total(t: ibis.Table) -> int:
    return t.count().to_pyarrow().as_py()


def stars_rolling(t: ibis.Table, days: int = 28) -> ibis.Table:
    t = (
        t.mutate(starred_at=t["starred_at"].truncate("D"))
        .group_by("starred_at")
        .agg(stars=ibis._.count())
    )
    t = t.select(
        timestamp="starred_at",
        rolling_stars=ibis._["stars"]
        .sum()
        .over(ibis.window(order_by="starred_at", preceding=days, following=0)),
    ).order_by("timestamp")

    return t


def downloads_rolling(t: ibis.Table, days: int = 28) -> ibis.Table:
    t = t.mutate(
        timestamp=t["date"].cast("timestamp"),
    )
    t = t.group_by("timestamp").agg(downloads=ibis._["count"].sum())
    t = t.select(
        "timestamp",
        rolling_downloads=ibis._["downloads"]
        .sum()
        .over(
            ibis.window(
                order_by="timestamp",
                preceding=days,
                following=0,
            )
        ),
    ).order_by("timestamp")

    return t


def downloads_rolling_by_version(
    t: ibis.Table, version_style=None, days: int = 28
) -> ibis.Table:
    t = t.mutate(
        version=t["version"].split(".")[0]
        if version_style == "major"
        else t["version"].split(".")[0] + "." + t["version"].split(".")[1]
        if version_style == "major.minor"
        else t["version"],
        timestamp=t["date"].cast("timestamp"),
    )
    t = t.group_by("timestamp", "version").agg(downloads=ibis._["count"].sum())
    t = t.filter(~t["version"].startswith("v"))
    t = t.filter(~t["version"].contains("dev"))
    t = t.select(
        "timestamp",
        "version",
        rolling_downloads=ibis._["downloads"]
        .sum()
        .over(
            ibis.window(
                order_by="timestamp",
                group_by="version",
                preceding=days,
                following=0,
            )
        ),
    ).order_by("timestamp")

    return t


def docs_rolling(t: ibis.Table, days: int = 28) -> ibis.Table:
    t = t.mutate(
        timestamp=t["timestamp"].cast("timestamp").truncate("D"),
    )
    t = t.group_by("timestamp").agg(docs=ibis._.count())
    t = t.select(
        "timestamp",
        rolling_docs=ibis._["docs"]
        .sum()
        .over(
            ibis.window(
                order_by="timestamp",
                preceding=days,
                following=0,
            )
        ),
    ).order_by("timestamp")

    return t


def docs_rolling_by_path(t: ibis.Table, days: int = 28) -> ibis.Table:
    t = t.mutate(
        timestamp=t["timestamp"].cast("timestamp").truncate("D"),
    )
    t = t.group_by("timestamp", "path").agg(docs=ibis._.count())
    t = t.select(
        "timestamp",
        "path",
        rolling_docs=ibis._["docs"]
        .sum()
        .over(
            ibis.window(
                order_by="timestamp",
                group_by="path",
                preceding=days,
                following=0,
            )
        ),
    ).order_by("timestamp")

    return t
