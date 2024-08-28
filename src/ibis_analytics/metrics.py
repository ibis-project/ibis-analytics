# imports
import ibis


# define metrics
def get_categories(t: ibis.Table, column: str) -> list:
    return t.distinct(on=column)[column].to_pandas().tolist()


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
                preceding=28,
                following=0,
            )
        ),
    ).order_by("timestamp")

    return t
