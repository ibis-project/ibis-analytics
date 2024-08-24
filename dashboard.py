import ibis
import plotly.express as px

from shiny import reactive, render
from shinyswatch import theme
from shinywidgets import render_plotly
from shiny.express import input, ui

from datetime import datetime, timedelta

from ibis_analytics.metrics import (
    pulls_t,
    stars_t,
    forks_t,
    issues_t,
    commits_t,
    downloads_t,
    docs_t,
    zulip_members_t,
    zulip_messages_t,
)
from ibis_analytics.config import GH_REPO, PYPI_PACKAGE


# dark themes
px.defaults.template = "plotly_dark"
ui.page_opts(theme=theme.darkly)

# page options
ui.page_opts(
    title="Ibis analytics",
    fillable=False,
    full_width=True,
)

# add page title and sidebar
with ui.sidebar(open="desktop"):
    ui.input_date_range(
        "date_range",
        "Date range",
        start=(datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )
    ui.input_action_button("last_7d", "Last 7 days")
    ui.input_action_button("last_14d", "Last 14 days")
    ui.input_action_button("last_28d", "Last 28 days")
    ui.input_action_button("last_91d", "Last 91 days")
    ui.input_action_button("last_182d", "Last 182 days")
    ui.input_action_button("last_365d", "Last 365 days")
    ui.input_action_button("last_730d", "Last 730 days")
    ui.input_action_button("last_all", "All available data")

    with ui.value_box(full_screen=True):
        "Total days in range"

        @render.express
        def total_days():
            start_date, end_date = date_range()
            days = (end_date - start_date).days
            f"{days:,}"


with ui.nav_panel("GitHub metrics"):
    f"GitHub repo: {GH_REPO}"
    with ui.layout_columns():
        with ui.value_box():
            "Total stars"

            @render.express
            def total_stars():
                val = stars_data().count().to_pyarrow().as_py()
                f"{val:,}"

        with ui.value_box():
            "Total pulls"

            @render.express
            def total_pulls():
                val = pulls_data().count().to_pyarrow().as_py()
                f"{val:,}"

        with ui.value_box():
            "Total issues"

            @render.express
            def total_issues():
                val = issues_data().count().to_pyarrow().as_py()
                f"{val:,}"

        with ui.value_box():
            "Total forks"

            @render.express
            def total_forks():
                val = forks_data().count().to_pyarrow().as_py()
                f"{val:,}"

        with ui.value_box():
            "Total commits"

            @render.express
            def total_commits():
                val = commits_data().count().to_pyarrow().as_py()
                f"{val:,}"

    with ui.layout_columns():
        with ui.card(full_screen=True):
            "Total stars"

            @render_plotly
            def stars_line():
                t = stars_data().order_by("starred_at")

                c = px.line(
                    t,
                    x="starred_at",
                    y="total_stars",
                )

                return c

        with ui.card(full_screen=True):
            "Rolling 28d stars"

            @render_plotly
            def stars_roll():
                t = stars_t

                t = (
                    t.mutate(starred_at=t["starred_at"].truncate("D"))
                    .group_by("starred_at")
                    .agg(stars=ibis._.count())
                )
                t = t.select(
                    timestamp="starred_at",
                    rolling_stars=ibis._["stars"]
                    .sum()
                    .over(
                        ibis.window(order_by="starred_at", preceding=28, following=0)
                    ),
                ).order_by("timestamp")

                c = px.line(
                    t,
                    x="timestamp",
                    y="rolling_stars",
                    range_x=[str(x) for x in date_range()],
                )

                return c

    with ui.card(full_screen=True):
        "Stars"

        with ui.card_header(class_="d-flex justify-content-between align-items-center"):
            with ui.layout_columns():
                ui.input_select(
                    "truncate_by_stars",
                    "Truncate to:",
                    ["D", "W", "M", "Y"],
                    selected="D",
                )
                ui.input_select(
                    "group_by_stars",
                    "Group by:",
                    [None, "company"],
                    selected=None,
                )

        @render_plotly
        def stars_flex():
            truncate_by = input.truncate_by_stars()
            group_by = input.group_by_stars()

            t = stars_data().order_by("starred_at")
            t = t.mutate(starred_at=t["starred_at"].truncate(truncate_by))
            t = t.group_by(["starred_at", group_by] if group_by else "starred_at").agg(
                stars=ibis._.count()
            )
            if group_by:
                t = t.mutate(company=t["company"][:16])
            t = t.order_by("starred_at", ibis.desc("stars"))

            c = px.bar(
                t,
                x="starred_at",
                y="stars",
                color="company" if group_by else None,
                barmode="stack",
            )

            return c


with ui.nav_panel("PyPI metrics"):
    f"PyPI package: {PYPI_PACKAGE}"
    with ui.layout_columns():
        with ui.value_box(full_screen=True):
            "Total downloads"

            @render.express
            def total_downloads():
                val = downloads_data()["count"].sum().to_pyarrow().as_py()
                f"{val:,}"

        with ui.value_box(full_screen=True):
            "Total versions"

            @render.express
            def total_versions():
                val = (
                    downloads_data()
                    .distinct(on="version")["version"]
                    .to_pyarrow()
                    .to_pylist()
                )
                f"{len(val):,}"

    with ui.card(full_screen=True):
        "Downloads by version"

        @render.data_frame
        def downloads_by_version():
            t = downloads_data()

            t = (
                t.mutate(
                    version=t["version"].split(".")[0],
                )
                .filter(~ibis._["version"].startswith("v"))
                .group_by("version")
                .agg(downloads=ibis._["count"].sum())
                .order_by(ibis.desc("downloads"))
            )

            return render.DataGrid(t.to_polars())

    with ui.layout_columns():
        with ui.card(full_screen=True):
            "Rolling 28d downloads"

            @render_plotly
            def downloads_roll():
                t = downloads_t
                min_date, max_date = input.date_range()

                t = t.mutate(
                    timestamp=t["date"].cast("timestamp"),
                )
                t = t.group_by("timestamp").agg(downloads=ibis._["count"].sum())
                t = (
                    t.select(
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
                    )
                    .filter(t["timestamp"] >= min_date, t["timestamp"] <= max_date)
                    .order_by("timestamp")
                )

                c = px.line(
                    t,
                    x="timestamp",
                    y="rolling_downloads",
                )

                return c

        with ui.card(full_screen=True):
            "Rolling 28d downloads by version"

            with ui.card_header(
                class_="d-flex justify-content-between align-items-center"
            ):
                with ui.layout_columns():
                    ui.input_select(
                        "version_style",
                        "Version style",
                        ["major", "major.minor", "major.minor.patch"],
                        selected="major",
                    )

            @render_plotly
            def downloads_by_version_roll():
                version_style = input.version_style()
                min_date, max_date = input.date_range()

                t = downloads_t

                t = t.mutate(
                    version=t["version"].split(".")[0]
                    if version_style == "major"
                    else t["version"].split(".")[0] + "." + t["version"].split(".")[1]
                    if version_style == "major.minor"
                    else t["version"],
                    timestamp=t["date"].cast("timestamp"),
                )
                t = t.group_by("timestamp", "version").agg(
                    downloads=ibis._["count"].sum()
                )
                t = t.filter(~t["version"].startswith("v"))
                t = t.filter(~t["version"].contains("dev"))
                t = (
                    t.select(
                        "timestamp",
                        "version",
                        rolling_downloads=ibis._["downloads"]
                        .sum()
                        .over(
                            ibis.window(
                                order_by="timestamp",
                                group_by="version",
                                preceding=28,
                                following=0,
                            )
                        ),
                    )
                    .filter(t["timestamp"] >= min_date, t["timestamp"] <= max_date)
                    .order_by("timestamp")
                )

                c = px.line(
                    t,
                    x="timestamp",
                    y="rolling_downloads",
                    color="version",
                    category_orders={
                        "version": reversed(
                            sorted(
                                t.distinct(on="version")["version"]
                                .to_pyarrow()
                                .to_pylist(),
                                # smartly convert string to float for sorting
                                # key=lambda x: int(x),
                                key=lambda x: tuple(map(float, x.split("."))),
                            )
                        )
                    },
                )

                return c

    with ui.card(full_screen=True):
        "Downloads"

        with ui.card_header(class_="d-flex justify-content-between align-items-center"):
            with ui.layout_columns():
                ui.input_select(
                    "group_by_downloads",
                    "Group by:",
                    [None, "version", "country_code", "installer", "type"],
                    selected="version",
                )

        @render_plotly
        def downloads_flex():
            group_by = input.group_by_downloads()

            t = downloads_data()
            t = t.mutate(timestamp=t["date"].cast("timestamp"))
            t = t.filter(~t["version"].startswith("v"))
            t = t.mutate(version=t["version"].split(".")[0])
            t = t.group_by(["timestamp", group_by] if group_by else "timestamp").agg(
                downloads=ibis._["count"].sum()
            )
            t = t.order_by("timestamp", ibis.desc("downloads"))

            c = px.bar(
                t,
                x="timestamp",
                y="downloads",
                color=group_by if group_by else None,
                barmode="stack",
                category_orders={
                    "version": reversed(
                        sorted(
                            t.distinct(on="version")["version"]
                            .to_pyarrow()
                            .to_pylist(),
                            key=lambda x: int(x),
                        )
                    )
                }
                if group_by == "version"
                else None,
            )

            return c


with ui.nav_panel("Docs metrics"):
    with ui.layout_columns():
        with ui.value_box(full_screen=True):
            "Total docs"

            @render.express
            def total_docs():
                val = docs_t.count().to_pyarrow().as_py()
                f"{val:,}"

    with ui.card(full_screen=True):
        "Docs"

        @render.data_frame
        def docs_grid():
            t = docs_t

            return render.DataGrid(t.limit(10_000).to_polars())


with ui.nav_panel("Zulip metrics"):
    with ui.layout_columns():
        with ui.value_box(full_screen=True):
            "Total messages"

            @render.express
            def total_messages():
                val = zulip_messages_t.count().to_pyarrow().as_py()
                f"{val:,}"

        with ui.value_box(full_screen=True):
            "Total members"

            @render.express
            def total_members():
                val = zulip_members_t.count().to_pyarrow().as_py()
                f"{val:,}"


# reactive calculations and effects
@reactive.calc
def date_range():
    start_date, end_date = input.date_range()

    return start_date, end_date


@reactive.calc
def stars_data(stars_t=stars_t):
    start_date, end_date = input.date_range()

    t = stars_t.filter(
        stars_t["starred_at"] >= start_date, stars_t["starred_at"] <= end_date
    )

    return t


@reactive.calc
def pulls_data(pulls_t=pulls_t):
    start_date, end_date = input.date_range()

    t = pulls_t.filter(
        pulls_t["created_at"] >= start_date, pulls_t["created_at"] <= end_date
    )

    return t


@reactive.calc
def forks_data(forks_t=forks_t):
    start_date, end_date = input.date_range()

    t = forks_t.filter(
        forks_t["created_at"] >= start_date, forks_t["created_at"] <= end_date
    )

    return t


@reactive.calc
def downloads_data(downloads_t=downloads_t):
    start_date, end_date = input.date_range()

    t = downloads_t.filter(
        downloads_t["date"] >= start_date, downloads_t["date"] <= end_date
    )

    return t


@reactive.calc
def issues_data(issues_t=issues_t):
    start_date, end_date = input.date_range()

    t = issues_t.filter(
        issues_t["created_at"] >= start_date, issues_t["created_at"] <= end_date
    )

    return t


@reactive.calc
def commits_data(commits_t=commits_t):
    start_date, end_date = input.date_range()

    t = commits_t.filter(
        commits_t["committed_date"] >= start_date,
        commits_t["committed_date"] <= end_date,
    )

    return t


@reactive.effect
@reactive.event(input.last_7d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_14d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_28d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_91d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=91)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_182d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=182)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_365d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_730d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_all)
def _():
    # TODO: pretty hacky
    min_all_tables = [
        (col, t[col].cast("timestamp").min().to_pyarrow().as_py())
        for t in [stars_t, pulls_t, forks_t, issues_t, commits_t, downloads_t]
        for col in t.columns
        if (
            str(t[col].type()).startswith("timestamp")
            or str(t[col].type()).startswith("date")
        )
        # this in particular should be cleaned up in the DAG
        and "created_at" not in col
    ]
    min_all_tables = min([x[1] for x in min_all_tables]) - timedelta(days=1)
    max_now = datetime.now() + timedelta(days=1)

    ui.update_date_range(
        "date_range",
        start=(min_all_tables).strftime("%Y-%m-%d"),
        end=max_now.strftime("%Y-%m-%d"),
    )
