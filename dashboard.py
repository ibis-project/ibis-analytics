import ibis
import plotly.express as px

from shiny import reactive, render
from shinyswatch import theme
from shinywidgets import render_plotly
from shiny.express import input, ui

from datetime import datetime, timedelta

import ibis_analytics.plots as plots
import ibis_analytics.metrics as metrics

from ibis_analytics.tables import (
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
from ibis_analytics.config import GH_REPOS, PYPI_PACKAGES

gh_repos = [gh_repo.split("/")[1] for gh_repo in GH_REPOS]

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
        start=(datetime.now() - timedelta(days=28)),
        end=datetime.now() + timedelta(days=1),
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
            days = (end_date - start_date).days - 1  # YOLO
            f"{days:,}"


with ui.nav_panel("GitHub metrics"):
    with ui.layout_columns():
        ui.input_select(
            "repo_name",
            "Repo:",
            gh_repos,
            selected=gh_repos[0],
        )

    with ui.layout_columns():
        with ui.value_box():
            "Total stars"

            @render.express
            def total_stars():
                f"{metrics.total(stars_data()):,}"

        with ui.value_box():
            "Total pulls"

            @render.express
            def total_pulls():
                f"{metrics.total(pulls_data()):,}"

        with ui.value_box():
            "Total issues"

            @render.express
            def total_issues():
                f"{metrics.total(issues_data()):,}"

        with ui.value_box():
            "Total forks"

            @render.express
            def total_forks():
                f"{metrics.total(forks_data()):,}"

        with ui.value_box():
            "Total commits"

            @render.express
            def total_commits():
                f"{metrics.total(commits_data()):,}"

    with ui.layout_columns():
        with ui.card(full_screen=True):
            "Total stars"

            @render_plotly
            def stars_line():
                t = stars_data()
                c = plots.line(t, x="starred_at", y="total_stars")
                return c

        with ui.card(full_screen=True):
            "Rolling 28d stars"

            @render_plotly
            def stars_roll():
                start_date, end_date = input.date_range()
                repo_name = input.repo_name()

                t = stars_t
                t = t.filter(t["repo_name"] == repo_name)
                t = metrics.stars_rolling(t, days=28)
                t = t.filter(t["timestamp"] >= start_date, t["timestamp"] <= end_date)

                c = plots.line(
                    t,
                    x="timestamp",
                    y="rolling_stars",
                )

                return c

    with ui.card(full_screen=True):
        "Stars"

        with ui.card_header(class_="d-flex justify-content-between align-items-center"):
            with ui.layout_columns():
                ui.input_select(
                    "truncate_by_stars",
                    "Truncate to:",
                    ["D", "W", "M", "Q", "Y"],
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
            # this is for plotly legend display reasons
            if group_by == "company":
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
    with ui.layout_columns():
        ui.input_select(
            "package_name",
            "Package:",
            PYPI_PACKAGES,
            selected=PYPI_PACKAGES[0],
        )
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
                val = metrics.get_categories(downloads_data(), "version")
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
                package_name = input.package_name()

                t = downloads_t
                t = t.filter(t["project"] == package_name)
                min_date, max_date = input.date_range()

                t = metrics.downloads_rolling(t, days=28)
                t = t.filter(
                    t["timestamp"] >= min_date, t["timestamp"] <= max_date
                ).order_by("timestamp")

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
                package_name = input.package_name()
                version_style = input.version_style()
                min_date, max_date = input.date_range()

                t = downloads_t
                t = t.filter(t["project"] == package_name)

                t = metrics.downloads_rolling_by_version(
                    t, version_style=version_style, days=28
                )
                t = t.filter(
                    t["timestamp"] >= min_date, t["timestamp"] <= max_date
                ).order_by("timestamp")

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
                    [None, "version", "system", "country_code"],
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

    with ui.layout_columns():
        with ui.card(full_screen=True):
            "Rolling 28d docs"

            @render_plotly
            def docs_roll():
                t = docs_t
                min_date, max_date = input.date_range()

                t = metrics.docs_rolling(t, days=28)
                t = t.filter(
                    t["timestamp"] >= min_date, t["timestamp"] <= max_date
                ).order_by("timestamp")

                c = px.line(
                    t,
                    x="timestamp",
                    y="rolling_docs",
                )

                return c

        with ui.card(full_screen=True):
            "Rolling 28d docs by path"

            @render_plotly
            def docs_by_path_roll():
                t = docs_t
                min_date, max_date = input.date_range()

                t = metrics.docs_rolling_by_path(t, days=28)
                t = t.filter(
                    t["timestamp"] >= min_date, t["timestamp"] <= max_date
                ).order_by("timestamp")

                c = px.line(
                    t,
                    x="timestamp",
                    y="rolling_docs",
                    color="path",
                )

                # no legend
                c.update_layout(showlegend=False)

                return c

    with ui.card(full_screen=True):
        with ui.card(full_screen=True):
            with ui.layout_columns():
                ui.input_select(
                    "truncate_to_docs",
                    "Truncate to:",
                    ["D", "W", "M", "Q", "Y"],
                )
                ui.input_select(
                    "group_by_docs",
                    "Group by:",
                    [
                        None,
                        "path",
                        "browser",
                        "system",
                        "bot",
                        "referrer",
                        "location",
                        "first_visit",
                    ],
                    selected=None,
                )

        @render_plotly
        def docs_flex():
            group_by = input.group_by_docs()
            truncate_to = input.truncate_to_docs()

            t = docs_data()
            t = t.mutate(timestamp=t["timestamp"].truncate(truncate_to))
            t = t.group_by(["timestamp", group_by] if group_by else "timestamp").agg(
                count=ibis._.count()
            )
            t = t.order_by("timestamp", ibis.desc("count"))

            if group_by in ["path", "referrer"]:
                t = t.mutate(**{group_by: t[group_by][:10]})

            c = px.bar(
                t,
                x="timestamp",
                y="count",
                color=group_by if group_by else None,
                barmode="stack",
            )

            # no legend
            # c.update_layout(showlegend=False)

            return c

    with ui.card(full_screen=True):
        "Referrer by path"

        with ui.layout_columns():
            paths = (
                docs_t.group_by("path")
                .agg(count=ibis._.count())
                .mutate(rank=ibis.row_number().over(order_by=ibis.desc("count")))[
                    "path"
                ]
                .to_pyarrow()
                .to_pylist()
            )
            ui.input_select(
                "docs_path",
                "Doc page:",
                paths,
                selected=paths[0],
            )

        @render.data_frame
        def docs_referrer_table():
            path = input.docs_path()

            t = docs_data()
            t = t.filter(t["path"] == path)
            t = t.group_by("referrer").agg(count=ibis._.count())
            t = t.order_by(ibis.desc("count"))

            return render.DataGrid(t.to_polars())


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

    with ui.card(full_screen=True):
        "Members over time"

        @render_plotly
        def members_line():
            t = zulip_members_data()
            t = (
                t.mutate(timestamp=t["date_joined"].cast("date"))
                .group_by("timestamp")
                .agg(total_members=ibis._["total_members"].max())
                .order_by(ibis.desc("timestamp"))
            )

            # c = plots.line(t.to_polars(), x="timestamp", y="total_members")
            c = px.line(t.to_polars(), x="timestamp", y="total_members")
            return c

    with ui.card(full_screen=True):
        "Messages over time"

        @render_plotly
        def messages_line():
            t = zulip_messages_data()
            t = t.mutate(timestamp=t["timestamp"].cast("timestamp"))

            c = plots.line(t, x="timestamp", y="total_messages")
            return c


# reactive calculations and effects
@reactive.calc
def date_range():
    start_date, end_date = input.date_range()

    return start_date, end_date


@reactive.calc
def stars_data(stars_t=stars_t):
    start_date, end_date = input.date_range()
    repo_name = input.repo_name()

    t = stars_t.filter(
        stars_t["starred_at"] >= start_date, stars_t["starred_at"] <= end_date
    ).filter(stars_t["repo_name"] == repo_name)

    return t


@reactive.calc
def pulls_data(pulls_t=pulls_t):
    start_date, end_date = input.date_range()
    repo_name = input.repo_name()

    t = pulls_t.filter(
        pulls_t["created_at"] >= start_date, pulls_t["created_at"] <= end_date
    ).filter(pulls_t["repo_name"] == repo_name)

    return t


@reactive.calc
def forks_data(forks_t=forks_t):
    start_date, end_date = input.date_range()
    repo_name = input.repo_name()

    t = forks_t.filter(
        forks_t["created_at"] >= start_date, forks_t["created_at"] <= end_date
    ).filter(forks_t["repo_name"] == repo_name)

    return t


@reactive.calc
def downloads_data(downloads_t=downloads_t):
    start_date, end_date = input.date_range()
    package_name = input.package_name()

    t = (
        downloads_t.filter(
            downloads_t["date"] >= start_date, downloads_t["date"] <= end_date
        )
        .filter(downloads_t["project"] == package_name)
        .mutate(system=ibis.ifelse(ibis._["system"] == "", "unknown", ibis._["system"]))
    )

    return t


@reactive.calc
def docs_data(docs_t=docs_t):
    start_date, end_date = input.date_range()

    t = docs_t.filter(
        docs_t["timestamp"] >= start_date, docs_t["timestamp"] <= end_date
    )

    return t


@reactive.calc
def issues_data(issues_t=issues_t):
    start_date, end_date = input.date_range()
    repo_name = input.repo_name()

    t = issues_t.filter(
        issues_t["created_at"] >= start_date, issues_t["created_at"] <= end_date
    ).filter(issues_t["repo_name"] == repo_name)

    return t


@reactive.calc
def commits_data(commits_t=commits_t):
    start_date, end_date = input.date_range()
    repo_name = input.repo_name()

    t = commits_t.filter(
        commits_t["committed_date"] >= start_date,
        commits_t["committed_date"] <= end_date,
    ).filter(commits_t["repo_name"] == repo_name)

    return t


@reactive.calc
def zulip_messages_data(zulip_messages_t=zulip_messages_t):
    start_date, end_date = input.date_range()

    t = zulip_messages_t.filter(
        zulip_messages_t["timestamp"] >= start_date,
        zulip_messages_t["timestamp"] <= end_date,
    )

    return t


@reactive.calc
def zulip_members_data(zulip_members_t=zulip_members_t):
    start_date, end_date = input.date_range()

    t = zulip_members_t.filter(
        zulip_members_t["date_joined"].cast("date") >= start_date,
        zulip_members_t["date_joined"].cast("date") <= end_date,
    )

    return t


def _update_date_range(days):
    start_date = datetime.now() - timedelta(days=days)
    end_date = datetime.now() + timedelta(days=1)
    ui.update_date_range(
        "date_range",
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_7d)
def _():
    _update_date_range(days=7)


@reactive.effect
@reactive.event(input.last_14d)
def _():
    _update_date_range(days=14)


@reactive.effect
@reactive.event(input.last_28d)
def _():
    _update_date_range(days=28)


@reactive.effect
@reactive.event(input.last_91d)
def _():
    _update_date_range(days=91)


@reactive.effect
@reactive.event(input.last_182d)
def _():
    _update_date_range(days=182)


@reactive.effect
@reactive.event(input.last_365d)
def _():
    _update_date_range(days=365)


@reactive.effect
@reactive.event(input.last_730d)
def _():
    _update_date_range(days=730)


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
