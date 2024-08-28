# imports
import ibis
import ibis.selectors as s

import plotly.express as px

# configure plotly
px.defaults.template = "plotly_dark"


# define metrics
def line(t: ibis.Table, x: str, y: str, **kwargs) -> px.line:
    t = t.order_by(x)

    c = px.line(
        t,
        x=x,
        y=y,
        **kwargs,
    )

    return c
