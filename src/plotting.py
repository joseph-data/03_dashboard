"""Plot helpers used by the Shiny app.

The app is responsible for data filtering and column selection; these helpers only
turn a prepared DataFrame into Plotly figures. If the input DataFrame is empty,
the helpers return an empty figure to keep the UI responsive.
"""

from __future__ import annotations

from typing import Sequence

import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure


def format_metric_value(value: float) -> str:
    if pd.isna(value):
        return "N/A"
    if 0 <= value <= 1:
        return f"{value:.0%}"
    return f"{value:.2f}"


def format_raw_value(value: float) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{value:.3f}"


def build_trend_plot(
    df: pd.DataFrame,
    *,
    metric_col: str,
    metric_label: str,
    title: str,
    order: Sequence[str] = (),
) -> Figure:
    """Line chart: metric over time for each occupation label."""
    if df.empty:
        return px.line()

    fig = px.line(
        df,
        x="year",
        y=metric_col,
        color="label",
        markers=True,
        category_orders={"label": list(order)},
        labels={
            "label": "Occupation",
            "year": "Year",
            metric_col: metric_label,
        },
    )
    fig.update_layout(
        hovermode="x unified",
        title=title,
        xaxis_showgrid=True,
        yaxis_showgrid=True,
        template="plotly_white",
    )
    return fig


def build_bar_plot(
    df: pd.DataFrame,
    *,
    percentile_col: str,
    raw_col: str,
    metric_label: str,
    title: str,
    order: Sequence[str] = (),
) -> Figure:
    """Horizontal bar chart: percentile ranks for the latest year."""
    if df.empty:
        return px.bar()

    # Always compare within the latest year available in the filtered data.
    latest = df["year"].max()
    latest_df = df[df["year"] == latest]

    order_list = list(order)
    if order_list:
        latest_df = latest_df.set_index("label").loc[order_list].reset_index()

    latest_df = latest_df.copy()
    latest_df["bar_label"] = latest_df.apply(
        lambda row: f"{format_raw_value(row[raw_col])} | {format_metric_value(row[percentile_col])}",
        axis=1,
    )

    fig = px.bar(
        latest_df,
        x=percentile_col,
        y="label",
        orientation="h",
        text="bar_label",
        category_orders={"label": order_list},
        labels={
            "label": "Occupation",
            percentile_col: f"{metric_label} (percentile rank)",
        },
    )
    fig.update_layout(
        title=title,
        xaxis_showgrid=True,
        yaxis_showgrid=True,
        template="plotly_white",
    )
    fig.update_traces(textposition="inside")
    fig.update_xaxes(range=[0, 1], tickformat=".0%")
    return fig
