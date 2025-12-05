from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
from faicons import icon_svg
from numpy import info
import pandas as pd
import plotly.express as px
from shiny import reactive, render
import shiny.ui as classic_ui
from shiny.express import input, ui
from shinywidgets import render_widget
from shinyswatch import theme

from scripts import run_pipeline  # type: ignore
from scripts import config

TAXONOMY_OPTIONS = config.TAXONOMY_OPTIONS
METRIC_OPTIONS: List[Tuple[str, str]] = config.METRIC_OPTIONS
WEIGHTING_OPTIONS: List[Tuple[str, str]] = config.WEIGHTING_OPTIONS

LEVEL_OPTIONS = config.LEVEL_OPTIONS
LEVEL_LABELS = {value: label for label, value in LEVEL_OPTIONS}
LEVEL_CHOICES = {str(value): label for label, value in LEVEL_OPTIONS}


def load_data() -> Dict[str, pd.DataFrame]:
    """Fetch raw + SCB data on the fly and build weighting outputs in-memory."""
    pipeline_results = run_pipeline()
    frames: Dict[str, pd.DataFrame] = {}

    for _, taxonomy in TAXONOMY_OPTIONS:
        payload = pipeline_results.get(taxonomy)
        if not payload:
            continue

        dfs = []
        weighted = payload.get("weighted")
        simple = payload.get("simple")

        if isinstance(weighted, pd.DataFrame) and not weighted.empty:
            tmp = weighted.copy()
            tmp["weighting"] = "emp_weighted"
            tmp["weighting_label"] = WEIGHTING_OPTIONS[0][0]
            dfs.append(tmp)

        if isinstance(simple, pd.DataFrame) and not simple.empty:
            tmp = simple.copy()
            tmp["weighting"] = "simple_avg"
            tmp["weighting_label"] = WEIGHTING_OPTIONS[1][0]
            dfs.append(tmp)

        if dfs:
            full = pd.concat(dfs, ignore_index=True)
            if "year" in full.columns:
                full["year"] = full["year"].astype(int)
            if "level" in full.columns:
                full["level"] = full["level"].astype(int)
            frames[taxonomy] = full

    if not frames:
        raise RuntimeError("No aggregated DAIOE datasets could be built in-memory.")
    return frames


DATA = load_data()

ALL_YEARS = sorted(
    {int(year) for frame in DATA.values() for year in frame["year"].unique()}
)
GLOBAL_YEAR_MIN = ALL_YEARS[0]
GLOBAL_YEAR_MAX = ALL_YEARS[-1]
DEFAULT_TAXONOMY = config.DEFAULT_TAXONOMY
DEFAULT_LEVEL = config.DEFAULT_LEVEL
DEFAULT_WEIGHTING = config.DEFAULT_WEIGHTING
DEFAULT_YEAR_RANGE = (GLOBAL_YEAR_MIN, GLOBAL_YEAR_MAX)
DEFAULT_SORT_DESC = config.DEFAULT_SORT_DESC
DEFAULT_LEVEL_CHOICE = str(DEFAULT_LEVEL)
DEFAULT_TOP_N = config.DEFAULT_TOP_N


def metric_mapping() -> Dict[str, str]:
    return {value: label for label, value in METRIC_OPTIONS}


def weighting_mapping() -> Dict[str, str]:
    return {value: label for label, value in WEIGHTING_OPTIONS}


def taxonomy_mapping() -> Dict[str, str]:
    return {value: label for label, value in TAXONOMY_OPTIONS}


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


@reactive.calc
def chart_title() -> str:
    """
    Shared chart title that captures the current metric, taxonomy, weighting,
    level, and the latest year available in the filtered data.
    """
    df = filtered_data()
    latest_year = int(df["year"].max()) if not df.empty else None

    metric_text = metric_label()
    weight_label = weighting_mapping().get(input.weighting(), input.weighting())
    taxonomy_label = taxonomy_mapping().get(input.taxonomy(), input.taxonomy())
    level_value = int(input.level())
    group_label = LEVEL_LABELS.get(level_value, f"Level {level_value}")

    base = f"{metric_text} ({weight_label}, {taxonomy_label}) — {group_label}"
    if latest_year is None:
        return base
    return f"{base} in {latest_year}"


# ---------------------------------------------------------------------------
# Sidebar UI
# ---------------------------------------------------------------------------

with ui.sidebar(open="open"):
    ui.img(
        src="lab.svg",  # because it's in www/
        style="max-width: 200px; margin-bottom: 8px;",
    )
    ui.input_radio_buttons(
        "taxonomy",
        "Taxonomy",
        taxonomy_mapping(),
        selected=DEFAULT_TAXONOMY,
    )
    ui.input_select(
        "level",
        "Level",
        LEVEL_CHOICES,
        selected=DEFAULT_LEVEL_CHOICE,
    )
    ui.input_select(
        "metric",
        "Sub-index",
        metric_mapping(),
        selected=METRIC_OPTIONS[0][1],
    )
    ui.input_select(
        "weighting",
        "Weighting",
        weighting_mapping(),
        selected=DEFAULT_WEIGHTING,
    )

    ui.input_slider(
        "year_range",
        "Year range",
        min=GLOBAL_YEAR_MIN,
        max=GLOBAL_YEAR_MAX,
        value=DEFAULT_YEAR_RANGE,
        step=1,
        sep="",
    )

    ui.input_slider(
        "top_n",
        "Occupations to display (0 = all)",
        min=0,
        max=30,
        value=DEFAULT_TOP_N,
        step=1,
    )

    ui.input_switch("sort_desc", "Sort descending", value=DEFAULT_SORT_DESC)
    ui.input_text("search", "Search by occupation", placeholder="e.g. statistician")
    with ui.popover(id="help_popover"):
        ui.input_action_button(
            "show_help",
            "Quick Guide",
            class_="btn btn-outline-primary btn-sm w-100",
            icon=icon_svg("circle-info"),
        )
        ui.markdown(
            """
#### **Quick Guide**

- **Taxonomy**: SSYK 2012 = current; SSYK 1996 = historic.
- **Level**: 4-digit shows individual occupations; 1-digit shows broad groups.
- **Sub-index**: Pick the DAIOE metric to visualize; chart titles reflect your selection.
- **Weighting**: Employment-weighted highlights labour-market impact; Simple average treats each occupation equally.
- **Years**: Use the slider; charts always use the latest year within the range.
- **Top N / Search**: Limit to the N highest values (0 shows all) and filter by occupation name; toggle sort direction.
- **Reading charts**: Hover lines for per-year values; bars display raw + percentile labels; value boxes show the most/least exposed in the latest year.
            """
        )


css_file = Path(__file__).parent / "css" / "theme.scss"

ui.include_css(css_file)

ui.page_opts(
    fillable=True,
    fillable_mobile=True,
    full_width=True,
    id="page",
    lang="en",
)


# ---------------------------------------------------------------------------
# Reactive helpers
# ---------------------------------------------------------------------------


@reactive.calc
def metric_name() -> str:
    return f"daioe_{input.metric()}"


@reactive.calc
def metric_label() -> str:
    return metric_mapping()[input.metric()]


@reactive.calc
def percentile_metric_name() -> str:
    return f"pct_rank_{input.metric()}"


@reactive.calc
def current_data() -> pd.DataFrame:
    """
    Base filtered dataset for the current taxonomy, weighting, and level.
    This is the 'structural' filter and is reused by downstream reactives.
    """
    taxonomy = input.taxonomy()
    if taxonomy not in DATA:
        return pd.DataFrame()

    df = DATA[taxonomy]

    level = int(input.level())
    weight = input.weighting()

    # Filter once here instead of repeatedly in downstream logic
    df = df[(df["weighting"] == weight) & (df["level"] == level)]

    return df


@reactive.calc
def filtered_data() -> pd.DataFrame:
    """
    Further filters current_data() by metric availability, year range,
    search term, and top_n selection.
    """
    df = current_data()
    if df.empty:
        return df

    metric_col = metric_name()

    # Keep only rows with valid metric values
    df = df.dropna(subset=[metric_col])

    # Year range filter
    year_min, year_max = input.year_range()
    df = df[(df["year"] >= year_min) & (df["year"] <= year_max)]

    # Search filter (occupation label in Swedish)
    search_term = input.search().strip().lower()
    if search_term:
        labels = df["label"].astype(str).str.lower()
        df = df[labels.str.contains(search_term, na=False)]

    if df.empty:
        return df

    # Top-N by latest year metric value
    latest_year = df["year"].max()
    latest_slice = df[df["year"] == latest_year].sort_values(
        metric_col,
        ascending=not input.sort_desc(),
    )

    top_n = input.top_n()
    if top_n > 0:
        latest_slice = latest_slice.head(top_n)

    keep_codes = latest_slice["code"].unique()
    df = df[df["code"].isin(keep_codes)]

    return df


@reactive.calc
def latest_order() -> List[str]:
    """
    Provides a consistent ordering of occupations (labels) based on
    the latest year and the chosen sort direction.
    This is shared by both the trend and bar plots.
    """
    df = filtered_data()
    if df.empty:
        return []

    metric_col = metric_name()
    latest_year = df["year"].max()
    ascending = not input.sort_desc()

    latest_slice = df[df["year"] == latest_year].sort_values(
        metric_col, ascending=ascending
    )

    return latest_slice["label"].tolist()


# ---------------------------------------------------------------------------
# Extremes (value boxes)
# ---------------------------------------------------------------------------
@reactive.calc
def latest_extremes() -> Dict[str, Dict[str, float | str]]:
    # Use all occupations (ignore top_n) but respect other filters
    df = current_data()
    if df.empty:
        return {}

    metric_col = metric_name()  # raw DAIOE index
    percentile_col = percentile_metric_name()
    df = df.dropna(subset=[metric_col, percentile_col])

    # Apply year range filter (but not top_n)
    year_min, year_max = input.year_range()
    df = df[(df["year"] >= year_min) & (df["year"] <= year_max)]

    # Apply search filter (consistent with main plots)
    search_term = input.search().strip().lower()
    if search_term:
        labels = df["label"].astype(str).str.lower()
        df = df[labels.str.contains(search_term, na=False)]

    if df.empty:
        return {}

    latest_year = df["year"].max()
    latest_df = df[df["year"] == latest_year]

    sorted_df = latest_df.sort_values(metric_col, ascending=False)
    top_row = sorted_df.iloc[0]
    bottom_row = sorted_df.iloc[-1]

    return {
        "year": int(latest_year),
        "most": {
            "label": str(top_row["label"]),
            "value": float(top_row[metric_col]),
            "percentile": float(top_row[percentile_col]),
        },
        "least": {
            "label": str(bottom_row["label"]),
            "value": float(bottom_row[metric_col]),
            "percentile": float(bottom_row[percentile_col]),
        },
    }


# ---------------------------------------------------------------------------
# Top summary boxes
# ---------------------------------------------------------------------------
with ui.layout_columns(col_widths=[6, 6], gap="9px"):

    @render.ui
    def most_exposed_box():
        info = latest_extremes()
        if not info:
            return ui.value_box(
                "Most exposed occupation",
                "No data in range",
                "Adjust filters to see values",
            )

        most = info["most"]
        year = info["year"]
        return classic_ui.value_box(
            "Most exposed occupation\n",
            ui.h4(most["label"]),
            f"{metric_label()} raw: {format_raw_value(most['value'])} | \n"
            f"percentile: {format_metric_value(most['percentile'])} (year {year})",
            theme="blue",
            fill=True,
        )

    @render.ui
    def least_exposed_box():
        info = latest_extremes()
        if not info:
            return ui.value_box(
                "Least exposed occupation",
                "No data in range",
                "Adjust filters to see values",
            )

        least = info["least"]
        year = info["year"]
        return classic_ui.value_box(
            "Least exposed occupation\n",
            ui.h4(least["label"]),
            f"{metric_label()} raw: {format_raw_value(least['value'])} | \n"
            f"percentile: {format_metric_value(least['percentile'])} (year {year})",
            theme="bg-gradient-orange-red",
            full_screen=True,
            fill=True,
        )


# ---------------------------------------------------------------------------
# Main UI cards & plots
# ---------------------------------------------------------------------------
with ui.card(full_screen=True, fill=True):
    ui.card_header("Trend by occupation")

    @render_widget
    def trend_plot():
        df = filtered_data()
        if df.empty:
            return px.line()

        metric_col = metric_name()
        fig = px.line(
            df,
            x="year",
            y=metric_col,
            color="label",
            markers=True,  # kept for identical look
            category_orders={"label": latest_order()},
            labels={
                "label": "Occupation",
                "year": "Year",
                metric_col: metric_label(),
            },
        )
        fig.update_layout(hovermode="x unified", title=chart_title())
        return fig


with ui.card(full_screen=True, fill=True):
    ui.card_header("Latest year comparison")

    @render_widget
    def bar_plot():
        df = filtered_data()
        if df.empty:
            return px.bar()

        metric_col = percentile_metric_name()
        raw_col = metric_name()
        latest = df["year"].max()
        latest_df = df[df["year"] == latest]

        order = latest_order()
        # Reorder rows to match the order list if not empty
        if order:
            latest_df = latest_df.set_index("label").loc[order].reset_index()

        latest_df = latest_df.copy()
        latest_df["bar_label"] = latest_df.apply(
            lambda row: f"{format_raw_value(row[raw_col])} | {format_metric_value(row[metric_col])}",
            axis=1,
        )

        fig = px.bar(
            latest_df,
            x=metric_col,
            y="label",
            orientation="h",
            text="bar_label",
            category_orders={"label": order},
            labels={
                "label": "Occupation",
                metric_col: f"{metric_label()} (percentile rank)",
            },
        )
        fig.update_layout(title=chart_title())
        fig.update_traces(textposition="inside")
        fig.update_xaxes(range=[0, 1], tickformat=".0%")
        return fig


if __name__ == "__main__":
    ui.run()
