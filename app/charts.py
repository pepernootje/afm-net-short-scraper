import altair as alt
import pandas as pd

FONT = "system-ui, sans-serif"

_AXIS = {"labelFontSize": 12, "titleFontSize": 13}
_LEGEND = {"labelFontSize": 12, "titleFontSize": 13}
_TITLE = {"fontSize": 15, "fontWeight": "normal"}

_THEME = {
    "font": FONT,
    "axis": _AXIS,
    "legend": _LEGEND,
    "title": _TITLE,
}


def total_short_line(df: pd.DataFrame) -> alt.Chart:
    line = (
        alt.Chart(df)
        .mark_line(strokeWidth=2, color="#2563EB")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "total_net_short:Q",
                title="Total net short (%)",
                scale=alt.Scale(domainMin=0),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%d %b %Y"),
                alt.Tooltip("total_net_short:Q", title="Net short", format=".2f"),
                alt.Tooltip("active_holders:Q", title="Holders"),
            ],
        )
    )

    threshold = (
        alt.Chart(pd.DataFrame({"y": [0.5]}))
        .mark_rule(strokeDash=[4, 4], color="gray", opacity=0.5)
        .encode(y="y:Q")
    )

    return (
        (line + threshold)
        .properties(title="Total net short position over time")
        .configure(**_THEME)
    )


def stacked_area(df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_area()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "net_short_position:Q",
                title="Net short (%)",
                stack="zero",
            ),
            color=alt.Color(
                "position_holder:N",
                title="Position holder",
                scale=alt.Scale(scheme="tableau10"),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%d %b %Y"),
                alt.Tooltip("position_holder:N", title="Holder"),
                alt.Tooltip("net_short_position:Q", title="Position", format=".2f"),
            ],
        )
        .properties(title="Holder contributions (stacked)")
        .configure(**_THEME)
    )


def holder_lines(df: pd.DataFrame) -> alt.Chart:
    highlight = alt.selection_point(
        fields=["position_holder"], on="mouseover", nearest=True
    )

    base = alt.Chart(df).encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("net_short_position:Q", title="Net short (%)"),
        color=alt.Color(
            "position_holder:N",
            scale=alt.Scale(scheme="tableau10"),
            legend=None,
        ),
        opacity=alt.condition(highlight, alt.value(1.0), alt.value(0.2)),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%d %b %Y"),
            alt.Tooltip("position_holder:N", title="Holder"),
            alt.Tooltip("net_short_position:Q", title="Position", format=".2f"),
        ],
    )

    return (
        base.mark_line(strokeWidth=1.5)
        .add_params(highlight)
        .properties(title="Individual holder positions")
        .configure(**_THEME)
    )


def holders_bar(df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_bar(color="#2563EB")
        .encode(
            x=alt.X("net_short_position:Q", title="Net short (%)"),
            y=alt.Y(
                "position_holder:N",
                title=None,
                sort=alt.EncodingSortField(
                    field="net_short_position", order="descending"
                ),
            ),
            tooltip=[
                alt.Tooltip("position_holder:N", title="Holder"),
                alt.Tooltip("net_short_position:Q", title="Position", format=".2f"),
            ],
        )
        .properties(
            title="Current holders (most recent date)",
            height=max(150, len(df) * 35),
        )
        .configure(**_THEME)
    )
