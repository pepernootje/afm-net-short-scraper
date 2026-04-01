import sys
from pathlib import Path

# Make src/ importable when running via `streamlit run app/main.py`
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import streamlit as st

from app import charts, components, data

st.set_page_config(
    page_title="AFM Short Positions",
    page_icon="📉",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

if "page" not in st.session_state:
    st.session_state["page"] = "overview"
if "selected_isin" not in st.session_state:
    st.session_state["selected_isin"] = None

# ---------------------------------------------------------------------------
# Data loaded on every render (cached)
# ---------------------------------------------------------------------------

min_date, max_date = data.get_date_bounds()
all_issuers = data.get_all_issuers()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("AFM Short Positions")
    st.caption(f"Data as of {max_date:%d %b %Y}")

    labels = [f"{name} ({isin})" for name, isin in all_issuers]
    selected_label = st.selectbox("Jump to issuer", ["— select —"] + labels)
    if selected_label != "— select —":
        idx = labels.index(selected_label)
        st.session_state["selected_isin"] = all_issuers[idx][1]
        st.session_state["page"] = "detail"
        st.rerun()

    st.divider()

    if st.session_state["page"] == "detail":
        if st.button("← Back to overview"):
            st.session_state["page"] = "overview"
            st.session_state["selected_isin"] = None
            st.rerun()

# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def render_overview() -> None:
    st.title("Market overview")
    st.caption(f"Net short positions as reported to AFM — last updated {max_date:%d %b %Y}")

    overview = data.get_overview()

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Issuers with active shorts", len(overview))
    with m2:
        st.metric("Highest single total", f"{overview['total_net_short'].max():.2f}%")
    with m3:
        st.metric("Total active holders", int(overview["active_holders"].sum()))

    event = st.dataframe(
        overview,
        use_container_width=True,
        height=600,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "issuer_name":     st.column_config.TextColumn("Issuer"),
            "isin":            st.column_config.TextColumn("ISIN"),
            "total_net_short": st.column_config.NumberColumn("Net short %", format="%.2f%%"),
            "active_holders":  st.column_config.NumberColumn("Holders"),
            "peak_short":      st.column_config.NumberColumn("Peak %", format="%.2f%%"),
            "latest_date":     st.column_config.DateColumn("As of", format="DD MMM YYYY"),
        },
        key="overview_table",
    )

    rows = event.selection.rows
    if rows:
        st.session_state["selected_isin"] = overview.iloc[rows[0]]["isin"]
        st.session_state["page"] = "detail"
        st.rerun()


def render_detail(isin: str) -> None:
    overview = data.get_overview()
    row = overview[overview["isin"] == isin]

    if row.empty:
        st.error(f"No data found for ISIN {isin}.")
        return

    meta = row.iloc[0]
    st.title(meta["issuer_name"])
    st.caption(isin)

    components.metric_row(
        issuer_name=meta["issuer_name"],
        isin=isin,
        total_short=meta["total_net_short"],
        active_holders=int(meta["active_holders"]),
        peak_short=meta["peak_short"],
    )

    st.divider()

    date_from, date_to = components.date_filter(min_date, max_date)

    issuer_hist = data.get_issuer_history(isin, date_from, date_to)
    trader_hist = data.get_trader_history(isin, date_from, date_to)
    current_hold = data.get_current_holders(isin)

    if issuer_hist.empty:
        st.warning("No data available for this issuer in the selected date range.")
        return

    st.altair_chart(charts.total_short_line(issuer_hist), use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.altair_chart(charts.stacked_area(trader_hist), use_container_width=True)
    with col2:
        st.altair_chart(charts.holder_lines(trader_hist), use_container_width=True)

    _, center, _ = st.columns([1, 2, 1])
    with center:
        st.altair_chart(charts.holders_bar(current_hold), use_container_width=True)

    with st.expander("Raw trader positions"):
        st.dataframe(trader_hist, use_container_width=True)


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

if st.session_state["page"] == "overview":
    render_overview()
else:
    isin = st.session_state["selected_isin"]
    if isin:
        render_detail(isin)
    else:
        st.session_state["page"] = "overview"
        st.rerun()
