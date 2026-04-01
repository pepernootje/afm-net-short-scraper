from datetime import date

import streamlit as st
from dateutil.relativedelta import relativedelta


def date_filter(min_date: date, max_date: date) -> tuple[date, date]:
    """Preset buttons + custom date pickers. Returns (date_from, date_to)."""

    if "date_from" not in st.session_state:
        st.session_state["date_from"] = max_date - relativedelta(months=6)
    if "date_to" not in st.session_state:
        st.session_state["date_to"] = max_date

    presets = {
        "1M": relativedelta(months=1),
        "3M": relativedelta(months=3),
        "6M": relativedelta(months=6),
        "1Y": relativedelta(years=1),
        "All": None,
    }

    cols = st.columns(len(presets) + 1)
    for col, (label, delta) in zip(cols, presets.items()):
        with col:
            if st.button(label, use_container_width=True):
                if delta is None:
                    st.session_state["date_from"] = min_date
                else:
                    st.session_state["date_from"] = max(
                        min_date, max_date - delta
                    )
                st.session_state["date_to"] = max_date

    with cols[-1]:
        st.write("")  # spacer

    picker_cols = st.columns(2)
    with picker_cols[0]:
        date_from = st.date_input(
            "From",
            value=st.session_state["date_from"],
            min_value=min_date,
            max_value=st.session_state["date_to"],
            key="date_from_picker",
        )
        st.session_state["date_from"] = date_from

    with picker_cols[1]:
        date_to = st.date_input(
            "To",
            value=st.session_state["date_to"],
            min_value=st.session_state["date_from"],
            max_value=max_date,
            key="date_to_picker",
        )
        st.session_state["date_to"] = date_to

    return date_from, date_to


def metric_row(
    issuer_name: str,
    isin: str,
    total_short: float,
    active_holders: int,
    peak_short: float,
) -> None:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total net short", f"{total_short:.2f}%")
    with col2:
        st.metric("Active holders", active_holders)
    with col3:
        st.metric("Peak short (all time)", f"{peak_short:.2f}%")
