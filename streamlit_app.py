"""City Energy Observatory dashboard."""

import streamlit as st
import plotly.express as px

from utils.city_energy import (
    build_kpis,
    load_source_data,
    monthly_consumption,
    quality_summary,
    standardize_meters,
    standardize_readings,
)


st.set_page_config(page_title="City Energy Observatory", page_icon="⚡", layout="wide")
st.markdown(
    """
    <style>
    .main-header { font-size: 2.4rem; font-weight: 700; color: #12343b; }
    .subtle { color: #547078; font-size: 1.05rem; }
    [data-testid="stMetric"] { border-top: 3px solid #e07a5f; padding-top: .6rem; }
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown('<div class="main-header">City Energy Observatory</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtle">Collect, standardize, analyze, and visualize urban energy data.</div>',
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Data catalog")
    city = st.selectbox("City", ["Reference dataset", "City 2", "City 3", "City 4"])
    view = st.radio("View", ["Overview", "Data quality", "Catalog readiness"])
    st.caption("The checked-in IESCO extract is the reference source. Every new connector should publish the same canonical tables.")


@st.cache_data
def load_reference_data():
    source = load_source_data()
    if "meters" not in source or "readings" not in source:
        return None
    return standardize_meters(source["meters"]), standardize_readings(source["readings"])


data = load_reference_data()
if data is None:
    st.error("No meter and reading extracts were found. Add meters_*.csv and readings_*.csv or run an ingestion connector.")
    st.stop()

meters, readings = data
kpis = build_kpis(meters, readings)

if view == "Overview":
    st.caption(f"{city} | canonical energy observations | source: CSV reference extract")
    columns = st.columns(4)
    columns[0].metric("Meters", f"{kpis['meter_count']:,}")
    columns[1].metric("Consumption", f"{kpis['consumption_kwh']:,.0f} kWh")
    columns[2].metric("Readings", f"{kpis['reading_count']:,}")
    columns[3].metric("Quality rate", f"{kpis['quality_rate']:.1f}%")

    monthly = monthly_consumption(readings)
    if not monthly.empty:
        st.subheader("Consumption trend")
        st.plotly_chart(
            px.line(monthly, x="year_month", y="consumption_kwh", markers=True, labels={"year_month": "Month", "consumption_kwh": "kWh"}),
            use_container_width=True,
        )
    left, right = st.columns(2)
    with left:
        st.subheader("Asset footprint")
        asset_columns = [column for column in ["meter_number", "district", "division", "latitude", "longitude", "status"] if column in meters]
        st.dataframe(meters[asset_columns], hide_index=True, use_container_width=True)
    with right:
        st.subheader("Latest observations")
        st.dataframe(readings.sort_values("timestamp", ascending=False).head(12), hide_index=True, use_container_width=True)
elif view == "Data quality":
    st.subheader("Data quality monitor")
    st.dataframe(quality_summary(readings), hide_index=True, use_container_width=True)
    st.metric("Retained observations", f"{kpis['reading_count']:,}")
else:
    st.subheader("Four-city integration catalog")
    st.dataframe(
        [
            {"city": "Reference dataset", "source": "CSV extract", "cadence": "Daily or monthly", "status": "Available"},
            {"city": "City 2", "source": "REST API", "cadence": "To configure", "status": "Connector pending"},
            {"city": "City 3", "source": "Object storage", "cadence": "To configure", "status": "Connector pending"},
            {"city": "City 4", "source": "Data Catalog upload", "cadence": "To configure", "status": "Connector pending"},
        ],
        hide_index=True,
        use_container_width=True,
    )
    st.info("Connectors must emit canonical meter, reading, location, and metadata contracts before loading PostgreSQL.")

st.divider()
st.caption("Pipeline: source connectors -> Airflow -> validation -> PostgreSQL -> Superset / Streamlit")
