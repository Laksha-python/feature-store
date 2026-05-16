import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
DEFAULT_API_BASE = "http://localhost:8000"
API_BASE = os.environ.get("API_BASE", DEFAULT_API_BASE)
WINDOW_HOURS = 24
logging.basicConfig(level=logging.INFO)
st.set_page_config(
    layout="wide",
    page_title="Feature Store Control Plane",
    page_icon="🚀"
)
st.markdown("""
<style>
html, body, [class*="css"] {
    background-color: #0e1117;
    color: #e6edf3;
}

.main-title {
    font-size: 34px;
    font-weight: 700;
    margin-bottom: 5px;
}
.sub-title {
    color: #8b949e;
    margin-bottom: 20px;
}

.section-card {
    background-color: #161b22;
    padding: 20px;
    border-radius: 12px;
    border: 1px solid #30363d;
    margin-bottom: 25px;
}

.kpi-card {
    background-color: #161b22;
    padding: 18px;
    border-radius: 12px;
    border: 1px solid #30363d;
    text-align: center;
}

.metric-title {
    color: #8b949e;
    font-size: 13px;
}
.metric-value {
    font-size: 28px;
    font-weight: 700;
}

.health-good {
    color: #3fb950;
    font-weight: 600;
}

.health-bad {
    color: #f85149;
    font-weight: 600;
}

.sidebar-title {
    font-size: 20px;
    font-weight: 700;
}
</style>
""", unsafe_allow_html=True)
@st.cache_data(ttl=30)
def fetch(endpoint, default=None):
    try:
        response = requests.get(
            f"{API_BASE}{endpoint}",
            timeout=5
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"API Error [{endpoint}]: {e}")
        return default


def post(endpoint):
    try:
        response = requests.post(
            f"{API_BASE}{endpoint}",
            timeout=5
        )
        response.raise_for_status()
        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"POST Error [{endpoint}]: {e}")
        return False
with st.spinner("Loading platform data..."):
    metrics = fetch("/metrics", {}) or {}
    raw_events = fetch("/raw_events", []) or []
    users_list = fetch("/all-users", []) or []
    feature_catalog_response = fetch("/features", {}) or {}
    feature_catalog = feature_catalog_response.get("features", [])
if raw_events:
    df = pd.DataFrame(raw_events)
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(
            df["event_time"],
            errors="coerce"
        )
else:
    df = pd.DataFrame()
now = datetime.utcnow()
window_start = now - timedelta(hours=WINDOW_HOURS)
if not df.empty and "event_time" in df.columns:
    df_window = df[df["event_time"] >= window_start]
else:
    df_window = pd.DataFrame()

with st.sidebar:
    st.markdown('<div class="sidebar-title">🚀 Control Plane</div>', unsafe_allow_html=True)
    st.caption("Real-Time Feature Store")
    st.divider()
    st.write("### System Status")
    postgres_ok = fetch("/health/postgres") is not None
    redis_ok = fetch("/health/redis") is not None
    api_ok = fetch("/health") is not None
    st.markdown(
        f"PostgreSQL: {'🟢 Healthy' if postgres_ok else '🔴 Down'}"
    )
    st.markdown(
        f"Redis: {'🟢 Healthy' if redis_ok else '🔴 Down'}"
    )
    st.markdown(
        f"API: {'🟢 Healthy' if api_ok else '🔴 Down'}"
    )
    st.divider()
    if st.button("🔄 Refresh Dashboard"):
        st.cache_data.clear()
        st.rerun()
    if st.button("⚡ Trigger Backfill"):
        success = post("/trigger-pipeline")
        if success:
            st.success("Backfill Triggered")
        else:
            st.error("Backfill Failed")

st.markdown(
    '<div class="main-title">Feature Store Control Plane</div>',
    unsafe_allow_html=True
)
st.markdown(
    '<div class="sub-title">Streaming Data Platform • MLOps • Feature Governance</div>',
    unsafe_allow_html=True
)

col1, col2 = st.columns([6, 2])
with col1:
    st.caption("Production-inspired feature platform with Kafka, Redis, PostgreSQL, Airflow, and FastAPI")

with col2:
    st.info(f"Updated: {datetime.utcnow().strftime('%H:%M:%S UTC')}")

st.divider()
st.subheader("📈 Platform Metrics")

k1, k2, k3, k4, k5, k6 = st.columns(6)
def kpi(col, title, value):
    col.markdown(f'''
    <div class="kpi-card">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{value}</div>
    </div>
    ''', unsafe_allow_html=True)


events = metrics.get("events_processed_count", 0)
duplicates = metrics.get("duplicate_event_count", 0)
dlq_count = metrics.get("dlq_count", 0)
watermark = metrics.get("watermark_age_seconds", 0)
last_computed = metrics.get("last_computed_timestamp", "N/A")

error_rate = round((dlq_count / events * 100), 2) if events else 0

kpi(k1, "Events", events)
kpi(k2, "Duplicates", duplicates)
kpi(k3, "DLQ", dlq_count)
kpi(k4, "Error Rate %", error_rate)
kpi(k5, "Watermark Lag", watermark)
kpi(k6, "Last Computed", last_computed)

st.write("")


if df.empty:
    st.warning("No event data available.")
    st.stop()

if not users_list:
    st.warning("No users available.")
    st.stop()


left, right = st.columns(2)

with left:
    st.markdown(
        '<div class="section-card">',
        unsafe_allow_html=True
    )
    st.subheader("👤 User Analytics")
    if users_list:
        if isinstance(users_list[0], dict):
            user_ids = sorted({
                u["user_id"]
                for u in users_list
                if "user_id" in u
            })
        else:
            user_ids = sorted(users_list)

    else:
        user_ids = []

    if not user_ids:
        st.warning("No users available.")
    else:
        selected_user = st.selectbox(
            "Select User",
            user_ids,
            key="user_select"
        )

        user_events = df[
            df["user_id"] == selected_user
        ]
        if not user_events.empty:
            breakdown = (
                user_events["event_type"]
                .value_counts()
                .reset_index()
            )
            breakdown.columns = [
                "event_type",
                "count"
            ]
            fig = px.bar(
                breakdown,
                x="event_type",
                y="count",
                template="plotly_dark",
                title="User Event Distribution"
            )
            st.plotly_chart(
                fig,
                use_container_width=True
            )

        else:
            st.info("No events available for this user.")
        user_data = fetch(
            f"/users/{selected_user}",
            {}
        ) or {}

        features = user_data.get(
            "features",
            {}
        )

        c1, c2, c3 = st.columns(3)
        c1.metric(
            "7D Purchases",
            features.get(
                "rolling_7d_purchase_count",
                0
            )
        )
        c2.metric(
            "30D Spend",
            features.get(
                "rolling_30d_spend",
                0
            )
        )
        c3.metric(
            "Recency Days",
            features.get(
                "recency_days",
                0
            )
        )

    st.markdown(
        '</div>',
        unsafe_allow_html=True
    )
with right:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("📦 Product Analytics")
    if "product_id" in df.columns:
        product_ids = sorted(df["product_id"].dropna().unique())
    else:
        product_ids = []

    if product_ids:
        selected_product = st.selectbox(
            "Select Product",
            product_ids,
            key="product_select"
        )
        product_events = df[
            df["product_id"] == selected_product
        ]

        if not product_events.empty:
            breakdown = (
                product_events["event_type"]
                .value_counts()
                .reset_index()
            )

            breakdown.columns = ["event_type", "count"]

            fig = px.bar(
                breakdown,
                x="event_type",
                y="count",
                template="plotly_dark",
                title="Product Event Distribution"
            )

            st.plotly_chart(fig, use_container_width=True)
        sales = len(
            product_events[
                product_events["event_type"] == "purchase"
            ]
        )

        views = len(
            product_events[
                product_events["event_type"] == "view"
            ]
        )

        conversion_rate = round(
            (sales / views * 100),
            2
        ) if views else 0

        p1, p2, p3 = st.columns(3)

        p1.metric("1H Sales", sales)
        p2.metric("24H Sales", sales)
        p3.metric("Conversion Rate", conversion_rate)
    else:
        st.info("No product data available.")

    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("🔄 Interaction Funnel")
u_col, p_col = st.columns(2)
selected_u = u_col.selectbox(
    "User",
    user_ids,
    key="funnel_user"
)

selected_p = p_col.selectbox(
    "Product",
    product_ids,
    key="funnel_product"
)

interaction_df = df[
    (df["user_id"] == selected_u)
    & (df["product_id"] == selected_p)
]

views = len(
    interaction_df[
        interaction_df["event_type"] == "view"
    ]
)

cart = len(
    interaction_df[
        interaction_df["event_type"] == "add_to_cart"
    ]
)

purchases = len(
    interaction_df[
        interaction_df["event_type"] == "purchase"
    ]
)

funnel_df = pd.DataFrame({
    "stage": ["View", "Cart", "Purchase"],
    "count": [views, cart, purchases]
})

fig = px.funnel(
    funnel_df,
    x="count",
    y="stage",
    template="plotly_dark",
    title="User Conversion Funnel"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

st.markdown('</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-card">',
    unsafe_allow_html=True
)

st.subheader("📊 Feature Monitoring")

if not feature_catalog:
    st.info("No features available.")

else:
    selected_feature = st.selectbox(
        "Select Feature",
        feature_catalog
    )

    values = []
    for user in user_ids:
        response = fetch(
            f"/users/{user}",
            {}
        ) or {}
        feature_value = (
            response
            .get("features", {})
            .get(selected_feature)
        )
        if isinstance(feature_value, (int, float)):
            values.append(feature_value)
    if values:
        series = pd.Series(values)
        c1, c2 = st.columns(2)
        hist_fig = px.histogram(
            series,
            template="plotly_dark",
            title="Feature Distribution"
        )
        box_fig = px.box(
            series,
            template="plotly_dark",
            title="Feature Outliers"
        )
        c1.plotly_chart(
            hist_fig,
            use_container_width=True
        )
        c2.plotly_chart(
            box_fig,
            use_container_width=True
        )
    else:
        st.warning(
            "No numeric values available for this feature."
        )

st.markdown(
    '</div>',
    unsafe_allow_html=True
)

st.divider()
st.caption(
    "Built with FastAPI • Kafka • Redis • PostgreSQL • Airflow • Streamlit"
)