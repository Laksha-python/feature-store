# coding: utf-8
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime, timedelta
import time

DEFAULT_API_BASE = "http://localhost:8000"
API_BASE = os.environ.get("API_BASE", DEFAULT_API_BASE)

st.set_page_config(layout="wide", page_title="Feature Store Control Plane")

# =====================================================
# 🎨 ENTERPRISE DARK THEME
# =====================================================

st.markdown("""
<style>
html, body, [class*="css"] {
    background-color: #0e1117;
    color: #e6edf3;
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

.header-bar {
    background: linear-gradient(135deg, #161b22 0%, #21262d 100%);
    padding: 20px 28px;
    border-radius: 12px;
    border: 1px solid #30363d;
    margin-bottom: 24px;
    box-shadow: 0 4px 8px rgba(0,0,0,0.16);
}

.kpi-strip {
    display: flex;
    gap: 14px;
    margin-bottom: 24px;
    overflow-x: auto;
}

.kpi-card {
    background-color: #161b22;
    padding: 18px;
    border-radius: 12px;
    border: 1px solid #30363d;
    min-width: 200px;
    flex: 1;
    box-shadow: 0 3px 6px rgba(0,0,0,0.12);
}

.kpi-card.healthy { border-left: 5px solid #238636; }
.kpi-card.warning { border-left: 5px solid #d29922; }
.kpi-card.critical { border-left: 5px solid #da3633; }

.section-card {
    background-color: #161b22;
    padding: 24px;
    border-radius: 14px;
    border: 1px solid #30363d;
    margin-bottom: 28px;
    box-shadow: 0 4px 10px rgba(0,0,0,0.14);
}

.metric-title {
    font-size: 12px;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin-bottom: 8px;
}

.metric-value {
    font-size: 26px;
    font-weight: 700;
    color: #e6edf3;
    font-family: 'Courier New', monospace;
}

.metric-subtitle {
    font-size: 11px;
    color: #8b949e;
    margin-top: 6px;
}

.status-indicator {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    margin-right: 8px;
}

.status-healthy { background-color: #238636; }
.status-warning { background-color: #d29922; }
.status-critical { background-color: #da3633; }

.control-panel {
    background-color: #21262d;
    padding: 16px;
    border-radius: 12px;
    border: 1px solid #30363d;
    margin-bottom: 16px;
}

.stButton>button {
    background-color: #238636;
    color: white;
    border: none;
    padding: 10px 18px;
    border-radius: 8px;
    font-weight: 600;
}

.stButton>button:hover {
    background-color: #2ea043;
}

.stTabs [data-baseweb="tab-list"] {
    background-color: #161b22;
    border-radius: 10px 10px 0 0;
}

.stTabs [data-baseweb="tab"] {
    background-color: #21262d;
    color: #e6edf3;
}

.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background-color: #30363d;
    border-bottom: 2px solid #58a6ff;
}

.pr-status {
    font-size: 14px;
    font-weight: 700;
    margin-top: 8px;
}

.table-card {
    background-color: #0f1319;
    padding: 16px;
    border-radius: 12px;
    border: 1px solid #30363d;
}
</style>
""", unsafe_allow_html=True)

# =====================================================
# 🔌 API FUNCTIONS
# =====================================================

def _safe_get(path, default, timeout=5):
    try:
        r = requests.get(f"{API_BASE}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return default


def _safe_post(path, timeout=30):
    try:
        r = requests.post(f"{API_BASE}{path}", timeout=timeout)
        r.raise_for_status()
        return True, r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
    except Exception:
        return False, {}


@st.cache_data(ttl=15)
def fetch_health():
    return _safe_get("/health", {"status": "down"}).get("status") == "healthy"


@st.cache_data(ttl=15)
def fetch_metrics():
    fallback = {
        "events_processed_count": 0,
        "duplicate_event_count": 0,
        "dlq_count": 0,
        "watermark_age_seconds": 0,
        "last_computed": "unknown"
    }
    return _safe_get("/metrics", fallback)


@st.cache_data(ttl=60)
def fetch_users():
    return _safe_get("/all-users", []) or []


@st.cache_data(ttl=60)
def fetch_products():
    return _safe_get("/products", []) or []


@st.cache_data(ttl=30)
def fetch_events(limit=1000):
    return _safe_get(f"/raw_events?limit={limit}", []) or []


@st.cache_data(ttl=60)
def fetch_features():
    return _safe_get("/features", {}).get("features", []) or []


def fetch_user_features(user_id):
    return _safe_get(f"/users/{user_id}", {"features": {}})


def fetch_product_features(product_id):
    return _safe_get(f"/products/{product_id}", {"features": {}})


def trigger_pipeline():
    return _safe_post("/trigger-pipeline", timeout=30)


def _status_level(value, warning_threshold=None, critical_threshold=None, invert=False):
    if value is None:
        return "warning"
    if invert:
        if critical_threshold is not None and value <= critical_threshold:
            return "critical"
        if warning_threshold is not None and value <= warning_threshold:
            return "warning"
        return "healthy"
    if critical_threshold is not None and value >= critical_threshold:
        return "critical"
    if warning_threshold is not None and value >= warning_threshold:
        return "warning"
    return "healthy"


def format_count(value):
    try:
        return f"{int(value):,}"
    except Exception:
        return "0"


# =====================================================
# 🎛️ STATE MANAGEMENT
# =====================================================

if "live_mode" not in st.session_state:
    st.session_state.live_mode = False

if "pipeline_running" not in st.session_state:
    st.session_state.pipeline_running = False

if "last_backfill" not in st.session_state:
    st.session_state.last_backfill = None

# =====================================================
# 📊 DATA LOADING
# =====================================================

with st.spinner("Loading control plane data..."):
    health_ok = fetch_health()
    metrics = fetch_metrics()
    users_list = fetch_users()
    products_list = fetch_products()
    raw_events = fetch_events()
    feature_catalog = fetch_features()

# Build event DataFrame
if raw_events and isinstance(raw_events, list):
    df = pd.DataFrame(raw_events)
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
        df = df.dropna(subset=["event_time"])
else:
    df = pd.DataFrame()

# =====================================================
# 🏗️ HEADER
# =====================================================

st.markdown('<div class="header-bar">', unsafe_allow_html=True)
col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])

with col1:
    st.title("🎯 Feature Store Control Plane")
    st.caption("Real-time Feature Engineering & Serving Platform")

with col2:
    health_label = "✅ Online" if health_ok else "❌ Offline"
    state = "healthy" if health_ok else "critical"
    st.markdown(
        f"""
        <div style='text-align:center;'>
            <span class='status-indicator status-{state}'></span>
            <div style='font-size:12px;color:#8b949e;'>API Health</div>
            <div style='font-size:14px;font-weight:700;'>{health_label}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col3:
    pipeline_state = "Running" if st.session_state.pipeline_running else "Idle"
    pipeline_color = "warning" if pipeline_state == "Running" else "healthy"
    st.markdown(
        f"""
        <div style='text-align:center;'>
            <span class='status-indicator status-{pipeline_color}'></span>
            <div style='font-size:12px;color:#8b949e;'>Pipeline State</div>
            <div style='font-size:14px;font-weight:700;'>{pipeline_state}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col4:
    if st.button("🚀 Run Backfill", use_container_width=True):
        with st.spinner("Triggering backfill..."):
            success, payload = trigger_pipeline()
            if success:
                st.success("✅ Backfill pipeline requested")
                st.session_state.pipeline_running = True
                st.session_state.last_backfill = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                st.error("❌ Backfill trigger failed")

with col5:
    live_mode = st.checkbox("🔄 Live Mode", value=st.session_state.live_mode)
    if live_mode != st.session_state.live_mode:
        st.session_state.live_mode = live_mode
    if st.session_state.live_mode:
        time.sleep(5)
        try:
            st.experimental_rerun()
        except Exception:
            st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# 📈 SYSTEM OVERVIEW
# =====================================================

st.markdown('<div class="kpi-strip">', unsafe_allow_html=True)

overview_metrics = [
    ("events_processed_count", "Events Processed", "Total events ingested", None, None),
    ("duplicate_event_count", "Duplicate Events", "Duplicate events detected", 1, None),
    ("dlq_count", "Dead Letter Queue", "Events in DLQ", 1, 10),
    ("watermark_age_seconds", "Watermark Lag", "Seconds behind ingestion", 60, 300),
]

for key, title, subtitle, warn, crit in overview_metrics:
    value = metrics.get(key, 0)
    status = _status_level(value, warning_threshold=warn, critical_threshold=crit)
    label = format_count(value)
    if key == "watermark_age_seconds":
        label = f"{value}s"

    st.markdown(
        f"""
        <div class='kpi-card {status}'>
            <div class='metric-title'>{title}</div>
            <div class='metric-value'>{label}</div>
            <div class='metric-subtitle'>{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

last_computed = metrics.get("last_computed", "unknown")
status = "healthy" if last_computed != "unknown" else "warning"
st.markdown(
    f"""
    <div class='kpi-card {status}'>
        <div class='metric-title'>Last Computed</div>
        <div class='metric-value'>{last_computed}</div>
        <div class='metric-subtitle'>Latest feature computation time</div>
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# 📊 DASHBOARD TABS
# =====================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "👤 User Analytics",
    "📦 Product Analytics",
    "🔄 Interaction Explorer",
    "📈 Feature Monitoring",
    "⚙️ Pipeline Control"
])

# =====================================================
# 👤 USER ANALYTICS
# =====================================================

with tab1:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("👤 User Analytics")

    if users_list:
        selected_user = st.selectbox(
            "Select User ID",
            users_list,
            key="user_select",
            help="Choose a user to inspect features and behaviors"
        )

        user_data = fetch_user_features(selected_user)
        user_features = user_data.get("features", {})

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("7D Purchase Count", user_features.get("rolling_7d_purchase_count", 0))
        col2.metric("30D Spend", f"${user_features.get('rolling_30d_spend', 0):,.2f}")
        col3.metric("Recency Days", user_features.get("recency_days", 0))
        col4.metric("30D Net Revenue", f"${user_features.get('net_revenue_30d', 0):,.2f}")

        if not df.empty:
            user_events = df[df["user_id"] == selected_user]
            if not user_events.empty:
                chart_col1, chart_col2 = st.columns(2)

                with chart_col1:
                    breakdown = user_events["event_type"].value_counts().reset_index()
                    breakdown.columns = ["event_type", "count"]
                    fig = px.bar(
                        breakdown,
                        x="event_type",
                        y="count",
                        title="Event Breakdown",
                        template="plotly_dark",
                        color="event_type",
                        color_discrete_map={"view": "#58a6ff", "purchase": "#238636", "refund": "#da3633"}
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with chart_col2:
                    recent = user_events[user_events["event_time"] >= (datetime.now() - timedelta(days=7))]
                    if not recent.empty:
                        timeline = recent.groupby(pd.Grouper(key="event_time", freq="D")).size().reset_index(name="events")
                        fig = px.line(
                            timeline,
                            x="event_time",
                            y="events",
                            title="Activity Timeline (7D)",
                            template="plotly_dark"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No events for this user in the last 7 days.")

                st.markdown("### Insights")
                insights = []
                recency = user_features.get("recency_days", 0)
                if recency > 30:
                    insights.append("⚠️ User inactive for over 30 days.")
                elif recency > 7:
                    insights.append("⚠️ User inactive for over 7 days.")

                if user_features.get("rolling_30d_spend", 0) > 1000:
                    insights.append("💎 High-value customer detected.")

                if user_features.get("net_revenue_30d", 0) < 0:
                    insights.append("🚨 Net revenue is negative due to refunds.")

                if insights:
                    for insight in insights:
                        st.write(insight)
                else:
                    st.write("✅ User behavior looks stable.")
            else:
                st.info("No raw event records found for this user.")
        else:
            st.warning("No raw event data is available.")
    else:
        st.warning("No users are available from the backend.")

    st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# 📦 PRODUCT ANALYTICS
# =====================================================

with tab2:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("📦 Product Analytics")

    available_products = products_list if products_list else (
        sorted(df["product_id"].dropna().unique()) if not df.empty and "product_id" in df.columns else []
    )

    if available_products:
        selected_product = st.selectbox(
            "Select Product ID",
            available_products,
            key="product_select",
            help="Choose a product to inspect feature health and behavior"
        )

        product_data = fetch_product_features(selected_product)
        product_features = product_data.get("features", {})

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("1H Sales", product_features.get("rolling_1h_sales", 0))
        col2.metric("24H Sales", product_features.get("rolling_24h_sales", 0))
        col3.metric("Conversion Rate", f"{product_features.get('conversion_rate', 0):.1%}")
        col4.metric("Refund Rate", f"{product_features.get('refund_rate', 0):.1%}")

        if not df.empty and "product_id" in df.columns:
            product_events = df[df["product_id"] == selected_product]
            if not product_events.empty:
                chart_col1, chart_col2 = st.columns(2)

                with chart_col1:
                    breakdown = product_events["event_type"].value_counts().reset_index()
                    breakdown.columns = ["event_type", "count"]
                    fig = px.bar(
                        breakdown,
                        x="event_type",
                        y="count",
                        title="Event Breakdown",
                        template="plotly_dark",
                        color="event_type",
                        color_discrete_map={"view": "#58a6ff", "purchase": "#238636", "refund": "#da3633"}
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with chart_col2:
                    recent_sales = product_events[
                        (product_events["event_time"] >= (datetime.now() - timedelta(hours=24))) &
                        (product_events["event_type"] == "purchase")
                    ]
                    if not recent_sales.empty:
                        velocity = recent_sales.groupby(pd.Grouper(key="event_time", freq="H")).size().reset_index(name="sales")
                        fig = px.line(
                            velocity,
                            x="event_time",
                            y="sales",
                            title="Sales Velocity (24H)",
                            template="plotly_dark"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No purchases for this product in the last 24 hours.")

                st.markdown("### Insights")
                insights = []
                refund_rate = product_features.get("refund_rate", 0)
                conversion_rate = product_features.get("conversion_rate", 0)

                if refund_rate > 0.10:
                    insights.append("🚨 High refund rate affecting product health.")
                elif refund_rate > 0.05:
                    insights.append("⚠️ Medium refund rate detected.")

                if conversion_rate < 0.01:
                    insights.append("⚠️ Conversion rate is critically low.")
                elif conversion_rate < 0.05:
                    insights.append("⚠️ Conversion rate is below expectations.")

                if product_features.get("rolling_24h_sales", 0) == 0:
                    insights.append("📉 No sales recorded in the last 24 hours.")

                if insights:
                    for insight in insights:
                        st.write(insight)
                else:
                    st.write("✅ Product feature performance is healthy.")
            else:
                st.info("No raw event records found for this product.")
        else:
            st.warning("No raw event data is available.")
    else:
        st.warning("No products are available from the backend.")

    st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# 🔄 INTERACTION EXPLORER
# =====================================================

with tab3:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("🔄 Interaction Explorer")

    col1, col2, col3 = st.columns(3)
    with col1:
        filter_user = st.selectbox("User ID (optional)", ["All"] + users_list, key="filter_user")
    with col2:
        filter_product = st.selectbox(
            "Product ID (optional)",
            ["All"] + available_products,
            key="filter_product"
        )
    with col3:
        filter_event = st.selectbox("Event Type", ["All", "view", "purchase", "refund"], key="filter_event")

    filtered_df = df.copy() if not df.empty else pd.DataFrame()
    if not filtered_df.empty:
        if filter_user != "All":
            filtered_df = filtered_df[filtered_df["user_id"] == filter_user]
        if filter_product != "All":
            filtered_df = filtered_df[filtered_df["product_id"] == filter_product]
        if filter_event != "All":
            filtered_df = filtered_df[filtered_df["event_type"] == filter_event]

    if not filtered_df.empty:
        cols = st.columns([2, 1])
        with cols[0]:
            stage_counts = [
                {"stage": "view", "count": len(filtered_df[filtered_df["event_type"] == "view"])},
                {"stage": "add_to_cart", "count": len(filtered_df[filtered_df["event_type"] == "add_to_cart"])},
                {"stage": "purchase", "count": len(filtered_df[filtered_df["event_type"] == "purchase"])},
            ]
            funnel = pd.DataFrame(stage_counts)
            fig = px.funnel(funnel, x="count", y="stage", title="Behavior Funnel", template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

        with cols[1]:
            interaction_freq = filtered_df.groupby("user_id").size().reset_index(name="interactions")
            fig = px.histogram(
                interaction_freq,
                x="interactions",
                title="Interaction Frequency per User",
                template="plotly_dark",
                nbins=20
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Derived Metrics")
        total_events = len(filtered_df)
        total_views = len(filtered_df[filtered_df["event_type"] == "view"])
        total_purchases = len(filtered_df[filtered_df["event_type"] == "purchase"])
        unique_users = filtered_df["user_id"].nunique()
        conversion_rate = total_purchases / total_views if total_views else 0
        drop_off = 1 - conversion_rate if total_views else 0
        avg_interactions = total_events / unique_users if unique_users else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events", format_count(total_events))
        col2.metric("Conversion Rate", f"{conversion_rate:.1%}")
        col3.metric("Drop-off Rate", f"{drop_off:.1%}")
        col4.metric("Avg Interactions/User", f"{avg_interactions:.1f}")

        st.markdown("### Filtered Event Sample")
        display_df = filtered_df.sort_values(by="event_time", ascending=False).head(30)
        if "event_time" in display_df.columns:
            display_df = display_df.copy()
            display_df["event_time"] = display_df["event_time"].dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(display_df.reset_index(drop=True), use_container_width=True)
    else:
        st.info("No events match the selected filters. Adjust filters or verify ingestion.")

    st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# 📈 FEATURE MONITORING
# =====================================================

with tab4:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("📈 Feature Monitoring")

    if feature_catalog:
        selected_feature = st.selectbox(
            "Select Feature",
            feature_catalog,
            key="feature_monitor_select",
            help="Choose a feature to inspect distribution and drift"
        )

        feature_values = []
        for user_id in users_list:
            user_payload = fetch_user_features(user_id)
            val = user_payload.get("features", {}).get(selected_feature)
            if val is not None:
                feature_values.append(val)

        if feature_values:
            series = pd.Series(feature_values)
            chart_col, stat_col = st.columns([2, 1])

            with chart_col:
                fig = px.histogram(
                    series,
                    title=f"Distribution of {selected_feature}",
                    template="plotly_dark",
                    nbins=30
                )
                st.plotly_chart(fig, use_container_width=True)

            with stat_col:
                mean_val = series.mean()
                std_val = series.std()
                min_val = series.min()
                max_val = series.max()
                null_pct = series.isnull().mean() * 100
                drift_state = "✅ Stable"
                if std_val > abs(mean_val) * 0.5:
                    drift_state = "⚠️ High variance"
                if null_pct > 20:
                    drift_state = "⚠️ High null rate"

                st.markdown("### Statistics")
                st.write(f"**Mean:** {mean_val:.2f}")
                st.write(f"**Std Dev:** {std_val:.2f}")
                st.write(f"**Min:** {min_val:.2f}")
                st.write(f"**Max:** {max_val:.2f}")
                st.write(f"**Null %:** {null_pct:.1f}%")
                st.write(f"**Drift:** {drift_state}")

                if drift_state != "✅ Stable":
                    st.warning("Feature behavior warrants follow-up investigation.")
                else:
                    st.success("Feature distribution is stable.")
        else:
            st.info("No feature values available for the selected feature.")
    else:
        st.warning("No features were returned by the backend.")

    st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# ⚙️ PIPELINE CONTROL
# =====================================================

with tab5:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("⚙️ Pipeline Control Panel")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("### Current Pipeline Status")
        st.write(f"**State:** {pipeline_state}")
        st.write(f"**Last Backfill:** {st.session_state.last_backfill or 'Not started'}")
        st.write(f"**Last Computed:** {last_computed}")

        if st.session_state.pipeline_running:
            st.warning("Pipeline is currently running. Monitoring in progress...")
        else:
            st.success("Pipeline is idle and ready for new jobs.")

    with col2:
        st.markdown("### Run Operations")
        if st.button("Run Full Backfill", use_container_width=True):
            with st.spinner("Submitting full backfill job..."):
                success, payload = trigger_pipeline()
                if success:
                    st.success("✅ Full backfill job submitted")
                    st.session_state.pipeline_running = True
                    st.session_state.last_backfill = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                else:
                    st.error("❌ Failed to submit full backfill job")

    st.markdown("### Operational Notes")
    st.write(
        "This control plane panel is used for pipeline orchestration and troubleshooting. "
        "Use the metrics above to validate ingestion health and feature freshness."
    )
    st.markdown("---")
    st.write("**Recent logs**")
    st.code(
        """
[INFO] System health check passed.
[INFO] Feature ingestion completed.
[WARN] Sample DLQ alert threshold reached.
[INFO] Backfill triggered by control plane.
        """
    )
    st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# 🔍 DEBUG PANEL
# =====================================================

with st.expander("Show Debug Information"):
    st.write({
        "api_health": health_ok,
        "metrics": metrics,
        "users_count": len(users_list),
        "products_count": len(products_list),
        "features_count": len(feature_catalog),
        "raw_events_count": len(raw_events) if raw_events else 0,
    })
    st.write("Raw event schema:", df.columns.tolist())

# =====================================================
# ℹ️ RUN INSTRUCTIONS
# =====================================================

st.markdown("---")
st.markdown("**To run the dashboard:**")
st.code("streamlit run app.py")
