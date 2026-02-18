import streamlit as st
import requests
import pandas as pd
import plotly.express as px

API_BASE_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="Feature Store Dashboard",
    layout="wide"
)

st.markdown("""
<style>
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
}

h1 {
    font-weight: 600;
}

.metric-card {
    background-color: #f9fafb;
    padding: 20px;
    border-radius: 12px;
    border: 1px solid #e5e7eb;
}

.section-title {
    font-size: 20px;
    font-weight: 600;
    margin-top: 40px;
    margin-bottom: 10px;
}
</style>
""", unsafe_allow_html=True)

st.title("Feature Store Dashboard")

try:
    response = requests.get(f"{API_BASE_URL}/all-users")
    response.raise_for_status()
    df = pd.DataFrame(response.json())
except:
    st.error("Backend API unavailable.")
    st.stop()

if df.empty:
    st.warning("No feature data available.")
    st.stop()

st.markdown('<div class="section-title">Overview</div>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

col1.markdown(f"""
<div class="metric-card">
<h4>Total Users</h4>
<h2>{len(df)}</h2>
</div>
""", unsafe_allow_html=True)

col2.markdown(f"""
<div class="metric-card">
<h4>Avg Purchase Value</h4>
<h2>{round(df["user_avg_purchase_value_last_30d"].mean(), 2)}</h2>
</div>
""", unsafe_allow_html=True)

col3.markdown(f"""
<div class="metric-card">
<h4>Total Purchases (30d)</h4>
<h2>{int(df["user_purchase_count_last_30d"].sum())}</h2>
</div>
""", unsafe_allow_html=True)

col4.markdown(f"""
<div class="metric-card">
<h4>Total Events (7d)</h4>
<h2>{int(df["user_event_count_last_7d"].sum())}</h2>
</div>
""", unsafe_allow_html=True)

st.markdown('<div class="section-title">Feature Analytics</div>', unsafe_allow_html=True)

feature_options = [
    "user_purchase_count_last_30d",
    "user_event_count_last_7d",
    "user_avg_purchase_value_last_30d"
]

selected_feature = st.selectbox("Select Feature", feature_options)

col1, col2 = st.columns(2)

with col1:
    fig_hist = px.histogram(
        df,
        x=selected_feature,
        nbins=20,
        template="plotly_white",
        color_discrete_sequence=["#2563eb"]
    )
    fig_hist.update_layout(
        title="Distribution",
        margin=dict(l=10, r=10, t=40, b=10)
    )
    st.plotly_chart(fig_hist, use_container_width=True)

with col2:
    fig_box = px.box(
        df,
        y=selected_feature,
        template="plotly_white",
        color_discrete_sequence=["#4b5563"]
    )
    fig_box.update_layout(
        title="Outlier Analysis",
        margin=dict(l=10, r=10, t=40, b=10)
    )
    st.plotly_chart(fig_box, use_container_width=True)

st.markdown('<div class="section-title">User Ranking</div>', unsafe_allow_html=True)

sorted_df = df.sort_values(selected_feature, ascending=False)

st.dataframe(
    sorted_df[["user_id"] + feature_options],
    use_container_width=True,
    hide_index=True
)

st.markdown('<div class="section-title">User Lookup</div>', unsafe_allow_html=True)

user_id = st.selectbox("Select User", df["user_id"].unique())

user_row = df[df["user_id"] == user_id]

if not user_row.empty:
    st.table(user_row.set_index("user_id"))

