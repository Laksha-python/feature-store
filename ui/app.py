
import streamlit as st
import requests

API_BASE_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="Feature Store UI", layout="centered")

st.title("🧱 Feature Store Platform")

st.header("📚 Feature Catalog")

try:
    response = requests.get(f"{API_BASE_URL}/features")
    response.raise_for_status()
    features = response.json()["features"]

    for feature in features:
        st.subheader(feature["name"])
        st.write(feature["description"])
        st.write(f"**Update frequency:** {feature['update_frequency']}")
        st.write(f"**Available stores:** {', '.join(feature['available_stores'])}")
        st.markdown("---")

except Exception as e:
    st.error("Failed to load feature catalog")


st.header("🔍 User Feature Lookup")

user_id = st.text_input("Enter user_id (e.g. user_1)")

if user_id:
    try:
        response = requests.get(f"{API_BASE_URL}/features/{user_id}")

        if response.status_code == 404:
            st.warning(f"No features found for user_id = {user_id}")
        else:
            response.raise_for_status()
            data = response.json()

            st.subheader(f"Features for {data['user_id']}")

            for name, value in data["features"].items():
                st.write(f"**{name}:** {value}")

            st.caption(f"Last updated at {data['computed_at']}")

    except Exception:
        st.error("Failed to fetch user features")
