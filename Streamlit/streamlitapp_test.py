# to run:
# cd Streamlit
# streamlit run streamlitapp_test.py

import streamlit as st
import pandas as pd

st.title("Retail Rocket Events Dashboard")

# =========================
# Load Data
# =========================
file_path = "../output.txt"
df = pd.read_json(file_path, lines=True)

st.header("Raw Data Preview")
st.dataframe(df.head())

# =========================
# Sidebar Filters
# =========================
st.sidebar.header("Search Filters")

visitor_id = st.sidebar.text_input("Visitor ID")
transaction_id = st.sidebar.text_input("Transaction ID")

# =========================
# Events by Visitor
# =========================
if visitor_id:
    df_visitor = df[df["visitorid"].astype(str) == visitor_id]

    st.header("Events by Visitor")

    if df_visitor.empty:
        st.warning("No events found for this visitor.")
    else:
        st.dataframe(df_visitor)

# =========================
# Events by Transaction
# =========================
if transaction_id:
    df_tx = df[df["transactionid"].astype(str) == transaction_id]

    st.header("Events by Transaction")

    if df_tx.empty:
        st.warning("No events found for this transaction.")
    else:
        st.dataframe(df_tx)

# =========================
# Event Distribution
# =========================
st.header("Event Distribution")

event_counts = df["event"].value_counts().reset_index()
event_counts.columns = ["Event", "Count"]

st.bar_chart(event_counts.set_index("Event"))

# =========================
# User Conversion Funnel
# =========================
st.header("User Conversion Funnel")

view_users = df[df["event"] == "view"]["visitorid"].nunique()
cart_users = df[df["event"] == "addtocart"]["visitorid"].nunique()
transaction_users = df[df["event"] == "transaction"]["visitorid"].nunique()

funnel_df = pd.DataFrame({
    "Stage": ["View", "Add to Cart", "Transaction"],
    "Unique Users": [view_users, cart_users, transaction_users]
})

st.subheader("Funnel Overview")
st.dataframe(funnel_df)

st.subheader("Funnel Visualization")
st.bar_chart(funnel_df.set_index("Stage"))

if view_users > 0:
    st.subheader("Conversion Rates")
    st.write(f"View → Cart: {cart_users / view_users:.2%}")
    st.write(
        f"Cart → Transaction: {transaction_users / cart_users:.2%}"
        if cart_users > 0 else
        "Cart → Transaction: N/A"
    )

# =========================
# User Activity Distribution
# =========================
st.header("User Activity Distribution")

user_activity = (
    df.groupby("visitorid")
      .size()
      .reset_index(name="events")
      .sort_values("events", ascending=False)
)

st.bar_chart(user_activity.head(20).set_index("visitorid"))

# =========================
# Top Purchased Items
# =========================
st.header("Top Purchased Items")

top_items = (
    df[df["event"] == "transaction"]
    .groupby("itemid")
    .size()
    .reset_index(name="transactions")
    .sort_values("transactions", ascending=False)
    .head(10)
)

st.bar_chart(top_items.set_index("itemid"))

# =========================
# Events by Hour of Day
# =========================
st.header("Events by Hour of Day")

df["timestamp"] = pd.to_datetime(df["timestamp"])
df["hour"] = df["timestamp"].dt.hour

hourly_counts = df.groupby("hour").size()

st.line_chart(hourly_counts)
