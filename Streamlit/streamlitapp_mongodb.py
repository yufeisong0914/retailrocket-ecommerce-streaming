# to run:
# cd Streamlit
# streamlit run streamlitapp_mongodb.py

import streamlit as st
from pandas import DataFrame, to_datetime
import pymongo

# =========================
# MongoDB Connection
# =========================
client = pymongo.MongoClient(
    "mongodb://localhost:27017/",
    username="root",
    password="example"
)

db = client["docstreaming"]
events_col = db["events"]

st.title("Retail Rocket Events Dashboard")

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
    cursor = events_col.find(
        {"visitorid": visitor_id},
        {"_id": 0}
    )
    df_visitor = DataFrame(cursor)

    st.header("Events by Visitor")

    if df_visitor.empty:
        st.warning("No events found for this visitor.")
    else:
        st.dataframe(df_visitor)

# =========================
# Events by Transaction
# =========================
if transaction_id:
    cursor = events_col.find(
        {"transactionid": transaction_id},
        {"_id": 0}
    )
    df_tx = DataFrame(cursor)

    st.header("Events by Transaction")

    if df_tx.empty:
        st.warning("No events found for this transaction.")
    else:
        st.dataframe(df_tx)

# =========================
# Event Distribution
# =========================
st.header("Event Distribution")

event_counts = list(events_col.aggregate([
    {"$group": {"_id": "$event", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]))

if not event_counts:
    st.warning("No event data available.")
else:
    df_event_dist = DataFrame(event_counts)
    df_event_dist.rename(columns={"_id": "Event"}, inplace=True)

    st.bar_chart(df_event_dist.set_index("Event"))

# =========================
# User Conversion Funnel
# =========================
st.header("User Conversion Funnel")

funnel_events = list(
    events_col.find(
        {},
        {"_id": 0, "visitorid": 1, "event": 1}
    )
)

df_funnel = DataFrame(funnel_events)

if df_funnel.empty:
    st.warning("No data available for funnel analysis.")
else:
    view_users = df_funnel[df_funnel["event"] == "view"]["visitorid"].nunique()
    cart_users = df_funnel[df_funnel["event"] == "addtocart"]["visitorid"].nunique()
    transaction_users = df_funnel[df_funnel["event"] == "transaction"]["visitorid"].nunique()

    funnel_df = DataFrame({
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

user_activity = events_col.aggregate([
    {"$group": {"_id": "$visitorid", "events": {"$sum": 1}}},
    {"$sort": {"events": -1}}
])

df_users = DataFrame(user_activity).rename(columns={"_id": "VisitorID"})

st.bar_chart(df_users.head(20).set_index("VisitorID"))

# =========================
# Top Purchased Items
# =========================
st.header("Top Purchased Items")

top_items = events_col.aggregate([
    {"$match": {"event": "transaction"}},
    {"$group": {"_id": "$itemid", "transactions": {"$sum": 1}}},
    {"$sort": {"transactions": -1}},
    {"$limit": 10}
])

df_items = DataFrame(top_items).rename(columns={"_id": "ItemID"})

st.bar_chart(df_items.set_index("ItemID"))

# =========================
# Events by Hour of Day
# =========================
st.header("Events by Hour of Day")

time_events = list(
    events_col.find(
        {},
        {"_id": 0, "timestamp": 1}
    )
)

df_time = DataFrame(time_events)

if not df_time.empty:
    df_time["timestamp"] = to_datetime(df_time["timestamp"])
    df_time["hour"] = df_time["timestamp"].dt.hour

    hourly_counts = df_time.groupby("hour").size()

    st.line_chart(hourly_counts)
else:
    st.warning("No timestamp data available.")
