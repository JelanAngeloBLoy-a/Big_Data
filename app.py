
"""
Streaming Data Dashboard
Weather Data Analytics - Kafka + MongoDB
"""

import json
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
from pymongo import MongoClient


# --------------------------------------------------
# Page configuration
# --------------------------------------------------

st.set_page_config(
    page_title="Weather Streaming Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# --------------------------------------------------
# Sidebar configuration
# --------------------------------------------------

def setup_sidebar():
    st.sidebar.title("Dashboard Controls")

    st.sidebar.subheader("Data Source Configuration")

    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092"
    )

    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="weather-data"
    )

    st.sidebar.subheader("Storage Configuration")

    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["MongoDB", "HDFS"],
        index=0
    )

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }


# --------------------------------------------------
# Kafka consumer
# --------------------------------------------------

def consume_kafka_data(config):
    """
    Consume real weather messages from Kafka.

    Keep received records in Streamlit session state so
    the live table does not reset on every refresh.
    """

    kafka_broker = config.get(
        "kafka_broker", "localhost:9092"
    )
    kafka_topic = config.get(
        "kafka_topic", "weather-data"
    )

    consumer_key = (
        f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    )
    records_key = (
        f"weather_records_{kafka_broker}_{kafka_topic}"
    )

    if records_key not in st.session_state:
        st.session_state[records_key] = {}

    if consumer_key not in st.session_state:
        try:
            st.session_state[consumer_key] = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda message: json.loads(
                    message.decode("utf-8")
                ),
                consumer_timeout_ms=1000
            )
        except Exception as e:
            st.session_state[consumer_key] = None
            st.error(f"Kafka connection failed: {e}")

    consumer = st.session_state[consumer_key]

    if consumer is not None:
        try:
            # Poll briefly instead of waiting several seconds
            # on every Streamlit rerun.
            message_batches = consumer.poll(timeout_ms=500)

            for messages in message_batches.values():
                for message in messages:
                    data = message.value

                    if not isinstance(data, dict):
                        continue

                    timestamp_text = data.get("timestamp")

                    # Support both weather and template formats.
                    temperature = data.get(
                        "temperature_c",
                        data.get("value")
                    )

                    if timestamp_text is None or temperature is None:
                        continue

                    try:
                        timestamp = datetime.fromisoformat(
                            str(timestamp_text).replace(
                                "Z", "+00:00"
                            )
                        )

                        record = {
                            "timestamp": timestamp,
                            "value": float(temperature),
                            "metric_type": data.get(
                                "metric_type", "temperature"
                            ),
                            "sensor_id": data.get(
                                "sensor_id",
                                data.get("city", "unknown")
                            ),
                            "city": data.get(
                                "city",
                                data.get("location", "Unknown")
                            ),
                            "unit": data.get("unit", "celsius"),
                            "source": data.get("source", "Kafka")
                        }

                        # Keep one copy per sensor/city, timestamp,
                        # and metric to avoid repeated entries.
                        record_key = "|".join([
                            str(record["city"]),
                            str(record["sensor_id"]),
                            timestamp.isoformat(),
                            str(record["metric_type"])
                        ])

                        st.session_state[records_key][
                            record_key
                        ] = record

                    except (ValueError, TypeError):
                        continue

        except Exception as e:
            st.warning(f"Could not read new Kafka messages: {e}")

    records = list(st.session_state[records_key].values())

    if not records:
        return pd.DataFrame(
            columns=[
                "timestamp", "value", "metric_type",
                "sensor_id", "city", "unit", "source"
            ]
        )

    return (
        pd.DataFrame(records)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


# --------------------------------------------------
# Historical data from MongoDB
# --------------------------------------------------

def query_historical_data(time_range="24h", metrics=None):
    """
    Retrieve historical weather records from MongoDB.
    """

    range_durations = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30)
    }

    start_time = (
        datetime.now(timezone.utc)
        - range_durations.get(
            time_range, timedelta(hours=24)
        )
    )

    try:
        client = MongoClient(
            "mongodb://localhost:27017",
            serverSelectionTimeoutMS=3000
        )

        try:
            client.admin.command("ping")

            collection = client["bigdata"]["weather_history"]

            # Records use ISO-format timestamp strings.
            start_timestamp = start_time.strftime(
                "%Y-%m-%dT%H:%M"
            )

            query = {
                "timestamp": {"$gte": start_timestamp}
            }

            if metrics:
                query["metric_type"] = {"$in": metrics}

            records = list(
                collection.find(
                    query,
                    {"_id": 0}
                ).sort("timestamp", 1)
            )

        finally:
            client.close()

        if not records:
            return pd.DataFrame(
                columns=[
                    "timestamp", "value", "metric_type",
                    "sensor_id", "city", "unit", "source"
                ]
            )

        df = pd.DataFrame(records)

        # Normalize different record formats.
        if "temperature_c" in df.columns:
            if "value" not in df.columns:
                df["value"] = df["temperature_c"]
            else:
                df["value"] = df["value"].fillna(
                    df["temperature_c"]
                )

        if "value" not in df.columns:
            df["value"] = pd.NA

        if "metric_type" not in df.columns:
            df["metric_type"] = "temperature"

        if "city" not in df.columns:
            df["city"] = df.get("location", "Unknown")

        if "sensor_id" not in df.columns:
            df["sensor_id"] = df["city"]

        if "unit" not in df.columns:
            df["unit"] = "celsius"

        if "source" not in df.columns:
            df["source"] = "MongoDB"

        df["value"] = pd.to_numeric(
            df["value"], errors="coerce"
        )

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
            utc=True
        )

        return df.dropna(
            subset=["timestamp", "value"]
        ).reset_index(drop=True)

    except Exception as e:
        st.error(f"Could not retrieve historical data: {e}")

        return pd.DataFrame(
            columns=[
                "timestamp", "value", "metric_type",
                "sensor_id", "city", "unit", "source"
            ]
        )


# --------------------------------------------------
# Real-time dashboard
# --------------------------------------------------

def display_real_time_view(config, refresh_interval):
    st.header("📈 Real-time Weather Dashboard")

    refresh_state = st.session_state.refresh_state

    status = (
        "🟢 Enabled"
        if refresh_state["auto_refresh"]
        else "🔴 Disabled"
    )

    st.info(
        f"Auto-refresh: {status} | "
        f"Interval: {refresh_interval} seconds"
    )

    with st.spinner("Checking Kafka for new weather messages..."):
        real_time_data = consume_kafka_data(config)

    if real_time_data.empty:
        st.warning(
            "No new weather records have been received in "
            "this dashboard session yet. Check that your "
            "producer is sending messages to the selected topic."
        )
        return

    st.success(
        f"Showing {len(real_time_data)} unique records "
        "received during this dashboard session."
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Records Received", len(real_time_data))

    with col2:
        st.metric(
            "Latest Temperature",
            f"{real_time_data['value'].iloc[-1]:.1f} °C"
        )

    with col3:
        latest_timestamp = real_time_data["timestamp"].max()

        st.metric(
            "Latest Record Time",
            latest_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        )

    st.subheader("📈 Live Temperature Trend")

    chart_data = real_time_data.sort_values("timestamp")

    fig = px.line(
        chart_data,
        x="timestamp",
        y="value",
        color="city",
        markers=True,
        title="Weather Records Received from Kafka",
        labels={
            "timestamp": "Time",
            "value": "Temperature (°C)",
            "city": "City"
        },
        template="plotly_white"
    )

    st.plotly_chart(fig, width="stretch")

    with st.expander("📋 View Live Records"):
        st.dataframe(
            real_time_data.sort_values(
                "timestamp", ascending=False
            ),
            width="stretch",
            hide_index=True
        )


# --------------------------------------------------
# Historical dashboard
# --------------------------------------------------

def display_historical_view(config):
    st.header("📊 Historical Weather Analysis")

    col1, col2, col3 = st.columns(3)

    with col1:
        time_range = st.selectbox(
            "Time Range",
            ["1h", "24h", "7d", "30d"],
            index=1
        )

    with col2:
        metric_type = st.selectbox(
            "Metric Type",
            ["temperature", "all"]
        )

    with col3:
        aggregation = st.selectbox(
            "Aggregation",
            ["raw", "hourly", "daily", "weekly"]
        )

    metrics = (
        None if metric_type == "all"
        else [metric_type]
    )

    historical_data = query_historical_data(
        time_range, metrics
    )

    if historical_data.empty:
        st.warning(
            "No saved records were found for this time range. "
            "Try a longer range or check MongoDB."
        )
        return

    # Optional aggregation of records for the chart and table.
    chart_data = historical_data.copy()

    if aggregation != "raw":
        chart_data = chart_data.set_index("timestamp")

        frequency = {
            "hourly": "h",
            "daily": "D",
            "weekly": "W"
        }[aggregation]

        chart_data = (
            chart_data.groupby("metric_type")
            .resample(frequency)["value"]
            .mean()
            .reset_index()
        )

    st.subheader("Historical Data")

    st.dataframe(
        historical_data.sort_values(
            "timestamp", ascending=False
        ),
        width="stretch",
        hide_index=True
    )

    st.subheader("📉 Historical Temperature Trend")

    fig = px.line(
        chart_data,
        x="timestamp",
        y="value",
        color="metric_type",
        markers=True,
        title=f"Historical Weather ({time_range})",
        labels={
            "timestamp": "Time",
            "value": "Temperature (°C)",
            "metric_type": "Metric"
        },
        template="plotly_white"
    )

    st.plotly_chart(fig, width="stretch")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Records", len(historical_data))

    with col2:
        st.metric(
            "Average Temperature",
            f"{historical_data['value'].mean():.2f} °C"
        )

    with col3:
        st.metric(
            "Temperature Standard Deviation",
            f"{historical_data['value'].std():.2f}"
            if len(historical_data) > 1
            else "N/A"
        )

    csv_data = historical_data.to_csv(
        index=False
    ).encode("utf-8")

    st.download_button(
        "Download Historical Data as CSV",
        data=csv_data,
        file_name="weather_history.csv",
        mime="text/csv"
    )


# --------------------------------------------------
# Main application
# --------------------------------------------------

def main():
    st.title("🌦️ Weather Streaming Data Dashboard")

    if "refresh_state" not in st.session_state:
        st.session_state.refresh_state = {
            "auto_refresh": True,
            "last_refresh": datetime.now()
        }

    config = setup_sidebar()

    st.sidebar.subheader("Refresh Settings")

    st.session_state.refresh_state["auto_refresh"] = (
        st.sidebar.checkbox(
            "Enable Auto Refresh",
            value=st.session_state.refresh_state[
                "auto_refresh"
            ]
        )
    )

    # Default is defined even when auto-refresh is disabled.
    refresh_interval = 15

    if st.session_state.refresh_state["auto_refresh"]:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15
        )

        st_autorefresh(
            interval=refresh_interval * 1000,
            key="weather_auto_refresh"
        )

    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state["last_refresh"] = (
            datetime.now()
        )
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Last page refresh: "
        + datetime.now().strftime("%H:%M:%S")
    )

    tab1, tab2 = st.tabs([
        "📈 Real-time Streaming",
        "📊 Historical Data"
    ])

    with tab1:
        display_real_time_view(config, refresh_interval)

    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()
