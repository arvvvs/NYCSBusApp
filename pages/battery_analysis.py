import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from sklearn.ensemble import IsolationForest

from streamlit_utilities import (
    format_breakdown_for_chart,
    get_battery_data,
    get_breakdown_count_by_bus,
)


# Function to detect anomalies and return the outlier indices
@st.cache_data(persist=True)
def detect_anomalies(data):
    # Convert data to numpy array
    X = data.values.reshape(-1, 1)

    # Initialize and fit the Isolation Forest model
    model = IsolationForest(
        contamination=0.05
    )  # Adjust the contamination parameter as needed
    model.fit(X)

    # Predict outlier labels (-1 for outliers, 1 for inliers)
    labels = model.predict(X)

    # Get indices of outliers
    outlier_indices = np.where(labels == -1)[0]

    return outlier_indices


# Function to create a line chart using Altair
def create_line_chart(data, breakdowns):
    chart = (
        alt.Chart(data)
        .mark_line()
        .encode(x="estDateTime:T", y="Battery Voltage:Q")
        .properties(width="container", height=400)
    )
    dots = (
        alt.Chart(data.loc[data["anomaly"] == True])
        .mark_point(color="red")
        .encode(x="estDateTime:T", y="Battery Voltage:Q")
    )
    breakdowns = (
        alt.Chart(breakdowns)
        .mark_rule(color="red")
        .encode(x="estDateTime:T")
        .properties(width="container", height=400)
    )
    return chart + dots + breakdowns


# Main function
def main():
    # Retrieve the file data from Google Drive
    battery_view_df = get_battery_data()
    if battery_view_df is None or not len(battery_view_df):
        return "Error: no battery data available"

    # Convert the 'estDateTime' column to datetime type
    battery_view_df["estDateTime"] = pd.to_datetime(
        battery_view_df["estDateTime"], format="mixed"
    )

    # Detect anomalies
    outlier_indices = detect_anomalies(battery_view_df["data"])

    battery_view_df = battery_view_df.rename(columns={"data": "Battery Voltage"})
    # Get the data points with anomalies
    anomalies = battery_view_df.iloc[outlier_indices]
    # TODO: convert to breakdowns before and after anomaly detected
    anomalies = anomalies.merge(get_breakdown_count_by_bus(), how="left", on=["Bus #"])
    battery_view_df["anomaly"] = False
    battery_view_df.anomaly.iloc[outlier_indices] = True

    # Sort the anomalies by 'data' column to identify top, middle, and bottom buses
    anomaly_count_by_bus = (
        anomalies.groupby(by=["Bus #", "Breakdowns"])["Battery Voltage"]
        .agg("count")
        .reset_index()
    )
    voltage_sorted_battery_reading_anomalies = anomalies.sort_values(
        by="Battery Voltage",
        ascending=False,
    )

    # Get the top X buses with anomalies and their corresponding datetime
    top_x = 5  # Set the desired number of top buses
    top_buses = anomaly_count_by_bus.head(top_x)
    top_buses = top_buses.rename(columns={"Battery Voltage": "Anomaly Count"})
    top_buses_anomolies = top_buses[["Bus #", "Anomaly Count", "Breakdowns"]]

    # Print the detected anomalies
    st.title("Battery Reading Anomaly Detection using IsolationForest Method")
    st.write("Detected anomalies:")
    st.write(anomalies.reset_index())

    st.write("Buses with Top # of Anomalies")
    st.write(
        top_buses_anomolies.sort_values(
            by=["Anomaly Count"], ascending=False
        ).reset_index(drop=True)
    )

    # Plot each of the top 5 buses separately
    bus_options = set(battery_view_df["Bus #"])
    selected_bus = st.selectbox("Select Bus", bus_options)

    selected_bus_data = battery_view_df[battery_view_df["Bus #"] == selected_bus]
    st.write(f"Data for Bus {selected_bus}:")
    st.altair_chart(
        create_line_chart(selected_bus_data, format_breakdown_for_chart(selected_bus)),
        use_container_width=True,
    )
    # Display the breakdown of the top, middle, and bottom buses side by side
    col1, col2, col3 = st.columns(3)

    with col1:
        st.write(
            f"Top {top_x} Buses with detected anomalies with high voltage readings: "
        )
        top_buses = voltage_sorted_battery_reading_anomalies.drop_duplicates(
            subset=["Bus #"], keep="first"
        ).head(top_x)
        st.write(top_buses[["Bus #", "Battery Voltage", "Breakdowns"]])

    with col2:
        # Get the middle X buses with anomalies and their corresponding datetime
        middle_x = 5  # Set the desired number of middle buses
        middle_buses = voltage_sorted_battery_reading_anomalies[
            int(len(anomaly_count_by_bus) / 2)
            - int(middle_x / 2) : int(len(voltage_sorted_battery_reading_anomalies) / 2)
            + int(middle_x * 10 / 2)
        ]
        middle_buses_breakdown = (
            middle_buses[["Bus #", "Battery Voltage", "Breakdowns"]]
            .drop_duplicates(subset=["Bus #"], keep="first")
            .head(middle_x)
        )
        st.write(
            f"Top {middle_x} Buses with detected anomalies with mid level voltage readings: "
        )
        st.write(middle_buses_breakdown.reset_index())

    with col3:
        # Get the bottom X buses with anomalies and their corresponding datetime
        bottom_x = 5  # Set the desired number of bottom buses
        bottom_buses_breakdown = (
            voltage_sorted_battery_reading_anomalies[
                ["Bus #", "Battery Voltage", "Breakdowns"]
            ]
            .drop_duplicates(subset=["Bus #"], keep="first")
            .tail(bottom_x)
        )
        st.write(
            f"Bottom {bottom_x} Buses with detected anomalies with lowest level voltage readings: "
        )
        st.write(bottom_buses_breakdown.reset_index())


if __name__ == "__main__":
    main()
