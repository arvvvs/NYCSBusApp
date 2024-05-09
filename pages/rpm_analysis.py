import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from data.CONSTANTS import RPM_VIEW_DATA_CSV
from streamlit_utilities import (
    format_breakdown_for_chart,
    get_breakdown_count_by_bus,
    get_rpm_data,
)


# Function to detect anomalies based on standard deviation
def detect_anomalies(data, threshold=3):
    # Calculate the mean and standard deviation
    mean = data.mean()
    std = data.std()

    # Set the upper and lower anomaly thresholds based on standard deviation
    upper_threshold = mean + threshold * std
    lower_threshold = mean - threshold * std

    # Find the indices of data points above the upper threshold or below the lower threshold
    outlier_indices = np.where((data > upper_threshold) | (data < lower_threshold))[0]

    return outlier_indices


# Function to create a simple line chart using Altair
def create_line_chart(data, breakdowns):
    # Extract the hour component from the estDateTime column

    chart = (
        alt.Chart(data)
        .mark_line(color="orange")
        .encode(
            x=alt.X("estDateTime:T", title="Date Time"),
            y=alt.Y("RPM:Q", title="RPM"),
            tooltip=["Date Time:T", "RPM:Q"],
        )
        .properties(width="container", height=400)
        .interactive()
    )
    dots = (
        alt.Chart(data.loc[data["anomaly"] == True])
        .mark_point(color="red")
        .encode(x="estDateTime:T", y="RPM:Q")
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
    # Retrieve the file ID from the CONSTANTS module
    file_id = RPM_VIEW_DATA_CSV

    # Call the get_rpm_data function to retrieve the file data
    rpm_view_df = get_rpm_data(nrows="Random")
    if rpm_view_df is None or not len(rpm_view_df):
        return "Error: no RPM data available"
    rpm_view_df = rpm_view_df.rename(columns={"data": "RPM"})
    # Convert the 'estDateTime' column to datetime type
    rpm_view_df["estDateTime"] = pd.to_datetime(
        rpm_view_df["estDateTime"], format="mixed"
    )

    # Detect anomalies based on standard deviation
    outlier_indices = detect_anomalies(rpm_view_df["RPM"])
    anomalies = rpm_view_df.iloc[outlier_indices]
    anomalies = anomalies.merge(get_breakdown_count_by_bus(), how="left", on=["Bus #"])

    rpm_view_df["anomaly"] = False
    rpm_view_df.anomaly.iloc[outlier_indices] = True

    # Get the data points with anomalies

    # Sort the anomalies by 'rpm' column to identify top, middle, and bottom buses
    anomalies_sorted = anomalies.sort_values(by="RPM", ascending=False)

    # Get the top X buses with anomalies and their corresponding datetime
    top_x = 5  # Set the desired number of top buses
    top_buses = anomalies_sorted.head(top_x)

    anomaly_count_by_bus = (
        anomalies.groupby(by=["Bus #", "Breakdowns"])["RPM"].agg("count").reset_index()
    )
    top_buses = anomaly_count_by_bus.rename(columns={"RPM": "Anomaly Count"})
    top_buses = (
        top_buses[["Bus #", "Anomaly Count", "Breakdowns"]]
        .sort_values(by=["Anomaly Count"], ascending=False)
        .head(10)
    )

    top_buses_breakdown = (
        anomalies_sorted[["Bus #", "RPM", "Breakdowns"]]
        .drop_duplicates(keep="first", subset=["Bus #"])
        .reset_index()
    )

    # Get the middle X buses with anomalies and their corresponding datetime
    middle_x = 5  # Set the desired number of middle buses
    middle_buses = anomalies_sorted[
        int(len(anomalies_sorted) / 2)
        - int(middle_x / 2) : int(len(anomalies_sorted) / 2)
        + int(middle_x / 2)
        + 10
    ]
    middle_buses_breakdown = middle_buses[["Bus #", "RPM", "Breakdowns"]].head(middle_x)

    # Get the bottom X buses with anomalies and their corresponding datetime
    bottom_x = 5  # Set the desired number of bottom buses
    bottom_buses = anomalies_sorted.tail(bottom_x)
    bottom_buses_breakdown = bottom_buses[["Bus #", "RPM", "Breakdowns"]]

    # Plot each of the top, middle, and bottom buses separately
    bus_options = set(rpm_view_df["Bus #"])
    selected_bus = st.selectbox("Select Bus", bus_options)

    selected_bus_data = rpm_view_df[rpm_view_df["Bus #"] == selected_bus]
    st.write(f"Data for Bus {selected_bus}:")
    st.altair_chart(
        create_line_chart(selected_bus_data, format_breakdown_for_chart(selected_bus)),
        use_container_width=True,
    )

    # Print the detected anomalies
    st.title("RPM Anomaly Detection using standard deviation")
    st.write("Detected anomalies:")
    st.write(anomalies)

    st.title("Buses with top number of detected anomalies")
    st.write(top_buses.reset_index())

    # Display the breakdown of the top, middle, and bottom buses side by side
    col1, col2, col3 = st.columns(3)

    with col1:
        st.write(f"Buses with anomalies with top {top_x} highest RPM readings:")
        st.write(top_buses_breakdown.head(top_x))

    with col2:
        st.write(f"Buses with anomalies with {middle_x} RPM readings:")
        st.write(middle_buses_breakdown.reset_index())

    with col3:
        st.write(f"Buses with anomalies with bottom {bottom_x} RPM readings:")
        st.write(bottom_buses_breakdown.reset_index())


if __name__ == "__main__":
    main()
