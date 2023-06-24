import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
from data.CONSTANTS import RPM_VIEW_DATA_CSV
from data.utilities import get_csv_from_drive_as_dataframe
from data.metrics_transformation import get_rpm_data


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
def create_line_chart(data):
    # Extract the hour component from the estDateTime column
    data["hour"] = data["estDateTime"].dt.hour

    chart = (
        alt.Chart(data)
        .mark_line(color='orange')
        .encode(
            x=alt.X("hour:T", title='Hour'),
            y=alt.Y("data:Q", title='RPM'),
            tooltip=["hour:T", "data:Q"],
        )
        .properties(width="container", height=400)
        .interactive()
    )

    return chart


# Main function
def main():
    # Retrieve the file ID from the CONSTANTS module
    file_id = RPM_VIEW_DATA_CSV

    # Call the get_rpm_data function to retrieve the file data
    file_data = get_rpm_data(nrows="Random")

    if file_data is not None:
        # Convert the 'estDateTime' column to datetime type
        file_data["estDateTime"] = pd.to_datetime(file_data["estDateTime"], format="mixed")

        # Detect anomalies based on standard deviation
        outlier_indices = detect_anomalies(file_data["data"])

        # Get the data points with anomalies
        anomalies = file_data.iloc[outlier_indices]

        # Sort the anomalies by 'data' column to identify top, middle, and bottom buses
        anomalies_sorted = anomalies.sort_values(by='data')

        # Get the top X buses with anomalies and their corresponding datetime
        top_x = 5  # Set the desired number of top buses
        top_buses = anomalies_sorted.head(top_x)
        top_buses_breakdown = top_buses[['Bus #']]

        # Get the middle X buses with anomalies and their corresponding datetime
        middle_x = 5  # Set the desired number of middle buses
        middle_buses = anomalies_sorted[int(len(anomalies_sorted) / 2) - int(middle_x / 2):int(len(anomalies_sorted) / 2) + int(middle_x / 2)]
        middle_buses_breakdown = middle_buses[['Bus #']]

        # Get the bottom X buses with anomalies and their corresponding datetime
        bottom_x = 5  # Set the desired number of bottom buses
        bottom_buses = anomalies_sorted.tail(bottom_x)
        bottom_buses_breakdown = bottom_buses[['Bus #']]

        # Plot each of the top, middle, and bottom buses separately
        bus_options = np.concatenate([top_buses['Bus #'].unique(), middle_buses['Bus #'].unique(), bottom_buses['Bus #'].unique()])
        selected_bus = st.selectbox('Select Bus', bus_options)

        selected_bus_data = file_data[file_data['Bus #'] == selected_bus]
        st.write(f"Data for Bus {selected_bus}:")
        st.altair_chart(create_line_chart(selected_bus_data), use_container_width=True)

        # Print the detected anomalies
        st.title('RPM Anomaly Detection')
        st.write("Detected anomalies:")
        st.write(anomalies)

        # Display the breakdown of the top, middle, and bottom buses side by side
        col1, col2, col3 = st.columns(3)

        with col1:
            st.write(f"Buses with top {top_x} anomalies:")
            st.write(top_buses_breakdown)

        with col2:
            st.write(f"Buses with middle {middle_x} anomalies:")
            st.write(middle_buses_breakdown)

        with col3:
            st.write(f"Buses with bottom {bottom_x} anomalies:")
            st.write(bottom_buses_breakdown)


if __name__ == "__main__":
    main()
