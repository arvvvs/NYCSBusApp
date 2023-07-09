import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
from sklearn.ensemble import IsolationForest
from data.CONSTANTS import BATTERY_VIEW_DATA_CSV
from data.utilities import get_csv_from_drive_as_dataframe

# Function to detect anomalies and return the outlier indices
def detect_anomalies(data):
    # Convert data to numpy array
    X = data.values.reshape(-1, 1)

    # Initialize and fit the Isolation Forest model
    model = IsolationForest(contamination=0.05)  # Adjust the contamination parameter as needed
    model.fit(X)

    # Predict outlier labels (-1 for outliers, 1 for inliers)
    labels = model.predict(X)

    # Get indices of outliers
    outlier_indices = np.where(labels == -1)[0]

    return outlier_indices

# Function to create a line chart using Altair
def create_line_chart(data):
    chart = alt.Chart(data).mark_line().encode(
        x='estDateTime:T',
        y='data:Q'
    ).properties(
        width='container',
        height=400
    )
    return chart

# Main function
def main():
    # Retrieve the file ID from the CONSTANTS module
    file_id = BATTERY_VIEW_DATA_CSV

    # Retrieve the file data from Google Drive
    file_data = get_csv_from_drive_as_dataframe(file_id)

    if file_data is not None:
        # Convert the 'estDateTime' column to datetime type
        file_data['estDateTime'] = pd.to_datetime(file_data['estDateTime'], format='mixed')

        # Detect anomalies
        outlier_indices = detect_anomalies(file_data['data'])

        # Get the data points with anomalies
        anomalies = file_data.iloc[outlier_indices]

        # Sort the anomalies by 'data' column to identify top, middle, and bottom buses
        anomalies_sorted = anomalies.sort_values(by='data')

        # Get the top X buses with anomalies and their corresponding datetime
        top_x = 5  # Set the desired number of top buses
        top_buses = anomalies_sorted.head(top_x)
        top_buses_breakdown = top_buses[['Bus #']]

        # Calculate anomaly statistics per bus
        anomaly_counts = top_buses['Bus #'].value_counts()
        avg_anomalies = anomaly_counts.mean()

        # Calculate average data for each bus
        bus_data_avg = top_buses.groupby('Bus #')['data'].mean()

        # Print the detected anomalies
        st.title('Battery Reading Anomaly Detection')
        st.write("Detected anomalies:")
        st.write(anomalies)

        # Plot each of the top 5 buses separately
        bus_options = top_buses['Bus #'].unique()
        selected_bus = st.selectbox('Select Bus', bus_options)

        selected_bus_data = file_data[file_data['Bus #'] == selected_bus]
        st.write(f"Data for Bus {selected_bus}:")
        st.altair_chart(create_line_chart(selected_bus_data), use_container_width=True)

        # Display the breakdown of the top, middle, and bottom buses side by side
        col1, col2, col3 = st.columns(3)

        with col1:
            st.write(f"Buses with top {top_x} anomalies:")
            st.write(top_buses_breakdown)

        with col2:
            # Get the middle X buses with anomalies and their corresponding datetime
            middle_x = 5  # Set the desired number of middle buses
            middle_buses = anomalies_sorted[
                int(len(anomalies_sorted) / 2) - int(middle_x / 2) : int(len(anomalies_sorted) / 2) + int(middle_x / 2)
            ]
            middle_buses_breakdown = middle_buses[['Bus #']]
            st.write(f"Buses with middle {middle_x} anomalies:")
            st.write(middle_buses_breakdown)

        with col3:
            # Get the bottom X buses with anomalies and their corresponding datetime
            bottom_x = 5  # Set the desired number of bottom buses
            bottom_buses = anomalies_sorted.tail(bottom_x)
            bottom_buses_breakdown = bottom_buses[['Bus #']]
            st.write(f"Buses with bottom {bottom_x} anomalies:")
            st.write(bottom_buses_breakdown)

if __name__ == "__main__":
    main()
