import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
from sklearn.ensemble import IsolationForest
from data.CONSTANTS import BATTERY_VIEW_DATA_CSV, RPM_VIEW_DATA_CSV, BUS_BREAKDOWN_VIEW
from data.utilities import get_csv_from_drive_as_dataframe
from data.metrics_transformation import get_rpm_data


# Function to detect anomalies based on Isolation Forest
def detect_anomalies(data):
    values = data.tolist()
    X = np.reshape(values, (-1, 1))
    model = IsolationForest(contamination=0.05)
    model.fit(X)
    labels = model.predict(X)
    outlier_indices = np.where(labels == -1)[0]
    return outlier_indices


# Function to create a line chart using Altair
def create_line_chart(data):
    chart = alt.Chart(data).mark_line(color='orange').encode(
        x='estDateTime:T',
        y='data:Q'
    ).properties(
        width='container',
        height=400
    )
    return chart


# Function to display tables side by side
def display_tables_side_by_side(tables, titles=[]):
    col_count = len(tables)
    with st.container():
        cols = st.columns(col_count)
        for i in range(col_count):
            with cols[i]:
                if titles and i < len(titles):
                    st.write(f"## {titles[i]}")  # Add the title as a header
                st.write(tables[i])


# Function for Battery anomaly detection
def battery_anomaly_detection():
    file_id = BATTERY_VIEW_DATA_CSV
    file_data = get_csv_from_drive_as_dataframe(file_id)

    if file_data is not None:
        file_data['estDateTime'] = pd.to_datetime(file_data['estDateTime'], format='mixed')
        outlier_indices = detect_anomalies(file_data['data'])
        anomalies = file_data.iloc[outlier_indices]
        anomalies_sorted = anomalies.sort_values(by='data')
        top_x = 5
        top_buses = anomalies_sorted.head(top_x)
        middle_buses = anomalies_sorted.iloc[len(anomalies_sorted) // 2 - top_x // 2: len(anomalies_sorted) // 2 + top_x // 2]
        bottom_buses = anomalies_sorted.tail(top_x)

        st.title('Battery Reading Anomaly Detection')

        option_list = ['Top 5 Buses', 'Middle 5 Buses', 'Bottom 5 Buses']
        selected_option = st.selectbox("Select Buses to Plot", option_list)

        if selected_option == 'Top 5 Buses':
            buses = top_buses
        elif selected_option == 'Middle 5 Buses':
            buses = middle_buses
        else:
            buses = bottom_buses

        for bus in buses['Bus #']:
            bus_data = file_data[file_data['Bus #'] == bus]
            st.write(f"Data for Bus {bus}:")
            st.altair_chart(create_line_chart(bus_data), use_container_width=True)

        tables = [top_buses[['Bus #']], middle_buses[['Bus #']], bottom_buses[['Bus #']]]
        titles = ["Top Buses", "Middle Buses", "Bottom Buses"]
        display_tables_side_by_side(tables, titles)

        st.write("Detected anomalies:")
        st.write(anomalies)


# Function for RPM anomaly detection
def rpm_anomaly_detection():
    file_id = RPM_VIEW_DATA_CSV
    file_data = get_rpm_data(nrows="Random")

    if file_data is not None:
        file_data["estDateTime"] = pd.to_datetime(file_data["estDateTime"], format="mixed")
        outlier_indices = detect_anomalies(file_data["data"])
        anomalies = file_data.iloc[outlier_indices]
        anomalies_sorted = anomalies.sort_values(by='data')
        top_x = 5
        top_buses = anomalies_sorted.head(top_x)
        middle_buses = anomalies_sorted.iloc[len(anomalies_sorted) // 2 - top_x // 2: len(anomalies_sorted) // 2 + top_x // 2]
        bottom_buses = anomalies_sorted.tail(top_x)

        st.title('RPM Anomaly Detection')

        option_list = ['Top 5 Buses', 'Middle 5 Buses', 'Bottom 5 Buses']
        selected_option = st.selectbox("Select Buses to Plot", option_list)

        if selected_option == 'Top 5 Buses':
            buses = top_buses
        elif selected_option == 'Middle 5 Buses':
            buses = middle_buses
        else:
            buses = bottom_buses

        for bus in buses['Bus #']:
            bus_data = file_data[file_data['Bus #'] == bus]
            st.write(f"Data for Bus {bus}:")
            st.altair_chart(create_line_chart(bus_data), use_container_width=True)

        tables = [top_buses[['Bus #']], middle_buses[['Bus #']], bottom_buses[['Bus #']]]
        titles = ["Top Buses", "Middle Buses", "Bottom Buses"]
        display_tables_side_by_side(tables, titles)

        st.write("Detected anomalies:")
        st.write(anomalies)


# Function to extract year from datetime value or return "NaT" on exception
def get_year(x):
    try:
        return pd.to_datetime(x).strftime('%Y')
    except:
        return "NaT"


# Function to extract month from datetime value or return "NaT" on exception
def get_month(x):
    try:
        return pd.to_datetime(x).strftime('%B')
    except:
        return "NaT"


def breakdown_data_analysis():
    # Read the breakdown data CSV file
    file_id = BUS_BREAKDOWN_VIEW
    file_data = get_csv_from_drive_as_dataframe(file_id)

    # Convert the 'estReportedAt' column to datetime type and extract year and month
    file_data['Year'] = file_data['estReportedAt'].apply(get_year)
    file_data['Month'] = file_data['estReportedAt'].apply(get_month)

    # Perform data analysis and visualization
    # Example: Display breakdown data table
    st.write("Breakdown Data:")
    st.write(file_data)

    # Example: Plot breakdown data trends over time
    st.write("Breakdown Data Trends:")
    file_data['Year'] = file_data['Year'].replace("NaT", np.nan)  # Replace "NaT" with NaN for plotting
    file_data['Month'] = file_data['Month'].replace("NaT", np.nan)  # Replace "NaT" with NaN for plotting
    
    # Convert 'Year' column to numeric values with errors coerced to NaN
    file_data['Year'] = pd.to_numeric(file_data['Year'], errors='coerce')

    # Filter data for the year range
    filtered_data = file_data[(file_data['Year'] >= 2022) & (file_data['Year'] <= 2023)]
    chart = alt.Chart(filtered_data).mark_line().encode(
        x='Year',
        y='count()',
        color='description:N'
    ).properties(
        width='container',
        height=400
    )
    st.altair_chart(chart, use_container_width=True)


# Main function
def main():
    st.sidebar.title("Navigation")
    pages = {
    "Battery Anomaly Detection": battery_anomaly_detection,
    "RPM Anomaly Detection": rpm_anomaly_detection,
    "Breakdown Data Analysis": breakdown_data_analysis  # New page
}

    selected_page = st.sidebar.selectbox("Select Page", tuple(pages.keys()))
    pages[selected_page]()


if __name__ == "__main__":
    main()
