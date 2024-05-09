import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from data.CONSTANTS import BATTERY_VIEW_DATA_CSV, BUS_BREAKDOWN_VIEW, RPM_VIEW_DATA_CSV
from streamlit_utilities import get_battery_data, get_breakdown_data, get_rpm_data


# Function to detect anomalies based on Isolation Forest
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


# Function to create a line chart using Altair
def create_line_chart(data):
    chart = (
        alt.Chart(data)
        .mark_line(color="orange")
        .encode(x="estDateTime:T", y="data:Q")
        .properties(width="container", height=400)
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
    file_data = get_battery_data()

    if file_data is not None:
        file_data["estDateTime"] = pd.to_datetime(
            file_data["estDateTime"], format="mixed"
        )
        outlier_indices = detect_anomalies(file_data["data"])
        anomalies = file_data.iloc[outlier_indices]
        anomalies_sorted = anomalies.sort_values(by="data")
        top_x = 5
        top_buses = anomalies_sorted.head(top_x)
        middle_buses = anomalies_sorted.iloc[
            len(anomalies_sorted) // 2
            - top_x // 2 : len(anomalies_sorted) // 2
            + top_x // 2
        ]
        bottom_buses = anomalies_sorted.tail(top_x)

        st.title("Battery Reading Anomaly Detection")

        option_list = ["Top 5 Buses", "Middle 5 Buses", "Bottom 5 Buses"]
        selected_option = st.selectbox("Select Buses to Plot", option_list)

        if selected_option == "Top 5 Buses":
            buses = top_buses
        elif selected_option == "Middle 5 Buses":
            buses = middle_buses
        else:
            buses = bottom_buses

        for bus in buses["Bus #"]:
            bus_data = file_data[file_data["Bus #"] == bus]
            st.write(f"Data for Bus {bus}:")
            st.altair_chart(create_line_chart(bus_data), use_container_width=True)

        tables = [
            top_buses[["Bus #"]],
            middle_buses[["Bus #"]],
            bottom_buses[["Bus #"]],
        ]
        titles = ["Top Buses", "Middle Buses", "Bottom Buses"]
        display_tables_side_by_side(tables, titles)

        st.write("Detected anomalies:")
        st.write(anomalies)


# Function for RPM anomaly detection
def rpm_anomaly_detection():
    file_data = get_rpm_data(nrows="Random")

    if file_data is not None:
        file_data["estDateTime"] = pd.to_datetime(
            file_data["estDateTime"], format="mixed"
        )
        outlier_indices = detect_anomalies(file_data["data"])
        anomalies = file_data.iloc[outlier_indices]
        anomalies_sorted = anomalies.sort_values(by="data")
        top_x = 5
        top_buses = anomalies_sorted.head(top_x)
        middle_buses = anomalies_sorted.iloc[
            len(anomalies_sorted) // 2
            - top_x // 2 : len(anomalies_sorted) // 2
            + top_x // 2
        ]
        bottom_buses = anomalies_sorted.tail(top_x)

        st.title("RPM Anomaly Detection")

        option_list = ["Top 5 Buses", "Middle 5 Buses", "Bottom 5 Buses"]
        selected_option = st.selectbox("Select Buses to Plot", option_list)

        if selected_option == "Top 5 Buses":
            buses = top_buses
        elif selected_option == "Middle 5 Buses":
            buses = middle_buses
        else:
            buses = bottom_buses

        for bus in buses["Bus #"]:
            bus_data = file_data[file_data["Bus #"] == bus]
            st.write(f"Data for Bus {bus}:")
            st.altair_chart(create_line_chart(bus_data), use_container_width=True)

        tables = [
            top_buses[["Bus #"]],
            middle_buses[["Bus #"]],
            bottom_buses[["Bus #"]],
        ]
        titles = ["Top Buses", "Middle Buses", "Bottom Buses"]
        display_tables_side_by_side(tables, titles)

        st.write("Detected anomalies:")
        st.write(anomalies)


# Function to extract year from datetime value or return "NaT" on exception
def get_year(x):
    try:
        return pd.to_datetime(x, errors="coerce").dt.year
    except:
        return pd.NaT


# Function to extract month from datetime value or return "NaT" on exception
def get_month(x):
    try:
        return pd.to_datetime(x).strftime("%B")
    except:
        return "NaT"


# Function for the "Overview" page
def overview_page():
    battery_data = get_battery_data()
    rpm_data = get_rpm_data(nrows="Random")
    breakdown_data = get_breakdown_data()

    if battery_data is not None and rpm_data is not None and breakdown_data is not None:
        st.title("Overview - Average Statistics for All Buses")

        st.subheader("Battery Readings")
        avg_battery_readings = (
            battery_data.groupby("Bus #")["data"].mean().reset_index()  # type: ignore
        )
        overall_avg_battery = avg_battery_readings["data"].mean()
        st.write(f"Average Battery Reading for All Buses: {overall_avg_battery:.2f}")

        st.subheader("RPM Data")
        avg_rpm_data = rpm_data.groupby("Bus #")["data"].mean().reset_index()
        overall_avg_rpm = avg_rpm_data["data"].mean()
        st.write(f"Average RPM Data for All Buses: {overall_avg_rpm:.2f}")

        st.subheader("Breakdown Counts")
        total_breakdown_count = breakdown_data.shape[0]
        st.write(f"Total Breakdown Count for All Buses: {total_breakdown_count}")


# Function for the "Individual Bus Statistics" page
def individual_bus_statistics_page():
    breakdown_file_id = BUS_BREAKDOWN_VIEW

    battery_data = get_battery_data()
    rpm_data = get_rpm_data()
    breakdown_data = get_breakdown_data()
    color_scheme = ["steelblue", "darkorange", "limegreen"]

    if battery_data is not None and rpm_data is not None and breakdown_data is not None:
        unique_buses = (
            set(battery_data["Bus #"].astype(str))
            | set(rpm_data["Bus #"].astype(str))
            | set(breakdown_data["Bus #"].astype(str))
        )
        unique_buses = sorted(unique_buses)  # Sort the unique bus numbers

        if not unique_buses:
            st.title("No matching bus numbers found in the datasets.")
            return

        selected_bus = st.sidebar.selectbox("Select Bus", unique_buses)

        if selected_bus:
            st.title(f"Individual Bus Statistics - Bus {selected_bus}")
        # RPM Data
        selected_rpm_data = rpm_data[rpm_data["Bus #"] == selected_bus]
        selected_rpm_data_display = selected_rpm_data[["estDateTime", "data"]]

        # Display RPM Data
        st.write("RPM Data:")
        st.write(selected_rpm_data_display)

        # Plot RPM Data
        st.write("RPM Data Chart:")
        chart_rpm = (
            alt.Chart(selected_rpm_data)
            .mark_line(color=color_scheme[0])
            .encode(x="estDateTime", y="data")
        )
        st.altair_chart(chart_rpm, use_container_width=True)

        # Battery Readings
        selected_battery_data = battery_data[battery_data["Bus #"] == selected_bus]
        selected_battery_data_display = selected_battery_data[["estDateTime", "data"]]

        # Display Battery Readings
        st.write("Battery Readings:")
        st.write(selected_battery_data_display)

        # Plot Battery Readings
        st.write("Battery Readings Chart:")
        chart_battery = (
            alt.Chart(selected_battery_data)
            .mark_line(color=color_scheme[1])
            .encode(x="estDateTime", y="data")
        )
        st.altair_chart(chart_battery, use_container_width=True)

        # Breakdown Counts
        selected_breakdown_data = breakdown_data[
            breakdown_data["Bus #"] == selected_bus
        ]
        selected_breakdown_data["estReportedAt"] = pd.to_datetime(
            selected_breakdown_data["estReportedAt"], format="mixed"
        )
        selected_breakdown_data["Year"] = selected_breakdown_data[
            "estReportedAt"
        ].apply(get_year)
        selected_breakdown_data["Month"] = selected_breakdown_data[
            "estReportedAt"
        ].apply(get_month)

        # Calculate breakdown counts
        breakdown_counts = (
            selected_breakdown_data.groupby(["Year", "Month"])
            .size()
            .reset_index(name="count")
        )

        # Display Breakdown Counts
        st.write("Breakdown Counts:")
        total_breakdown_count = len(selected_breakdown_data)
        st.write(total_breakdown_count)

        # Plot Breakdown Counts
        st.write("Total Breakdown Counts Chart:")
        chart_breakdown = (
            alt.Chart(breakdown_counts)
            .mark_bar(color=color_scheme[2])
            .encode(
                x="Year", y="count", color="Month:N", tooltip=["Year", "Month", "count"]
            )
        )
        st.altair_chart(chart_breakdown, use_container_width=True)


# Function for the "Breakdown Data Analysis" page
def breakdown_data_analysis():
    import contractions
    import nltk
    from rake_nltk import Rake
    from st_aggrid import AgGrid
    from st_aggrid.shared import GridUpdateMode

    nltk.download("stopwords")
    nltk.download("punkt")
    # Read the breakdown data CSV file
    bus_breakdown_view_df = get_breakdown_data()

    r = Rake(
        min_length=1,
        max_length=4,
        language="english",
        include_repeated_phrases=False,
        stopwords={
            "will not start",
            "ignition broke",
            "vandalized",
            "shift gears",
            "driver" "door",
        },
    )

    def extract_keywords(text: str):
        replace_dict = {"over heated": "overheated", "wasnt": "wasn't"}
        text = text.lower()
        for replace, replacement in replace_dict.items():
            text = text.replace(replace, replacement)
        text = contractions.fix(text)
        r.extract_keywords_from_text(text)
        return (
            ranked_phrase_list[0]
            if len(ranked_phrase_list := r.get_ranked_phrases())
            else ""
        )

    bus_breakdown_view_df["description"] = bus_breakdown_view_df["description"].astype(
        str
    )
    bus_breakdown_view_df["keywords"] = bus_breakdown_view_df["description"].apply(
        extract_keywords
    )
    # Convert the 'estReportedAt' column to datetime type and extract year and month
    # file_data['Year'] = file_data['estReportedAt'].apply(get_year)
    bus_breakdown_view_df["Month"] = bus_breakdown_view_df["estReportedAt"].apply(
        get_month
    )

    # Group the data by Month and breakdown description, and count the occurrences
    grouped_data = (
        bus_breakdown_view_df.groupby(["Month", "keywords"])["Bus #"]
        .count()
        .reset_index()
    )
    grouped_data.columns = ["Month", "Keywords", "Count"]

    # Perform data analysis and visualization
    # Example: Display breakdown data table
    st.write("Breakdown Data:")
    st.write(grouped_data)

    # Example: Plot breakdown data trends over time
    st.write("Breakdown Data Trends:")
    # file_data['Year'] = file_data['Year'].replace("NaT", np.nan)  # Replace "NaT" with NaN for plotting
    # x     cfr5file_data['Month'] = file_data['Month'].replace("NaT", np.nan)  # Replace "NaT" with NaN for plotting

    # Convert 'Year' column to numeric values with errors coerced to NaN
    # file_data['Month'] = pd.to_numeric(file_data['Month'], errors='coerce')
    # st.write(file_data.head())

    # Create the chart
    chart = (
        alt.Chart(grouped_data)
        .mark_bar()
        .encode(
            alt.X("Month:O", title="Month"),
            alt.Y("Count:Q", title="Breakdown Count"),
            # alt.Color('Month:N', title='Month'),
            alt.Color("Keywords:N", title="Keywords"),
            # column='Keywords:N',  # Separate columns by description
            tooltip=["Month:N", "Keywords:N", "Count:O"],
        )
        .properties(width="container", height=400)
    )

    # Display the chart using Streamlit
    st.altair_chart(chart, use_container_width=True)
    bus_breakdown_view_df = bus_breakdown_view_df[
        ["Bus #", "estReportedAt", "Month", "description", "keywords"]
    ]
    bus_breakdown_view_df = bus_breakdown_view_df.rename(
        columns={
            "estReportedAt": "Reported At",
            "description": "Description",
            "keywords": "Keywords",
        }
    )
    AgGrid(
        bus_breakdown_view_df,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        reload_data=True,
    )


# Main function to run the app
def main():
    pages = {
        "Overview": overview_page,
        "Individual Bus Statistics": individual_bus_statistics_page,
        "Battery Anomaly Detection": battery_anomaly_detection,
        "RPM Anomaly Detection": rpm_anomaly_detection,
        "Breakdown Data Analysis": breakdown_data_analysis,
    }

    st.sidebar.title("Navigation")
    page_selection = st.sidebar.radio("Go to", list(pages.keys()))

    # Execute the selected page function
    pages[page_selection]()


if __name__ == "__main__":
    main()
