import numpy as np
from sklearn.ensemble import IsolationForest

from data.CONSTANTS import BATTERY_VIEW_DATA_CSV
from data.utilities import get_csv_from_drive_as_dataframe


def main():
    # Retrieve the file ID from the CONSTANTS module
    file_id = BATTERY_VIEW_DATA_CSV

    # Retrieve the file data from Google Drive
    file_data = get_csv_from_drive_as_dataframe(file_id)
    print(file_data)

    if file_data is not None:
        # Process the file data and detect outliers
        outliers = detect_outliers(file_data)

        # Print the outliers
        print("Detected outliers:")
        for outlier in outliers:
            print(outlier)


def detect_outliers(data):
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

    # Get the outlier values
    outliers = [data.iloc[i, 0] for i in outlier_indices]

    return outliers


if __name__ == "__main__":
    main()
