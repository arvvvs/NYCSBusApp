import streamlit as st
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Load data from the shared file
def load_data(file_id):
    file_data = drive_service.files().get_media(fileId=file_id).execute()
    df = pd.read_csv(file_data.decode('utf-8'))
    return df

# Convert estDateTime column to datetime
def convert_to_datetime(df):
    df['estDateTime'] = pd.to_datetime(df['estDateTime'], format='%Y-%m-%d %H:%M:%S.%f%z')
    return df

# Detect anomalies using Isolation Forest
def detect_anomalies(df):
    # Select the column for anomaly detection (estDateTime)
    data = df[['estDateTime']]
    
    # Fit the Isolation Forest model
    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(data)
    
    # Predict anomaly scores
    scores = model.decision_function(data)
    df['anomaly_score'] = scores
    
    # Set a threshold for anomaly detection
    threshold = df['anomaly_score'].quantile(0.9)
    
    # Flag anomalies
    df['anomaly'] = df['anomaly_score'] > threshold
    
    return df

# Run the Streamlit app
def run_streamlit_app():
    st.title("Anomaly Detection")
    
    # Example: Search for a file in a shared folder by name
    folder_id = '1m85Uhkqe6glbcMOj2uaAqxAyX2ZWu-8z'  # Replace with the ID of the shared folder
    file_name = 'battery_view_data.csv'  # Replace with the name of the file you want to access
    
    # Search for files in the shared folder
    results = drive_service.files().list(q=f"'{folder_id}' in parents").execute()
    files = results.get('files', [])
    
    # Find the specific file within the shared folder
    file_id = None
    for file in files:
        if file['name'] == file_name:
            file_id = file['id']
            break
    
    # Read file contents into a Pandas DataFrame if found
    if file_id:
        df = load_data(file_id)
        
        # Convert estDateTime column to datetime
        df = convert_to_datetime(df)
        
        # Display a preview of the data
        st.subheader("Data Preview")
        st.write(df.head())  # Display the first few rows
        
        # Detect anomalies and perform further analysis
        df = detect_anomalies(df)
        
        # Display the DataFrame with anomalies flagged
        st.subheader("Data with Anomalies")
        st.dataframe(df)
        
    else:
        st.error('File not found in the shared folder.')


# Authenticate and run the Streamlit app
flow = InstalledAppFlow.from_client_secrets_file('/Users/ananyarajesh/Desktop/NYCSBUS/client_secret_33984701204-qjcuo9cfb8q2p5la1n140skh2b6k8425.apps.googleusercontent.com.json', ['https://www.googleapis.com/auth/drive'])
credentials = flow.run_local_server(port=0)
drive_service = build('drive', 'v3', credentials=credentials)
run_streamlit_app()