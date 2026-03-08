import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "arman",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 6),
}

dag = DAG(
    dag_id="spotify_data_pipeline",
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False,
)

# Identify OUTPUT_DIR
if os.path.exists('/opt/airflow'):
    # In Docker container
    OUTPUT_DIR = '/tmp'
else:
    # Local environment fallback
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')

URL = "https://raw.githubusercontent.com/rushi4git/spotify-playlist-data/refs/heads/main/spotify_playlist.json"


def fetch_playlist_data():
    """Fetch playlist data from JSON URL and save to CSV/JSON."""
    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()
    
    df = pd.DataFrame(data["tracks"])
    
    # Expected dataset columns
    expected_columns = [
        "track_name", "artist_name", "album_name", 
        "popularity", "duration_ms", "release_date"
    ]
    df = df[expected_columns]
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    raw_csv_path = os.path.join(OUTPUT_DIR, "playlist_raw.csv")
    raw_json_path = os.path.join(OUTPUT_DIR, "playlist_raw.json")
    
    df.to_csv(raw_csv_path, index=False)
    df.to_json(raw_json_path, orient="records", indent=4)
    print(f"Row data successfully downloaded. DataFrame Head:")
    print(df.head())


def transform_playlist_data():
    """Perform data transformations using Pandas."""
    raw_csv_path = os.path.join(OUTPUT_DIR, "playlist_raw.csv")
    df = pd.read_csv(raw_csv_path)
    
    # 1. Convert duration from milliseconds to minutes
    df["duration_minutes"] = df["duration_ms"] / 60000
    
    # 2. Extract release year from release date
    # Handle potentially malformed dates gracefully
    df["release_year"] = pd.to_datetime(df["release_date"], errors='coerce').dt.year
    
    # 3. Remove duplicate tracks
    df = df.drop_duplicates(subset=["track_name", "artist_name"])
    
    # 4. Handle missing values
    df = df.dropna(subset=["track_name", "artist_name"])
    
    # 5. Create popularity_category
    def categorize_popularity(pop):
        if pd.isna(pop):
            return "Unknown"
        elif pop <= 40:
            return "Low"
        elif pop <= 70:
            return "Medium"
        else:
            return "High"
            
    df["popularity_category"] = df["popularity"].apply(categorize_popularity)
    
    transformed_csv_path = os.path.join(OUTPUT_DIR, "playlist_transformed.csv")
    transformed_json_path = os.path.join(OUTPUT_DIR, "playlist_transformed.json")
    
    df.to_csv(transformed_csv_path, index=False)
    df.to_json(transformed_json_path, orient="records", indent=4)
    print(f"Transformed data saved. DataFrame Head:")
    print(df.head())


def generate_summary_report():
    """Generate a text summary report with insights."""
    transformed_csv_path = os.path.join(OUTPUT_DIR, "playlist_transformed.csv")
    df = pd.read_csv(transformed_csv_path)
    
    report_path = os.path.join(OUTPUT_DIR, "summary_report.txt")
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("Spotify Playlist Summary Report\n")
        f.write("===============================\n\n")
        
        # Bonus Transformation 1: Count number of tracks per artist
        f.write("Tracks per Artist:\n")
        artist_counts = df.groupby("artist_name").size().sort_values(ascending=False)
        f.write(artist_counts.to_string() + "\n\n")
        
        # Bonus Transformation 2: Calculate average track duration
        avg_duration = df["duration_minutes"].mean()
        f.write(f"Average Track Duration: {avg_duration:.2f} minutes\n\n")
        
        # Bonus Transformation 3: Find Top 5 most popular tracks
        f.write("Top 5 Most Popular Tracks:\n")
        top_5 = df.nlargest(5, "popularity")[["track_name", "artist_name", "popularity"]]
        f.write(top_5.to_string(index=False) + "\n\n")
        
        # Bonus Transformation 4: Identify most frequent artist in playlist
        most_frequent = artist_counts.index[0] if not artist_counts.empty else "N/A"
        f.write(f"Most Frequent Artist: {most_frequent}\n")
        
    print(f"Summary report saved to {report_path}")


# Define Airflow tasks
fetch_task = PythonOperator(
    task_id="fetch_playlist_data",
    python_callable=fetch_playlist_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_playlist_data",
    python_callable=transform_playlist_data,
    dag=dag,
)

summary_task = PythonOperator(
    task_id="generate_summary_report",
    python_callable=generate_summary_report,
    dag=dag,
)

# Set dependencies
fetch_task >> transform_task >> summary_task
