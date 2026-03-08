# 🎵 Spotify Data Pipeline with Apache Airflow

A robust, automated data engineering pipeline that extracts, transforms, and loads Spotify playlist metadata using Apache Airflow and Pandas. 

## 👥 Team Members

| Name | USN |
| :--- | :--- |
| **Akash Ram S B** | `4SF23CI020` |
| **Arman Raj** | `4SF23CI032` |
| **Muhammad Faazil** | `4SF23CI092` |

**Course:** Data Engineering Advanced using Apache Airflow

---

## 📖 Project Overview

This project simulates a real-world batch data engineering pipeline modeled off music streaming telemetry. The pipeline systematically ingests metadata for popular tracks and applies programmatic transformations to prepare the dataset for downstream analysis.

### 🔹 Pipeline Architecture
1. **Extraction:** Fetches raw playlist metadata via HTTP request from an external JSON data source.
2. **Transformation:** Leverages Python and Pandas to:
   - Convert track durations from milliseconds to human-readable minutes.
   - Extract the `release_year` from raw release dates.
   - Categorize track popularity into distinct tiers (`Low`, `Medium`, `High`).
   - Clean data by identifying and removing duplicates and `Null`/missing values.
3. **Loading:** Persists the processed data into local container volumes as `CSV` and `JSON` artifacts, alongside an automated statistical summary report.

---

## ⚙️ Technologies Used

| Technology | Purpose |
| :--- | :--- |
| **Apache Airflow** | Workflow orchestration and task scheduling |
| **Docker** | Containerization of the Airflow environment and its dependencies (Redis/Postgres) |
| **Python 3** | Core pipeline language |
| **Pandas** | In-memory data manipulation and analytical transformations |

---

## 🚀 Quick Start Guide

### Prerequisites
- Docker & Docker Compose installed natively or via WSL2.

### Execution Steps

1. **Initialize the Airflow Infrastructure**
   Launch the pre-configured Docker Compose cluster:
   ```bash
   docker-compose up -d
   ```

2. **Access the Airflow Dashboard**
   Open your browser and navigate to the Airflow Web UI:
   - **URL:** `http://localhost:8087` *(or your custom configured port)*
   - **Username:** `airflow`
   - **Password:** `airflow`

3. **Trigger the Pipeline**
   - Locate the `spotify_data_pipeline` DAG in the dashboard.
   - Click the toggle on the left to unpause the DAG.
   - Click the **▶ Trigger DAG** button to manually execute the workflow.

4. **Review Artifacts**
   Once the DAG turns green (Success), inspect your local `output/` directory for the newly generated models.

---

## 📂 Output Deliverables

The pipeline seamlessly generates the following analytical assets:

1. **Raw Backups:** 
   - `playlist_raw.csv` / `playlist_raw.json`
2. **Transformed Models:** 
   - `playlist_transformed.csv` / `playlist_transformed.json`
3. **Statistical Summary (`summary_report.txt`):**

> **📊 Sample Insights Generation**
> ```text
> Spotify Playlist Summary Report
> ===============================
> 
> Tracks per Artist:
> Ed Sheeran              7
> Arijit Singh            7
> Coldplay                5
> OneRepublic             3
> ...
> 
> Average Track Duration: 3.71 minutes
> 
> Top 5 Most Popular Tracks:
>      track_name     artist_name  popularity
> Blinding Lights      The Weeknd          98
>    Shape of You      Ed Sheeran          97
>       Tum Hi Ho    Arijit Singh          95
>        Believer Imagine Dragons          94
>       Sunflower     Post Malone          94
> ```

---

## 🗓️ Scheduling Configuration

The DAG is configured for automated execution at a daily cadence:
- **Interval:** `@daily` (Midnight UTC)
- **Parameter:** `schedule=timedelta(days=1)`

---

## 📈 Airflow DAG Execution Graph 

![Airflow DAG Execution](Screenshot%202026-03-07%20120004.png)
