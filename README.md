# 🚦 Traffic Violations Insight System

A scalable data analytics pipeline for processing and analyzing 2M+ traffic violation records with interactive insights

---

## Problem Statement

Urban traffic systems generate massive datasets, but extracting actionable insights from raw violation records is complex due to:

* Inconsistent and noisy data
* Lack of structured pipelines
* Limited real-time exploratory capabilities

This project solves these challenges by building a **modular data pipeline + analytics layer + visualization interface**.

---

## System Architecture

```
Raw CSV Dataset
      ↓
Data Cleaning & Feature Engineering (Pandas)
      ↓
Processed Dataset
      ↓
MySQL Database (SQLAlchemy)
      ↓
Analytical Queries
      ↓
Streamlit Dashboard (Visualization Layer)
```

---

## Core Components

### 1. Data Processing Layer

* Handles **2M+ rows efficiently**
* Performs:

  * Data cleaning (nulls, duplicates)
  * Standardization (categorical + formats)
  * Feature engineering:

    * `time_of_day`
    * `stop_hour`
    * `day_of_week`
    * `month`

---

### 2. Storage Layer (MySQL)

* Structured schema for analytical queries
* Optimized for:

  * Filtering
  * Aggregations
  * Time-based queries

---

### 3. Analytics Layer

* EDA answering:

  * Violation distribution
  * Temporal trends
  * Demographic patterns
  * High-risk zones

---

### 4. Visualization Layer (Streamlit)

* Interactive dashboard with:

  * Multi-dimensional filters
  * Time-series analysis
  * Geo heatmaps
  * KPI summaries

---

## Key Engineering Highlights

* Handles **large-scale dataset (~1M rows)**
* Efficient data pipeline with clear separation of concerns
* Modular code structure (processing / DB / UI layers)
* Query-driven insights via MySQL
* Geospatial visualization using Folium
* Interactive analytics with minimal latency

---

## Tech Stack

| Category        | Tools                       |
| --------------- | --------------------------- |
| Language        | Python 3.10+                |
| Data Processing | Pandas, NumPy               |
| Visualization   | Plotly, Seaborn, Matplotlib |
| Backend Storage | MySQL, SQLAlchemy           |
| Dashboard       | Streamlit                   |
| Geo Mapping     | Folium                      |

---

## Repository Structure

```id="y2k6nq"
Traffic_System/
├── dataset/              # Raw data source
├── data/processed/       # Cleaned dataset
├── src/                  # Data pipeline logic
├── app/                  # Visualization layer
├── sql/                  # DB schema & loaders
├── reports/              # Generated insights
└── README.md
```

---

## Setup

```bash id="b6g8mk"
git clone https://github.com/saravana0801/traffic-violations.git
cd Traffic_System
pip install -r requirements.txt
```

Configure `.env` and MySQL, then run:

```bash id="a1pnzq"
python src/preprocessing.py
python sql/db_loader.py
streamlit run app/dashboard.py
```

---

## Example Insights

* Peak violations occur during **specific hourly windows**
* Certain locations consistently act as **traffic hotspots**
* Violation patterns vary across **demographics and vehicle types**
* Accident-related violations show **distinct clustering patterns**

---
