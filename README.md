# BARS Enhanced - Broadcaster Activity Rating System

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.25+-red.svg)
![MongoDB](https://img.shields.io/badge/MongoDB-4.4+-green.svg)

## Overview

BARS (Broadcaster Activity Rating System) is an automated data pipeline and analysis tool designed to provide strategic insights into the global entertainment content market. It systematically scrapes news from leading industry publications, uses Natural Language Processing (NLP) to extract and structure deal-making information, and scores broadcasters based on their recent activity. The results are presented in an interactive Streamlit dashboard, allowing for easy exploration and analysis of market trends.

## Features

- **Automated Web Scraping**: Concurrently scrapes articles from multiple industry sources (Variety, Kidscreen, etc.) using a robust Playwright-based framework.
- **Intelligent NLP Extraction**: Leverages spaCy for Named Entity Recognition (NER) to identify broadcasters, show titles, deal types, genres, and regions from unstructured article text.
- **Advanced Grading Engine**: Scores and grades broadcasters using a weighted algorithm that considers deal volume, recency, and type. It uses MongoDB's aggregation framework for high performance.
- **Interactive Dashboard**: A comprehensive Streamlit dashboard provides filtered views, summary metrics, and historical trend visualizations of broadcaster activity.
- **Centralized Configuration**: Key settings, such as grading weights and CSS selectors, are stored in configuration files for easy maintenance.
- **Secure Environment Management**: Uses `.env` files for secure handling of sensitive credentials like database URIs.

## Project Structure

```
BARS_Enhanced_Project/
│
├── core/              # Core application logic (DB Manager, NLP, Grading Engine)
├── scrapers/          # Individual scraper modules for each data source
├── dashboard/         # Streamlit user interface code
├── .env.example       # Template for environment variables
├── requirements.txt   # Python package dependencies
└── enhanced_run_complete_pipeline.py  # Main script to run the entire pipeline
```

## Prerequisites

To run this project, you will need the following software installed:
- Python 3.9+
- MongoDB
- Git

## Setup and Installation

Follow these steps to get your development environment set up:

1.  **Clone the Repository**
```bash
    git clone <repository-url>
cd BARS_Enhanced_Project
```

2.  **Create and Activate a Virtual Environment**
```bash
    # For Windows
    python -m venv venv
    .\\venv\\Scripts\\activate

    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Dependencies**
```bash
pip install -r requirements.txt
```

4.  **Set Up Environment Variables**
    Create a `.env` file in the project root by copying the template.
```bash
    cp .env.example .env
    ```
    Open the new `.env` file and replace the placeholder with your MongoDB connection string.
    ```
    # .env
    MONGODB_URI="mongodb://your-mongodb-host:27017/"
    ```

## Usage

### Running the Full Pipeline
To run the entire data pipeline (scraping, NLP, and grading), execute the main script:
```bash
python enhanced_run_complete_pipeline.py
```
You can run it for specific sources or in test mode:
```bash
# Run for specific sources
python enhanced_run_complete_pipeline.py --sources variety,kidscreen

# Run in test mode (processes a limited number of articles)
python enhanced_run_complete_pipeline.py --test-mode
```

### Launching the Dashboard
To view the results and explore the data, launch the Streamlit dashboard:
```bash
streamlit run dashboard/enhanced_dashboard.py
```

## Troubleshooting

-   **Scraper Fails or Returns No Data**:
    -   **Cause**: The target website's layout (HTML/CSS) has likely changed, making the CSS selectors in the scraper obsolete.
    -   **Solution**: Open the relevant scraper file in the `scrapers/` directory. Update the CSS selector constants at the top of the file to match the new website structure.
-   **Database Connection Issues**:
    -   **Cause**: The `MONGODB_URI` in your `.env` file may be incorrect, or the MongoDB server may not be running.
    -   **Solution**: Verify that your MongoDB server is active and that the connection string in the `.env` file is correct.
-   **NLP Extraction is Inaccurate**:
    -   **Cause**: The spaCy model may not be correctly identifying entities, or the keyword lists may need updating.
    -   **Solution**: In `core/enhanced_nlp_extractor.py`, expand the `known_broadcasters`, `deal_keywords`, `genre_keywords`, or `region_keywords` lists to improve accuracy. For significant improvements, consider training a custom spaCy NER model.

## Future Enhancements

-   **Alerting System**: Implement an automated email or Slack notification system to alert stakeholders when a key broadcaster (e.g., Grade 'A') makes a new deal.
-   **Dynamic Weighting**: Allow users to adjust the weights for deal types and other scoring parameters directly from the dashboard to run custom analysis scenarios.
-   **Advanced NLP**: Integrate a more powerful language model or custom-trained NER models to improve the accuracy and granularity of deal extraction.
-   **User Authentication**: Add a login system to the dashboard to secure access for different user roles.
-   **Task Scheduling**: Integrate a scheduler like APScheduler or use system cron jobs to run the data pipeline automatically on a recurring basis.


