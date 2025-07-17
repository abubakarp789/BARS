import os
import re
import pandas as pd
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timezone
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import validators

# --- ENVIRONMENT SETUP ---
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', '.env'))

# --- SYNC MONGODB MANAGER FOR DASHBOARD ---
class SyncMongoDBManager:
    def __init__(self):
        self.connection_uri = os.getenv("MONGODB_URI")
        self.db_name = os.getenv("DATABASE_NAME", "bars")
        if not self.connection_uri:
            raise ValueError("MONGODB_URI environment variable not set. Cannot connect to the database.")
        self.client = MongoClient(self.connection_uri)
        self.db = self.client[self.db_name]
        self.articles = self.db['articles']
        self.deals = self.db['deals']
        self.grades = self.db['grades']

    def get_all_articles(self, limit=100):
        cursor = self.articles.find().sort("published_at", DESCENDING).limit(limit)
        return list(cursor)

    def get_all_deals(self):
        cursor = self.deals.find().sort("publication_date", DESCENDING)
        return list(cursor)

    def get_all_grades(self):
        cursor = self.grades.find().sort("score", DESCENDING)
        return list(cursor)

    def get_database_stats(self):
        return {
            "articles_count": self.articles.count_documents({}),
            "deals_count": self.deals.count_documents({}),
            "grades_count": self.grades.count_documents({}),
            "broadcasters_count": len(self.grades.distinct("broadcaster_name")),
        }
    
    def insert_deal(self, deal_data: dict):
        """Inserts a single deal record into the 'deals' collection."""
        try:
            # Ensure dates are in the correct format for MongoDB
            if isinstance(deal_data.get('publication_date'), datetime):
                deal_data['publication_date'] = deal_data['publication_date'].replace(tzinfo=timezone.utc)
            if isinstance(deal_data.get('created_at'), datetime):
                deal_data['created_at'] = deal_data['created_at'].replace(tzinfo=timezone.utc)
            
            self.deals.insert_one(deal_data)
            return True
        except Exception as e:
            st.error(f"Database Error: Failed to insert deal. {e}")
            return False

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Broadcaster Activity Rating System (BARS)",
    page_icon="📺",
    layout="wide"
)

# --- DB MANAGER SETUP ---
@st.cache_resource
def get_db_manager():
    return SyncMongoDBManager()

db_manager = get_db_manager()

# --- DATA LOADING WITH CACHING ---
@st.cache_data(ttl=600)
def load_data():
    grades_data = db_manager.get_all_grades()
    deals_data = db_manager.get_all_deals()
    articles_data = db_manager.get_all_articles(limit=100)
    stats = db_manager.get_database_stats()
    grades_df = pd.DataFrame(grades_data)
    deals_df = pd.DataFrame(deals_data)
    articles_df = pd.DataFrame(articles_data)
    if not grades_df.empty:
        grades_df['score'] = pd.to_numeric(grades_df['score'])
        grades_df = grades_df.sort_values(by='score', ascending=False).reset_index(drop=True)
    if not deals_df.empty:
        deals_df['publication_date'] = pd.to_datetime(deals_df['publication_date'], errors='coerce', utc=True)
    if not articles_df.empty:
        articles_df['published_at'] = pd.to_datetime(articles_df['published_at'], errors='coerce', utc=True)
        articles_df['published_at_str'] = articles_df['published_at'].dt.strftime('%Y-%m-%d').fillna('Unknown')
    return grades_df, deals_df, articles_df, stats, pd.Timestamp.now(tz='UTC')

# --- UI & LAYOUT ---

st.title("📺 Broadcaster Activity Rating System (BARS)")
st.markdown("An automated dashboard for tracking and grading broadcaster activity in the entertainment industry.")

# Load all data once
grades_df, deals_df, articles_df, stats, last_updated = load_data()

# Sidebar for navigation and controls
st.sidebar.title("Navigation")
page = st.sidebar.radio("Choose a page", [
    "Dashboard", 
    "Broadcaster Grades", 
    "Deal Analysis",
    "Historical Analysis",
    "Recent Articles",
    "Manual Entry"
])

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.info(f"Data last updated:\n{last_updated.strftime('%Y-%m-%d %H:%M:%S %Z')}")

# --- PAGE DISPLAY LOGIC ---

def show_dashboard():
    st.header("📊 Dashboard Overview")
    
    if grades_df.empty or deals_df.empty:
        st.warning("No data available. Please run the data collection and processing pipelines.")
        return

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Broadcasters", stats.get('broadcasters_count', 0))
    col2.metric("Total Deals Tracked", stats.get('deals_count', 0))
    grade_a_count = len(grades_df[grades_df['grade'] == 'A'])
    col3.metric("Grade 'A' Broadcasters", grade_a_count)
    most_active = grades_df.iloc[0]['broadcaster_name'] if not grades_df.empty else "N/A"
    col4.metric("Top Scorer", most_active)

    st.markdown("---")

    # Tabs for better organization
    tab1, tab2, tab3 = st.tabs(["🏆 Top 10 Broadcasters", "📈 Grade Distribution", "🔍 Score vs. Deals"])

    with tab1:
        st.subheader("Top 10 Most Active Broadcasters by Score")
        top_10 = grades_df.head(10)[['broadcaster_name', 'grade', 'score', 'deal_count', 'last_activity_date']]
        st.dataframe(top_10, use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Distribution of Broadcaster Grades")
        grade_order = ['A', 'B', 'C', 'D']
        grade_counts = grades_df['grade'].value_counts().reindex(grade_order, fill_value=0)
        fig = px.bar(grade_counts, x=grade_counts.index, y=grade_counts.values, 
                     labels={'x': 'Grade', 'y': 'Number of Broadcasters'},
                     color=grade_counts.index,
                     color_discrete_map={'A': '#28a745', 'B': '#17a2b8', 'C': '#ffc107', 'D': '#fd7e14'})
        st.plotly_chart(fig, use_container_width=True)
        
    with tab3:
        st.subheader("Score vs. Total Deals")
        fig_scatter = px.scatter(grades_df, x='deal_count', y='score', 
                                 color='grade', hover_name='broadcaster_name',
                                 title="Each point represents a broadcaster",
                                 labels={'deal_count': 'Total Deals', 'score': 'BARS Score'},
                                 color_discrete_map={'A': '#28a745', 'B': '#17a2b8', 'C': '#ffc107', 'D': '#fd7e14'})
        st.plotly_chart(fig_scatter, use_container_width=True)

def show_broadcaster_grades():
    st.header("🎯 Broadcaster Grades")
    if grades_df.empty:
        st.warning("No grades available.")
        return

    # Display all grades in a table
    st.info("Click on a row to see recent deals for that broadcaster.")
    st.dataframe(
        grades_df[['broadcaster_name', 'grade', 'score', 'deal_count']],
        use_container_width=True,
        hide_index=True
    )

def show_deal_analysis():
    st.header("🤝 Deal Analysis")
    if deals_df.empty:
        st.warning("No deals available.")
        return

    # Filters
    all_broadcasters = sorted(deals_df['broadcaster_name'].unique())
    all_deal_types = sorted(deals_df['deal_type'].unique())
    all_genres = sorted(deals_df['genres'].explode().dropna().unique()) if 'genres' in deals_df.columns else []
    all_regions = sorted(deals_df['regions'].explode().dropna().unique()) if 'regions' in deals_df.columns else []

    st.subheader("Filters")
    col1, col2 = st.columns(2)
    with col1:
        broadcaster_filter = st.multiselect("Filter by Broadcaster", all_broadcasters)
        genre_filter = st.multiselect("Filter by Genre", all_genres)
    with col2:
        deal_type_filter = st.multiselect("Filter by Deal Type", all_deal_types)
        region_filter = st.multiselect("Filter by Region", all_regions)

    # Apply filters
    filtered_deals = deals_df.copy()
    if broadcaster_filter:
        filtered_deals = filtered_deals[filtered_deals['broadcaster_name'].isin(broadcaster_filter)]
    if deal_type_filter:
        filtered_deals = filtered_deals[filtered_deals['deal_type'].isin(deal_type_filter)]
    if genre_filter:
        filtered_deals = filtered_deals[filtered_deals['genres'].apply(lambda x: isinstance(x, list) and any(g in genre_filter for g in x))]
    if region_filter:
        filtered_deals = filtered_deals[filtered_deals['regions'].apply(lambda x: isinstance(x, list) and any(r in region_filter for r in x))]
    
    st.markdown("---")
    st.subheader(f"Found {len(filtered_deals)} deals matching your criteria")
    
    # Display filtered deals
    display_df = filtered_deals[['publication_date', 'broadcaster_name', 'show_title', 'deal_type', 'source', 'article_url']].head(100)
    st.dataframe(
        display_df,
        column_config={"article_url": st.column_config.LinkColumn("Article Link")},
        use_container_width=True,
        hide_index=True
    )

def show_historical_analysis():
    st.header("📈 Historical Trend Analysis")
    if deals_df.empty:
        st.warning("No deal data available for historical analysis.")
        return

    # Broadcaster selection
    broadcasters = ["All Broadcasters"] + sorted(deals_df['broadcaster_name'].unique())
    selected_broadcaster = st.selectbox(
        "Select a broadcaster to analyze", broadcasters
    )

    # Filter data for the chart
    if selected_broadcaster == "All Broadcasters":
        chart_data = deals_df
    else:
        chart_data = deals_df[deals_df['broadcaster_name'] == selected_broadcaster]

    if chart_data.empty:
        st.info("No deals found for the selected broadcaster.")
        return
        
    st.subheader(f"Monthly Deal Volume: {selected_broadcaster}")
    
    # Drop rows where publication_date is NaT before resampling
    chart_data = chart_data.dropna(subset=['publication_date'])
    
    # Resample data to get monthly counts
    deals_by_month = chart_data.set_index('publication_date').resample('M').size().rename('deal_count')
    
    if deals_by_month.empty:
        st.info("No deal activity for the selected broadcaster.")
    else:
        st.line_chart(deals_by_month)
        st.markdown("This chart displays the total number of deals announced per month.")

def show_recent_articles():
    st.header("📰 Recent Articles")
    if articles_df.empty:
        st.warning("No articles available.")
        return

    # Filter out articles with "No title found"
    valid_articles = articles_df[articles_df['title'] != "No title found"]

    if valid_articles.empty:
        st.info("No valid recent articles to display.")
        return
        
    for _, article in valid_articles.head(50).iterrows():
        with st.expander(f"**{article['title']}** ({article['source']} - {article.get('published_at_str', 'Unknown')})"):
            st.markdown(f"**URL:** [{article['url']}]({article['url']})")
            st.markdown(f"**Content Preview:**\n\n> {article.get('content', 'No content available.')[:500]}...")

def show_manual_entry():
    st.header("✍️ Manual Deal Entry")
    st.info("This form allows you to add a new deal directly to the database.")
    st.warning("Ensure all information is accurate before submitting. Submitted data cannot be edited here.")

    # Get unique values for selectboxes to provide suggestions
    deal_types = sorted(deals_df['deal_type'].dropna().unique()) if not deals_df.empty else []
    genres = sorted(deals_df['genres'].explode().dropna().unique()) if not deals_df.empty and 'genres' in deals_df.columns else []

    with st.form("manual_deal_form", clear_on_submit=True):
        st.subheader("Enter Deal Details")
        
        c1, c2 = st.columns(2)
        with c1:
            broadcaster_name = st.text_input("Broadcaster Name*", help="e.g., Netflix, Disney+, Amazon Prime Video")
            deal_type = st.selectbox("Deal Type", options=deal_types)
            regions = st.text_input("Regions (comma-separated)", help="e.g., USA, UK, LATAM")
        with c2:
            show_title = st.text_input("Show Title*", help="e.g., Stranger Things, The Mandalorian")
            publication_date = st.date_input("Publication Date*")
            article_url = st.text_input("Source URL", help="Link to the article announcing the deal")

        genres = st.multiselect("Genres", options=genres)
        notes = st.text_area("Notes / Content Preview")

        submitted = st.form_submit_button("Add Deal to Database")

        if submitted:
            if not broadcaster_name or not show_title or not publication_date:
                st.error("Please fill out all required fields: Broadcaster Name, Show Title, and Publication Date.")
                return

            def sanitize_text(text):
                if not text: return ""
                return re.sub(r'[^\w\s.,-]', '', text).strip()

            s_broadcaster = sanitize_text(broadcaster_name)
            s_show = sanitize_text(show_title)
            s_notes = sanitize_text(notes)
            s_regions = [sanitize_text(r.strip()) for r in regions.split(',') if r.strip()]

            valid_url = ""
            if article_url:
                if validators.url(article_url):
                    valid_url = article_url
                else:
                    st.warning("The provided URL is not valid and will not be saved.")

            deal_record = {
                "broadcaster_name": s_broadcaster,
                "show_title": s_show,
                "deal_type": deal_type,
                "genres": genres,
                "regions": s_regions,
                "publication_date": datetime.combine(publication_date, datetime.min.time()),
                "source": "manual_entry",
                "article_url": valid_url,
                "content": s_notes,
                "created_at": datetime.now(timezone.utc),
            }

            if db_manager.insert_deal(deal_record):
                st.success("Deal added successfully! Refresh data to see the update.")
                st.cache_data.clear()
            else:
                st.error("Failed to add deal to the database.")

# --- MAIN APP ROUTER ---
if page == "Dashboard":
    show_dashboard()
elif page == "Broadcaster Grades":
    show_broadcaster_grades()
elif page == "Deal Analysis":
    show_deal_analysis()
elif page == "Historical Analysis":
    show_historical_analysis()
elif page == "Recent Articles":
    show_recent_articles()
elif page == "Manual Entry":
    show_manual_entry()
