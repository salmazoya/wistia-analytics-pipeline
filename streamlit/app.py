"""
Wistia Analytics Dashboard
Built with Streamlit + Athena

Install requirements:
pip install streamlit pyathena pandas plotly boto3
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyathena import connect
from datetime import datetime, timedelta
import os
from functools import lru_cache

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Wistia Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# STYLING
# ============================================================================

st.markdown("""
    <style>
        .main {
            padding: 0rem 1rem;
        }
        h1 {
            color: #1f77b4;
            font-size: 3em;
            margin-bottom: 0.5rem;
        }
        h2 {
            color: #1f77b4;
            margin-top: 2rem;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 1.5rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# ATHENA CONNECTION
# ============================================================================

@st.cache_resource
def get_athena_connection():
    """Create cached Athena connection"""
    return connect(
        s3_output='s3://wistia-analytics-athena-results/',
        region_name='us-east-1'
    )

@lru_cache(maxsize=128)
def run_query(query: str) -> pd.DataFrame:
    """Execute Athena query and return as DataFrame"""
    try:
        conn = get_athena_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        
        if not results:
            return pd.DataFrame()
        
        return pd.DataFrame(results, columns=columns)
    except Exception as e:
        st.error(f"Query error: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# PAGE HEADER
# ============================================================================

col1, col2 = st.columns([3, 1])
with col1:
    st.title("📊 Wistia Analytics Dashboard")
    st.markdown("Real-time video engagement metrics and performance tracking")

with col2:
    st.markdown("")
    st.markdown("")
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.caption(f"Last updated: {last_updated}")

st.divider()

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

st.sidebar.header("📋 Filters")

# Date range selector
date_range = st.sidebar.select_slider(
    "Select date range",
    options=["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
    value="Last 30 days"
)

date_mapping = {
    "Last 7 days": 7,
    "Last 30 days": 30,
    "Last 90 days": 90,
    "All time": 999999
}
days_back = date_mapping[date_range]

st.sidebar.divider()

# ============================================================================
# KEY METRICS
# ============================================================================

st.header("📈 Key Metrics")

# Query for overall metrics
metrics_query = f"""
SELECT
    SUM(page_loads) as total_page_loads,
    SUM(plays) as total_plays,
    SUM(unique_visitors) as unique_visitors,
    ROUND(AVG(play_rate), 4) as avg_play_rate,
    ROUND(SUM(time_played_seconds) / 3600.0, 2) as total_watch_hours,
    COUNT(DISTINCT media_id) as videos_tracked,
    ROUND(AVG(avg_engagement_pct), 2) as avg_engagement_pct
FROM wistia_analytics.fact_media_daily
WHERE date_key >= CURRENT_DATE - INTERVAL '{days_back}' DAY
"""

metrics_df = run_query(metrics_query)

if not metrics_df.empty:
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total Page Loads",
            f"{int(metrics_df.iloc[0]['total_page_loads']):,}",
            delta="Views"
        )
    
    with col2:
        st.metric(
            "Total Plays",
            f"{int(metrics_df.iloc[0]['total_plays']):,}",
            delta="Videos Started"
        )
    
    with col3:
        st.metric(
            "Unique Visitors",
            f"{int(metrics_df.iloc[0]['unique_visitors']):,}",
            delta="People"
        )
    
    with col4:
        st.metric(
            "Avg Play Rate",
            f"{metrics_df.iloc[0]['avg_play_rate']:.1%}",
            delta="Conversion"
        )
    
    with col5:
        st.metric(
            "Watch Hours",
            f"{metrics_df.iloc[0]['total_watch_hours']:,.0f}",
            delta="Hours"
        )

st.divider()

# ============================================================================
# DAILY TRENDS
# ============================================================================

st.header("📊 Daily Engagement Trends")

trends_query = f"""
SELECT
    date_key,
    SUM(page_loads) as page_loads,
    SUM(plays) as plays,
    SUM(unique_visitors) as unique_visitors,
    ROUND(AVG(play_rate), 4) as play_rate
FROM wistia_analytics.fact_media_daily
WHERE date_key >= CURRENT_DATE - INTERVAL '{days_back}' DAY
GROUP BY date_key
ORDER BY date_key DESC
"""

trends_df = run_query(trends_query)

if not trends_df.empty:
    trends_df['date_key'] = pd.to_datetime(trends_df['date_key'])
    trends_df = trends_df.sort_values('date_key')
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_plays = px.line(
            trends_df,
            x='date_key',
            y='plays',
            title='Daily Plays Trend',
            markers=True,
            labels={'date_key': 'Date', 'plays': 'Total Plays'}
        )
        fig_plays.update_layout(hovermode='x unified')
        st.plotly_chart(fig_plays, use_container_width=True)
    
    with col2:
        fig_visitors = px.bar(
            trends_df,
            x='date_key',
            y='unique_visitors',
            title='Daily Unique Visitors',
            labels={'date_key': 'Date', 'unique_visitors': 'Unique Visitors'},
            color='unique_visitors',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_visitors, use_container_width=True)

st.divider()

# ============================================================================
# TOP PERFORMING VIDEOS
# ============================================================================

st.header("🏆 Top Performing Videos")

top_videos_query = f"""
SELECT
    m.title,
    m.media_id,
    SUM(f.plays) as total_plays,
    SUM(f.unique_visitors) as unique_visitors,
    ROUND(AVG(f.play_rate), 4) as avg_play_rate,
    ROUND(SUM(f.time_played_seconds) / 3600.0, 2) as watch_hours,
    ROUND(AVG(f.avg_engagement_pct), 2) as avg_engagement_pct
FROM wistia_analytics.fact_media_daily f
JOIN wistia_analytics.dim_media m ON f.media_id = m.media_id
WHERE f.date_key >= CURRENT_DATE - INTERVAL '{days_back}' DAY
GROUP BY m.title, m.media_id
ORDER BY total_plays DESC
LIMIT 10
"""

top_videos_df = run_query(top_videos_query)

if not top_videos_df.empty:
    # Display as table
    st.subheader("Top 10 Videos by Plays")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_top = px.bar(
            top_videos_df.head(10),
            x='total_plays',
            y='title',
            orientation='h',
            title='Total Plays',
            labels={'total_plays': 'Plays', 'title': 'Video Title'},
            color='total_plays',
            color_continuous_scale='Viridis'
        )
        fig_top.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig_top, use_container_width=True)
    
    with col2:
        fig_engagement = px.scatter(
            top_videos_df.head(10),
            x='avg_play_rate',
            y='avg_engagement_pct',
            size='total_plays',
            hover_name='title',
            title='Play Rate vs Engagement',
            labels={'avg_play_rate': 'Play Rate', 'avg_engagement_pct': 'Avg Engagement %'},
            color='total_plays',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_engagement, use_container_width=True)
    
    # Show detailed table
    st.subheader("Video Performance Details")
    display_df = top_videos_df.copy()
    display_df = display_df.drop('media_id', axis=1)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# ENGAGEMENT ANALYSIS
# ============================================================================

st.header("🎯 Engagement Analysis")

engagement_query = f"""
SELECT
    m.title,
    ROUND(AVG(f.avg_engagement_pct), 2) as avg_engagement,
    SUM(f.plays) as total_plays,
    CASE
        WHEN AVG(f.avg_engagement_pct) >= 80 THEN 'High'
        WHEN AVG(f.avg_engagement_pct) >= 50 THEN 'Medium'
        ELSE 'Low'
    END as engagement_level
FROM wistia_analytics.fact_media_daily f
JOIN wistia_analytics.dim_media m ON f.media_id = m.media_id
WHERE f.date_key >= CURRENT_DATE - INTERVAL '{days_back}' DAY
GROUP BY m.title
ORDER BY avg_engagement DESC
"""

engagement_df = run_query(engagement_query)

if not engagement_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        fig_engagement_dist = px.histogram(
            engagement_df,
            x='avg_engagement',
            nbins=20,
            title='Engagement Distribution',
            labels={'avg_engagement': 'Engagement %'},
            color_discrete_sequence=['#1f77b4']
        )
        st.plotly_chart(fig_engagement_dist, use_container_width=True)
    
    with col2:
        engagement_level_counts = engagement_df['engagement_level'].value_counts()
        fig_pie = px.pie(
            values=engagement_level_counts.values,
            names=engagement_level_counts.index,
            title='Videos by Engagement Level',
            color_discrete_map={
                'High': '#2ecc71',
                'Medium': '#f39c12',
                'Low': '#e74c3c'
            }
        )
        st.plotly_chart(fig_pie, use_container_width=True)

st.divider()

# ============================================================================
# WEEKLY SUMMARY
# ============================================================================

st.header("📅 Weekly Summary")

weekly_query = f"""
SELECT
    FLOOR((CAST(date_key AS INTEGER) - CAST(CURRENT_DATE AS INTEGER)) / 7) as weeks_ago,
    COUNT(DISTINCT date_key) as days_in_week,
    SUM(page_loads) as weekly_page_loads,
    SUM(plays) as weekly_plays,
    SUM(unique_visitors) as weekly_visitors,
    ROUND(AVG(play_rate), 4) as weekly_play_rate
FROM wistia_analytics.fact_media_daily
WHERE date_key >= CURRENT_DATE - INTERVAL '{days_back}' DAY
GROUP BY FLOOR((CAST(date_key AS INTEGER) - CAST(CURRENT_DATE AS INTEGER)) / 7)
ORDER BY weeks_ago ASC
LIMIT 12
"""

weekly_df = run_query(weekly_query)

if not weekly_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        fig_weekly_plays = px.bar(
            weekly_df,
            x='weeks_ago',
            y='weekly_plays',
            title='Weekly Plays',
            labels={'weeks_ago': 'Weeks Ago', 'weekly_plays': 'Total Plays'}
        )
        st.plotly_chart(fig_weekly_plays, use_container_width=True)
    
    with col2:
        fig_weekly_visitors = px.line(
            weekly_df,
            x='weeks_ago',
            y='weekly_visitors',
            title='Weekly Unique Visitors',
            markers=True,
            labels={'weeks_ago': 'Weeks Ago', 'weekly_visitors': 'Visitors'}
        )
        st.plotly_chart(fig_weekly_visitors, use_container_width=True)

st.divider()

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("""
    ---
    **About this dashboard:**
    - Data source: Wistia Analytics via Athena
    - Updated: Daily at 2 AM UTC
    - Powered by: Streamlit + Athena + Parquet
    
    **Questions?** Contact the analytics team
""")
