import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyathena import connect
from datetime import datetime, timedelta
import time

# Page configuration
st.set_page_config(
    page_title="Wistia Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)

# Title
st.markdown("# 📊 Wistia Analytics Dashboard")
st.markdown("*Real-time video engagement metrics and performance tracking*")
st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

# Sidebar filters
st.sidebar.markdown("## 🔍 Filters")
date_range = st.sidebar.radio("Select date range:", ["Last 7 days", "Last 30 days", "All time"])

# ============================================================================
# CONNECT TO ATHENA
# ============================================================================

@st.cache_resource
def get_athena_connection():
    """Create cached Athena connection"""
    try:
        conn = connect(
            s3_output='s3://wistia-analytics-athena-results/',
            region_name='us-east-1'
        )
        return conn
    except Exception as e:
        st.error(f"❌ Failed to connect to Athena: {str(e)}")
        return None

@st.cache_data(ttl=300)
def query_athena(sql_query):
    """Execute Athena query with caching (5 min TTL)"""
    try:
        conn = get_athena_connection()
        if conn is None:
            return None
        
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        if not results:
            return pd.DataFrame()
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=columns)
        
        cursor.close()
        return df
        
    except Exception as e:
        st.warning(f"⚠️ Query error: {str(e)}")
        return None

# ============================================================================
# KEY METRICS
# ============================================================================

st.markdown("## 📈 Key Metrics")

try:
    # Total Media
    total_media_query = "SELECT COUNT(*) as total FROM wistia_analytics.dim_media"
    total_media_df = query_athena(total_media_query)
    
    # Total Engagement
    engagement_query = "SELECT SUM(plays) as total_plays, SUM(unique_visitors) as total_visitors FROM wistia_analytics.fact_engagement"
    engagement_df = query_athena(engagement_query)
    
    # Create metric columns
    col1, col2, col3, col4 = st.columns(4)
    
    if total_media_df is not None and not total_media_df.empty:
        total_media = int(total_media_df['total'].values[0])
        col1.metric("📹 Total Videos", f"{total_media:,}")
    
    if engagement_df is not None and not engagement_df.empty:
        total_plays = int(engagement_df['total_plays'].values[0]) if engagement_df['total_plays'].values[0] else 0
        total_visitors = int(engagement_df['total_visitors'].values[0]) if engagement_df['total_visitors'].values[0] else 0
        
        col2.metric("▶️ Total Plays", f"{total_plays:,}")
        col3.metric("👥 Total Visitors", f"{total_visitors:,}")
        
        if total_plays > 0 and total_visitors > 0:
            avg_play_rate = (total_plays / total_visitors) * 100
            col4.metric("📊 Avg Play Rate", f"{avg_play_rate:.1f}%")

except Exception as e:
    st.error(f"❌ Error loading metrics: {str(e)}")

# ============================================================================
# TOP PERFORMING VIDEOS
# ============================================================================

st.markdown("## 🏆 Top Performing Videos")

try:
    top_videos_query = """
    SELECT 
        dm.title,
        dm.duration_seconds,
        dm.project_name,
        fe.plays,
        fe.unique_visitors,
        fe.engagement_pct,
        ROUND(fe.play_rate * 100, 2) as play_rate_pct
    FROM wistia_analytics.dim_media dm
    JOIN wistia_analytics.fact_engagement fe ON dm.media_id = fe.media_id
    ORDER BY fe.plays DESC
    LIMIT 20
    """
    
    top_videos_df = query_athena(top_videos_query)
    
    if top_videos_df is not None and not top_videos_df.empty:
        st.dataframe(top_videos_df, use_container_width=True)
        
        # Chart: Top videos by plays
        fig = px.bar(
            top_videos_df.head(10),
            x='title',
            y='plays',
            title='Top 10 Videos by Plays',
            labels={'plays': 'Play Count', 'title': 'Video Title'},
            color='engagement_pct',
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No video data available yet.")
        
except Exception as e:
    st.error(f"❌ Error loading top videos: {str(e)}")

# ============================================================================
# ENGAGEMENT ANALYSIS
# ============================================================================

st.markdown("## 📊 Engagement Analysis")

try:
    engagement_analysis_query = """
    SELECT 
        media_name,
        plays,
        unique_visitors,
        ROUND(engagement_pct, 2) as engagement_pct,
        ROUND(hours_watched, 2) as hours_watched
    FROM wistia_analytics.fact_engagement
    WHERE plays > 0
    ORDER BY engagement_pct DESC
    LIMIT 15
    """
    
    engagement_analysis_df = query_athena(engagement_analysis_query)
    
    if engagement_analysis_df is not None and not engagement_analysis_df.empty:
        col1, col2 = st.columns(2)
        
        # Chart 1: Engagement vs Plays
        with col1:
            fig1 = px.scatter(
                engagement_analysis_df,
                x='plays',
                y='engagement_pct',
                size='unique_visitors',
                hover_name='media_name',
                title='Engagement vs Plays',
                labels={'plays': 'Plays', 'engagement_pct': 'Engagement %'}
            )
            st.plotly_chart(fig1, use_container_width=True)
        
        # Chart 2: Hours Watched Distribution
        with col2:
            fig2 = px.bar(
                engagement_analysis_df.head(10),
                x='media_name',
                y='hours_watched',
                title='Top 10 Videos by Watch Time',
                labels={'hours_watched': 'Hours Watched', 'media_name': 'Video'}
            )
            fig2.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No engagement data available yet.")
        
except Exception as e:
    st.error(f"❌ Error loading engagement analysis: {str(e)}")

# ============================================================================
# VIDEO LIBRARY
# ============================================================================

st.markdown("## 📚 Complete Video Library")

try:
    library_query = """
    SELECT 
        media_id,
        title,
        duration_seconds,
        media_type,
        status,
        project_name,
        created_at
    FROM wistia_analytics.dim_media
    ORDER BY created_at DESC
    LIMIT 50
    """
    
    library_df = query_athena(library_query)
    
    if library_df is not None and not library_df.empty:
        st.dataframe(library_df, use_container_width=True)
    else:
        st.info("No videos in library yet.")
        
except Exception as e:
    st.error(f"❌ Error loading video library: {str(e)}")

# ============================================================================
# RAW DATA EXPLORER
# ============================================================================

st.markdown("## 🔬 Raw Data Explorer")

tab1, tab2 = st.tabs(["Dimension Tables", "Fact Tables"])

with tab1:
    st.markdown("### DIM_MEDIA (Video Metadata)")
    try:
        dim_query = "SELECT * FROM wistia_analytics.dim_media LIMIT 100"
        dim_df = query_athena(dim_query)
        if dim_df is not None and not dim_df.empty:
            st.dataframe(dim_df, use_container_width=True)
        else:
            st.info("No data in DIM_MEDIA")
    except Exception as e:
        st.error(f"Error: {str(e)}")

with tab2:
    st.markdown("### FACT_ENGAGEMENT (Engagement Metrics)")
    try:
        fact_query = "SELECT * FROM wistia_analytics.fact_engagement LIMIT 100"
        fact_df = query_athena(fact_query)
        if fact_df is not None and not fact_df.empty:
            st.dataframe(fact_df, use_container_width=True)
        else:
            st.info("No data in FACT_ENGAGEMENT")
    except Exception as e:
        st.error(f"Error: {str(e)}")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #888; font-size: 12px;'>
    <p>🚀 Wistia Analytics Pipeline | Powered by AWS Lambda, Glue, Athena & Streamlit</p>
    <p>Data updates daily at 2 AM UTC via EventBridge automation</p>
    </div>
    """, unsafe_allow_html=True)