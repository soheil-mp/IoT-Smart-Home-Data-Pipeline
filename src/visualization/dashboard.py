import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine
import plotly.io as pio
import numpy as np

# Set Plotly theme
pio.templates.default = "plotly_dark"

# Database configuration
DB_CONFIG = {
    "dbname": "iot_db",
    "user": "iot_user",
    "password": "iot_password",
    "host": "localhost",
    "port": "5432"
}

# Custom color scheme
COLORS = {
    'background': '#0E1117',
    'text': '#FFFFFF',
    'accent': '#FF4B4B',
    'success': '#00CC96',
    'warning': '#FFA500',
    'info': '#00D4FF',
    'grid': '#1F2937',
    'card': '#1E1E1E',
    'gradient1': '#FF4B4B',
    'gradient2': '#00D4FF'
}

def get_db_engine():
    """Create a SQLAlchemy engine."""
    return create_engine(
        f'postgresql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'
    )

def get_latest_readings():
    """Get the latest readings for all sensors."""
    engine = get_db_engine()
    
    # Latest temperature readings
    temp_df = pd.read_sql_query("""
        SELECT DISTINCT ON (t.sensor_id)
            t.sensor_id, t.value, t.timestamp,
            s.location
        FROM temperature_readings t
        JOIN sensors s ON t.sensor_id = s.sensor_id
        ORDER BY t.sensor_id, t.timestamp DESC
    """, engine)
    
    # Latest humidity readings
    humidity_df = pd.read_sql_query("""
        SELECT DISTINCT ON (h.sensor_id)
            h.sensor_id, h.value, h.timestamp,
            s.location
        FROM humidity_readings h
        JOIN sensors s ON h.sensor_id = s.sensor_id
        ORDER BY h.sensor_id, h.timestamp DESC
    """, engine)
    
    # Latest motion events
    motion_df = pd.read_sql_query("""
        SELECT DISTINCT ON (m.sensor_id)
            m.sensor_id, m.detected as value, m.timestamp,
            s.location
        FROM motion_events m
        JOIN sensors s ON m.sensor_id = s.sensor_id
        ORDER BY m.sensor_id, m.timestamp DESC
    """, engine)
    
    return temp_df, humidity_df, motion_df

def get_historical_data(hours=24):
    """Get historical data for trend analysis."""
    engine = get_db_engine()
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    # Temperature trends
    temp_trends = pd.read_sql_query("""
        SELECT 
            t.sensor_id,
            date_trunc('hour', t.timestamp) as hour,
            AVG(t.value) as avg_value,
            MIN(t.value) as min_value,
            MAX(t.value) as max_value,
            s.location
        FROM temperature_readings t
        JOIN sensors s ON t.sensor_id = s.sensor_id
        WHERE t.timestamp >= %(start_time)s
        GROUP BY t.sensor_id, date_trunc('hour', t.timestamp), s.location
        ORDER BY hour
    """, engine, params={"start_time": start_time})
    
    # Humidity trends
    humidity_trends = pd.read_sql_query("""
        SELECT 
            h.sensor_id,
            date_trunc('hour', h.timestamp) as hour,
            AVG(h.value) as avg_value,
            MIN(h.value) as min_value,
            MAX(h.value) as max_value,
            s.location
        FROM humidity_readings h
        JOIN sensors s ON h.sensor_id = s.sensor_id
        WHERE h.timestamp >= %(start_time)s
        GROUP BY h.sensor_id, date_trunc('hour', h.timestamp), s.location
        ORDER BY hour
    """, engine, params={"start_time": start_time})
    
    # Motion activity trends
    motion_trends = pd.read_sql_query("""
        SELECT 
            m.sensor_id,
            date_trunc('hour', m.timestamp) as hour,
            COUNT(*) as total_events,
            SUM(CASE WHEN m.detected THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as activity_rate,
            s.location
        FROM motion_events m
        JOIN sensors s ON m.sensor_id = s.sensor_id
        WHERE m.timestamp >= %(start_time)s
        GROUP BY m.sensor_id, date_trunc('hour', m.timestamp), s.location
        ORDER BY hour
    """, engine, params={"start_time": start_time})
    
    return temp_trends, humidity_trends, motion_trends

def create_gauge(value, title, min_val, max_val, threshold, unit=""):
    """Create a styled gauge chart."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'color': COLORS['text'], 'size': 16}},
        number={'suffix': unit, 'font': {'color': COLORS['text'], 'size': 20}},
        gauge={
            'axis': {
                'range': [min_val, max_val],
                'tickwidth': 2,
                'tickcolor': COLORS['text']
            },
            'bar': {'color': COLORS['info']},
            'bgcolor': COLORS['background'],
            'borderwidth': 1,
            'bordercolor': COLORS['text'],
            'steps': [
                {'range': [min_val, (max_val-min_val)*0.33 + min_val], 'color': COLORS['success']},
                {'range': [(max_val-min_val)*0.33 + min_val, (max_val-min_val)*0.66 + min_val], 'color': COLORS['warning']},
                {'range': [(max_val-min_val)*0.66 + min_val, max_val], 'color': COLORS['accent']}
            ],
            'threshold': {
                'line': {'color': COLORS['accent'], 'width': 2},
                'thickness': 0.75,
                'value': threshold
            }
        }
    ))
    
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': COLORS['text']},
        margin=dict(l=10, r=10, t=30, b=10),
        height=180
    )
    return fig

def create_trend_chart(df, x, y, color, title, y_label):
    """Create a styled trend chart."""
    fig = px.line(df, x=x, y=y, color=color, title=title)
    
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': COLORS['text']},
        title={
            'font': {'size': 16},
            'x': 0.5,
            'xanchor': 'center'
        },
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            bordercolor=COLORS['text'],
            borderwidth=1,
            font={'size': 10}
        ),
        xaxis=dict(
            gridcolor=COLORS['grid'],
            title_font={'size': 12}
        ),
        yaxis=dict(
            gridcolor=COLORS['grid'],
            title=y_label,
            title_font={'size': 12}
        ),
        margin=dict(l=10, r=10, t=30, b=10),
        height=250
    )
    
    return fig

def create_heatmap(df, title):
    """Create a styled heatmap."""
    pivot_df = df.pivot_table(
        values='activity_rate',
        index='location',
        columns=pd.to_datetime(df['hour']).dt.strftime('%H:00'),
        aggfunc='mean'
    ).fillna(0)
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale=[[0, COLORS['success']], [0.5, COLORS['warning']], [1, COLORS['accent']]],
    ))
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': COLORS['text'], 'size': 12},
        margin=dict(l=10, r=10, t=30, b=10),
        height=200
    )
    
    return fig

def main():
    # Page config
    st.set_page_config(
        page_title="Smart Home IoT Dashboard",
        page_icon="🏠",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    # Custom CSS
    st.markdown("""
        <style>
        .stApp {
            background: linear-gradient(135deg, #0E1117 0%, #1F2937 100%);
        }
        .stMetric {
            background-color: rgba(31, 41, 55, 0.5);
            padding: 5px;
            border-radius: 8px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .stMetric label {
            font-size: 0.8rem !important;
        }
        .stMetric .css-1wivap2 {
            font-size: 1.2rem !important;
        }
        .stAlert {
            background-color: rgba(31, 41, 55, 0.5);
            color: white;
            border: 1px solid rgba(255, 75, 75, 0.5);
            backdrop-filter: blur(10px);
            padding: 5px !important;
            font-size: 0.9rem !important;
        }
        .css-1d391kg {
            padding-top: 0.5rem;
        }
        .stProgress .st-bo {
            background: linear-gradient(90deg, #FF4B4B 0%, #00D4FF 100%);
        }
        .sensor-card {
            background-color: rgba(31, 41, 55, 0.5);
            border-radius: 8px;
            padding: 0.5rem;
            border: 1px solid rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            margin-bottom: 0.5rem;
        }
        .glass-card {
            background-color: rgba(31, 41, 55, 0.5);
            border-radius: 8px;
            padding: 0.8rem;
            border: 1px solid rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            margin: 0.3rem 0;
        }
        .header-gradient {
            background: linear-gradient(90deg, #FF4B4B 0%, #00D4FF 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-size: 1.5rem !important;
            margin: 0 !important;
            padding: 0 !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 4px 8px;
            font-size: 0.8rem;
        }
        h1, h2, h3, h4, h5, h6 {
            margin: 0 !important;
            padding: 0.2rem 0 !important;
        }
        .plot-container {
            margin: 0 !important;
            padding: 0 !important;
        }
        .js-plotly-plot {
            margin: 0 !important;
            padding: 0 !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Header
    col1, col2 = st.columns([2, 3])
    with col1:
        st.markdown('<h1 class="header-gradient">🏠 Smart Home IoT</h1>', unsafe_allow_html=True)
    with col2:
        st.markdown(
            f"""
            <div style='text-align: right; padding: 0.2rem;'>
                <h3 style='color: {COLORS["info"]}; font-size: 1rem;'>Live Dashboard</h3>
                <p style='color: {COLORS["text"]}; font-size: 0.8rem;'>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    # Main content
    tab1, tab2, tab3 = st.tabs(["📊 Real-time", "📈 Analytics", "🔍 Insights"])
    
    with tab1:
        # Get latest readings
        temp_df, humidity_df, motion_df = get_latest_readings()
        
        # Status Overview
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        status_cols = st.columns(4)
        
        with status_cols[0]:
            total_sensors = len(temp_df) + len(humidity_df) + len(motion_df)
            st.metric("Total Sensors", total_sensors)
        
        with status_cols[1]:
            active_motion = motion_df['value'].sum()
            st.metric("Active Motion", f"{active_motion}/{len(motion_df)}")
        
        with status_cols[2]:
            avg_temp = temp_df['value'].mean()
            st.metric("Avg Temp", f"{avg_temp:.1f}°C")
        
        with status_cols[3]:
            avg_humidity = humidity_df['value'].mean()
            st.metric("Avg Humidity", f"{avg_humidity:.1f}%")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Sensor Readings
        sensor_cols = st.columns([1, 1, 1])
        
        with sensor_cols[0]:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown("#### 🌡️ Temperature")
            for _, row in temp_df.iterrows():
                fig = create_gauge(
                    value=row['value'],
                    title=f"{row['location']}",
                    min_val=15,
                    max_val=30,
                    threshold=28,
                    unit="°C"
                )
                st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with sensor_cols[1]:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown("#### 💧 Humidity")
            for _, row in humidity_df.iterrows():
                fig = create_gauge(
                    value=row['value'],
                    title=f"{row['location']}",
                    min_val=30,
                    max_val=70,
                    threshold=65,
                    unit="%"
                )
                st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with sensor_cols[2]:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown("#### 🚶 Motion")
            for _, row in motion_df.iterrows():
                status = "Active" if row['value'] else "Inactive"
                color = COLORS['success'] if row['value'] else COLORS['accent']
                st.markdown(
                    f"""
                    <div style='background-color: rgba(31, 41, 55, 0.3); padding: 0.5rem; border-radius: 8px; margin-bottom: 0.5rem;'>
                        <h4 style='font-size: 0.9rem;'>{row['location']}</h4>
                        <p style='color: {color}; font-size: 1.2rem; margin: 0.2rem 0;'>● {status}</p>
                        <p style='color: {COLORS['text']}; font-size: 0.7rem; margin: 0;'>
                            Last: {row['timestamp'].strftime('%H:%M:%S')}
                        </p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        # Get historical data
        temp_trends, humidity_trends, motion_trends = get_historical_data(24)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown("#### 🌡️ Temperature Trends")
            fig = create_trend_chart(
                temp_trends,
                x='hour',
                y='avg_value',
                color='location',
                title="Last 24 hours",
                y_label="Temperature (°C)"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown("#### 💧 Humidity Trends")
            fig = create_trend_chart(
                humidity_trends,
                x='hour',
                y='avg_value',
                color='location',
                title="Last 24 hours",
                y_label="Humidity (%)"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown("#### 🔍 Activity Analysis")
        
        fig = create_heatmap(
            df=motion_trends,
            title="Motion Activity Heatmap"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Activity patterns in a more compact format
        activity_patterns = motion_trends.groupby('location')['activity_rate'].agg(['mean', 'max', 'min']).round(2)
        st.markdown("##### Activity Summary")
        
        # Create a more compact table view
        pattern_df = pd.DataFrame(activity_patterns)
        pattern_df.columns = ['Avg %', 'Max %', 'Min %']
        st.dataframe(
            pattern_df,
            hide_index=False,
            use_container_width=True
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Minimal footer
    st.markdown(
        """
        <div style='text-align: center; color: #666; font-size: 0.7rem; padding: 0.5rem;'>
            IoT Smart Home Dashboard
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Auto-refresh
    time.sleep(5)  # Fixed 5-second refresh
    st.rerun()

if __name__ == "__main__":
    main() 