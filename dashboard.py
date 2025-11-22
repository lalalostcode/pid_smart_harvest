import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import pickle
import os

# Page Config
st.set_page_config(
    page_title="Smart Harvest Dashboard",
    page_icon="🌾",
    layout="wide"
)

# Title
st.title("🌾 Smart Harvest Prediction System")
st.markdown("### Data Engineering & Machine Learning Pipeline Monitoring")

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="harvest_dw"
    )

# --- LOAD DATA ---
@st.cache_data
def load_data():
    conn = get_db_connection()
    
    # Production Data
    query_prod = """
    SELECT year, month, province_name, commodity_name, production_ton 
    FROM vw_production_detail
    """
    df_prod = pd.read_sql(query_prod, conn)
    
    # Prediction Data
    query_pred = """
    SELECT year, month, province_name, commodity_name, predicted_ton, model_name 
    FROM vw_prediction_detail
    """
    df_pred = pd.read_sql(query_pred, conn)
    
    # Weather Data
    query_weather = """
    SELECT year, month, province_name, season, total_rainfall_mm, avg_temperature_c 
    FROM vw_weather_detail
    """
    df_weather = pd.read_sql(query_weather, conn)
    
    conn.close()
    return df_prod, df_pred, df_weather

# Load Data
try:
    df_prod, df_pred, df_weather = load_data()
    st.success("✅ Connected to Data Warehouse (harvest_dw)")
except Exception as e:
    st.error(f"❌ Database Connection Failed: {e}")
    st.stop()

# --- TOP FILTERS ---
st.markdown("### 🔍 Filters")
f1, f2, f3 = st.columns(3)

with f1:
    selected_year = st.slider("Select Year", 
                            int(df_prod['year'].min()), 
                            int(df_prod['year'].max()), 
                            (2018, 2020))

with f2:
    commodity_options = sorted(df_prod['commodity_name'].unique().tolist())
    all_commodities = st.checkbox("Select All Commodities")
    if all_commodities:
        selected_commodities = commodity_options
        st.multiselect("Select Commodity", commodity_options, default=commodity_options, disabled=True)
    else:
        selected_commodities = st.multiselect("Select Commodity", commodity_options, default=commodity_options[:1])

with f3:
    province_options = sorted(df_prod['province_name'].unique().tolist())
    all_provinces = st.checkbox("Select All Provinces")
    if all_provinces:
        selected_provinces = province_options
        st.multiselect("Select Province", province_options, default=province_options, disabled=True)
    else:
        selected_provinces = st.multiselect("Select Province", province_options, default=province_options[:1])

# Filter Data
mask = (df_prod['year'].between(selected_year[0], selected_year[1]))

if selected_commodities:
    mask = mask & (df_prod['commodity_name'].isin(selected_commodities))

if selected_provinces:
    mask = mask & (df_prod['province_name'].isin(selected_provinces))

filtered_prod = df_prod[mask]

# --- MAIN LAYOUT ---
# Left: Main Chart (2/3) | Right: KPIs & Secondary Chart (1/3)
left_col, right_col = st.columns([2, 1])

# --- LEFT COLUMN: MAIN CHART ---
with left_col:
    # Determine title based on selection
    if len(selected_commodities) == 1:
        chart_title = f"📈 Production Trend: {selected_commodities[0].upper()}"
    else:
        chart_title = "📈 Production Trend: Multiple Commodities"
        
    st.subheader(chart_title)
    
    # Always show multiple lines if multiple commodities selected
    if len(selected_commodities) > 1:
        prod_trend = filtered_prod.groupby(['year', 'month', 'commodity_name'])['production_ton'].sum().reset_index()
        prod_trend['date'] = pd.to_datetime(prod_trend[['year', 'month']].assign(day=1))
        fig_trend = px.line(prod_trend, x='date', y='production_ton', color='commodity_name', markers=True, 
                            title=f"Monthly Production Trend ({selected_year[0]}-{selected_year[1]})")
    else:
        prod_trend = filtered_prod.groupby(['year', 'month'])['production_ton'].sum().reset_index()
        prod_trend['date'] = pd.to_datetime(prod_trend[['year', 'month']].assign(day=1))
        fig_trend = px.line(prod_trend, x='date', y='production_ton', markers=True, 
                            title=f"Monthly Production Trend ({selected_year[0]}-{selected_year[1]})")
    
    fig_trend.update_layout(height=550) # Taller chart
    st.plotly_chart(fig_trend, use_container_width=True)

# --- RIGHT COLUMN: KPIs & SECONDARY CHART ---
with right_col:
    st.subheader("📊 Key Metrics")
    
    # Calculate Metrics
    total_production = filtered_prod['production_ton'].sum()
    avg_temp = df_weather[df_weather['year'].between(selected_year[0], selected_year[1])]['avg_temperature_c'].mean()
    avg_rain = df_weather[df_weather['year'].between(selected_year[0], selected_year[1])]['total_rainfall_mm'].mean()
    
    # Display Metrics (2x2 Grid)
    m1, m2 = st.columns(2)
    m1.metric("Total Production", f"{total_production:,.0f} T")
    m2.metric("Avg Temp", f"{avg_temp:.1f} °C")
    
    m3, m4 = st.columns(2)
    m3.metric("Avg Rainfall", f"{avg_rain:.1f} mm")
    
    # Show count of selected items
    sel_text = f"{len(selected_commodities)} Com / {len(selected_provinces)} Prov"
    m4.metric("Selected", sel_text)
    
    st.markdown("---")
    
    # Secondary Chart: Top Provinces or Seasonality
    # If many provinces selected, show Top 10
    if len(selected_provinces) > 5:
        st.subheader("🏆 Top Provinces")
        top_prov = filtered_prod.groupby('province_name')['production_ton'].sum().nlargest(10).reset_index()
        fig_bar = px.bar(top_prov, x='production_ton', y='province_name', orientation='h', 
                         title="Top 10 Producers", color='production_ton')
        fig_bar.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.subheader("🥧 Production Share")
        # Group by commodity to see share
        comm_share = filtered_prod.groupby('commodity_name')['production_ton'].sum().reset_index()
        fig_pie = px.pie(comm_share, values='production_ton', names='commodity_name', 
                         title="Share by Commodity", hole=0.4)
        fig_pie.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_pie, use_container_width=True)



# --- PREDICTION SIMULATOR ---
st.markdown("---")
st.subheader("🤖 AI Prediction Simulator")

if len(selected_commodities) != 1:
    st.warning("⚠️ Please select EXACTLY ONE commodity to use the Prediction Simulator.")
else:
    target_commodity = selected_commodities[0]
    st.info(f"Adjust weather parameters to predict **{target_commodity.upper()}** harvest yield using the trained ML model.")

    # Load Model
    model_path = f"models/linear_regression_{target_commodity}_v1.pkl"
    if os.path.exists(model_path):
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        c1, c2, c3 = st.columns(3)
        input_temp = c1.slider("Temperature (°C)", 20.0, 35.0, 27.0)
        input_rain = c2.slider("Rainfall (mm)", 0.0, 500.0, 150.0)
        input_humid = c3.slider("Humidity (%)", 50.0, 100.0, 80.0)
        
        # Predict
        prediction = model.predict([[input_temp, input_rain, input_humid]])[0]
        
        st.metric(label=f"Predicted {target_commodity.title()} Production", 
                  value=f"{prediction:,.2f} Tons", 
                  delta="Based on ML Model")
    else:
        st.warning(f"Model for {target_commodity} not found. Please train the model first.")

# --- FOOTER ---
st.markdown("---")
st.caption("🚀 Smart Harvest System | Built with Streamlit, MySQL, & Scikit-Learn")
