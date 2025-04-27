import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import time
import os
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client

# Fetch Environment variables
URL=os.getenv("supabase_url")
KEY=os.getenv("supabase_key")

# Set the style for matplotlib
plt.style.use('seaborn-v0_8-whitegrid')

# Set a consistent figure size for all charts
CHART_SIZE = (12, 8)

def get_connection_hosted_db():
    try:
        # Initialize connection.
        supabase_client = create_client(URL, KEY)
        print("PostgreSQL connection to the hosted db established!")
        return supabase_client
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# # Database connection function
# def get_db_connection():
#     try:
#         connection = psycopg2.connect(
#             database=DATABASE,
#             user=USER,
#             password=PASSWORD,
#             host=HOST,
#             port=PORT
#         )
#         print("PostgreSQL connection established!")
#         return connection
#     except Exception as e:
#         st.error(f"Error connecting to database: {e}")
#         return None

# Function to fetch data from database
def fetch_data(time_duration):
    # connection = get_db_connection()
    supabase_client = get_connection_hosted_db()
    if supabase_client:
        try:
            # Calculate the time filter based on selected duration
            # now = datetime.strptime("2010-12-01 12:00 PM", "%Y-%m-%d %I:%M %p")
            now = datetime.now()
            now = now.replace(year=2010, month=12, day=1)
            st.write(f"Current Time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

            if time_duration == "30 minutes":
                time_filter = now - timedelta(minutes=30)
            elif time_duration == "1 hour":
                time_filter = now - timedelta(hours=1)
            elif time_duration == "3 hours":
                time_filter = now - timedelta(hours=3)
            elif time_duration == "6 hours":
                time_filter = now - timedelta(hours=6)
            elif time_duration == "24 hours":
                time_filter = now - timedelta(hours=24)
            elif time_duration == "All time":
                time_filter = None
            else:
                time_filter = now - timedelta(hours=24)  # Default to 24 hours

            print(time_filter)
            # if time_filter == None:
            #     query = f"""
            #         SELECT 
            #             description, quantity, invoicedate, country, totalamount
            #         FROM online_retail 
            #         """
            # else:
            #     query = f"""
            #         SELECT 
            #             description, quantity, invoicedate, country, totalamount
            #         FROM online_retail 
            #         WHERE 
            #             invoicedate >= '{time_filter}' and invoicedate <= '{now}'
            #         """
            # df = pd.read_sql(query, connection)

            # Build the query
            if time_filter is None:
                response = (
                    supabase_client
                    .table("online_retail")
                    .select("description, quantity, invoicedate, country, totalamount")
                    .execute()
                )
            else:
                response = (
                    supabase_client
                    .table("online_retail")
                    .select("description, quantity, invoicedate, country, totalamount")
                    .gte('invoicedate', time_filter.isoformat())
                    .lte('invoicedate', now.isoformat())
                    .execute()
                )
            
            if response.data:
                df = pd.DataFrame(response.data)
                st.write(f"Displaying data for the last: {time_duration}")
                return df
            else:
                return pd.DataFrame()
            
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return pd.DataFrame()
        
        # connection.close()
    else:
        return pd.DataFrame()

# Function to create metrics
def create_metrics(df):
    if not df.empty:
        total_sales = df['totalamount'].sum()
        total_transactions = len(df)
        avg_transaction_value = total_sales / total_transactions if total_transactions > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Sales", f"${total_sales:,.2f}")
        with col2:
            st.metric("Total Transactions", f"{total_transactions:,}")
        with col3:
            st.metric("Avg Transaction Value", f"${avg_transaction_value:,.2f}")

# Function to create top products chart
def create_top_products_chart(df):
    if not df.empty:
        top_products = df.groupby('description')['quantity'].sum().nlargest(10)
        
        fig, ax = plt.subplots(figsize=(12, 10))
        sns.barplot(x=top_products.values, y=top_products.index, ax=ax)
        ax.set_title('Top 10 Selling Products', fontsize=24)
        ax.set_xlabel('Quantity Sold')
        ax.set_ylabel('Product')
        plt.tight_layout()
        
        chart_placeholder = st.empty()
        with chart_placeholder.container():
            st.pyplot(fig)

        plt.close(fig)

# Function to create revenue trend chart
def create_revenue_trend_chart(df):
    if not df.empty:
        df['hour'] = df['invoicedate'].dt.hour
        hourly_revenue = df.groupby('hour')['totalamount'].sum()
        
        fig, ax = plt.subplots(figsize=(12, 10))
        sns.lineplot(x=hourly_revenue.index, y=hourly_revenue.values, marker='o', ax=ax)
        ax.set_title('Hourly Revenue Trend', fontsize=26)
        ax.set_xlabel('Hour of Day')
        ax.set_ylabel('Revenue')
        ax.grid(True)
        plt.tight_layout()
        
        chart_placeholder = st.empty()
        with chart_placeholder.container():
            st.pyplot(fig)

        plt.close(fig)

# Function to create country-wise sales chart
def create_country_sales_chart(df):
    if not df.empty:
        country_sales = df.groupby('country')['totalamount'].sum().nlargest(10)
        
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.pie(country_sales.values, labels=country_sales.index, autopct='%1.1f%%')
        ax.set_title('Top 10 Countries by Sales', fontsize=16)
        plt.tight_layout()
        
        chart_placeholder = st.empty()
        with chart_placeholder.container():
            st.pyplot(fig)

        plt.close(fig)

# Function to create category sales chart
def create_category_sales_chart(df):
    if not df.empty and 'description' in df.columns:
        df['category'] = df['description'].str.split().str[0]
        category_sales = df.groupby('category')['totalamount'].sum().nlargest(10)
        
        fig, ax = plt.subplots(figsize=(12, 9))
        sns.barplot(x=category_sales.values, y=category_sales.index, ax=ax)
        ax.set_title('Top 10 Product Categories by Sales', fontsize=24)
        ax.set_xlabel('Total Sales')
        ax.set_ylabel('Category')
        plt.tight_layout()
        
        chart_placeholder = st.empty()
        with chart_placeholder.container():
            st.pyplot(fig)
        plt.close(fig)

if __name__ == "__main__":
    st.set_page_config(
        page_title="Real-time Retail Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("Real-time Online Retail Dashboard")
    
    # Initialize session state for last update time and time duration
    if 'last_update' not in st.session_state:
        st.session_state.last_update = datetime.now()
    if 'time_duration' not in st.session_state:
        st.session_state.time_duration = "24 hours"
    
    # Add time duration selection buttons
    time_options = ["30 minutes", "1 hour", "3 hours", "6 hours", "24 hours", "All time"]
    selected_duration = st.radio(
        "Select Time Duration:",
        time_options,
        index=time_options.index(st.session_state.time_duration),
        horizontal=True
    )
    st.session_state.time_duration = selected_duration
    
    # Add refresh button and timestamp at the top
    if st.button("Refresh Now", key="refresh_button"):
        st.session_state.last_update = datetime.now()
        st.rerun()
    
    st.write(f"Last updated: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create a placeholder for the dashboard
    dashboard_placeholder = st.empty()
    
    # Auto-refresh every 30 seconds
    refresh_interval = 30
    
    # Main dashboard content
    with dashboard_placeholder.container():
        # Fetch latest data with time filter
        df = fetch_data(selected_duration)
        # Convert InvoiceDate to datetime format
        df["invoicedate"] = pd.to_datetime(df["invoicedate"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
        
        if not df.empty:
            # Create metrics
            create_metrics(df)
            
            # Create charts - Row 1
            col1, col2 = st.columns(2)
            with col1:
                with st.container():
                    create_top_products_chart(df)
                    create_revenue_trend_chart(df)
            with col2:
                with st.container():
                    create_country_sales_chart(df)
                    create_category_sales_chart(df)
                
        else:
            st.warning("No data available for the selected time period.")
    
    # Auto-refresh logic
    time.sleep(refresh_interval)
    st.session_state.last_update = datetime.now()
    st.rerun()