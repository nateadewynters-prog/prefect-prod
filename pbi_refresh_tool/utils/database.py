"""
Utility functions for interacting with the SQL Server database.
"""

import pyodbc
import pandas as pd
import streamlit as st
from config import DB_CONFIG

@st.cache_data
def fetch_doc_id_data():
    """
    Fetches show, theatre, and document ID information for the DocID Tool.
    """
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    query = "SELECT ShowName, TheatreName, DocumentName, ShowId, TheatreId, DocumentTypeId FROM [dbo].[DocumentsAndVenues] ORDER BY ShowName;"
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn)
            for col in ['ShowId', 'TheatreId', 'DocumentTypeId']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            return df.drop_duplicates(subset=['ShowId', 'TheatreId', 'DocumentTypeId'])
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()

def fetch_metrics(config):
    """
    Fetches performance and sales metrics for a specific show based on its config.
    """
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    yesterday_query = "CAST(DATEADD(day, -1, GETDATE()) AS Date)"
    metrics = {}
    
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        show_id = config['show_id']
        
        # Number of performances
        cursor.execute(f"SELECT COUNT(PerformanceDetailId) FROM PerformanceDetail WHERE ShowId = {show_id} AND CAST(PerformanceDateTime AS Date) = {yesterday_query}")
        metrics['no_of_perfs'] = cursor.fetchone()[0]
        
        # Main sales summary
        cursor.execute(f"SELECT FORMAT(Wrap,'N0'), FORMAT(Tickets,'N0'), FORMAT(SalesATP,'N2'), FORMAT(Advance,'N0'), FORMAT(AdvanceTicketsSales,'N0'), FORMAT(Advance/AdvanceTicketsSales,'N2'), FORMAT(Reserved,'N0'), FORMAT(CumulativeGross,'N0'), FORMAT(CumulativeTicketSales,'N0'), FORMAT(CumulativeGross/CumulativeTicketSales,'N2') FROM CombinedWithEventsView WHERE ShowId = {show_id} AND Wrap IS NOT NULL AND RecordDate = {yesterday_query}")
        metrics['main'] = cursor.fetchone()
        
        # Weekly performance averages
        cursor.execute(f"WITH ThisWeek AS (SELECT DATEADD(dd, -(DATEPART(dw, MAX(RecordDate))-1), MAX(RecordDate)+1) AS WCDate, DATEADD(dd, 8-(DATEPART(dw, MAX(RecordDate))), MAX(RecordDate)) AS WEDate FROM ChannelSalesView WHERE ShowId = {show_id}) SELECT FORMAT(AVG(PercentageGross)*100,'N0'), FORMAT(AVG(PercentageTicketsSold)*100,'N0') FROM SalesByPerformanceView04 CROSS JOIN ThisWeek WHERE ShowId = {show_id} AND PerformanceDateTime BETWEEN WCDate AND WEDate AND DateOfUpdate = {yesterday_query}")
        metrics['weekly'] = cursor.fetchone()
        
        # Detailed performance info if perfs occurred yesterday
        if metrics['no_of_perfs'] > 0:
            cursor.execute(f"SELECT FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentGrossSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentTicketsSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN Gross/1000 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentGrossSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentTicketsSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN Gross/1000 END),'N0') FROM CombinedWithEventsView WHERE ShowId = {show_id} AND RecordDate = {yesterday_query}")
            metrics['perf_detail'] = cursor.fetchone()
            
    return metrics
