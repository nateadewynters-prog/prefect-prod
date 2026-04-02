# --- SQL QUERIES ---
# Note: Using ? allows pyodbc to securely inject variables later
SQL_PERF_COUNT = "SELECT COUNT(PerformanceDetailId) FROM PerformanceDetail WHERE ShowId = ? AND CAST(PerformanceDateTime AS Date) = ?"

SQL_MAIN_STATS = """
SELECT FORMAT(Wrap,'N0'), FORMAT(Tickets,'N0'), FORMAT(SalesATP,'N2'), 
       FORMAT(Advance,'N0'), FORMAT(AdvanceTicketsSales,'N0'), FORMAT(Advance/AdvanceTicketsSales,'N2'), 
       FORMAT(Reserved,'N0'), FORMAT(CumulativeGross,'N0'), FORMAT(CumulativeTicketSales,'N0'), 
       FORMAT(CumulativeGross/CumulativeTicketSales,'N2') 
FROM CombinedWithEventsView WHERE ShowId = ? AND Wrap IS NOT NULL AND RecordDate = ?
"""

SQL_WEEKLY_STATS = """
WITH ThisWeek AS (SELECT DATEADD(dd, -(DATEPART(dw, MAX(RecordDate))-1), MAX(RecordDate)+1) AS WCDate, DATEADD(dd, 8-(DATEPART(dw, MAX(RecordDate))), MAX(RecordDate)) AS WEDate FROM ChannelSalesView WHERE ShowId = ?) 
SELECT FORMAT(AVG(PercentageGross)*100,'N0'), FORMAT(AVG(PercentageTicketsSold)*100,'N0') 
FROM SalesByPerformanceView04 CROSS JOIN ThisWeek 
WHERE ShowId = ? AND PerformanceDateTime BETWEEN WCDate AND WEDate AND DateOfUpdate = ?
"""

SQL_MAT_EVE_STATS = """
SELECT FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentGrossSold * 100 END),'N0'), 
       FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentTicketsSold * 100 END),'N0'), 
       FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN Gross/1000 END),'N0'), 
       FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentGrossSold * 100 END),'N0'), 
       FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentTicketsSold * 100 END),'N0'), 
       FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN Gross/1000 END),'N0') 
FROM CombinedWithEventsView WHERE ShowId = ? AND RecordDate = ?
"""

# --- HTML TEMPLATE ---
def get_html_body(config, w_g, w_t, w_atp, a_g, a_t, a_atp, res_g, c_g, c_t, c_atp, wk_gp, wk_cap, perf_section):
    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; color: #000000; line-height: 1.4; font-size: 11pt;">
        <div style="max-width: 850px;">
            <p>Dear all,</p>
            <p>Please find attached your report for <strong>{config['show_name']}</strong><br>
            To view this on the Power BI Dashboard click <a href="{config['dashboard_url']}" style="color: #0078D4; text-decoration: none;">here</a>.</p>
            
            <p style="margin:0;">In summary:</p>
            <p style="margin:0;">&emsp;&bull; Yesterday’s wrap was <strong>£{w_g}</strong> and <strong>{w_t}</strong> tickets with an ATP of <strong>£{w_atp}</strong>.</p>
            <p style="margin:0;">&emsp;&bull; Cumulative sales are currently at <strong>£{c_g}</strong> and <strong>{c_t}</strong> tickets with an ATP of <strong>£{c_atp}</strong>.</p>
            <p style="margin:0;">&emsp;&bull; The advance is currently at <strong>£{a_g}</strong> and <strong>{a_t}</strong> tickets with an ATP of <strong>£{a_atp}</strong> (incl. comps).</p>
            <p style="margin:0;">&emsp;&bull; The reserve gross is currently <strong>£{res_g}</strong>.</p>
            {perf_section}
            <p style="margin:0;">&emsp;&bull; This week’s performances average <strong>{wk_gp}% GP</strong> and <strong>{wk_cap}% capacity</strong>.</p>
            
            <br>
            <img src="cid:preview_image_001" style="width: 100%; max-width: 800px; border: 1px solid #EEEEEE; display: block;">
            <br>
            <p style="margin:0;">All the best,<br><strong>The Dewynters Team</strong></p>
        </div>
    </body>
    </html>
    """