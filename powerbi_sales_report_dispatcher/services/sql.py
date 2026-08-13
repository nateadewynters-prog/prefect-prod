"""
services/sql.py — sales metrics out of SQL Server.

Two shapes of source database sit behind the same interface:

  Legacy       (TicketingDS)     the original ticketing warehouse
  TransactLive (TransactDSLive)  the newer platform, remapped to the Legacy
                                 tuple layout so the email builder and the
                                 /query panel only ever see one shape

get_show_metrics(config) is the router — everything upstream calls that and
never needs to know which database a show lives in.
"""

import os

import pyodbc


def fetch_legacy_metrics(show_id):
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE=TicketingDS;UID={os.getenv('SQL_USERNAME_BILOGIN')};PWD={os.getenv('SQL_PASSWORD_BILOGIN')};TrustServerCertificate=yes;"
    yesterday_query = "CAST(DATEADD(day, -1, GETDATE()) AS Date)"
    metrics = {}
    
    with pyodbc.connect(conn_str, timeout=10) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(PerformanceDetailId) FROM PerformanceDetail WHERE ShowId = {show_id} AND CAST(PerformanceDateTime AS Date) = {yesterday_query}")
        metrics['no_of_perfs'] = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT FORMAT(Wrap,'N0'), FORMAT(Tickets,'N0'), FORMAT(SalesATP,'N2'), FORMAT(Advance,'N0'), FORMAT(AdvanceTicketsSales,'N0'), FORMAT(Advance/AdvanceTicketsSales,'N2'), FORMAT(Reserved,'N0'), FORMAT(CumulativeGross,'N0'), FORMAT(CumulativeTicketSales,'N0'), FORMAT(CumulativeGross/CumulativeTicketSales,'N2') FROM CombinedWithEventsView WHERE ShowId = {show_id} AND Wrap IS NOT NULL AND RecordDate = {yesterday_query}")
        metrics['main'] = cursor.fetchone()
        
        cursor.execute(f"WITH ThisWeek AS (SELECT MAX(RecordDate) AS MaxUpdateDate, DATEADD(wk, DATEDIFF(wk, 0, MAX(RecordDate)), 0) AS WCDate, DATEADD(wk, DATEDIFF(wk, 0, MAX(RecordDate)), 6) AS WEDate FROM ChannelSalesView WHERE ShowId = {show_id}) SELECT FORMAT(AVG(PercentageGross)*100,'N0') AS PercentGP, FORMAT(AVG(PercentageTicketsSold)*100,'N0') AS PercentCap FROM SalesByPerformanceView04 CROSS JOIN ThisWeek WHERE ShowId = {show_id} AND PerformanceDateTime BETWEEN WCDate AND WEDate AND DateOfUpdate = MaxUpdateDate")
        metrics['weekly'] = cursor.fetchone()
        
        if metrics['no_of_perfs'] > 0:
            cursor.execute(f"SELECT FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentGrossSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentTicketsSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN Gross/1000 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentGrossSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentTicketsSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN Gross/1000 END),'N0') FROM CombinedWithEventsView WHERE ShowId = {show_id} AND RecordDate = {yesterday_query}")
            metrics['perf_detail'] = cursor.fetchone()
            
    return metrics

def fetch_transact_metrics(show_id):
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE=TransactDSLive;UID={os.getenv('SQL_USERNAME_BILOGIN')};PWD={os.getenv('SQL_PASSWORD_BILOGIN')};TrustServerCertificate=yes;"
    metrics = {}
    
    with pyodbc.connect(conn_str, timeout=10) as conn:
        cursor = conn.cursor()
        
        # 1. Perf Checker
        cursor.execute(f"SELECT DISTINCT(COUNT(PerformanceDetailId)) FROM PerformanceDetail WHERE ShowId = {show_id} AND CAST(PerformanceDateTime AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date)")
        metrics['no_of_perfs'] = cursor.fetchone()[0]
        
        # 2. Wrap
        cursor.execute(f"SELECT FORMAT(SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 THEN Gross ELSE 0 END),'N0'), FORMAT(SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 THEN TicketCount ELSE 0 END),'N0'), CAST(SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 THEN Gross ELSE 0 END)/ SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 AND IsComp = 0 THEN TicketCount ELSE 0 END) AS decimal(5,2)) FROM TicketSale TS WHERE TS.PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id}) AND CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) GROUP BY CAST(TS.PurchaseDate AS Date)")
        wrap = cursor.fetchone() or (None, None, None)
        
        # 3. Advance
        cursor.execute(f"SELECT FORMAT(SUM(CASE WHEN IsReservation = 0 THEN Gross ELSE 0 END),'N0'), ROUND(SUM(CASE WHEN IsReservation = 0 THEN Gross ELSE 0 END) / 1000000.0,2), FORMAT(SUM(CASE WHEN IsReservation = 0 THEN TicketCount ELSE 0 END),'N0'), ROUND(SUM(CASE WHEN IsReservation = 0 THEN TicketCount ELSE 0 END)/1000.0,1), CAST(SUM(CASE WHEN IsReservation = 0 THEN Gross ELSE 0 END)/ SUM(CASE WHEN IsReservation = 0 AND IsComp = 0 THEN TicketCount ELSE 0 END) as decimal (5,2)), FORMAT(SUM(CASE WHEN IsReservation = 1 THEN Gross ELSE 0 END),'N0'), FORMAT(SUM(CASE WHEN IsReservation = 1 THEN TicketCount ELSE 0 END),'N0') FROM TicketSale WHERE PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id} AND PerformanceDatetime > GETDATE())")
        adv = cursor.fetchone() or (None, None, None, None, None, None, None)
        
        # 4. Cumulative
        cursor.execute(f"SELECT FORMAT((SUM(Gross)+ 14359011),'N0'), ROUND((SUM(Gross)+ 14359011)/1000000.0,1), FORMAT((SUM(TicketCount) + 204451),'N0'), ROUND((SUM(TicketCount) + 204451)/1000.0,1), CAST( (SUM(Gross)+ 14359011) /(SUM(CASE WHEN IsComp = 0 THEN TicketCount ELSE 0 END) + 204451) as decimal (5,2)) FROM TicketSale WHERE PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id}) AND IsReservation = 0")
        cumul = cursor.fetchone() or (None, None, None, None, None)
        
        # Map to Legacy standard, appending Reserved Tickets (adv[6]) to the end
        metrics['main'] = (
            wrap[0], wrap[1], wrap[2],   
            adv[0], adv[2], adv[4], adv[5], 
            cumul[0], cumul[2], cumul[4], adv[6]  
        )
        
        # 5. Weekly
        cursor.execute(f"WITH A AS (SELECT PerformanceDateTime, SUM(Gross) AS Gross, GrossPotential, SUM(TicketCount) AS TicketCount, SeatingCapacity FROM TicketSale TS JOIN PerformanceDetail PD ON TS.PerformanceDetailId = PD.PerformanceDetailId WHERE TS.PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id}) AND IsReservation = 0 AND CAST(PerformanceDateTime AS Date) BETWEEN CAST(GETDATE() - DATEPART(dw, GETDATE()-2) AS Date) AND CAST(GETDATE() - DATEPART(dw, GETDATE()-2)+7 AS Date) GROUP BY PerformanceDateTime, GrossPotential, SeatingCapacity) SELECT CAST((SUM(A.Gross) / SUM(A.GrossPotential))*100 AS decimal(4,0)), CAST((SUM(A.TicketCount) *1.0 / SUM(A.SeatingCapacity))*100 AS decimal(4,0)) FROM A")
        metrics['weekly'] = cursor.fetchone() or (None, None)
        
        # 6. Perf Detail
        if metrics['no_of_perfs'] > 0:
            cursor.execute(f"SELECT ROUND(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN Gross END)/1000.0,1), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN Gross END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN GrossPotential END) AS float) *100,'N0'), SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN TicketCount END), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN TicketCount END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN SeatingCapacity END) AS float) *100,'N0'), ROUND(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN Gross ELSE 0 END)/1000.0,1), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN Gross END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN GrossPotential END) AS float) *100,'N0'), SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN TicketCount ELSE 0 END), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN TicketCount END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN SeatingCapacity END) AS float) *100,'N0'), ROUND(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN Gross ELSE 0 END)/1000.0,1), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN Gross END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN GrossPotential END) AS float) *100,'N0'), SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN TicketCount ELSE 0 END), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN TicketCount END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN SeatingCapacity END) AS float) *100,'N0') FROM TicketSale TS JOIN PerformanceDetail PD ON PD.PerformanceDetailId = TS.PerformanceDetailId JOIN TimeOfPerformanceNames PN ON PN.TimeOfPerformanceNameId = PD.TimeOfPerformanceNameId WHERE ShowId = {show_id} AND CAST(PD.PerformanceDateTime AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0")
            perf = cursor.fetchone()
            if perf:
                metrics['perf_detail'] = (
                    perf[1], perf[3], perf[0],  # Matinee (GP%, Cap%, Gross)
                    perf[5], perf[7], perf[4],  # Evening (GP%, Cap%, Gross)
                    perf[9], perf[11], perf[8]  # Night   (GP%, Cap%, Gross)
                )
                
    return metrics

def get_show_metrics(config):
    """Router function to decouple database logic from the rest of the app."""
    if config.get("db_type") == "TransactLive":
        return fetch_transact_metrics(config["show_id"])
    else:
        return fetch_legacy_metrics(config["show_id"])
