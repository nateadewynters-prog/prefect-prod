"""
Utility functions for building and sending report emails via Microsoft Graph API.
"""

import requests
import base64
from datetime import datetime, timedelta
from config import SENDER_EMAIL
from utils.auth import get_token

def build_email_body(config, m):
    """
    Constructs an HTML email body using configuration and SQL metrics.
    """
    w_g, w_t, w_atp, a_g, a_t, a_atp, res_g, c_g, c_t, c_atp = m['main']
    wk_gp, wk_cap = m['weekly']
    
    perf_section = ""
    if m.get('no_of_perfs', 0) > 0 and 'perf_detail' in m:
        m_gp, m_cap, m_gr, e_gp, e_cap, e_gr = m['perf_detail']
        perf_section = '<p style="margin:0;">&emsp;&bull; Yesterday’s performances:</p>'
        if m_gp is not None:
            perf_section += f'\n                <p style="margin:0;">&emsp;&emsp;Matinee - {m_gp}% GP (£{m_gr}k) and {m_cap}% capacity.</p>'
        if e_gp is not None:
            perf_section += f'\n                <p style="margin:0;">&emsp;&emsp;Evening - {e_gp}% GP (£{e_gr}k) and {e_cap}% capacity.</p>'

    body = f"""
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
    return body

def send_graph_email(config, html_body, pdf_content, png_bytes):
    """
    Sends an email with attachments via the Microsoft Graph API.
    """
    token = get_token(["https://graph.microsoft.com/.default"])
    if not token:
        raise Exception("Failed to retrieve Microsoft Graph token.")
        
    display_date = (datetime.now() - timedelta(1)).strftime('%a %d/%m/%Y')
    file_date = (datetime.now() - timedelta(1)).strftime('%d_%m_%Y')
    
    pdf_b64 = base64.b64encode(pdf_content).decode()
    png_b64 = base64.b64encode(png_bytes).decode()
    
    payload = {
        "message": {
            "subject": f"{config['show_name']} Sales Report - {display_date}",
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": email}} for email in config['recipients']],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": f"{config['show_name']} Sales Report_{file_date}.pdf",
                    "contentType": "application/pdf",
                    "contentBytes": pdf_b64,
                    "isInline": False
                },
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "preview.png",
                    "contentType": "image/png",
                    "contentBytes": png_b64,
                    "contentId": "preview_image_001",
                    "isInline": True
                }
            ]
        }
    }
    
    send_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    response = requests.post(send_url, headers=headers, json=payload)
    response.raise_for_status()
