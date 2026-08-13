"""
services/email.py — the email body and the MS Graph send.

build_email_html() is a pure function that sends nothing, which is what lets
/preview/<id> render exactly the email that would go out without dispatching
it. Only send_graph_email() actually posts to Graph.

Mail is sent as the shared BI mailbox (SENDER_EMAIL) using application
permissions, hence /users/{sender}/sendMail rather than /me/sendMail.
"""

import base64
from datetime import datetime, timedelta

import requests

from config import SENDER_EMAIL


def build_email_html(config, m):
    main_data = m.get('main', [])
    w_g = main_data[0] if len(main_data) > 0 else None
    w_t = main_data[1] if len(main_data) > 1 else None
    w_atp = main_data[2] if len(main_data) > 2 else None
    a_g = main_data[3] if len(main_data) > 3 else None
    a_t = main_data[4] if len(main_data) > 4 else None
    a_atp = main_data[5] if len(main_data) > 5 else None
    res_g = main_data[6] if len(main_data) > 6 else None
    c_g = main_data[7] if len(main_data) > 7 else None
    c_t = main_data[8] if len(main_data) > 8 else None
    c_atp = main_data[9] if len(main_data) > 9 else None
    res_t = main_data[10] if len(main_data) > 10 else None

    wk_gp, wk_cap = m.get('weekly', (None, None))
    
    def is_val(val):
        return val is not None and str(val).lower() != 'none'

    summary_items = []
    if is_val(w_g) and is_val(w_t):
        summary_items.append(f"&emsp;&bull; Yesterday’s wrap was <strong>£{w_g}</strong> and <strong>{w_t}</strong> tickets with an ATP of <strong>£{w_atp}</strong>.")
    if is_val(a_g) and is_val(a_t):
        summary_items.append(f"&emsp;&bull; The advance is currently at <strong>£{a_g}</strong> and <strong>{a_t}</strong> tickets with an ATP of <strong>£{a_atp}</strong>.")
    if is_val(c_g) and is_val(c_t):
        summary_items.append(f"&emsp;&bull; Cumulative sales are currently at <strong>£{c_g}</strong> and <strong>{c_t}</strong> tickets with an ATP of <strong>£{c_atp}</strong>.")
    
    if is_val(res_g):
        if is_val(res_t):
            summary_items.append(f"&emsp;&bull; The reserve gross is currently <strong>£{res_g}</strong> and <strong>{res_t}</strong> tickets.")
        else:
            summary_items.append(f"&emsp;&bull; The reserve gross is currently <strong>£{res_g}</strong>.")

    # Dynamic Performances
    if m.get('no_of_perfs', 0) > 0 and 'perf_detail' in m:
        p = m['perf_detail']
        perf_sub_items = []
        
        # Parse Matinee and Evening
        if len(p) >= 6:
            m_gp, m_cap, m_gr, e_gp, e_cap, e_gr = p[0:6]
            if is_val(m_gp):
                perf_sub_items.append(f"&emsp;&emsp;Matinee - {m_gp}% GP (£{m_gr}k) and {m_cap}% capacity.")
            if is_val(e_gp):
                perf_sub_items.append(f"&emsp;&emsp;Evening - {e_gp}% GP (£{e_gr}k) and {e_cap}% capacity.")
        
        # Parse Night (if present)
        if len(p) >= 9:
            n_gp, n_cap, n_gr = p[6:9]
            if is_val(n_gp):
                perf_sub_items.append(f"&emsp;&emsp;Night - {n_gp}% GP (£{n_gr}k) and {n_cap}% capacity.")
                
        if perf_sub_items:
            summary_items.append("&emsp;&bull; Yesterday’s performances:")
            summary_items.extend(perf_sub_items)

    if is_val(wk_gp) and is_val(wk_cap):
        summary_items.append(f"&emsp;&bull; This week’s performances average <strong>{wk_gp}% GP</strong> and <strong>{wk_cap}% capacity</strong>.")

    summary_html_block = "".join([f'<p style="margin:0;">{item}</p>' for item in summary_items])

    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; color: #000000; line-height: 1.4; font-size: 11pt;">
        <div style="max-width: 850px;">
            <p>Dear all,</p>
            <p>Please find attached your report for <strong>{config['show_name']}</strong><br>
            To view this on the Power BI Dashboard click <a href="{config['dashboard_url']}" style="color: #0078D4; text-decoration: none;">here</a>.</p>
            <p style="margin-bottom: 4px;"><strong>In summary:</strong></p>
            {summary_html_block}
            <br>
            <img src="cid:preview_image_001" style="width: 100%; max-width: 800px; border: 1px solid #EEEEEE; display: block;">
            <br>
            <p style="margin:0;">All the best,<br><strong>The Dewynters Team</strong></p>
        </div>
    </body>
    </html>
    """

def send_graph_email(config, html_body, pdf_content, png_bytes, graph_token):
    display_date = (datetime.now() - timedelta(1)).strftime('%a %d/%m/%Y')
    file_date = (datetime.now() - timedelta(1)).strftime('%d_%m_%Y')
    
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
                    "contentBytes": base64.b64encode(pdf_content).decode(),
                    "isInline": False
                },
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "preview.png",
                    "contentType": "image/png",
                    "contentBytes": base64.b64encode(png_bytes).decode(),
                    "contentId": "preview_image_001",
                    "isInline": True
                }
            ]
        }
    }
    
    send_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    headers = {"Authorization": f"Bearer {graph_token}", "Content-Type": "application/json"}
    requests.post(send_url, headers=headers, json=payload).raise_for_status()
