"""
services/email.py — building and sending the report email via MS Graph.

build_email_html produces the HTML body (also reused by the /preview route,
which is why it's a pure function that sends nothing). send_graph_email
attaches the PPTX and the inline PNG and posts to the Graph sendMail endpoint.
"""

import base64
from datetime import datetime

import requests

from config import SENDER_EMAIL


def build_email_html(config: dict, metrics: dict, date_range: str) -> str:
    # NOTE: ROAS is a ratio (revenue ÷ spend). We show it with a £ sign to match
    # the house style the recipients are used to from the original scripts,
    # i.e. "£3.42" reads as "£3.42 back per £1 spent".
    return f"""
    <html>
      <body style="font-family: Calibri, sans-serif; font-size: 11pt; color: #000000;">
        <p>Dear All,</p>
        <p>Please find attached your weekly digital media report for {config['show_name']}.</p>
        <p>You can find a link to the dashboard <a href="{config['dashboard_url']}">here</a>.</p>
        <p><b>{date_range}</b></p>
        <ul style="list-style-type: disc; margin-top: 0; margin-bottom: 0;">
          <li>Total Spend: &pound;{metrics['spend']:,.2f}</li>
          <li>Total Revenue: &pound;{metrics['revenue']:,.2f}</li>
          <li>Overall ROAS: &pound;{metrics['roas']:.2f}</li>
        </ul>
        <p>All the best,<br>The Dewynters Team</p>
        <br>
        <img src="cid:report_image" style="width: 100%; max-width: 800px; border: 1px solid #ccc;">
      </body>
    </html>
    """


def send_graph_email(config: dict, html_body: str, pptx_bytes: bytes,
                     png_bytes: bytes, date_range: str, graph_token: str) -> None:
    payload = {
        "message": {
            "subject": f"{config['show_name']} - Digital Media Report - {date_range}",
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": e}} for e in config["recipients"]],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": f"{config['code']}_Digital_Media_Report_{datetime.now():%Y%m%d}.pptx",
                    "contentType": "application/vnd.openxmlformats-officedocument."
                                   "presentationml.presentation",
                    "contentBytes": base64.b64encode(pptx_bytes).decode(),
                    "isInline": False,
                },
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "Preview.png",
                    "contentType": "image/png",
                    "contentBytes": base64.b64encode(png_bytes).decode(),
                    "contentId": "report_image",
                    "isInline": True,
                },
            ],
        }
    }
    url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    headers = {"Authorization": f"Bearer {graph_token}", "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()  # Graph returns 202 on success
