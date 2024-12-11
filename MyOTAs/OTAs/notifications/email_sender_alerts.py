from datetime import datetime 
from azure.communication.email import EmailClient
import os
import base64


class EmailSenderAlerts:
    def __init__(self, email_address, product_name, product_url, alert_date, price_before, price_after, logger) -> None:
        self.email_address = email_address
        self.product_name = product_name
        self.product_url = product_url
        self.alert_date = alert_date
        self.price_before = price_before
        self.price_after = price_after
        self.access_key = "UN7iDkL+01/1HUHqRVgxYIxUZ4nGh6JUnKUW+x5CE5jGPgR9DLkKb4/EEgX74s1iKinxnaRANqRk6TNDzhyZ5w=="
        self.logger = logger

        # self.send_email()

   
    def send_email(self):
        try:
            # Azure Communication Services connection string
            connection_string = f"endpoint=https://cs-emailsender-myotas.germany.communication.azure.com/;accesskey={self.access_key}"
            client = EmailClient.from_connection_string(connection_string)

            # Use the image URL hosted on Azure Blob Storage
            logo_url = 'https://sapublicresourcesmyotas.blob.core.windows.net/resources/logo_color.png'

            # Ensure you have the necessary attributes
            product_url = self.product_url  # URL to the product page
            product_name = self.product_name  # Name of the product

            message = {
                "senderAddress": "DoNotReply@6befcbca-8357-4801-8832-a8e8ffcf5b4c.azurecomm.net",
                "recipients":  {
                    "to": [{"address": f"{self.email_address}" }],
                },
                "content": {
                    "subject": f"MyOTAs - Price Update for {product_name}",
                    "plainText": f"""Hello,

    Alert: The price for product {product_name} has changed from {self.price_before} to {self.price_after} on {self.alert_date}.
    Visit the product page at {product_url}.

    Best regards,
    MyOTAs Team""",
                    "html": f"""
                        <html>
                        <body style="font-family: Arial, sans-serif; color: #333333; line-height: 1.6; margin: 0; padding: 0;">
                            <table width="100%" style="max-width: 600px; margin: auto; border-collapse: collapse;">
                                <tr>
                                    <td style="text-align: center; padding: 20px;">
                                        <img src="{logo_url}" alt="MyOTAs Logo" style="max-width: 200px; height: auto;">
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 20px; background-color: #ffffff;">
                                        <h2 style="color: #555555; margin-top: 0;">Price Update Notification</h2>
                                        <p>Dear Customer,</p>
                                        <p>We wanted to let you know that the price for the product you're interested in has changed.</p>
                                        <p><strong>Product:</strong> <a href="{product_url}" style="color: #1a73e8; text-decoration: none;">{product_name}</a></p>
                                        <p><strong>Previous Price:</strong> {self.price_before}€</p>
                                        <p><strong>New Price:</strong> {self.price_after}€</p>
                                        <p><strong>Date:</strong> {self.alert_date}</p>
                                        <p>You can view the product and take advantage of the new price by clicking the link above.</p>
                                        <p>If you have any questions, feel free to contact our support team.</p>
                                        <p style="margin-bottom: 0;">Best regards,<br/>MyOTAs Team</p>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="text-align: center; padding: 10px; background-color: #f2f2f2; font-size: 12px; color: #888888;">
                                        <p style="margin: 0;">© {datetime.now().year} MyOTAs. All rights reserved.</p>
                                    </td>
                                </tr>
                            </table>
                        </body>
                        </html>
                    """
                }
            }

            poller = client.begin_send(message)
            result = poller.result()
            self.logger.logger_info.info(f"Email sent successfully to {self.email_address} for product {product_name}.")

        except Exception as ex:
            self.logger.logger_err.error(f"Failed to send email to {self.email_address}: {ex}")


    def send_report_email_with_attachment(self, pdf_path, overview_html):
        """
        Sends an automated scheduled product report email with the given PDF file attached.
        Includes a short overview of the PDF content in the email body.
        """
        try:
            # Read PDF file and encode in base64
            with open(pdf_path, "rb") as pdf_file:
                pdf_data = pdf_file.read()
            pdf_base64 = base64.b64encode(pdf_data).decode('utf-8')

            # Azure Communication Services connection string
            connection_string = (
                f"endpoint=https://cs-emailsender-myotas.germany.communication.azure.com/;"
                f"accesskey={self.access_key}"
            )
            client = EmailClient.from_connection_string(connection_string)

            message = {
                "senderAddress": "DoNotReply@6befcbca-8357-4801-8832-a8e8ffcf5b4c.azurecomm.net",
                "recipients": {
                    "to": [{"address": f"{self.email_address}"}],
                },
                "content": {
                    "subject": "Your Comprehensive Automated Product Report",
                    "plainText": (
                        "Hello,\n\n"
                        "We are pleased to provide you with your latest product performance report.\n"
                        "This detailed PDF includes insights into pricing, reviews, and more, "
                        "helping you make data-driven decisions.\n\n"
                        "Please find the attached PDF report.\n\n"
                        "Best regards,\n"
                        "MyOTAs Team"
                    ),
                    "html": f"""
                        <html>
                        <head>
                            <meta charset="UTF-8" />
                            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
                            <style>
                                body {{
                                    font-family: Arial, sans-serif; 
                                    color: #333; 
                                    line-height: 1.6; 
                                    margin: 0; 
                                    padding: 0;
                                }}
                                .header {{
                                    background-color: #009ADB;
                                    padding: 20px;
                                    text-align: center;
                                    color: #ffffff;
                                }}
                                .content {{
                                    padding: 20px;
                                }}
                                .content h2 {{
                                    color: #009ADB;
                                    font-size: 24px;
                                    margin-top: 0;
                                }}
                                .report-overview {{
                                    background-color: #f9f9f9;
                                    border: 1px solid #ddd;
                                    padding: 15px;
                                    border-radius: 5px;
                                    margin: 20px 0;
                                }}
                                .report-overview h3 {{
                                    color: #009ADB;
                                    margin-top: 0;
                                }}
                                .report-overview p {{
                                    margin: 5px 0;
                                }}
                                .footer {{
                                    text-align: center;
                                    color: #999;
                                    font-size: 12px;
                                    padding: 20px;
                                }}
                                a.button {{
                                    background-color: #009ADB;
                                    color: #ffffff;
                                    padding: 10px 20px;
                                    text-decoration: none;
                                    border-radius: 4px;
                                    font-weight: bold;
                                }}
                                a.button:hover {{
                                    background-color: #0079a8;
                                }}
                            </style>
                        </head>
                        <body>
                            <div class="header">
                                <img src="https://sapublicresourcesmyotas.blob.core.windows.net/resources/logo_color.png" alt="MyOTAs Logo" style="max-width: 200px; margin-bottom: 10px;" /><br/>
                                <h1>Your Scheduled Product Report</h1>
                            </div>
                            <div class="content">
                                <h2>Overview & Insights</h2>
                                <p>Dear Valued Partner,</p>
                                <p>We are pleased to share your automated product report, offering a deep dive into the latest performance metrics. Below is a snapshot of key insights:</p>
                                <div class="report-overview">
                                    {overview_html}
                                </div>
                                <p>For a detailed analysis, please review the attached PDF report. It includes comprehensive data on pricing trends, user reviews, and other valuable metrics to help you make informed decisions.</p>
                                <p>Should you have any questions or require further assistance, feel free to reach out to our support team.</p>
                                <p>Thank you for choosing MyOTAs!</p>
                                <p><strong>Best regards,<br/>MyOTAs Team</strong></p>
                            </div>
                            <div class="footer">
                                <p>You are receiving this email because you subscribed to MyOTAs reports.</p>
                                <p>© {datetime.now().year} MyOTAs.com. All rights reserved.</p>
                            </div>
                        </body>
                        </html>
                    """
                },
                "attachments": [
                    {
                        "name": "product_report.pdf",
                        "contentType": "application/pdf",
                        "contentInBase64": pdf_base64
                    }
                ]
            }

            # Send the email
            poller = client.begin_send(message)
            result = poller.result()
            self.logger.logger_info.info(f"Report email with attachment sent successfully to {self.email_address}.")

        except Exception as ex:
            self.logger.logger_err.error(f"Failed to send report email to {self.email_address}: {ex}")
