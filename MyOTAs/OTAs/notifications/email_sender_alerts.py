from datetime import datetime 
from azure.communication.email import EmailClient
from bs4 import BeautifulSoup
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

    def extract_information_from_html(self, overview_html):
        soup = BeautifulSoup(overview_html, 'html.parser')
        title_element = soup.find('a')
        if not title_element:
            raise ValueError("Title is missing")
        title = title_element.text.strip() 

        link = title_element['href'] if title_element and 'href' in title_element.attrs else "N/A"

        # Extract additional information
        records_analyzed = soup.find(text=lambda x: "Total records analyzed:" in x)
        date_range = soup.find(text=lambda x: "Date range:" in x)
        average_price = soup.find(text=lambda x: "Average price:" in x)
        highest_price = soup.find(text=lambda x: "Highest price:" in x)
        reviews = soup.find(text=lambda x: "Number of reviews:" in x)
        ota = soup.find(text=lambda x: "OTA:" in x)
        booked = soup.find(text=lambda x: "Booked:" in x)

        # Format the extracted data
        extracted_data = {
            "Title": title,
            "Link": link,
            "Total Records Analyzed": records_analyzed.split(":")[1].strip() if records_analyzed else "N/A",
            "Date Range": date_range.split(":")[1].strip() if date_range else "N/A",
            "Average Price": average_price.split(":")[1].strip() if average_price else "N/A",
            "Highest Price": highest_price.split(":")[1].strip() if highest_price else "N/A",
            "Number of Reviews": reviews.split(":")[1].strip() if reviews else "N/A",
            "Booked": booked.split(":")[1].strip() if booked else "N/A",
            "OTA": ota.split(":")[1].strip() if ota else "N/A",
        }

        return extracted_data
    def send_report_email_with_attachment(self, pdf_path, overview_html):
        """
        Sends an automated scheduled product report email with the given PDF file attached.
        Includes a concise overview of the PDF content in the email body.
        """
        overview_detials = self.extract_information_from_html(overview_html=overview_html)

        
        filtered_data = {key: value for key, value in overview_detials.items() if value != "N/A"}
        
        key_metrics_html = ""
        # Build the dynamic HTML for the key metrics
        for key, value in filtered_data.items():
            # Handle Title with clickable link
            if key == "Title":
                link = filtered_data.get("Link", "#")  # Get the link if available, or use '#' as a fallback
                key_metrics_html += f"""
                    <p><span class="metric-title">{key.replace('_', ' ')}:</span> 
                    <span class="metric-value"><a href="{link}" target="_blank" style="color: #009ADB; text-decoration: none;">{value}</a></span></p>
                """
            # Skip the link field as it's already embedded in the title
            elif key == "Link":
                continue
            # Handle other metrics normally
            else:
                key_metrics_html += f"""
                    <p><span class="metric-title">{key.replace('_', ' ')}:</span> <span class="metric-value">{value}</span></p>
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

            # Construct the email message
            message = {
                "senderAddress": "DoNotReply@6befcbca-8357-4801-8832-a8e8ffcf5b4c.azurecomm.net",
                "recipients": {
                    "to": [{"address": f"{self.email_address}"}],
                },
                "content": {
                    "subject": f"MyOTAs: Performance Report for {filtered_data.get('Title', 'Your Product')}",
                    "plainText": (
                        "Hello,\n\n"
                        "Product performance report is ready. "
                        "Key insights and a detailed PDF are attached.\n\n"
                        "Best regards,\nMyOTAs Team"
                    ),
                    "html": f"""
                        <html>
                        <head>
                            <meta charset="UTF-8" />
                            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
                            <style>
                                body {{
                                    font-family: 'Arial', sans-serif;
                                    max-width: 600px;
                                    margin: 0 auto;
                                    color: #333;
                                    line-height: 1.6;
                                    padding: 20px;
                                    background-color: #f4f6f9;
                                }}
                                .logo-container {{
                                    text-align: center;
                                    margin-bottom: 20px;
                                }}
                                .logo {{
                                    max-width: 200px;
                                    height: auto;
                                }}
                                .header {{
                                    background-color: #009ADB;
                                    color: white;
                                    text-align: center;
                                    padding: 15px;
                                    border-radius: 5px;
                                    margin-bottom: 20px;
                                    font-size: 20px;
                                    font-weight: bold;
                                }}
                                .content {{
                                    background-color: #ffffff;
                                    padding: 20px;
                                    border: 1px solid #e0e0e0;
                                    border-radius: 8px;
                                    box-shadow: 0px 3px 6px rgba(0, 0, 0, 0.1);
                                }}
                                .section-title {{
                                    font-size: 18px;
                                    color: #009ADB;
                                    margin-bottom: 10px;
                                    font-weight: bold;
                                }}
                                .overview {{
                                    background-color: #f9f9f9;
                                    border: 1px solid #ddd;
                                    padding: 15px;
                                    margin: 15px 0;
                                    border-radius: 5px;
                                }}
                                .highlight {{
                                    font-weight: bold;
                                    color: #009ADB;
                                }}
                                .footer {{
                                    text-align: center;
                                    color: #777;
                                    font-size: 12px;
                                    margin-top: 20px;
                                }}
                                .key-metrics {{
                                    margin-top: 20px;
                                    padding: 15px;
                                    border-radius: 5px;
                                    border: 1px solid #ddd;
                                    background-color: #f9f9f9;
                                }}
                                .key-metrics p {{
                                    margin: 8px 0;
                                    font-size: 16px;
                                }}
                                .metric-title {{
                                    font-weight: bold;
                                    color: #555;
                                }}
                                .metric-value {{
                                    color: #333;
                                }}
                            </style>
                        </head>
                        <body>
                            <div class="logo-container">
                                <img src="https://sapublicresourcesmyotas.blob.core.windows.net/resources/logo_color.png" 
                                    alt="MyOTAs Logo" class="logo" />
                            </div>
                            <div class="header">
                                Performance Insights for {filtered_data.get('Title', 'Your Product')}
                            </div>
                            <div class="content">
                                <p>Hello,</p>
                                <p>Here are the latest performance insights for your product:</p>
                                <div class="key-metrics">
                                    {key_metrics_html}
                                </div>
                                <p>For detailed information, please refer to the attached PDF report.</p>
                                <p>If you need any assistance, feel free to contact our support team.</p>
                                <p><strong>Best regards,<br/>MyOTAs Team</strong></p>
                            </div>
                            <div class="footer">
                                © {datetime.now().year} MyOTAs.com | Automated Product Reports
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