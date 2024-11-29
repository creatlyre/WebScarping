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

        self.send_email()

   
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
