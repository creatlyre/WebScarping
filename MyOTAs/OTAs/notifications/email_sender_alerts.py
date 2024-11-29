from azure.communication.email import EmailClient



class EmailSenderAlerts:
    def __init__(self, email_adress, product_name, product_url, alert_date, price_before, price_after, logger) -> None:
        self.email_adress = email_adress
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
            connection_string = f"endpoint=https://cs-emailsender-myotas.germany.communication.azure.com/;accesskey={self.access_key}"
            client = EmailClient.from_connection_string(connection_string)

            message = {
                "senderAddress": "DoNotReply@6befcbca-8357-4801-8832-a8e8ffcf5b4c.azurecomm.net",
                "recipients":  {
                    "to": [{"address": f"{self.email_adress}" }],
                },
                "content": {
                    "subject": f"MyOTAs - Price Update for {self.product_name}",
                    "plainText": f"""Hello,

                    Alert: The price for product {self.product_name} has changed from {self.price_before} to {self.price_after} for {self.alert_date}.
                    Visit the product page at {self.product_url}.

                    Best regards,
                    MyOTAs Team""",
                    "html": f"""
                        <html>
                        <body>
                            <p>Hello,</p>
                            <p>Alert: The price for product <a href="{self.product_url}">{self.product_name}</a> has changed from {self.price_before} to {self.price_after} for {self.alert_date}.</p>
                            <p>Best regards,<br/>MyOTAs Team</p>
                        </body>
                        </html>
                    """
                }
            }

            poller = client.begin_send(message)
            result = poller.result()
            self.logger.logger_info.info(f"Email sent successfully to {self.email_adress} for product {self.product_name}.")

        except Exception as ex:
            self.logger.logger_err.error(f"Failed to send email to {self.email_adress}: {ex}")

# %%