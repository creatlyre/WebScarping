from azure.communication.email import EmailClient


class EmailSenderAlerts:
    def __init__(self, email_adress, product, alert_date, price_before, price_after) -> None:
        self.email_adress = email_adress
        self.product = product
        self.alert_date = alert_date
        self.price_before = price_before
        self.price_after = price_after
        self.access_key = "UN7iDkL+01/1HUHqRVgxYIxUZ4nGh6JUnKUW+x5CE5jGPgR9DLkKb4/EEgX74s1iKinxnaRANqRk6TNDzhyZ5w=="

   
    def main(self):
        try:
            connection_string = f"endpoint=https://cs-emailsender-myotas.germany.communication.azure.com/;accesskey={self.access_key}"
            client = EmailClient.from_connection_string(connection_string)

            message = {
                "senderAddress": "DoNotReply@6befcbca-8357-4801-8832-a8e8ffcf5b4c.azurecomm.net",
                "recipients":  {
                    "to": [{"address": f"{self.email_adress}" }],
                },
                "content": {
                    "subject": f"MyOTAs - Price Update for Product {self.product}",
                    "plainText": f"Alert: The price for product ABC123 has changed from {self.price_before} to {self.price_after} for {self.alert_date}. MyOTAs Team",
                }
            }

            poller = client.begin_send(message)
            result = poller.result()

        except Exception as ex:
            print(ex)
# %%