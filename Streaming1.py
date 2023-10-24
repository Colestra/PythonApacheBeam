import csv
import time
from google.cloud import pubsub_v1
import os


service_account_key = "chave-curso-apache-beam.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

topico = 'projects/curso-data-flow-368611/topics/MeusVoos'
publisher = pubsub_v1.PublisherClient()

entrada = 'voos_sample.csv'

with open(entrada, 'rb') as file:
    for row in file:
        print("Publishing in Topic")
        publisher.publish(topico, row)
        time.sleep(2)