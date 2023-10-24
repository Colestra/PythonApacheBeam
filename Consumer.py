import csv
import time
from google.cloud import pubsub_v1
import os


service_account_key = "chave-curso-apache-beam.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

subscription = 'projects/curso-data-flow-368611/subscriptions/MeusVoos-sub'
subscriber = pubsub_v1.SubscriberClient()

def mostrar_msg(mensagem):
    print(('Mensagem: {}'.format(mensagem)))
    mensagem.ack()

subscriber.subscribe(subscription, callback=mostrar_msg)

while True:
    time.sleep(5)