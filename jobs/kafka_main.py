import json
import os
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import requests

load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_PENDING_TRANSACTION_TOPIC = 'pending_transaction_topic'
KAFKA_TRANSACTION_CHANGE_STATUS = 'transaction_change_status'

mensagens_processadas = set()

def transaction_change_status(transaction_id, new_status):

    status_update_message = {
        "transaction_id": f"{transaction_id}",
        "new_status": f"{new_status}"
    }

    status_update_message_json = json.dumps(status_update_message)

    producer.send(KAFKA_TRANSACTION_CHANGE_STATUS, value=status_update_message_json)

def send_payment_request(transaction):
    
    try:
        asaas_api_url = 'https://sandbox.asaas.com/api/v3/payments'

        headers = {
            'Content-Type': 'application/json',
            'access_token': access_token
        }

        payload = {
            "billingType": transaction['type_transaction'],
            "creditCard": {
                "holderName": transaction['name'] + " " + transaction['surname'],
                "number": transaction['card_number'],
                "expiryMonth": transaction['card_month'],
                "expiryYear": transaction['card_year'],
                "ccv": transaction['card_cvv']
            },
            "creditCardHolderInfo": {
                "name": transaction['name'] + " " + transaction['surname'],
                "email": transaction['email'],
                "cpfCnpj": transaction['cpf'],
                "postalCode": transaction['cep'],
                "addressNumber": transaction['street_number'],
                "phone": transaction['phone_number']
            },
            "customer": transaction['customer_asaas_id'],
            "value": transaction['value'],
            "dueDate": transaction['expiration_date'][:10],
            "remoteIp": "localhost"
        }

        transaction_change_status(transaction['id'], 'SEND')
        response = requests.post(asaas_api_url, headers=headers, json=payload)

        if response.status_code == 200:
            print(f"Pedido de pagamento enviado com sucesso para a transação ID {transaction['id']}")
            transaction_change_status(transaction['id'], 'CONFIRMED')
        
        elif response.status_code == 400:
            print(f"Pedido de pagamento rejeitado para a transação ID {transaction['id']}")
            transaction_change_status(transaction['id'], 'REJECTED')

        else:
            print(f"Erro ao enviar pedido de pagamento para a transação ID {transaction['id']}: {response.text}")
            transaction_change_status(transaction['id'], 'SEND_ERROR')

    except Exception as e:
        print(f"Erro ao enviar pedido de pagamento: {str(e)}")


if __name__ == '__main__':

    consumer = KafkaConsumer(KAFKA_PENDING_TRANSACTION_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    count = 0

    for message in consumer:
        if count == 0:
            start_time = time.time()
        if count == 1000:
            break
        transaction = json.loads(message.value)
        if transaction['id'] in mensagens_processadas:
            continue
        mensagens_processadas.add(transaction['id'])
        print(transaction)
        send_payment_request(transaction)
        count+=1

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tempo de execução total: {elapsed_time} segundos")

