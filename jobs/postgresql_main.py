import os
import json
import requests
import time
from config.database import connect_to_database, fetch_pending_transactions, update_status
from dotenv import load_dotenv

def pending_transactions():

    conn = connect_to_database()
    rows = fetch_pending_transactions(conn, "pending_transaction_postgresql")

    transactions = []

    for row in rows:
       message = {
           'id': row[0],
           'type_transaction': row[1],
           'created_date': str(row[2]),
           'expiration_date': str(row[3]),
           'payment_date': str(row[4]),
           'value': float(row[5]),
           'status': row[6],
           'name': row[7],
           'surname': row[8],
           'birthday_date': str(row[9]),
           'email': row[10],
           'cpf': row[11],
           'cep': row[12],
           'state': row[13],
           'city': row[14],
           'street': row[15],
           'street_number': row[16],
           'phone_number': row[17],
           'card_number': row[18],
           'card_month': row[19],
           'card_year': row[20],
           'card_cvv': row[21],
           'customer_asaas_id': row[22]
       }
       update_status(conn, row[0], 'SEND_POSTGRESQL', "pending_transaction_postgresql")
       transactions.append(message)
    conn.close()
    return json.dumps(transactions)

def send_payment_request(transaction):
    
    conn = connect_to_database()

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

        update_status(conn, transaction['id'], 'SEND_API', "pending_transaction_postgresql")
        response = requests.post(asaas_api_url, headers=headers, json=payload)

        if response.status_code == 200:
            print(f"Pedido de pagamento enviado com sucesso para a transação ID {transaction['id']}")
            update_status(conn, transaction['id'], 'CONFIRMED', "pending_transaction_postgresql")
        
        elif response.status_code == 400:
            print(f"Pedido de pagamento rejeitado para a transação ID {transaction['id']}")
            update_status(conn, transaction['id'], 'REJECTED', "pending_transaction_postgresql")

        else:
            print(f"Erro ao enviar pedido de pagamento para a transação ID {transaction['id']}: {response.text}")
            update_status(conn, transaction['id'], 'SEND_ERROR', "pending_transaction_postgresql")

    except Exception as e:
        print(f"Erro ao enviar pedido de pagamento: {str(e)}")

    conn.close()

if __name__ == '__main__':

    load_dotenv()
    access_token = os.getenv("ACCESS_TOKEN")

    count = 0
    start_time = time.time()
    while count < 5000:
        transactions = json.loads(pending_transactions())

        for transaction in transactions:
            
            print(transaction)
            send_payment_request(transaction)
            count += 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tempo de execução total: {elapsed_time} segundos")
