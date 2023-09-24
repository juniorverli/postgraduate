import psycopg2
from config.database import connect_to_database, fetch_pending_transactions, update_status
from kafka import KafkaProducer
import json

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_PENDING_TRANSACTION_TOPIC = 'pending_transaction_topic'

def pending_transactions():

    try:

        conn = connect_to_database()
        rows = fetch_pending_transactions(conn, "pending_transaction_kafka")

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
            
            transaction_json = json.dumps(message)

            producer.send(KAFKA_PENDING_TRANSACTION_TOPIC, value=transaction_json)
            print(f"Enviado para Kafka: {message}")
            update_status(conn, row[0], 'SEND_KAFKA', "pending_transaction_kafka")

        print(f"{len(rows)} registros enviados para o Kafka com sucesso.")

    except psycopg2.Error as e:
        print(f"Erro ao buscar e enviar registros para o Kafka: {e}")
    except Exception as e:
        print(f"Erro desconhecido: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    count = 0

    while count < 5000:
        pending_transactions()
        count += 1000
