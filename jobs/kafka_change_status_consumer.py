import json
from kafka import KafkaConsumer
from config.database import connect_to_database, update_status

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

if __name__ == '__main__':

    consumer = KafkaConsumer("transaction_change_status", bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    for message in consumer:
        print(message)
        conn = connect_to_database()
        change_status = json.loads(message.value)
        update_status(conn, change_status['transaction_id'], change_status['new_status'], "pending_transaction_kafka")
        conn.close()
