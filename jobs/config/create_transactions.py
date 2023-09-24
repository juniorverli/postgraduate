import random
import datetime

from database import connect_to_database, insert_table

def generate_random_value():
    random_number = round(random.uniform(200, 5000), 2)
    return random_number

def generate_random_date():
    today = datetime.datetime.today()
    
    random_date = random.choice([today + datetime.timedelta(days=random.randint(30, 90)) for _ in range(10)])
    
    return random_date

def create_transactions(table_name):

    for i in range(0, 5000):
        customer_id = 1
        type_transaction = 'CREDIT_CARD'
        expiration_date = generate_random_date().strftime("%Y-%m-%d 23:59:59.999")
        value = generate_random_value()
        status = 'PENDING_SEND'

        insert_values = [
            (customer_id, type_transaction, expiration_date, value, status)
        ]

        print(f'INSERINDO: {customer_id}, {type_transaction}, {expiration_date}, {value}, {status}')

        conn = connect_to_database()
        if conn:
            insert_table(
                conn,
                table=table_name,
                columns=[
                    "customer_id", "type_transaction", "expiration_date", "value", "status"
                ],
                rows=insert_values
            )
            conn.close()

if __name__ == "__main__":
    create_transactions("pending_transaction_postgresql")
    create_transactions("pending_transaction_kafka")
