from time import sleep
import requests
import psycopg2
from database import connect_to_database
from dotenv import load_dotenv
import os

load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")

def register_customer_in_api(name, surname, cpf):
    url = 'https://sandbox.asaas.com/api/v3/customers'

    payload = {
        "name": f'{name} {surname}',
        "cpfCnpj": f'{cpf}'
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "access_token": access_token
    }

    try:
        sleep(0.5)
        response = requests.post(url, json=payload, headers=headers)
        data = response.json()

        if 'erro' in data:
            return print("CPF Inválido")
        else:
            asaas_id = data.get('id', '')
            return asaas_id

    except requests.RequestException:
        return register_customer_in_api(name, surname, cpf)

def get_and_insert_customer_asaas_id(conn):
    try:
        with conn.cursor() as cursor:

            sql = "SELECT name, surname, cpf FROM customer"
            cursor.execute(sql)

            for row in cursor:
                name, surname, cpf = row
                print(f"Name: {name}, Surname: {surname}, CPF: {cpf}")
                asaas_id = register_customer_in_api(name, surname, cpf)
                print(f"ID Asaas: {asaas_id}")
                with conn.cursor() as cursor_update:
                    update = f"UPDATE customer SET customer_asaas_id = '{asaas_id}' WHERE cpf = '{cpf}'"
                    cursor_update.execute(update)

    except (Exception, psycopg2.Error) as error:
        print("Erro ao buscar dados:", error)

if __name__ == "__main__":

    conn = connect_to_database()
    if conn:
        get_and_insert_customer_asaas_id(conn)
        conn.close()
