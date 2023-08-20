from time import sleep
import requests
import psycopg2
from dotenv import load_dotenv
import os

def registra_cliente_na_api(nome, sobrenome, cpf):
    url = 'https://sandbox.asaas.com/api/v3/customers'

    payload = {
        "name": f'{nome} {sobrenome}',
        "cpfCnpj": f'cpf'
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
        return registra_cliente_na_api(nome, sobrenome, cpf)

def busca_e_insere_dados(host, database, user, password):
    try:
        with psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        ) as connection:

            with connection.cursor() as cursor:

                sql = "SELECT name, surname, cpf FROM customers"
                cursor.execute(sql)

                for row in cursor:
                    name, surname, cpf = row
                    print(f"Name: {name}, Surname: {surname}, CPF: {cpf}")
                    asaas_id = registra_cliente_na_api(name, surname, cpf)
                    print(f"ID Asaas: {asaas_id}")
                    with connection.cursor() as cursor_update:
                        update = f"UPDATE customers SET customer_asaas_id = '{asaas_id}' WHERE cpf = '{cpf}'"
                        cursor_update.execute(update)

    except (Exception, psycopg2.Error) as error:
        print("Erro ao buscar dados:", error)

if __name__ == "__main__":

    load_dotenv()
    access_token = os.getenv("ACCESS_TOKEN")

    busca_e_insere_dados(
        host="localhost",
        database="mydb",
        user="myuser",
        password="mysecretpassword",

    )
