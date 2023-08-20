import random
import datetime
import psycopg2

def gerar_valor_aleatorio():
    numero_aleatorio = round(random.uniform(200, 5000), 2)
    return numero_aleatorio

def gerar_data_aleatoria():
    hoje = datetime.datetime.today()
    um_mes_a_frente = hoje + datetime.timedelta(days=30)
    tres_meses_a_frente = hoje + datetime.timedelta(days=90)
    
    data_aleatoria = random.choice([hoje + datetime.timedelta(days=random.randint(30, 90)) for _ in range(10)])
    
    return data_aleatoria

def inserir_dados(host, database, user, password, tabela, colunas, dados):
    try:
        with psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        ) as connection:

            with connection.cursor() as cursor:

                colunas_str = ", ".join(colunas)
                placeholders = ", ".join(["%s"] * len(colunas))
                sql = f"INSERT INTO {tabela} ({colunas_str}) VALUES ({placeholders})"

                for dado in dados:
                    cursor.execute(sql, dado)

                connection.commit()

        print("Dados inseridos com sucesso!")

    except (Exception, psycopg2.Error) as error:
        print("Erro ao inserir dados:", error)

if __name__ == "__main__":

    for i in range(0, 1000):
        customer_id = 1
        type_transaction = 'CREDIT_CARD'
        expiration_date = gerar_data_aleatoria().strftime("%Y-%m-%d 23:59:59.999")
        value = gerar_valor_aleatorio()
        status = 'PENDING_SEND'

        dados_para_inserir = [
            (customer_id, type_transaction, expiration_date, value, status)
        ]

        print(f'INSERINDO: {customer_id}, {type_transaction}, {expiration_date}, {value}, {status}')

        inserir_dados(host="localhost",
                    database="mydb",
                    user="myuser",
                    password="mysecretpassword",
                    tabela="pending_transactions",
                    colunas=[
                        "customer_id", "type_transaction", "expiration_date", "value", "status"
                    ],
                    dados=dados_para_inserir
        )



