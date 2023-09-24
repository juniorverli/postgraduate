import psycopg2

db_config = {
    'host': 'localhost',
    'database': 'mydb',
    'user': 'myuser',
    'password': 'mysecretpassword'
}

def connect_to_database():
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {str(e)}")
        return None
    
def insert_table(conn, table, columns, rows):
    try:
        with conn.cursor() as cursor:
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
            for row in rows:
                cursor.execute(sql, row)
            conn.commit()
        print("Dados inseridos com sucesso!")

    except (Exception, psycopg2.Error) as error:
        print("Erro ao inserir dados:", error)

def fetch_pending_transactions(conn, table_name):
    try:
        cursor = conn.cursor()
        sql = f"""
        SELECT
            {table_name}.id,
            {table_name}.type_transaction,
            {table_name}.created_date,
            {table_name}.expiration_date,
            {table_name}.payment_date,
            {table_name}.value,
            {table_name}.status,
            customer.name,
            customer.surname,
            customer.birthday_date,
            customer.email,
            customer.cpf,
            customer.cep,
            customer.state,
            customer.city,
            customer.street,
            customer.street_number,
            customer.phone_number,
            customer.card_number,
            customer.card_month,
            customer.card_year,
            customer.card_cvv,
            customer.customer_asaas_id
        FROM public.{table_name}
        INNER JOIN public.customer ON
	        {table_name}.customer_id = customer.id
        WHERE {table_name}.status = 'PENDING_SEND'
        ORDER BY 1 ASC
        LIMIT 1000;
        """
    
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    
    except Exception as e:
        print(f"Erro ao buscar transações pendentes: {str(e)}")
        return None
    
def update_status(conn, transaction_id, new_status, table_name):
    try:
        cursor = conn.cursor()
        update_sql = f"""
        UPDATE public.{table_name}
        SET status = '{new_status}'
        WHERE id = {transaction_id};
        """
        cursor.execute(update_sql)
        conn.commit()
        cursor.close()
        print(f"Status atualizado para '{new_status}' para a transação ID {transaction_id}")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao atualizar status para '{new_status}': {str(e)}")
