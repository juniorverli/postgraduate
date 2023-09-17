CREATE TABLE customer (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    surname VARCHAR(50) NOT NULL,
    birthday_date DATE NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    cpf VARCHAR(11) NOT NULL UNIQUE,
    cep VARCHAR(8) NOT NULL,
    state VARCHAR(2) NULL,
    city VARCHAR(255) NULL,
    street VARCHAR(255) NULL,
    street_number INT NULL,
    phone_number VARCHAR(11) NOT NULL,
    card_number VARCHAR(20) NOT NULL,
    card_month VARCHAR(2) NOT NULL,
    card_year VARCHAR(4) NOT NULL,
    card_cvv INT NOT NULL,
    customer_asaas_id VARCHAR(50) NULL
);

INSERT INTO customer (
    name,
    surname,
    birthday_date,
    email,
    cpf,
    cep,
    state,
    city,
    street,
    street_number,
    phone_number,
    card_number,
    card_month,
    card_year,
    card_cvv
)
VALUES (
    'Pedro',
    'Santos',
    '1995-06-22',
    'pedro.santos@testxpeducacao.com.br',
    '67425765101',
    '48000001',
    'BA',
    'Alagoinhas',
    'Rua Mílton Santos França',
    '754',
    '95914336002',
    '5317727644864293',
    '10',
    '2026',
    '375'
);
