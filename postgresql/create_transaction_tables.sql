CREATE TABLE pending_transaction_postgresql (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    type_transaction VARCHAR(50) NOT NULL,
    created_date timestamp DEFAULT current_timestamp,
    expiration_date timestamp NOT NULL,
    payment_date timestamp NULL,
    value NUMERIC(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL
);

CREATE TABLE transaction_history_postgresql (
    id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    changed_date timestamp DEFAULT current_timestamp,
    FOREIGN KEY (transaction_id) REFERENCES pending_transaction_postgresql (id)
);

CREATE TABLE pending_transaction_kafka (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    type_transaction VARCHAR(50) NOT NULL,
    created_date timestamp DEFAULT current_timestamp,
    expiration_date timestamp NOT NULL,
    payment_date timestamp NULL,
    value NUMERIC(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL
);

CREATE TABLE transaction_history_kafka (
    id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    changed_date timestamp DEFAULT current_timestamp,
    FOREIGN KEY (transaction_id) REFERENCES pending_transaction_kafka (id)
);

CREATE OR REPLACE FUNCTION log_transaction_status_change_postgresql()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO transaction_history_postgresql (transaction_id, status)
    VALUES (OLD.id, NEW.status);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER transaction_status_change_postgresql_trigger
AFTER UPDATE ON pending_transaction_postgresql
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION log_transaction_status_change_postgresql();

CREATE OR REPLACE FUNCTION log_transaction_status_change_kafka()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO transaction_history_kafka (transaction_id, status)
    VALUES (OLD.id, NEW.status);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER transaction_status_change_kafka_trigger
AFTER UPDATE ON pending_transaction_kafka
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION log_transaction_status_change_kafka();
