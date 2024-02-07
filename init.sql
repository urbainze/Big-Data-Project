
CREATE TABLE IF NOT EXISTS purchase_aggregates(
    city VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered INTEGER,
    price_each DOUBLE PRECISION,
    states   VARCHAR(255),
    order_date TIMESTAMP,
    money_spend DOUBLE PRECISION
);

-- grant all privileges to the postgreSQL user 
GRANT ALL PRIVILEGES ON DATABASE postgresdb TO postgresuser;