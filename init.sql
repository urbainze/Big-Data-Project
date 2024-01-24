
CREATE TABLE IF NOT EXISTS purchase_aggregates(
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    product VARCHAR(255),
    avg_ord DOUBLE PRECISION
);

-- grant all privileges to the postgreSQL user 
GRANT ALL PRIVILEGES ON DATABASE postgresdb TO postgresuser;