CREATE TABLE Person (
    document STRING(15),
    name STRING(MAX),
    dt_nascimento DATE,
    id INT64 NOT NULL,
) PRIMARY KEY (id);