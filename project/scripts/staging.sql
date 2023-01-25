-- Generated by Oracle SQL Developer Data Modeler 22.2.0.165.1149
--   at:        2023-01-26 00:07:25 EET
--   site:      Oracle Database 21c
--   type:      Oracle Database 21c



-- predefined type, no DDL - MDSYS.SDO_GEOMETRY

-- predefined type, no DDL - XMLTYPE

CREATE TABLE account (
    staged_at   DATE NOT NULL,
    account_id  INTEGER NOT NULL,
    district_id INTEGER,
    "date"      INTEGER,
    frequency   VARCHAR(30)
);

ALTER TABLE account ADD CONSTRAINT account_pk PRIMARY KEY ( account_id,
                                                            staged_at );

CREATE TABLE client (
    staged_at    DATE NOT NULL,
    client_id    INTEGER NOT NULL,
    district_id  INTEGER,
    birth_number INTEGER
);

ALTER TABLE client ADD CONSTRAINT client_pk PRIMARY KEY ( client_id,
                                                          staged_at );

CREATE TABLE credit_card (
    staged_at DATE NOT NULL,
    card_id   INTEGER NOT NULL,
    disp_id   INTEGER,
    type      VARCHAR(10),
    issued    VARCHAR(20)
);

ALTER TABLE credit_card ADD CONSTRAINT credit_card_pk PRIMARY KEY ( card_id,
                                                                    staged_at );

CREATE TABLE demographic_data (
    staged_at DATE NOT NULL,
    a1        INTEGER NOT NULL,
    a2        VARCHAR(50),
    a3        VARCHAR(50),
    a4        INTEGER,
    a5        INTEGER,
    a6        INTEGER,
    a7        INTEGER,
    a8        INTEGER,
    a9        INTEGER,
    a10       FLOAT,
    a11       INTEGER,
    a12       FLOAT,
    a13       FLOAT,
    a14       INTEGER,
    a15       INTEGER,
    a16       INTEGER
);

ALTER TABLE demographic_data ADD CONSTRAINT demographic_data_pk PRIMARY KEY ( a1,
                                                                              staged_at );

CREATE TABLE disposition (
    staged_at  DATE NOT NULL,
    disp_id    INTEGER NOT NULL,
    client_id  INTEGER,
    account_id INTEGER,
    type       VARCHAR(10)
);

ALTER TABLE disposition ADD CONSTRAINT disposition_pk PRIMARY KEY ( disp_id,
                                                                    staged_at );

CREATE TABLE loan (
    staged_at  DATE NOT NULL,
    loan_id    INTEGER NOT NULL,
    account_id INTEGER,
    "date"     INTEGER,
    amount     INTEGER,
    duration   INTEGER,
    payments   DECIMAL,
    status     VARCHAR(1)
);

ALTER TABLE loan ADD CONSTRAINT loan_pk PRIMARY KEY ( loan_id,
                                                      staged_at );

CREATE TABLE permanent_order (
    staged_at  DATE NOT NULL,
    order_id   INTEGER NOT NULL,
    account_id INTEGER,
    bank_to    VARCHAR(2),
    account_to INTEGER,
    amount     DECIMAL,
    k_symbol   VARCHAR(20)
);

ALTER TABLE permanent_order ADD CONSTRAINT permanent_order_pk PRIMARY KEY ( order_id,
                                                                            staged_at );

CREATE TABLE transaction (
    staged_at  DATE NOT NULL,
    trans_id   INTEGER NOT NULL,
    account_id INTEGER,
    "date"     INTEGER,
    type       VARCHAR(10),
    operation  VARCHAR(20),
    amount     DECIMAL,
    balance    DECIMAL,
    k_symbol   VARCHAR(20),
    bank       VARCHAR(2),
    account    INTEGER
);

ALTER TABLE transaction ADD CONSTRAINT transaction_pk PRIMARY KEY ( trans_id,
                                                                    staged_at );



-- Oracle SQL Developer Data Modeler Summary Report: 
-- 
-- CREATE TABLE                             8
-- CREATE INDEX                             0
-- ALTER TABLE                              8
-- CREATE VIEW                              0
-- ALTER VIEW                               0
-- CREATE PACKAGE                           0
-- CREATE PACKAGE BODY                      0
-- CREATE PROCEDURE                         0
-- CREATE FUNCTION                          0
-- CREATE TRIGGER                           0
-- ALTER TRIGGER                            0
-- CREATE COLLECTION TYPE                   0
-- CREATE STRUCTURED TYPE                   0
-- CREATE STRUCTURED TYPE BODY              0
-- CREATE CLUSTER                           0
-- CREATE CONTEXT                           0
-- CREATE DATABASE                          0
-- CREATE DIMENSION                         0
-- CREATE DIRECTORY                         0
-- CREATE DISK GROUP                        0
-- CREATE ROLE                              0
-- CREATE ROLLBACK SEGMENT                  0
-- CREATE SEQUENCE                          0
-- CREATE MATERIALIZED VIEW                 0
-- CREATE MATERIALIZED VIEW LOG             0
-- CREATE SYNONYM                           0
-- CREATE TABLESPACE                        0
-- CREATE USER                              0
-- 
-- DROP TABLESPACE                          0
-- DROP DATABASE                            0
-- 
-- REDACTION POLICY                         0
-- 
-- ORDS DROP SCHEMA                         0
-- ORDS ENABLE SCHEMA                       0
-- ORDS ENABLE OBJECT                       0
-- 
-- ERRORS                                   0
-- WARNINGS                                 0
