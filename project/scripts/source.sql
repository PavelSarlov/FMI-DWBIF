-- Generated by Oracle SQL Developer Data Modeler 22.2.0.165.1149
--   at:        2023-01-25 23:48:21 EET
--   site:      Oracle Database 21c
--   type:      Oracle Database 21c



-- predefined type, no DDL - MDSYS.SDO_GEOMETRY

-- predefined type, no DDL - XMLTYPE

CREATE TABLE account (
    account_id  INTEGER NOT NULL,
    district_id INTEGER NOT NULL,
    "date"      INTEGER,
    frequency   VARCHAR(30)
);

ALTER TABLE account ADD CONSTRAINT account_pk PRIMARY KEY ( account_id );

CREATE TABLE client (
    client_id    INTEGER NOT NULL,
    district_id  INTEGER NOT NULL,
    birth_number INTEGER
);

ALTER TABLE client ADD CONSTRAINT client_pk PRIMARY KEY ( client_id );

CREATE TABLE credit_card (
    card_id INTEGER NOT NULL,
    disp_id INTEGER NOT NULL,
    type    VARCHAR(10),
    issued  VARCHAR(20)
);

ALTER TABLE credit_card ADD CONSTRAINT credit_card_pk PRIMARY KEY ( card_id );

CREATE TABLE demographic_data (
    a1  INTEGER NOT NULL,
    a2  VARCHAR(50),
    a3  VARCHAR(50),
    a4  INTEGER,
    a5  INTEGER,
    a6  INTEGER,
    a7  INTEGER,
    a8  INTEGER,
    a9  INTEGER,
    a10 FLOAT,
    a11 INTEGER,
    a12 FLOAT,
    a13 FLOAT,
    a14 INTEGER,
    a15 INTEGER,
    a16 INTEGER
);

ALTER TABLE demographic_data ADD CONSTRAINT demographic_data_pk PRIMARY KEY ( a1 );

CREATE TABLE disposition (
    disp_id    INTEGER NOT NULL,
    client_id  INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    type       VARCHAR(10)
);

ALTER TABLE disposition ADD CONSTRAINT disposition_pk PRIMARY KEY ( disp_id );

CREATE TABLE loan (
    loan_id    INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    "date"     INTEGER,
    amount     INTEGER,
    duration   INTEGER,
    payments   DECIMAL,
    status     VARCHAR(1)
);

CREATE UNIQUE INDEX loan__idx ON
    loan (
        account_id
    ASC );

ALTER TABLE loan ADD CONSTRAINT loan_pk PRIMARY KEY ( loan_id );

CREATE TABLE permanent_order (
    order_id   INTEGER NOT NULL,
    account_id INTEGER,
    bank_to    VARCHAR(2),
    account_to INTEGER,
    amount     DECIMAL,
    k_symbol   VARCHAR(20)
);

ALTER TABLE permanent_order ADD CONSTRAINT permanent_order_pk PRIMARY KEY ( order_id );

CREATE TABLE transaction (
    trans_id   INTEGER NOT NULL,
    account_id INTEGER,
    "date"     INTEGER,
    type       VARCHAR(10),
    operation  VARCHAR(30),
    amount     DECIMAL,
    balance    DECIMAL,
    k_symbol   VARCHAR(20),
    bank       VARCHAR(2),
    account    INTEGER
);

ALTER TABLE transaction ADD CONSTRAINT transaction_pk PRIMARY KEY ( trans_id );

ALTER TABLE disposition
    ADD CONSTRAINT account__disposition FOREIGN KEY ( account_id )
        REFERENCES account ( account_id );

ALTER TABLE loan
    ADD CONSTRAINT account__loan FOREIGN KEY ( account_id )
        REFERENCES account ( account_id )
            ON DELETE CASCADE;

ALTER TABLE permanent_order
    ADD CONSTRAINT account__permanent_order FOREIGN KEY ( account_id )
        REFERENCES account ( account_id );

ALTER TABLE transaction
    ADD CONSTRAINT account__transaction FOREIGN KEY ( account_id )
        REFERENCES account ( account_id );

ALTER TABLE disposition
    ADD CONSTRAINT client__disposition FOREIGN KEY ( client_id )
        REFERENCES client ( client_id );

ALTER TABLE account
    ADD CONSTRAINT demographic_data__account FOREIGN KEY ( district_id )
        REFERENCES demographic_data ( a1 );

ALTER TABLE client
    ADD CONSTRAINT demographic_data__client FOREIGN KEY ( district_id )
        REFERENCES demographic_data ( a1 );

ALTER TABLE credit_card
    ADD CONSTRAINT disposition__credit_card FOREIGN KEY ( disp_id )
        REFERENCES disposition ( disp_id );



-- Oracle SQL Developer Data Modeler Summary Report: 
-- 
-- CREATE TABLE                             8
-- CREATE INDEX                             1
-- ALTER TABLE                             16
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
