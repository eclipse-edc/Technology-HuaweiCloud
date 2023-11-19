-- Statements are designed for and tested with Postgres only!

CREATE TABLE IF NOT EXISTS edc_lease
(
    leased_by      VARCHAR               NOT NULL,
    leased_at      BIGINT,
    lease_duration INTEGER DEFAULT 60000 NOT NULL,
    lease_id       VARCHAR               NOT NULL
        CONSTRAINT lease_pk
            PRIMARY KEY
);

COMMENT ON COLUMN edc_lease.leased_at IS 'posix timestamp of lease';

COMMENT ON COLUMN edc_lease.lease_duration IS 'duration of lease in milliseconds';



CREATE TABLE IF NOT EXISTS edc_contract_agreement
(
    agr_id            VARCHAR NOT NULL
        CONSTRAINT contract_agreement_pk
            PRIMARY KEY,
    provider_agent_id VARCHAR,
    consumer_agent_id VARCHAR,
    signing_date      BIGINT,
    start_date        BIGINT,
    end_date          INTEGER,
    asset_id          VARCHAR NOT NULL,
    policy            JSON
);


CREATE TABLE IF NOT EXISTS edc_contract_negotiation
(
    id                   VARCHAR           NOT NULL
        CONSTRAINT contract_negotiation_pk
            PRIMARY KEY,
    created_at           BIGINT            NOT NULL,
    updated_at           BIGINT            NOT NULL,
    correlation_id       VARCHAR,
    counterparty_id      VARCHAR           NOT NULL,
    counterparty_address VARCHAR           NOT NULL,
    protocol             VARCHAR           NOT NULL,
    type                 VARCHAR           NOT NULL,
    state                INTEGER DEFAULT 0 NOT NULL,
    state_count          INTEGER DEFAULT 0,
    state_timestamp      BIGINT,
    error_detail         VARCHAR,
    agreement_id         VARCHAR
        CONSTRAINT contract_negotiation_contract_agreement_id_fk
            REFERENCES edc_contract_agreement,
    contract_offers      JSON,
    callback_addresses   JSON,
    trace_context        JSON,
    pending              BOOLEAN DEFAULT FALSE,
    lease_id             VARCHAR
        CONSTRAINT contract_negotiation_lease_lease_id_fk
            REFERENCES edc_lease
            ON DELETE SET NULL,
    CONSTRAINT provider_correlation_id CHECK (type = '0' OR correlation_id IS NOT NULL)
);

COMMENT ON COLUMN edc_contract_negotiation.agreement_id IS 'ContractAgreement serialized as JSON';

COMMENT ON COLUMN edc_contract_negotiation.contract_offers IS 'List<ContractOffer> serialized as JSON';

COMMENT ON COLUMN edc_contract_negotiation.trace_context IS 'Map<String,String> serialized as JSON';

-- create indexes: IF NOT EXISTS syntax is not available in PG 9.2
-- more info here: https://dba.stackexchange.com/questions/35616/create-index-if-it-does-not-exist
DO
$$
    declare
        namespace text;
    BEGIN

        -- lease index
        namespace := 'public';
        IF (SELECT COUNT(*)
            FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = 'lease_lease_id_uindex'
              AND n.nspname = namespace) < 1 THEN
            CREATE UNIQUE INDEX lease_lease_id_uindex
                ON edc_lease (lease_id);
        END IF;

        -- correlation id index
        IF (SELECT COUNT(*)
            FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = 'contract_negotiation_correlationid_index'
              AND n.nspname = namespace) < 1 THEN
            CREATE UNIQUE INDEX contract_negotiation_correlationid_index
                ON edc_contract_negotiation (correlation_id);
        END IF;

        -- contract negotiation id index
        IF (SELECT COUNT(*)
            FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = 'contract_negotiation_id_uindex'
              AND n.nspname = namespace) < 1 THEN
            CREATE UNIQUE INDEX contract_negotiation_id_uindex
                ON edc_contract_negotiation (id);
        END IF;

        -- contract agreemnet id index
        IF (SELECT COUNT(*)
            FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = 'contract_agreement_id_uindex'
              AND n.nspname = namespace) < 1 THEN
            CREATE UNIQUE INDEX contract_agreement_id_uindex
                ON edc_contract_agreement (agr_id);
        END IF;
    END
$$;