/*
 *  Copyright (c) 2024 Huawei Technologies
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Huawei Technologies - initial API and implementation
 *
 */

-- Statements are designed for and tested with Postgres only!

CREATE TABLE IF NOT EXISTS edc_lease
(
    leased_by      VARCHAR NOT NULL,
    leased_at      BIGINT,
    lease_duration INTEGER NOT NULL,
    lease_id       VARCHAR NOT NULL
        CONSTRAINT lease_pk
            PRIMARY KEY
);

COMMENT ON COLUMN edc_lease.leased_at IS 'posix timestamp of lease';
COMMENT ON COLUMN edc_lease.lease_duration IS 'duration of lease in milliseconds';

CREATE TABLE IF NOT EXISTS edc_policy_monitor
(
    entry_id         VARCHAR           NOT NULL PRIMARY KEY,
    state            INTEGER           NOT NULL,
    created_at       BIGINT            NOT NULL,
    updated_at       BIGINT            NOT NULL,
    state_count      INTEGER DEFAULT 0 NOT NULL,
    state_time_stamp BIGINT,
    trace_context    JSON,
    error_detail     VARCHAR,
    lease_id         VARCHAR
        CONSTRAINT policy_monitor_lease_lease_id_fk
            REFERENCES edc_lease
            ON DELETE SET NULL,
    properties       JSON,
    contract_id      VARCHAR
);
