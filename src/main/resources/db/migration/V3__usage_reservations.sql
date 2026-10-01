ALTER TABLE tenants ADD COLUMN reserved_units BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenants ADD CONSTRAINT ck_tenants_reserved CHECK (reserved_units >= 0);
-- Open reservations hold quota too, so a reservation can never promise more than is left.
ALTER TABLE tenants ADD CONSTRAINT ck_tenants_committed CHECK (used_units + reserved_units <= quota_units);

CREATE TABLE usage_reservations (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id VARCHAR(36) NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    reserved_units BIGINT NOT NULL,
    state VARCHAR(16) NOT NULL,
    committed_units BIGINT NULL,
    event_id VARCHAR(36) NULL,
    expires_at TIMESTAMP(6) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    settled_at TIMESTAMP(6) NULL,
    CONSTRAINT fk_reservations_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT uq_reservations_request UNIQUE (tenant_id, request_id),
    CONSTRAINT ck_reservations_units CHECK (reserved_units > 0),
    CONSTRAINT ck_reservations_state CHECK (state IN ('RESERVED', 'COMMITTED', 'RELEASED', 'EXPIRED')),
    CONSTRAINT ck_reservations_committed CHECK (committed_units IS NULL OR committed_units BETWEEN 1 AND reserved_units)
);
CREATE INDEX ix_reservations_expiry ON usage_reservations(state, expires_at);
