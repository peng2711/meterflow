CREATE TABLE tenants (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    quota_units BIGINT NOT NULL,
    used_units BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT ck_tenants_quota CHECK (quota_units >= 0),
    CONSTRAINT ck_tenants_used CHECK (used_units >= 0)
);

CREATE TABLE api_keys (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id VARCHAR(36) NOT NULL,
    secret_hash CHAR(64) NOT NULL UNIQUE,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT fk_api_keys_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE INDEX ix_api_keys_tenant ON api_keys(tenant_id);

CREATE TABLE usage_events (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id VARCHAR(36) NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    units BIGINT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT fk_usage_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT uq_usage_request UNIQUE (tenant_id, request_id),
    CONSTRAINT ck_usage_units CHECK (units > 0)
);
CREATE INDEX ix_usage_tenant_created ON usage_events(tenant_id, created_at);
