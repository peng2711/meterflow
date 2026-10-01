ALTER TABLE tenants
    ADD CONSTRAINT ck_tenants_balance CHECK (used_units <= quota_units);
