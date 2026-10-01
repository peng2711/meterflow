package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.CreateTenant;
import dev.peng.meterflow.Contracts.EventView;
import dev.peng.meterflow.Contracts.IssuedKey;
import dev.peng.meterflow.Contracts.Reconciliation;
import dev.peng.meterflow.Contracts.TenantView;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
class AdminController {
    private final TenantService tenants;

    AdminController(TenantService tenants) {
        this.tenants = tenants;
    }

    @PostMapping("/tenants")
    ResponseEntity<TenantView> create(@Valid @RequestBody CreateTenant input) {
        TenantView created = tenants.create(input.name(), input.quotaUnits());
        return ResponseEntity.created(URI.create("/admin/tenants/" + created.id())).body(created);
    }

    @GetMapping("/tenants/{tenantId}")
    TenantView get(@PathVariable String tenantId) {
        return tenants.get(tenantId);
    }

    @PostMapping("/tenants/{tenantId}/keys")
    IssuedKey issue(@PathVariable String tenantId) {
        return tenants.issueKey(tenantId);
    }

    @DeleteMapping("/keys/{keyId}")
    ResponseEntity<Void> revoke(@PathVariable String keyId) {
        tenants.revokeKey(keyId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/tenants/{tenantId}/events")
    List<EventView> events(@PathVariable String tenantId) {
        return tenants.events(tenantId);
    }

    @GetMapping("/tenants/{tenantId}/reconciliation")
    Reconciliation reconcile(@PathVariable String tenantId) {
        return tenants.reconcile(tenantId);
    }
}

