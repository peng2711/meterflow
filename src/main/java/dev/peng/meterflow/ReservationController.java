package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.CommitInput;
import dev.peng.meterflow.Contracts.ReservationView;
import dev.peng.meterflow.Contracts.ReserveInput;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Reserve quota before an upstream call, then commit the actual usage or release it. */
@RestController
@RequestMapping("/v1/reservations")
class ReservationController {
    private final UsageService usage;

    ReservationController(UsageService usage) {
        this.usage = usage;
    }

    @PostMapping
    ReservationView reserve(@RequestHeader(value = "X-Api-Key", required = false) String apiKey,
                            @Valid @RequestBody ReserveInput input) {
        return usage.reserve(apiKey, input);
    }

    @PostMapping("/{requestId}/commit")
    ReservationView commit(@RequestHeader(value = "X-Api-Key", required = false) String apiKey,
                           @PathVariable String requestId, @Valid @RequestBody CommitInput input) {
        return usage.commit(apiKey, requestId, input);
    }

    @PostMapping("/{requestId}/release")
    ReservationView release(@RequestHeader(value = "X-Api-Key", required = false) String apiKey,
                            @PathVariable String requestId) {
        return usage.release(apiKey, requestId);
    }
}
