package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.Contracts.UsageView;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/usage")
class UsageController {
    private final UsageService usage;

    UsageController(UsageService usage) {
        this.usage = usage;
    }

    @PostMapping
    UsageView record(@RequestHeader(value = "X-Api-Key", required = false) String apiKey,
                     @Valid @RequestBody UsageInput input) {
        return usage.record(apiKey, input);
    }
}
