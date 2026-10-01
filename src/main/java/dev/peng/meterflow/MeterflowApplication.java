package dev.peng.meterflow;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MeterflowApplication {
    public static void main(String[] args) {
        SpringApplication.run(MeterflowApplication.class, args);
    }

    /** Reservation expiry reads time through this bean so tests can control it. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}

