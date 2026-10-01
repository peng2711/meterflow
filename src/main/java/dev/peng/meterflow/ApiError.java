package dev.peng.meterflow;

import java.time.Instant;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

class ApiError extends RuntimeException {
    private final HttpStatus status;
    private final String code;

    ApiError(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    HttpStatus status() { return status; }
    String code() { return code; }
}

@RestControllerAdvice
class ApiErrorHandler {
    @ExceptionHandler(ApiError.class)
    ResponseEntity<Map<String, Object>> known(ApiError error) {
        return ResponseEntity.status(error.status()).body(Map.of(
                "code", error.code(), "message", error.getMessage(), "timestamp", Instant.now().toString()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Map<String, Object>> invalid(MethodArgumentNotValidException error) {
        return ResponseEntity.badRequest().body(Map.of("code", "INVALID_INPUT",
                "message", "请求字段无效", "timestamp", Instant.now().toString()));
    }
}

