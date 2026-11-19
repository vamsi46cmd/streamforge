package io.streamforge.api;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Consistent RFC 7807 error bodies for every endpoint. */
@RestControllerAdvice
public class Errors {

    public static class DependencyUnavailableException extends RuntimeException {
        public DependencyUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static ProblemDetail problem(HttpStatus status, String title, String detail, List<String> errors) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(status, detail);
        p.setTitle(title);
        if (errors != null && !errors.isEmpty()) {
            p.setProperty("errors", errors);
        }
        return p;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalid(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage()).toList();
        return problem(HttpStatus.BAD_REQUEST, "Validation failed", "The event is invalid.", errors);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ProblemDetail unreadable(HttpMessageNotReadableException ex) {
        return problem(HttpStatus.BAD_REQUEST, "Malformed request",
                "Body is not valid JSON or contains an unsupported value (check eventType, deviceType, timestamp, numbers).", null);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail badArgument(IllegalArgumentException ex) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", ex.getMessage(), null);
    }

    @ExceptionHandler(DependencyUnavailableException.class)
    ProblemDetail unavailable(DependencyUnavailableException ex) {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Dependency unavailable", ex.getMessage(), null);
    }
}
