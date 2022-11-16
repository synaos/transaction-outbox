package com.synaos.transactionoutbox;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestValidator {

    private static final Invocation COMPLEX_INVOCATION =
            new Invocation(
                    "Foo",
                    "Bar",
                    new Class<?>[]{int.class, BigDecimal.class, String.class},
                    new Object[]{1, BigDecimal.TEN, null});

    private final Instant now = Instant.now();
    private final Validator validator = new Validator(() -> Clock.fixed(now, ZoneId.of("+4")));

    @Test
    void testEntryDateInPast() {
        TransactionOutboxEntry entry =
                TransactionOutboxEntry.builder()
                        .id("FOO")
                        .invocation(COMPLEX_INVOCATION)
                        .nextAttemptTime(now.minusMillis(1))
                        .build();
        assertThrows(IllegalArgumentException.class, () -> validator.validate(entry));
    }

    @Test
    void testEntryDateNow() {
        TransactionOutboxEntry entry =
                TransactionOutboxEntry.builder()
                        .id("FOO")
                        .invocation(COMPLEX_INVOCATION)
                        .nextAttemptTime(now)
                        .build();
        assertThrows(IllegalArgumentException.class, () -> validator.validate(entry));
    }

    @Test
    void testEntryDateFuture() {
        TransactionOutboxEntry entry =
                TransactionOutboxEntry.builder()
                        .id("FOO")
                        .invocation(COMPLEX_INVOCATION)
                        .nextAttemptTime(now.plusMillis(1))
                        .createdAt(Instant.now())
                        .build();
        assertDoesNotThrow(() -> validator.validate(entry));
    }
}
