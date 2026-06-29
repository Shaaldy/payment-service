package by.shaaldy.paymentservice.messaging.event.payment;

import java.util.UUID;

public record PaymentProcessedEvent(UUID paymentId, UUID orderId, boolean success) {}
