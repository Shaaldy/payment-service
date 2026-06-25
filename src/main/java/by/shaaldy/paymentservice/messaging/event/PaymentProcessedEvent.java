package by.shaaldy.paymentservice.messaging.event;

import java.util.UUID;

public record PaymentProcessedEvent(UUID paymentId, UUID orderId, boolean success) {}
