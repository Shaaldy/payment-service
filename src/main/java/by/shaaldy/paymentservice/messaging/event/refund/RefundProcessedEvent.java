package by.shaaldy.paymentservice.messaging.event.refund;

import java.util.UUID;

public record RefundProcessedEvent(UUID orderId) {}
