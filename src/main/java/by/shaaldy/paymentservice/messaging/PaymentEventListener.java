package by.shaaldy.paymentservice.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import by.shaaldy.paymentservice.messaging.event.OrderCreatedEvent;
import by.shaaldy.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentEventListener {
  private final PaymentService paymentService;
  private final PaymentEventPublisher publisher;

  @KafkaListener(topics = "order.created", groupId = "payment-service")
  public void onOrderCreated(OrderCreatedEvent event) {
    log.info("Received order.created for order {}", event.orderId());
    paymentService.processPayment(event).ifPresent(publisher::publish);
  }
}
