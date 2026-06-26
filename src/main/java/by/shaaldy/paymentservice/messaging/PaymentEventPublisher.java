package by.shaaldy.paymentservice.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import by.shaaldy.paymentservice.messaging.event.PaymentProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventPublisher {
  private static final String TOPIC = "payment.processed";
  private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;

  public void publish(PaymentProcessedEvent event) {
    kafkaTemplate.send(TOPIC, event.orderId().toString(), event);
    log.info("Published payment result for order {}", event.orderId());
  }
}
