package by.shaaldy.paymentservice.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import by.shaaldy.paymentservice.domain.Payment;
import by.shaaldy.paymentservice.domain.PaymentStatus;
import by.shaaldy.paymentservice.messaging.event.refund.OrderCancelledEvent;
import by.shaaldy.paymentservice.messaging.event.refund.RefundProcessedEvent;
import by.shaaldy.paymentservice.repository.PaymentRepository;
import by.shaaldy.paymentservice.repository.RefundRepository;
import tools.jackson.databind.ObjectMapper;

@SuppressWarnings("removal")
public class RefundSagaIT extends AbstractIntegrationTest {
  @Autowired private PaymentRepository paymentRepository;
  @Autowired private RefundRepository refundRepository;
  @Autowired ObjectMapper objectMapper;
  private Producer<String, String> producer;
  private Consumer<String, String> consumer;
  private static final String INPUT_TOPIC = "order.cancelled";
  private static final String OUTPUT_TOPIC = "payment.refunded";

  @BeforeEach
  void setUp() {
    refundRepository.deleteAll();
    paymentRepository.deleteAll();
    producer =
        new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(kafka.getBootstrapServers()),
                new StringSerializer(),
                new StringSerializer())
            .createProducer();

    Map<String, Object> cProps =
        KafkaTestUtils.consumerProps(
            kafka.getBootstrapServers(), "test-" + UUID.randomUUID(), false);
    cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumer =
        new DefaultKafkaConsumerFactory<>(
                cProps,
                new StringDeserializer(),
                new StringDeserializer()) // ← было JsonDeserializer<>(RefundProcessedEvent.class,
            // false)
            .createConsumer();
    consumer.subscribe(List.of(OUTPUT_TOPIC));
  }

  @AfterEach
  void tearDown() {
    if (producer != null) producer.close();
    if (consumer != null) consumer.close();
  }

  @Test
  void cancelledPaidOrder_refundsAndPublishes() {
    UUID orderId = UUID.randomUUID();
    Payment payment =
        Payment.builder()
            .orderId(orderId)
            .amount(new BigDecimal("100.00"))
            .status(PaymentStatus.SUCCESS)
            .build();
    payment = paymentRepository.save(payment);
    UUID paymentId = payment.getId();

    String payload = objectMapper.writeValueAsString(new OrderCancelledEvent(orderId));
    producer.send(new ProducerRecord<>(INPUT_TOPIC, orderId.toString(), payload));
    producer.flush();

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              Payment p = paymentRepository.findByOrderId(orderId).orElseThrow();
              assertThat(p.getStatus()).isEqualTo(PaymentStatus.REFUNDED);
            });

    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> refundRepository.existsByPaymentId(paymentId));

    RefundProcessedEvent event =
        await()
            .atMost(Duration.ofSeconds(10))
            .until(
                () -> {
                  ConsumerRecords<String, String> records =
                      KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
                  return StreamSupport.stream(records.spliterator(), false)
                      .map(
                          r ->
                              objectMapper.readValue(
                                  r.value(), RefundProcessedEvent.class)) // String → объект
                      .filter(e -> e.orderId().equals(orderId))
                      .findFirst()
                      .orElse(null);
                },
                Objects::nonNull);
    assertThat(event.orderId()).isEqualTo(orderId);
  }
}
