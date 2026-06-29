package by.shaaldy.paymentservice.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import by.shaaldy.paymentservice.domain.Payment;
import by.shaaldy.paymentservice.domain.PaymentStatus;
import by.shaaldy.paymentservice.messaging.event.payment.OrderCreatedEvent;
import by.shaaldy.paymentservice.messaging.event.payment.PaymentProcessedEvent;
import by.shaaldy.paymentservice.repository.PaymentRepository;

@SuppressWarnings("removal")
class PaymentEventListenerIT extends AbstractIntegrationTest {

  @Autowired private PaymentRepository paymentRepository;

  private Producer<String, OrderCreatedEvent> producer;
  private Consumer<String, PaymentProcessedEvent> consumer;
  private static final String INPUT_TOPIC = "order.created";
  private static final String OUTPUT_TOPIC = "payment.processed";

  @BeforeEach
  void setUp() {
    paymentRepository.deleteAll();

    producer =
        new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(kafka.getBootstrapServers()),
                new StringSerializer(),
                new JsonSerializer<OrderCreatedEvent>())
            .createProducer();

    Map<String, Object> cProps =
        KafkaTestUtils.consumerProps(
            kafka.getBootstrapServers(), "test-" + UUID.randomUUID(), false);
    cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumer =
        new DefaultKafkaConsumerFactory<>(
                cProps,
                new StringDeserializer(),
                new JsonDeserializer<>(PaymentProcessedEvent.class, false))
            .createConsumer();
    consumer.subscribe(List.of(OUTPUT_TOPIC));
  }

  @AfterEach
  void tearDown() {
    if (producer != null) producer.close();
    if (consumer != null) consumer.close();
  }

  @Test
  void evenAmount_persistsSuccessPayment_andPublishesEvent() {

    UUID orderId = UUID.randomUUID();
    BigDecimal totalAmount = BigDecimal.valueOf(100000);
    OrderCreatedEvent event = new OrderCreatedEvent(orderId, totalAmount);
    producer.send(new ProducerRecord<>(INPUT_TOPIC, orderId.toString(), event));
    producer.flush();

    await().atMost(Duration.ofSeconds(10)).until(() -> paymentRepository.existsByOrderId(orderId));

    Optional<Payment> payment = paymentRepository.findByOrderId(orderId);

    assertThat(payment).isPresent();
    Payment res = payment.get();
    assertThat(res.getAmount()).isEqualByComparingTo(totalAmount);
    assertThat(res.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
    ConsumerRecords<String, PaymentProcessedEvent> records =
        KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
    PaymentProcessedEvent processed =
        StreamSupport.stream(records.spliterator(), false)
            .map(ConsumerRecord::value)
            .filter(e -> e.orderId().equals(orderId))
            .findFirst()
            .orElseThrow();
    assertThat(processed.success()).isTrue();
    assertThat(processed.orderId()).isEqualTo(orderId);
  }

  @Test
  void oddAmount_persistsFailedPayment_andPublishesEvent() {
    UUID orderId = UUID.randomUUID();
    BigDecimal totalAmount = BigDecimal.valueOf(100001);
    OrderCreatedEvent event = new OrderCreatedEvent(orderId, totalAmount);
    producer.send(new ProducerRecord<>(INPUT_TOPIC, orderId.toString(), event));
    producer.flush();

    await().atMost(Duration.ofSeconds(10)).until(() -> paymentRepository.existsByOrderId(orderId));

    Optional<Payment> payment = paymentRepository.findByOrderId(orderId);

    assertThat(payment).isPresent();
    Payment res = payment.get();
    assertThat(res.getAmount()).isEqualByComparingTo(totalAmount);
    assertThat(res.getStatus()).isEqualTo(PaymentStatus.FAILED);
    ConsumerRecords<String, PaymentProcessedEvent> records =
        KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
    PaymentProcessedEvent processed =
        StreamSupport.stream(records.spliterator(), false)
            .map(ConsumerRecord::value)
            .filter(e -> e.orderId().equals(orderId))
            .findFirst()
            .orElseThrow();
    assertThat(processed.success()).isFalse();
    assertThat(processed.orderId()).isEqualTo(orderId);
  }

  @Test
  void duplicateOrderId_processedOnce() {
    UUID orderId = UUID.randomUUID();
    OrderCreatedEvent event = new OrderCreatedEvent(orderId, BigDecimal.valueOf(100000));

    producer.send(new ProducerRecord<>(INPUT_TOPIC, orderId.toString(), event));
    producer.send(new ProducerRecord<>(INPUT_TOPIC, orderId.toString(), event));
    producer.flush();

    await().atMost(Duration.ofSeconds(10)).until(() -> paymentRepository.existsByOrderId(orderId));
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(10))
        .until(() -> paymentRepository.count() == 1);
  }
}
