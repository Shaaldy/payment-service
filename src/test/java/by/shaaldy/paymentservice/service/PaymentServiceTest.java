package by.shaaldy.paymentservice.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import by.shaaldy.paymentservice.domain.OutboxMessage;
import by.shaaldy.paymentservice.domain.Payment;
import by.shaaldy.paymentservice.domain.PaymentStatus;
import by.shaaldy.paymentservice.messaging.event.payment.OrderCreatedEvent;
import by.shaaldy.paymentservice.messaging.event.payment.PaymentProcessedEvent;
import by.shaaldy.paymentservice.repository.OutboxRepository;
import by.shaaldy.paymentservice.repository.PaymentRepository;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
public class PaymentServiceTest {
  @Mock private PaymentRepository paymentRepository;

  @Captor private ArgumentCaptor<Payment> paymentCaptor;
  @Mock private OutboxRepository outboxRepository;
  @Spy private ObjectMapper objectMapper = new ObjectMapper();
  @InjectMocks private PaymentService paymentService;

  @Test
  void valid_evenIntegerPart_returnsSuccess() {
    UUID id = UUID.randomUUID();
    BigDecimal totalAmount = BigDecimal.valueOf(100000);
    OrderCreatedEvent event = new OrderCreatedEvent(id, totalAmount);
    when(paymentRepository.existsByOrderId(id)).thenReturn(false);
    when(paymentRepository.save(any(Payment.class)))
        .thenAnswer(
            invocation -> {
              Payment p = invocation.getArgument(0);
              p.setId(UUID.randomUUID());
              return p;
            });
    ArgumentCaptor<OutboxMessage> outboxCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
    Optional<PaymentProcessedEvent> res = paymentService.processPayment(event);

    assertThat(res).isPresent();
    PaymentProcessedEvent processed = res.get();
    assertThat(processed.orderId()).isEqualTo(id);
    assertThat(processed.paymentId()).isNotNull();
    assertThat(processed.success()).isTrue();

    verify(outboxRepository).save(outboxCaptor.capture());
    OutboxMessage message = outboxCaptor.getValue();
    assertThat(message.getTopic()).isEqualTo("payment.processed");

    verify(paymentRepository).save(paymentCaptor.capture());
    Payment persisted = paymentCaptor.getValue();
    assertThat(persisted.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
    assertThat(persisted.getAmount()).isEqualByComparingTo(totalAmount);
  }

  @Test
  void valid_oddIntegerPart_returnsFailed() {
    UUID id = UUID.randomUUID();
    BigDecimal totalAmount = BigDecimal.valueOf(511);
    OrderCreatedEvent event = new OrderCreatedEvent(id, totalAmount);
    when(paymentRepository.existsByOrderId(id)).thenReturn(false);
    when(paymentRepository.save(any(Payment.class)))
        .thenAnswer(
            invocation -> {
              Payment p = invocation.getArgument(0);
              p.setId(UUID.randomUUID());
              return p;
            });

    ArgumentCaptor<OutboxMessage> outboxCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
    Optional<PaymentProcessedEvent> res = paymentService.processPayment(event);

    assertThat(res).isPresent();
    PaymentProcessedEvent processed = res.get();
    assertThat(processed.orderId()).isEqualTo(id);
    assertThat(processed.paymentId()).isNotNull();
    assertThat(processed.success()).isFalse();

    verify(outboxRepository).save(outboxCaptor.capture());
    OutboxMessage message = outboxCaptor.getValue();
    assertThat(message.getTopic()).isEqualTo("payment.processed");

    verify(paymentRepository).save(paymentCaptor.capture());
    Payment persisted = paymentCaptor.getValue();
    assertThat(persisted.getStatus()).isEqualTo(PaymentStatus.FAILED);
    assertThat(persisted.getAmount()).isEqualByComparingTo(totalAmount);
  }

  @Test
  void existingPayment_skipsProcessing() {
    UUID id = UUID.randomUUID();
    BigDecimal totalAmount = BigDecimal.valueOf(10000);
    OrderCreatedEvent event = new OrderCreatedEvent(id, totalAmount);
    when(paymentRepository.existsByOrderId(id)).thenReturn(true);

    Optional<PaymentProcessedEvent> res = paymentService.processPayment(event);
    assertThat(res).isEmpty();
    verify(paymentRepository, never()).save(any());
    verify(outboxRepository, never()).save(any());
  }

  @Test
  void valid_evenDoublePart_returnsSuccess() {
    UUID id = UUID.randomUUID();
    BigDecimal totalAmount = new BigDecimal("100000.99");
    OrderCreatedEvent event = new OrderCreatedEvent(id, totalAmount);
    when(paymentRepository.existsByOrderId(id)).thenReturn(false);
    when(paymentRepository.save(any(Payment.class)))
        .thenAnswer(
            invocation -> {
              Payment p = invocation.getArgument(0);
              p.setId(UUID.randomUUID());
              return p;
            });

    ArgumentCaptor<OutboxMessage> outboxCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
    Optional<PaymentProcessedEvent> res = paymentService.processPayment(event);

    assertThat(res).isPresent();
    PaymentProcessedEvent processed = res.get();
    assertThat(processed.orderId()).isEqualTo(id);
    assertThat(processed.paymentId()).isNotNull();
    assertThat(processed.success()).isTrue();

    verify(outboxRepository).save(outboxCaptor.capture());
    OutboxMessage message = outboxCaptor.getValue();
    assertThat(message.getTopic()).isEqualTo("payment.processed");

    verify(paymentRepository).save(paymentCaptor.capture());
    Payment persisted = paymentCaptor.getValue();
    assertThat(persisted.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
    assertThat(persisted.getAmount()).isEqualByComparingTo(totalAmount);
  }

  @Test
  void valid_oddDoublePart_returnsFailed() {
    UUID id = UUID.randomUUID();
    BigDecimal totalAmount = new BigDecimal("100001.99");
    OrderCreatedEvent event = new OrderCreatedEvent(id, totalAmount);
    when(paymentRepository.existsByOrderId(id)).thenReturn(false);
    when(paymentRepository.save(any(Payment.class)))
        .thenAnswer(
            invocation -> {
              Payment p = invocation.getArgument(0);
              p.setId(UUID.randomUUID());
              return p;
            });

    ArgumentCaptor<OutboxMessage> outboxCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
    Optional<PaymentProcessedEvent> res = paymentService.processPayment(event);

    assertThat(res).isPresent();
    PaymentProcessedEvent processed = res.get();
    assertThat(processed.orderId()).isEqualTo(id);
    assertThat(processed.paymentId()).isNotNull();
    assertThat(processed.success()).isFalse();

    verify(outboxRepository).save(outboxCaptor.capture());
    OutboxMessage message = outboxCaptor.getValue();
    assertThat(message.getTopic()).isEqualTo("payment.processed");

    verify(paymentRepository).save(paymentCaptor.capture());
    Payment persisted = paymentCaptor.getValue();
    assertThat(persisted.getStatus()).isEqualTo(PaymentStatus.FAILED);
    assertThat(persisted.getAmount()).isEqualByComparingTo(totalAmount);
  }
}
